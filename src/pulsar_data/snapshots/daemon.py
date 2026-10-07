"""The snapshot collect daemon (采集守护进程).

A long-running process that accumulates realtime snapshots into the lake's
``snapshots`` partition family.  It *consumes* the D5 realtime channel
unchanged — the same :class:`~pulsar_data.realtime.collector.FailoverQuoteSource`
routing (Sina primary, Eastmoney fallback, guarded egress, rate limits,
circuit breakers), the same best-effort dispatcher semantics (per-symbol
monotonic ``seq``, late/missing markers) — and only adds persistence:

* **buffer → partition write** — every dispatcher event (snapshot, late,
  missing, thinned-by-policy) is buffered and flushed into the lake with
  atomic merge-writes and watermark refreshes; a flush failure keeps the
  buffer and retries on the next tick, so nothing is discarded silently;
* **restart continuation** — on start the daemon reloads the persisted
  per-symbol state; when the new session resumes after downtime on the
  same trade date, the discontinuity is appended to the gap ledger
  (断线重连自动补水位并记缺口 — snapshots cannot be backfilled from the
  free sources, so the interval is recorded explicitly instead);
* **disk watermark monitoring** — each flush tick measures the family
  footprint and the volume's free space against the policy thresholds and
  writes an alert record on each crossing.

Deterministic driving: pass ``manual=True`` and the daemon never starts a
thread — tests (and replay tooling) push cycles through the dispatcher's
``ingest`` and call :meth:`SnapshotCollectorDaemon.flush` themselves.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from ..lake import DataLake
from ..realtime.collector import SnapshotSource, build_default_source
from ..realtime.dispatcher import SnapshotDispatcher
from ..realtime.events import EventKind, StreamEvent
from ..schema import Dataset, ts_to_date
from .policy import SnapshotPolicy
from .store import SnapshotStore

logger = logging.getLogger("pulsar_data.snapshots.daemon")

__all__ = ["SnapshotCollectorDaemon", "collect_status"]

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _now() -> datetime:
    return datetime.now(tz=_SHANGHAI)


class SnapshotCollectorDaemon:
    """Accumulate the realtime snapshot stream into the lake, long-term.

    Parameters
    ----------
    lake:
        the data lake to write into (``snapshots`` family).
    policy:
        the validated accumulation policy (universe, sampling cap, flush
        cadence, disk thresholds).  The universe resolves at construction.
    source:
        overrides the quote source (tests inject fakes); default is the
        declared D5 routing — Sina primary, Eastmoney fallback.
    dispatcher:
        overrides the dispatcher entirely (tests drive manual ones).
    clock:
        injectable "now" for restart-gap detection.
    manual:
        when True no background thread is started and the caller drives
        cycles/flushes — the offline test path.
    session_id:
        stable session label persisted with every row (default: random).
    """

    def __init__(
        self,
        lake: DataLake,
        policy: SnapshotPolicy,
        *,
        source: SnapshotSource | None = None,
        dispatcher: SnapshotDispatcher | None = None,
        clock: Callable[[], datetime] = _now,
        manual: bool = False,
        session_id: str | None = None,
    ) -> None:
        self.lake = lake
        self.policy = policy
        self.store = SnapshotStore(lake)
        self.clock = clock
        self.manual = manual
        self.session_id = session_id or uuid.uuid4().hex[:8]
        self.symbols: tuple[str, ...] = policy.resolve_symbols(lake)
        self._owned_dispatcher = dispatcher is None
        self.dispatcher = dispatcher or SnapshotDispatcher(
            source if source is not None else build_default_source(),
            poll_interval=policy.poll_interval_s,
            auto_start=not manual,
        )
        self._buffer: list[dict[str, Any]] = []
        self._buffer_lock = threading.Lock()
        self._flush_stop = threading.Event()
        self._flusher: threading.Thread | None = None
        self._subscription = None
        self._last_sample_ts: dict[str, datetime] = {}
        self._disk_warned = False
        self._started_at: datetime | None = None
        self.stats: dict[str, int] = {
            "cycles": 0,
            "snapshots": 0,
            "late": 0,
            "missing": 0,
            "thinned": 0,
            "rows_written": 0,
            "flushes": 0,
            "flush_errors": 0,
            "session_gaps_recorded": 0,
        }

    # ------------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Subscribe to the channel, record restart gaps, start flushing."""
        now = self.clock()
        self._started_at = now
        self._record_restart_gaps(now)
        self._subscription = self.dispatcher.subscribe_events(self.symbols, self._on_event)
        logger.info(
            "snapshot collect session %s started for %d symbol(s) (%s)",
            self.session_id,
            len(self.symbols),
            "full market" if self.policy.full_market else "watchlist",
        )
        if not self.manual:
            self._flusher = threading.Thread(
                target=self._flush_loop, name="pulsar-snapshot-flush", daemon=True
            )
            self._flusher.start()

    def stop(self) -> None:
        """Unsubscribe, stop the flusher, and flush what remains (idempotent)."""
        subscription = self._subscription
        if subscription is not None:
            try:
                subscription.unsubscribe()
            except Exception:  # noqa: BLE001 - teardown must not fail
                logger.exception("unsubscribe during stop failed")
            self._subscription = None
        self._flush_stop.set()
        flusher = self._flusher
        if flusher is not None and flusher.is_alive():
            flusher.join(timeout=max(5.0, 2 * self.policy.flush_interval_s))
        self._flusher = None
        try:
            self.flush()
        except Exception:  # noqa: BLE001 - final flush best effort, already counted
            logger.exception("final snapshot flush failed")
        if self._owned_dispatcher:
            self.dispatcher.close()

    def __enter__(self) -> "SnapshotCollectorDaemon":
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    # ------------------------------------------------------------ the stream
    def _on_event(self, event: StreamEvent) -> None:
        """Buffer exactly one row per event; thinning may re-mark usable quotes."""
        self.stats["cycles"] += 1
        kind = event.kind.value
        snapshot = event.snapshot
        if snapshot is not None and self.policy.min_sample_interval_s > 0:
            last = self._last_sample_ts.get(event.symbol)
            if (
                last is not None
                and abs((event.ts - last).total_seconds()) < self.policy.min_sample_interval_s
            ):
                kind = "thinned"
                snapshot = None  # the data is dropped by policy, the marker stays
        if snapshot is not None:
            self._last_sample_ts[event.symbol] = event.ts
        self.stats[
            {
                EventKind.SNAPSHOT.value: "snapshots",
                EventKind.LATE.value: "late",
                EventKind.MISSING.value: "missing",
                "thinned": "thinned",
            }[kind]
        ] += 1
        source_name = getattr(self.dispatcher._source, "last_source", "") or ""  # noqa: SLF001
        row = SnapshotStore.event_to_row(
            event, session_id=self.session_id, source_name=source_name, kind=kind
        )
        trigger = False
        with self._buffer_lock:
            self._buffer.append(row)
            trigger = len(self._buffer) >= self.policy.flush_rows
        if trigger and not self.manual:
            # backpressure: flush inline on the pump thread rather than
            # letting the buffer (and memory) grow without bound
            self._safe_flush()

    # -------------------------------------------------------------- flushing
    def _flush_loop(self) -> None:
        while not self._flush_stop.wait(self.policy.flush_interval_s):
            self._safe_flush()
            self._check_disk()  # disk watermark is monitored even on idle ticks

    def _safe_flush(self) -> None:
        try:
            self.flush()
        except Exception:  # noqa: BLE001 - the daemon must survive flush errors
            self.stats["flush_errors"] += 1
            logger.exception("snapshot flush failed; buffer retained for retry")

    def pending_rows(self) -> int:
        with self._buffer_lock:
            return len(self._buffer)

    def flush(self) -> int:
        """Write the buffered rows to the lake; returns rows persisted.

        The buffer is claimed atomically; a write failure re-extends the
        buffer (front position kept) so the rows survive for the next
        attempt — a flush error never silently drops frames.  State and
        disk checks follow a successful write only.
        """
        with self._buffer_lock:
            rows = self._buffer
            self._buffer = []
        if not rows:
            return 0
        try:
            result = self.store.write_events(rows, session_id=self.session_id)
        except Exception:
            with self._buffer_lock:
                self._buffer = rows + self._buffer
            raise
        self.stats["rows_written"] += result.rows
        self.stats["flushes"] += 1
        self._write_state(rows)
        self._check_disk()
        return result.rows

    def _write_state(self, rows: list[dict[str, Any]] | None = None) -> None:
        """Persist the per-symbol watermark tail (what the lake truly holds).

        The state file is the persistence watermark used for restart-gap
        detection: it advances only with successfully written rows, merging
        over the previous file so idle ticks never erase it.  ``ts`` is the
        *newest persisted row timestamp* per symbol (marker rows carry the
        cycle time, so this is the last-contact watermark — a late quote
        never rewinds it), and ``seq`` the highest persisted cycle number
        of its session.
        """
        if rows is None:
            return
        symbols = self.store.load_state().get("symbols", {})
        for row in rows:
            symbol = row["symbol"]
            entry = symbols.get(symbol)
            if entry is not None and entry.get("session") == row["session"]:
                row = {
                    **row,
                    "seq": max(int(entry.get("seq", 0)), int(row["seq"])),
                    "ts": max(pd.Timestamp(entry["ts"]), row["ts"]),
                }
            symbols[symbol] = {
                "session": row["session"],
                "seq": int(row["seq"]),
                "ts": row["ts"].isoformat(),
            }
        self.store.save_state(
            session_id=self.session_id,
            symbols=symbols,
            extra={
                "pid": os.getpid(),
                "started_at": self._started_at.isoformat() if self._started_at else None,
            },
        )

    # -------------------------------------------------------- restart gaps
    def _record_restart_gaps(self, now: datetime) -> None:
        """Compare persisted state with ``now``; ledger every downtime gap.

        A gap is: same trade date, previous state exists, and the
        discontinuity exceeds the policy threshold.  Free sources offer no
        snapshot backfill, so the record itself is the remediation — the
        watermark catches up with the first fresh poll of this session.
        """
        state = self.store.load_state()
        threshold = timedelta(seconds=self.policy.reconnect_gap_threshold_s)
        recorded = 0
        for symbol in self.symbols:
            info = state.get("symbols", {}).get(symbol)
            if not info:
                continue
            try:
                last_ts = datetime.fromisoformat(info["ts"])
            except (KeyError, ValueError):
                continue
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=_SHANGHAI)
            downtime = now - last_ts
            if ts_to_date(last_ts) != ts_to_date(now) or downtime <= threshold:
                continue
            self.store.append_gap(
                {
                    "type": "session_gap",
                    "symbol": symbol,
                    "from_ts": last_ts.isoformat(),
                    "to_ts": now.isoformat(),
                    "duration_s": downtime.total_seconds(),
                    "from_session": info.get("session"),
                    "to_session": self.session_id,
                    "recorded_at": now.isoformat(),
                    "note": "daemon restart; free sources offer no snapshot backfill",
                }
            )
            recorded += 1
        if recorded:
            self.stats["session_gaps_recorded"] += recorded
            logger.warning(
                "session %s recorded %d restart gap(s) into %s",
                self.session_id,
                recorded,
                self.store.gaps_path,
            )

    # -------------------------------------------------------------- disk
    def _check_disk(self) -> None:
        """Alert (ledger + log) when a disk threshold is crossed."""
        report = self.store.disk_report(self.policy)
        if report["warn"] and not self._disk_warned:
            self._disk_warned = True
            record = {"type": "disk_warn", "at": _now().isoformat(), **report}
            self.store.append_alert(record)
            logger.warning("snapshot disk watermark crossed: %s", report["reasons"])
        elif not report["warn"]:
            self._disk_warned = False

    # -------------------------------------------------------------- status
    def status(self) -> dict[str, Any]:
        """A snapshot of daemon health for the CLI / pulsar-app."""
        return {
            "session": self.session_id,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "symbols": list(self.symbols),
            "stats": dict(self.stats),
            "pending_rows": self.pending_rows(),
            "disk": self.store.disk_report(self.policy),
        }


def collect_status(store: SnapshotStore, policy: SnapshotPolicy, *, days: int = 7) -> dict[str, Any]:
    """Operator-facing lake status: partitions, watermarks, gaps, disk."""
    state = store.load_state()
    watermarks = store.lake.watermarks()
    snapshot_marks = (
        watermarks[watermarks["dataset"] == Dataset.SNAPSHOTS.value]
        if not watermarks.empty
        else watermarks
    )
    return {
        "last_session": state.get("session"),
        "updated_at": state.get("updated_at"),
        "symbols_state": state.get("symbols", {}),
        "raw_partitions": store.partition_count(Dataset.SNAPSHOTS),
        "archive_partitions": store.partition_count(Dataset.SNAPSHOTS_1M),
        "raw_bytes": store.family_bytes(Dataset.SNAPSHOTS),
        "archive_bytes": store.family_bytes(Dataset.SNAPSHOTS_1M),
        "missing_markers_recent": store.missing_marker_summary(days=days),
        "session_gaps": store.session_gaps(),
        "alerts": store.alerts(),
        "watermark_rows": len(snapshot_marks),
        "disk": store.disk_report(policy),
        "policy": policy.to_dict(),
    }
