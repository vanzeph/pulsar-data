"""The ``snapshots`` partition family: persistence, state, gaps, disk.

This is the lake-side half of the snapshot-accumulation daemon
(设计：快照流积累).  It reuses the D3 lake machinery verbatim —
partition splitting, atomic ``.tmp`` + ``os.replace`` writes, merge-dedupe
and watermark bookkeeping — and adds what a long-running collector needs
on top:

* :meth:`SnapshotStore.write_events` turns buffered
  :class:`~pulsar_data.realtime.events.StreamEvent` rows into one
  ``snapshots/symbol=…/date=…/part.parquet`` merge-write plus a
  watermark refresh (每源每分区水位);
* a small JSON *state file* (``_meta/snapshot_state.json``) carries the
  per-symbol last persisted ``seq``/``ts`` across daemon restarts, so a
  new session can detect the discontinuity and record it;
* an append-only *gap ledger* (``_meta/snapshot_gaps.jsonl``) makes every
  session gap (断线重连) an explicit, reviewable record — downtime is
  never silently skipped;
* an *alert ledger* (``_meta/snapshot_alerts.jsonl``) records disk
  watermark crossings;
* :meth:`SnapshotStore.disk_report` measures the family footprint against
  the policy's alert thresholds (磁盘水位监控与告警阈值).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from ..errors import LakeError
from ..lake import DataLake
from ..realtime.events import EventKind, StreamEvent
from ..schema import SNAPSHOT_COLUMNS, Dataset, ts_to_date
from .policy import SnapshotPolicy

logger = logging.getLogger("pulsar_data.snapshots.store")

__all__ = ["SnapshotStore", "WriteResult", "SNAPSHOT_SOURCE"]

_SHANGHAI = ZoneInfo("Asia/Shanghai")

#: Watermark ``source`` label of the collect daemon (the realtime channel).
SNAPSHOT_SOURCE = "realtime"
#: Watermark ``source`` label of the downsample archive task.
ARCHIVE_SOURCE = "snapshot-archive"

_STATE_COLUMNS = ("session", "seq", "ts")


@dataclass(frozen=True)
class WriteResult:
    """Outcome of one :meth:`SnapshotStore.write_events` flush."""

    rows: int
    partitions: list[str]
    synced_through: date | None


def _iso(ts: datetime) -> str:
    return ts.astimezone(_SHANGHAI).isoformat()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


class SnapshotStore:
    """Read/write access to the ``snapshots`` family of one lake."""

    def __init__(self, lake: DataLake) -> None:
        self.lake = lake
        self.root = lake.root
        self.state_path = self.root / "_meta" / "snapshot_state.json"
        self.gaps_path = self.root / "_meta" / "snapshot_gaps.jsonl"
        self.alerts_path = self.root / "_meta" / "snapshot_alerts.jsonl"

    # ------------------------------------------------------------------ write
    @staticmethod
    def event_to_row(
        event: StreamEvent, *, session_id: str, source_name: str, kind: str | None = None
    ) -> dict[str, Any]:
        """Flatten one dispatcher event into a canonical snapshot row.

        ``kind`` overrides the persisted marker (the daemon passes
        ``"thinned"`` when the sampling cap drops a usable quote — the
        quote data is then dropped with it, the marker row stays) — every
        cycle still lands exactly one row, so gaps in the persisted ``seq``
        space can only mean unflushed crash loss, never policy skipping.
        """
        snapshot = event.snapshot if kind is None or kind == event.kind.value else None
        row: dict[str, Any] = {
            "symbol": event.symbol,
            "ts": pd.Timestamp(event.ts),
            "seq": int(event.seq),
            "kind": kind or event.kind.value,
            "source": source_name,
            "session": session_id,
        }
        if snapshot is not None:
            row["last_price"] = snapshot.last_price
            row["volume"] = snapshot.volume
            row["amount"] = snapshot.amount
            for side in ("bids", "asks"):
                levels = getattr(snapshot, side)
                prefix = "bid" if side == "bids" else "ask"
                for level in range(1, 6):
                    entry = levels[level - 1] if level - 1 < len(levels) else None
                    row[f"{prefix}{level}_price"] = float(entry.price) if entry else float("nan")
                    row[f"{prefix}{level}_volume"] = float(entry.volume) if entry else float("nan")
        else:
            row["last_price"] = float("nan")
            row["volume"] = float("nan")
            row["amount"] = float("nan")
            for prefix in ("bid", "ask"):
                for level in range(1, 6):
                    row[f"{prefix}{level}_price"] = float("nan")
                    row[f"{prefix}{level}_volume"] = float("nan")
        return row

    def write_events(self, rows: Sequence[dict[str, Any]], *, session_id: str) -> WriteResult:
        """Merge ``rows`` into their ``symbol × date`` partitions atomically.

        Uses :meth:`DataLake.merge_write` (dedupe on symbol+session+seq,
        incoming rows win), then refreshes the per-partition watermarks —
        so a crash between write and watermark only ever repeats work.
        """
        if not rows:
            return WriteResult(0, [], None)
        frame = pd.DataFrame(rows)[list(SNAPSHOT_COLUMNS)]
        self._validate(frame)
        partitions = self.lake.merge_write(Dataset.SNAPSHOTS, frame, source=SNAPSHOT_SOURCE)
        days = sorted({ts_to_date(ts) for ts in frame["ts"]})
        synced_through = days[-1] if days else None
        self.lake.update_watermark(
            source=SNAPSHOT_SOURCE,
            dataset=Dataset.SNAPSHOTS.value,
            partitions=partitions,
            rows=len(frame),
            synced_through=synced_through or date.today(),
        )
        return WriteResult(len(frame), partitions, synced_through)

    @staticmethod
    def _validate(frame: pd.DataFrame) -> None:
        """Light hard gates (the heavy quality gates are the bar pipeline's)."""
        if list(frame.columns) != list(SNAPSHOT_COLUMNS):
            raise LakeError("snapshot frame columns do not match the canonical schema")
        usable = frame["kind"].isin((EventKind.SNAPSHOT.value, EventKind.LATE.value, "thinned"))
        bad = frame[usable & (frame["last_price"] <= 0)]
        if not bad.empty:
            raise LakeError(
                f"{len(bad)} usable snapshot row(s) carry a non-positive last_price"
            )

    # ------------------------------------------------------------------ read
    def read(self, symbols: Iterable[str] | None = None) -> pd.DataFrame:
        """Every persisted raw snapshot row (all partitions, sorted)."""
        return self.lake.read(Dataset.SNAPSHOTS, symbols=symbols)

    def read_archives(self, symbols: Iterable[str] | None = None) -> pd.DataFrame:
        """Every persisted downsampled archive row."""
        return self.lake.read(Dataset.SNAPSHOTS_1M, symbols=symbols)

    def partition_dates(self, dataset: Dataset = Dataset.SNAPSHOTS) -> list[date]:
        """Trade dates that exist for ``dataset``, ascending."""
        base = self.root / dataset.value
        out: set[date] = set()
        for directory in base.glob("symbol=*/date=*"):
            value = directory.name.removeprefix("date=")
            try:
                out.add(date.fromisoformat(value))
            except ValueError:
                logger.warning("unparseable snapshot partition date %s", directory)
        return sorted(out)

    def partition_count(self, dataset: Dataset = Dataset.SNAPSHOTS) -> int:
        return sum(1 for _ in (self.root / dataset.value).glob("symbol=*/date=*"))

    # ----------------------------------------------------------------- state
    def load_state(self) -> dict[str, Any]:
        """The daemon state file (per-symbol last seq/ts of the last flush)."""
        if not self.state_path.exists():
            return {"symbols": {}, "updated_at": None}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except ValueError:
            logger.warning("snapshot state file unreadable; starting fresh")
            return {"symbols": {}, "updated_at": None}
        payload.setdefault("symbols", {})
        return payload

    def save_state(
        self, *, session_id: str, symbols: dict[str, dict[str, Any]], extra: dict[str, Any] | None = None
    ) -> None:
        """Atomically persist the collector state (tmp + replace)."""
        payload: dict[str, Any] = {
            "session": session_id,
            "symbols": symbols,
            "updated_at": _iso(datetime.now(tz=_SHANGHAI)),
        }
        if extra:
            payload.update(extra)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_name(f".tmp-{uuid.uuid4().hex}.json")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.state_path)

    # ------------------------------------------------------------- gap ledger
    def append_gap(self, record: dict[str, Any]) -> None:
        """Append one session-gap record to the ledger (one JSON per line)."""
        self.gaps_path.parent.mkdir(parents=True, exist_ok=True)
        with self.gaps_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def session_gaps(self, *, symbol: str | None = None) -> list[dict[str, Any]]:
        """Every recorded session gap, oldest first."""
        if not self.gaps_path.exists():
            return []
        out: list[dict[str, Any]] = []
        for line in self.gaps_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                logger.warning("unparseable gap ledger line skipped")
                continue
            if symbol is None or record.get("symbol") == symbol:
                out.append(record)
        return out

    def missing_marker_summary(self, *, days: int = 7, dataset: Dataset = Dataset.SNAPSHOTS) -> dict[str, int]:
        """Count persisted ``missing`` marker rows per symbol over recent days.

        The in-stream form of the gap report: every cycle that produced no
        usable quote is already a persisted row, so this scan is pure
        bookkeeping over the last ``days`` trade dates.
        """
        if days <= 0 or not self.partition_dates(dataset):
            return {}
        recent = set(self.partition_dates(dataset)[-days:])
        counts: dict[str, int] = {}
        for file in sorted((self.root / dataset.value).glob("symbol=*/date=*/part.parquet")):
            day = date.fromisoformat(file.parent.name.removeprefix("date="))
            if day not in recent:
                continue
            frame = pd.read_parquet(file, engine="pyarrow", columns=["symbol", "kind"])
            missing = frame[frame["kind"] == EventKind.MISSING.value]
            for symbol, count in missing.groupby("symbol").size().items():
                counts[symbol] = counts.get(symbol, 0) + int(count)
        return counts

    # ---------------------------------------------------------------- alerts
    def append_alert(self, record: dict[str, Any]) -> None:
        """Append one alert event (e.g. a disk watermark crossing)."""
        self.alerts_path.parent.mkdir(parents=True, exist_ok=True)
        with self.alerts_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    def alerts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """The most recent alert records, oldest first (up to ``limit``)."""
        if not self.alerts_path.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in self.alerts_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except ValueError:
                continue
        return records[-limit:]

    # ------------------------------------------------------------------ disk
    def family_bytes(self, dataset: Dataset = Dataset.SNAPSHOTS) -> int:
        """On-disk footprint (bytes) of one snapshot family, meta included."""
        base = self.root / dataset.value
        if not base.exists():
            return 0
        total = 0
        for path in base.rglob("*"):
            if path.is_file() and not path.name.startswith(".tmp-"):
                total += path.stat().st_size
        return total

    def disk_report(self, policy: SnapshotPolicy) -> dict[str, Any]:
        """Measure the family footprint against the policy's alert thresholds.

        Returns byte counts, the volume's free percentage, the estimated
        days of headroom for the current universe, and any warning
        reasons.  Pure measurement — callers decide what to do.
        """
        raw_bytes = self.family_bytes(Dataset.SNAPSHOTS)
        archive_bytes = self.family_bytes(Dataset.SNAPSHOTS_1M)
        total_bytes = raw_bytes + archive_bytes
        usage = shutil.disk_usage(self.root)
        free_percent = usage.free / usage.total * 100.0 if usage.total else 100.0
        reasons: list[str] = []
        if free_percent < policy.disk_warn_free_percent:
            reasons.append(
                f"volume free {free_percent:.1f}% is below the "
                f"{policy.disk_warn_free_percent:.1f}% threshold"
            )
        if policy.disk_warn_used_bytes is not None and total_bytes > policy.disk_warn_used_bytes:
            reasons.append(
                f"snapshots family {total_bytes} bytes exceeds the "
                f"{policy.disk_warn_used_bytes}-byte budget"
            )
        # headroom projection: extrapolate from observed raw bytes per
        # collected day when there is any, else from the policy universe
        dates = self.partition_dates(Dataset.SNAPSHOTS)
        per_day: float | None = None
        if dates:
            per_day = raw_bytes / max(1, len(dates))
        elif policy.full_market:
            per_day = float(1_000_000_000)
        days_left: float | None = None
        if per_day and per_day > 0:
            days_left = usage.free / per_day
        return {
            "raw_bytes": raw_bytes,
            "archive_bytes": archive_bytes,
            "total_bytes": total_bytes,
            "volume_free_bytes": usage.free,
            "volume_total_bytes": usage.total,
            "free_percent": round(free_percent, 2),
            "bytes_per_day": int(per_day) if per_day else None,
            "days_headroom": round(days_left, 1) if days_left is not None else None,
            "warn": bool(reasons),
            "reasons": reasons,
        }
