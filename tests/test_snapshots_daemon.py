"""End-to-end daemon tests over a simulated snapshot stream (fully offline).

Acceptance scenario of the design: 守护写入 → 重启续采 → 水位补齐 → 缺口标记.
A scripted :class:`SnapshotSource` feeds the *unmodified* D5 dispatcher
(manual mode — no threads, no network), the daemon consumes its events,
and every assertion runs against what actually landed in the lake.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.dispatcher import SnapshotDispatcher
from pulsar_data.realtime.events import EventKind
from pulsar_data.schema import Dataset
from pulsar_data.snapshots import SnapshotCollectorDaemon, SnapshotPolicy

_SH = ZoneInfo("Asia/Shanghai")


class ScriptedSource:
    """A deterministic poll source: one scripted batch per poll call."""

    name = "scripted"
    last_source = "scripted"

    def __init__(self, batches: list[dict[str, RawSnapshot]]) -> None:
        self.batches = batches
        self.polls = 0

    def poll(self, symbols):
        batch = self.batches[min(self.polls, len(self.batches) - 1)]
        self.polls += 1
        return {s: q for s, q in batch.items() if s in set(symbols)}


def _quote(ts: datetime, price: float = 10.0, volume: float = 1000.0) -> RawSnapshot:
    return RawSnapshot(
        symbol="SH600519", ts=ts, last_price=price, volume=volume, amount=volume * price
    )


def _daemon(lake, policy, source, *, clock=None):
    dispatcher = SnapshotDispatcher(source, poll_interval=policy.poll_interval_s, auto_start=False)
    return SnapshotCollectorDaemon(
        lake, policy, dispatcher=dispatcher, manual=True, clock=clock or (lambda: datetime.now(tz=_SH))
    )


SESSION_ONE = [
    {"SH600519": _quote(datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH), 10.0, 100)},
    {"SH600519": _quote(datetime(2026, 10, 5, 9, 30, 3, tzinfo=_SH), 10.2, 200)},
    {},  # a missing cycle (suspended upstream / dropped)
    {
        # a late quote: older than what was already delivered
        "SH600519": _quote(datetime(2026, 10, 5, 9, 30, 1, tzinfo=_SH), 10.1, 150)
    },
]
SESSION_ONE_TIMES = [0, 3, 6, 9]


class TestWriteRestartWatermarkGaps:
    def test_end_to_end_write_restart_catchup_gapmark(self, tmp_path):
        from pulsar_data.lake import DataLake

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"], poll_interval_s=3)

        # -- session one: four cycles, one missing, one late
        daemon = _daemon(lake, policy, ScriptedSource(SESSION_ONE))
        daemon.start()
        for i, seconds in enumerate(SESSION_ONE_TIMES):
            daemon.dispatcher.ingest(
                SESSION_ONE[min(i, len(SESSION_ONE) - 1)],
                cycle_ts=datetime(2026, 10, 5, 9, 30, seconds, tzinfo=_SH),
            )
        daemon.flush()
        daemon.stop()

        frame = lake.read(Dataset.SNAPSHOTS)
        assert len(frame) == 4  # every cycle persisted — nothing dropped silently
        by_cycle = frame.sort_values("seq")  # partitions sort by ts; seq is cycle order
        assert list(by_cycle["kind"]) == ["snapshot", "snapshot", "missing", "late"]
        assert list(by_cycle["seq"]) == [1, 2, 3, 4]  # no seq gaps: nothing skipped

        state = daemon.store.load_state()
        assert state["symbols"]["SH600519"]["seq"] == 4
        # last-contact watermark: the missing-marker cycle (09:30:06) is the
        # newest persisted row ts; the late 09:30:01 quote never rewinds it
        assert state["symbols"]["SH600519"]["ts"].startswith("2026-10-05T09:30:06")

        marks = lake.watermarks()
        snap_marks = marks[marks["dataset"] == "snapshots"]
        assert "symbol=SH600519/date=2026-10-05" in set(snap_marks["partition"])

        # -- restart after ~30 minutes of downtime (same trade date)
        resume = datetime(2026, 10, 5, 10, 0, 0, tzinfo=_SH)
        session_two = [{"SH600519": _quote(resume, 11.0, 500)}]
        daemon2 = _daemon(lake, policy, ScriptedSource(session_two), clock=lambda: resume)
        daemon2.start()  # restart: state reload → gap recorded, watermark resumes
        daemon2.dispatcher.ingest(session_two[0], cycle_ts=resume)
        daemon2.flush()
        daemon2.stop()

        gaps = daemon2.store.session_gaps()
        assert len(gaps) == 1
        gap = gaps[0]
        assert gap["type"] == "session_gap"
        assert gap["from_ts"].startswith("2026-10-05T09:30:06")
        assert gap["to_ts"].startswith("2026-10-05T10:00:00")
        assert gap["duration_s"] == pytest.approx(1794.0)

        frame2 = lake.read(Dataset.SNAPSHOTS)
        assert len(frame2) == 5  # continuation in the same day partition
        resumed = frame2.iloc[-1]
        assert resumed["kind"] == "snapshot"
        assert resumed["session"] == daemon2.session_id
        assert resumed["seq"] == 1  # fresh seq space per session, session column separates

        # watermark caught up with the resumed session
        marks2 = lake.watermarks()
        rows_before = len(snap_marks)
        snap_marks2 = marks2[marks2["dataset"] == "snapshots"]
        assert len(snap_marks2) == rows_before  # same partition, refreshed in place
        state2 = daemon2.store.load_state()
        assert state2["symbols"]["SH600519"]["ts"].startswith("2026-10-05T10:00:00")

        # gap report sees both forms: session gaps and in-stream missing markers
        summary = daemon2.store.missing_marker_summary(days=7)
        assert summary == {"SH600519": 1}

    def test_brief_blip_below_threshold_records_no_session_gap(self, tmp_path):
        from pulsar_data.lake import DataLake

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"], reconnect_gap_threshold_s=60)
        first = _daemon(lake, policy, ScriptedSource(SESSION_ONE))
        first.start()
        first.dispatcher.ingest(SESSION_ONE[0], cycle_ts=datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH))
        first.flush()
        first.stop()

        # restart 10s later: below the threshold — not a ledgered gap
        resume = datetime(2026, 10, 5, 9, 30, 10, tzinfo=_SH)
        second = _daemon(
            lake,
            policy,
            ScriptedSource([{"SH600519": _quote(resume, 10.1, 120)}]),
            clock=lambda: resume,
        )
        second.start()
        assert second.store.session_gaps() == []

    def test_new_trade_date_never_ledgers_a_session_gap(self, tmp_path):
        from pulsar_data.lake import DataLake

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"])
        first = _daemon(lake, policy, ScriptedSource(SESSION_ONE))
        first.start()
        first.dispatcher.ingest(SESSION_ONE[0], cycle_ts=datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH))
        first.flush()
        first.stop()

        next_day = datetime(2026, 10, 6, 9, 30, 0, tzinfo=_SH)
        second = _daemon(
            lake,
            policy,
            ScriptedSource([{"SH600519": _quote(next_day, 10.5, 100)}]),
            clock=lambda: next_day,
        )
        second.start()
        assert second.store.session_gaps() == []


class TestSamplingCap:
    def test_min_sample_interval_thins_extra_quotes_but_keeps_markers(self, tmp_path):
        from pulsar_data.lake import DataLake

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"], min_sample_interval_s=10)
        batches = [
            {"SH600519": _quote(datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH), 10.0, 100)},
            {"SH600519": _quote(datetime(2026, 10, 5, 9, 30, 3, tzinfo=_SH), 10.1, 200)},
            {},  # missing within the thinned window
            {"SH600519": _quote(datetime(2026, 10, 5, 9, 30, 12, tzinfo=_SH), 10.3, 400)},
        ]
        daemon = _daemon(lake, policy, ScriptedSource(batches))
        daemon.start()
        for i, seconds in enumerate((0, 3, 6, 12)):
            daemon.dispatcher.ingest(
                batches[i], cycle_ts=datetime(2026, 10, 5, 9, 30, seconds, tzinfo=_SH)
            )
        daemon.flush()
        daemon.stop()

        frame = lake.read(Dataset.SNAPSHOTS)
        assert list(frame["kind"]) == ["snapshot", "thinned", "missing", "snapshot"]
        assert list(frame["seq"]) == [1, 2, 3, 4]
        thinned = frame.iloc[1]
        assert thinned["last_price"] != thinned["last_price"]  # NaN: data dropped by policy
        assert daemon.stats["thinned"] == 1


class TestBufferResilience:
    def test_failed_flush_retains_buffer(self, tmp_path):
        from pulsar_data.lake import DataLake

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"])
        daemon = _daemon(lake, policy, ScriptedSource(SESSION_ONE))
        daemon.start()
        daemon.dispatcher.ingest(SESSION_ONE[0], cycle_ts=datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH))
        assert daemon.pending_rows() == 1

        def _boom(rows, *, session_id):
            raise RuntimeError("disk full")

        original = daemon.store.write_events
        daemon.store.write_events = _boom  # type: ignore[method-assign]
        daemon._safe_flush()  # noqa: SLF001 - exercising the daemon's own guard
        daemon.store.write_events = original  # type: ignore[method-assign]

        assert daemon.stats["flush_errors"] == 1
        assert daemon.pending_rows() == 1  # the row survived for the retry
        daemon.flush()
        daemon.stop()
        assert len(lake.read(Dataset.SNAPSHOTS)) == 1


class TestDiskAlerting:
    def test_disk_crossing_writes_alert_and_recovers(self, tmp_path):
        from pulsar_data.lake import DataLake

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"], disk_warn_used_bytes=1)
        daemon = _daemon(lake, policy, ScriptedSource(SESSION_ONE))
        daemon.start()
        daemon.dispatcher.ingest(SESSION_ONE[0], cycle_ts=datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH))
        daemon.flush()
        alerts = daemon.store.alerts()
        assert alerts and alerts[-1]["type"] == "disk_warn"
        daemon.stop()


class TestStatus:
    def test_status_reports_policy_state_gaps_disk(self, tmp_path):
        from pulsar_data.lake import DataLake
        from pulsar_data.snapshots import SnapshotStore, collect_status

        lake = DataLake(tmp_path / "lake")
        policy = SnapshotPolicy(symbols=["SH600519"])
        daemon = _daemon(lake, policy, ScriptedSource(SESSION_ONE))
        daemon.start()
        daemon.dispatcher.ingest(SESSION_ONE[0], cycle_ts=datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH))
        daemon.flush()
        daemon.stop()

        payload = collect_status(SnapshotStore(lake), policy)
        assert payload["raw_partitions"] == 1
        assert payload["policy"]["symbols"] == ["SH600519"]
        assert payload["missing_markers_recent"] == {}
        assert payload["disk"]["raw_bytes"] > 0
