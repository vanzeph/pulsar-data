"""The ``snapshots`` partition family: layout, atomicity, watermarks, gaps, disk."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pulsar_contracts import QuoteLevel

from pulsar_data.lake import DataLake
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.dispatcher import SnapshotDispatcher
from pulsar_data.realtime.events import EventKind, StreamEvent
from pulsar_data.schema import SNAPSHOT_COLUMNS, Dataset
from pulsar_data.snapshots import SnapshotPolicy, SnapshotStore

_SH = ZoneInfo("Asia/Shanghai")


def _event(
    symbol: str = "SH600519",
    *,
    seq: int = 1,
    ts: datetime | None = None,
    kind: EventKind = EventKind.SNAPSHOT,
    price: float = 10.0,
) -> StreamEvent:
    ts = ts or datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH)
    snapshot = None
    if kind is not EventKind.MISSING:
        from pulsar_contracts import Snapshot

        snapshot = Snapshot(
            symbol=symbol,
            ts=ts,
            seq=seq,
            last_price=price,
            volume=1000.0,
            amount=10_000.0,
            bids=(QuoteLevel(price=9.99, volume=100),),
            asks=(QuoteLevel(price=10.01, volume=200),),
        )
    return StreamEvent(kind=kind, symbol=symbol, seq=seq, ts=ts, snapshot=snapshot)


def _rows(*events: StreamEvent, session: str = "sess1") -> list[dict]:
    return [SnapshotStore.event_to_row(e, session_id=session, source_name="sina") for e in events]


class TestWriteAndLayout:
    def test_partition_is_symbol_by_day(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        morning = _event(ts=datetime(2026, 10, 5, 9, 30, tzinfo=_SH))
        afternoon = _event(seq=2, ts=datetime(2026, 10, 5, 14, 0, tzinfo=_SH), price=10.5)
        other_day = _event(seq=3, ts=datetime(2026, 10, 6, 9, 30, tzinfo=_SH), price=11.0)
        result = store.write_events(_rows(morning, afternoon, other_day), session_id="sess1")
        assert result.rows == 3
        target = lake.root / "snapshots" / "symbol=SH600519" / "date=2026-10-05" / "part.parquet"
        assert target.exists()
        assert (lake.root / "snapshots" / "symbol=SH600519" / "date=2026-10-06").is_dir()
        assert sorted(result.partitions) == [
            "symbol=SH600519/date=2026-10-05",
            "symbol=SH600519/date=2026-10-06",
        ]

    def test_columns_match_canonical_schema(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        store.write_events(_rows(_event()), session_id="sess1")
        frame = store.read()
        assert list(frame.columns) == list(SNAPSHOT_COLUMNS)
        row = frame.iloc[0]
        assert row["kind"] == "snapshot"
        assert row["bid1_price"] == pytest.approx(9.99)
        assert row["ask1_volume"] == pytest.approx(200.0)

    def test_marker_rows_carry_nan_book(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        store.write_events(
            _rows(_event(kind=EventKind.MISSING, seq=1)), session_id="sess1"
        )
        row = store.read().iloc[0]
        assert row["kind"] == "missing"
        assert pd.isna(row["last_price"]) and pd.isna(row["bid5_price"])

    def test_merge_write_is_idempotent(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        rows = _rows(_event(seq=1), _event(seq=2, price=10.4))
        store.write_events(rows, session_id="sess1")
        store.write_events(rows, session_id="sess1")  # crash-retry replay
        assert len(store.read()) == 2

    def test_watermark_refreshed_per_partition(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        store.write_events(_rows(_event()), session_id="sess1")
        marks = lake.watermarks()
        snap = marks[marks["dataset"] == "snapshots"]
        assert set(snap["partition"]) == {"*", "symbol=SH600519/date=2026-10-05"}
        assert snap["synced_through"].eq("2026-10-05").all()

    def test_non_positive_price_rejected(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        rows = _rows(_event())
        rows[0]["last_price"] = -1.0  # a corrupted usable row must not land
        with pytest.raises(Exception, match="last_price"):
            store.write_events(rows, session_id="sess1")


class TestStateAndGaps:
    def test_state_roundtrip_and_gap_ledger(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        assert store.load_state() == {"symbols": {}, "updated_at": None}
        store.save_state(
            session_id="s1",
            symbols={"SH600519": {"session": "s1", "seq": 7, "ts": "2026-10-05T09:30:21+08:00"}},
        )
        loaded = store.load_state()
        assert loaded["symbols"]["SH600519"]["seq"] == 7
        store.append_gap(
            {"type": "session_gap", "symbol": "SH600519", "from_ts": "a", "to_ts": "b"}
        )
        gaps = store.session_gaps()
        assert len(gaps) == 1 and gaps[0]["symbol"] == "SH600519"

    def test_missing_marker_summary_counts_recent_days(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        day1 = [
            _event(seq=1, kind=EventKind.MISSING, ts=datetime(2026, 10, 5, 9, 30, tzinfo=_SH)),
            _event(seq=2, ts=datetime(2026, 10, 5, 9, 30, 3, tzinfo=_SH)),
        ]
        day2 = [_event(seq=1, kind=EventKind.MISSING, ts=datetime(2026, 10, 7, 9, 30, tzinfo=_SH))]
        store.write_events(_rows(*day1), session_id="s1")
        store.write_events(_rows(*day2, session="s2"), session_id="s2")
        summary = store.missing_marker_summary(days=7)
        assert summary == {"SH600519": 2}
        assert store.partition_dates() == [date(2026, 10, 5), date(2026, 10, 7)]
        assert store.partition_count() == 2


class TestDiskReport:
    def test_disk_report_measures_family_and_flags_thresholds(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        store.write_events(_rows(_event()), session_id="s1")
        policy = SnapshotPolicy(
            symbols=["SH600519"], disk_warn_used_bytes=1
        )  # any byte trips the budget
        report = store.disk_report(policy)
        assert report["raw_bytes"] > 0
        assert report["warn"] is True
        assert any("budget" in reason for reason in report["reasons"])

    def test_disk_report_quiet_under_thresholds(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        store.write_events(_rows(_event()), session_id="s1")
        policy = SnapshotPolicy(symbols=["SH600519"], disk_warn_used_bytes=10**12)
        report = store.disk_report(policy)
        assert report["warn"] is False
        assert report["days_headroom"] is not None

    def test_alert_ledger_roundtrip(self, tmp_path):
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        store.append_alert({"type": "disk_warn", "at": "2026-10-05T15:00:00+08:00"})
        assert store.alerts()[0]["type"] == "disk_warn"


class TestDispatcherIntegration:
    def test_rows_built_from_dispatcher_events_verbatim(self, tmp_path):
        """The store consumes dispatcher output unchanged — no re-classification."""

        class OneShot:
            name = "fake"

            def poll(self, symbols):
                ts = datetime(2026, 10, 5, 9, 30, 0, tzinfo=_SH)
                return {
                    "SH600519": RawSnapshot(
                        symbol="SH600519",
                        ts=ts,
                        last_price=10.0,
                        volume=100.0,
                        amount=1000.0,
                    )
                }

        dispatcher = SnapshotDispatcher(OneShot(), auto_start=False)
        dispatcher.subscribe_events(["SH600519"], lambda event: None)
        events = dispatcher.ingest(OneShot().poll(["SH600519"]))
        assert events[0].kind is EventKind.SNAPSHOT
        rows = _rows(*events)
        assert rows[0]["seq"] == 1 and rows[0]["kind"] == "snapshot"
