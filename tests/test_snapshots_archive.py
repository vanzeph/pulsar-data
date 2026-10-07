"""Retention and downsample archiving: 3s raw → 1m aggregate, then reclaim."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pulsar_contracts import Snapshot
from pulsar_data.lake import DataLake
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.events import EventKind, StreamEvent
from pulsar_data.schema import SNAPSHOT_1M_COLUMNS, Dataset
from pulsar_data.snapshots import (
    SnapshotPolicy,
    SnapshotStore,
    aggregate_snapshots,
    archive_expired,
)

_SH = ZoneInfo("Asia/Shanghai")


def _rows(
    symbol: str,
    day: str,
    *,
    cadence_s: int = 3,
    cycles: int = 60,
    missing_every: int = 0,
) -> list[dict]:
    """Rows of one scripted raw session: rising cumulative volume, drifting price.

    ``missing_every`` turns every n-th cycle into a MISSING marker (no quote).
    """
    start = datetime.fromisoformat(f"{day}T09:30:00+08:00")
    rows: list[dict] = []
    for i in range(cycles):
        ts = start + timedelta(seconds=i * cadence_s)
        if missing_every and i % missing_every == 0:
            event = StreamEvent(kind=EventKind.MISSING, symbol=symbol, seq=i + 1, ts=ts)
        else:
            volume = i * 50.0
            event = StreamEvent(
                kind=EventKind.SNAPSHOT,
                symbol=symbol,
                seq=i + 1,
                ts=ts,
                snapshot=Snapshot(
                    symbol=symbol,
                    ts=ts,
                    seq=i + 1,
                    last_price=10.0 + (i % 10) * 0.1,
                    volume=volume,
                    amount=volume * 10.0,
                ),
            )
        rows.append(
            SnapshotStore.event_to_row(event, session_id="sess-archive", source_name="scripted")
        )
    return rows


class TestAggregate:
    def test_ohlc_volume_span_and_marker_counts(self):
        rows = _rows("SH600519", "2026-10-05", cadence_s=3, cycles=40, missing_every=4)
        agg = aggregate_snapshots(pd.DataFrame(rows), interval_s=60)
        # cycles 0..19 (3s cadence) land in the 09:30 minute bucket
        minute = agg[agg["ts"] == pd.Timestamp("2026-10-05 09:30:00+08:00")]
        assert len(minute) == 1
        row = minute.iloc[0]
        first_minute = [r for r in rows if r["ts"] < pd.Timestamp("2026-10-05 09:31:00+08:00")]
        usable = [r["last_price"] for r in first_minute if r["kind"] == "snapshot"]
        assert row["open"] == pytest.approx(usable[0])
        assert row["close"] == pytest.approx(usable[-1])
        assert row["high"] == pytest.approx(max(usable))
        assert row["low"] == pytest.approx(min(usable))
        # cumulative volume span over the bucket: max − min of the day totals
        volumes = [r["volume"] for r in first_minute if r["kind"] == "snapshot"]
        assert row["volume"] == pytest.approx(max(volumes) - min(volumes))
        amounts = [r["amount"] for r in first_minute if r["kind"] == "snapshot"]
        assert row["amount"] == pytest.approx(max(amounts) - min(amounts))
        assert row["samples"] == len(usable)  # 20 cycles − every 4th missing
        assert row["missing"] == 5
        assert row["thinned"] == 0
        assert row["quality"] == "ok"
        # 40 cycles × 3s span 0..117s → two left-closed minute buckets
        assert len(agg) == 2

    def test_marker_only_bucket_stays_visible(self):
        rows = _rows("SH600519", "2026-10-05", cadence_s=3, cycles=4, missing_every=1)
        agg = aggregate_snapshots(pd.DataFrame(rows), interval_s=60)
        assert len(agg) == 1
        row = agg.iloc[0]
        assert row["samples"] == 0 and row["missing"] == 4
        assert pd.isna(row["open"]) and pd.isna(row["close"])
        assert row["volume"] == 0.0

    def test_columns_match_canonical_schema(self):
        rows = _rows("SH600519", "2026-10-05", cycles=2)
        agg = aggregate_snapshots(pd.DataFrame(rows), interval_s=60)
        assert list(agg.columns) == list(SNAPSHOT_1M_COLUMNS)


class TestArchiveExpired:
    def _lake_with(self, tmp_path, days: dict[str, int]) -> DataLake:
        lake = DataLake(tmp_path / "lake")
        store = SnapshotStore(lake)
        for day, cycles in days.items():
            store.write_events(_rows("SH600519", day, cycles=cycles), session_id="sess-archive")
        return lake

    def test_expired_partitions_archived_and_reclaimed(self, tmp_path):
        lake = self._lake_with(tmp_path, {"2026-09-01": 60, "2026-10-05": 60})
        policy = SnapshotPolicy(symbols=["SH600519"], raw_retention_days=14)
        report = archive_expired(lake, policy, today=date(2026, 10, 5))

        assert report.raw_partitions_archived == 1
        assert report.raw_bytes_reclaimed > 0
        assert not (lake.root / "snapshots" / "symbol=SH600519" / "date=2026-09-01").exists()
        assert (lake.root / "snapshots" / "symbol=SH600519" / "date=2026-10-05").exists()
        archived = lake.read(Dataset.SNAPSHOTS_1M)
        assert len(archived) == 3  # 60 cycles × 3s = 3 left-closed minute buckets
        assert archived["samples"].sum() == 60
        assert (archived["missing"] == 0).all()
        marks = lake.watermarks()
        archive_marks = marks[marks["dataset"] == "snapshots_1m"]
        assert "symbol=SH600519/date=2026-09-01" in set(archive_marks["partition"])

    def test_rerun_is_idempotent(self, tmp_path):
        lake = self._lake_with(tmp_path, {"2026-09-01": 60})
        policy = SnapshotPolicy(symbols=["SH600519"], raw_retention_days=14)
        today = date(2026, 10, 5)
        first = archive_expired(lake, policy, today=today)
        second = archive_expired(lake, policy, today=today)
        assert first.raw_partitions_archived == 1
        assert second.raw_partitions_archived == 0  # raw already reclaimed
        assert second.skipped == []  # the directory itself is gone
        assert len(lake.read(Dataset.SNAPSHOTS_1M)) == 3  # archive untouched

    def test_partition_without_data_file_is_skipped(self, tmp_path):
        lake = self._lake_with(tmp_path, {"2026-09-01": 10})
        stale = lake.root / "snapshots" / "symbol=SH600519" / "date=2026-08-01"
        stale.mkdir(parents=True)
        policy = SnapshotPolicy(symbols=["SH600519"], raw_retention_days=14)
        report = archive_expired(lake, policy, today=date(2026, 10, 5))
        assert report.skipped == ["snapshots/symbol=SH600519/date=2026-08-01"]

    def test_live_window_is_never_archived(self, tmp_path):
        lake = self._lake_with(tmp_path, {"2026-09-22": 60})  # exactly the cutoff day
        policy = SnapshotPolicy(symbols=["SH600519"], raw_retention_days=14)
        report = archive_expired(lake, policy, today=date(2026, 10, 6))
        assert report.raw_partitions_archived == 0
        assert (lake.root / "snapshots" / "symbol=SH600519" / "date=2026-09-22").exists()

    def test_archive_retention_prunes_old_aggregates(self, tmp_path):
        lake = self._lake_with(tmp_path, {"2026-07-01": 60, "2026-09-01": 60})
        # July is 96d old (past 60d archive retention); September only 34d (kept)
        policy = SnapshotPolicy(
            symbols=["SH600519"], raw_retention_days=14, archive_retention_days=60
        )
        today = date(2026, 10, 5)
        report = archive_expired(lake, policy, today=today)
        assert report.raw_partitions_archived == 2
        assert report.archive_partitions_pruned == 1
        assert not (lake.root / "snapshots_1m" / "symbol=SH600519" / "date=2026-07-01").exists()
        assert (lake.root / "snapshots_1m" / "symbol=SH600519" / "date=2026-09-01").exists()

    def test_failed_aggregate_keeps_raw_partition(self, tmp_path, monkeypatch):
        lake = self._lake_with(tmp_path, {"2026-09-01": 10})
        policy = SnapshotPolicy(symbols=["SH600519"], raw_retention_days=14)

        def _boom(frame, *, interval_s):
            raise RuntimeError("aggregation exploded")

        monkeypatch.setattr("pulsar_data.snapshots.archive.aggregate_snapshots", _boom)
        with pytest.raises(Exception, match="archiving failed"):
            archive_expired(lake, policy, today=date(2026, 10, 5))
        assert (lake.root / "snapshots" / "symbol=SH600519" / "date=2026-09-01" / "part.parquet").exists()
        assert not list((lake.root / "snapshots_1m").glob("symbol=*/date=*"))
