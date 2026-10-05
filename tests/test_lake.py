"""Lake behavior: layout, atomic replacement, watermarks, completeness."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.lake import DataLake
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts


def _bars(days: list[str], symbol: str = "SH600519", factor: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts": [daily_ts(day) for day in days],
            "open": [10.0] * len(days),
            "high": [11.0] * len(days),
            "low": [9.5] * len(days),
            "close": [10.5] * len(days),
            "volume": [100.0] * len(days),
            "amount": [1050.0] * len(days),
            "adjust_factor": [factor] * len(days),
            "quality": ["ok"] * len(days),
        }
    )[list(BAR_COLUMNS)]


def test_partition_layout_matches_design(lake: DataLake):
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02"]), source="akshare")
    target = lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    assert target.exists()
    assert target.is_file()


def test_write_is_atomic_and_replaces(lake: DataLake):
    first = _bars(["2024-01-02", "2024-01-03"])
    lake.write(Dataset.BARS_1D, first, source="akshare")
    partition = lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"

    second = _bars(["2024-01-02", "2024-01-03", "2024-01-04"], factor=2.0)
    lake.write(Dataset.BARS_1D, second, source="akshare")

    # no tmp leftovers, single file, content replaced
    directory = partition.parent
    files = sorted(p.name for p in directory.iterdir())
    assert files == ["part.parquet"]
    stored = pd.read_parquet(partition)
    assert len(stored) == 3
    assert set(stored["adjust_factor"]) == {2.0}


def test_idempotent_rewrite_yields_identical_content(lake: DataLake):
    frame = _bars(["2024-01-02", "2024-01-03"])
    lake.write(Dataset.BARS_1D, frame, source="akshare")
    first_bytes = (
        lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    ).read_bytes()
    lake.write(Dataset.BARS_1D, frame, source="akshare")
    second_bytes = (
        lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    ).read_bytes()
    assert first_bytes == second_bytes


def test_year_partitioning(lake: DataLake):
    frame = pd.concat(
        [_bars(["2023-12-29"]), _bars(["2024-01-02"])], ignore_index=True
    )
    written = lake.write(Dataset.BARS_1D, frame, source="akshare")
    assert "symbol=SH600519/year=2023" in written
    assert "symbol=SH600519/year=2024" in written


def test_read_filters_symbols(lake: DataLake):
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02"], symbol="SH600519"), source="akshare")
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02"], symbol="SZ000001"), source="akshare")
    only = lake.read(Dataset.BARS_1D, symbols=["SZ000001"])
    assert set(only["symbol"]) == {"SZ000001"}


def test_watermarks_upsert(lake: DataLake):
    lake.update_watermark(
        source="akshare", dataset="bars_1d", partitions=["symbol=SH600519/year=2024"],
        rows=5, synced_through=date(2024, 12, 31),
    )
    lake.update_watermark(
        source="akshare", dataset="bars_1d", partitions=["symbol=SH600519/year=2024"],
        rows=6, synced_through=date(2025, 12, 31),
    )
    marks = lake.watermarks()
    assert len(marks) == 2  # partition row + dataset "*" row
    partition_row = marks[marks["partition"] == "symbol=SH600519/year=2024"].iloc[0]
    assert partition_row["rows"] == 6
    assert partition_row["synced_through"] == "2025-12-31"


def test_completeness_categories(lake: DataLake):
    calendar = pd.DataFrame({"trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]})
    lake.write(Dataset.CALENDAR, calendar, source="akshare")
    # symbol trades only the middle day; day3 is suspended
    lake.write(Dataset.BARS_1D, _bars(["2024-01-03"]), source="akshare")
    report = lake.completeness(
        start=date(2024, 1, 2),
        end=date(2024, 1, 4),
        suspension_days={"SH600519": {date(2024, 1, 4)}},
    )
    counts = report["SH600519"]
    assert counts == {
        "ok": 1,
        "not_listed": 1,  # 01-02 before first bar
        "coverage_end": 0,
        "suspended": 1,
        "gap": 0,
    }


def test_completeness_flags_unexplained_gap(lake: DataLake):
    calendar = pd.DataFrame({"trade_date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]})
    lake.write(Dataset.CALENDAR, calendar, source="akshare")
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02", "2024-01-04"]), source="akshare")
    report = lake.completeness(start=date(2024, 1, 2), end=date(2024, 1, 4))
    assert report["SH600519"]["gap"] == 1


def test_completeness_needs_calendar(lake: DataLake):
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02"]), source="akshare")
    from pulsar_data.errors import LakeError

    with pytest.raises(LakeError, match="calendar"):
        lake.completeness(start=date(2024, 1, 1), end=date(2024, 1, 31))
