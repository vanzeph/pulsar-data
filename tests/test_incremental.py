"""Watermark-driven incremental update: merge semantics and idempotent re-entry.

The fake adapter serves an extendable day list through the exact
fetch_raw → normalize contract, so these tests exercise the real
pipeline (quality gate included) end to end, offline.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import ConfigurationError
from pulsar_data.incremental import IncrementalRunner
from pulsar_data.lake import DataLake
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts

JAN = [f"2024-01-{day:02d}" for day in (2, 3, 4, 5, 8, 9, 10, 11)]


def bars_frame(days: list[str], symbol: str = "SH600519") -> pd.DataFrame:
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
            "adjust_factor": [1.0] * len(days),
            "quality": ["ok"] * len(days),
        }
    )[list(BAR_COLUMNS)]


class FakeAdapter:
    """Serves whatever day list it currently holds, per requested window."""

    source_id = "fake"

    def __init__(self, days: list[str]) -> None:
        self.days = days
        self.calls: list[tuple[str, date, date, str | None]] = []

    # -- SourceAdapter protocol ------------------------------------------
    def fetch_raw(self, dataset: Dataset, request) -> pd.DataFrame:
        self.calls.append((dataset.value, request.start, request.end, request.symbol))
        in_window = [d for d in self.days if request.start <= date.fromisoformat(d) <= request.end]
        if dataset is Dataset.BARS_1D:
            return pd.DataFrame({"day": in_window}) if request.symbol == "SH600519" else pd.DataFrame()
        if dataset is Dataset.CALENDAR:
            return pd.DataFrame({"day": in_window})
        if dataset is Dataset.INSTRUMENTS:
            return pd.DataFrame({"sym": ["SH600519"]})
        return pd.DataFrame()

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request) -> pd.DataFrame:
        if dataset is Dataset.BARS_1D:
            return bars_frame(raw["day"].tolist()) if not raw.empty else raw
        if dataset is Dataset.CALENDAR:
            return pd.DataFrame({"trade_date": [date.fromisoformat(d) for d in raw["day"]]})
        if dataset is Dataset.INSTRUMENTS:
            return pd.DataFrame(
                [
                    {
                        "symbol": "SH600519",
                        "name": "fake",
                        "exchange": "SSE",
                        "board": "main",
                        "is_st": False,
                        "status": "listed",
                        "list_date": date(2001, 8, 27),
                        "delist_date": None,
                        "shares_outstanding": None,
                    }
                ]
            )
        if dataset is Dataset.CORPORATE_ACTIONS:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "ex_date",
                    "cash_dividend_per_share",
                    "bonus_share_ratio",
                    "rights_issue_ratio",
                    "rights_issue_price",
                    "description",
                ]
            )
        return pd.DataFrame(columns=["symbol", "start_date", "end_date", "reason"])


def _partition(lake: DataLake) -> pd.DataFrame:
    file = lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    return pd.read_parquet(file)


def test_increment_extends_without_truncation(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = FakeAdapter(JAN[:4])
    runner = IncrementalRunner(adapter, lake, initial_start=date(2024, 1, 2))

    first = runner.run(date(2024, 1, 5))
    assert first.bars_rows == 4
    days = [str(pd.Timestamp(ts).date()) for ts in _partition(lake)["ts"]]
    assert days == JAN[:4]

    adapter.days = JAN  # two new trading days arrived upstream
    second = runner.run(date(2024, 1, 9))
    assert second.bars_rows == 3  # overlap day (01-05) + 01-08 + 01-09
    days = [str(pd.Timestamp(ts).date()) for ts in _partition(lake)["ts"]]
    assert days == JAN[:6]  # earlier days survive the incremental write


def test_incremental_reentry_is_idempotent(tmp_path):
    """Re-running the same incremental window leaves the lake identical."""
    lake = DataLake(tmp_path / "lake")
    adapter = FakeAdapter(JAN[:4])
    runner = IncrementalRunner(adapter, lake, initial_start=date(2024, 1, 2))
    runner.run(date(2024, 1, 5))
    adapter.days = JAN
    runner.run(date(2024, 1, 9))

    partition = lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    snapshot = partition.read_bytes()
    frame_before = _partition(lake)

    again = runner.run(date(2024, 1, 9))  # no new upstream data
    assert partition.read_bytes() == snapshot  # byte-identical partition
    pd.testing.assert_frame_equal(_partition(lake), frame_before)
    assert again.failed_symbols == {}
    # the overlap day was re-fetched and re-merged, producing no duplicates
    assert not _partition(lake).duplicated(subset=["symbol", "ts"]).any()

    marks = lake.watermarks()
    bars_marks = marks[(marks["dataset"] == "bars_1d") & (marks["partition"].str.startswith("symbol="))]
    assert (bars_marks["synced_through"] == "2024-01-09").all()


def test_crash_reentry_after_partial_window(tmp_path):
    """A run that crashes mid-symbol resumes cleanly: whatever landed is
    kept, the re-run merges over it with the same end state as one
    clean run."""
    lake = DataLake(tmp_path / "lake")
    clean = DataLake(tmp_path / "clean")
    adapter = FakeAdapter(JAN[:4])
    runner = IncrementalRunner(adapter, lake, initial_start=date(2024, 1, 2))
    # simulate a crashed first pass: only part of the window landed
    lake.merge_write(Dataset.BARS_1D, bars_frame(JAN[:2]), source="fake")
    lake.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": [date.fromisoformat(d) for d in JAN[:2]]}), source="fake")
    runner.run(date(2024, 1, 5))

    reference_adapter = FakeAdapter(JAN[:4])
    IncrementalRunner(reference_adapter, clean, initial_start=date(2024, 1, 2)).run(date(2024, 1, 5))
    pd.testing.assert_frame_equal(
        lake.read(Dataset.BARS_1D).reset_index(drop=True),
        clean.read(Dataset.BARS_1D).reset_index(drop=True),
    )


def test_watermark_drives_window_and_stale_end_skips(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = FakeAdapter(JAN[:4])
    runner = IncrementalRunner(adapter, lake, initial_start=date(2024, 1, 2))
    runner.run(date(2024, 1, 5))

    # re-running the same end re-fetches only the watermark day (overlap
    # healing: a run that landed mid-session can be repaired by re-running)
    report = runner.run(date(2024, 1, 5))
    bars_calls = [c for c in adapter.calls if c[0] == "bars_1d"]
    assert bars_calls[-1][1:3] == (date(2024, 1, 5), date(2024, 1, 5))
    assert report.bars_rows == 1
    assert report.skipped_symbols == []

    # a stale end (before the watermark) costs zero upstream calls
    adapter.calls.clear()
    stale = runner.run(date(2024, 1, 4))
    assert [c for c in adapter.calls if c[0] == "bars_1d"] == []
    assert stale.skipped_symbols == ["SH600519"]


def test_fresh_lake_needs_initial_start_or_symbols(tmp_path):
    lake = DataLake(tmp_path / "lake")
    runner = IncrementalRunner(FakeAdapter(JAN), lake)
    with pytest.raises(ConfigurationError, match="initial_start|symbols"):
        runner.run(date(2024, 1, 9))


def test_fresh_lake_with_initial_start_fetches_universe(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = FakeAdapter(JAN)
    runner = IncrementalRunner(adapter, lake, initial_start=date(2024, 1, 2))
    report = runner.run(date(2024, 1, 9))
    assert report.symbols == ["SH600519"]
    days = [str(pd.Timestamp(ts).date()) for ts in _partition(lake)["ts"]]
    assert days == JAN[:6]


def test_increment_marks_rows_ok_not_backfilled(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = FakeAdapter(JAN[:4])
    runner = IncrementalRunner(adapter, lake, initial_start=date(2024, 1, 2))
    runner.run(date(2024, 1, 5))
    assert set(lake.read(Dataset.BARS_1D)["quality"]) == {"ok"}


def _fake_factory(config=None):
    return FakeAdapter(JAN)


def test_cli_update_end_to_end(tmp_path, capsys):
    from pulsar_data.cli import main
    from pulsar_data.sources import register_adapter

    register_adapter("increment-fake")(_fake_factory)
    lake_dir = tmp_path / "cli-lake"
    assert main(["update", "--source", "increment-fake", "--lake", str(lake_dir),
                 "--end", "2024-01-09", "--initial-start", "2024-01-02"]) == 0
    out = capsys.readouterr().out
    assert "bars_rows=6" in out
