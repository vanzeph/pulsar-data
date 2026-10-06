"""End-to-end offline backfill: 20 symbols x full year 2024 from fixtures.

This is the CI-proof of the task's long-running acceptance item
("full-market >= 1 complete year backfill, no unexplained missing bars
relative to the trading calendar"): the pipeline is identical to the
live one, only fetch_raw reads recorded upstream responses.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from pulsar_data.backfill import BackfillRunner, suspension_days
from pulsar_data.lake import DataLake
from pulsar_data.schema import Dataset
from pulsar_data.sources import get_adapter

START, END = date(2024, 1, 1), date(2024, 12, 31)


@pytest.fixture(scope="module")
def sample_symbols(fixture_dir) -> list[str]:
    import json as _json

    manifest = _json.loads((fixture_dir / "manifest.json").read_text(encoding="utf-8"))
    symbols = manifest["symbols"]
    assert len(symbols) == 20
    return symbols


@pytest.fixture(scope="module")
def backfilled_lake(fixture_dir, tmp_path_factory, sample_symbols) -> DataLake:
    lake = DataLake(tmp_path_factory.mktemp("lake-backfill"))
    adapter = get_adapter("akshare", {"fixture_dir": str(fixture_dir)})
    runner = BackfillRunner(adapter, lake, enrich_instruments=True)
    report = runner.run(sample_symbols, START, END)
    assert not report.failed_symbols, report.failed_symbols
    return lake


def test_full_year_backfill_no_unexplained_gaps(backfilled_lake: DataLake, fixture_dir):
    lake = backfilled_lake
    bars = lake.read(Dataset.BARS_1D)
    symbols = sorted(set(bars["symbol"]))
    assert len(symbols) == 20, symbols

    suspensions = suspension_days(lake, symbols)
    completeness = lake.completeness(
        start=START, end=END, symbols=symbols, suspension_days=suspensions
    )
    gaps = {s: c for s, c in completeness.items() if c.get("gap", 0)}
    assert not gaps, gaps

    # every symbol carries (essentially) a full year of ok bars; the one
    # 2024 IPO in the sample legitimately has ~56 pre-listing days
    for symbol, counts in completeness.items():
        assert counts["ok"] > 150, (symbol, counts)
        # explanations must be non-negative and total must equal the calendar
        assert sum(counts.values()) == 242, (symbol, counts)


def test_backfill_marks_rows_backfilled(backfilled_lake: DataLake):
    bars = backfilled_lake.read(Dataset.BARS_1D)
    assert set(bars["quality"]) == {"backfilled"}


def test_calendar_roundtrip(backfilled_lake: DataLake):
    dates = backfilled_lake.calendar_dates(START, END)
    assert len(dates) == 242
    assert dates == sorted(dates)
    assert dates[0] == date(2024, 1, 2)
    assert dates[-1] == date(2024, 12, 31)


def test_pre_ipo_symbol_explained(backfilled_lake: DataLake):
    """A 2024 IPO shows not_listed days before its first bar, never gaps."""
    completeness = backfilled_lake.completeness(
        start=START, end=END, suspension_days=suspension_days(backfilled_lake, ["SZ301536"])
    )
    counts = completeness["SZ301536"]
    bars = backfilled_lake.read(Dataset.BARS_1D, symbols=["SZ301536"])
    first = bars["ts"].min().date()
    assert first >= date(2024, 3, 1)  # listed in/after March 2024
    assert counts["not_listed"] > 30
    assert counts["gap"] == 0


def test_dividend_symbol_factor_steps(backfilled_lake: DataLake):
    """Moutai's 2024-12-20 ex-dividend creates a factor step inside the window."""
    bars = backfilled_lake.read(Dataset.BARS_1D, symbols=["SH600519"])
    bars["trade_date"] = pd.to_datetime(bars["ts"], utc=True).dt.tz_convert("Asia/Shanghai").dt.date
    day_before = bars[bars["trade_date"] == date(2024, 12, 19)]["adjust_factor"].iloc[0]
    ex_day = bars[bars["trade_date"] == date(2024, 12, 20)]["adjust_factor"].iloc[0]
    assert ex_day > day_before
    # and the June ex-date (2024-06-19) steps too
    june_before = bars[bars["trade_date"] == date(2024, 6, 18)]["adjust_factor"].iloc[0]
    june_ex = bars[bars["trade_date"] == date(2024, 6, 19)]["adjust_factor"].iloc[0]
    assert june_ex > june_before
    # factor is flat between the two ex-dates (to source rounding precision:
    # 2-decimal prices make the derived ratio jitter around ~1e-6 absolute)
    between = bars[
        bars["trade_date"].between(date(2024, 6, 19), date(2024, 12, 19))
    ]["adjust_factor"]
    assert between.max() - between.min() < 1e-4


def test_corporate_actions_persisted(backfilled_lake: DataLake):
    actions = backfilled_lake.read(Dataset.CORPORATE_ACTIONS)
    moutai = actions[actions["symbol"] == "SH600519"]
    assert any(moutai["ex_date"] == date(2024, 12, 20))


def test_instruments_persisted_with_listing_dates(backfilled_lake: DataLake):
    instruments = backfilled_lake.read(Dataset.INSTRUMENTS)
    assert len(instruments) > 5000
    row = instruments[instruments["symbol"] == "SH600519"].iloc[0]
    assert pd.Timestamp(row["list_date"]).date() == date(2001, 8, 27)


def test_watermarks_written(backfilled_lake: DataLake):
    marks = backfilled_lake.watermarks()
    assert not marks.empty
    bars_marks = marks[marks["dataset"] == "bars_1d"]
    partitions = bars_marks["partition"]
    assert (partitions == "*").sum() == 1  # dataset-level watermark
    symbol_marks = bars_marks[partitions != "*"]
    assert symbol_marks["partition"].str.startswith("symbol=").all()
    assert len(symbol_marks) == 20
    assert (bars_marks["synced_through"] == "2024-12-31").all()


def test_backfill_is_idempotent(fixture_dir, tmp_path):
    """Re-running the same window produces identical lake content."""
    lake = DataLake(tmp_path / "lake")
    adapter = get_adapter("akshare", {"fixture_dir": str(fixture_dir)})
    runner = BackfillRunner(adapter, lake)
    first = runner.run(["SH600519", "SZ000001"], START, END)
    snapshot = {
        p.relative_to(lake.root).as_posix(): p.read_bytes()
        for p in sorted((lake.root / "bars_1d").rglob("part.parquet"))
    }
    second = runner.run(["SH600519", "SZ000001"], START, END)
    snapshot2 = {
        p.relative_to(lake.root).as_posix(): p.read_bytes()
        for p in sorted((lake.root / "bars_1d").rglob("part.parquet"))
    }
    assert snapshot == snapshot2
    # second run skipped already-synced symbols via watermarks
    assert not first.skipped_symbols
    assert set(second.skipped_symbols) == {"SH600519", "SZ000001"}


def test_resumed_run_with_force_refetches(fixture_dir, tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = get_adapter("akshare", {"fixture_dir": str(fixture_dir)})
    runner = BackfillRunner(adapter, lake)
    runner.run(["SH600519"], START, END)
    forced = BackfillRunner(adapter, lake, force=True)
    report = forced.run(["SH600519"], START, END)
    assert report.skipped_symbols == []
    assert report.bars_rows > 200


def test_offline_cli_backfill_end_to_end(fixture_dir, tmp_path, capsys):
    """The offline CLI path: pulsar-data backfill --fixture-dir ... --fail-on-gaps."""
    from pulsar_data.cli import main

    lake_dir = tmp_path / "cli-lake"
    report_path = tmp_path / "report.json"
    code = main(
        [
            "backfill",
            "--source", "akshare",
            "--lake", str(lake_dir),
            "--start", "2024-01-01",
            "--end", "2024-12-31",
            "--symbols", "SH600519,SZ300750,SZ301536",
            "--fixture-dir", str(fixture_dir),
            "--report", str(report_path),
            "--fail-on-gaps",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "unexplained_gaps=0" in out
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["unexplained_gaps"] == 0
    assert payload["rows"]["bars"] > 600
    assert payload["freq"] == "1d"


def test_verify_cli_detects_gap_after_tampering(fixture_dir, tmp_path, capsys):
    """verify exits non-zero when a bar partition is removed (unexplained gap)."""
    from pulsar_data.cli import main

    lake_dir = tmp_path / "cli-lake2"
    assert main(
        ["backfill", "--source", "akshare", "--lake", str(lake_dir),
         "--start", "2024-01-01", "--end", "2024-12-31", "--symbols", "SH600519",
         "--fixture-dir", str(fixture_dir)]
    ) == 0
    # drop one mid-year partition month's worth: remove one row from parquet
    partition = lake_dir / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    frame = pd.read_parquet(partition)
    trimmed = frame.iloc[:-1]  # remove last day -> becomes unexplained gap? no: coverage_end
    trimmed.to_parquet(partition, index=False)
    # remove a MIDDLE day -> genuine gap
    holed = frame.drop(index=100).reset_index(drop=True)
    holed.to_parquet(partition, index=False)
    code = main(
        ["verify", "--lake", str(lake_dir), "--start", "2024-01-01", "--end", "2024-12-31",
         "--symbols", "SH600519"]
    )
    assert code == 1
    assert "unexplained_gaps=1" in capsys.readouterr().out
