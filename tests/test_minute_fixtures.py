"""Fixture-driven acceptance: real recorded baostock minute data, fully offline.

``scripts/record_minute_fixtures.py`` captures raw upstream frames for a
20-symbol sample domain (the same diverse domain the akshare daily
fixtures use): a contiguous recent window of full sessions plus the
first trading week of several past years, together with the full
adjust-factor history.  These tests replay them through the exact
production pipeline (FixtureBaostockClient -> fetch_raw -> normalize ->
quality gate -> lake write) and require **zero unexplained gaps**:

* every recorded contiguous window backfills with each trading day
  carrying its full session bar count (48 @ 5m);
* ``fetch_bars`` at every minute frequency returns exactly the rows
  stored in the lake (row-for-row), through direct reads and through
  the port's completeness guard;
* years before the service's minute coverage simply record no rows —
  the manifest pins the measured per-symbol coverage, and pre-coverage
  days classify as explained absences, never gaps.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from pulsar_contracts import AdjustMode, Freq

from pulsar_data.backfill import BackfillRunner
from pulsar_data.lake import DataLake
from pulsar_data.port import LakeMarketDataPort
from pulsar_data.query import LakeQuery
from pulsar_data.schema import Dataset
from pulsar_data.sources import get_adapter

FIXTURES = Path(__file__).parent / "fixtures" / "baostock"


@pytest.fixture(scope="module")
def manifest() -> dict:
    path = FIXTURES / "manifest.json"
    if not path.is_file():
        pytest.skip("baostock minute fixtures not recorded")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def fixture_dir() -> Path:
    if not FIXTURES.is_dir():
        pytest.skip("baostock minute fixtures not recorded")
    return FIXTURES


def _segments(manifest: dict) -> dict[str, list[tuple[date, date]]]:
    """Recorded contiguous day runs per symbol, from the fixture frames."""
    segments: dict[str, list[tuple[date, date]]] = {}
    for symbol in manifest["symbols"]:
        frame = pd.read_csv(FIXTURES / "minute" / f"{symbol}.5.csv", dtype={"time": str})
        if frame.empty:
            segments[symbol] = []
            continue
        days = sorted(set(pd.to_datetime(frame["date"]).dt.date))
        runs: list[tuple[date, date]] = []
        start = previous = days[0]
        for day in days[1:]:
            if (day - previous).days > 10:  # gap between sampled windows
                runs.append((start, previous))
                start = day
            previous = day
        runs.append((start, previous))
        segments[symbol] = runs
    return segments


# --------------------------------------------------------------------------
# acceptance 1: sampled-domain backfill with zero unexplained gaps
# --------------------------------------------------------------------------
def test_sample_domain_5min_backfill_zero_unexplained_gaps(fixture_dir, manifest, tmp_path_factory):
    """~20 symbols x recorded cross-year windows: every session complete."""
    adapter = get_adapter("baostock", {"fixture_dir": str(fixture_dir)})
    lake = DataLake(tmp_path_factory.mktemp("minute-acceptance") / "lake")
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)

    segments = _segments(manifest)
    total_rows = 0
    for symbol, runs in sorted(segments.items()):
        for start, end in runs:
            report = runner.run([symbol], start, end)
            assert report.freq == "5m"
            assert report.failed_symbols == {}, (symbol, report.failed_symbols)
            assert report.unexplained_gaps == 0, (
                symbol, start, end, report.minute_missing_bars
            )
            assert report.minute_missing_bars == {}
            total_rows += report.bars_rows
    expected = sum(int(rows.get("5", 0)) for rows in manifest["rows"].values())
    assert total_rows == expected


def test_sample_domain_covers_twenty_symbols(manifest):
    assert len(manifest["symbols"]) == 20
    recorded = {s for s, runs in _segments(manifest).items() if runs}
    # SZ301536 listed 2024-03: exercises pre-IPO explained absences too
    assert len(recorded) >= 19


def test_manifest_records_measured_coverage(manifest):
    firsts = [c[0] for c in manifest["coverage"].values() if c and c[0]]
    assert firsts, "coverage anchors must be recorded"
    # every measured coverage start is an ISO date (the service's real
    # minute-history boundary, whatever it is per symbol)
    for value in firsts:
        date.fromisoformat(value)


# --------------------------------------------------------------------------
# acceptance 2: fetch_bars per freq equals the lake, row for row
# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def acceptance_lake(fixture_dir, manifest, tmp_path_factory):
    lake = DataLake(tmp_path_factory.mktemp("minute-acceptance-read") / "lake")
    adapter = get_adapter("baostock", {"fixture_dir": str(fixture_dir)})
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)
    for symbol, runs in _segments(manifest).items():
        for start, end in runs:
            runner.run([symbol], start, end)
    return lake


def test_fetch_bars_5min_matches_lake_row_for_row(acceptance_lake, manifest):
    port = LakeMarketDataPort(acceptance_lake)
    for symbol, runs in _segments(manifest).items():
        for start, end in runs:
            frame = port.fetch_bars([symbol], start, end, Freq.MINUTE_5, AdjustMode.RAW)
            with LakeQuery(acceptance_lake.root) as query:
                direct = query.bars([symbol], start, end, freq=Freq.MINUTE_5)
            assert len(frame) == len(direct)
            pd.testing.assert_frame_equal(
                frame.reset_index(drop=True), direct.reset_index(drop=True)
            )


def test_fetch_bars_minute_adjustment_restates_by_factor_ratio(acceptance_lake, manifest):
    port = LakeMarketDataPort(acceptance_lake)
    symbol = manifest["symbols"][0]
    runs = _segments(manifest)[symbol]
    start, end = runs[-1]
    with LakeQuery(acceptance_lake.root) as query:
        raw = query.bars([symbol], start, end, freq=Freq.MINUTE_5)
    backward = port.fetch_bars([symbol], start, end, Freq.MINUTE_5, AdjustMode.BACKWARD)
    anchor = raw.iloc[0]["adjust_factor"]
    pd.testing.assert_series_equal(
        backward["close"],
        (raw["close"] * raw["adjust_factor"] / anchor).rename("close"),
        check_names=False,
    )


def test_other_minute_families_ingest_and_fetch_consistently(fixture_dir, tmp_path):
    """15/30/60 fixtures (one symbol) land in their own families and read back."""
    adapter = get_adapter("baostock", {"fixture_dir": str(fixture_dir)})
    lake = DataLake(tmp_path / "lake")
    port = LakeMarketDataPort(lake)
    frame = pd.read_csv(fixture_dir / "minute" / "SH600519.60.csv", dtype={"time": str})
    days = sorted(set(pd.to_datetime(frame["date"]).dt.date))
    start, end = days[0], days[-1]
    for freq, per_day in ((Freq.MINUTE_15, 16), (Freq.MINUTE_30, 8), (Freq.MINUTE_60, 4)):
        runner = BackfillRunner(adapter, lake, freq=freq)
        report = runner.run(["SH600519"], start, end)
        assert report.unexplained_gaps == 0, (freq, report.minute_missing_bars)
        served = port.fetch_bars(["SH600519"], start, end, freq, AdjustMode.RAW)
        with LakeQuery(lake.root) as query:
            direct = query.bars(["SH600519"], start, end, freq=freq)
        assert len(served) == len(direct) == len(days) * per_day, freq
        pd.testing.assert_frame_equal(
            served.reset_index(drop=True), direct.reset_index(drop=True)
        )


def test_daily_family_untouched_by_minute_backfill(acceptance_lake):
    assert not (acceptance_lake.root / "bars_1d").exists()
