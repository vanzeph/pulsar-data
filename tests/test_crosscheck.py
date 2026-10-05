"""Dual-source sampled cross-validation (收盘价 / 成交量一致性).

Two acceptance cases anchor this file: consistent sources pass cleanly,
and over-threshold disagreement produces a quality-event report.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from pulsar_data.crosscheck import CrossValidator, _relative_diff, _sample_indices
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts
from pulsar_data.sources.base import FetchRequest

DAYS = [f"2024-01-{day:02d}" for day in (2, 3, 4, 5, 8, 9)]


class BarsAdapter:
    """Serves canonical-shaped daily bars from recorded literals."""

    def __init__(
        self,
        source_id: str,
        closes: dict[str, dict[str, float]],
        volumes: dict[str, dict[str, float]] | None = None,
    ):
        self.source_id = source_id
        self.closes = closes
        self.volumes = volumes or {
            symbol: {day: 100_000.0 for day in table} for symbol, table in closes.items()
        }
        self.calls: list[str | None] = []

    def _table(self, request: FetchRequest) -> dict[str, float]:
        return self.closes.get(request.symbol or "", {})

    def _volume_table(self, request: FetchRequest) -> dict[str, float]:
        return self.volumes.get(request.symbol or "", {})

    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        self.calls.append(request.symbol)
        return pd.DataFrame({"day": list(self._table(request))})

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        days = list(raw["day"])
        closes = self._table(request)
        volumes = self._volume_table(request)
        return pd.DataFrame(
            {
                "symbol": request.symbol,
                "ts": [daily_ts(day) for day in days],
                "open": [closes[day] * 0.99 for day in days],
                "high": [closes[day] * 1.02 for day in days],
                "low": [closes[day] * 0.98 for day in days],
                "close": [closes[day] for day in days],
                "volume": [volumes[day] for day in days],
                "amount": [closes[day] * volumes[day] for day in days],
                "adjust_factor": 1.0,
                "quality": "ok",
            }
        )[list(BAR_COLUMNS)]


def _closes(values: list[float]) -> dict[str, float]:
    return dict(zip(DAYS, values))


def _tables(single: dict[str, float]) -> dict[str, dict[str, float]]:
    return {"SH600519": single}


# --------------------------------------------------------------------------
# Consistent sources pass
# --------------------------------------------------------------------------
def test_consistent_sources_pass_with_no_events():
    primary = BarsAdapter("akshare", _tables(_closes([10.0] * 6)))
    secondary = BarsAdapter("baostock", _tables(_closes([10.0] * 6)))
    report = CrossValidator(primary, secondary).check(
        ["SH600519"], date(2024, 1, 2), date(2024, 1, 9)
    )
    assert report.passed is True
    assert report.events == []
    stats = report.per_symbol["SH600519"]
    assert stats.overlap_rows == 6
    assert stats.compared_rows == 6
    assert stats.close_mismatches == 0
    assert stats.volume_mismatches == 0


def test_within_tolerance_differences_pass():
    primary = BarsAdapter("akshare", _tables(_closes([10.0, 10.0, 10.0, 10.0, 10.0, 10.0])))
    secondary = BarsAdapter("baostock", _tables(_closes([10.0, 10.001, 9.9995, 10.0, 10.0, 10.0])))
    report = CrossValidator(primary, secondary, close_tolerance=0.001).check(
        ["SH600519"], date(2024, 1, 2), date(2024, 1, 9)
    )
    assert report.passed is True  # 0.01% wobble is noise, not disagreement


# --------------------------------------------------------------------------
# Over-threshold disagreement is reported (差异检出)
# --------------------------------------------------------------------------
def test_close_mismatch_over_threshold_reports_quality_event(tmp_path):
    primary = BarsAdapter("akshare", _tables(_closes([10.0, 10.0, 10.5, 10.0, 10.0, 10.0])))
    secondary = BarsAdapter("baostock", _tables(_closes([10.0, 10.0, 10.0, 10.0, 10.0, 10.0])))
    validator = CrossValidator(primary, secondary, close_tolerance=0.001)
    report = validator.check(["SH600519"], date(2024, 1, 2), date(2024, 1, 9))

    assert report.passed is False
    assert report.total_mismatches == 1
    event = report.events[0]
    assert event.type == "cross_source_mismatch"
    assert event.field == "close"
    assert event.symbol == "SH600519"
    assert event.trade_date == "2024-01-04"
    assert event.value_primary == pytest.approx(10.5)
    assert event.value_secondary == pytest.approx(10.0)
    assert event.relative_diff == pytest.approx(0.5 / 10.5, rel=1e-6)
    assert event.tolerance == 0.001
    stats = report.per_symbol["SH600519"]
    assert stats.close_mismatches == 1
    assert stats.max_close_relative_diff == pytest.approx(0.5 / 10.5, rel=1e-6)

    artifact = report.to_json(tmp_path / "crosscheck-report.json")
    data = json.loads(artifact.read_text(encoding="utf-8"))
    assert data["passed"] is False
    assert data["total_mismatches"] == 1
    assert data["quality_events"][0]["field"] == "close"
    assert data["source_primary"] == "akshare"
    assert data["source_secondary"] == "baostock"
    assert data["window"] == ["2024-01-02", "2024-01-09"]


def test_volume_mismatch_over_threshold_reports_quality_event():
    primary = BarsAdapter(
        "akshare", _tables(_closes([10.0] * 6)), volumes={"SH600519": _closes([100_000.0] * 6)}
    )
    secondary = BarsAdapter(
        "baostock",
        _tables(_closes([10.0] * 6)),
        volumes={"SH600519": _closes([100_000.0, 100_000.0, 130_000.0, 100_000.0, 100_000.0, 100_000.0])},
    )
    report = CrossValidator(primary, secondary, volume_tolerance=0.05).check(
        ["SH600519"], date(2024, 1, 2), date(2024, 1, 9)
    )
    assert report.passed is False
    volume_events = [event for event in report.events if event.field == "volume"]
    assert len(volume_events) == 1
    assert volume_events[0].trade_date == "2024-01-04"
    assert volume_events[0].relative_diff == pytest.approx(0.3)
    assert report.per_symbol["SH600519"].volume_mismatches == 1


def test_multiple_symbols_reported_independently():
    primary = BarsAdapter(
        "akshare",
        {"SH600519": _closes([10.0] * 6), "SZ000001": _closes([5.0] * 6)},
    )
    secondary = BarsAdapter(
        "baostock",
        {"SH600519": _closes([10.0] * 6), "SZ000001": _closes([5.5] * 6)},
    )
    report = CrossValidator(primary, secondary).check(
        ["SH600519", "SZ000001"], date(2024, 1, 2), date(2024, 1, 9)
    )
    assert report.per_symbol["SH600519"].close_mismatches == 0
    assert report.per_symbol["SZ000001"].close_mismatches == 6
    assert all(event.symbol == "SZ000001" for event in report.events)
    assert report.symbols == ["SH600519", "SZ000001"]


# --------------------------------------------------------------------------
# Overlap semantics and sampling
# --------------------------------------------------------------------------
def test_only_overlap_is_compared_partial_coverage():
    # primary disagrees wildly on days the secondary does not cover at all
    primary = BarsAdapter("akshare", _tables(_closes([99.0, 99.0, 10.0, 10.0, 10.0, 10.0])))
    secondary = BarsAdapter(
        "baostock",
        _tables({day: 10.0 for day in DAYS[2:]}),  # covers only the last 4 days
    )
    report = CrossValidator(primary, secondary).check(
        ["SH600519"], date(2024, 1, 2), date(2024, 1, 9)
    )
    stats = report.per_symbol["SH600519"]
    assert stats.overlap_rows == 4
    assert stats.primary_only_rows == 2
    assert stats.secondary_only_rows == 0
    # the 99.0 rows live outside the overlap and never count as mismatches
    assert report.passed is True


def test_empty_side_reports_coverage_without_events():
    primary = BarsAdapter("akshare", _tables(_closes([10.0] * 6)))
    secondary = BarsAdapter("baostock", {"SH600519": {}})
    report = CrossValidator(primary, secondary).check(
        ["SH600519"], date(2024, 1, 2), date(2024, 1, 9)
    )
    stats = report.per_symbol["SH600519"]
    assert stats.compared_rows == 0
    assert stats.primary_only_rows == 6
    assert report.passed is True  # coverage differences are not mismatches


def test_sampling_caps_compared_rows():
    days = [f"2024-{month:02d}-{day:02d}" for month in (1, 2, 3) for day in range(1, 21)]
    closes = {day: 10.0 for day in days}
    primary = BarsAdapter("akshare", _tables(closes))
    secondary = BarsAdapter("baostock", _tables(dict(closes)))
    report = CrossValidator(primary, secondary, sample_size=10).check(
        ["SH600519"], date(2024, 1, 1), date(2024, 3, 31)
    )
    stats = report.per_symbol["SH600519"]
    assert stats.overlap_rows == 60
    assert stats.sampled_rows == 10
    assert stats.compared_rows == 10


def test_sampler_spreads_indices_across_overlap():
    assert _sample_indices(6, 10) == [0, 1, 2, 3, 4, 5]  # fewer rows than sample
    assert _sample_indices(60, 6) == [0, 10, 20, 30, 40, 50]


def test_relative_diff_handles_zero_reference():
    assert _relative_diff(0.0, 0.0) == 0.0
    assert _relative_diff(0.0, 5.0) == 1.0
    assert _relative_diff(10.0, 10.0) == 0.0
