"""Quality-gate unit tests on synthetic canonical frames."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import QualityViolation
from pulsar_data.quality import check_canonical, explain_frame_problems
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts
from pulsar_data.sources.base import FetchRequest


def _bars_frame(**overrides) -> pd.DataFrame:
    base = {
        "symbol": ["SH600519", "SH600519"],
        "ts": [daily_ts("2024-01-02"), daily_ts("2024-01-03")],
        "open": [10.0, 10.5],
        "high": [11.0, 10.8],
        "low": [9.8, 10.2],
        "close": [10.8, 10.4],
        "volume": [1000.0, 1200.0],
        "amount": [10800.0, 12480.0],
        "adjust_factor": [1.0, 1.0],
        "quality": ["ok", "ok"],
    }
    base.update(overrides)
    frame = pd.DataFrame(base)
    return frame[list(BAR_COLUMNS)]


def _request() -> FetchRequest:
    return FetchRequest(Dataset.BARS_1D, date(2024, 1, 1), date(2024, 1, 31), "SH600519")


def test_clean_bars_pass():
    check_canonical(Dataset.BARS_1D, _bars_frame(), _request())


def test_column_set_enforced():
    frame = _bars_frame().drop(columns=["amount"])
    assert any("canonical columns" in problem for problem in explain_frame_problems(Dataset.BARS_1D, frame))
    with pytest.raises(QualityViolation, match="canonical columns"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_ohlc_invariant_violated():
    frame = _bars_frame(high=[11.0, 10.1])  # close 10.4 > high 10.1
    with pytest.raises(QualityViolation, match="OHLC"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_non_positive_price_rejected():
    frame = _bars_frame(open=[0.0, 10.5])
    with pytest.raises(QualityViolation, match="non-positive"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_nan_price_rejected():
    frame = _bars_frame(close=[float("nan"), 10.4])
    with pytest.raises(QualityViolation, match="null"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_duplicate_bars_rejected():
    frame = _bars_frame(
        ts=[daily_ts("2024-01-02"), daily_ts("2024-01-02")],
    )
    with pytest.raises(QualityViolation, match="duplicate"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_non_midnight_daily_ts_rejected():
    frame = _bars_frame(
        ts=[daily_ts("2024-01-02").replace(hour=9, minute=30), daily_ts("2024-01-03")],
    )
    with pytest.raises(QualityViolation, match="00:00"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_zero_volume_is_allowed():
    # limit-locked days legitimately trade zero shares on the limit price
    frame = _bars_frame(volume=[0.0, 1200.0], amount=[0.0, 12480.0])
    check_canonical(Dataset.BARS_1D, frame, _request())


def test_non_positive_adjust_factor_rejected():
    frame = _bars_frame(adjust_factor=[1.0, 0.0])
    with pytest.raises(QualityViolation, match="adjust_factor"):
        check_canonical(Dataset.BARS_1D, frame, _request())


def test_corporate_action_requires_component():
    frame = pd.DataFrame(
        [
            {
                "symbol": "SH600519",
                "ex_date": date(2024, 6, 20),
                "cash_dividend_per_share": 0.0,
                "bonus_share_ratio": 0.0,
                "rights_issue_ratio": 0.0,
                "rights_issue_price": None,
                "description": "empty",
            }
        ]
    )
    with pytest.raises(QualityViolation, match="all-zero"):
        check_canonical(Dataset.CORPORATE_ACTIONS, frame)


def test_rights_issue_requires_price():
    frame = pd.DataFrame(
        [
            {
                "symbol": "SZ000002",
                "ex_date": date(2024, 6, 20),
                "cash_dividend_per_share": 0.0,
                "bonus_share_ratio": 0.0,
                "rights_issue_ratio": 0.2727,
                "rights_issue_price": None,
                "description": "rights",
            }
        ]
    )
    with pytest.raises(QualityViolation, match="rights issues without a price"):
        check_canonical(Dataset.CORPORATE_ACTIONS, frame)
