"""Query-time adjustment derivation against hand-computed cases.

The sample encodes one bonus issue (10送2.5, ratio 0.25) and one cash
dividend (2.0 CNY/share) with deliberately round numbers so every
expected value below is derived by hand:

=== setup (factor is cumulative, hfq-style: 后复权价 = raw × factor) ===

day      event                    raw close   cumulative factor
d1 01-02                          10.0        1.0
d2 01-03                          10.0        1.0
d3 01-04  10送2.5: ex_ref=10/1.25  8.0        1.25
d4 01-05                          10.0        1.25
d5 01-08  cash div 2.0: ex=10-2    8.0        1.25*1.25=1.5625
d6 01-09                          8.0        1.5625

=== BACKWARD (后复权, anchor = first bar of the window) ===

adj = raw x factor / factor_anchor; full window anchors at d1 (F=1):
    10.0, 10.0, 10.0, 12.5, 12.5, 12.5
(the ex-day drop and the dividend both vanish: total-return continuity)

=== FORWARD (前复权, anchor = last bar of the window) ===

full window anchors at d6 (F=1.5625):
    6.4, 6.4, 6.4, 8.0, 8.0, 8.0
(the newest bar keeps its raw price)

Both derivations match the source-side arithmetic by construction: the
lake factor is hfq_close/raw_close from the same endpoint, so
source hfq(t) = raw(t) x F(t) equals our BACKWARD series anchored at
listing, and source qfq equals our FORWARD series anchored at the
latest synced day.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from pulsar_contracts import AdjustMode

from pulsar_data.adjust import anchor_factors, derive_adjusted
from pulsar_data.schema import BAR_COLUMNS, daily_ts

DAYS = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]
CLOSES = [10.0, 10.0, 8.0, 10.0, 8.0, 8.0]
FACTORS = [1.0, 1.0, 1.25, 1.25, 1.5625, 1.5625]


def sample(symbol: str = "SH600519", days=None, closes=None, factors=None) -> pd.DataFrame:
    days = days or DAYS
    closes = closes or CLOSES
    factors = factors or FACTORS
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts": [daily_ts(day) for day in days],
            "open": [close * 0.99 for close in closes],
            "high": [close * 1.02 for close in closes],
            "low": [close * 0.98 for close in closes],
            "close": closes,
            "volume": [100.0] * len(days),
            "amount": [800.0] * len(days),
            "adjust_factor": factors,
            "quality": ["ok"] * len(days),
        }
    )[list(BAR_COLUMNS)]


def closes_of(frame: pd.DataFrame) -> list[float]:
    return frame.sort_values("ts")["close"].tolist()


def test_backward_matches_hand_computed_full_window():
    out = derive_adjusted(sample(), AdjustMode.BACKWARD)
    assert closes_of(out) == pytest.approx([10.0, 10.0, 10.0, 12.5, 12.5, 12.5])


def test_forward_matches_hand_computed_full_window():
    out = derive_adjusted(sample(), AdjustMode.FORWARD)
    assert closes_of(out) == pytest.approx([6.4, 6.4, 6.4, 8.0, 8.0, 8.0])
    # anchor property: the newest bar keeps its raw price, on every OHLC field
    last = out.sort_values("ts").iloc[-1]
    raw_last = sample().sort_values("ts").iloc[-1]
    for column in ("open", "high", "low", "close"):
        assert last[column] == pytest.approx(raw_last[column])


def test_backward_anchor_is_first_bar_of_window():
    out = derive_adjusted(sample(), AdjustMode.BACKWARD)
    first = out.sort_values("ts").iloc[0]
    assert first["close"] == pytest.approx(10.0)  # raw close of d1 kept


def test_subwindow_reanchors():
    """A shorter window re-anchors: [d3, d5] forward anchors at d5 (F=1.5625),
    backward at d3 (F=1.25) — hand-derived from the same table."""
    sub = sample(days=DAYS[2:5], closes=CLOSES[2:5], factors=FACTORS[2:5])
    forward = derive_adjusted(sub, AdjustMode.FORWARD)
    assert closes_of(forward) == pytest.approx([8.0 * 1.25 / 1.5625, 10.0 * 1.25 / 1.5625, 8.0])
    backward = derive_adjusted(sub, AdjustMode.BACKWARD)
    assert closes_of(backward) == pytest.approx([8.0, 10.0 * 1.25 / 1.25, 8.0 * 1.5625 / 1.25])


def test_raw_mode_is_identity():
    frame = sample()
    out = derive_adjusted(frame, AdjustMode.RAW)
    pd.testing.assert_frame_equal(out, frame)


def test_volume_amount_and_factor_pass_through():
    original = sample()
    for mode in (AdjustMode.RAW, AdjustMode.FORWARD, AdjustMode.BACKWARD):
        out = derive_adjusted(original, mode)
        assert out["volume"].tolist() == original["volume"].tolist()
        assert out["amount"].tolist() == original["amount"].tolist()
        assert out["adjust_factor"].tolist() == original["adjust_factor"].tolist()
        assert list(out.columns) == list(BAR_COLUMNS)


def test_total_return_ratios_preserved():
    """Adjusted close ratios between consecutive days equal the
    total-return ratios raw x factor — the property that makes
    adjusted series usable for return computation."""
    frame = sample()
    raw_returns = pd.Series(
        [raw * factor for raw, factor in zip(CLOSES, FACTORS)]
    ).pct_change().dropna()
    for mode in (AdjustMode.FORWARD, AdjustMode.BACKWARD):
        adjusted = derive_adjusted(frame, mode).sort_values("ts")["close"]
        adjusted_returns = adjusted.pct_change().dropna()
        assert adjusted_returns.tolist() == pytest.approx(raw_returns.tolist())


def test_multiple_symbols_anchor_independently():
    frame = pd.concat(
        [sample("SH600519"), sample("SZ000001", closes=[20.0] * 6, factors=[1.0] * 6)],
        ignore_index=True,
    )
    out = derive_adjusted(frame, AdjustMode.FORWARD)
    moutai = out[out["symbol"] == "SH600519"]
    plain = out[out["symbol"] == "SZ000001"]
    assert closes_of(moutai) == pytest.approx([6.4, 6.4, 6.4, 8.0, 8.0, 8.0])
    assert closes_of(plain) == pytest.approx([20.0] * 6)  # flat factor: no restatement


def test_unsorted_input_is_anchored_by_ts_not_row_order():
    shuffled = sample().sample(frac=1.0, random_state=7).reset_index(drop=True)
    out = derive_adjusted(shuffled, AdjustMode.FORWARD)
    assert closes_of(out) == pytest.approx([6.4, 6.4, 6.4, 8.0, 8.0, 8.0])


def test_anchor_factors_raw_is_one():
    frame = sample()
    assert set(anchor_factors(frame, AdjustMode.RAW)) == {1.0}


def test_empty_frame_passes_through():
    empty = sample().iloc[0:0]
    out = derive_adjusted(empty, AdjustMode.FORWARD)
    assert out.empty
    assert list(out.columns) == list(BAR_COLUMNS)


def test_missing_columns_rejected():
    with pytest.raises(KeyError, match="adjust_factor"):
        derive_adjusted(pd.DataFrame({"symbol": [], "ts": []}), AdjustMode.FORWARD)
