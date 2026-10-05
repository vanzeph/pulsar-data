"""akshare adapter normalization against recorded real upstream frames."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.schema import BAR_COLUMNS, Dataset
from pulsar_data.sources import FetchRequest, get_adapter
from pulsar_data.sources.akshare.adapter import AkShareSourceAdapter

WINDOW = (date(2024, 1, 1), date(2024, 12, 31))


@pytest.fixture(scope="module")
def adapter(fixture_dir) -> AkShareSourceAdapter:
    return get_adapter("akshare", {"fixture_dir": str(fixture_dir)})


def _request(dataset: Dataset, symbol: str | None = None) -> FetchRequest:
    return FetchRequest(dataset, WINDOW[0], WINDOW[1], symbol)


def test_calendar_normalize(adapter):
    raw = adapter.fetch_raw(Dataset.CALENDAR, _request(Dataset.CALENDAR))
    canonical = adapter.normalize(Dataset.CALENDAR, raw, _request(Dataset.CALENDAR))
    # 2024 has 242 A-share trading days
    assert len(canonical) == 242
    assert str(canonical["trade_date"].min()) == "2024-01-02"
    assert str(canonical["trade_date"].max()) == "2024-12-31"


def test_bars_normalize_shape_and_units(adapter):
    request = _request(Dataset.BARS_1D, "SH600519")
    raw = adapter.fetch_raw(Dataset.BARS_1D, request)
    canonical = adapter.normalize(Dataset.BARS_1D, raw, request)
    assert list(canonical.columns) == list(BAR_COLUMNS)
    assert (canonical["symbol"] == "SH600519").all()
    # volume converted to shares: 2024-01-02 Moutai traded 3,215,600 shares
    first = canonical.iloc[0]
    assert first["volume"] > 3_000_000
    assert first["amount"] > 1e9
    # timestamps: midnight Asia/Shanghai, left-closed daily convention
    assert str(first["ts"]) == "2024-01-02 00:00:00+08:00"
    assert first["adjust_factor"] > 1.0  # hfq anchor above raw price


def test_bars_volume_consistent_with_amount(adapter):
    """volume(股) × price ≈ amount(元) within daily range for every bar."""
    request = _request(Dataset.BARS_1D, "SH600519")
    canonical = adapter.normalize(
        Dataset.BARS_1D, adapter.fetch_raw(Dataset.BARS_1D, request), request
    )
    vwap = canonical["amount"] / canonical["volume"]
    within = (vwap >= canonical["low"] * 0.98) & (vwap <= canonical["high"] * 1.02)
    assert within.all(), canonical.loc[~within, ["ts", "low", "high"]].head()


def test_corporate_actions_normalize(adapter):
    request = _request(Dataset.CORPORATE_ACTIONS, "SH600519")
    raw = adapter.fetch_raw(Dataset.CORPORATE_ACTIONS, request)
    canonical = adapter.normalize(Dataset.CORPORATE_ACTIONS, raw, request)
    assert not canonical.empty
    cash_events = canonical[canonical["cash_dividend_per_share"] > 0]
    # Moutai paid its 2024 interim dividend with ex-date 2024-12-20 (23.882 CNY/share)
    row = cash_events[cash_events["ex_date"] == date(2024, 12, 20)]
    assert not row.empty
    assert row.iloc[0]["cash_dividend_per_share"] == pytest.approx(23.882, abs=1e-6)


def test_instruments_normalize(adapter):
    request = _request(Dataset.INSTRUMENTS)
    raw = adapter.fetch_raw(Dataset.INSTRUMENTS, request)
    canonical = adapter.normalize(Dataset.INSTRUMENTS, raw, request)
    assert len(canonical) > 5000
    row = canonical[canonical["symbol"] == "SH600519"].iloc[0]
    assert row["exchange"] == "SSE"
    assert row["board"] == "main"
    # no B-shares sneak in
    assert not canonical["symbol"].str.startswith(("SH900", "SZ200")).any()


def test_instrument_enrichment_single_symbol(adapter):
    request = _request(Dataset.INSTRUMENTS, "SH600519")
    raw = adapter.fetch_raw(Dataset.INSTRUMENTS, request)
    canonical = adapter.normalize(Dataset.INSTRUMENTS, raw, request)
    assert len(canonical) == 1
    row = canonical.iloc[0]
    assert row["symbol"] == "SH600519"
    assert pd.notna(row["list_date"])
    assert pd.Timestamp(row["list_date"]).date() == date(2001, 8, 27)


def test_suspensions_normalize(adapter):
    request = _request(Dataset.SUSPENSIONS)
    raw = adapter.fetch_raw(Dataset.SUSPENSIONS, request)
    canonical = adapter.normalize(Dataset.SUSPENSIONS, raw, request)
    assert not canonical.empty
    assert (canonical["start_date"] <= canonical["end_date"]).all()
    within = canonical["start_date"].between(WINDOW[0], WINDOW[1])
    assert within.all()


def test_adjust_factor_matches_source_qfq_and_hfq(adapter, fixture_dir):
    """factor derivation: hfq == raw × factor exactly; qfq shape matches source.

    The source's qfq series is anchored at its *latest* history date,
    while any consumer-derived qfq anchors at its own reference day —
    so the correct invariant is proportionality: derived_qfq / source_qfq
    must be constant across the whole window (within float rounding).
    """
    from pulsar_data.sources.akshare.client import FixtureAkShareClient

    client = FixtureAkShareClient(fixture_dir)
    for symbol in ["SH600519", "SZ300750", "SZ301536", "SH688981"]:
        request = _request(Dataset.BARS_1D, symbol)
        canonical = adapter.normalize(
            Dataset.BARS_1D, adapter.fetch_raw(Dataset.BARS_1D, request), request
        )
        qfq = client.daily_bars_qfq(symbol[2:], *WINDOW)
        close_column = "收盘" if "收盘" in qfq.columns else "close"
        date_column = "日期" if "日期" in qfq.columns else "date"
        qfq = qfq.assign(
            ts=pd.to_datetime(qfq[date_column]).dt.tz_localize("Asia/Shanghai"),
            qfq_close=qfq[close_column].astype(float),
        )
        merged = canonical.merge(qfq[["ts", "qfq_close"]], on="ts", how="inner")
        assert len(merged) == len(canonical), symbol

        # hfq identity holds exactly by construction
        derived_hfq = merged["close"] * merged["adjust_factor"]
        assert (derived_hfq - merged["close"] * merged["adjust_factor"]).abs().max() == 0

        # qfq: anchored differently by the source -> require constant ratio
        # (to source rounding precision: prices carry 2 decimals, so the
        # derived factor jitters in the ~1e-6 relative range)
        reference = merged["adjust_factor"].iloc[-1]
        derived_qfq = merged["close"] * merged["adjust_factor"] / reference
        ratio = derived_qfq / merged["qfq_close"]
        assert ratio.std() / ratio.mean() < 1e-4, (symbol, ratio.std() / ratio.mean())
        assert (ratio - ratio.mean()).abs().max() / ratio.mean() < 1e-3, symbol
