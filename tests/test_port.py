"""LakeMarketDataPort: the MarketDataPort read side over the local lake."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from pulsar_contracts import AdjustMode, Freq, MarketDataPort

from pulsar_data.adjust import derive_adjusted
from pulsar_data.errors import DataNotAvailable
from pulsar_data.lake import DataLake
from pulsar_data.port import LakeMarketDataPort
from pulsar_data.query import LakeQuery
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts

# Same hand-computed sample as tests/test_adjust.py (10送2.5 then 派2.0).
DAYS = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]
CLOSES = [10.0, 10.0, 8.0, 10.0, 8.0, 8.0]
FACTORS = [1.0, 1.0, 1.25, 1.25, 1.5625, 1.5625]


def _bars(
    days=DAYS,
    symbol="SH600519",
    closes=None,
    factors=None,
) -> pd.DataFrame:
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


@pytest.fixture()
def port(lake: DataLake) -> LakeMarketDataPort:
    lake.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": [date.fromisoformat(d) for d in DAYS]}), source="akshare")
    lake.write(Dataset.BARS_1D, _bars(), source="akshare")
    lake.write(Dataset.BARS_1D, _bars(symbol="SZ000001", closes=[20.0] * 6, factors=[1.0] * 6), source="akshare")
    lake.write(
        Dataset.INSTRUMENTS,
        pd.DataFrame(
            [
                {
                    "symbol": "SH600519",
                    "name": "Kweichow Moutai",
                    "exchange": "SSE",
                    "board": "main",
                    "is_st": False,
                    "status": "listed",
                    "list_date": date(2001, 8, 27),
                    "delist_date": None,
                    "shares_outstanding": 1256197800.0,
                },
                {
                    "symbol": "SZ000001",
                    "name": "Ping An Bank",
                    "exchange": "SZSE",
                    "board": "main",
                    "is_st": False,
                    "status": "listed",
                    "list_date": date(1991, 4, 3),
                    "delist_date": None,
                    "shares_outstanding": None,
                },
                {
                    "symbol": "SZ000002",
                    "name": "Delisted Co",
                    "exchange": "SZSE",
                    "board": "main",
                    "is_st": False,
                    "status": "delisted",
                    "list_date": date(1991, 1, 1),
                    "delist_date": date(2020, 12, 31),
                    "shares_outstanding": None,
                },
            ]
        ),
        source="akshare",
    )
    lake.write(
        Dataset.CORPORATE_ACTIONS,
        pd.DataFrame(
            [
                {
                    "symbol": "SH600519",
                    "ex_date": date(2024, 1, 4),
                    "cash_dividend_per_share": 0.0,
                    "bonus_share_ratio": 0.25,
                    "rights_issue_ratio": 0.0,
                    "rights_issue_price": None,
                    "description": "10送2.5",
                },
                {
                    "symbol": "SH600519",
                    "ex_date": date(2024, 1, 8),
                    "cash_dividend_per_share": 2.0,
                    "bonus_share_ratio": 0.0,
                    "rights_issue_ratio": 0.0,
                    "rights_issue_price": None,
                    "description": "cash 2.0",
                },
            ]
        ),
        source="akshare",
    )
    return LakeMarketDataPort(lake)


def test_satisfies_port_protocol(port: LakeMarketDataPort):
    assert isinstance(port, MarketDataPort)


def test_fetch_bars_raw_forward_backward(port: LakeMarketDataPort):
    for mode, expected in (
        (AdjustMode.RAW, CLOSES),
        (AdjustMode.BACKWARD, [10.0, 10.0, 10.0, 12.5, 12.5, 12.5]),
        (AdjustMode.FORWARD, [6.4, 6.4, 6.4, 8.0, 8.0, 8.0]),
    ):
        frame = port.fetch_bars(["SH600519"], date(2024, 1, 1), date(2024, 1, 31), Freq.DAILY, mode)
        assert list(frame.columns) == list(BAR_COLUMNS)
        assert frame.sort_values("ts")["close"].tolist() == pytest.approx(expected)


def test_fetch_bars_multi_symbol(port: LakeMarketDataPort):
    frame = port.fetch_bars(
        ["sh600519", "000001"],  # non-canonical inputs accepted
        date(2024, 1, 2),
        date(2024, 1, 9),
        Freq.DAILY,
        AdjustMode.RAW,
    )
    assert set(frame["symbol"]) == {"SH600519", "SZ000001"}
    assert len(frame) == 12


def test_fetch_bars_empty_symbols_returns_canonical_empty(port: LakeMarketDataPort):
    frame = port.fetch_bars([], date(2024, 1, 1), date(2024, 1, 31), Freq.DAILY, AdjustMode.RAW)
    assert frame.empty
    assert list(frame.columns) == list(BAR_COLUMNS)


def test_fetch_bars_one_minute_freq_has_no_source(port: LakeMarketDataPort):
    """1m stays reserved (no source serves it); 5/15/30/60m are lake datasets."""
    from pulsar_data.errors import ConfigurationError

    with pytest.raises(ConfigurationError, match="no lake dataset serves freq"):
        port.fetch_bars(["SH600519"], date(2024, 1, 1), date(2024, 1, 31), Freq.MINUTE, AdjustMode.RAW)


def test_fetch_bars_raises_on_gap(lake: DataLake):
    """One in-range trading day missing without explanation -> hard error."""
    holed = _bars(
        days=DAYS[:4] + DAYS[5:],  # drop 2024-01-08
        closes=CLOSES[:4] + CLOSES[5:],
        factors=FACTORS[:4] + FACTORS[5:],
    )
    lake.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": [date.fromisoformat(d) for d in DAYS]}), source="akshare")
    lake.write(Dataset.BARS_1D, holed, source="akshare")
    port = LakeMarketDataPort(lake)
    with pytest.raises(DataNotAvailable, match="2024-01-08"):
        port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 9), Freq.DAILY, AdjustMode.RAW)


def test_fetch_bars_missing_symbol_raises(port: LakeMarketDataPort):
    with pytest.raises(DataNotAvailable, match="SZ600000"):
        port.fetch_bars(["SZ600000"], date(2024, 1, 2), date(2024, 1, 9), Freq.DAILY, AdjustMode.RAW)


def test_suspended_day_is_excused(lake: DataLake):
    lake.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": [date.fromisoformat(d) for d in DAYS]}), source="akshare")
    lake.write(
        Dataset.BARS_1D,
        _bars(days=DAYS[:4] + DAYS[5:], closes=CLOSES[:4] + CLOSES[5:], factors=FACTORS[:4] + FACTORS[5:]),
        source="akshare",
    )  # 01-08 suspended
    lake.write(
        Dataset.SUSPENSIONS,
        pd.DataFrame(
            [
                {
                    "symbol": "SH600519",
                    "start_date": date(2024, 1, 8),
                    "end_date": date(2024, 1, 8),
                    "reason": "trading halt",
                }
            ]
        ),
        source="akshare",
    )
    port = LakeMarketDataPort(lake)
    frame = port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 9), Freq.DAILY, AdjustMode.RAW)
    assert len(frame) == 5


def test_pre_listing_window_days_excused(port: LakeMarketDataPort):
    """Window days before the first stored bar (coverage start) never raise."""
    frame = port.fetch_bars(["SH600519"], date(2023, 12, 1), date(2024, 1, 9), Freq.DAILY, AdjustMode.RAW)
    assert frame["ts"].min() == daily_ts("2024-01-02")


def test_list_instruments_filters_by_as_of(port: LakeMarketDataPort):
    symbols_at_2024 = {instrument.symbol for instrument in port.list_instruments(date(2024, 1, 5))}
    assert symbols_at_2024 == {"SH600519", "SZ000001"}  # delisted SZ000002 excluded
    # Moutai not listed until 2001-08-27; the two 1991 listings are
    symbols_at_2001 = {instrument.symbol for instrument in port.list_instruments(date(2001, 8, 26))}
    assert symbols_at_2001 == {"SZ000001", "SZ000002"}
    assert port.list_instruments(date(1990, 1, 1)) == []  # nothing listed yet
    instruments = {instrument.symbol: instrument for instrument in port.list_instruments(date(2024, 1, 5))}
    moutai = instruments["SH600519"]
    assert moutai.exchange.value == "SSE"
    assert moutai.board.value == "main"
    assert moutai.list_date == date(2001, 8, 27)
    assert moutai.shares_outstanding == 1256197800.0
    assert instruments["SZ000001"].shares_outstanding is None


def test_fetch_corporate_actions(port: LakeMarketDataPort):
    actions = port.fetch_corporate_actions("SH600519")
    assert len(actions) == 2
    bonus = actions[0]
    assert bonus.ex_date == date(2024, 1, 4)
    assert bonus.bonus_share_ratio == 0.25
    assert bonus.cash_dividend_per_share == 0.0
    cash = actions[1]
    assert cash.ex_date == date(2024, 1, 8)
    assert cash.cash_dividend_per_share == 2.0
    assert port.fetch_corporate_actions("SZ000001") == []


def test_calendar(port: LakeMarketDataPort):
    days = port.calendar(date(2024, 1, 1), date(2024, 1, 31))
    assert days == [date.fromisoformat(d) for d in DAYS]
    assert port.calendar(date(2024, 2, 1), date(2024, 2, 28)) == []


def test_calendar_requires_ingested_calendar(lake: DataLake):
    lake.write(Dataset.BARS_1D, _bars(), source="akshare")
    port = LakeMarketDataPort(lake)
    with pytest.raises(DataNotAvailable, match="calendar"):
        port.calendar(date(2024, 1, 1), date(2024, 1, 31))


def test_subscribe_is_d5_scope(port: LakeMarketDataPort):
    with pytest.raises(NotImplementedError, match="D5|realtime"):
        port.subscribe(["SH600519"], lambda snapshot: None)


def test_query_layer_composition(port: LakeMarketDataPort):
    """fetch_bars == DuckDB query + derive_adjusted, per the design."""
    raw = port.query.bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 9))
    expected = derive_adjusted(raw, AdjustMode.BACKWARD)
    got = port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 9), Freq.DAILY, AdjustMode.BACKWARD)
    pd.testing.assert_frame_equal(got, expected, check_dtype=False)


# --------------------------------------------------------------------------
# Real-data cross-check against recorded akshare fixtures (Moutai 2024).
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def moutai_lake(fixture_dir, tmp_path_factory) -> DataLake:
    from pulsar_data.backfill import BackfillRunner
    from pulsar_data.sources import get_adapter

    lake = DataLake(tmp_path_factory.mktemp("lake-port"))
    adapter = get_adapter("akshare", {"fixture_dir": str(fixture_dir)})
    report = BackfillRunner(adapter, lake).run(["SH600519"], date(2024, 1, 1), date(2024, 12, 31))
    assert not report.failed_symbols, report.failed_symbols
    return lake


def test_adjustment_cross_checks_against_recorded_source(moutai_lake: DataLake):
    """Known dividend/bonus sample (SH600519, ex-dates 2024-06-19 and
    2024-12-20): derived forward/backward series reproduce the source's
    own adjustment arithmetic, whose factor was recorded as
    hfq_close/raw_close from the same endpoint."""
    port = LakeMarketDataPort(moutai_lake)
    start, end = date(2024, 1, 2), date(2024, 12, 31)
    raw = port.fetch_bars(["SH600519"], start, end, Freq.DAILY, AdjustMode.RAW)
    forward = port.fetch_bars(["SH600519"], start, end, Freq.DAILY, AdjustMode.FORWARD)
    backward = port.fetch_bars(["SH600519"], start, end, Freq.DAILY, AdjustMode.BACKWARD)

    raw = raw.sort_values("ts").reset_index(drop=True)
    forward = forward.sort_values("ts").reset_index(drop=True)
    backward = backward.sort_values("ts").reset_index(drop=True)

    # source-side hfq proxy built directly from stored columns
    source_hfq = (raw["close"] * raw["adjust_factor"]).rename("hfq")

    # (1) anchor properties
    assert forward["close"].iloc[-1] == pytest.approx(raw["close"].iloc[-1])
    assert backward["close"].iloc[0] == pytest.approx(raw["close"].iloc[0])

    # (2) daily returns of our derived series == returns of source hfq
    for series in (forward["close"], backward["close"]):
        derived_returns = series.pct_change().dropna()
        source_returns = source_hfq.pct_change().dropna()
        assert derived_returns.tolist() == pytest.approx(source_returns.tolist(), rel=1e-9, abs=1e-12)

    # (3) continuity across both known ex-dividend dates: the raw close
    # drops by roughly the dividend, the adjusted series must not
    for ex_date in (date(2024, 6, 19), date(2024, 12, 20)):
        raw_ret = raw["close"].pct_change()
        adj_ret = forward["close"].pct_change()
        index = raw.index[raw["ts"].map(lambda ts: ts.date()) == ex_date][0]
        # adjusted return stays within the raw drop plus/minus the
        # dividend yield (i.e. adjustment healed the gap, not widened it)
        assert adj_ret.loc[index] > raw_ret.loc[index]
        assert abs(adj_ret.loc[index]) < abs(raw_ret.loc[index]) + 0.05

    # (4) factor actually steps on both ex-dates (sample has known events)
    factors = raw.set_index(raw["ts"].map(lambda ts: ts.date()))["adjust_factor"]
    assert factors.loc[date(2024, 6, 19)] > factors.loc[date(2024, 6, 18)]
    assert factors.loc[date(2024, 12, 20)] > factors.loc[date(2024, 12, 19)]
