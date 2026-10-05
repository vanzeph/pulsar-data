"""baostock adapter: session lifecycle, egress reuse, normalization, ingestion.

Everything here runs fully offline: the baostock SDK is replaced by a
fake module (socket-protocol SDK shape: login/logout + ResultData
cursors) or by a protocol-level fake client returning recorded raw
frames.  No test touches the network.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import (
    ConfigurationError,
    EgressViolation,
    FetchError,
)
from pulsar_data.lake import DataLake
from pulsar_data.schema import BAR_COLUMNS, CALENDAR_COLUMNS, Dataset, daily_ts
from pulsar_data.quality import check_canonical
from pulsar_data.sources import list_adapters
from pulsar_data.sources.base import FetchRequest, run_ingestion
from pulsar_data.sources.baostock import BaostockSourceAdapter, LiveBaostockClient
from pulsar_data.sources.baostock.client import to_baostock_code, from_baostock_code

# --------------------------------------------------------------------------
# Fake baostock SDK machinery (ResultData cursor protocol + module)
# --------------------------------------------------------------------------
K_FIELDS = [
    "date", "code", "open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST",
]


class FakeResultData:
    def __init__(self, fields, rows, error_code="0", error_msg="success"):
        self.fields = list(fields)
        self._rows = [list(row) for row in rows]
        self.error_code = error_code
        self.error_msg = error_msg
        self._cursor = 0

    def next(self):
        if self._cursor < len(self._rows):
            self._cursor += 1
            return True
        return False

    def get_row_data(self):
        return list(self._rows[self._cursor - 1])


#: (date, open, high, low, close, volume, amount) raw daily bars for sh.600519.
RAW_BARS = [
    ("2024-01-02", "9.80", "10.20", "9.70", "10.00", "1000000", "9900000"),
    ("2024-01-03", "10.00", "10.60", "9.95", "10.50", "1200000", "12500000"),
    ("2024-01-04", "10.50", "10.80", "10.40", "10.70", "900000", "9500000"),
    ("2024-01-05", "10.70", "10.90", "10.60", "10.80", "1100000", "11800000"),
    # 2024-01-06/07 is a weekend; 01-08 carries the ex-date in the hfq series
    ("2024-01-08", "5.30", "5.60", "5.25", "5.40", "2400000", "12900000"),
    ("2024-01-09", "5.40", "5.65", "5.35", "5.50", "800000", "4350000"),
]

#: hfq close per day: 1.0x before the ex-date, 2.0x from 01-08 on.
HFQ_SCALE = {
    "2024-01-02": 1.0, "2024-01-03": 1.0, "2024-01-04": 1.0,
    "2024-01-05": 1.0, "2024-01-08": 2.0, "2024-01-09": 2.0,
}


def _k_rows(window_days: list[str], *, adjustflag: str, suspended: frozenset[str] = frozenset()) -> list[list[str]]:
    rows = []
    for entry in RAW_BARS:
        day, open_, high, low, close, volume, amount = entry
        if day not in window_days:
            continue
        if day in suspended:
            open_ = high = low = close = ""
            volume = ""
            amount = ""
        else:
            close = f"{float(close) * HFQ_SCALE[day]:.4f}" if adjustflag == "1" else close
        rows.append(
            [day, "sh.600519", open_, high, low, close, close,
             volume, amount, adjustflag, "1.0", "1", "0.5", "0"]
        )
    return rows


class FakeBaostockModule:
    """SDK-shaped fake: records calls, serves bars/calendar from literals."""

    def __init__(self, *, error_code: str = "0", raise_on_k: BaseException | None = None):
        self.error_code = error_code
        self.raise_on_k = raise_on_k
        self.logins = 0
        self.logouts = 0
        self.queries: list[tuple] = []

    def login(self, *args, **kwargs):
        self.logins += 1
        return FakeResultData([], [], error_code="0")

    def logout(self):
        self.logouts += 1
        return FakeResultData([], [], error_code="0")

    def query_history_k_data_plus(
        self, code, fields, start_date, end_date, frequency="d", adjustflag="3"
    ):
        self.queries.append(("k", code, start_date, end_date, frequency, adjustflag))
        if self.raise_on_k is not None:
            raise self.raise_on_k
        window = [row[0] for row in RAW_BARS if start_date <= row[0] <= end_date]
        rows = _k_rows(window, adjustflag=adjustflag)
        return FakeResultData(K_FIELDS, rows, error_code=self.error_code)

    def query_trade_dates(self, start_date, end_date):
        self.queries.append(("calendar", None, start_date, end_date, None, None))
        rows = []
        from datetime import date as _date, timedelta as _timedelta

        cursor = _date.fromisoformat(start_date)
        final = _date.fromisoformat(end_date)
        while cursor <= final:
            rows.append([cursor.isoformat(), "1" if cursor.weekday() < 5 else "0"])
            cursor += _timedelta(days=1)
        return FakeResultData(["calendar_date", "is_trading_day"], rows, error_code=self.error_code)

    def query_adjust_factor(self, code, start_date, end_date):
        self.queries.append(("adjust_factor", code, start_date, end_date, None, None))
        rows = [["sh.600519", "2024-01-08", "2.000000"]]
        return FakeResultData(["code", "dividAdjustFactor", "adjustDate"], rows)


# --------------------------------------------------------------------------
# Protocol-level fake client (raw frames in, no SDK shape)
# --------------------------------------------------------------------------
class FakeBaostockClient:
    """Implements the BaostockClient protocol from recorded literals."""

    def __init__(self, *, suspended: frozenset[str] = frozenset(), drop_hfq_day: str | None = None):
        self.suspended = suspended
        self.drop_hfq_day = drop_hfq_day
        self.calls: list[tuple] = []

    def daily_bars_pair(self, code, start, end):
        self.calls.append(("bars", code, start, end))
        window = [row[0] for row in RAW_BARS if start.isoformat() <= row[0] <= end.isoformat()]
        raw = pd.DataFrame(_k_rows(window, adjustflag="3", suspended=self.suspended), columns=K_FIELDS)
        hfq_window = [day for day in window if day != self.drop_hfq_day]
        hfq = pd.DataFrame(_k_rows(hfq_window, adjustflag="1", suspended=self.suspended), columns=K_FIELDS)
        return raw, hfq

    def minute_bars(self, code, start, end, frequency="5"):
        self.calls.append(("minute", code, start, end, frequency))
        return pd.DataFrame()

    def adjust_factor_events(self, code, start, end):
        self.calls.append(("adjust_factor", code, start, end))
        return pd.DataFrame([{"code": "sh.600519", "dividAdjustFactor": 2.0, "adjustDate": "2024-01-08"}])

    def trade_dates(self, start, end):
        self.calls.append(("calendar", start, end))
        rows = []
        from datetime import date as _date, timedelta as _timedelta

        cursor = start
        while cursor <= end:
            rows.append({"calendar_date": cursor.isoformat(), "is_trading_day": "1" if cursor.weekday() < 5 else "0"})
            cursor += _timedelta(days=1)
        return pd.DataFrame(rows)

    def close(self):
        pass


# --------------------------------------------------------------------------
# Symbol conversion
# --------------------------------------------------------------------------
def test_symbol_round_trip():
    assert to_baostock_code("SH600519") == "sh.600519"
    assert from_baostock_code("sh.600519") == "SH600519"
    assert to_baostock_code("SZ000001") == "sz.000001"


def test_baostock_registered():
    assert "baostock" in list_adapters()


# --------------------------------------------------------------------------
# Normalization
# --------------------------------------------------------------------------
def _bars_request() -> FetchRequest:
    return FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")


def test_normalize_daily_bars_canonical_schema_and_factor():
    adapter = BaostockSourceAdapter(client=FakeBaostockClient())
    request = _bars_request()
    raw = adapter.fetch_raw(Dataset.BARS_1D, request)
    canonical = adapter.normalize(Dataset.BARS_1D, raw, request)

    assert list(canonical.columns) == list(BAR_COLUMNS)
    check_canonical(Dataset.BARS_1D, canonical, request)  # hard gates pass
    assert len(canonical) == 6
    # cumulative factor doubles from the ex-date on
    by_day = {str(pd.Timestamp(ts).date()): factor for ts, factor in zip(canonical["ts"], canonical["adjust_factor"])}
    assert by_day["2024-01-05"] == pytest.approx(1.0)
    assert by_day["2024-01-08"] == pytest.approx(2.0)
    assert by_day["2024-01-09"] == pytest.approx(2.0)
    # volume stays in shares (no lots->shares conversion like Eastmoney)
    assert canonical.loc[0, "volume"] == pytest.approx(1_000_000.0)
    # daily ts at midnight Asia/Shanghai
    assert canonical.loc[0, "ts"] == daily_ts("2024-01-02")


def test_suspended_rows_are_dropped_not_fatal():
    adapter = BaostockSourceAdapter(client=FakeBaostockClient(suspended={"2024-01-04"}))
    request = _bars_request()
    raw = adapter.fetch_raw(Dataset.BARS_1D, request)
    canonical = adapter.normalize(Dataset.BARS_1D, raw, request)
    days = {str(pd.Timestamp(ts).date()) for ts in canonical["ts"]}
    assert "2024-01-04" not in days
    assert len(canonical) == 5


def test_misaligned_hfq_series_refused():
    adapter = BaostockSourceAdapter(client=FakeBaostockClient(drop_hfq_day="2024-01-04"))
    request = _bars_request()
    with pytest.raises(ConfigurationError, match="misaligned"):
        adapter.fetch_raw(Dataset.BARS_1D, request)


def test_calendar_filters_non_trading_days():
    adapter = BaostockSourceAdapter(client=FakeBaostockClient())
    request = FetchRequest(Dataset.CALENDAR, date(2024, 1, 2), date(2024, 1, 9))
    raw = adapter.fetch_raw(Dataset.CALENDAR, request)
    calendar = adapter.normalize(Dataset.CALENDAR, raw, request)
    assert list(calendar.columns) == list(CALENDAR_COLUMNS)
    check_canonical(Dataset.CALENDAR, calendar, request)
    days = [str(day) for day in calendar["trade_date"]]
    assert days == ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]


def test_unsupported_dataset_rejected():
    adapter = BaostockSourceAdapter(client=FakeBaostockClient())
    request = FetchRequest(Dataset.INSTRUMENTS, date(2024, 1, 2), date(2024, 1, 9))
    with pytest.raises(ConfigurationError, match="not supported"):
        adapter.fetch_raw(Dataset.INSTRUMENTS, request)


def test_run_ingestion_through_full_pipeline(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = BaostockSourceAdapter(client=FakeBaostockClient())
    result = run_ingestion(adapter, _bars_request(), lake)
    assert result.rows == 6
    stored = lake.read(Dataset.BARS_1D)
    assert len(stored) == 6
    assert set(stored["quality"]) == {"ok"}
    marks = lake.watermarks()
    assert (marks["source"] == "baostock").all()


# --------------------------------------------------------------------------
# Live client: session lifecycle + egress reuse (fake module, offline)
# --------------------------------------------------------------------------
def _live_client(module: FakeBaostockModule, **kwargs) -> LiveBaostockClient:
    defaults = dict(
        min_interval=0.0,
        retries=1,
        breaker_threshold=3,
        breaker_cooldown=60.0,
    )
    defaults.update(kwargs)
    return LiveBaostockClient(module_factory=lambda: module, **defaults)


def test_session_login_logout_lifecycle():
    module = FakeBaostockModule()
    client = _live_client(module)
    client.daily_bars_pair("sh.600519", date(2024, 1, 2), date(2024, 1, 5))
    client.daily_bars_pair("sh.600519", date(2024, 1, 8), date(2024, 1, 9))
    assert module.logins == 1  # one session reused across queries
    assert module.logouts == 0
    client.logout()
    assert module.logouts == 1
    # the next query transparently re-logins
    client.trade_dates(date(2024, 1, 2), date(2024, 1, 9))
    assert module.logins == 2


def test_context_managers_close_sessions():
    module = FakeBaostockModule()
    client = _live_client(module)
    with client:
        client.trade_dates(date(2024, 1, 2), date(2024, 1, 9))
    assert module.logins == 1 and module.logouts == 1


def test_daily_bars_pair_uses_raw_and_hfq_adjustflags():
    module = FakeBaostockModule()
    client = _live_client(module)
    raw, hfq = client.daily_bars_pair("sh.600519", date(2024, 1, 8), date(2024, 1, 9))
    k_queries = [query for query in module.queries if query[0] == "k"]
    assert [query[5] for query in k_queries] == ["3", "1"]  # raw first, hfq second
    assert all(query[1] == "sh.600519" for query in k_queries)
    assert k_queries[0][2:] == ("2024-01-08", "2024-01-09", "d", "3")
    # raw close 5.40 vs hfq close 10.80 on the ex-date
    assert raw.loc[0, "close"] == "5.40"
    assert float(hfq.loc[0, "close"]) == pytest.approx(10.80, abs=1e-3)
    assert list(raw.columns) == K_FIELDS


def test_minute_bars_frequency_validated():
    module = FakeBaostockModule()
    client = _live_client(module)
    with pytest.raises(ConfigurationError, match="frequency"):
        client.minute_bars("sh.600519", date(2024, 1, 8), date(2024, 1, 9), "7")
    frame = client.minute_bars("sh.600519", date(2024, 1, 8), date(2024, 1, 9), "5")
    k_queries = [query for query in module.queries if query[0] == "k"]
    assert k_queries[-1][4] == "5"
    assert isinstance(frame, pd.DataFrame)


def test_adjust_factor_events_served():
    module = FakeBaostockModule()
    client = _live_client(module)
    events = client.adjust_factor_events("sh.600519", date(2024, 1, 1), date(2024, 1, 31))
    assert not events.empty
    assert "dividAdjustFactor" in events.columns


def test_egress_guard_refuses_private_resolution():
    module = FakeBaostockModule()
    client = _live_client(module, resolver=lambda host: ["192.168.10.1"])  # private address
    with pytest.raises(EgressViolation, match="forbidden"):
        client.login()
    assert module.logins == 0  # never reached the SDK


def test_egress_guard_refuses_localhost_by_name():
    module = FakeBaostockModule()
    client = _live_client(module, host="localhost")
    with pytest.raises(EgressViolation, match="localhost"):
        client.login()
    assert module.logins == 0


def test_query_error_code_maps_to_fetch_error():
    module = FakeBaostockModule(error_code="10001")
    client = _live_client(module)
    with pytest.raises(FetchError, match="10001"):
        client.trade_dates(date(2024, 1, 2), date(2024, 1, 9))


def test_client_circuit_breaker_opens_after_consecutive_failures():
    module = FakeBaostockModule(raise_on_k=ConnectionError("socket reset by peer"))
    client = _live_client(module, retries=1, breaker_threshold=2, breaker_cooldown=60.0)
    with pytest.raises(FetchError):
        client.daily_bars_pair("sh.600519", date(2024, 1, 2), date(2024, 1, 3))
    with pytest.raises(FetchError):
        client.daily_bars_pair("sh.600519", date(2024, 1, 4), date(2024, 1, 5))
    # tripped now: refused before any new upstream attempt
    with pytest.raises(FetchError, match="circuit breaker open"):
        client.daily_bars_pair("sh.600519", date(2024, 1, 8), date(2024, 1, 9))
    assert len([query for query in module.queries if query[0] == "k"]) == 2  # one raw query per failed call
