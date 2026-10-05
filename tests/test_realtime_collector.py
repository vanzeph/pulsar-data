"""Offline tests for the realtime snapshot collectors.

All network payloads are canned from the *real* endpoint shapes (verified
against live Sina / Eastmoney responses on 2026-10-05), so CI never
depends on the network.  A live smoke test lives in
``tests/network/test_realtime_network.py`` and is skipped by default.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from pulsar_contracts import QuoteLevel

from pulsar_data.errors import EgressViolation, FetchError
from pulsar_data.netguard import SafeHTTPSession
from pulsar_data.realtime.collector import (
    EastmoneyQuoteSource,
    FailoverQuoteSource,
    RawSnapshot,
    SinaQuoteSource,
    build_default_source,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")

# --------------------------------------------------------------------------
# canned payloads (mirroring the live endpoints byte-for-byte in structure)
# --------------------------------------------------------------------------


def _sina_stock_line(
    key: str,
    *,
    name: str = "示例股份",
    open_: str = "10.000",
    prev_close: str = "9.900",
    last: str = "10.100",
    high: str = "10.200",
    low: str = "9.950",
    volume: str = "1234500",
    amount: str = "12456789.000",
    date: str = "2026-09-30",
    time_: str = "14:59:30",
    bids: tuple[tuple[str, str], ...] = (
        ("10.100", "100"),
        ("10.090", "200"),
        ("10.080", "300"),
        ("10.070", "400"),
        ("10.060", "500"),
    ),
    asks: tuple[tuple[str, str], ...] = (
        ("10.110", "110"),
        ("10.120", "120"),
        ("10.130", "130"),
        ("10.140", "140"),
        ("10.150", "150"),
    ),
) -> str:
    """Build one Sina ``var hq_str_...`` line in the verified stock layout.

    ``bids``/``asks`` are (price, volume) pairs; the bid1/ask1 preview
    fields 6/7 are derived from the book exactly the way the live feed
    repeats them (the parser requires that repetition).
    """
    bid1, ask1 = bids[0][0], asks[0][0]
    book = ",".join(f"{vol},{price}" for price, vol in bids)
    book += "," + ",".join(f"{vol},{price}" for price, vol in asks)
    return (
        f'var hq_str_{key}="{name},{open_},{prev_close},{last},{high},{low},'
        f'{bid1},{ask1},{volume},{amount},{book},{date},{time_},00,D|3600|123.00";'
    )


SINA_BODY = "\n".join(
    [
        _sina_stock_line("sh600519", name="贵州茅台", last="1258.620", volume="3833098", amount="4797246636.000"),
        _sina_stock_line("sz000001", name="平安银行", last="11.570"),
        # suspended / unknown symbols come back as empty quotes
        'var hq_str_sz000651="";',
        # a line that is not wanted at all
        _sina_stock_line("sh600000", name="浦发银行"),
    ]
).encode("gbk")


class FakeResponse:
    def __init__(self, content: bytes, text: str | None = None, status_code: int = 200) -> None:
        self.content = content
        self.text = text if text is not None else content.decode("gbk", errors="replace")
        self.status_code = status_code


class RecordingSession:
    """A fake ``requests.Session`` that records calls and replays a response."""

    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


# --------------------------------------------------------------------------
# Sina
# --------------------------------------------------------------------------


class TestSinaParse:
    def test_parses_wanted_symbols_only(self):
        found = SinaQuoteSource.parse(SINA_BODY, ["SH600519", "SZ000001", "SZ000651"])
        assert set(found) == {"SH600519", "SZ000001"}  # suspended SZ000651 omitted

    def test_snapshot_fields_and_book(self):
        found = SinaQuoteSource.parse(SINA_BODY, ["SH600519"])
        snap = found["SH600519"]
        assert snap.symbol == "SH600519"
        assert snap.last_price == pytest.approx(1258.62)
        assert snap.volume == pytest.approx(3833098)
        assert snap.amount == pytest.approx(4797246636)
        # default template values (the canned line uses its own book)
        assert [level.price for level in snap.bids] == [10.1, 10.09, 10.08, 10.07, 10.06]
        assert [level.volume for level in snap.bids] == [100, 200, 300, 400, 500]
        assert [level.price for level in snap.asks] == [10.11, 10.12, 10.13, 10.14, 10.15]
        assert [level.volume for level in snap.asks] == [110, 120, 130, 140, 150]
        assert snap.ts == datetime(2026, 9, 30, 14, 59, 30, tzinfo=_SHANGHAI)

    def test_real_layout_cross_check(self):
        """The parser accepts a byte-exact real Sina line (recorded 2026-09-30)."""
        real = (
            'var hq_str_sh600519="贵州茅台,1239.530,1235.580,1258.620,1268.000,1236.050,'
            "1258.620,1258.650,3833098,4797246636.000,1445,1258.620,100,1258.440,100,"
            "1258.160,200,1258.050,4100,1258.000,200,1258.650,300,1258.660,100,1258.680,"
            '200,1258.690,8000,1258.750,2026-09-30,15:34:59,00,D|3600|4531032.00";'
        ).encode("gbk")
        found = SinaQuoteSource.parse(real, ["SH600519"])
        assert set(found) == {"SH600519"}
        snap = found["SH600519"]
        assert snap.last_price == pytest.approx(1258.62)
        assert snap.volume == pytest.approx(3833098)
        assert snap.amount == pytest.approx(4797246636)
        assert [level.price for level in snap.bids] == [1258.62, 1258.44, 1258.16, 1258.05, 1258.0]
        assert [level.volume for level in snap.bids] == [1445, 100, 100, 200, 4100]
        assert [level.price for level in snap.asks] == [1258.65, 1258.66, 1258.68, 1258.69, 1258.75]
        assert [level.volume for level in snap.asks] == [200, 300, 100, 200, 8000]
        assert snap.ts == datetime(2026, 9, 30, 15, 34, 59, tzinfo=_SHANGHAI)

    def test_non_stock_layout_rejected(self):
        """Index lines (different layout) must not be force-parsed as stocks."""
        index_line = (
            'var hq_str_sh000001="上证指数,3200.000,3190.000,3205.000,3210.000,3185.000,'
            "4145602,679398992445,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"
            '2026-09-30,15:00:00,00";'
        ).encode("gbk")
        assert SinaQuoteSource.parse(index_line, ["SH000001"]) == {}

    def test_zero_last_price_rejected(self):
        zero_book = tuple(("0.000", "0") for _ in range(5))
        line = _sina_stock_line("sz000651", last="0.000", bids=zero_book, asks=zero_book)
        assert SinaQuoteSource.parse(line.encode("gbk"), ["SZ000651"]) == {}


class TestSinaPoll:
    def _source(self, response: FakeResponse) -> tuple[SinaQuoteSource, RecordingSession]:
        session = RecordingSession(response)
        return SinaQuoteSource(session=session, min_interval=0.0), session

    def test_url_headers_and_symbol_mapping(self):
        source, session = self._source(FakeResponse(SINA_BODY))
        found = source.poll(["600519", "SZ000651"])
        url, kwargs = session.calls[0]
        assert url == "https://hq.sinajs.cn/list=sh600519,sz000651"
        assert kwargs["headers"]["Referer"] == "https://finance.sina.com.cn"
        assert set(found) == {"SH600519"}  # canonical form out; suspended omitted

    def test_default_session_is_egress_guarded(self):
        source = SinaQuoteSource()
        assert isinstance(source._session, SafeHTTPSession)

    def test_private_target_refused(self, monkeypatch):
        """The egress guard refuses a Sina host resolving into a private net."""
        import socket

        def fake_getaddrinfo(host, *args, **kwargs):
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 0))]

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        import pulsar_data.netguard as netguard

        monkeypatch.setattr(netguard, "_resolution_cache", {})
        source = SinaQuoteSource()
        with pytest.raises(EgressViolation):
            source.poll(["SH600519"])

    def test_http_error_becomes_fetch_error(self):
        source, _ = self._source(FakeResponse(b"", status_code=403))
        with pytest.raises(FetchError):
            source.poll(["SH600519"])


# --------------------------------------------------------------------------
# Eastmoney
# --------------------------------------------------------------------------


def _em_payload(rows: list[dict]) -> str:
    import json

    return json.dumps({"rc": 0, "rt": 11, "data": {"total": len(rows), "diff": rows}})


EM_ROWS = [
    {"f2": 1258.62, "f5": 38331, "f6": 4797246636.0, "f12": "600519", "f13": 1},
    {"f2": 11.57, "f5": 1045357, "f6": 1205814857.64, "f12": "000001", "f13": 0},
    {"f2": "-", "f5": 0, "f6": 0.0, "f12": "300750", "f13": 0},  # suspended → "-"
]

POLL_TS = datetime(2026, 9, 30, 15, 0, 0, tzinfo=_SHANGHAI)


class TestEastmoneyParse:
    def test_fields_and_volume_conversion(self):
        found = EastmoneyQuoteSource.parse(
            _em_payload(EM_ROWS), ["SH600519", "SZ000001", "SZ300750"], poll_ts=POLL_TS
        )
        assert set(found) == {"SH600519", "SZ000001"}  # "-" price omitted
        snap = found["SH600519"]
        assert snap.last_price == pytest.approx(1258.62)
        assert snap.volume == pytest.approx(3833100)  # lots × 100
        assert snap.amount == pytest.approx(4797246636)
        assert snap.bids == () and snap.asks == ()  # documented: book-less fallback
        assert snap.ts == POLL_TS  # no per-quote clock on this field set

    def test_f124_timestamp_used_when_plausible(self):
        rows = [dict(EM_ROWS[0], f124=1759221600)]  # 2025-09-30 16:40:00 +08:00
        found = EastmoneyQuoteSource.parse(_em_payload(rows), ["SH600519"], poll_ts=POLL_TS)
        assert found["SH600519"].ts == datetime(2025, 9, 30, 16, 40, 0, tzinfo=_SHANGHAI)

    def test_garbage_payload_raises_fetch_error(self):
        with pytest.raises(FetchError):
            EastmoneyQuoteSource.parse("<html>502</html>", ["SH600519"], poll_ts=POLL_TS)


class TestEastmoneyPoll:
    def test_secid_mapping_and_beijing_symbol(self):
        session = RecordingSession(FakeResponse(b"", text=_em_payload([])))
        source = EastmoneyQuoteSource(session=session, min_interval=0.0, clock=lambda: POLL_TS)
        source.poll(["SH600519", "SZ000001", "BJ920000"])
        url, _ = session.calls[0]
        assert "secids=1.600519%2C0.000001%2C0.920000" in url or (
            "secids=1.600519,0.000001,0.920000" in url
        )


# --------------------------------------------------------------------------
# failover routing
# --------------------------------------------------------------------------


class FlakySource:
    """A source whose poll raises on demand."""

    def __init__(self, name: str, quotes: dict[str, RawSnapshot] | None = None) -> None:
        self.name = name
        self._quotes = quotes or {}
        self._breaker = None  # failover tolerates sources without breakers

    def fail_with(self) -> "FlakySource":
        self._quotes = None
        return self

    def poll(self, symbols):
        if self._quotes is None:
            raise FetchError(f"{self.name} exploded")
        return {s: self._quotes[s] for s in symbols if s in self._quotes}

    def snapshot(self, symbol: str, price: float) -> None:
        self._quotes[symbol] = RawSnapshot(
            symbol=symbol,
            ts=datetime(2026, 9, 30, 14, 0, 0, tzinfo=_SHANGHAI),
            last_price=price,
            volume=1.0,
            amount=price,
        )


class TestFailover:
    def test_primary_used_when_healthy(self):
        primary, fallback = FlakySource("primary"), FlakySource("fallback")
        primary.snapshot("SH600519", 10.0)
        fallback.snapshot("SH600519", 20.0)
        router = FailoverQuoteSource([primary, fallback])
        assert router.poll(["SH600519"])["SH600519"].last_price == 10.0
        assert router.last_source == "primary"
        assert router.degradations == []

    def test_failure_routes_to_fallback_and_records_degradation(self):
        primary, fallback = FlakySource("primary"), FlakySource("fallback")
        fallback.snapshot("SH600519", 20.0)
        router = FailoverQuoteSource([primary.fail_with(), fallback])
        assert router.poll(["SH600519"])["SH600519"].last_price == 20.0
        assert router.last_source == "fallback"
        assert len(router.degradations) == 1
        degradation = router.degradations[0]
        assert degradation.from_source == "primary"
        assert degradation.to_source == "fallback"
        assert "poll failed" in degradation.reason

    def test_all_sources_failed_raises(self):
        primary, fallback = FlakySource("primary"), FlakySource("fallback")
        router = FailoverQuoteSource([primary.fail_with(), fallback.fail_with()])
        with pytest.raises(FetchError):
            router.poll(["SH600519"])

    def test_open_breaker_skips_source(self):
        from pulsar_data.ratelimit import CircuitBreaker

        class Breakered:
            name = "breakered"

            def __init__(self) -> None:
                self._breaker = CircuitBreaker(threshold=1, cooldown=60.0)

            def poll(self, symbols):
                raise AssertionError("must not be polled while the breaker is open")

        fallback = FlakySource("fallback")
        fallback.snapshot("SH600519", 20.0)
        breakered = Breakered()
        breakered._breaker.record_failure()
        router = FailoverQuoteSource([breakered, fallback])
        assert router.poll(["SH600519"])["SH600519"].last_price == 20.0
        assert router.degradations[0].reason.startswith("skipped: circuit breaker open")

    def test_default_routing_sina_then_eastmoney(self):
        router = build_default_source()
        names = [source.name for source in router.sources]
        assert names == ["sina", "eastmoney"]
