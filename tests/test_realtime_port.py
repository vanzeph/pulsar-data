"""Composition of the realtime ``subscribe`` onto the read-side port.

The D3 task ships the read-side ``MarketDataPort`` (``fetch_bars`` /
``calendar`` / ``list_instruments`` / ``fetch_corporate_actions``) in
``pulsar_data.port`` with ``subscribe`` deliberately unimplemented.  This
task provides :class:`RealtimeSubscriptionMixin`; the tests here prove
the two compose into a full contract implementation — using a stub for
the read side so this file never depends on D3's (not yet merged)
module, exactly the coordination the task boundary demands.
"""

from __future__ import annotations

import queue
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from pulsar_contracts import (
    AdjustMode,
    CorporateAction,
    Freq,
    Instrument,
    MarketDataPort,
    Snapshot,
)
from pandas import DataFrame

from pulsar_data.realtime import RealtimeSubscriptionMixin, SnapshotDispatcher
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.events import EventKind

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class StubReadSide:
    """Minimal stand-in for D3's ``LakeMarketDataPort`` read methods."""

    def list_instruments(self, as_of: date) -> list[Instrument]:
        return []

    def fetch_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        freq: Freq,
        adjust: AdjustMode,
    ) -> DataFrame:
        return DataFrame()

    def fetch_corporate_actions(self, symbol: str) -> list[CorporateAction]:
        return []

    def calendar(self, start: date, end: date) -> list[date]:
        return []


class StaticSource:
    """One in-memory quote per symbol, constant across polls."""

    name = "static"

    def __init__(self, quotes: dict[str, RawSnapshot]) -> None:
        self.quotes = quotes

    def poll(self, symbols):
        return {s: self.quotes[s] for s in symbols if s in self.quotes}


def raw(symbol: str, price: float) -> RawSnapshot:
    return RawSnapshot(
        symbol=symbol,
        ts=datetime(2026, 9, 30, 14, 0, 0, tzinfo=_SHANGHAI),
        last_price=price,
        volume=100.0,
        amount=price * 100.0,
    )


class MarketDataService(RealtimeSubscriptionMixin, StubReadSide):
    """Exactly the composition pulsar-app will wire once D3 merges."""

    def __init__(self, source: StaticSource) -> None:
        self.realtime_source = source
        self.realtime_poll_interval = 0.005


class TestPortComposition:
    def test_composed_class_satisfies_the_port_contract(self):
        service = MarketDataService(StaticSource({}))
        assert isinstance(service, MarketDataPort)  # runtime-checkable protocol

    def test_subscribe_streams_snapshots(self):
        service = MarketDataService(StaticSource({"SH600519": raw("SH600519", 1258.6)}))
        try:
            received: queue.Queue = queue.Queue()
            subscription = service.subscribe(["SH600519"], received.put)
            first = received.get(timeout=5.0)
            assert isinstance(first, Snapshot)
            assert first.symbol == "SH600519"
            assert first.last_price == 1258.6
            assert first.seq >= 1
            subscription.unsubscribe()
        finally:
            service.close_realtime()

    def test_marker_events_surface_through_the_port_object(self):
        service = MarketDataService(StaticSource({}))  # nothing ever arrives
        try:
            markers: queue.Queue = queue.Queue()
            subscription = service.subscribe_events(["SH600519"], markers.put)
            event = markers.get(timeout=5.0)
            assert event.kind is EventKind.MISSING
            assert event.symbol == "SH600519"
            subscription.unsubscribe()
        finally:
            service.close_realtime()

    def test_dispatcher_is_shared_per_port_instance(self):
        service = MarketDataService(StaticSource({"SH600519": raw("SH600519", 10.0)}))
        try:
            first = service._dispatcher()
            second = service._dispatcher()
            assert first is second
            assert isinstance(first, SnapshotDispatcher)
        finally:
            service.close_realtime()

    def test_mixin_alone_warns_about_missing_read_side(self):
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            class Bare(RealtimeSubscriptionMixin):
                pass

        assert any("read-side" in str(w.message) for w in caught)


class TestJunctionWithRealReadSide:
    """The D3 seam, proven against the merged read-side port (offline).

    D3's ``LakeMarketDataPort`` deliberately raises ``NotImplementedError``
    from ``subscribe``; composing the mixin ahead of it in the MRO must
    override exactly that method while leaving the read side untouched —
    zero edits to either task's files.
    """

    def test_subscribe_overrides_the_read_side_stub(self, tmp_path):
        from pulsar_data.port import LakeMarketDataPort

        class Composed(RealtimeSubscriptionMixin, LakeMarketDataPort):
            pass

        service = Composed(tmp_path / "lake")
        service.realtime_source = StaticSource({"SH600519": raw("SH600519", 10.0)})
        service.realtime_poll_interval = 0.005
        try:
            assert isinstance(service, MarketDataPort)
            assert service.subscribe.__func__ is not LakeMarketDataPort.subscribe
            received: queue.Queue = queue.Queue()
            subscription = service.subscribe(["SH600519"], received.put)
            first = received.get(timeout=5.0)
            assert first.symbol == "SH600519"
            subscription.unsubscribe()
        finally:
            service.close_realtime()
