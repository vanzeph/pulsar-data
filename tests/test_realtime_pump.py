"""Threaded end-to-end tests: a scripted source feeding a live pump.

Unlike ``test_realtime_dispatcher.py`` (synchronous ``ingest`` calls),
these tests run the real background pump thread against a source whose
poll results change over time, then verify delivery and cancellation
under actual concurrency.
"""

from __future__ import annotations

import queue
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pulsar_data.errors import FetchError
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.dispatcher import SnapshotDispatcher

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class ScriptedSource:
    """Yields one scripted poll result per call, then repeats the last one."""

    name = "scripted"

    def __init__(self, script: list[dict[str, RawSnapshot] | Exception]) -> None:
        self.script = list(script)
        self.lock = threading.Lock()
        self.calls = 0

    def poll(self, symbols):
        with self.lock:
            index = min(self.calls, len(self.script) - 1)
            self.calls += 1
            step = self.script[index]
        if isinstance(step, Exception):
            raise step
        return dict(step)


def snap(symbol: str, minute: int, price: float = 10.0) -> RawSnapshot:
    return RawSnapshot(
        symbol=symbol,
        ts=datetime(2026, 9, 30, 14, minute, 0, tzinfo=_SHANGHAI),
        last_price=price,
        volume=100.0,
        amount=price * 100.0,
    )


def collect(q: queue.Queue, count: int, timeout: float = 5.0) -> list:
    """Collect up to ``count`` items, stopping as soon as they arrived.

    Bounded by design: the pump keeps producing after the scripted part
    repeats, so an "until empty" drain would never terminate.
    """
    items: list = []
    deadline = time.monotonic() + timeout
    while len(items) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            items.append(q.get(timeout=remaining))
        except queue.Empty:
            break
    return items


class TestPumpEndToEnd:
    def test_delivery_survives_source_failures_and_recovers(self):
        script = [
            {"SH600519": snap("SH600519", 1)},
            FetchError("transient outage"),  # cycle 2: total failure
            {"SH600519": snap("SH600519", 3)},  # cycle 3: recovery (repeats)
        ]
        source = ScriptedSource(script)
        dispatcher = SnapshotDispatcher(source, poll_interval=0.005)
        try:
            snapshots: queue.Queue = queue.Queue()
            events: queue.Queue = queue.Queue()
            dispatcher.subscribe(["SH600519"], snapshots.put)
            dispatcher.subscribe_events(["SH600519"], events.put)
            delivered = collect(snapshots, 2)
            markers = collect(events, 3)
            assert [s.seq for s in delivered] == [1, 3]  # cycle 2 went missing
            kinds = [e.kind.value for e in markers]
            assert "missing" in kinds
            missing = [e for e in markers if e.kind.value == "missing"][0]
            assert missing.seq == 2 and "outage" in missing.reason
        finally:
            dispatcher.close()

    def test_unsubscribe_stops_the_stream(self):
        source = ScriptedSource([{"SH600519": snap("SH600519", 1)}])
        dispatcher = SnapshotDispatcher(source, poll_interval=0.005)
        try:
            snapshots: queue.Queue = queue.Queue()
            handle = dispatcher.subscribe(["SH600519"], snapshots.put)
            first = collect(snapshots, 1)
            assert first  # we were delivering
            handle.unsubscribe()
            thread = dispatcher._thread
            if thread is not None:
                thread.join(timeout=2.0)
                assert not thread.is_alive()
            count = snapshots.qsize()
            time.sleep(0.08)
            assert snapshots.qsize() == count  # nothing new after cancel
        finally:
            dispatcher.close()

    def test_close_is_idempotent(self):
        source = ScriptedSource([{"SH600519": snap("SH600519", 1)}])
        dispatcher = SnapshotDispatcher(source, poll_interval=0.005)
        handle = dispatcher.subscribe(["SH600519"], lambda s: None)
        dispatcher.close()
        dispatcher.close()  # must not raise
        assert not handle.active
