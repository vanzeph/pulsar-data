"""Deterministic tests for the snapshot dispatcher's best-effort semantics.

Everything here drives :meth:`SnapshotDispatcher.ingest` directly — no
threads, no network — so the normal / late / missing / cancel semantics
(the acceptance core of task D5) are asserted exactly:

* late and missing situations never raise;
* every snapshot carries a per-symbol monotonic ``seq`` and a timestamp;
* missing cycles consume their ``seq`` (gaps stay visible to consumers)
  and emit an explicit ``MISSING`` marker through ``subscribe_events``;
* late arrivals are flagged ``LATE``, still delivered, and never move the
  newest-delivered timestamp backwards;
* ``unsubscribe`` is idempotent and stops delivery.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from pulsar_contracts import Snapshot

from pulsar_data.errors import ConfigurationError
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.dispatcher import SnapshotDispatcher
from pulsar_data.realtime.events import EventKind

_SHANGHAI = ZoneInfo("Asia/Shanghai")

CYCLE = datetime(2026, 9, 30, 14, 30, 0, tzinfo=_SHANGHAI)


class FakeSource:
    """A SnapshotSource whose poll results are scripted per test."""

    name = "fake"

    def __init__(self) -> None:
        self.polled: list[list[str]] = []

    def poll(self, symbols):
        self.polled.append(list(symbols))
        return {}


def raw(symbol: str, minute: int, *, price: float = 10.0, volume: float = 100.0) -> RawSnapshot:
    return RawSnapshot(
        symbol=symbol,
        ts=CYCLE.replace(minute=minute),
        last_price=price,
        volume=volume,
        amount=price * volume,
    )


def make_dispatcher() -> tuple[SnapshotDispatcher, FakeSource]:
    """A manual-cycle dispatcher: subscribers register, the host drives ``ingest``.

    ``auto_start=False`` keeps the background pump out of the picture so
    every assertion below is deterministic (no racing ingest cycles).
    """
    source = FakeSource()
    return SnapshotDispatcher(source, poll_interval=0.01, auto_start=False), source


class Recorder:
    def __init__(self) -> None:
        self.snapshots: list[Snapshot] = []
        self.events: list = []

    def on_snapshot(self, snapshot: Snapshot) -> None:
        self.snapshots.append(snapshot)

    def on_event(self, event) -> None:
        self.events.append(event)


# --------------------------------------------------------------------------
# normal delivery
# --------------------------------------------------------------------------


class TestNormalDelivery:
    def test_snapshots_delivered_with_monotonic_seq_and_ts(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        handle = dispatcher.subscribe(["SH600519", "SZ000001"], recorder.on_snapshot)
        events = dispatcher.ingest({"SH600519": raw("SH600519", 1), "SZ000001": raw("SZ000001", 1)})
        events += dispatcher.ingest({"SH600519": raw("SH600519", 2), "SZ000001": raw("SZ000001", 2)})
        assert handle.active
        assert [(s.symbol, s.seq) for s in recorder.snapshots] == [
            ("SH600519", 1),
            ("SZ000001", 1),
            ("SH600519", 2),
            ("SZ000001", 2),
        ]
        assert all(s.ts.minute in (1, 2) for s in recorder.snapshots)
        assert all(event.kind is EventKind.SNAPSHOT for event in events)

    def test_symbols_are_routed_per_subscription(self):
        dispatcher, _ = make_dispatcher()
        a, b = Recorder(), Recorder()
        dispatcher.subscribe(["SH600519"], a.on_snapshot)
        dispatcher.subscribe(["SZ000001"], b.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 1), "SZ000001": raw("SZ000001", 1)})
        assert [s.symbol for s in a.snapshots] == ["SH600519"]
        assert [s.symbol for s in b.snapshots] == ["SZ000001"]

    def test_non_canonical_input_is_canonicalized(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        assert [s.symbol for s in recorder.snapshots] == ["SH600519"]
        assert dispatcher.active_symbols == {"SH600519"}

    def test_empty_symbol_list_rejected(self):
        dispatcher, _ = make_dispatcher()
        with pytest.raises(ConfigurationError):
            dispatcher.subscribe([], lambda snapshot: None)


# --------------------------------------------------------------------------
# missing markers (the core acceptance semantics)
# --------------------------------------------------------------------------


class TestMissingSemantics:
    def test_missing_symbol_never_raises_and_marker_is_visible(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        events = dispatcher.ingest({})  # the symbol produced nothing
        assert len(events) == 1
        event = events[0]
        assert event.kind is EventKind.MISSING
        assert event.symbol == "SH600519"
        assert event.seq == 1
        assert event.snapshot is None
        assert event.reason  # the reason is attached, not swallowed
        assert recorder.snapshots == []  # nothing to deliver — and no raise

    def test_missing_cycle_consumes_seq_so_gap_is_visible(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        dispatcher.ingest({})  # cycle 2: missing
        dispatcher.ingest({"SH600519": raw("SH600519", 3)})
        seqs = [snapshot.seq for snapshot in recorder.snapshots]
        assert seqs == [1, 3]  # the gap in seq IS the missing marker
        kinds = [e.kind for e in dispatcher.ingest({})]
        assert kinds == [EventKind.MISSING]

    def test_source_error_marks_every_symbol_missing_with_reason(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519", "SZ000001"], recorder.on_snapshot)
        events = dispatcher.ingest({}, error="source error: boom")
        assert [e.kind for e in events] == [EventKind.MISSING, EventKind.MISSING]
        assert all("boom" in e.reason for e in events)
        assert recorder.snapshots == []

    def test_invalid_quote_becomes_marker_not_exception(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        bad = raw("SH600519", 1, price=0.0)  # violates last_price > 0
        events = dispatcher.ingest({"SH600519": bad})
        assert events[0].kind is EventKind.MISSING
        assert "rejected" in events[0].reason
        assert recorder.snapshots == []

    def test_markers_visible_via_subscribe_events(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe_events(["SH600519"], recorder.on_event)
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        dispatcher.ingest({})
        kinds = [event.kind for event in recorder.events]
        assert kinds == [EventKind.SNAPSHOT, EventKind.MISSING]
        assert recorder.events[1].seq == 2


# --------------------------------------------------------------------------
# late snapshots
# --------------------------------------------------------------------------


class TestLateSemantics:
    def test_late_snapshot_delivered_and_flagged(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", minute=5)})
        events = dispatcher.ingest({"SH600519": raw("SH600519", minute=3)})  # out of order
        assert events[0].kind is EventKind.LATE
        assert events[0].snapshot is not None
        assert events[0].reason  # lateness explained
        assert len(recorder.snapshots) == 2  # still delivered: no data dropped
        assert recorder.snapshots[-1].ts.minute == 3

    def test_late_does_not_regress_the_clock(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", minute=5)})
        dispatcher.ingest({"SH600519": raw("SH600519", minute=3)})  # late
        events = dispatcher.ingest({"SH600519": raw("SH600519", minute=4)})
        # minute=4 is still older than the newest delivered (5) → also late
        assert [e.kind for e in events] == [EventKind.LATE]
        events = dispatcher.ingest({"SH600519": raw("SH600519", minute=6)})
        assert [e.kind for e in events] == [EventKind.SNAPSHOT]

    def test_seq_stays_monotonic_across_late_arrivals(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", minute=5)})
        dispatcher.ingest({"SH600519": raw("SH600519", minute=3)})
        dispatcher.ingest({"SH600519": raw("SH600519", minute=6)})
        assert [s.seq for s in recorder.snapshots] == [1, 2, 3]


# --------------------------------------------------------------------------
# consumer isolation & cancellation
# --------------------------------------------------------------------------


class TestConsumerIsolation:
    def test_raising_callback_does_not_break_other_subscribers(self):
        dispatcher, _ = make_dispatcher()
        healthy = Recorder()

        def broken(snapshot: Snapshot) -> None:
            raise RuntimeError("consumer bug")

        dispatcher.subscribe(["SH600519"], broken)
        dispatcher.subscribe(["SH600519"], healthy.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        assert len(healthy.snapshots) == 1
        assert dispatcher.callback_errors == 1

    def test_raising_event_callback_counted_not_raised(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()

        def broken(event) -> None:
            raise RuntimeError("consumer bug")

        dispatcher.subscribe_events(["SH600519"], broken)
        events = dispatcher.ingest({})  # marker event goes to the broken callback
        assert events[0].kind is EventKind.MISSING
        assert dispatcher.callback_errors == 1


class TestUnsubscribe:
    def test_unsubscribe_stops_delivery(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        handle = dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        handle.unsubscribe()
        assert not handle.active
        dispatcher.ingest({"SH600519": raw("SH600519", 2)})
        assert len(recorder.snapshots) == 1

    def test_unsubscribe_is_idempotent(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        handle = dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        handle.unsubscribe()
        handle.unsubscribe()  # must not raise
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        assert recorder.snapshots == []

    def test_seq_state_resets_after_everyone_leaves(self):
        dispatcher, _ = make_dispatcher()
        recorder = Recorder()
        handle = dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 1)})
        handle.unsubscribe()
        dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        dispatcher.ingest({"SH600519": raw("SH600519", 2)})
        assert [s.seq for s in recorder.snapshots] == [1, 1]  # fresh seq space

    def test_pump_thread_stops_when_nobody_listens(self):
        import time

        # this one needs the real auto-starting pump (empty polls are fine)
        source = FakeSource()
        dispatcher = SnapshotDispatcher(source, poll_interval=0.01)
        recorder = Recorder()
        handle = dispatcher.subscribe(["SH600519"], recorder.on_snapshot)
        thread = dispatcher._thread
        assert thread is not None and thread.is_alive()
        handle.unsubscribe()
        thread.join(timeout=2.0)
        assert not thread.is_alive()
        time.sleep(0.05)
        assert dispatcher._thread is None or not dispatcher._thread.is_alive()
