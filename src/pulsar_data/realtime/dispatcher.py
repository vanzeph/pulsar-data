"""Best-effort snapshot subscription dispatching.

This is the heart of the realtime channel (task D5): a poll loop pulls
batches of quotes from a :class:`~pulsar_data.realtime.collector.SnapshotSource`
and fans them out to subscribers with the semantics the design fixes for
Paper / Live:

* **best effort** — late or missing snapshots never raise; they surface as
  marker events (:class:`~pulsar_data.realtime.events.StreamEvent`) and as
  gaps in ``Snapshot.seq``;
* **per-symbol monotonic sequence** — every poll cycle consumes exactly
  one ``seq`` per subscribed symbol, delivered or not, so a delivered
  ``seq`` that skips values marks exactly the cycles that went missing;
* **timestamps** — every snapshot carries the source quote time
  (Asia/Shanghai); a quote arriving out of order (older than one already
  delivered) is flagged ``LATE`` and still delivered — data is never
  dropped silently;
* **cancelable** — ``subscribe`` returns the contract's
  :class:`~pulsar_contracts.common.Subscription`; ``unsubscribe()`` is
  idempotent and the poll thread stops once nobody is listening.

The cycle logic lives in :meth:`SnapshotDispatcher.ingest`, a synchronous
method free of threading, so the normal / late / missing semantics are
unit-tested deterministically without any thread or network.
"""

from __future__ import annotations

import itertools
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from zoneinfo import ZoneInfo

from pulsar_contracts import Snapshot, Subscription

from ..errors import ConfigurationError
from ..symbols import to_canonical_symbol
from .collector import RawSnapshot, SnapshotSource
from .events import EventKind, StreamEvent

logger = logging.getLogger("pulsar_data.realtime")

__all__ = ["SnapshotDispatcher", "SubscriptionHandle"]

_SHANGHAI = ZoneInfo("Asia/Shanghai")

#: Type of the port-level callback: receives every delivered ``Snapshot``.
SnapshotCallback = Callable[[Snapshot], None]
#: Type of the extended callback: receives every :class:`StreamEvent`.
EventCallback = Callable[[StreamEvent], None]


def _now() -> datetime:
    return datetime.now(tz=_SHANGHAI)


class _SymbolState:
    """Per-symbol dispatch state: the seq consumed so far and the newest ts delivered."""

    __slots__ = ("last_seq", "last_ts")

    def __init__(self) -> None:
        self.last_seq = 0  # seq 0 means "nothing delivered yet"
        self.last_ts: datetime | None = None


class _Subscriber:
    """One active subscription: its symbol set plus one of the two callbacks."""

    __slots__ = ("symbols", "on_snapshot", "on_event")

    def __init__(
        self,
        symbols: frozenset[str],
        on_snapshot: SnapshotCallback | None,
        on_event: EventCallback | None,
    ) -> None:
        self.symbols = symbols
        self.on_snapshot = on_snapshot
        self.on_event = on_event


class SubscriptionHandle:
    """The :class:`Subscription` the dispatcher hands out.

    Implements the port contract (``unsubscribe``, idempotent) and adds
    read-only introspection: ``symbols`` and ``active``.
    """

    def __init__(self, dispatcher: SnapshotDispatcher, token: int, symbols: frozenset[str]) -> None:
        self._dispatcher = dispatcher
        self._token = token
        self._symbols = symbols

    @property
    def symbols(self) -> frozenset[str]:
        """The canonical symbols this subscription asked for."""
        return self._symbols

    @property
    def active(self) -> bool:
        """True until ``unsubscribe`` is called."""
        return self._token in self._dispatcher._subscribers  # noqa: SLF001 - deliberate anchor

    def unsubscribe(self) -> None:
        """Stop delivery for this subscription; safe to call repeatedly."""
        self._dispatcher._remove_subscriber(self._token)


class SnapshotDispatcher:
    """Fan quotes from one source out to any number of subscriptions.

    Parameters
    ----------
    source:
        any :class:`SnapshotSource`; typically
        :func:`~pulsar_data.realtime.collector.build_default_source`
        (Sina primary, Eastmoney fallback, guarded egress + rate limits).
    poll_interval:
        seconds between polls while the source is healthy.
    max_poll_interval:
        ceiling for the failure backoff — after consecutive failed cycles
        the interval doubles up to this bound and resets on the first
        success (loop-level counterpart of D1's exponential backoff).
    clock:
        injectable "now" (Asia/Shanghai) for the cycle timestamps.
    auto_start:
        when True (default) the first ``subscribe`` spawns the background
        poll thread — the port behavior.  Hosts that want to drive cycles
        themselves (tests, deterministic replay) pass False and call
        :meth:`ingest` directly.
    """

    def __init__(
        self,
        source: SnapshotSource,
        *,
        poll_interval: float = 3.0,
        max_poll_interval: float = 60.0,
        clock: Callable[[], datetime] = _now,
        auto_start: bool = True,
    ) -> None:
        if poll_interval <= 0:
            raise ConfigurationError("poll_interval must be positive")
        self._source = source
        self._poll_interval = poll_interval
        self._max_poll_interval = max(max_poll_interval, poll_interval)
        self._clock = clock
        self._auto_start = auto_start
        self._subscribers: dict[int, _Subscriber] = {}
        self._states: dict[str, _SymbolState] = {}
        self._tokens = itertools.count(1)
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._closed = False
        self.callback_errors = 0

    # ------------------------------------------------------------ public API
    def subscribe(self, symbols: Sequence[str], on_snapshot: SnapshotCallback) -> Subscription:
        """Start best-effort snapshot delivery for ``symbols``.

        ``on_snapshot`` receives every delivered :class:`Snapshot` for the
        requested symbols — including late ones; missing cycles produce no
        call here (use :meth:`subscribe_events` to observe the markers).
        Exceptions raised by the callback are counted
        (:attr:`callback_errors`) and never propagate.
        """
        return self._subscribe(symbols, on_snapshot=on_snapshot, on_event=None)

    def subscribe_events(self, symbols: Sequence[str], on_event: EventCallback) -> Subscription:
        """Like :meth:`subscribe` but with the full marker stream.

        ``on_event`` sees every :class:`StreamEvent`: ``SNAPSHOT`` and
        ``LATE`` (snapshot attached) plus ``MISSING`` markers — this is how
        lateness/missingness is made explicitly visible to consumers.
        """
        return self._subscribe(symbols, on_snapshot=None, on_event=on_event)

    @property
    def active_symbols(self) -> frozenset[str]:
        """Canonical symbols at least one subscriber currently listens to."""
        with self._lock:
            union: set[str] = set()
            for subscriber in self._subscribers.values():
                union |= subscriber.symbols
            return frozenset(union)

    def close(self) -> None:
        """Stop the poll thread and drop every subscriber (idempotent)."""
        self._closed = True
        with self._lock:
            self._subscribers.clear()
        self._wake.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(2.0, self._max_poll_interval))
        self._thread = None

    # ------------------------------------------------------------- internals
    def _subscribe(
        self,
        symbols: Sequence[str],
        *,
        on_snapshot: SnapshotCallback | None,
        on_event: EventCallback | None,
    ) -> Subscription:
        if self._closed:
            raise ConfigurationError("dispatcher is closed")
        wanted = frozenset(to_canonical_symbol(s) for s in symbols)
        if not wanted:
            raise ConfigurationError("subscribe needs at least one symbol")
        with self._lock:
            token = next(self._tokens)
            self._subscribers[token] = _Subscriber(wanted, on_snapshot, on_event)
            for symbol in wanted:
                self._states.setdefault(symbol, _SymbolState())
        self._ensure_pump()
        return SubscriptionHandle(self, token, wanted)

    def _remove_subscriber(self, token: int) -> None:
        with self._lock:
            self._subscribers.pop(token, None)
            if not self._subscribers:
                self._states.clear()  # fresh seq space on the next subscription
        self._wake.set()

    def _ensure_pump(self) -> None:
        if not self._auto_start:
            return  # manual mode: the host drives cycles through ingest()
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._wake.clear()
            self._thread = threading.Thread(
                target=self._run, name="pulsar-realtime-pump", daemon=True
            )
            self._thread.start()

    def _run(self) -> None:
        """The poll loop (the only blocking piece; everything else is testable)."""
        failures = 0
        while not self._closed:
            symbols = sorted(self.active_symbols)
            if not symbols:
                return  # nobody listening: pump exits until re-subscription
            error = ""
            quotes: dict[str, RawSnapshot] = {}
            try:
                quotes = self._source.poll(symbols)
            except Exception as exc:  # noqa: BLE001 - the loop must survive source errors
                error = f"source error: {exc!r}"
                failures += 1
            else:
                failures = 0
            try:
                self.ingest(quotes, error=error)
            except Exception:  # noqa: BLE001 - defensive: ingest is designed not to raise
                logger.exception("snapshot ingest failed unexpectedly")
            interval = (
                min(self._max_poll_interval, self._poll_interval * (2**failures))
                if failures
                else self._poll_interval
            )
            self._wake.wait(interval)

    # ------------------------------------------------------------- the cycle
    def ingest(
        self,
        quotes: Mapping[str, RawSnapshot],
        *,
        cycle_ts: datetime | None = None,
        error: str = "",
    ) -> list[StreamEvent]:
        """Run one poll cycle synchronously and return the emitted events.

        This is the deterministic core: it assigns each active symbol the
        next ``seq`` (consuming it whether or not a quote arrived), builds
        and delivers the ``Snapshot`` objects, flags late arrivals, and
        emits ``MISSING`` markers for symbols without a usable quote.  It
        never raises towards consumers: unusable quotes (bad prices,
        contract violations) degrade into ``MISSING`` markers with the
        reason attached.
        """
        cycle_ts = cycle_ts or self._clock()
        if cycle_ts.tzinfo is None:
            cycle_ts = cycle_ts.replace(tzinfo=_SHANGHAI)
        events: list[StreamEvent] = []
        with self._lock:
            subscribers = list(self._subscribers.values())
        for symbol in sorted(self.active_symbols):
            state = self._states.get(symbol)
            if state is None:
                continue
            seq = state.last_seq + 1
            state.last_seq = seq
            event = self._event_for(symbol, seq, cycle_ts, quotes.get(symbol), error, state)
            events.append(event)
            self._dispatch(subscribers, event)
        return events

    def _event_for(
        self,
        symbol: str,
        seq: int,
        cycle_ts: datetime,
        quote: RawSnapshot | None,
        error: str,
        state: _SymbolState,
    ) -> StreamEvent:
        if quote is None:
            return StreamEvent(
                kind=EventKind.MISSING,
                symbol=symbol,
                seq=seq,
                ts=cycle_ts,
                reason=error or "no usable quote returned for symbol",
            )
        try:
            snapshot = Snapshot(
                symbol=symbol,
                ts=quote.ts,
                seq=seq,
                last_price=quote.last_price,
                volume=quote.volume,
                amount=quote.amount,
                bids=quote.bids,
                asks=quote.asks,
            )
        except Exception as exc:  # noqa: BLE001 - defensive: bad quotes become markers
            return StreamEvent(
                kind=EventKind.MISSING,
                symbol=symbol,
                seq=seq,
                ts=cycle_ts,
                reason=f"quote rejected by contract validation: {exc!r}",
            )
        if state.last_ts is not None and snapshot.ts < state.last_ts:
            return StreamEvent(
                kind=EventKind.LATE,
                symbol=symbol,
                seq=seq,
                ts=snapshot.ts,
                snapshot=snapshot,
                reason=f"quote ts {snapshot.ts.isoformat()} older than last delivered "
                f"{state.last_ts.isoformat()}",
            )
        state.last_ts = snapshot.ts
        return StreamEvent(
            kind=EventKind.SNAPSHOT, symbol=symbol, seq=seq, ts=snapshot.ts, snapshot=snapshot
        )

    def _dispatch(self, subscribers: list[_Subscriber], event: StreamEvent) -> None:
        for subscriber in subscribers:
            if event.symbol not in subscriber.symbols:
                continue
            if subscriber.on_snapshot is not None and event.snapshot is not None:
                self._guard(subscriber.on_snapshot, event.snapshot)
            if subscriber.on_event is not None:
                self._guard(subscriber.on_event, event)

    def _guard(self, callback: Callable[..., None], payload: object) -> None:
        """Invoke a consumer callback, counting instead of propagating failures."""
        try:
            callback(payload)  # type: ignore[arg-type]
        except Exception:  # noqa: BLE001 - consumer code must not kill the pump
            self.callback_errors += 1
            logger.exception("snapshot consumer callback raised; continuing")
