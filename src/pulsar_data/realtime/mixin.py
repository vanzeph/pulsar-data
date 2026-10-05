"""Composing ``subscribe`` onto the read-side ``MarketDataPort``.

Junction point with the D3 query-layer task (deliberately a *separate*
module so the two tasks never touch the same file): D3's
``LakeMarketDataPort`` implements ``fetch_bars`` / ``calendar`` /
``list_instruments`` / ``fetch_corporate_actions`` over the DuckDB query
layer and leaves ``subscribe`` raising ``NotImplementedError`` by design.
The realtime channel ships here as a mixin, so the full port is assembled
by composition with the mixin first in the MRO::

    from pulsar_data.port import LakeMarketDataPort          # D3 (read side)
    from pulsar_data.realtime import RealtimeSubscriptionMixin

    class MarketDataService(RealtimeSubscriptionMixin, LakeMarketDataPort):
        pass

    service = MarketDataService(lake_dir)
    subscription = service.subscribe(["SH600519"], on_snapshot)

Once D3 merges, ``pulsar-app`` wires exactly this class; nothing in either
task needs to change.  Until then the mixin composes with *any* class
exposing the read-side methods (tests use a stub).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from pulsar_contracts import Snapshot, Subscription

from .collector import SnapshotSource, build_default_source
from .dispatcher import EventCallback, SnapshotCallback, SnapshotDispatcher

__all__ = ["RealtimeSubscriptionMixin"]


class RealtimeSubscriptionMixin:
    """Best-effort ``MarketDataPort.subscribe`` over the realtime channel.

    Intended to be mixed into a read-side port implementation (see the
    module docstring for the D3 junction).  Subclasses may override the
    class attributes or simply set the instance attributes before the
    first ``subscribe`` call:

    * ``realtime_source`` — a :class:`SnapshotSource` (defaults to the
      declared routing: Sina primary, Eastmoney fallback);
    * ``realtime_poll_interval`` — seconds between polls (default 3).
    """

    realtime_source: SnapshotSource | None = None
    realtime_poll_interval: float = 3.0

    #: Lazily created :class:`SnapshotDispatcher`; one per port instance.
    _realtime_dispatcher: SnapshotDispatcher | None = None

    def subscribe(
        self, symbols: Sequence[str], on_snapshot: Callable[[Snapshot], None]
    ) -> Subscription:
        """Start best-effort snapshot delivery for ``symbols``.

        Late or missing snapshots never raise; gaps in ``Snapshot.seq``
        mark the missing cycles.  Use :meth:`subscribe_events` to observe
        the explicit markers.
        """
        return self._dispatcher().subscribe(symbols, on_snapshot)

    def subscribe_events(self, symbols: Sequence[str], on_event: EventCallback) -> Subscription:
        """Extended subscription: the full marker stream is visible here."""
        return self._dispatcher().subscribe_events(symbols, on_event)

    def close_realtime(self) -> None:
        """Stop the realtime poll thread (read-side methods stay usable)."""
        dispatcher = self._realtime_dispatcher
        if dispatcher is not None:
            dispatcher.close()
        self._realtime_dispatcher = None

    def _dispatcher(self) -> SnapshotDispatcher:
        if self._realtime_dispatcher is None:
            source: SnapshotSource = (
                self.realtime_source
                if self.realtime_source is not None
                else build_default_source()
            )
            self._realtime_dispatcher = SnapshotDispatcher(
                source, poll_interval=self.realtime_poll_interval
            )
        return self._realtime_dispatcher

    # keep static checkers honest: the mixin alone is not a full port
    _READ_SIDE_METHODS = ("list_instruments", "fetch_bars", "fetch_corporate_actions", "calendar")

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        missing = [name for name in cls._READ_SIDE_METHODS if not hasattr(cls, name)]
        if missing:
            # a friendly reminder at class-definition time, not a runtime crash
            import warnings

            warnings.warn(
                f"{cls.__name__} mixes in realtime subscribe() but lacks the read-side "
                f"methods {missing}; compose with the D3 read-side port "
                "(pulsar_data.port.LakeMarketDataPort) for the full contract.",
                stacklevel=2,
            )
