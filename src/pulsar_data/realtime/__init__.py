"""Realtime snapshot subscription channel (best-effort, Paper / Live).

Task D5 of the data-integration design: poll public free quote endpoints
(Sina primary, Eastmoney fallback — both behind the D1 egress guard and
per-source rate limiting), normalize into the contract's
:class:`~pulsar_contracts.Snapshot`, and fan out to subscriptions with

* per-symbol monotonic ``seq`` consumed every cycle, so gaps mark missing
  snapshots without ever raising, and
* explicit marker events (:class:`StreamEvent`) for late / missing
  snapshots, visible through ``subscribe_events``.

``MarketDataPort.subscribe`` is composed onto the D3 read-side port via
:class:`RealtimeSubscriptionMixin` — see :mod:`pulsar_data.realtime.mixin`
for the junction contract between the two tasks.
"""

from .collector import (
    Degradation,
    EastmoneyQuoteSource,
    FailoverQuoteSource,
    RawSnapshot,
    SinaQuoteSource,
    SnapshotSource,
    build_default_source,
)
from .dispatcher import SnapshotDispatcher, SubscriptionHandle
from .events import EventKind, StreamEvent
from .mixin import RealtimeSubscriptionMixin

__all__ = [
    "Degradation",
    "EastmoneyQuoteSource",
    "EventKind",
    "FailoverQuoteSource",
    "RawSnapshot",
    "RealtimeSubscriptionMixin",
    "SinaQuoteSource",
    "SnapshotDispatcher",
    "SnapshotSource",
    "StreamEvent",
    "SubscriptionHandle",
    "build_default_source",
]
