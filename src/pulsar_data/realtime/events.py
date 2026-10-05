"""Marker events for the realtime snapshot channel.

The ``MarketDataPort.subscribe`` contract is best-effort by design: late or
missing snapshots never raise, and consumers detect them through gaps in
``Snapshot.seq``.  This module makes the same signals *explicitly visible*:
the dispatcher classifies every poll cycle per symbol into one of three
event kinds and offers them through ``subscribe_events`` —
``pulsar-contracts`` deliberately carries no marker type on the wire, so
the marker stream lives here, data-side, without touching the port
contract.

* :attr:`EventKind.SNAPSHOT` — an on-time snapshot was delivered;
* :attr:`EventKind.LATE` — a snapshot arrived whose quote timestamp is
  older than what was already delivered for that symbol (out-of-order
  upstream).  The snapshot is still delivered (data is never dropped
  silently) but flagged;
* :attr:`EventKind.MISSING` — a subscribed symbol produced no usable
  quote this cycle (suspended upstream, dropped from the response, or the
  whole poll failed).  No ``Snapshot`` exists to deliver; the event itself
  is the marker, and the cycle's sequence number is consumed so the gap
  stays visible in ``seq`` space too.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pulsar_contracts import Snapshot

__all__ = ["EventKind", "StreamEvent"]


class EventKind(enum.StrEnum):
    """Classification of one dispatcher event for one symbol."""

    SNAPSHOT = "snapshot"
    LATE = "late"
    MISSING = "missing"


@dataclass(frozen=True)
class StreamEvent:
    """One marker/delivery event emitted by the snapshot dispatcher.

    ``seq`` is the per-symbol monotonic sequence number the event occupies
    (consumed whether or not a snapshot was delivered, so gaps in delivered
    ``seq`` values line up exactly with :attr:`EventKind.MISSING` events).
    ``ts`` is the quote timestamp for delivered snapshots and the poll
    cycle time (Asia/Shanghai) for markers; ``snapshot`` is attached for
    ``SNAPSHOT`` and ``LATE`` events, ``None`` for ``MISSING``.
    """

    kind: EventKind
    symbol: str
    seq: int
    ts: datetime
    snapshot: "Snapshot | None" = None
    reason: str = ""
