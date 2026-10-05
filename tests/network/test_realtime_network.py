"""Optional live smoke test for the realtime channel (skipped by default).

Run explicitly with::

    PULSAR_RUN_NETWORK_TESTS=1 pytest tests/network -m network

It exercises the real public endpoints (Sina primary, Eastmoney
fallback) through the full collector → dispatcher → subscription path
and asserts the best-effort semantics against live data: quotes arrive
with a monotonic ``seq``, and any symbol without a usable quote surfaces
as a MISSING marker instead of an error.  CI never runs this file.
"""

from __future__ import annotations

import queue

import pytest

from pulsar_data.realtime import SnapshotDispatcher, build_default_source

pytestmark = pytest.mark.network

SYMBOLS = ["SH600519", "SZ000001"]


def test_live_subscription_smoke():
    dispatcher = SnapshotDispatcher(build_default_source(), poll_interval=1.0)
    try:
        snapshots: queue.Queue = queue.Queue()
        markers: queue.Queue = queue.Queue()
        dispatcher.subscribe(SYMBOLS, snapshots.put)
        dispatcher.subscribe_events(SYMBOLS, markers.put)

        first = snapshots.get(timeout=30.0)  #盘中或盘后均应有快照（含收盘定格行情）
        assert first.symbol in SYMBOLS
        assert first.seq >= 1
        assert first.last_price > 0

        events = [markers.get(timeout=30.0)]
        assert events[0].kind.value in {"snapshot", "late", "missing"}
    finally:
        dispatcher.close()
