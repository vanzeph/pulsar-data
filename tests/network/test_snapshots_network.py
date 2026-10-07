"""Optional live smoke test for the snapshot collect daemon (skipped by default).

Run explicitly with::

    PULSAR_RUN_NETWORK_TESTS=1 pytest tests/network -m network

It drives the *real* declared source routing (Sina primary, Eastmoney
fallback) through the full D5 dispatcher → daemon → lake path for a
couple of cycles, in or out of trading hours (Sina serves the closing
frame after hours), and asserts the accumulated partition family:
rows land in ``snapshots/symbol=…/date=…`` with monotonic in-session
``seq``, and every cycle is represented (delivered or marked).  CI never
runs this file.
"""

from __future__ import annotations

import time

import pytest

from pulsar_data.lake import DataLake
from pulsar_data.schema import Dataset
from pulsar_data.snapshots import SnapshotCollectorDaemon, SnapshotPolicy

pytestmark = pytest.mark.network

SYMBOLS = ["SH600519", "SZ000001"]


def test_live_collect_smoke(tmp_path):
    lake = DataLake(tmp_path / "lake")
    policy = SnapshotPolicy(
        symbols=SYMBOLS, poll_interval_s=2.0, flush_interval_s=1.0, reconnect_gap_threshold_s=60.0
    )
    daemon = SnapshotCollectorDaemon(lake, policy)  # real failover source
    daemon.start()
    try:
        deadline = time.monotonic() + 40.0
        while time.monotonic() < deadline and daemon.stats["rows_written"] < 2:
            time.sleep(0.5)
        assert daemon.stats["rows_written"] >= 2  # at least one flush landed
        frame = lake.read(Dataset.SNAPSHOTS)
        assert not frame.empty
        assert set(frame["symbol"]) <= set(SYMBOLS)
        usable = frame[frame["kind"].isin(("snapshot", "late"))]
        assert not usable.empty  # 盘中或盘后均应有快照（含收盘定格行情）
        for symbol in SYMBOLS:
            rows = frame[frame["symbol"] == symbol].sort_values("seq")
            seqs = list(rows["seq"])
            assert seqs == list(range(1, len(seqs) + 1))  # no silent seq gaps
    finally:
        daemon.stop()
    state = daemon.store.load_state()
    assert state["symbols"]  # persistence watermark advanced
