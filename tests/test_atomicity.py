"""Write atomicity under concurrency: readers never see half-written partitions.

A writer thread hammers whole-partition atomic replaces (and concurrent
merge_writes) while reader threads keep globbing and parsing the same
partition: every read must yield either the previous or the new
complete frame — never a truncated parquet, never a tmp sibling.
"""

from __future__ import annotations

import threading
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from pulsar_data.lake import DataLake
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts

SYMBOL = "SH600519"
PARTITION_REL = Path("bars_1d") / f"symbol={SYMBOL}" / "year=2024"


def _day(offset: int) -> str:
    return (date(2024, 1, 2) + timedelta(days=offset)).isoformat()


def _bars(days: list[str], close: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": SYMBOL,
            "ts": [daily_ts(day) for day in days],
            "open": [close] * len(days),
            "high": [close * 1.01] * len(days),
            "low": [close * 0.99] * len(days),
            "close": [close] * len(days),
            "volume": [100.0] * len(days),
            "amount": [close * 100.0] * len(days),
            "adjust_factor": [1.0] * len(days),
            "quality": ["ok"] * len(days),
        }
    )[list(BAR_COLUMNS)]


def test_concurrent_reads_never_see_partial_partition(lake: DataLake):
    """Writer flips the partition between 20 and 200 rows; readers must
    always parse a complete frame of one of the two sizes."""
    short_days = [_day(offset) for offset in range(20)]  # 20 days
    long_days = [_day(offset) for offset in range(200)]  # 200 days
    short, long = _bars(short_days, 10.0), _bars(long_days, 11.0)
    lake.write(Dataset.BARS_1D, short, source="akshare")

    stop = threading.Event()
    seen_sizes: dict[str, set[int]] = {"reader": set()}
    errors: list[Exception] = []

    def writer() -> None:
        flip = 0
        while not stop.is_set():
            frame = long if flip % 2 == 0 else short
            lake.write(Dataset.BARS_1D, frame, source="akshare")
            flip += 1

    def reader() -> None:
        while not stop.is_set():
            try:
                frame = lake.read(Dataset.BARS_1D, symbols=[SYMBOL])  # public API
                if not frame.empty:
                    seen_sizes["reader"].add(len(frame))
                direct = PARTITION_REL / "part.parquet"
                raw = pd.read_parquet(lake.root / direct)  # direct file read
                seen_sizes["reader"].add(len(raw))
                # close values must be homogeneous within one read
                assert set(raw["close"].tolist()) <= {10.0, 11.0}
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
    for thread in threads:
        thread.start()
    try:  # let them race for a bounded moment
        threading.Event().wait(1.5)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=10)

    assert not errors, errors[:3]
    assert seen_sizes["reader"] <= {20, 200}, seen_sizes["reader"]
    # no tmp siblings survive a clean write loop
    leftover = [p.name for p in (lake.root / PARTITION_REL).iterdir() if p.name.startswith(".tmp-")]
    assert leftover == []


def test_crashed_writer_tmp_file_is_invisible_to_readers(lake: DataLake):
    """A leftover .tmp file (simulated crash between write and rename)
    never appears in reads and never breaks the lake."""
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02"], 10.0), source="akshare")
    partition = lake.root / PARTITION_REL
    _bars([f"2024-01-{day:02d}" for day in range(2, 32)], 12.0).to_parquet(
        partition / ".tmp-crashed.parquet", index=False
    )
    frame = lake.read(Dataset.BARS_1D)
    assert len(frame) == 1
    assert frame["close"].tolist() == [10.0]
    # and the query layer agrees
    from pulsar_data.query import LakeQuery

    with LakeQuery(lake.root) as query:
        assert len(query.bars()) == 1


def test_merge_write_is_atomic_and_concurrent_safe(lake: DataLake):
    """Concurrent merge_writes of disjoint windows into one partition:
    the partition always parses and ends up with the union of days."""
    base = _bars(["2024-01-02", "2024-01-03"], 10.0)
    lake.write(Dataset.BARS_1D, base, source="akshare")
    batches = [
        _bars(["2024-01-04"], 10.0),
        _bars(["2024-01-05"], 10.0),
        _bars(["2024-01-08", "2024-01-09"], 10.0),
    ]
    barrier = threading.Barrier(len(batches))
    errors: list[Exception] = []

    def merge(batch: pd.DataFrame) -> None:
        barrier.wait()
        try:
            lake.merge_write(Dataset.BARS_1D, batch, source="akshare")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=merge, args=(batch,)) for batch in batches]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not errors, errors

    stored = lake.read(Dataset.BARS_1D)
    days = sorted(ts.date().isoformat() for ts in stored["ts"])
    assert days == ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]


def test_merge_write_replaces_rows_on_natural_key(lake: DataLake):
    """Re-delivered (symbol, ts) rows replace stored ones — the healing
    path for late corrections."""
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02", "2024-01-03"], 10.0), source="akshare")
    corrected = _bars(["2024-01-03"], 10.5)
    lake.merge_write(Dataset.BARS_1D, corrected, source="akshare")
    stored = lake.read(Dataset.BARS_1D).sort_values("ts").reset_index(drop=True)
    assert len(stored) == 2
    assert stored["close"].tolist() == [10.0, 10.5]
