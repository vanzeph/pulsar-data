"""Retention and downsample archiving of raw snapshot partitions.

到期降采样归档 (e.g. 3s raw → 1m aggregate): raw ``snapshots``
partitions strictly older than the policy's ``raw_retention_days`` are
aggregated into the ``snapshots_1m`` family — OHLC over quote
``last_price``, volume/amount as spans of the cumulative day totals,
plus the marker counts so gaps stay visible after the raw rows are
reclaimed — and only after the aggregate partition lands atomically is
the raw partition directory removed.

Ordering gives the task its safety properties:

* **idempotent** — re-aggregating the same raw partition writes a
  byte-identical logical aggregate (whole-partition replace), and a raw
  partition that is already gone is simply skipped; a crash between the
  aggregate write and the raw removal leaves both, and the next run
  finishes the job;
* **never lossy** — the raw partition is deleted only after its archive
  exists; a failed aggregate write aborts that partition (and the
  exception propagates) without touching the raw data;
* **readable throughout** — both families are only ever touched through
  the lake's atomic ``.tmp`` + ``os.replace`` writes, so a concurrent
  DuckDB reader never sees a half-written frame.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ..errors import LakeError
from ..lake import DataLake
from ..realtime.events import EventKind
from ..schema import SNAPSHOT_1M_COLUMNS, Dataset
from .policy import SnapshotPolicy
from .store import ARCHIVE_SOURCE, SnapshotStore

logger = logging.getLogger("pulsar_data.snapshots.archive")

__all__ = ["ArchiveReport", "aggregate_snapshots", "archive_expired"]

_USABLE = (EventKind.SNAPSHOT.value, EventKind.LATE.value)


@dataclass
class ArchiveReport:
    """Outcome of one retention pass."""

    raw_partitions_archived: int = 0
    raw_rows_aggregated: int = 0
    archive_rows_written: int = 0
    raw_bytes_reclaimed: int = 0
    archive_partitions_pruned: int = 0
    skipped: list[str] = field(default_factory=list)
    failures: dict[str, str] = field(default_factory=dict)


def aggregate_snapshots(frame: pd.DataFrame, *, interval_s: float) -> pd.DataFrame:
    """Aggregate raw snapshot rows into fixed intervals, pure function.

    Buckets are left-closed (``ts`` floors to the interval start, the
    same convention as the bar families).  Per symbol × bucket:

    * ``open/high/low/close`` — first / max / min / last quote
      ``last_price`` over *usable* rows (``snapshot``/``late``);
    * ``volume``/``amount`` — span (max − min) of the cumulative day
      totals across usable rows, which yields the interval increment
      without depending on row order;
    * ``samples``/``missing``/``thinned`` — cycle counts by kind, so a
      minute whose quotes were all dropped or missing stays visible as a
      marker row (NaN prices) instead of vanishing.
    """
    if frame.empty:
        return pd.DataFrame(columns=list(SNAPSHOT_1M_COLUMNS))
    data = frame.copy()
    data["bucket"] = data["ts"].dt.floor(f"{interval_s}s")
    usable = data[data["kind"].isin(_USABLE)]
    quote_stats = usable.groupby(["symbol", "bucket"], sort=True).agg(
        open=("last_price", "first"),
        high=("last_price", "max"),
        low=("last_price", "min"),
        close=("last_price", "last"),
        volume_max=("volume", "max"),
        volume_min=("volume", "min"),
        amount_max=("amount", "max"),
        amount_min=("amount", "min"),
        samples=("kind", "size"),
    )
    kind_counts = data.groupby(["symbol", "bucket", "kind"], sort=True).size().unstack(fill_value=0)
    for column in (EventKind.MISSING.value, "thinned"):
        if column not in kind_counts.columns:
            kind_counts[column] = 0
    # outer join: a bucket may hold marker rows only (all quotes missing or
    # thinned) — those land as marker rows with NaN prices and zero volume
    merged = quote_stats.join(kind_counts[[EventKind.MISSING.value, "thinned"]], how="outer")
    out = pd.DataFrame(
        {
            "symbol": [symbol for symbol, _ in merged.index],
            "ts": [bucket for _, bucket in merged.index],
            "open": merged["open"].astype(float),
            "high": merged["high"].astype(float),
            "low": merged["low"].astype(float),
            "close": merged["close"].astype(float),
            "volume": (
                merged["volume_max"].fillna(0.0) - merged["volume_min"].fillna(0.0)
            ).clip(lower=0.0),
            "amount": (
                merged["amount_max"].fillna(0.0) - merged["amount_min"].fillna(0.0)
            ).clip(lower=0.0),
            "samples": merged["samples"].fillna(0).astype(int),
            "missing": merged[EventKind.MISSING.value].fillna(0).astype(int),
            "thinned": merged["thinned"].fillna(0).astype(int),
            "quality": "ok",
        }
    )
    return out[list(SNAPSHOT_1M_COLUMNS)]


def archive_expired(
    lake: DataLake,
    policy: SnapshotPolicy,
    *,
    today: date | None = None,
    store: SnapshotStore | None = None,
) -> ArchiveReport:
    """Run one retention pass: archive then reclaim expired raw partitions.

    ``today`` defaults to the wall clock (injectable for tests).  A raw
    partition qualifies when its trade date is *strictly older* than
    ``today - raw_retention_days`` — the live window always stays raw.
    When ``archive_retention_days > 0``, archive partitions older than
    that are pruned as well (they are the smallest copy; 0 keeps them
    forever, the default).
    """
    store = store or SnapshotStore(lake)
    report = ArchiveReport()
    cutoff = (today or date.today()) - timedelta(days=policy.raw_retention_days)
    base = lake.root / Dataset.SNAPSHOTS.value
    for directory in sorted(base.glob("symbol=*/date=*")):
        try:
            day = date.fromisoformat(directory.name.removeprefix("date="))
        except ValueError:
            report.failures[str(directory)] = "unparseable partition date"
            continue
        if day >= cutoff:
            continue
        part = directory / "part.parquet"
        if not part.exists():
            report.skipped.append(str(directory.relative_to(lake.root)))
            continue
        raw = pd.read_parquet(part, engine="pyarrow")
        try:
            aggregate = aggregate_snapshots(raw, interval_s=policy.archive_interval_s)
            written = lake.write(Dataset.SNAPSHOTS_1M, aggregate, source=ARCHIVE_SOURCE)
            lake.update_watermark(
                source=ARCHIVE_SOURCE,
                dataset=Dataset.SNAPSHOTS_1M.value,
                partitions=written,
                rows=len(aggregate),
                synced_through=day,
            )
        except Exception as exc:  # noqa: BLE001 - raw stays intact on any failure
            report.failures[str(directory.relative_to(lake.root))] = repr(exc)
            logger.exception("archiving %s failed; raw partition kept", directory)
            continue
        reclaimed = _tree_size(directory)
        shutil.rmtree(directory)
        report.raw_partitions_archived += 1
        report.raw_rows_aggregated += len(raw)
        report.archive_rows_written += len(aggregate)
        report.raw_bytes_reclaimed += reclaimed
        logger.info(
            "archived %s (%d raw rows → %d aggregate rows, %d bytes reclaimed)",
            directory,
            len(raw),
            len(aggregate),
            reclaimed,
        )
    if policy.archive_retention_days > 0:
        archive_cutoff = (today or date.today()) - timedelta(days=policy.archive_retention_days)
        archive_base = lake.root / Dataset.SNAPSHOTS_1M.value
        for directory in sorted(archive_base.glob("symbol=*/date=*")):
            try:
                day = date.fromisoformat(directory.name.removeprefix("date="))
            except ValueError:
                continue
            if day >= archive_cutoff:
                continue
            shutil.rmtree(directory)
            report.archive_partitions_pruned += 1
    if report.failures:
        raise LakeError(
            f"snapshot archiving failed for {len(report.failures)} partition(s); "
            "raw data kept intact"
        )
    return report


def _tree_size(directory: Path) -> int:
    return sum(p.stat().st_size for p in directory.rglob("*") if p.is_file())
