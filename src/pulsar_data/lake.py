"""The local market-data lake: partitioned Parquet with atomic writes.

Layout (mirrors the integration design exactly)::

    lake/
      bars_1d/symbol=SH600519/year=2024/part.parquet
      corporate_actions/symbol=SH600519/part.parquet
      instruments/instruments.parquet
      calendar/calendar.parquet
      suspensions/symbol=SH600519/part.parquet
      _meta/watermarks.parquet

Write atomicity: every partition file is written to a ``.tmp-<uuid>``
sibling first and moved into place with ``os.replace`` — a reader
globs either the previous complete file or the new one, never a
half-written frame.  Whole-partition replacement (not append) makes
backfill idempotent: re-running a partition yields byte-identical
logical content.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .errors import LakeError
from .schema import Dataset, ts_to_date
from .symbols import to_canonical_symbol

__all__ = ["DataLake"]


class DataLake:
    """Read/write access to one local lake directory."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "_meta").mkdir(exist_ok=True)

    # ------------------------------------------------------------------ paths
    def partition_path(self, dataset: Dataset, keys: dict[str, str]) -> Path:
        """Path of the (single-file) partition for ``keys``.

        ``keys`` are hive-style partition components, e.g.
        ``{"symbol": "SH600519", "year": "2024"}``.
        """
        parts = [self.root, dataset.value]
        for key, value in keys.items():
            parts.append(f"{key}={value}")
        return Path(*parts) / "part.parquet"

    def _partition_keys(self, dataset: Dataset, frame: pd.DataFrame, row: pd.Series) -> dict[str, str]:
        if dataset is Dataset.BARS_1D:
            year = ts_to_date(row["ts"]).year
            return {"symbol": row["symbol"], "year": str(year)}
        if dataset in (Dataset.CORPORATE_ACTIONS, Dataset.SUSPENSIONS):
            return {"symbol": row["symbol"]}
        return {}

    # ------------------------------------------------------------------ write
    def write(self, dataset: Dataset, frame: pd.DataFrame, *, source: str) -> list[str]:
        """Write ``frame`` partition by partition with atomic replacement.

        Returns the list of partition identifiers written (relative,
        hive-style, e.g. ``symbol=SH600519/year=2024``).
        """
        written: list[str] = []
        if dataset in (Dataset.INSTRUMENTS, Dataset.CALENDAR):
            written.append(self._write_file(dataset, {}, frame))
        else:
            for keys, group in self._split_partitions(dataset, frame):
                written.append(self._write_file(dataset, keys, group))
        return written

    def _split_partitions(self, dataset: Dataset, frame: pd.DataFrame):
        grouped: dict[tuple[tuple[str, str], ...], pd.DataFrame] = {}
        for index, row in frame.iterrows():
            keys = tuple(sorted(self._partition_keys(dataset, frame, row).items()))
            grouped.setdefault(keys, []).append(index)
        for keys, indices in grouped.items():
            yield dict(keys), frame.loc[indices]

    def _write_file(self, dataset: Dataset, keys: dict[str, str], frame: pd.DataFrame) -> str:
        target = self.partition_path(dataset, keys)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".tmp-{uuid.uuid4().hex}.parquet")
        frame.to_parquet(tmp, index=False, engine="pyarrow")
        os.replace(tmp, target)
        relative = "/".join(f"{k}={v}" for k, v in keys.items())
        return relative

    # ------------------------------------------------------------------ read
    def read(self, dataset: Dataset, symbols: Iterable[str] | None = None) -> pd.DataFrame:
        """Read a dataset back as one frame (all partitions, sorted).

        Hidden ``.tmp-*`` files are never globbed, so concurrent writers
        cannot leak partial frames to readers.
        """
        base = self.root / dataset.value
        if dataset in (Dataset.INSTRUMENTS, Dataset.CALENDAR):
            file = base / "part.parquet"
            if not file.exists():
                raise LakeError(f"{dataset.value} has not been ingested into {self.root}")
            return pd.read_parquet(file, engine="pyarrow")
        frames: list[pd.DataFrame] = []
        wanted = set(symbols) if symbols is not None else None
        for file in sorted(base.glob("**/part.parquet")):
            if wanted is not None:
                symbol_parts = [
                    part.removeprefix("symbol=")
                    for part in file.parts
                    if part.startswith("symbol=")
                ]
                if symbol_parts and symbol_parts[0] not in wanted:
                    continue
            frames.append(pd.read_parquet(file, engine="pyarrow"))
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        if "symbol" in out.columns:
            out = out.sort_values(["symbol"] + (["ts"] if "ts" in out.columns else [])).reset_index(drop=True)
        return out

    def calendar_dates(self, start: date, end: date) -> list[date]:
        """Trading days in ``[start, end]`` from the ingested calendar."""
        frame = self.read(Dataset.CALENDAR)
        dates = pd.to_datetime(frame["trade_date"]).dt.date
        return [d for d in dates if start <= d <= end]

    # -------------------------------------------------------------- watermarks
    def update_watermark(
        self,
        *,
        source: str,
        dataset: str,
        partitions: list[str],
        rows: int,
        synced_through: date,
    ) -> None:
        """Upsert the per source/dataset/partition sync watermark."""
        path = self.root / "_meta" / "watermarks.parquet"
        now = datetime.now().isoformat(timespec="seconds")
        fresh = pd.DataFrame(
            [
                {
                    "source": source,
                    "dataset": dataset,
                    "partition": p or "*",
                    "rows": rows,
                    "synced_through": synced_through.isoformat(),
                    "updated_at": now,
                }
                for p in partitions
            ]
            + [
                {  # dataset-level watermark even when there is no partition key
                    "source": source,
                    "dataset": dataset,
                    "partition": "*",
                    "rows": rows,
                    "synced_through": synced_through.isoformat(),
                    "updated_at": now,
                }
            ]
        )
        if partitions:
            fresh = fresh.drop_duplicates(subset=["source", "dataset", "partition"])
        else:
            fresh = fresh.head(1)
        if path.exists():
            existing = pd.read_parquet(path, engine="pyarrow")
            for partition_id in set(fresh["partition"]):
                existing = existing[
                    ~(
                        (existing["source"] == source)
                        & (existing["dataset"] == dataset)
                        & (existing["partition"] == partition_id)
                    )
                ]
            fresh = pd.concat([existing, fresh], ignore_index=True)
        tmp = path.with_name(f".tmp-{uuid.uuid4().hex}.parquet")
        fresh.sort_values(["source", "dataset", "partition"]).to_parquet(
            tmp, index=False, engine="pyarrow"
        )
        os.replace(tmp, path)

    def watermarks(self) -> pd.DataFrame:
        path = self.root / "_meta" / "watermarks.parquet"
        if not path.exists():
            return pd.DataFrame(
                columns=["source", "dataset", "partition", "rows", "synced_through", "updated_at"]
            )
        return pd.read_parquet(path, engine="pyarrow")

    # --------------------------------------------------------- completeness
    def completeness(
        self,
        *,
        start: date,
        end: date,
        symbols: Iterable[str] | None = None,
        suspension_days: dict[str, set[date]] | None = None,
    ) -> dict[str, dict[str, int]]:
        """Classify every (symbol, trading day) cell relative to the calendar.

        Returns ``{symbol: {category: count}}`` with categories:
        ``ok`` (bar present), ``not_listed`` (before the symbol's first
        bar — pre-IPO or outside source coverage), ``coverage_end``
        (after the symbol's last bar), ``suspended`` (suspension record
        covers the day), ``gap`` (unexplained missing bar).
        """
        trading_days = self.calendar_dates(start, end)
        if not trading_days:
            raise LakeError(
                f"no calendar rows in [{start}, {end}]; ingest the calendar dataset first"
            )
        bars = self.read(Dataset.BARS_1D, symbols=symbols)
        if bars.empty:
            return {}
        suspension_days = suspension_days or {}
        out: dict[str, dict[str, int]] = {}
        bars["trade_date"] = pd.to_datetime(bars["ts"], utc=True).dt.tz_convert(
            "Asia/Shanghai"
        ).dt.date
        for symbol, group in bars.groupby("symbol"):
            canonical = to_canonical_symbol(symbol)
            present = set(group["trade_date"])
            first, last = min(present), max(present)
            suspended = suspension_days.get(canonical, set())
            counts = {"ok": 0, "not_listed": 0, "coverage_end": 0, "suspended": 0, "gap": 0}
            for day in trading_days:
                if day in present:
                    counts["ok"] += 1
                elif day in suspended:
                    counts["suspended"] += 1
                elif day < first:
                    counts["not_listed"] += 1
                elif day > last:
                    counts["coverage_end"] += 1
                else:
                    counts["gap"] += 1
            out[canonical] = counts
        return out
