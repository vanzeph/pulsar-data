"""Partition-level quality report over the ``quality`` mark column.

Design mapping — 数据质量与补数 §可信度落地: 「每个分区带 quality 标记列
（ok / backfilled / suspect），读侧可过滤」.  :func:`build_quality_report`
aggregates the marks per bars partition: counts per mark plus the day
lists (清单) of ``backfilled`` and ``suspect`` rows, so an operator can
see at a glance which partitions carry repaired or flagged data and a
researcher can cross-check what a read-side ``quality`` filter would
drop.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from .lake import DataLake
from .schema import QUALITY_VALUES, Dataset, ts_to_date
from .symbols import to_canonical_symbol

__all__ = ["PartitionQuality", "QualityReport", "build_quality_report"]


@dataclass(frozen=True)
class PartitionQuality:
    """Mark summary of one bars partition (``symbol × year``)."""

    symbol: str
    year: int
    rows: int
    counts: dict[str, int] = field(default_factory=dict)
    backfilled_days: list[str] = field(default_factory=list)
    suspect_days: list[str] = field(default_factory=list)

    @property
    def partition(self) -> str:
        return f"symbol={self.symbol}/year={self.year}"

    def to_dict(self) -> dict[str, object]:
        return {
            "partition": self.partition,
            "symbol": self.symbol,
            "year": self.year,
            "rows": self.rows,
            "counts": self.counts,
            "backfilled_days": self.backfilled_days,
            "suspect_days": self.suspect_days,
        }


@dataclass
class QualityReport:
    """The whole lake's partition-quality summary (serializable)."""

    partitions: list[PartitionQuality] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    @property
    def totals(self) -> dict[str, int]:
        totals = {mark: 0 for mark in QUALITY_VALUES}
        for item in self.partitions:
            for mark, count in item.counts.items():
                totals[mark] = totals.get(mark, 0) + count
        return totals

    @property
    def rows(self) -> int:
        return sum(item.rows for item in self.partitions)

    def to_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "partitions": len(self.partitions),
            "rows": self.rows,
            "totals": self.totals,
            "per_partition": [item.to_dict() for item in self.partitions],
        }

    def to_json(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return target


def build_quality_report(
    lake: DataLake, *, symbols: Iterable[str] | None = None
) -> QualityReport:
    """Aggregate the ``quality`` marks of every bars partition in the lake."""
    bars = lake.read(Dataset.BARS_1D, symbols=symbols)
    if bars.empty:
        return QualityReport()
    frame = pd.DataFrame({
        "symbol": [to_canonical_symbol(str(symbol)) for symbol in bars["symbol"]],
        "trade_date": [ts_to_date(ts) for ts in bars["ts"]],
        "quality": bars["quality"].astype(str),
    })
    partitions: list[PartitionQuality] = []
    for (symbol, year), group in frame.groupby(["symbol", frame["trade_date"].map(lambda d: d.year)]):
        counts = {mark: int((group["quality"] == mark).sum()) for mark in QUALITY_VALUES}
        backfilled = sorted(day.isoformat() for day, mark in zip(group["trade_date"], group["quality"]) if mark == "backfilled")
        suspect = sorted(day.isoformat() for day, mark in zip(group["trade_date"], group["quality"]) if mark == "suspect")
        partitions.append(
            PartitionQuality(
                symbol=symbol,
                year=int(year),
                rows=len(group),
                counts=counts,
                backfilled_days=backfilled,
                suspect_days=suspect,
            )
        )
    partitions.sort(key=lambda item: (item.symbol, item.year))
    return QualityReport(partitions=partitions)
