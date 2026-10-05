"""Dual-source sampled cross-validation (双源抽样交叉校验).

Design mapping — 数据质量与补数 §一致性: 「双源覆盖区间抽样交叉校验
（收盘价、成交量），差异超阈值记质量事件」.

:class:`CrossValidator` pulls the same window from two adapters through
the *full* fetch_raw → normalize pipeline (so both sides speak the
canonical schema and share units), aligns them on ``(symbol, ts)`` over
the overlap of their coverage, samples up to ``sample_size`` bars per
symbol spread evenly across that overlap, and compares ``close`` and
``volume`` row by row.  Every row where the relative difference exceeds
its tolerance becomes a structured :class:`CrossCheckEvent` (质量事件)
carried in the :class:`CrossCheckReport`; the report serializes to JSON
so drills and audits have a durable artifact.

Boundary note: partition-level ``quality`` marking and gap-driven
backfill belong to the quality/backfill task; this module only
*detects and reports* cross-source disagreement.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd

from .schema import Dataset, ts_to_date
from .sources.base import FetchRequest, SourceAdapter

__all__ = [
    "CrossCheckEvent",
    "SymbolCrossCheck",
    "CrossCheckReport",
    "CrossValidator",
]


@dataclass(frozen=True)
class CrossCheckEvent:
    """One over-threshold disagreement between the two sources (质量事件)."""

    type: str  # always "cross_source_mismatch"
    source_primary: str
    source_secondary: str
    symbol: str
    trade_date: str
    field: str  # "close" | "volume"
    value_primary: float
    value_secondary: float
    relative_diff: float
    tolerance: float

    def to_dict(self) -> dict[str, object]:
        return {
            "type": self.type,
            "source_primary": self.source_primary,
            "source_secondary": self.source_secondary,
            "symbol": self.symbol,
            "trade_date": self.trade_date,
            "field": self.field,
            "value_primary": self.value_primary,
            "value_secondary": self.value_secondary,
            "relative_diff": self.relative_diff,
            "tolerance": self.tolerance,
        }


@dataclass
class SymbolCrossCheck:
    """Per-symbol comparison statistics."""

    symbol: str
    overlap_rows: int = 0
    sampled_rows: int = 0
    compared_rows: int = 0
    close_mismatches: int = 0
    volume_mismatches: int = 0
    max_close_relative_diff: float = 0.0
    max_volume_relative_diff: float = 0.0
    primary_only_rows: int = 0
    secondary_only_rows: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "overlap_rows": self.overlap_rows,
            "sampled_rows": self.sampled_rows,
            "compared_rows": self.compared_rows,
            "close_mismatches": self.close_mismatches,
            "volume_mismatches": self.volume_mismatches,
            "max_close_relative_diff": self.max_close_relative_diff,
            "max_volume_relative_diff": self.max_volume_relative_diff,
            "primary_only_rows": self.primary_only_rows,
            "secondary_only_rows": self.secondary_only_rows,
        }


@dataclass
class CrossCheckReport:
    """Outcome of one cross-validation run over a window."""

    source_primary: str
    source_secondary: str
    start: date
    end: date
    symbols: list[str]
    per_symbol: dict[str, SymbolCrossCheck] = field(default_factory=dict)
    events: list[CrossCheckEvent] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    @property
    def passed(self) -> bool:
        return not self.events

    @property
    def total_mismatches(self) -> int:
        return len(self.events)

    def to_dict(self) -> dict[str, object]:
        return {
            "source_primary": self.source_primary,
            "source_secondary": self.source_secondary,
            "window": [self.start.isoformat(), self.end.isoformat()],
            "generated_at": self.generated_at,
            "symbols": self.symbols,
            "passed": self.passed,
            "total_mismatches": self.total_mismatches,
            "per_symbol": {
                symbol: stats.to_dict() for symbol, stats in sorted(self.per_symbol.items())
            },
            "quality_events": [event.to_dict() for event in self.events],
        }

    def to_json(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return target


def _relative_diff(primary: float, secondary: float) -> float:
    """Symmetric-ish relative difference, normalized by the primary value.

    Falls back to the absolute difference when the primary value is ~0
    (a pure zero-vs-nonzero disagreement is a 100% mismatch at most).
    """
    reference = abs(primary)
    if reference < 1e-12:
        reference = abs(secondary)
    if reference < 1e-12:
        return 0.0 if math.isclose(primary, secondary, abs_tol=1e-12) else 1.0
    return abs(primary - secondary) / reference


def _sample_indices(count: int, sample_size: int) -> list[int]:
    """Up to ``sample_size`` row indices spread evenly over ``[0, count)``."""
    if count <= 0:
        return []
    if sample_size <= 0 or count <= sample_size:
        return list(range(count))
    step = count / sample_size
    indices = sorted({int(index * step) for index in range(sample_size)})
    return indices


class CrossValidator:
    """Compare two sources on close price and volume over their overlap."""

    def __init__(
        self,
        primary: SourceAdapter,
        secondary: SourceAdapter,
        *,
        close_tolerance: float = 0.001,
        volume_tolerance: float = 0.05,
        sample_size: int = 20,
        sampler: Callable[[int, int], list[int]] = _sample_indices,
    ) -> None:
        self.primary = primary
        self.secondary = secondary
        self.close_tolerance = close_tolerance
        self.volume_tolerance = volume_tolerance
        self.sample_size = sample_size
        self.sampler = sampler

    # ------------------------------------------------------------------ run
    def check(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        *,
        dataset: Dataset = Dataset.BARS_1D,
    ) -> CrossCheckReport:
        """Cross-validate ``symbols`` over ``[start, end]``.

        Both sources run through their own fetch_raw → normalize for the
        requested window; only their *overlap* (same trading days) is
        compared.  Non-overlapping coverage is counted per side but never
        treated as a mismatch — coverage differences are the completeness
        machinery's problem, not the consistency check's.
        """
        report = CrossCheckReport(
            source_primary=self.primary.source_id,
            source_secondary=self.secondary.source_id,
            start=start,
            end=end,
            symbols=list(symbols),
        )
        for symbol in symbols:
            report.per_symbol[symbol] = self._check_symbol(symbol, start, end, dataset, report)
        return report

    # ------------------------------------------------------------ internals
    def _canonical(self, adapter: SourceAdapter, symbol: str, start: date, end: date, dataset: Dataset) -> pd.DataFrame:
        request = FetchRequest(dataset, start, end, symbol=symbol)
        raw = adapter.fetch_raw(dataset, request)
        return adapter.normalize(dataset, raw, request)

    def _check_symbol(
        self,
        symbol: str,
        start: date,
        end: date,
        dataset: Dataset,
        report: CrossCheckReport,
    ) -> SymbolCrossCheck:
        stats = SymbolCrossCheck(symbol=symbol)
        primary = self._canonical(self.primary, symbol, start, end, dataset)
        secondary = self._canonical(self.secondary, symbol, start, end, dataset)
        if primary.empty or secondary.empty:
            # An empty side means "no coverage to compare": report the
            # row counts per side but never fabricate mismatches.
            stats.primary_only_rows = len(primary)
            stats.secondary_only_rows = len(secondary)
            return stats

        primary = primary.assign(
            _day=primary["ts"].map(ts_to_date)
        ).sort_values("_day").reset_index(drop=True)
        secondary = secondary.assign(
            _day=secondary["ts"].map(ts_to_date)
        ).sort_values("_day").reset_index(drop=True)

        p_days = list(primary["_day"])
        s_days = set(secondary["_day"])
        overlap_days = [day for day in p_days if day in s_days]
        stats.overlap_rows = len(overlap_days)
        stats.primary_only_rows = len(p_days) - len(overlap_days)
        stats.secondary_only_rows = len(secondary) - len(overlap_days)

        merged = primary.merge(secondary, on="_day", how="inner", suffixes=("_p", "_s"))
        indices = self.sampler(len(merged), self.sample_size)
        stats.sampled_rows = len(indices)
        sample = merged.iloc[indices]

        for _, row in sample.iterrows():
            stats.compared_rows += 1
            for column, tolerance, mismatch_attr, max_attr in (
                ("close", self.close_tolerance, "close_mismatches", "max_close_relative_diff"),
                ("volume", self.volume_tolerance, "volume_mismatches", "max_volume_relative_diff"),
            ):
                value_p = float(row[f"{column}_p"])
                value_s = float(row[f"{column}_s"])
                diff = _relative_diff(value_p, value_s)
                if diff > getattr(stats, max_attr):
                    setattr(stats, max_attr, diff)
                if diff > tolerance:
                    setattr(stats, mismatch_attr, getattr(stats, mismatch_attr) + 1)
                    report.events.append(
                        CrossCheckEvent(
                            type="cross_source_mismatch",
                            source_primary=self.primary.source_id,
                            source_secondary=self.secondary.source_id,
                            symbol=symbol,
                            trade_date=row["_day"].isoformat(),
                            field=column,
                            value_primary=value_p,
                            value_secondary=value_s,
                            relative_diff=diff,
                            tolerance=tolerance,
                        )
                    )
        return stats
