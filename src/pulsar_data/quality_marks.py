"""Row-level ``suspect`` marking driven by quality events (质量标记落地).

Design mapping — 数据质量与补数 §可信度落地: 「每个分区带 quality 标记列
（ok / backfilled / suspect），读侧可过滤」 and §一致性: 「差异超阈值记
质量事件」.  :func:`mark_suspect` closes the loop D2 opened: the
:class:`~pulsar_data.crosscheck.CrossValidator` only *detects and
reports* cross-source disagreement; this module persists the verdict
onto the affected lake rows so the read side can filter them.

Semantics:

* **Row-level** — only the ``(symbol, trade_date)`` cells named by the
  events are marked; every other row keeps its current mark.
* **Partition-atomic** — each touched partition is rewritten through
  the same ``.tmp`` + ``os.replace`` whole-partition replace as every
  other lake write, so readers never observe a half-marked partition.
* **Idempotent** — marking the same events twice writes the identical
  partition content (already-``suspect`` rows simply stay ``suspect``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Iterable

from .lake import DataLake
from .schema import Dataset, ts_to_date
from .symbols import to_canonical_symbol

__all__ = ["SuspectMarkResult", "mark_suspect", "events_to_cells"]

#: The only crosscheck event type that carries a per-row verdict today.
_MISMATCH_EVENT_TYPE = "cross_source_mismatch"


@dataclass(frozen=True)
class SuspectMarkResult:
    """Outcome of one :func:`mark_suspect` pass."""

    #: Partitions rewritten (hive-style ids, e.g. ``symbol=SH600519/year=2024``).
    partitions: list[str] = field(default_factory=list)
    #: Rows whose quality mark became ``suspect`` in this pass.
    rows_marked: int = 0
    #: Event cells that matched no stored bar (date outside lake coverage).
    unmatched_cells: list[str] = field(default_factory=list)


def _event_cells(events: Iterable[object]) -> set[tuple[str, date]]:
    """Extract ``(symbol, trade_date)`` cells from events or a report.

    Accepts a :class:`~pulsar_data.crosscheck.CrossCheckReport` (its
    ``.events`` are used), an iterable of
    :class:`~pulsar_data.crosscheck.CrossCheckEvent`, or an iterable of
    the serialized event dicts a stored JSON report contains — so a
    drill can replay a persisted report without re-running the check.
    """
    if hasattr(events, "events"):  # CrossCheckReport
        events = events.events
    cells: set[tuple[str, date]] = set()
    for event in events:
        if isinstance(event, dict):
            if event.get("type", _MISMATCH_EVENT_TYPE) != _MISMATCH_EVENT_TYPE:
                continue
            symbol = event.get("symbol")
            trade_date = event.get("trade_date")
        else:
            if getattr(event, "type", _MISMATCH_EVENT_TYPE) != _MISMATCH_EVENT_TYPE:
                continue
            symbol = event.symbol
            trade_date = event.trade_date
        if not symbol or not trade_date:
            continue
        cells.add((to_canonical_symbol(str(symbol)), date.fromisoformat(str(trade_date))))
    return cells


def events_to_cells(events: Iterable[object] | object) -> set[tuple[str, date]]:
    """Public form of the event-to-cell extraction (see :func:`_event_cells`)."""
    return _event_cells(events)


def mark_suspect(
    lake: DataLake,
    events: Iterable[object] | object,
    *,
    dataset: Dataset = Dataset.BARS_1D,
) -> SuspectMarkResult:
    """Persist crosscheck verdicts onto the affected lake rows.

    ``events`` may be a :class:`CrossCheckReport`, an iterable of
    :class:`CrossCheckEvent`, or an iterable of serialized event dicts.
    Rows named by the events get ``quality = "suspect"``; the partitions
    holding them are rewritten atomically.  Re-running with the same
    events is a no-op on content (idempotent).
    """
    cells = _event_cells(events)
    if not cells:
        return SuspectMarkResult()
    symbols = sorted({symbol for symbol, _ in cells})
    bars = lake.read(dataset, symbols=symbols)
    if bars.empty:
        return SuspectMarkResult(unmatched_cells=sorted(f"{s}:{d}" for s, d in cells))

    frame = bars.copy()
    frame["trade_date"] = frame["ts"].map(ts_to_date)
    wanted = [
        (to_canonical_symbol(str(symbol)), day)
        for symbol, day in zip(frame["symbol"], frame["trade_date"])
    ]
    frame["_suspect"] = [cell in cells for cell in wanted]

    touched = frame[frame["_suspect"]]
    if touched.empty:
        return SuspectMarkResult(
            unmatched_cells=sorted(f"{s}:{d.isoformat()}" for s, d in cells)
        )

    matched = {
        (to_canonical_symbol(str(symbol)), day)
        for symbol, day in zip(touched["symbol"], touched["trade_date"])
    }
    # Rewrite only the partitions that actually hold a suspect row; the
    # whole-partition replace always carries every row of that partition.
    suspect_partitions = {
        (to_canonical_symbol(str(symbol)), day.year)
        for symbol, day in zip(touched["symbol"], touched["trade_date"])
    }
    keep = [
        (to_canonical_symbol(str(symbol)), day.year) in suspect_partitions
        for symbol, day in zip(frame["symbol"], frame["trade_date"])
    ]
    marked = frame[keep].drop(columns=["trade_date", "_suspect"]).reset_index(drop=True)
    marked.loc[frame[keep]["_suspect"].to_numpy(), "quality"] = "suspect"

    partitions = lake.write(dataset, marked, source="quality_marks")
    return SuspectMarkResult(
        partitions=partitions,
        rows_marked=int(frame["_suspect"].sum()),
        unmatched_cells=sorted(
            f"{symbol}:{day.isoformat()}"
            for symbol, day in cells
            if (symbol, day) not in matched
        ),
    )
