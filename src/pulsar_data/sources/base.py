"""The internal ``SourceAdapter`` contract and the ingestion pipeline.

Every data source plugged into pulsar-data implements one thing:
:class:`SourceAdapter`.  The contract mirrors the integration design:

1. ``fetch_raw(dataset, request)`` — call the upstream SDK or HTTP API
   and return the *raw* frame exactly as the source shapes it;
2. ``normalize(dataset, raw)`` — translate the raw frame into the
   canonical schema for that dataset.

Everything downstream — quality gating, lake writes, watermarks — is
framework work performed by :func:`run_ingestion`, never by adapters.
Adapters only collect and translate; upstream API churn is absorbed
inside a single adapter package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol, runtime_checkable

import pandas as pd

from ..schema import Dataset

__all__ = [
    "FetchRequest",
    "SourceAdapter",
    "IngestionResult",
    "run_ingestion",
]


@dataclass(frozen=True)
class FetchRequest:
    """What the framework wants from a source.

    ``symbol`` is the canonical form (``SH600519``); datasets without a
    per-symbol notion (calendar, instruments) ignore it.
    """

    dataset: Dataset
    start: date
    end: date
    symbol: str | None = None


@runtime_checkable
class SourceAdapter(Protocol):
    """Internal contract every data source implements.

    ``source_id`` is the registry identifier (e.g. ``akshare``).
    ``fetch_raw`` talks to the upstream; ``normalize`` turns one raw
    frame into the canonical schema for the dataset.  Implementations
    must be stateless between calls (the lake is the state).
    """

    source_id: str

    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame: ...

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame: ...


@dataclass
class IngestionResult:
    """Outcome of one fetch → normalize → quality-check → write cycle."""

    source: str
    dataset: Dataset
    request: FetchRequest
    rows: int
    partitions: list[str] = field(default_factory=list)
    quality_marks: dict[str, int] = field(default_factory=dict)
    skipped: int = 0

    @property
    def ok(self) -> bool:
        return True  # quality gates raise on failure; reaching here means ok


def run_ingestion(
    adapter: SourceAdapter,
    request: FetchRequest,
    lake: Any,
    *,
    quality_column_value: str = "ok",
) -> IngestionResult:
    """Drive one dataset through the fixed pipeline into ``lake``.

    ``lake`` is a :class:`pulsar_data.lake.DataLake`; it is kept as an
    opaque parameter here so the framework layering stays explicit.
    The returned frame's ``quality`` column (when the dataset carries
    one) is set to ``quality_column_value`` before writing — historical
    backfills mark rows ``backfilled``, daily increments ``ok``.
    """
    from ..quality import check_canonical

    raw = adapter.fetch_raw(request.dataset, request)
    canonical = adapter.normalize(request.dataset, raw, request)
    check_canonical(request.dataset, canonical, request)

    if canonical.empty:
        # nothing in window (not listed / no upstream data): record no
        # partition, no watermark — a later run may still fill it.
        return IngestionResult(
            source=adapter.source_id,
            dataset=request.dataset,
            request=request,
            rows=0,
            partitions=[],
        )

    if "quality" in canonical.columns:
        canonical["quality"] = quality_column_value

    partitions = lake.write(request.dataset, canonical, source=adapter.source_id)
    lake.update_watermark(
        source=adapter.source_id,
        dataset=request.dataset.value,
        partitions=partitions,
        rows=len(canonical),
        synced_through=request.end,
    )
    return IngestionResult(
        source=adapter.source_id,
        dataset=request.dataset,
        request=request,
        rows=len(canonical),
        partitions=partitions,
    )
