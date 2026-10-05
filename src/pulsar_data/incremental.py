"""Watermark-driven daily incremental update (日终增量).

Design mapping: “增量更新：以 watermark 记录每源每分区已同步位置，
日终增量拉取” and “本包只保证任务可重入”.  :class:`IncrementalRunner`
reuses the exact backfill pipeline (fetch_raw → normalize → quality
gate → lake write) — only the write goes through
:meth:`pulsar_data.lake.DataLake.merge_write` and each symbol's window
starts at its own bars watermark (inclusive, to heal a partial
last-day write) instead of a fixed historical start.

Idempotent re-entry: merging dedupes on the dataset's natural key with
incoming rows winning, so re-running the same incremental window (or
running it twice after a crash) leaves the lake byte-identical at the
partition level and simply refreshes watermark bookkeeping.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Sequence

import pandas as pd

from .backfill import BackfillReport, suspension_days
from .errors import ConfigurationError, LakeError, PulsarDataError
from .lake import DataLake
from .schema import Dataset
from .sources.base import FetchRequest, SourceAdapter, run_ingestion
from .symbols import to_canonical_symbol

logger = logging.getLogger("pulsar_data.incremental")

__all__ = ["IncrementalRunner"]


class IncrementalRunner:
    """Runs one daily incremental pass for one source adapter."""

    def __init__(
        self,
        adapter: SourceAdapter,
        lake: DataLake,
        *,
        initial_start: date | None = None,
        include_corporate_actions: bool = True,
        include_suspensions: bool = True,
        include_instruments: bool = True,
    ) -> None:
        self.adapter = adapter
        self.lake = lake
        self.initial_start = initial_start
        self.include_corporate_actions = include_corporate_actions
        self.include_suspensions = include_suspensions
        self.include_instruments = include_instruments

    # ------------------------------------------------------------------ run
    def run(self, end: date, *, symbols: Sequence[str] | None = None) -> BackfillReport:
        """Bring the lake up to ``end`` from the per-symbol watermarks.

        ``symbols`` defaults to every instrument known to the lake.
        Symbols whose bars watermark is already at/after ``end`` are
        skipped (recorded in the report) — they cost one watermark read,
        zero upstream calls.
        """
        report = BackfillReport(source=self.adapter.source_id, start=end, end=end, symbols=[])
        resolved = self._resolve_symbols(symbols, end)
        report.symbols = list(resolved)
        self._update_calendar(end, report)
        if self.include_instruments:
            self._update_instruments(end, report)
        if self.include_suspensions:
            self._update_suspensions(end, report)
        self._update_bars(resolved, end, report)
        if self.include_corporate_actions:
            self._update_corporate_actions(resolved, end, report)
        self._attach_completeness(resolved, end, report)
        return report

    # -------------------------------------------------------------- symbols
    def _resolve_symbols(self, symbols: Sequence[str] | None, end: date) -> list[str]:
        """Explicit symbols, the lake's instrument snapshot, or (fresh
        lake with ``initial_start``) the adapter's full universe — the
        same resolution :class:`~pulsar_data.backfill.BackfillRunner`
        uses for ``--all``."""
        if symbols:
            return [to_canonical_symbol(symbol) for symbol in symbols]
        try:
            frame = self.lake.read(Dataset.INSTRUMENTS)
        except LakeError:
            frame = pd.DataFrame()
        if frame.empty:
            if self.initial_start is None:
                raise ConfigurationError(
                    "no symbols given, the lake has no instrument snapshot, and no "
                    "initial_start to fetch a universe from the source; pass symbols=, "
                    "run a backfill first, or give --initial-start"
                )
            request = FetchRequest(Dataset.INSTRUMENTS, self.initial_start, end)
            frame = self.adapter.normalize(
                Dataset.INSTRUMENTS, self.adapter.fetch_raw(Dataset.INSTRUMENTS, request), request
            )
        return sorted(frame["symbol"].map(to_canonical_symbol).unique())

    # ------------------------------------------------------------- datasets
    def _update_calendar(self, end: date, report: BackfillReport) -> None:
        start = self._dataset_watermark(Dataset.CALENDAR) or self.initial_start
        if start is None:
            raise ConfigurationError(
                "no calendar watermark and no --initial-start given; "
                "cannot infer the incremental calendar window"
            )
        if start > end:
            return
        result = run_ingestion(
            self.adapter,
            FetchRequest(Dataset.CALENDAR, start, end),
            self.lake,
            quality_column_value="ok",
            merge=True,
        )
        report.calendar_rows += result.rows

    def _update_instruments(self, end: date, report: BackfillReport) -> None:
        start = self.initial_start or date(end.year, 1, 1)
        if start > end:
            return
        result = run_ingestion(
            self.adapter,
            FetchRequest(Dataset.INSTRUMENTS, start, end),
            self.lake,
            quality_column_value="ok",
        )
        report.instrument_rows += result.rows

    def _update_suspensions(self, end: date, report: BackfillReport) -> None:
        start = self._dataset_watermark(Dataset.SUSPENSIONS) or self.initial_start
        if start is None or start > end:
            return
        try:
            result = run_ingestion(
                self.adapter,
                FetchRequest(Dataset.SUSPENSIONS, start, end),
                self.lake,
                quality_column_value="ok",
                merge=True,
            )
            report.suspension_rows += result.rows
        except PulsarDataError as exc:
            logger.warning("suspension incremental failed; gaps will surface in report: %s", exc)

    def _update_bars(self, symbols: Sequence[str], end: date, report: BackfillReport) -> None:
        for index, symbol in enumerate(symbols, start=1):
            start = self._bars_watermark(symbol) or self.initial_start
            if start is None:
                report.failed_symbols[symbol] = "no bars watermark and no initial_start given"
                continue
            if start > end:
                report.skipped_symbols.append(symbol)
                continue
            try:
                result = run_ingestion(
                    self.adapter,
                    FetchRequest(Dataset.BARS_1D, start, end, symbol=symbol),
                    self.lake,
                    quality_column_value="ok",
                    merge=True,
                )
                report.bars_rows += result.rows
            except PulsarDataError as exc:
                report.failed_symbols[symbol] = str(exc)[:300]
                logger.warning("bars incremental failed for %s: %s", symbol, exc)
            if index % 25 == 0:
                logger.info("bars incremental progress: %d/%d", index, len(symbols))

    def _update_corporate_actions(
        self, symbols: Sequence[str], end: date, report: BackfillReport
    ) -> None:
        for symbol in symbols:
            start = self._corporate_actions_watermark(symbol) or self.initial_start
            if start is None or start > end:
                continue
            try:
                result = run_ingestion(
                    self.adapter,
                    FetchRequest(Dataset.CORPORATE_ACTIONS, start, end, symbol=symbol),
                    self.lake,
                    quality_column_value="ok",
                    merge=True,
                )
                report.corporate_action_rows += result.rows
            except PulsarDataError as exc:
                report.failed_symbols.setdefault(symbol, f"corporate_actions: {str(exc)[:200]}")
                logger.warning("corporate-action incremental failed for %s: %s", symbol, exc)

    # ------------------------------------------------------------ watermarks
    def _marks_for(self, dataset: Dataset, partition_predicate) -> list[pd.Timestamp]:
        marks = self.lake.watermarks()
        if marks.empty:
            return []
        rows = marks[
            (marks["source"] == self.adapter.source_id)
            & (marks["dataset"] == dataset.value)
        ]
        return [pd.to_datetime(value) for value in rows.loc[partition_predicate(rows), "synced_through"]]

    def _dataset_watermark(self, dataset: Dataset) -> date | None:
        values = self._marks_for(dataset, lambda rows: rows["partition"] == "*")
        return max(values).date() if values else None

    def _bars_watermark(self, symbol: str) -> date | None:
        prefix = f"symbol={symbol}/"
        values = self._marks_for(Dataset.BARS_1D, lambda rows: rows["partition"].str.startswith(prefix))
        return max(values).date() if values else None

    def _corporate_actions_watermark(self, symbol: str) -> date | None:
        target = f"symbol={symbol}"
        values = self._marks_for(
            Dataset.CORPORATE_ACTIONS, lambda rows: rows["partition"] == target
        )
        return max(values).date() if values else None

    # ------------------------------------------------------------ reporting
    def _attach_completeness(
        self, symbols: Sequence[str], end: date, report: BackfillReport
    ) -> None:
        """Classify the target day only: did the increment actually land?

        A non-trading ``end`` (weekend/holiday) has no calendar rows;
        completeness is then vacuous and skipped rather than an error.
        """
        try:
            report.completeness = self.lake.completeness(
                start=end,
                end=end,
                symbols=symbols,
                suspension_days=suspension_days(self.lake, symbols),
            )
        except LakeError:
            report.completeness = {}
