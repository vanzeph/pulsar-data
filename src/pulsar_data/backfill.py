"""Full-market historical backfill over the ingestion pipeline.

``BackfillRunner`` drives one :class:`~pulsar_data.sources.base.SourceAdapter`
through the fixed pipeline (fetch_raw → normalize → quality gate → lake
write) for a calendar, the universe, suspensions, and per-symbol bars
and corporate actions.  Properties:

* **idempotent** — every write is a whole-partition atomic replace;
  re-running a window yields identical lake content;
* **resumable** — symbols whose bars are already synced through the
  window end (per watermark) are skipped unless ``force=True``;
* **non-blocking per symbol** — a failing symbol is recorded and the
  run continues (single-source failure never stalls the whole market);
* **quality-reportable** — after the run, :meth:`completeness_report`
  classifies every (symbol × trading day) cell against the calendar
  (``ok / not_listed / coverage_end / suspended / gap``); ``gap`` cells
  are the "unexplained missing bars" of the acceptance criteria.

Minute frequencies (``5m/15m/30m/60m``) take a dedicated path:
``freq`` selects the ``bars_<freq>`` dataset, fetches are chunked per
calendar year (one upstream call per symbol-year keeps retries cheap
and partitions land whole), and the post-run report classifies each
trading day by its full session bar count (a partially filled day is
an unexplained gap).  Day-domain reference data (instruments,
suspensions, corporate actions) is not re-ingested on the minute
path — the daily backfill owns it, and suspension records already in
the lake still excuse missing minute days.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pandas as pd
from pulsar_contracts import Freq

from .errors import ConfigurationError, LakeError, PulsarDataError
from .lake import DataLake
from .schema import Dataset, dataset_for_freq
from .sources.base import FetchRequest, SourceAdapter, run_ingestion
from .symbols import to_canonical_symbol

logger = logging.getLogger("pulsar_data.backfill")

__all__ = ["BackfillRunner", "BackfillReport", "suspension_days"]


def suspension_days(lake: DataLake, symbols: Iterable[str]) -> dict[str, set[date]]:
    """Expand stored suspension ranges to per-symbol day sets (trading days only)."""
    wanted = set(symbols)
    frame = lake.read(Dataset.SUSPENSIONS)
    calendar = lake.read(Dataset.CALENDAR)
    if frame.empty or calendar.empty or not wanted:
        return {}
    trading_days = sorted(pd.to_datetime(calendar["trade_date"]).dt.date)
    days_index = pd.DatetimeIndex(pd.to_datetime(trading_days))
    out: dict[str, set[date]] = {}
    for _, row in frame.iterrows():
        symbol = to_canonical_symbol(row["symbol"])
        if symbol not in wanted:
            continue
        start = pd.to_datetime(row["start_date"]).date()
        end = pd.to_datetime(row["end_date"], errors="coerce")
        end_date = end.date() if pd.notna(end) else start
        mask = (days_index >= pd.Timestamp(start)) & (days_index <= pd.Timestamp(end_date))
        out.setdefault(symbol, set()).update(trading_days[i] for i in mask.nonzero()[0])
    return out


def _dataset_for(freq: Freq) -> Dataset:
    """Lake dataset the runner writes for ``freq`` (validated by the schema)."""
    return dataset_for_freq(freq)


@dataclass
class BackfillReport:
    """Summary of one backfill run plus its completeness classification."""

    source: str
    start: date
    end: date
    symbols: list[str]
    bars_rows: int = 0
    corporate_action_rows: int = 0
    calendar_rows: int = 0
    instrument_rows: int = 0
    suspension_rows: int = 0
    failed_symbols: dict[str, str] = field(default_factory=dict)
    skipped_symbols: list[str] = field(default_factory=list)
    completeness: dict[str, dict[str, int]] = field(default_factory=dict)
    freq: str = Freq.DAILY.value
    minute_missing_bars: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def unexplained_gaps(self) -> int:
        return sum(counts.get("gap", 0) for counts in self.completeness.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "window": [self.start.isoformat(), self.end.isoformat()],
            "symbols": self.symbols,
            "freq": self.freq,
            "rows": {
                "bars": self.bars_rows,
                "corporate_actions": self.corporate_action_rows,
                "calendar": self.calendar_rows,
                "instruments": self.instrument_rows,
                "suspensions": self.suspension_rows,
            },
            "failed_symbols": self.failed_symbols,
            "skipped_symbols": self.skipped_symbols,
            "completeness": self.completeness,
            "unexplained_gaps": self.unexplained_gaps,
            "minute_missing_bars": self.minute_missing_bars,
        }

    def to_json(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return target


class BackfillRunner:
    """Orchestrates a window backfill for one source adapter."""

    def __init__(
        self,
        adapter: SourceAdapter,
        lake: DataLake,
        *,
        include_corporate_actions: bool = True,
        include_suspensions: bool = True,
        include_instruments: bool = True,
        enrich_instruments: bool = False,
        force: bool = False,
        freq: Freq = Freq.DAILY,
    ) -> None:
        self.adapter = adapter
        self.lake = lake
        self.include_corporate_actions = include_corporate_actions
        self.include_suspensions = include_suspensions
        self.include_instruments = include_instruments
        self.enrich_instruments = enrich_instruments
        self.force = force
        self.freq = freq
        self.dataset = _dataset_for(freq)

    # ------------------------------------------------------------------ run
    def run(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        *,
        limit: int | None = None,
    ) -> BackfillReport:
        """Backfill ``symbols`` (or the whole universe when empty) over the window."""
        report = BackfillReport(
            source=self.adapter.source_id,
            start=start,
            end=end,
            symbols=[],
            freq=self.freq.value,
        )
        self._ingest_calendar(start, end, report)
        target_symbols = self._resolve_symbols(symbols, start, end, limit, report)
        report.symbols = list(target_symbols)
        if self.freq is Freq.DAILY:
            if self.include_instruments:
                self._ingest_instruments(target_symbols, start, end, report)
            if self.include_suspensions:
                self._ingest_suspensions(target_symbols, start, end, report)
            self._ingest_bars(target_symbols, start, end, report)
            if self.include_corporate_actions:
                self._ingest_corporate_actions(target_symbols, start, end, report)
            report.completeness = self.completeness_report(target_symbols, start, end)
            return report
        self._ingest_minute_bars(target_symbols, start, end, report)
        counts, missing = self.lake.minute_completeness(
            dataset=self.dataset,
            start=start,
            end=end,
            symbols=target_symbols,
            suspension_days=suspension_days(self.lake, target_symbols),
        )
        report.completeness = counts
        report.minute_missing_bars = missing
        return report

    # ------------------------------------------------------------- datasetes
    def _ingest_calendar(self, start: date, end: date, report: BackfillReport) -> None:
        result = run_ingestion(
            self.adapter,
            FetchRequest(Dataset.CALENDAR, start, end),
            self.lake,
            quality_column_value="ok",
        )
        report.calendar_rows += result.rows
        logger.info("calendar ingested: %d trade days", result.rows)

    def _resolve_symbols(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        limit: int | None,
        report: BackfillReport,
    ) -> list[str]:
        if symbols:
            resolved = [to_canonical_symbol(s) for s in symbols]
        else:
            try:
                raw = self.adapter.fetch_raw(
                    Dataset.INSTRUMENTS, FetchRequest(Dataset.INSTRUMENTS, start, end)
                )
                canonical = self.adapter.normalize(
                    Dataset.INSTRUMENTS, raw, FetchRequest(Dataset.INSTRUMENTS, start, end)
                )
                resolved = sorted(canonical["symbol"].tolist())
            except PulsarDataError:
                # full-market entry on a source without universe discovery
                # (baostock): fall back to the instruments the daily backfill
                # already landed — the lake is the state.
                try:
                    instruments = self.lake.read(Dataset.INSTRUMENTS)
                except LakeError:
                    instruments = pd.DataFrame()
                if instruments.empty:
                    raise ConfigurationError(
                        "cannot resolve the full market: the source serves no universe "
                        "dataset and the lake holds no instruments snapshot yet — run the "
                        "daily backfill once or pass --symbols/--universe-file"
                    ) from None
                resolved = sorted(to_canonical_symbol(s) for s in instruments["symbol"])
                logger.info(
                    "universe resolved from the lake's instruments snapshot: %d symbols",
                    len(resolved),
                )
        if limit is not None:
            resolved = resolved[:limit]
        return resolved

    def _ingest_instruments(
        self, symbols: Sequence[str], start: date, end: date, report: BackfillReport
    ) -> None:
        request = FetchRequest(Dataset.INSTRUMENTS, start, end)
        raw = self.adapter.fetch_raw(Dataset.INSTRUMENTS, request)
        canonical = self.adapter.normalize(Dataset.INSTRUMENTS, raw, request)
        if self.enrich_instruments:
            for symbol in symbols:
                single = FetchRequest(Dataset.INSTRUMENTS, start, end, symbol=symbol)
                try:
                    enriched = self.adapter.normalize(
                        Dataset.INSTRUMENTS,
                        self.adapter.fetch_raw(Dataset.INSTRUMENTS, single),
                        single,
                    )
                except PulsarDataError as exc:
                    logger.warning("instrument enrichment failed for %s: %s", symbol, exc)
                    continue
                if not enriched.empty:
                    canonical = pd.concat(
                        [canonical[canonical["symbol"] != symbol], enriched], ignore_index=True
                    )
        from .quality import check_canonical

        check_canonical(Dataset.INSTRUMENTS, canonical, request)
        partitions = self.lake.write(Dataset.INSTRUMENTS, canonical, source=self.adapter.source_id)
        self.lake.update_watermark(
            source=self.adapter.source_id,
            dataset=Dataset.INSTRUMENTS.value,
            partitions=partitions,
            rows=len(canonical),
            synced_through=end,
        )
        report.instrument_rows += len(canonical)
        logger.info("instruments ingested: %d rows", len(canonical))

    def _ingest_suspensions(
        self, symbols: Sequence[str], start: date, end: date, report: BackfillReport
    ) -> None:
        request = FetchRequest(Dataset.SUSPENSIONS, start, end)
        try:
            raw = self.adapter.fetch_raw(Dataset.SUSPENSIONS, request)
            canonical = self.adapter.normalize(Dataset.SUSPENSIONS, raw, request)
            result_rows = len(canonical)
            from .quality import check_canonical

            check_canonical(Dataset.SUSPENSIONS, canonical, request)
            wanted = set(symbols)
            canonical = canonical[canonical["symbol"].isin(wanted)].reset_index(drop=True)
            if not canonical.empty:
                self.lake.write(Dataset.SUSPENSIONS, canonical, source=self.adapter.source_id)
            self.lake.update_watermark(
                source=self.adapter.source_id,
                dataset=Dataset.SUSPENSIONS.value,
                partitions=["*"],
                rows=len(canonical),
                synced_through=end,
            )
            report.suspension_rows += len(canonical)
            logger.info("suspensions ingested: %d rows (raw %d)", len(canonical), result_rows)
        except PulsarDataError as exc:
            logger.warning("suspension ingestion failed; gaps will surface in report: %s", exc)

    def _ingest_bars(
        self, symbols: Sequence[str], start: date, end: date, report: BackfillReport
    ) -> None:
        for index, symbol in enumerate(symbols, start=1):
            if not self.force and self._bars_synced(symbol, start, end):
                report.skipped_symbols.append(symbol)
                continue
            request = FetchRequest(Dataset.BARS_1D, start, end, symbol=symbol)
            try:
                result = run_ingestion(
                    self.adapter, request, self.lake, quality_column_value="backfilled"
                )
                report.bars_rows += result.rows
            except PulsarDataError as exc:
                report.failed_symbols[symbol] = str(exc)[:300]
                logger.warning("bars backfill failed for %s: %s", symbol, exc)
            if index % 25 == 0:
                logger.info("bars progress: %d/%d", index, len(symbols))

    def _ingest_minute_bars(
        self, symbols: Sequence[str], start: date, end: date, report: BackfillReport
    ) -> None:
        """Per-symbol, per-calendar-year minute fetches into ``bars_<freq>``.

        One upstream call per symbol-year keeps single-call responses
        bounded, retries cheap and partitions whole; a year with no
        upstream coverage simply lands zero rows (the completeness walk
        files it under not_listed/coverage_end, never as a gap).
        """
        dataset = self.dataset
        years = range(start.year, end.year + 1)
        for index, symbol in enumerate(symbols, start=1):
            if not self.force and self._bars_synced(symbol, start, end):
                report.skipped_symbols.append(symbol)
                continue
            for year in years:
                window_start = max(start, date(year, 1, 1))
                window_end = min(end, date(year, 12, 31))
                if window_start > window_end:
                    continue
                request = FetchRequest(dataset, window_start, window_end, symbol=symbol)
                try:
                    result = run_ingestion(
                        self.adapter, request, self.lake, quality_column_value="backfilled"
                    )
                    report.bars_rows += result.rows
                except PulsarDataError as exc:
                    report.failed_symbols[symbol] = f"{dataset.value}: {str(exc)[:280]}"
                    logger.warning("minute backfill failed for %s (%d): %s", symbol, year, exc)
                    break
            if index % 5 == 0:
                logger.info("minute bars progress (%s): %d/%d", dataset.value, index, len(symbols))

    def _ingest_corporate_actions(
        self, symbols: Sequence[str], start: date, end: date, report: BackfillReport
    ) -> None:
        for symbol in symbols:
            request = FetchRequest(Dataset.CORPORATE_ACTIONS, start, end, symbol=symbol)
            try:
                result = run_ingestion(
                    self.adapter, request, self.lake, quality_column_value="ok"
                )
                report.corporate_action_rows += result.rows
            except PulsarDataError as exc:
                report.failed_symbols.setdefault(symbol, f"corporate_actions: {str(exc)[:200]}")
                logger.warning("corporate-action backfill failed for %s: %s", symbol, exc)

    # ------------------------------------------------------------ reporting
    def _bars_synced(self, symbol: str, start: date, end: date) -> bool:
        """True when a watermark says this symbol's bars cover the window end."""
        marks = self.lake.watermarks()
        if marks.empty:
            return False
        rows = marks[
            (marks["source"] == self.adapter.source_id)
            & (marks["dataset"] == self.dataset.value)
            & (marks["partition"].str.startswith(f"symbol={symbol}/"))
        ]
        for _, row in rows.iterrows():
            if pd.to_datetime(row["synced_through"]).date() >= end:
                return True
        return False

    def suspension_days(self, symbols: Iterable[str]) -> dict[str, set[date]]:
        """Expand stored suspension ranges to per-symbol day sets (trading days only)."""
        return suspension_days(self.lake, symbols)

    def completeness_report(
        self, symbols: Iterable[str], start: date, end: date
    ) -> dict[str, dict[str, int]]:
        """Classify each (symbol × trading day) cell; see :meth:`DataLake.completeness`."""
        symbol_list = list(symbols)
        suspension_days = self.suspension_days(symbol_list)
        return self.lake.completeness(
            start=start, end=end, symbols=symbol_list, suspension_days=suspension_days
        )
