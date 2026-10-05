"""Gap-driven backfill: detection, task list, and the repair executor.

Design mapping — 数据质量与补数 §完整性 + §补数幂等:

* 「完整性：以交易日历为基准检测缺 bar；停牌有标记，缺数据无标记即报
  缺口」 — :func:`detect_backfill_tasks` projects the D1 completeness
  walk (``ok / not_listed / coverage_end / suspended / gap``) onto the
  ``gap`` slice only: every unexplained missing trading day becomes
  part of a :class:`BackfillTask` for the partition (``symbol × year``)
  that should hold it.
* 「补数幂等：补数任务按分区整区覆盖写，重复执行结果一致」 —
  :class:`BackfillTaskExecutor` re-fetches the *whole calendar year*
  of a partition through the standard pipeline
  (fetch_raw → normalize → quality gate) and lands it with the lake's
  whole-partition atomic replace, rows marked ``backfilled``.  Running
  the same task twice fetches the same frame and writes identical
  bytes.
* Resumability — completed task ids are persisted to
  ``<lake>/_meta/backfill_tasks.json`` after each task, so a crashed
  run resumes where it stopped (done tasks are skipped unless
  ``force=True``).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

from .backfill import suspension_days
from .errors import PulsarDataError
from .lake import DataLake
from .schema import Dataset, ts_to_date
from .sources.base import FetchRequest, SourceAdapter, run_ingestion
from .symbols import to_canonical_symbol

logger = logging.getLogger("pulsar_data.gapfill")

__all__ = [
    "BackfillTask",
    "TaskExecution",
    "GapBackfillReport",
    "detect_backfill_tasks",
    "BackfillTaskExecutor",
]

#: Statuses a :class:`TaskExecution` can carry.
TASK_DONE = "done"
TASK_FAILED = "failed"
TASK_SKIPPED = "skipped"


@dataclass(frozen=True)
class BackfillTask:
    """Repair one bars partition (``symbol × year``) that holds gap days.

    ``missing_days`` are the unexplained trading days the detector
    attributed to this partition; the repair itself always re-fetches
    and rewrites the full calendar year so the partition lands as one
    consistent whole (整区覆盖写).
    """

    symbol: str
    year: int
    missing_days: tuple[date, ...]

    @property
    def task_id(self) -> str:
        """Deterministic identity: one task per bars partition."""
        return f"bars_1d/symbol={self.symbol}/year={self.year}"

    @property
    def partition(self) -> str:
        """Hive-style partition id the task overwrites."""
        return f"symbol={self.symbol}/year={self.year}"

    @property
    def window(self) -> tuple[date, date]:
        """Fetch window: the full calendar year of the partition."""
        return date(self.year, 1, 1), date(self.year, 12, 31)

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "symbol": self.symbol,
            "year": self.year,
            "partition": self.partition,
            "missing_days": [day.isoformat() for day in self.missing_days],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BackfillTask":
        return cls(
            symbol=str(payload["symbol"]),
            year=int(payload["year"]),
            missing_days=tuple(date.fromisoformat(str(day)) for day in payload["missing_days"]),
        )


def detect_backfill_tasks(
    lake: DataLake,
    *,
    start: date,
    end: date,
    symbols: Iterable[str] | None = None,
) -> list[BackfillTask]:
    """Turn the completeness ``gap`` cells into a backfill task list.

    Suspend-marked days, pre-listing days and after-coverage days are
    explained absences and never generate tasks — only unexplained
    missing bars do (缺数据无标记即报缺口).
    """
    if symbols is not None:
        wanted = [to_canonical_symbol(symbol) for symbol in symbols]
    else:
        bars = lake.read(Dataset.BARS_1D)
        wanted = sorted({to_canonical_symbol(str(s)) for s in bars["symbol"]}) if not bars.empty else []
    if not wanted:
        return []
    gaps = lake.gap_days(
        start=start,
        end=end,
        symbols=wanted,
        suspension_days=suspension_days(lake, wanted),
    )
    tasks: list[BackfillTask] = []
    for symbol, days in sorted(gaps.items()):
        by_year: dict[int, list[date]] = {}
        for day in days:
            by_year.setdefault(day.year, []).append(day)
        for year, year_days in sorted(by_year.items()):
            tasks.append(BackfillTask(symbol, year, tuple(sorted(year_days))))
    return tasks


@dataclass
class TaskExecution:
    """Outcome of executing one :class:`BackfillTask`."""

    task: BackfillTask
    status: str  # done | failed | skipped
    rows: int = 0
    detail: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = self.task.to_dict()
        payload.update({"status": self.status, "rows": self.rows, "detail": self.detail})
        return payload


@dataclass
class GapBackfillReport:
    """Summary of one executor run (serializable for audits/CLI)."""

    source: str
    results: list[TaskExecution] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    @property
    def done(self) -> list[TaskExecution]:
        return [item for item in self.results if item.status == TASK_DONE]

    @property
    def failed(self) -> list[TaskExecution]:
        return [item for item in self.results if item.status == TASK_FAILED]

    @property
    def skipped(self) -> list[TaskExecution]:
        return [item for item in self.results if item.status == TASK_SKIPPED]

    @property
    def rows_written(self) -> int:
        return sum(item.rows for item in self.results if item.status == TASK_DONE)

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "generated_at": self.generated_at,
            "tasks": len(self.results),
            "done": len(self.done),
            "failed": len(self.failed),
            "skipped": len(self.skipped),
            "rows_written": self.rows_written,
            "results": [item.to_dict() for item in self.results],
        }

    def to_json(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return target


class BackfillTaskExecutor:
    """Executes gap-repair tasks with whole-partition overwrite writes.

    Idempotency: each task re-fetches its partition's full calendar
    year and replaces the partition in one atomic write (rows marked
    ``backfilled``), so executing the same task twice yields identical
    partition content — the lake never accumulates duplicate rows from
    repairs.  Resumability: task ids that reached ``done`` are
    persisted after each task; a later run skips them unless
    ``force=True``.
    """

    def __init__(self, adapter: SourceAdapter, lake: DataLake, *, state_path: str | Path | None = None) -> None:
        self.adapter = adapter
        self.lake = lake
        self.state_path = Path(state_path) if state_path is not None else (
            lake.root / "_meta" / "backfill_tasks.json"
        )

    # ------------------------------------------------------------------ run
    def execute(self, tasks: Sequence[BackfillTask], *, force: bool = False) -> GapBackfillReport:
        """Run ``tasks`` in order; skip already-done ids unless ``force``."""
        report = GapBackfillReport(source=self.adapter.source_id)
        state = self._load_state()
        for task in tasks:
            if not force and task.task_id in state.get("done", {}):
                logger.info("task %s already done; skipping", task.task_id)
                report.results.append(
                    TaskExecution(task, TASK_SKIPPED, detail="already done in a previous run")
                )
                continue
            report.results.append(self._execute_one(task, state))
        self._save_state(state)
        return report

    def _execute_one(self, task: BackfillTask, state: dict) -> TaskExecution:
        window_start, window_end = task.window
        try:
            result = run_ingestion(
                self.adapter,
                FetchRequest(Dataset.BARS_1D, window_start, window_end, symbol=task.symbol),
                self.lake,
                quality_column_value="backfilled",
            )
        except PulsarDataError as exc:
            logger.warning("gap backfill failed for %s: %s", task.task_id, exc)
            return TaskExecution(task, TASK_FAILED, detail=str(exc)[:300])
        remaining = self._remaining_gaps(task)
        if remaining:
            detail = (
                f"{len(remaining)} of {len(task.missing_days)} gap days still missing "
                f"after repair (first: {remaining[0].isoformat()})"
            )
            logger.warning("task %s incomplete: %s", task.task_id, detail)
            return TaskExecution(task, TASK_FAILED, rows=result.rows, detail=detail)
        state.setdefault("done", {})[task.task_id] = {
            "partition": task.partition,
            "rows": result.rows,
            "completed_at": datetime.now().isoformat(timespec="seconds"),
        }
        self._save_state(state)  # checkpoint after each completed task
        return TaskExecution(task, TASK_DONE, rows=result.rows)

    def _remaining_gaps(self, task: BackfillTask) -> list[date]:
        """Which of the task's gap days are still absent after the repair."""
        bars = self.lake.read(Dataset.BARS_1D, symbols=[task.symbol])
        if bars.empty:
            return list(task.missing_days)
        present = {ts_to_date(ts) for ts in bars["ts"]}
        return [day for day in task.missing_days if day not in present]

    # ----------------------------------------------------------------- state
    def _load_state(self) -> dict:
        if not self.state_path.exists():
            return {"done": {}}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"done": {}}
        if not isinstance(payload, dict) or not isinstance(payload.get("done", {}), dict):
            return {"done": {}}
        return payload

    def _save_state(self, state: dict) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_name(f".tmp-{uuid.uuid4().hex}.json")
        tmp.write_text(
            json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(tmp, self.state_path)

    # ----------------------------------------------------------- convenience
    def detect_and_execute(
        self, *, start: date, end: date, symbols: Iterable[str] | None = None, force: bool = False
    ) -> tuple[list[BackfillTask], GapBackfillReport]:
        """Detect the lake's gaps, then execute the resulting task list."""
        tasks = detect_backfill_tasks(self.lake, start=start, end=end, symbols=symbols)
        return tasks, self.execute(tasks, force=force)
