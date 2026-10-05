"""Gap detection and idempotent gap repair (缺口检测与幂等补数).

Acceptance anchors from the task:

* 缺口检测用例 — construct a lake with holes, detect them, and get a
  backfill task list out;
* 补数幂等 — executing the same repair task twice leaves the partition
  bit-identical;
* 断点续跑 — done tasks are skipped on re-run; failed ones are retried.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import FetchError, QualityViolation
from pulsar_data.gapfill import (
    BackfillTask,
    BackfillTaskExecutor,
    detect_backfill_tasks,
)
from pulsar_data.lake import DataLake
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts

# A compact 2024 "calendar": 12 trading weeks (Mon–Fri), Jan 1 – Mar 25.
CALENDAR_DAYS = list(pd.bdate_range("2024-01-01", "2024-03-25").date)


class TableAdapter:
    """Serves canonical daily bars from an in-memory close table.

    The table maps ``symbol -> {iso day: close}``; days inside the
    request window are served, everything else ignored.  Symbols listed
    in ``fail_symbols`` raise :class:`FetchError` — used to exercise
    the executor's resumability.
    """

    source_id = "fake"

    def __init__(
        self,
        tables: dict[str, dict[str, float]],
        *,
        fail_symbols: set[str] | None = None,
    ) -> None:
        self.tables = tables
        self.fail_symbols = set(fail_symbols or ())
        self.calls: list[tuple[str, date, date]] = []

    def fetch_raw(self, dataset: Dataset, request) -> pd.DataFrame:
        assert dataset is Dataset.BARS_1D
        self.calls.append((request.symbol, request.start, request.end))
        if request.symbol in self.fail_symbols:
            raise FetchError(f"upstream unavailable for {request.symbol}")
        table = self.tables.get(request.symbol, {})
        days = sorted(
            day
            for day in table
            if request.start.isoformat() <= day <= request.end.isoformat()
        )
        return pd.DataFrame({"day": days})

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request) -> pd.DataFrame:
        table = self.tables.get(request.symbol, {})
        days = list(raw["day"])
        return pd.DataFrame(
            {
                "symbol": request.symbol,
                "ts": [daily_ts(day) for day in days],
                "open": [table[day] * 0.99 for day in days],
                "high": [table[day] * 1.02 for day in days],
                "low": [table[day] * 0.98 for day in days],
                "close": [table[day] for day in days],
                "volume": [1_000_000.0 for _ in days],
                "amount": [table[day] * 1_000_000.0 for day in days],
                "adjust_factor": 1.0,
                "quality": "ok",
            }
        )[list(BAR_COLUMNS)]


def _closes(values: list[float]) -> dict[str, float]:
    return {day.isoformat(): value for day, value in zip(CALENDAR_DAYS, values)}


FULL_TABLE = {"SH600519": _closes([10.0 + i * 0.01 for i in range(len(CALENDAR_DAYS))])}

START, END = date(2024, 1, 1), date(2024, 3, 25)


def _seed_calendar(lake: DataLake) -> None:
    lake.write(
        Dataset.CALENDAR, pd.DataFrame({"trade_date": CALENDAR_DAYS}), source="fake"
    )


def _seed_bars(lake: DataLake, frame: pd.DataFrame, *, quality: str = "ok") -> None:
    frame = frame.copy()
    frame["quality"] = quality
    lake.write(Dataset.BARS_1D, frame, source="fake")


def _bars_frame(symbol: str, days: list[date], closes: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts": [daily_ts(day) for day in days],
            "open": [closes[day.isoformat()] * 0.99 for day in days],
            "high": [closes[day.isoformat()] * 1.02 for day in days],
            "low": [closes[day.isoformat()] * 0.98 for day in days],
            "close": [closes[day.isoformat()] for day in days],
            "volume": [1_000_000.0 for _ in days],
            "amount": [closes[day.isoformat()] * 1_000_000.0 for day in days],
            "adjust_factor": 1.0,
            "quality": "ok",
        }
    )[list(BAR_COLUMNS)]


def _partition_files(lake: DataLake) -> dict[str, bytes]:
    return {
        path.relative_to(lake.root).as_posix(): path.read_bytes()
        for path in sorted((lake.root / "bars_1d").rglob("part.parquet"))
    }


@pytest.fixture()
def holed_lake(tmp_path) -> DataLake:
    """A lake whose SH600519 partition misses three middle trading days."""
    lake = DataLake(tmp_path / "lake")
    _seed_calendar(lake)
    hole_days = {CALENDAR_DAYS[20], CALENDAR_DAYS[21], CALENDAR_DAYS[22]}
    kept = [day for day in CALENDAR_DAYS if day not in hole_days]
    _seed_bars(lake, _bars_frame("SH600519", kept, FULL_TABLE["SH600519"]), quality="backfilled")
    return lake


# ---------------------------------------------------------------------------
# Gap detection: construct a hole -> detect it -> get a backfill task
# ---------------------------------------------------------------------------
def test_detect_converts_gap_cells_into_tasks(holed_lake: DataLake):
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)
    assert len(tasks) == 1
    task = tasks[0]
    assert task.symbol == "SH600519"
    assert task.year == 2024
    assert task.partition == "symbol=SH600519/year=2024"
    assert task.missing_days == (
        CALENDAR_DAYS[20],
        CALENDAR_DAYS[21],
        CALENDAR_DAYS[22],
    )
    assert task.window == (date(2024, 1, 1), date(2024, 12, 31))


def test_task_id_is_deterministic_and_partitions_unique():
    first = BackfillTask("SH600519", 2024, (date(2024, 2, 1),))
    second = BackfillTask("SH600519", 2024, (date(2024, 2, 1),))
    assert first.task_id == second.task_id
    assert BackfillTask("SH600519", 2025, ()).task_id != first.task_id


def test_task_serialization_round_trip():
    task = BackfillTask("SZ000001", 2023, (date(2023, 3, 1), date(2023, 3, 2)))
    restored = BackfillTask.from_dict(task.to_dict())
    assert restored == task


def test_complete_lake_yields_no_tasks(tmp_path):
    lake = DataLake(tmp_path / "lake")
    _seed_calendar(lake)
    _seed_bars(lake, _bars_frame("SH600519", CALENDAR_DAYS, FULL_TABLE["SH600519"]))
    assert detect_backfill_tasks(lake, start=START, end=END) == []


def test_suspended_and_coverage_days_are_not_gaps(tmp_path):
    """D1 taxonomy: suspension-marked / pre-IPO / after-coverage never repair."""
    lake = DataLake(tmp_path / "lake")
    _seed_calendar(lake)
    # bars only cover trading weeks 2..10: week 1 is not_listed? no —
    # week 1 days are before the first bar => not_listed, and trailing
    # weeks after the last bar => coverage_end.
    covered = CALENDAR_DAYS[5:50]
    _seed_bars(lake, _bars_frame("SH600519", covered, FULL_TABLE["SH600519"]))
    # one suspended day inside the covered span
    suspended_day = covered[10]
    holed = [day for day in covered if day != suspended_day]
    lake.write(
        Dataset.BARS_1D, _bars_frame("SH600519", holed, FULL_TABLE["SH600519"]), source="fake"
    )
    lake.write(
        Dataset.SUSPENSIONS,
        pd.DataFrame(
            {
                "symbol": ["SH600519"],
                "start_date": [suspended_day],
                "end_date": [suspended_day],
                "reason": ["planned"],
            }
        ),
        source="fake",
    )
    tasks = detect_backfill_tasks(lake, start=START, end=END)
    assert tasks == []  # the suspended day is explained, edges are coverage


def test_multi_year_gaps_split_per_partition(tmp_path):
    """Gap days in different years produce one task per partition."""
    lake = DataLake(tmp_path / "lake")
    days = list(pd.bdate_range("2023-12-01", "2024-01-31").date)
    lake.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": days}), source="fake")
    closes = {day.isoformat(): 10.0 for day in days}
    hole = days[10]  # December 2023
    hole24 = days[-10]  # January 2024
    kept = [day for day in days if day not in (hole, hole24)]
    _seed_bars(lake, _bars_frame("SH600519", kept, closes))
    tasks = detect_backfill_tasks(lake, start=days[0], end=days[-1])
    assert [(task.year, len(task.missing_days)) for task in tasks] == [(2023, 1), (2024, 1)]


# ---------------------------------------------------------------------------
# Repair executor: heal, idempotency, resumability
# ---------------------------------------------------------------------------
def test_execute_heals_the_gap(holed_lake: DataLake):
    executor = BackfillTaskExecutor(TableAdapter(FULL_TABLE), holed_lake)
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)
    report = executor.execute(tasks)

    assert len(report.done) == 1
    assert report.failed == []
    bars = holed_lake.read(Dataset.BARS_1D)
    assert len(bars) == len(CALENDAR_DAYS)  # the three hole days landed
    # repaired rows carry the backfilled mark
    repaired = bars[bars["quality"] == "backfilled"]
    assert len(repaired) == len(CALENDAR_DAYS)
    # the gap is gone from the detector's point of view
    assert detect_backfill_tasks(holed_lake, start=START, end=END) == []


def test_repair_is_idempotent_bit_for_bit(holed_lake: DataLake):
    """Acceptance: running the repair twice leaves identical partitions."""
    adapter = TableAdapter(FULL_TABLE)
    executor = BackfillTaskExecutor(adapter, holed_lake)
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)

    first = executor.execute(tasks, force=True)
    after_first = _partition_files(holed_lake)

    second = executor.execute(tasks, force=True)
    after_second = _partition_files(holed_lake)

    assert len(first.done) == len(second.done) == 1
    assert after_first == after_second  # 逐位一致
    # and the lake still holds exactly one row per trading day
    bars = holed_lake.read(Dataset.BARS_1D)
    assert len(bars) == len(CALENDAR_DAYS)
    assert not bars.duplicated(subset=["symbol", "ts"]).any()


def test_state_skips_done_tasks_and_resumes_failed(holed_lake: DataLake):
    """断点续跑: done tasks are skipped; failed tasks are retried and heal."""
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)

    failing = TableAdapter(FULL_TABLE, fail_symbols={"SH600519"})
    first_run = BackfillTaskExecutor(failing, holed_lake).execute(tasks)
    assert len(first_run.failed) == 1
    assert first_run.done == []

    healing = TableAdapter(FULL_TABLE)
    executor = BackfillTaskExecutor(healing, holed_lake)
    second_run = executor.execute(tasks)  # failed task is not state-blocked
    assert len(second_run.done) == 1
    calls_after_healing = len(healing.calls)

    third_run = BackfillTaskExecutor(healing, holed_lake).execute(tasks)
    assert len(third_run.skipped) == 1  # now done -> skipped without fetching
    assert len(healing.calls) == calls_after_healing  # no upstream call for a done task

    assert detect_backfill_tasks(holed_lake, start=START, end=END) == []


def test_execute_reports_failure_when_source_stays_empty(holed_lake: DataLake):
    """A source that returns nothing leaves the task failed, not done."""
    empty = TableAdapter({})
    executor = BackfillTaskExecutor(empty, holed_lake)
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)
    report = executor.execute(tasks)
    assert len(report.failed) == 1
    assert "still missing" in report.failed[0].detail
    # the state file must not mark the failed task done
    import json

    state = json.loads((holed_lake.root / "_meta" / "backfill_tasks.json").read_text())
    assert state["done"] == {}


def test_executor_report_serializes(tmp_path, holed_lake: DataLake):
    import json

    executor = BackfillTaskExecutor(TableAdapter(FULL_TABLE), holed_lake)
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)
    report = executor.execute(tasks)
    artifact = report.to_json(tmp_path / "repair.json")
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["done"] == 1
    assert payload["failed"] == 0
    assert payload["results"][0]["status"] == "done"
    assert payload["results"][0]["missing_days"] == [
        day.isoformat() for day in tasks[0].missing_days
    ]


def test_repair_refuses_non_canonical_quality_marks(holed_lake: DataLake):
    """The quality gate rejects frames carrying unknown marks."""
    bad = TableAdapter(FULL_TABLE)

    def normalize(dataset, raw, request):  # deliberately poison the marks
        frame = TableAdapter.normalize(bad, dataset, raw, request)
        frame["quality"] = "maybe"
        return frame

    bad.normalize = normalize  # type: ignore[method-assign]
    executor = BackfillTaskExecutor(bad, holed_lake)
    tasks = detect_backfill_tasks(holed_lake, start=START, end=END)
    report = executor.execute(tasks)
    assert len(report.failed) == 1
    assert "quality" in report.failed[0].detail


# ---------------------------------------------------------------------------
# Offline CLI: repair closes a tampered hole; quality prints the summary
# ---------------------------------------------------------------------------
def test_cli_repair_heals_tampered_partition(fixture_dir, tmp_path, capsys):
    """backfill -> punch a hole -> pulsar-data repair -> hole gone, exit 0."""
    from pulsar_data.cli import main

    lake_dir = tmp_path / "cli-repair-lake"
    assert main(
        ["backfill", "--source", "akshare", "--lake", str(lake_dir),
         "--start", "2024-01-01", "--end", "2024-12-31", "--symbols", "SH600519",
         "--fixture-dir", str(fixture_dir)]
    ) == 0
    partition = lake_dir / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    frame = pd.read_parquet(partition)
    holed = frame.drop(index=100).reset_index(drop=True)  # a genuine mid-year gap
    holed.to_parquet(partition, index=False)
    # sanity: the tampered hole is detectable
    assert main(
        ["verify", "--lake", str(lake_dir), "--start", "2024-01-01",
         "--end", "2024-12-31", "--symbols", "SH600519"]
    ) == 1

    report_path = tmp_path / "repair-report.json"
    task_path = tmp_path / "tasks.json"
    code = main(
        ["repair", "--source", "akshare", "--lake", str(lake_dir),
         "--start", "2024-01-01", "--end", "2024-12-31", "--symbols", "SH600519",
         "--fixture-dir", str(fixture_dir), "--report", str(report_path),
         "--task-list", str(task_path)]
    )
    assert code == 0, capsys.readouterr().out
    out = capsys.readouterr().out
    assert "detected 1 backfill task(s)" in out
    assert "done=1 failed=0" in out

    import json

    tasks_payload = json.loads(task_path.read_text(encoding="utf-8"))
    assert len(tasks_payload) == 1
    assert tasks_payload[0]["partition"] == "symbol=SH600519/year=2024"
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_payload["done"] == 1

    healed = pd.read_parquet(partition)
    assert len(healed) == len(frame)  # the hole is closed, nothing duplicated
    assert main(
        ["verify", "--lake", str(lake_dir), "--start", "2024-01-01",
         "--end", "2024-12-31", "--symbols", "SH600519"]
    ) == 0


def test_cli_quality_reports_partition_marks(fixture_dir, tmp_path, capsys):
    from pulsar_data.cli import main

    lake_dir = tmp_path / "cli-quality-lake"
    assert main(
        ["backfill", "--source", "akshare", "--lake", str(lake_dir),
         "--start", "2024-01-01", "--end", "2024-12-31", "--symbols", "SH600519",
         "--fixture-dir", str(fixture_dir)]
    ) == 0
    report_path = tmp_path / "quality.json"
    code = main(["quality", "--lake", str(lake_dir), "--report", str(report_path)])
    assert code == 0
    out = capsys.readouterr().out
    assert "partitions=1" in out
    assert "backfilled=" in out

    import json

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["partitions"] == 1
    partition = payload["per_partition"][0]
    assert partition["partition"] == "symbol=SH600519/year=2024"
    assert partition["counts"]["backfilled"] == partition["rows"]
    assert partition["suspect_days"] == []
