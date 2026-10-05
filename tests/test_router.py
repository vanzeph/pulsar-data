"""Primary/backup routing: failover, circuit trips, degradation events.

The acceptance drill — 「主源故障注入演练自动降级并完成当日增量，
降级事件留痕」 — runs end to end at the bottom of this file: a dead
primary, the baostock adapter over a fake client, and the real
:class:`~pulsar_data.incremental.IncrementalRunner` on top.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import ConfigurationError, FetchError
from pulsar_data.incremental import IncrementalRunner
from pulsar_data.lake import DataLake
from pulsar_data.router import DegradationEvent, DegradationLog, SourceRouter
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts
from pulsar_data.sources import register_adapter
from pulsar_data.sources.base import FetchRequest
from tests.test_baostock_adapter import FakeBaostockClient

JAN = [f"2024-01-{day:02d}" for day in (2, 3, 4, 5, 8, 9)]


class StubAdapter:
    """Serves a day list; optionally fails with a configurable error."""

    source_id = "stub"

    def __init__(self, *, days=None, fail_times=0, error=None, source_id=None):
        self.days = list(days or [])
        self.fail_times = fail_times
        self.error = error or FetchError("upstream is down")
        self.calls = 0
        if source_id is not None:
            self.source_id = source_id

    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.error
        in_window = [d for d in self.days if request.start <= date.fromisoformat(d) <= request.end]
        return pd.DataFrame({"day": in_window})

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        if dataset is Dataset.BARS_1D:
            if raw.empty:
                return pd.DataFrame(columns=list(BAR_COLUMNS))
            return pd.DataFrame(
                {
                    "symbol": request.symbol,
                    "ts": [daily_ts(day) for day in raw["day"]],
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "volume": 100.0,
                    "amount": 1050.0,
                    "adjust_factor": 1.0,
                    "quality": "ok",
                }
            )[list(BAR_COLUMNS)]
        if dataset is Dataset.CALENDAR:
            return pd.DataFrame({"trade_date": [date.fromisoformat(d) for d in raw["day"]]})
        raise ConfigurationError(f"dataset {dataset!r} not supported by stub")


class DeadAdapter:
    """Fault injection: every call dies like a broken primary."""

    source_id = "akshare"

    def __init__(self):
        self.calls = 0

    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        self.calls += 1
        raise FetchError(f"connection to primary refused ({dataset.value})")

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        raise AssertionError("a dead source must never be asked to normalize")


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


# --------------------------------------------------------------------------
# Per-call failover
# --------------------------------------------------------------------------
def test_primary_failure_fails_over_and_records_event():
    primary = StubAdapter(source_id="primary")
    backup = StubAdapter(days=JAN, source_id="backup")
    router = SourceRouter([primary, backup], failure_threshold=5)
    request = FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")

    primary.fail_times = 1
    raw = router.fetch_raw(Dataset.BARS_1D, request)
    assert list(raw["day"]) == JAN
    assert router.serving_source_id == "backup"
    assert router.source_id == "backup"  # attribution follows the serving source
    assert len(router.degradations) == 1
    event = router.degradations[0]
    assert event.from_source == "primary"
    assert event.to_source == "backup"
    assert event.dataset == "bars_1d"
    assert event.symbol == "SH600519"
    assert event.tripped is False  # below threshold
    assert "FetchError" in event.reason


def test_normalize_delegates_to_serving_source():
    primary = StubAdapter(days=JAN[:2], source_id="primary")
    backup = StubAdapter(days=JAN, source_id="backup")
    router = SourceRouter([primary, backup])
    request = FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")

    raw = router.fetch_raw(Dataset.BARS_1D, request)
    canonical = router.normalize(Dataset.BARS_1D, raw, request)
    assert len(canonical) == 2  # primary served its own 2 days

    primary.fail_times = 99
    raw = router.fetch_raw(Dataset.BARS_1D, request)
    canonical = router.normalize(Dataset.BARS_1D, raw, request)
    assert len(canonical) == 6  # backup shape, backup days


def test_success_resets_consecutive_failures():
    primary = StubAdapter(days=JAN[:2], source_id="primary")
    backup = StubAdapter(days=JAN, source_id="backup")
    router = SourceRouter([primary, backup], failure_threshold=2)
    request = FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")

    primary.fail_times = 1
    router.fetch_raw(Dataset.BARS_1D, request)  # fail -> backup; failures(primary) == 1
    router.fetch_raw(Dataset.BARS_1D, request)  # primary first again; succeeds; counter reset
    assert router.serving_source_id == "primary"
    primary.fail_times = 99
    router.fetch_raw(Dataset.BARS_1D, request)  # failure 1 again -> still not tripped
    assert router.is_tripped("primary") is False
    assert router.active_source_id == "primary"  # configured order never drifts


# --------------------------------------------------------------------------
# Consecutive-failure trip (熔断)
# --------------------------------------------------------------------------
def test_consecutive_failures_trip_and_skip_primary():
    primary = StubAdapter(source_id="primary")
    backup = StubAdapter(days=JAN, source_id="backup")
    router = SourceRouter([primary, backup], failure_threshold=2, cooldown=300.0)
    request = FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")
    primary.fail_times = 99

    router.fetch_raw(Dataset.BARS_1D, request)  # failure 1 -> backup serves
    assert not router.is_tripped("primary")
    router.fetch_raw(Dataset.BARS_1D, request)  # failure 2 -> trips
    assert router.is_tripped("primary")
    assert primary.calls == 2

    router.fetch_raw(Dataset.BARS_1D, request)  # primary skipped: zero new calls
    assert primary.calls == 2
    assert backup.calls == 3
    assert router.serving_source_id == "backup"

    # the trip event is marked in the trail
    trips = [event for event in router.degradations if event.tripped]
    assert len(trips) == 1
    assert trips[0].consecutive_failures == 2
    assert trips[0].from_source == "primary"


def test_tripped_source_probes_again_after_cooldown():
    clock = _FakeClock()
    primary = StubAdapter(days=JAN[:2], source_id="primary")
    backup = StubAdapter(days=JAN, source_id="backup")
    router = SourceRouter([primary, backup], failure_threshold=1, cooldown=300.0, clock=clock)
    request = FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")

    primary.fail_times = 99
    router.fetch_raw(Dataset.BARS_1D, request)  # trips immediately (threshold 1)
    assert router.is_tripped("primary")
    assert primary.calls == 1

    clock.now += 301.0  # cooldown elapsed -> half-open probe
    assert router.is_tripped("primary") is False
    router.fetch_raw(Dataset.BARS_1D, request)
    assert primary.calls == 2  # probed once, failed, back to backup


def test_degradation_stops_at_first_healthy_source():
    dead_a = StubAdapter(source_id="a")
    dead_b = StubAdapter(source_id="b")
    healthy = StubAdapter(days=JAN, source_id="c")
    router = SourceRouter([dead_a, dead_b, healthy], failure_threshold=5)
    request = FetchRequest(Dataset.CALENDAR, date(2024, 1, 2), date(2024, 1, 9))
    dead_a.fail_times = dead_b.fail_times = 99

    raw = router.fetch_raw(Dataset.CALENDAR, request)
    assert list(raw["day"]) == JAN
    assert healthy.calls == 1
    assert [event.to_source for event in router.degradations] == ["b", "c"]


def test_all_sources_failed_raises_combined_error():
    dead_a = StubAdapter(source_id="a")
    dead_b = StubAdapter(source_id="b")
    router = SourceRouter([dead_a, dead_b])
    request = FetchRequest(Dataset.CALENDAR, date(2024, 1, 2), date(2024, 1, 9))
    dead_a.fail_times = dead_b.fail_times = 99

    with pytest.raises(FetchError, match="all sources failed.*a:.*b:"):
        router.fetch_raw(Dataset.CALENDAR, request)
    # the last event has nowhere to go
    assert router.degradations[-1].to_source is None


def test_non_routable_programming_error_propagates():
    class Buggy(StubAdapter):
        def fetch_raw(self, dataset, request):
            raise KeyError("adapter bug")

    router = SourceRouter([Buggy(source_id="buggy"), StubAdapter(days=JAN, source_id="ok")])
    request = FetchRequest(Dataset.CALENDAR, date(2024, 1, 2), date(2024, 1, 9))
    with pytest.raises(KeyError):
        router.fetch_raw(Dataset.CALENDAR, request)
    assert router.degradations == []  # programming errors are not degradations


def test_duplicate_source_ids_rejected():
    first = StubAdapter(days=JAN, source_id="dup")
    second = StubAdapter(days=JAN, source_id="dup")
    with pytest.raises(FetchError, match="duplicate source id"):
        SourceRouter([first, second])


def test_empty_router_rejected():
    with pytest.raises(FetchError, match="at least one source"):
        SourceRouter([])


# --------------------------------------------------------------------------
# Configuration wiring + durable event trail
# --------------------------------------------------------------------------
def test_from_config_builds_ordered_router():
    @register_adapter("router-stub-a")
    def _a(config=None):
        return StubAdapter(days=JAN[:2], source_id="router-stub-a", **(config or {}))

    @register_adapter("router-stub-b")
    def _b(config=None):
        return StubAdapter(days=JAN, source_id="router-stub-b")

    router = SourceRouter.from_config(
        [{"id": "router-stub-a"}, {"id": "router-stub-b"}], failure_threshold=2
    )
    assert router.source_ids == ["router-stub-a", "router-stub-b"]
    request = FetchRequest(Dataset.BARS_1D, date(2024, 1, 2), date(2024, 1, 9), "SH600519")
    raw = router.fetch_raw(Dataset.BARS_1D, request)
    assert len(raw) == 2


def test_from_config_requires_ids():
    with pytest.raises(FetchError, match="needs an 'id'"):
        SourceRouter.from_config([{"source": "akshare"}])


def test_degradation_log_persists_jsonl(tmp_path):
    log = DegradationLog.for_lake(tmp_path / "lake")
    event = DegradationEvent(
        occurred_at="2026-10-05T09:00:00",
        from_source="akshare",
        to_source="baostock",
        dataset="bars_1d",
        symbol="SH600519",
        reason="FetchError: upstream is down",
        consecutive_failures=3,
        tripped=True,
    )
    log.record(event)
    log.record(event)
    entries = log.read()
    assert len(entries) == 2
    assert entries[0]["from_source"] == "akshare"
    assert entries[0]["tripped"] is True
    assert (tmp_path / "lake" / "_meta" / "degradation_events.jsonl").exists()


def test_router_writes_event_log(tmp_path):
    primary = StubAdapter(source_id="primary")
    backup = StubAdapter(days=JAN, source_id="backup")
    log = DegradationLog.for_lake(tmp_path / "lake")
    router = SourceRouter([primary, backup], failure_threshold=2, event_log=log)
    request = FetchRequest(Dataset.CALENDAR, date(2024, 1, 2), date(2024, 1, 9))
    primary.fail_times = 99

    router.fetch_raw(Dataset.CALENDAR, request)
    router.fetch_raw(Dataset.CALENDAR, request)
    entries = log.read()
    assert len(entries) == 2
    assert entries[0]["from_source"] == "primary"
    assert entries[1]["tripped"] is True
    assert entries[1]["dataset"] == "calendar"


# --------------------------------------------------------------------------
# THE DRILL: 主源故障注入 → 自动降级 baostock → 完成当日增量
# --------------------------------------------------------------------------
def test_fault_injected_primary_degrades_and_daily_increment_completes(tmp_path):
    """Acceptance: dead primary, live backup, real IncrementalRunner.

    The increment must complete through the baostock adapter (fake
    client), land all six January days with quality ``ok``, attribute
    watermarks to ``baostock``, and leave a structured degradation trail.
    """
    lake = DataLake(tmp_path / "lake")
    dead_primary = DeadAdapter()  # fault injection: akshare is gone
    from pulsar_data.sources.baostock import BaostockSourceAdapter

    backup = BaostockSourceAdapter(client=FakeBaostockClient())
    event_log = DegradationLog.for_lake(lake.root)
    router = SourceRouter(
        [dead_primary, backup],
        failure_threshold=2,
        cooldown=300.0,
        event_log=event_log,
    )
    runner = IncrementalRunner(
        router,
        lake,
        initial_start=date(2024, 1, 2),
        include_instruments=False,
        include_suspensions=False,
        include_corporate_actions=False,
    )

    report = runner.run(date(2024, 1, 9), symbols=["SH600519"])

    # 1) the increment completed and landed the full window
    assert report.failed_symbols == {}
    assert report.bars_rows == 6
    bars = lake.read(Dataset.BARS_1D)
    assert len(bars) == 6
    assert set(bars["quality"]) == {"ok"}
    days = sorted(str(pd.Timestamp(ts).date()) for ts in bars["ts"])
    assert days == JAN

    # 2) the calendar came through the backup too
    calendar_days = [str(day) for day in lake.read(Dataset.CALENDAR)["trade_date"]]
    assert calendar_days == JAN

    # 3) attribution: bars were written and watermarked by baostock
    marks = lake.watermarks()
    bars_marks = marks[(marks["dataset"] == "bars_1d") & (marks["partition"] != "*")]
    assert (bars_marks["source"] == "baostock").all()
    assert (bars_marks["synced_through"] == "2024-01-09").all()

    # 4) degradation trail: calendar + bars both failed over; the bars
    #    failure (2nd) tripped the primary, so later calls skip it
    entries = event_log.read()
    assert [entry["dataset"] for entry in entries] == ["calendar", "bars_1d"]
    assert all(entry["from_source"] == "akshare" for entry in entries)
    assert all(entry["to_source"] == "baostock" for entry in entries)
    assert [entry["tripped"] for entry in entries] == [False, True]
    assert dead_primary.calls == 2  # one calendar + one bars attempt, then tripped

    # 5) the run is idempotent: re-running the same end changes nothing
    partition = lake.root / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet"
    snapshot = partition.read_bytes()
    again = runner.run(date(2024, 1, 9), symbols=["SH600519"])
    assert partition.read_bytes() == snapshot
    # only the watermark day is re-fetched (overlap healing), no duplicates
    assert again.bars_rows == 1
    assert not lake.read(Dataset.BARS_1D).duplicated(subset=["symbol", "ts"]).any()
    assert dead_primary.calls == 2  # still tripped, zero wasted attempts
