"""Quality marks: suspect marking, read-side filtering, partition report.

Acceptance anchors from the task:

* suspect 标记可被读侧过滤 — a query-layer ``quality`` filter both
  *includes* marked rows when asked for them and *excludes* them
  otherwise (two-sided assertions);
* 与 D2 crosscheck 事件打通 — a real ``CrossValidator`` mismatch ends
  up as ``suspect`` rows in the lake;
* mark/repair writes are idempotent.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from pulsar_data.crosscheck import CrossValidator
from pulsar_data.errors import LakeError, QualityViolation
from pulsar_data.lake import DataLake
from pulsar_data.port import LakeMarketDataPort
from pulsar_data.quality import check_canonical
from pulsar_data.quality_marks import mark_suspect
from pulsar_data.quality_report import build_quality_report
from pulsar_data.query import LakeQuery
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts

# ---------------------------------------------------------------------------
# lake fixture: 2 symbols x 10 trading days (2024-01-02 .. 2024-01-15)
# ---------------------------------------------------------------------------
DAYS = list(pd.bdate_range("2024-01-02", "2024-01-15").date)
START, END = date(2024, 1, 2), date(2024, 1, 15)


def _bars_frame(symbol: str, quality: str = "ok") -> pd.DataFrame:
    close = 10.0 if symbol == "SH600519" else 5.0
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts": [daily_ts(day) for day in DAYS],
            "open": [close * 0.99] * len(DAYS),
            "high": [close * 1.02] * len(DAYS),
            "low": [close * 0.98] * len(DAYS),
            "close": [close] * len(DAYS),
            "volume": [1_000_000.0] * len(DAYS),
            "amount": [close * 1_000_000.0] * len(DAYS),
            "adjust_factor": 1.0,
            "quality": quality,
        }
    )[list(BAR_COLUMNS)]


@pytest.fixture()
def lake(tmp_path) -> DataLake:
    lake = DataLake(tmp_path / "lake")
    lake.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": DAYS}), source="test")
    lake.write(Dataset.BARS_1D, _bars_frame("SH600519", quality="ok"), source="test")
    lake.write(Dataset.BARS_1D, _bars_frame("SZ000001", quality="backfilled"), source="test")
    return lake


def _event(symbol: str, trade_date: str):
    from pulsar_data.crosscheck import CrossCheckEvent

    return CrossCheckEvent(
        type="cross_source_mismatch",
        source_primary="akshare",
        source_secondary="baostock",
        symbol=symbol,
        trade_date=trade_date,
        field="close",
        value_primary=10.5,
        value_secondary=10.0,
        relative_diff=0.05,
        tolerance=0.001,
    )


def _partition_files(lake: DataLake) -> dict[str, bytes]:
    return {
        path.relative_to(lake.root).as_posix(): path.read_bytes()
        for path in sorted((lake.root / "bars_1d").rglob("part.parquet"))
    }


# ---------------------------------------------------------------------------
# mark_suspect: row-level, partition-atomic, idempotent
# ---------------------------------------------------------------------------
def test_mark_suspect_marks_only_named_rows(lake: DataLake):
    result = mark_suspect(
        lake,
        [_event("SH600519", DAYS[2].isoformat()), _event("SH600519", DAYS[5].isoformat())],
    )
    assert result.rows_marked == 2
    assert result.partitions == ["symbol=SH600519/year=2024"]
    assert result.unmatched_cells == []

    bars = lake.read(Dataset.BARS_1D)
    moutai = bars[bars["symbol"] == "SH600519"].reset_index(drop=True)
    marked = moutai[moutai["quality"] == "suspect"]
    assert list(marked["ts"].map(lambda ts: ts.date())) == [DAYS[2], DAYS[5]]
    # every other row of the partition keeps its previous mark
    assert (moutai.loc[moutai["quality"] != "suspect", "quality"] == "ok").all()
    # and other partitions are untouched
    pingan = bars[bars["symbol"] == "SZ000001"]
    assert (pingan["quality"] == "backfilled").all()


def test_mark_suspect_is_idempotent(lake: DataLake):
    events = [_event("SH600519", DAYS[2].isoformat())]
    mark_suspect(lake, events)
    after_first = _partition_files(lake)
    again = mark_suspect(lake, events)
    assert again.rows_marked == 1  # the row is (re)matched, content unchanged
    assert _partition_files(lake) == after_first


def test_mark_suspect_from_serialized_event_dicts(lake: DataLake):
    """A stored crosscheck JSON report can be replayed onto the lake."""
    event = _event("SZ000001", DAYS[3].isoformat())
    mark_suspect(lake, [event.to_dict()])
    bars = lake.read(Dataset.BARS_1D)
    pingan = bars[bars["symbol"] == "SZ000001"].reset_index(drop=True)
    assert pingan.loc[3, "quality"] == "suspect"
    assert (pingan.loc[pingan.index != 3, "quality"] == "backfilled").all()


def test_mark_suspect_ignores_out_of_lake_dates(lake: DataLake):
    result = mark_suspect(lake, [_event("SH600519", "2019-07-01")])
    assert result.rows_marked == 0
    assert result.unmatched_cells == ["SH600519:2019-07-01"]
    assert result.partitions == []


def test_mark_suspect_accepts_crosscheck_report_object(lake: DataLake):
    """mark_suspect consumes a whole CrossCheckReport directly."""

    class _Report:
        events = [_event("SH600519", DAYS[1].isoformat())]

    result = mark_suspect(lake, _Report())  # type: ignore[arg-type]
    assert result.rows_marked == 1


# ---------------------------------------------------------------------------
# D2 integration: a real crosscheck mismatch lands as suspect rows
# ---------------------------------------------------------------------------
class _CheckAdapter:
    """Serves flat daily bars with optional per-day close overrides."""

    def __init__(self, source_id: str, close: float, overrides: dict[str, float] | None = None):
        self.source_id = source_id
        self.close = close
        self.overrides = overrides or {}

    def fetch_raw(self, dataset: Dataset, request):
        return pd.DataFrame({"day": [day.isoformat() for day in DAYS]})

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request):
        closes = {day: self.overrides.get(day, self.close) for day in raw["day"]}
        return pd.DataFrame(
            {
                "symbol": request.symbol,
                "ts": [daily_ts(day) for day in raw["day"]],
                "open": [closes[day] * 0.99 for day in raw["day"]],
                "high": [closes[day] * 1.02 for day in raw["day"]],
                "low": [closes[day] * 0.98 for day in raw["day"]],
                "close": [closes[day] for day in raw["day"]],
                "volume": [1_000_000.0] * len(raw),
                "amount": [closes[day] * 1_000_000.0 for day in raw["day"]],
                "adjust_factor": 1.0,
                "quality": "ok",
            }
        )[list(BAR_COLUMNS)]


def test_crosscheck_report_marks_suspect_rows(lake: DataLake):
    primary = _CheckAdapter("akshare", 10.0, overrides={DAYS[4].isoformat(): 10.6})
    secondary = _CheckAdapter("baostock", 10.0)
    report = CrossValidator(primary, secondary, close_tolerance=0.001).check(
        ["SH600519"], START, END
    )
    assert report.total_mismatches == 1

    result = mark_suspect(lake, report)
    assert result.rows_marked == 1

    with LakeQuery(lake.root) as query:
        suspect = query.bars(["SH600519"], quality="suspect")
    assert len(suspect) == 1
    assert suspect.iloc[0]["ts"].date() == DAYS[4]


# ---------------------------------------------------------------------------
# Read-side filtering: suspect rows included AND excluded (two-sided)
# ---------------------------------------------------------------------------
@pytest.fixture()
def suspected_lake(lake) -> DataLake:
    mark_suspect(lake, [_event("SH600519", DAYS[2].isoformat()), _event("SZ000001", DAYS[7].isoformat())])
    return lake


def test_quality_filter_includes_marked_rows(suspected_lake: DataLake):
    with LakeQuery(suspected_lake.root) as query:
        suspects = query.bars(quality="suspect")
    assert len(suspects) == 2
    assert set(suspects["symbol"]) == {"SH600519", "SZ000001"}
    assert (suspects["quality"] == "suspect").all()


def test_quality_filter_excludes_marked_rows(suspected_lake: DataLake):
    with LakeQuery(suspected_lake.root) as query:
        clean = query.bars(quality="ok")
        no_suspect = query.bars(quality=("ok", "backfilled"))
    # "ok" keeps only the 9 unflagged SH600519 rows (SZ000001 is backfilled)
    assert len(clean) == 9
    assert set(clean["symbol"]) == {"SH600519"}
    assert not (clean["quality"] == "suspect").any()
    # excluding suspect keeps backfilled rows but drops the flagged one
    assert len(no_suspect) == 18
    assert set(no_suspect["quality"]) == {"ok", "backfilled"}


def test_default_read_keeps_every_row_and_port_behavior_unchanged(suspected_lake: DataLake):
    """quality=None (default) and the port's fetch_bars stay unfiltered."""
    with LakeQuery(suspected_lake.root) as query:
        everything = query.bars()
    assert len(everything) == 20  # 2 symbols x 10 days, suspect rows included

    from pulsar_contracts import AdjustMode, Freq

    port = LakeMarketDataPort(suspected_lake)
    bars = port.fetch_bars(["SH600519"], START, END, Freq.DAILY, AdjustMode.RAW)
    assert len(bars) == 10  # suspect row still served; marks are opt-in


def test_unknown_quality_mark_rejected(suspected_lake: DataLake):
    with LakeQuery(suspected_lake.root) as query:
        with pytest.raises(LakeError, match="unknown quality mark"):
            query.bars(quality="definitive")


def test_quality_gate_rejects_foreign_marks(lake: DataLake):
    frame = _bars_frame("SH600519", quality="definitive")
    with pytest.raises(QualityViolation, match="quality mark"):
        check_canonical(Dataset.BARS_1D, frame)


# ---------------------------------------------------------------------------
# Partition quality report
# ---------------------------------------------------------------------------
def test_quality_report_counts_and_day_lists(suspected_lake: DataLake):
    report = build_quality_report(suspected_lake)
    assert report.rows == 20
    assert report.totals == {"ok": 9, "backfilled": 9, "suspect": 2}

    by_partition = {item.partition: item for item in report.partitions}
    moutai = by_partition["symbol=SH600519/year=2024"]
    assert moutai.counts == {"ok": 9, "backfilled": 0, "suspect": 1}
    assert moutai.suspect_days == [DAYS[2].isoformat()]
    assert moutai.backfilled_days == []

    pingan = by_partition["symbol=SZ000001/year=2024"]
    assert pingan.counts == {"ok": 0, "backfilled": 9, "suspect": 1}
    assert pingan.backfilled_days == [day.isoformat() for day in DAYS if day != DAYS[7]]
    assert pingan.suspect_days == [DAYS[7].isoformat()]


def test_quality_report_serializes(suspected_lake: DataLake, tmp_path):
    report = build_quality_report(suspected_lake)
    artifact = report.to_json(tmp_path / "quality.json")
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["rows"] == 20
    assert payload["totals"]["suspect"] == 2
    partitions = {item["partition"]: item for item in payload["per_partition"]}
    assert partitions["symbol=SH600519/year=2024"]["suspect_days"] == [DAYS[2].isoformat()]


def test_quality_report_empty_lake(tmp_path):
    empty = DataLake(tmp_path / "empty-lake")
    empty.write(Dataset.CALENDAR, pd.DataFrame({"trade_date": DAYS}), source="test")
    report = build_quality_report(empty)
    assert report.partitions == []
    assert report.rows == 0
    assert report.totals == {"ok": 0, "backfilled": 0, "suspect": 0}
