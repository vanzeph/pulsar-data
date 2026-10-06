"""Minute-bar lake pipeline: schema, normalize, quality gate, query, port, backfill.

Everything runs fully offline against a protocol-level fake client
serving baostock-shaped raw frames (end-labeled ``time`` strings,
string numerics, empty-string suspensions).  Ground truth used here
was verified against the live service:

* 5-minute day shape: 48 bars; ``time`` labels the interval **end**
  (first ``093500000``, last ``150000000``);
* 15/30/60-minute day shapes: 16 / 8 / 4 bars;
* ``query_adjust_factor`` exposes the cumulative hfq factor
  (``dividOperateDate``/``adjustFactor``) which equals the daily
  ``hfq_close / raw_close`` anchor exactly.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest
from pulsar_contracts import AdjustMode, Freq

from pulsar_data.errors import DataNotAvailable, LakeError, QualityViolation
from pulsar_data.lake import DataLake
from pulsar_data.port import LakeMarketDataPort
from pulsar_data.query import LakeQuery
from pulsar_data.quality import check_canonical
from pulsar_data.schema import (
    BAR_COLUMNS,
    Dataset,
    bars_per_trading_day,
    dataset_for_freq,
    minute_ts,
)
from pulsar_data.sources.base import FetchRequest, run_ingestion
from pulsar_data.sources.baostock import BaostockSourceAdapter

# --------------------------------------------------------------------------
# Raw-frame builders (baostock wire shapes)
# --------------------------------------------------------------------------
MINUTE_FIELDS = ["date", "time", "code", "open", "high", "low", "close", "volume", "amount", "adjustflag"]

#: End-labeled session times per frequency (verified against live data).
SESSION_ENDS = {
    5: [f"{9:02d}{m:02d}" for m in range(35, 60, 5)]
    + [f"10{m:02d}" for m in range(0, 60, 5)]
    + [f"11{m:02d}" for m in range(0, 31, 5)]
    + [f"13{m:02d}" for m in range(5, 60, 5)]
    + [f"14{m:02d}" for m in range(0, 60, 5)]
    + ["1500"],
    15: ["0945", "1000", "1015", "1030", "1045", "1100", "1115", "1130",
         "1315", "1330", "1345", "1400", "1415", "1430", "1445", "1500"],
    30: ["1000", "1030", "1100", "1130", "1330", "1400", "1430", "1500"],
    60: ["1030", "1130", "1400", "1500"],
}


def raw_minute_frame(days: list[str], minutes: int, *, base=10.0, symbol="sh.600519") -> pd.DataFrame:
    """A complete baostock-shaped minute frame for every day in ``days``."""
    rows = []
    for day_index, day in enumerate(days):
        for i, end in enumerate(SESSION_ENDS[minutes]):
            level = base + day_index * 100 + i * 0.01
            rows.append(
                {
                    "date": day,
                    "time": f"{day.replace('-', '')}{end}00000",
                    "code": symbol,
                    "open": f"{level:.4f}",
                    "high": f"{level + 0.5:.4f}",
                    "low": f"{level - 0.5:.4f}",
                    "close": f"{level + 0.2:.4f}",
                    "volume": str(1000 + i),
                    "amount": f"{(level + 0.2) * (1000 + i):.2f}",
                    "adjustflag": "3",
                }
            )
    return pd.DataFrame(rows, columns=MINUTE_FIELDS)


class FakeMinuteClient:
    """BaostockClient protocol fake: minute frames + factor events + calendar."""

    def __init__(
        self,
        frames: dict[int, pd.DataFrame],
        *,
        factors: pd.DataFrame | None = None,
        calendar_days: tuple[str, ...] = ("2024-01-02", "2024-01-03", "2024-01-04"),
    ) -> None:
        self.frames = frames  # minutes -> raw frame
        self.factors = factors
        self.calendar_days = calendar_days
        self.calls: list[tuple] = []

    def daily_bars_pair(self, code, start, end):
        self.calls.append(("bars", code, start, end))
        return pd.DataFrame(), pd.DataFrame()

    def minute_bars(self, code, start, end, frequency="5"):
        self.calls.append(("minute", code, start, end, frequency))
        frame = self.frames[int(frequency)]
        in_window = frame[frame["date"].between(start.isoformat(), end.isoformat())]
        return in_window.reset_index(drop=True)

    def adjust_factor_events(self, code, start, end):
        self.calls.append(("adjust_factor", code, start, end))
        if self.factors is None:
            return pd.DataFrame()
        frame = self.factors
        in_window = frame[frame["dividOperateDate"].between(start.isoformat(), end.isoformat())]
        return in_window.reset_index(drop=True)

    def trade_dates(self, start, end):
        self.calls.append(("calendar", start, end))
        rows = [
            {"calendar_date": day, "is_trading_day": "1" if pd.Timestamp(day).weekday() < 5 else "0"}
            for day in self.calendar_days
        ]
        return pd.DataFrame(rows)

    def close(self):
        pass


DAYS = ["2024-01-02", "2024-01-03", "2024-01-04"]

FACTORS = pd.DataFrame(
    [
        {"code": "sh.600519", "dividOperateDate": "2023-06-01", "adjustFactor": "6.5"},
        {"code": "sh.600519", "dividOperateDate": "2024-01-03", "adjustFactor": "7.669257"},
    ]
)


def _client(**kwargs) -> FakeMinuteClient:
    frames = kwargs.pop("frames", None) or {m: raw_minute_frame(DAYS, m) for m in (5, 15, 30, 60)}
    return FakeMinuteClient(frames, **kwargs)


def _request(minutes: int, start=DAYS[0], end=DAYS[-1]) -> FetchRequest:
    return FetchRequest(
        Dataset(f"bars_{minutes}min"), date.fromisoformat(start), date.fromisoformat(end), "SH600519"
    )


# --------------------------------------------------------------------------
# schema mappings
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("freq", "value"),
    [
        (Freq.MINUTE_5, "bars_5min"),
        (Freq.MINUTE_15, "bars_15min"),
        (Freq.MINUTE_30, "bars_30min"),
        (Freq.MINUTE_60, "bars_60min"),
        (Freq.DAILY, "bars_1d"),
    ],
)
def test_dataset_for_freq(freq: Freq, value: str):
    assert dataset_for_freq(freq).value == value


def test_one_minute_freq_has_no_dataset():
    from pulsar_data.errors import ConfigurationError

    with pytest.raises(ConfigurationError, match="no lake dataset serves freq"):
        dataset_for_freq(Freq.MINUTE)


@pytest.mark.parametrize(
    ("freq", "per_day"), [(Freq.MINUTE_5, 48), (Freq.MINUTE_15, 16), (Freq.MINUTE_30, 8), (Freq.MINUTE_60, 4)]
)
def test_bars_per_trading_day(freq: Freq, per_day: int):
    assert bars_per_trading_day(freq) == per_day


def test_minute_ts_shifts_end_label_to_left_closed():
    assert minute_ts("2024-01-02 09:35:00", minutes=5) == pd.Timestamp("2024-01-02 09:30:00+08:00")
    assert minute_ts("2024-01-02 15:00:00", minutes=5) == pd.Timestamp("2024-01-02 14:55:00+08:00")
    assert minute_ts("2024-01-02 10:30:00", minutes=60) == pd.Timestamp("2024-01-02 09:30:00+08:00")


# --------------------------------------------------------------------------
# adapter: fetch_raw + normalize
# --------------------------------------------------------------------------
@pytest.mark.parametrize("minutes", [5, 15, 30, 60])
def test_normalize_minute_bars_canonical_and_left_closed(minutes: int):
    adapter = BaostockSourceAdapter(client=_client())
    request = _request(minutes)
    canonical = adapter.normalize(Dataset(f"bars_{minutes}min"), adapter.fetch_raw(Dataset(f"bars_{minutes}min"), request), request)

    assert list(canonical.columns) == list(BAR_COLUMNS)
    check_canonical(Dataset(f"bars_{minutes}min"), canonical, request)
    assert len(canonical) == len(DAYS) * bars_per_trading_day(Freq(f"{minutes}m"))
    first = canonical.loc[0, "ts"]
    last = canonical.iloc[len(canonical) - 1]["ts"]
    expected_last = pd.Timestamp(f"{DAYS[-1]} {SESSION_ENDS[minutes][-1][:2]}:{SESSION_ENDS[minutes][-1][2:]}") - pd.Timedelta(minutes=minutes)
    assert first == pd.Timestamp("2024-01-02 09:30:00+08:00")
    assert last == expected_last.tz_localize("Asia/Shanghai")


def test_minute_adjust_factor_as_of_join():
    """Factor comes from the last event <= bar date; before any event it is 1.0."""
    adapter = BaostockSourceAdapter(client=_client(factors=FACTORS))
    request = _request(5)
    canonical = adapter.normalize(Dataset.BARS_5MIN, adapter.fetch_raw(Dataset.BARS_5MIN, request), request)
    by_day = {}
    for ts, factor in zip(canonical["ts"], canonical["adjust_factor"]):
        by_day[str(pd.Timestamp(ts).date())] = factor
    assert by_day["2024-01-02"] == pytest.approx(6.5)  # pre-window event anchors the join
    assert by_day["2024-01-03"] == pytest.approx(7.669257)
    assert by_day["2024-01-04"] == pytest.approx(7.669257)


def test_minute_factor_walk_starts_at_market_open():
    """The factor query spans full history so standing factors survive."""
    client = _client(factors=FACTORS)
    adapter = BaostockSourceAdapter(client=client)
    adapter.fetch_raw(Dataset.BARS_5MIN, _request(5))
    factor_calls = [call for call in client.calls if call[0] == "adjust_factor"]
    assert len(factor_calls) == 1
    assert factor_calls[0][2] == date(1990, 1, 1)


def test_minute_frame_without_events_defaults_to_unit_factor():
    adapter = BaostockSourceAdapter(client=_client(factors=None))
    canonical = adapter.normalize(Dataset.BARS_5MIN, adapter.fetch_raw(Dataset.BARS_5MIN, _request(5)), _request(5))
    assert (canonical["adjust_factor"] == 1.0).all()


def test_minute_frequency_validated_by_client():
    client = _client()
    adapter = BaostockSourceAdapter(client=client)
    request = _request(5)
    adapter.fetch_raw(Dataset.BARS_5MIN, request)
    assert [call[4] for call in client.calls if call[0] == "minute"] == ["5"]


# --------------------------------------------------------------------------
# quality gates
# --------------------------------------------------------------------------
def _quality_frame(minutes: int, times_of_day: list[str], day: str = "2024-01-02") -> pd.DataFrame:
    rows = []
    for time in times_of_day:
        rows.append(
            {
                "symbol": "SH600519",
                "ts": pd.Timestamp(f"{day} {time}").tz_localize("Asia/Shanghai"),
                "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                "volume": 100.0, "amount": 1000.0, "adjust_factor": 1.0, "quality": "ok",
            }
        )
    return pd.DataFrame(rows, columns=list(BAR_COLUMNS))


def test_session_grid_accepts_on_grid_minute_bars():
    check_canonical(Dataset.BARS_5MIN, _quality_frame(5, ["09:30", "09:35", "11:25", "13:00", "14:55"]))
    check_canonical(Dataset.BARS_60MIN, _quality_frame(60, ["09:30", "10:30", "13:00", "14:00"]))
    check_canonical(Dataset.BARS_15MIN, _quality_frame(15, ["09:45", "11:15", "13:15", "14:45"]))


@pytest.mark.parametrize(
    ("minutes", "bad_times"),
    [
        (5, ["09:32"]),        # off the 5-minute grid
        (15, ["09:35"]),       # off the 15-minute grid
        (60, ["10:00"]),       # hourly grid is session-anchored, not midnight-anchored
        (5, ["11:30"]),        # lunch break start is not a bar start
        (5, ["09:25"]),        # before the session
        (5, ["15:00"]),        # session end is exclusive for interval starts
    ],
)
def test_session_grid_rejects_off_grid_minute_bars(minutes: int, bad_times: list[str]):
    with pytest.raises(QualityViolation, match="session grid"):
        check_canonical(Dataset(f"bars_{minutes}min"), _quality_frame(minutes, bad_times))


def test_midnight_daily_check_not_applied_to_minute_bars():
    # a daily-shaped midnight timestamp would fail the intraday grid instead
    with pytest.raises(QualityViolation, match="session grid"):
        check_canonical(Dataset.BARS_5MIN, _quality_frame(5, ["00:00"]))


# --------------------------------------------------------------------------
# lake: partitions, watermarks, merge dedupe
# --------------------------------------------------------------------------
def test_minute_write_partitions_and_watermarks(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = BaostockSourceAdapter(client=_client())
    result = run_ingestion(adapter, _request(5), lake, quality_column_value="backfilled")
    assert result.rows == 3 * 48
    partition = tmp_path / "lake" / "bars_5min" / "symbol=SH600519" / "year=2024" / "part.parquet"
    assert partition.is_file()
    stored = lake.read(Dataset.BARS_5MIN)
    assert set(stored["quality"]) == {"backfilled"}
    marks = lake.watermarks()
    rows = marks[
        (marks["source"] == "baostock")
        & (marks["dataset"] == "bars_5min")
        & (marks["partition"] != "*")
    ]
    assert len(rows) == 1 and rows.iloc[0]["partition"] == "symbol=SH600519/year=2024"


def test_minute_merge_write_dedupes_on_symbol_ts(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = BaostockSourceAdapter(client=_client())
    run_ingestion(adapter, _request(5), lake)
    # re-fetch the same window: merge-write keeps one row per (symbol, ts)
    from pulsar_data.sources.base import run_ingestion as ingest

    merged = lake.merge_write(
        Dataset.BARS_5MIN,
        lake.read(Dataset.BARS_5MIN).assign(volume=lambda f: f["volume"] + 1),
        source="baostock",
    )
    assert merged == ["symbol=SH600519/year=2024"]
    assert len(lake.read(Dataset.BARS_5MIN)) == 3 * 48


def test_bars_1d_partitioning_unchanged_by_minute_datasets(tmp_path):
    lake = DataLake(tmp_path / "lake")
    daily = pd.DataFrame(
        [
            {
                "symbol": "SH600519", "ts": pd.Timestamp("2024-01-02").tz_localize("Asia/Shanghai"),
                "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                "volume": 100.0, "amount": 1000.0, "adjust_factor": 1.0, "quality": "ok",
            }
        ]
    )
    lake.write(Dataset.BARS_1D, daily, source="x")
    assert (tmp_path / "lake" / "bars_1d" / "symbol=SH600519" / "year=2024" / "part.parquet").is_file()
    assert not (tmp_path / "lake" / "bars_5min").exists()


# --------------------------------------------------------------------------
# query layer: freq routing, window semantics, downsampling
# --------------------------------------------------------------------------
@pytest.fixture()
def minute_lake(tmp_path):
    lake = DataLake(tmp_path / "lake")
    adapter = BaostockSourceAdapter(client=_client(factors=FACTORS))
    lake.write(
        Dataset.CALENDAR,
        pd.DataFrame({"trade_date": [date.fromisoformat(day) for day in DAYS]}),
        source="baostock",
    )
    for minutes in (5, 15, 30, 60):
        run_ingestion(adapter, _request(minutes), lake)
    return lake


def test_query_bars_routes_each_minute_family(minute_lake):
    with LakeQuery(minute_lake.root) as query:
        for freq, per_day in ((Freq.MINUTE_5, 48), (Freq.MINUTE_15, 16), (Freq.MINUTE_30, 8), (Freq.MINUTE_60, 4)):
            frame = query.bars(["SH600519"], freq=freq)
            assert len(frame) == len(DAYS) * per_day, freq
            assert list(frame.columns) == list(BAR_COLUMNS)


def test_query_bars_minute_window_is_whole_day_inclusive(minute_lake):
    with LakeQuery(minute_lake.root) as query:
        frame = query.bars(["SH600519"], start=date(2024, 1, 4), end=date(2024, 1, 4), freq=Freq.MINUTE_5)
    assert len(frame) == 48  # intraday rows of the end day are included


def test_query_bars_daily_window_stops_at_midnight(minute_lake):
    with LakeQuery(minute_lake.root) as query:
        with pytest.raises(DataNotAvailable, match="bars_1d"):
            query.bars(["SH600519"], freq=Freq.DAILY)  # no daily family in this lake


def test_query_bars_downsamples_missing_family(minute_lake):
    import shutil

    shutil.rmtree(minute_lake.root / "bars_15min")
    with LakeQuery(minute_lake.root) as query:
        native30 = query.bars(["SH600519"], freq=Freq.MINUTE_30)
        down15 = query.bars(["SH600519"], freq=Freq.MINUTE_15)
    assert len(native30) == 3 * 8
    assert len(down15) == 3 * 16  # 5m family served the 15m request


def test_downsampled_bars_aggregate_ohlc_volume(minute_lake):
    import shutil

    for family in ("bars_15min", "bars_30min", "bars_60min"):
        shutil.rmtree(minute_lake.root / family)
    with LakeQuery(minute_lake.root) as query:
        five = query.bars(["SH600519"], start=date(2024, 1, 2), end=date(2024, 1, 2), freq=Freq.MINUTE_5)
        hourly = query.bars(["SH600519"], start=date(2024, 1, 2), end=date(2024, 1, 2), freq=Freq.MINUTE_60)
    assert list(hourly["ts"].dt.strftime("%H:%M")) == ["09:30", "10:30", "13:00", "14:00"]
    for _, bucket in hourly.iterrows():
        members = five[five["ts"] == bucket["ts"]].index
        # buckets start at the hour anchors; recompute membership from the grid
        start = bucket["ts"]
        end = start + pd.Timedelta(minutes=60)
        window = five[(five["ts"] >= start) & (five["ts"] < end)]
        assert bucket["open"] == pytest.approx(window.iloc[0]["open"])
        assert bucket["high"] == pytest.approx(window["high"].max())
        assert bucket["low"] == pytest.approx(window["low"].min())
        assert bucket["close"] == pytest.approx(window.iloc[-1]["close"])
        assert bucket["volume"] == pytest.approx(window["volume"].sum())
        assert bucket["amount"] == pytest.approx(window["amount"].sum())
        assert bucket["adjust_factor"] == pytest.approx(window.iloc[-1]["adjust_factor"])
        _ = members


def test_query_bars_raises_when_no_family_present(minute_lake):
    import shutil

    for family in ("bars_15min", "bars_30min", "bars_60min"):
        shutil.rmtree(minute_lake.root / family)
    with LakeQuery(minute_lake.root) as query:
        # 60m can downsample from 5m; removing 5m too leaves nothing
        pass
    shutil.rmtree(minute_lake.root / "bars_5min")
    with LakeQuery(minute_lake.root) as query:
        with pytest.raises(DataNotAvailable, match="bars_60min"):
            query.bars(["SH600519"], freq=Freq.MINUTE_60)


# --------------------------------------------------------------------------
# port: fetch_bars minute passthrough + completeness guard
# --------------------------------------------------------------------------
def test_fetch_bars_minute_matches_lake_rows_exactly(minute_lake):
    port = LakeMarketDataPort(minute_lake)
    for freq in (Freq.MINUTE_5, Freq.MINUTE_15, Freq.MINUTE_30, Freq.MINUTE_60):
        frame = port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 4), freq, AdjustMode.RAW)
        with LakeQuery(minute_lake.root) as query:
            direct = query.bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 4), freq=freq)
        pd.testing.assert_frame_equal(frame.reset_index(drop=True), direct.reset_index(drop=True))


def test_fetch_bars_minute_adjustment_uses_factor_anchor(minute_lake):
    port = LakeMarketDataPort(minute_lake)
    with LakeQuery(minute_lake.root) as query:
        raw = query.bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 4), freq=Freq.MINUTE_5)
    backward = port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 4), Freq.MINUTE_5, AdjustMode.BACKWARD)
    # BACKWARD anchors at the first bar's factor (6.5 on 01-02): every later
    # close is restated by factor/anchor, matches stay raw.
    anchor = raw.iloc[0]["adjust_factor"]
    pd.testing.assert_series_equal(
        backward["close"], (raw["close"] * raw["adjust_factor"] / anchor).rename("close"), check_names=False
    )
    first_day = str(pd.Timestamp(raw.iloc[0]["ts"]).date())
    same_day = raw["ts"].map(lambda ts: str(pd.Timestamp(ts).date())) == first_day
    pd.testing.assert_series_equal(
        backward.loc[same_day.values, "close"].reset_index(drop=True),
        raw.loc[same_day.values, "close"].reset_index(drop=True),
    )


def test_fetch_bars_minute_raises_on_partial_session(minute_lake):
    frame = minute_lake.read(Dataset.BARS_5MIN)
    trimmed = frame[~(frame["ts"] == pd.Timestamp("2024-01-03 13:00:00+08:00"))]
    minute_lake.write(Dataset.BARS_5MIN, trimmed.reset_index(drop=True), source="baostock")
    port = LakeMarketDataPort(minute_lake)
    with pytest.raises(DataNotAvailable, match="incomplete"):
        port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 4), Freq.MINUTE_5, AdjustMode.RAW)


def test_fetch_bars_minute_suspension_excused(tmp_path):
    lake = DataLake(tmp_path / "lake")
    partial_days = [day for day in DAYS if day != "2024-01-03"]
    client = _client(frames={5: raw_minute_frame(partial_days, 5)})
    adapter = BaostockSourceAdapter(client=client)
    lake.write(
        Dataset.CALENDAR,
        pd.DataFrame({"trade_date": [date.fromisoformat(day) for day in DAYS]}),
        source="baostock",
    )
    run_ingestion(adapter, _request(5), lake)
    lake.write(
        Dataset.SUSPENSIONS,
        pd.DataFrame(
            [
                {
                    "symbol": "SH600519",
                    "start_date": date(2024, 1, 3),
                    "end_date": date(2024, 1, 3),
                    "reason": "halt",
                }
            ]
        ),
        source="akshare",
    )
    port = LakeMarketDataPort(lake)
    frame = port.fetch_bars(["SH600519"], date(2024, 1, 2), date(2024, 1, 4), Freq.MINUTE_5, AdjustMode.RAW)
    assert len(frame) == 2 * 48  # suspended day excused, both other days full


# --------------------------------------------------------------------------
# lake minute completeness
# --------------------------------------------------------------------------
def test_minute_completeness_zero_gaps_for_full_sessions(minute_lake):
    counts, missing = minute_lake.minute_completeness(
        dataset=Dataset.BARS_5MIN, start=date(2024, 1, 2), end=date(2024, 1, 4), symbols=["SH600519"]
    )
    assert counts["SH600519"]["ok"] == 3
    assert counts["SH600519"]["gap"] == 0
    assert missing == {}


def test_minute_completeness_flags_partial_day(tmp_path):
    lake = DataLake(tmp_path / "lake")
    client = _client(frames={5: raw_minute_frame(DAYS, 5)})
    adapter = BaostockSourceAdapter(client=client)
    lake.write(
        Dataset.CALENDAR,
        pd.DataFrame({"trade_date": [date.fromisoformat(day) for day in DAYS]}),
        source="baostock",
    )
    run_ingestion(adapter, _request(5), lake)
    frame = lake.read(Dataset.BARS_5MIN)
    keep = ~(frame["ts"] >= pd.Timestamp("2024-01-03 13:00:00+08:00"))
    lake.write(Dataset.BARS_5MIN, frame[keep].reset_index(drop=True), source="baostock")
    counts, missing = lake.minute_completeness(
        dataset=Dataset.BARS_5MIN, start=date(2024, 1, 2), end=date(2024, 1, 4), symbols=["SH600519"]
    )
    assert counts["SH600519"]["gap"] == 1
    assert missing["SH600519"]["2024-01-03"] == 24  # afternoon session missing


def test_minute_completeness_rejects_non_minute_dataset(minute_lake):
    with pytest.raises(LakeError, match="not a minute dataset"):
        minute_lake.minute_completeness(
            dataset=Dataset.BARS_1D, start=date(2024, 1, 2), end=date(2024, 1, 4)
        )


# --------------------------------------------------------------------------
# backfill runner + CLI
# --------------------------------------------------------------------------
def test_minute_backfill_runner_zero_unexplained_gaps(tmp_path):
    from pulsar_data.backfill import BackfillRunner

    lake = DataLake(tmp_path / "lake")
    adapter = BaostockSourceAdapter(client=_client(factors=FACTORS))
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)
    report = runner.run(["SH600519"], date(2024, 1, 2), date(2024, 1, 4))
    assert report.freq == "5m"
    assert report.bars_rows == 3 * 48
    assert report.unexplained_gaps == 0
    assert report.minute_missing_bars == {}
    assert (tmp_path / "lake" / "bars_5min" / "symbol=SH600519" / "year=2024").is_dir()
    assert not (tmp_path / "lake" / "bars_1d").exists()  # daily family untouched


def test_minute_backfill_chunks_per_calendar_year(tmp_path):
    from pulsar_data.backfill import BackfillRunner

    long_days = ["2023-12-29", "2024-01-02", "2024-01-03"]
    client = _client(frames={5: raw_minute_frame(long_days, 5)})
    adapter = BaostockSourceAdapter(client=client)
    lake = DataLake(tmp_path / "lake")
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)
    report = runner.run(["SH600519"], date(2023, 12, 29), date(2024, 1, 3))
    windows = [call for call in client.calls if call[0] == "minute"]
    assert [(w[2], w[3]) for w in windows] == [
        (date(2023, 12, 29), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 1, 3)),
    ]
    years = {p.name for p in (tmp_path / "lake" / "bars_5min" / "symbol=SH600519").iterdir()}
    assert years == {"year=2023", "year=2024"}
    assert report.unexplained_gaps == 0


def test_minute_backfill_universe_falls_back_to_lake_instruments(tmp_path):
    from pulsar_data.backfill import BackfillRunner

    lake = DataLake(tmp_path / "lake")
    lake.write(
        Dataset.CALENDAR,
        pd.DataFrame({"trade_date": [date.fromisoformat(day) for day in DAYS]}),
        source="baostock",
    )
    lake.write(
        Dataset.INSTRUMENTS,
        pd.DataFrame(
            [
                {
                    "symbol": "SH600519", "name": "Kweichow Moutai", "exchange": "SSE",
                    "board": "main", "is_st": False, "status": "listed",
                    "list_date": date(2001, 8, 27), "delist_date": None, "shares_outstanding": 1.0e9,
                }
            ]
        ),
        source="akshare",
    )
    adapter = BaostockSourceAdapter(client=_client(factors=FACTORS))
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)
    report = runner.run([], date(2024, 1, 2), date(2024, 1, 4))  # empty -> full market
    assert report.symbols == ["SH600519"]
    assert report.unexplained_gaps == 0


def test_minute_backfill_universe_fails_without_any_snapshot(tmp_path):
    from pulsar_data.errors import ConfigurationError
    from pulsar_data.backfill import BackfillRunner

    lake = DataLake(tmp_path / "lake")
    adapter = BaostockSourceAdapter(client=_client())
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)
    with pytest.raises(ConfigurationError, match="cannot resolve the full market"):
        runner.run([], date(2024, 1, 2), date(2024, 1, 4))


def test_minute_backfill_resumes_via_watermarks(tmp_path):
    from pulsar_data.backfill import BackfillRunner

    lake = DataLake(tmp_path / "lake")
    client = _client(factors=FACTORS)
    adapter = BaostockSourceAdapter(client=client)
    runner = BackfillRunner(adapter, lake, freq=Freq.MINUTE_5)
    first = runner.run(["SH600519"], date(2024, 1, 2), date(2024, 1, 4))
    assert first.skipped_symbols == []
    second = runner.run(["SH600519"], date(2024, 1, 2), date(2024, 1, 4))
    assert second.skipped_symbols == ["SH600519"]
    assert second.bars_rows == 0


def _write_baostock_fixtures(root, days=DAYS, factors=FACTORS) -> None:
    """Lay down a FixtureBaostockClient directory (used by CLI tests)."""
    minute_dir = root / "minute"
    factor_dir = root / "adjust_factors"
    minute_dir.mkdir(parents=True)
    factor_dir.mkdir(parents=True)
    for minutes in (5, 15, 30, 60):
        raw_minute_frame(days, minutes).to_csv(minute_dir / f"SH600519.{minutes}.csv", index=False)
    factors.to_csv(factor_dir / "SH600519.csv", index=False)
    pd.DataFrame(
        [
            {"calendar_date": day, "is_trading_day": "1" if pd.Timestamp(day).weekday() < 5 else "0"}
            for day in days
        ]
    ).to_csv(root / "calendar.csv", index=False)


def test_minute_cli_backfill_offline_end_to_end(tmp_path, capsys):
    from pulsar_data.cli import main

    fixture_dir = tmp_path / "fixtures"
    _write_baostock_fixtures(fixture_dir)
    lake_dir = tmp_path / "lake"
    report_path = tmp_path / "report.json"
    code = main(
        [
            "backfill", "--source", "baostock",
            "--lake", str(lake_dir),
            "--start", "2024-01-02", "--end", "2024-01-04",
            "--symbols", "SH600519",
            "--freq", "5m",
            "--fixture-dir", str(fixture_dir),
            "--report", str(report_path),
            "--fail-on-gaps",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "freq=5m" in out
    assert "unexplained_gaps=0" in out
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["freq"] == "5m"
    assert payload["rows"]["bars"] == 3 * 48
    assert payload["unexplained_gaps"] == 0
    assert (lake_dir / "bars_5min" / "symbol=SH600519" / "year=2024" / "part.parquet").is_file()

    # verify --freq 5m re-checks the minute family and stays green
    assert main(
        ["verify", "--lake", str(lake_dir), "--start", "2024-01-02", "--end", "2024-01-04",
         "--symbols", "SH600519", "--freq", "5m"]
    ) == 0


def test_minute_cli_verify_detects_tampered_partition(tmp_path, capsys):
    from pulsar_data.cli import main

    fixture_dir = tmp_path / "fixtures"
    _write_baostock_fixtures(fixture_dir)
    lake_dir = tmp_path / "lake"
    assert main(
        ["backfill", "--source", "baostock", "--lake", str(lake_dir),
         "--start", "2024-01-02", "--end", "2024-01-04", "--symbols", "SH600519",
         "--freq", "5m", "--fixture-dir", str(fixture_dir)]
    ) == 0
    partition = lake_dir / "bars_5min" / "symbol=SH600519" / "year=2024" / "part.parquet"
    frame = pd.read_parquet(partition)
    holed = frame.iloc[10:].reset_index(drop=True)  # drop morning rows of day one
    holed.to_parquet(partition, index=False)
    assert main(
        ["verify", "--lake", str(lake_dir), "--start", "2024-01-02", "--end", "2024-01-04",
         "--symbols", "SH600519", "--freq", "5m"]
    ) == 1


def test_minute_cli_rejects_unknown_freq(tmp_path, capsys):
    from pulsar_data.cli import main

    with pytest.raises(SystemExit):
        main(["backfill", "--source", "baostock", "--lake", str(tmp_path),
              "--start", "2024-01-02", "--end", "2024-01-04", "--freq", "7m"])
