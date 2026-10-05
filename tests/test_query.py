"""The DuckDB query layer: views over Parquet, read-only guard, filters."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import DataNotAvailable, LakeError
from pulsar_data.lake import DataLake
from pulsar_data.query import LakeQuery
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts


def _bars(days: list[str], symbol: str = "SH600519", factor: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "ts": [daily_ts(day) for day in days],
            "open": [10.0] * len(days),
            "high": [11.0] * len(days),
            "low": [9.5] * len(days),
            "close": [10.5] * len(days),
            "volume": [100.0] * len(days),
            "amount": [1050.0] * len(days),
            "adjust_factor": [factor] * len(days),
            "quality": ["ok"] * len(days),
        }
    )[list(BAR_COLUMNS)]


@pytest.fixture()
def stocked_lake(lake: DataLake) -> DataLake:
    lake.write(Dataset.BARS_1D, _bars(["2023-12-29"]), source="akshare")
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02", "2024-01-03"]), source="akshare")
    lake.write(Dataset.BARS_1D, _bars(["2024-01-02"], symbol="SZ000001"), source="akshare")
    lake.write(
        Dataset.CALENDAR,
        pd.DataFrame(
            {
                "trade_date": [
                    date(2023, 12, 29),
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                ]
            }
        ),
        source="akshare",
    )
    return lake


def test_duckdb_sees_every_partition_across_years_and_symbols(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        frame = query.bars()
    assert len(frame) == 4
    assert set(frame["symbol"]) == {"SH600519", "SZ000001"}
    # canonical columns only (no hive year column leaks in)
    assert list(frame.columns) == list(BAR_COLUMNS)


def test_query_matches_pandas_read(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        via_sql = query.bars(["SH600519"]).sort_values("ts").reset_index(drop=True)
    via_pandas = stocked_lake.read(Dataset.BARS_1D, symbols=["SH600519"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(via_sql, via_pandas, check_dtype=False)


def test_timestamps_stay_shanghai_midnight(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        frame = query.bars()
    assert str(frame["ts"].dt.tz) == "Asia/Shanghai"
    assert (frame["ts"].dt.time == pd.Timestamp("00:00").time()).all()


def test_symbol_and_window_filters(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        both = query.bars(start=date(2024, 1, 1), end=date(2024, 12, 31))
        one = query.bars(symbols=["SZ000001"])
    assert set(both["symbol"]) == {"SH600519", "SZ000001"}
    assert set(one["symbol"]) == {"SZ000001"}
    assert len(one) == 1


def test_empty_symbol_list_short_circuits(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        frame = query.bars([])
    assert frame.empty
    assert list(frame.columns) == list(BAR_COLUMNS)


def test_calendar_window_query(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        frame = query.calendar(start=date(2024, 1, 1), end=date(2024, 1, 31))
    days = [pd.Timestamp(value).date() for value in frame["trade_date"]]
    assert days == [date(2024, 1, 2), date(2024, 1, 3)]


def test_hidden_tmp_files_never_matched(lake: DataLake):
    """A crashed writer's .tmp sibling is invisible to the query layer."""
    partition = lake.root / "bars_1d" / "symbol=SH600519" / "year=2024"
    partition.mkdir(parents=True)
    _bars(["2024-01-02"]).to_parquet(partition / "part.parquet", index=False)
    _bars(["2024-01-02", "2024-01-03"]).to_parquet(
        partition / ".tmp-deadbeef.parquet", index=False
    )
    with LakeQuery(lake.root) as query:
        frame = query.bars()
    assert len(frame) == 1


def test_missing_dataset_raises_datanotavailable(lake: DataLake):
    with LakeQuery(lake.root) as query:
        assert not query.has(Dataset.BARS_1D)
        with pytest.raises(DataNotAvailable, match="bars_1d"):
            query.bars()


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO bars_1d VALUES (1)",
        "DELETE FROM bars_1d",
        "CREATE TABLE x AS SELECT 1",
        "DROP VIEW bars_1d",
        "UPDATE bars_1d SET close = 0",
        "COPY bars_1d TO '/tmp/x.parquet'",
        "SELECT 1; DROP VIEW bars_1d",
    ],
)
def test_read_only_guard_rejects_writes(stocked_lake: DataLake, sql: str):
    with LakeQuery(stocked_lake.root) as query:
        with pytest.raises(LakeError, match="read-only|one statement"):
            query.query(sql)


def test_guard_allows_select_and_cte(stocked_lake: DataLake):
    with LakeQuery(stocked_lake.root) as query:
        query.bars()  # registers the bars_1d view
        count = query.query("SELECT count(*) AS n FROM bars_1d")["n"].iloc[0]
        cte = query.query("WITH t AS (SELECT 1 AS x) SELECT x FROM t")["x"].iloc[0]
    assert count == 4
    assert cte == 1


def test_corporate_actions_view(lake: DataLake):
    frame = pd.DataFrame(
        [
            {
                "symbol": "SH600519",
                "ex_date": date(2024, 6, 19),
                "cash_dividend_per_share": 30.874,
                "bonus_share_ratio": 0.0,
                "rights_issue_ratio": 0.0,
                "rights_issue_price": None,
                "description": "annual dividend",
            }
        ]
    )
    lake.write(Dataset.CORPORATE_ACTIONS, frame, source="akshare")
    with LakeQuery(lake.root) as query:
        back = query.corporate_actions(["SH600519"])
        other = query.corporate_actions(["SZ000001"])
    assert len(back) == 1
    assert back["cash_dividend_per_share"].iloc[0] == 30.874
    assert other.empty
