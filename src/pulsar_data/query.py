"""Read-only DuckDB query layer over the Parquet lake.

Design mapping: “查询层：DuckDB 视图直查 Parquet；fetch_bars 的实现
= DuckDB 查询 + 按需复权计算”.  :class:`LakeQuery` opens one in-memory
DuckDB connection, points it at the lake's Parquet files through
session-local views, and serves every read from SQL — the pandas
row-at-a-time :meth:`DataLake.read` stays a maintenance path only.

Minute frequencies read their own ``bars_<freq>`` partition family
directly; when the requested granularity was never backfilled but a
finer minute family was, :meth:`LakeQuery.bars` downsamples on demand
(按需降采样): buckets are formed per symbol on the left-closed session
grid — 09:30-anchored mornings, 13:00-anchored afternoons, lunch break
excluded — with ``open=first, high=max, low=min, close=last,
volume/amount=sum, adjust_factor=last``.  A ``quality`` filter applied
together with downsampling filters the *source* rows before bucketing.

Read-only semantics are enforced twice:

* the connection is in-memory and no write statement is ever issued;
* :meth:`LakeQuery.query` rejects any statement that does not start
  with ``SELECT``/``WITH`` and refuses multi-statement input, so a
  mis-formed call cannot mutate the lake files.

Every partition glob only ever matches ``part.parquet`` — the hidden
``.tmp-*`` siblings written during atomic replacement can never leak
into a query result.
"""

from __future__ import annotations

import threading
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
import pandas as pd
from pulsar_contracts import Freq

from .errors import DataNotAvailable, LakeError
from .schema import (
    BAR_COLUMNS,
    DATASET_MINUTES,
    MINUTE_DATASETS,
    QUALITY_VALUES,
    SESSION_STARTS,
    Dataset,
    dataset_for_freq,
)
from .symbols import to_canonical_symbol

__all__ = ["LakeQuery"]

#: Statements the query layer is allowed to run (case-insensitive prefix).
_ALLOWED_PREFIXES = ("select", "with")

#: Datasets stored as one file directly under ``<dataset>/``.
_SINGLE_FILE_DATASETS = frozenset({Dataset.INSTRUMENTS, Dataset.CALENDAR})


class LakeQuery:
    """Session of read-only SQL views over one lake directory."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._registered: set[Dataset] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------ lifecycle
    def __enter__(self) -> "LakeQuery":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._registered.clear()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The lazily opened in-memory connection (Asia/Shanghai rendering)."""
        with self._lock:
            if self._connection is None:
                self._connection = duckdb.connect(":memory:")
                self._connection.execute("SET TimeZone='Asia/Shanghai'")
            return self._connection

    # ------------------------------------------------------------ plumbing
    def _files_glob(self, dataset: Dataset) -> str:
        base = self.root / dataset.value
        if dataset in _SINGLE_FILE_DATASETS:
            return str(base / "part.parquet")
        return str(base / "**" / "part.parquet")

    def has(self, dataset: Dataset) -> bool:
        """True when the lake holds at least one partition of ``dataset``."""
        if dataset in _SINGLE_FILE_DATASETS:
            return (self.root / dataset.value / "part.parquet").is_file()
        return any((self.root / dataset.value).glob("**/part.parquet"))

    def _ensure_view(self, dataset: Dataset) -> str:
        """Register (once) the Parquet view for ``dataset``; return its name."""
        if dataset not in self._registered:
            if not self.has(dataset):
                raise DataNotAvailable(
                    f"{dataset.value} is not present in lake {self.root}; "
                    "ingest it before querying"
                )
            glob = self._files_glob(dataset).replace("'", "''")
            self.connection.execute(
                f"CREATE OR REPLACE VIEW {dataset.value} AS "
                f"SELECT * FROM read_parquet('{glob}', hive_partitioning=false)"
            )
            self._registered.add(dataset)
        return dataset.value

    @staticmethod
    def _guard(sql: str) -> str:
        """Reject anything that is not a single read-only statement."""
        text = sql.strip().rstrip(";").strip()
        if not text or ";" in text:
            raise LakeError("query layer accepts exactly one statement per call")
        lowered = text.lower()
        if not lowered.startswith(_ALLOWED_PREFIXES):
            raise LakeError(
                "query layer is read-only: only SELECT/WITH statements are allowed"
            )
        return text

    def query(self, sql: str, params: Sequence[object] | None = None) -> pd.DataFrame:
        """Run one guarded SELECT/WITH statement and return a DataFrame."""
        statement = self._guard(sql)
        cursor = self.connection.execute(statement, list(params) if params else [])
        return cursor.df()

    # ------------------------------------------------------------ datasets
    def bars(
        self,
        symbols: Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        quality: str | Sequence[str] | None = None,
        freq: Freq = Freq.DAILY,
    ) -> pd.DataFrame:
        """Raw bars (canonical columns) filtered by symbol and window, per ``freq``.

        ``freq`` selects the partition family: ``1d`` reads ``bars_1d``
        (``ts <= end`` at midnight, the left-closed daily convention);
        a minute frequency reads its ``bars_<freq>`` family through the
        whole end day (``ts < end + 1 day``), and when that family was
        never backfilled but a finer one exists, the finer family is
        downsampled on demand (see the module docstring).

        ``quality`` optionally filters rows by their quality mark
        (``ok / backfilled / suspect``) — the read side of the lake's
        可信度落地.  A single mark or a sequence of marks is accepted;
        ``None`` (the default) keeps every row, preserving the
        pre-existing behavior for callers that do not care about marks.
        """
        dataset = dataset_for_freq(freq)
        if dataset is Dataset.BARS_1D:
            # daily bars live at midnight; the end day's bar equals the bound
            end_operator = "<="
            window_end = None if end is None else _midnight(end)
        else:
            # minute bars span the whole end day, up to (exclusive) next midnight
            end_operator = "<"
            window_end = None if end is None else _midnight(end + timedelta(days=1))
        source, source_dataset = self._bars_source(dataset)
        clauses: list[str] = []
        params: list[object] = []
        wanted: list[str] | None = None
        if symbols is not None:
            wanted = [to_canonical_symbol(symbol) for symbol in symbols]
            if not wanted:
                return pd.DataFrame(columns=list(BAR_COLUMNS))
            clauses.append(f"symbol IN ({', '.join('?' for _ in wanted)})")
            params.extend(wanted)
        if start is not None:
            clauses.append("ts >= CAST(? AS TIMESTAMPTZ)")
            params.append(_midnight(start))
        if window_end is not None:
            clauses.append(f"ts {end_operator} CAST(? AS TIMESTAMPTZ)")
            params.append(window_end)
        if quality is not None:
            allowed = [quality] if isinstance(quality, str) else list(quality)
            if not allowed:
                return pd.DataFrame(columns=list(BAR_COLUMNS))
            unknown = [mark for mark in allowed if mark not in QUALITY_VALUES]
            if unknown:
                raise LakeError(
                    f"unknown quality mark(s) {unknown}; valid marks are {list(QUALITY_VALUES)}"
                )
            clauses.append(f"quality IN ({', '.join('?' for _ in allowed)})")
            params.extend(allowed)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        columns = ", ".join(BAR_COLUMNS)
        frame = self.query(
            f"SELECT {columns} FROM {source}{where} ORDER BY symbol, ts", params or None
        )
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True).dt.tz_convert("Asia/Shanghai")
        frame = frame.reset_index(drop=True)
        if source_dataset is dataset:
            return frame
        return _downsample(
            frame, DATASET_MINUTES[source_dataset], DATASET_MINUTES[dataset]
        )

    def _bars_source(self, dataset: Dataset) -> tuple[str, Dataset]:
        """Registered view serving ``dataset``: itself, or a finer family.

        Raises :class:`DataNotAvailable` when neither the requested
        family nor any finer minute family is present.
        """
        if self.has(dataset):
            self._ensure_view(dataset)
            return dataset.value, dataset
        if dataset in DATASET_MINUTES:
            for finer in MINUTE_DATASETS:  # thinnest first
                if DATASET_MINUTES[finer] < DATASET_MINUTES[dataset] and self.has(finer):
                    self._ensure_view(finer)
                    return finer.value, finer
        raise DataNotAvailable(
            f"{dataset.value} is not present in lake {self.root} (nor is any finer "
            "minute family to downsample from); ingest it before querying"
        )

    def corporate_actions(self, symbols: Iterable[str] | None = None) -> pd.DataFrame:
        """Corporate-action rows, optionally restricted to ``symbols``."""
        self._ensure_view(Dataset.CORPORATE_ACTIONS)
        clauses: list[str] = []
        params: list[object] = []
        if symbols is not None:
            wanted = [to_canonical_symbol(symbol) for symbol in symbols]
            if not wanted:
                return pd.DataFrame()
            clauses.append(f"symbol IN ({', '.join('?' for _ in wanted)})")
            params.extend(wanted)
        where = f" WHERE {' OR '.join(clauses)}" if clauses else ""
        return self.query(
            f"SELECT * FROM corporate_actions{where} ORDER BY symbol, ex_date",
            params or None,
        )

    def instruments(self) -> pd.DataFrame:
        """The full instrument snapshot."""
        self._ensure_view(Dataset.INSTRUMENTS)
        return self.query("SELECT * FROM instruments ORDER BY symbol")

    def calendar(self, start: date | None = None, end: date | None = None) -> pd.DataFrame:
        """Trading calendar rows ascending, optionally windowed."""
        self._ensure_view(Dataset.CALENDAR)
        clauses: list[str] = []
        params: list[object] = []
        if start is not None:
            clauses.append("trade_date >= CAST(? AS DATE)")
            params.append(start.isoformat())
        if end is not None:
            clauses.append("trade_date <= CAST(? AS DATE)")
            params.append(end.isoformat())
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query(f"SELECT * FROM calendar{where} ORDER BY trade_date", params or None)


def _midnight(day: date) -> str:
    """ISO form of ``day`` at Asia/Shanghai midnight for TIMESTAMPTZ casts."""
    return f"{day.isoformat()}T00:00:00+08:00"


def _bucket_start(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    """Left-closed bucket start of ``ts`` on the session-relative grid.

    Sessions are anchored at 09:30 (morning) and 13:00 (afternoon); the
    bucket of a bar starts at ``session_start + floor(offset / minutes) *
    minutes``.  Timestamps outside both sessions (should not exist past
    the quality gate) bucket relative to the morning anchor.
    """
    local = ts.tz_convert("Asia/Shanghai")
    minute_of_day = local.hour * 60 + local.minute
    morning, afternoon = SESSION_STARTS
    anchor = afternoon if minute_of_day >= afternoon else morning
    offset = max(minute_of_day - anchor, 0)
    return local.normalize() + pd.Timedelta(minutes=anchor + (offset // minutes) * minutes)


def _downsample(frame: pd.DataFrame, source_minutes: int, target_minutes: int) -> pd.DataFrame:
    """Aggregate a finer-minute frame onto the coarser session grid.

    OHLC becomes first/max/min/last, ``volume``/``amount`` sum, and the
    bucket keeps the **last** ``adjust_factor`` (the cumulative factor
    is constant within a trading day by construction).  Rows arrive
    already filtered by symbol/window/quality from the SQL layer.
    """
    if target_minutes % source_minutes != 0:
        raise LakeError(
            f"cannot downsample {source_minutes}m partitions to {target_minutes}m "
            "(source does not divide the target)"
        )
    if frame.empty:
        return frame
    stamped = frame.copy()
    stamped["bucket"] = stamped["ts"].map(lambda ts: _bucket_start(ts, target_minutes))
    grouped = stamped.sort_values(["symbol", "ts"], kind="stable").groupby(
        ["symbol", "bucket"], sort=False
    )
    merged = pd.DataFrame(
        {
            "open": grouped["open"].first(),
            "high": grouped["high"].max(),
            "low": grouped["low"].min(),
            "close": grouped["close"].last(),
            "volume": grouped["volume"].sum(),
            "amount": grouped["amount"].sum(),
            "adjust_factor": grouped["adjust_factor"].last(),
            "quality": grouped["quality"].agg(
                lambda marks: "suspect" if (marks == "suspect").any()
                else ("backfilled" if (marks == "backfilled").any() else "ok")
            ),
        }
    ).reset_index()
    merged = merged.rename(columns={"bucket": "ts"})
    return merged.sort_values(["symbol", "ts"]).reset_index(drop=True)[list(BAR_COLUMNS)]
