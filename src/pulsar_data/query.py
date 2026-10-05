"""Read-only DuckDB query layer over the Parquet lake.

Design mapping: “查询层：DuckDB 视图直查 Parquet；fetch_bars 的实现
= DuckDB 查询 + 按需复权计算”.  :class:`LakeQuery` opens one in-memory
DuckDB connection, points it at the lake's Parquet files through
session-local views, and serves every read from SQL — the pandas
row-at-a-time :meth:`DataLake.read` stays a maintenance path only.

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
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
import pandas as pd

from .errors import DataNotAvailable, LakeError
from .schema import BAR_COLUMNS, Dataset
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
    ) -> pd.DataFrame:
        """Raw daily bars (canonical columns) filtered by symbol and window."""
        self._ensure_view(Dataset.BARS_1D)
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
        if end is not None:
            clauses.append("ts <= CAST(? AS TIMESTAMPTZ)")
            params.append(_midnight(end))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        columns = ", ".join(BAR_COLUMNS)
        frame = self.query(
            f"SELECT {columns} FROM bars_1d{where} ORDER BY symbol, ts", params or None
        )
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True).dt.tz_convert("Asia/Shanghai")
        return frame.reset_index(drop=True)

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
