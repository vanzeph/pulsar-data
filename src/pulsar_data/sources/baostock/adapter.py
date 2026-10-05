"""The baostock source adapter: fetch_raw + normalize for daily bars and calendar.

baostock is the free, credential-less *backup* source of the data-source
matrix (design: 「baostock —— 日线、分钟线（复权因子完备），首期备源」).
Raw shapes handled here:

* k-data (``query_history_k_data_plus``): string columns
  ``date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST``
  fetched twice per window — ``adjustflag=3`` (不复权, raw) and ``adjustflag=1``
  (后复权) — because the lake stores raw prices plus a cumulative
  ``adjust_factor`` derived as ``hfq_close / raw_close`` from the same
  endpoint (single anchor, the same rule the akshare adapter applies);
* trade calendar (``query_trade_dates``): ``calendar_date,is_trading_day``
  where non-trading days carry ``is_trading_day == 0`` and are dropped;
* volume arrives in shares and amount in CNY — the canonical units, no
  conversion (unlike akshare's Eastmoney lots).

Suspended days come back with empty OHLC strings; those rows are dropped
(suspension marking itself is the quality/backfill task's concern).
"""

from __future__ import annotations

import logging
from typing import Mapping

import pandas as pd

from ...errors import ConfigurationError
from ...schema import BAR_COLUMNS, CALENDAR_COLUMNS, Dataset, daily_ts
from ...symbols import to_canonical_symbol
from ..base import FetchRequest
from .client import LiveBaostockClient, to_baostock_code

logger = logging.getLogger("pulsar_data.baostock")

__all__ = ["BaostockSourceAdapter"]


def _pick(frame: pd.DataFrame, *candidates: str) -> str | None:
    lowered = {str(column).strip().lower(): column for column in frame.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    for candidate in candidates:
        for lowered_name, original in lowered.items():
            if candidate.lower() in lowered_name:
                return original
    return None


class BaostockSourceAdapter:
    """``SourceAdapter`` implementation for the baostock free backup source."""

    source_id = "baostock"

    def __init__(
        self,
        config: Mapping[str, object] | None = None,
        *,
        client: object | None = None,
    ) -> None:
        config = dict(config or {})
        if client is not None:
            self.client = client
        else:
            self.client = LiveBaostockClient(
                min_interval=float(config.get("min_interval", 0.5)),
                retries=int(config.get("retries", 4)),
                breaker_threshold=int(config.get("breaker_threshold", 5)),
                breaker_cooldown=float(config.get("breaker_cooldown", 120.0)),
                host=str(config.get("host", "www.baostock.com")),
                port=int(config.get("port", 80)),
            )

    # ------------------------------------------------------------- fetch_raw
    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        if dataset is Dataset.BARS_1D:
            if request.symbol is None:
                raise ConfigurationError("bars_1d fetch requires request.symbol")
            code = to_baostock_code(request.symbol)
            raw, hfq = self.client.daily_bars_pair(code, request.start, request.end)
            return self._merge_raw_hfq(raw, hfq)
        if dataset is Dataset.CALENDAR:
            return self.client.trade_dates(request.start, request.end)
        raise ConfigurationError(f"dataset {dataset!r} not supported by baostock adapter")

    @staticmethod
    def _merge_raw_hfq(raw: pd.DataFrame, hfq: pd.DataFrame) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame()
        if hfq is None or hfq.empty:
            raise ConfigurationError("upstream returned bars without hfq series")
        date_column = _pick(raw, "date")
        hfq_date_column = _pick(hfq, "date")
        hfq_close_column = _pick(hfq, "close")
        if not (date_column and hfq_date_column and hfq_close_column):
            raise ConfigurationError(
                f"upstream bar frame shape not recognized: raw={list(raw.columns)}, hfq={list(hfq.columns)}"
            )
        factor_frame = hfq[[hfq_date_column, hfq_close_column]].rename(
            columns={hfq_date_column: date_column, hfq_close_column: "__hfq_close"}
        )
        merged = raw.merge(factor_frame, on=date_column, how="left", validate="one_to_one")
        if merged["__hfq_close"].isna().any():
            missing = int(merged["__hfq_close"].isna().sum())
            raise ConfigurationError(
                f"raw/hfq bar series misaligned on {missing} dates; refusing to guess factors"
            )
        return merged

    # -------------------------------------------------------------- normalize
    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        if dataset is Dataset.BARS_1D:
            return self._normalize_bars(raw, request)
        if dataset is Dataset.CALENDAR:
            return self._normalize_calendar(raw, request)
        raise ConfigurationError(f"dataset {dataset!r} not supported by baostock adapter")

    # -- bars -----------------------------------------------------------
    def _normalize_bars(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        symbol = to_canonical_symbol(request.symbol or "")
        if raw is None or raw.empty:
            return pd.DataFrame(columns=list(BAR_COLUMNS))
        date_column = _pick(raw, "date")
        mapping = {
            "open": _pick(raw, "open"),
            "high": _pick(raw, "high"),
            "low": _pick(raw, "low"),
            "close": _pick(raw, "close"),
            "volume": _pick(raw, "volume"),
            "amount": _pick(raw, "amount"),
        }
        missing = [name for name, column in mapping.items() if column is None] + (
            [] if date_column else ["date"]
        )
        if missing:
            raise ConfigurationError(
                f"cannot map upstream bar columns {list(raw.columns)}; missing {missing}"
            )
        # baostock returns suspended days as empty strings; numeric coercion
        # turns them into NaN and the row is dropped below (not a data error).
        frame = pd.DataFrame(
            {
                "symbol": symbol,
                "ts": pd.to_datetime(raw[date_column], errors="coerce").map(daily_ts),
                "open": pd.to_numeric(raw[mapping["open"]], errors="coerce"),
                "high": pd.to_numeric(raw[mapping["high"]], errors="coerce"),
                "low": pd.to_numeric(raw[mapping["low"]], errors="coerce"),
                "close": pd.to_numeric(raw[mapping["close"]], errors="coerce"),
                # baostock volume is already in shares, amount in CNY.
                "volume": pd.to_numeric(raw[mapping["volume"]], errors="coerce"),
                "amount": pd.to_numeric(raw[mapping["amount"]], errors="coerce"),
                "adjust_factor": pd.to_numeric(raw["__hfq_close"], errors="coerce")
                / pd.to_numeric(raw[mapping["close"]], errors="coerce"),
                "quality": "ok",
            }
        )
        frame = frame.dropna(
            subset=["ts", "open", "high", "low", "close", "volume", "amount", "adjust_factor"]
        )
        window = frame["ts"].map(lambda ts: request.start <= ts.date() <= request.end)
        dropped = int((~window).sum())
        if dropped:
            logger.warning(
                "bars %s: %d rows outside [%s, %s] dropped", symbol, dropped, request.start, request.end
            )
        return (
            frame.loc[window]
            .sort_values("ts")
            .reset_index(drop=True)[list(BAR_COLUMNS)]
        )

    # -- calendar ---------------------------------------------------------
    def _normalize_calendar(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        date_column = _pick(raw, "calendar_date")
        flag_column = _pick(raw, "is_trading_day", "isTradingDay", "flag")
        if date_column is None or flag_column is None:
            raise ConfigurationError(
                f"calendar frame shape not recognized: {list(raw.columns)}"
            )
        frame = pd.DataFrame(
            {"trade_date": pd.to_datetime(raw[date_column], errors="coerce").dt.date}
        )
        trading = pd.to_numeric(raw[flag_column], errors="coerce") == 1
        frame = frame.loc[trading.values].dropna()
        frame = frame.drop_duplicates().sort_values("trade_date").reset_index(drop=True)
        in_window = frame["trade_date"].between(request.start, request.end)
        return frame.loc[in_window].reset_index(drop=True)[list(CALENDAR_COLUMNS)]


def _register() -> None:
    from ..registry import register_adapter

    @register_adapter("baostock")
    def _factory(config: Mapping[str, object] | None = None) -> BaostockSourceAdapter:
        return BaostockSourceAdapter(config)


_register()
