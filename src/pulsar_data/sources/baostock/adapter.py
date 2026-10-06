"""The baostock source adapter: fetch_raw + normalize for daily and minute bars.

baostock is the free, credential-less source of the data-source matrix
(design: 「baostock —— 日线、分钟线（复权因子完备）；分钟级主源
（5/15/30/60 分钟）」).  Raw shapes handled here:

* k-data (``query_history_k_data_plus``): string columns
  ``date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST``
  fetched twice per window — ``adjustflag=3`` (不复权, raw) and ``adjustflag=1``
  (后复权) — because the lake stores raw prices plus a cumulative
  ``adjust_factor`` derived as ``hfq_close / raw_close`` from the same
  endpoint (single anchor, the same rule the akshare adapter applies);
* minute k-data (``frequency="5" | "15" | "30" | "60"``): string columns
  ``date,time,code,open,high,low,close,volume,amount,adjustflag`` where
  ``time`` (``YYYYMMDDHHMMSSsss``) labels the bar's interval **end**;
  normalization shifts it to the lake's left-closed interval start.  The
  cumulative ``adjust_factor`` comes from ``query_adjust_factor``
  (``dividOperateDate``/``adjustFactor``, an as-of join by trading day —
  verified to equal the daily ``hfq_close / raw_close`` anchor exactly),
  queried from market start so pre-window events still anchor the join;
* trade calendar (``query_trade_dates``): ``calendar_date,is_trading_day``
  where non-trading days carry ``is_trading_day == 0`` and are dropped;
* volume arrives in shares and amount in CNY — the canonical units, no
  conversion (unlike akshare's Eastmoney lots).

Suspended days come back with empty OHLC strings; those rows are dropped
(suspension marking itself is the quality/backfill task's concern).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Mapping

import numpy as np
import pandas as pd

from ...errors import ConfigurationError
from ...schema import (
    BAR_COLUMNS,
    CALENDAR_COLUMNS,
    DATASET_MINUTES,
    Dataset,
    daily_ts,
    minute_ts,
)
from ...symbols import to_canonical_symbol
from ..base import FetchRequest
from .client import FixtureBaostockClient, LiveBaostockClient, to_baostock_code

logger = logging.getLogger("pulsar_data.baostock")

__all__ = ["BaostockSourceAdapter"]

#: Earliest query start for the full-history adjust-factor walk (SSE open).
_MARKET_START = date(1990, 1, 1)


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
            fixture_dir = config.get("fixture_dir")
            if fixture_dir:
                self.client = FixtureBaostockClient(fixture_dir)
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
        if dataset in DATASET_MINUTES:
            if request.symbol is None:
                raise ConfigurationError(f"{dataset.value} fetch requires request.symbol")
            code = to_baostock_code(request.symbol)
            frequency = str(DATASET_MINUTES[dataset])
            minute = self.client.minute_bars(code, request.start, request.end, frequency)
            factors = self.client.adjust_factor_events(code, _MARKET_START, request.end)
            return self._merge_minute_factors(minute, factors)
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

    @staticmethod
    def _merge_minute_factors(minute: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
        """Attach the as-of cumulative ``__adjust_factor`` to minute bars.

        ``factors`` is the full-history ``query_adjust_factor`` event list
        (``dividOperateDate``/``adjustFactor``).  A bar's factor is the
        latest event on or before its trading day; before the first event
        the factor is 1.0 (hfq prices start equal to raw prices).  The
        query starts at market open, so events before the requested bar
        window still anchor the join — a window with no events of its own
        inherits the standing factor instead of collapsing to 1.0.
        """
        if minute is None or minute.empty:
            return pd.DataFrame()
        date_column = _pick(minute, "date")
        if date_column is None:
            raise ConfigurationError(
                f"minute bar frame shape not recognized: {list(minute.columns)}"
            )
        merged = minute.copy()
        if factors is None or factors.empty:
            merged["__adjust_factor"] = 1.0
            return merged
        event_date = _pick(factors, "dividOperateDate", "adjustDate", "dividDate")
        factor_column = _pick(factors, "adjustFactor", "dividAdjustFactor", "backAdjustFactor")
        if event_date is None or factor_column is None:
            raise ConfigurationError(
                f"adjust-factor frame shape not recognized: {list(factors.columns)}"
            )
        events = (
            pd.DataFrame(
                {
                    "date": pd.to_datetime(factors[event_date], errors="coerce"),
                    "factor": pd.to_numeric(factors[factor_column], errors="coerce"),
                }
            )
            .dropna()
            .sort_values("date", kind="stable")
            .drop_duplicates(subset=["date"], keep="last")  # last event per date wins
        )
        event_keys = events["date"].to_numpy()
        bar_keys = pd.to_datetime(merged[date_column], errors="coerce").to_numpy()
        # as-of join backwards: index of the last event <= each bar date
        position = np.searchsorted(event_keys, bar_keys, side="right")
        values = events["factor"].to_numpy()
        factor = np.where(position > 0, values[np.maximum(position - 1, 0)], 1.0)
        factor = np.where(pd.isna(bar_keys), 1.0, factor)  # undated rows drop in normalize
        merged["__adjust_factor"] = factor
        return merged

    # -------------------------------------------------------------- normalize
    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        if dataset is Dataset.BARS_1D:
            return self._normalize_bars(raw, request)
        if dataset in DATASET_MINUTES:
            return self._normalize_minute_bars(raw, request, DATASET_MINUTES[dataset])
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

    # -- minute bars -------------------------------------------------------
    def _normalize_minute_bars(
        self, raw: pd.DataFrame, request: FetchRequest, minutes: int
    ) -> pd.DataFrame:
        symbol = to_canonical_symbol(request.symbol or "")
        if raw is None or raw.empty:
            return pd.DataFrame(columns=list(BAR_COLUMNS))
        date_column = _pick(raw, "date")
        time_column = _pick(raw, "time")
        mapping = {
            "open": _pick(raw, "open"),
            "high": _pick(raw, "high"),
            "low": _pick(raw, "low"),
            "close": _pick(raw, "close"),
            "volume": _pick(raw, "volume"),
            "amount": _pick(raw, "amount"),
        }
        missing = [name for name, column in mapping.items() if column is None]
        missing += [] if date_column else ["date"]
        missing += [] if time_column else ["time"]
        if "__adjust_factor" not in raw.columns:
            missing.append("__adjust_factor")
        if missing:
            raise ConfigurationError(
                f"cannot map upstream minute-bar columns {list(raw.columns)}; missing {missing}"
            )
        # ``time`` labels the interval end (YYYYMMDDHHMMSSsss); shift to the
        # left-closed start.  Suspended bars arrive as empty strings and drop
        # out through the same numeric-coercion path as the daily normalize.
        stamps = pd.to_datetime(
            raw[time_column].astype(str).str.slice(0, 14), format="%Y%m%d%H%M%S", errors="coerce"
        )
        frame = pd.DataFrame(
            {
                "symbol": symbol,
                "ts": [minute_ts(stamp, minutes=minutes) for stamp in stamps],
                "open": pd.to_numeric(raw[mapping["open"]], errors="coerce"),
                "high": pd.to_numeric(raw[mapping["high"]], errors="coerce"),
                "low": pd.to_numeric(raw[mapping["low"]], errors="coerce"),
                "close": pd.to_numeric(raw[mapping["close"]], errors="coerce"),
                "volume": pd.to_numeric(raw[mapping["volume"]], errors="coerce"),
                "amount": pd.to_numeric(raw[mapping["amount"]], errors="coerce"),
                "adjust_factor": pd.to_numeric(raw["__adjust_factor"], errors="coerce"),
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
                "minute bars %s: %d rows outside [%s, %s] dropped",
                symbol, dropped, request.start, request.end,
            )
        return (
            frame.loc[window]
            .sort_values("ts")
            .drop_duplicates(subset=["symbol", "ts"], keep="last")
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
