"""The akshare source adapter: fetch_raw + normalize for every dataset.

Column conventions handled defensively (upstream APIs have changed
shape before; the adapter absorbs that):

* Eastmoney daily bars (``stock_zh_a_hist``): Chinese columns
  ``日期 开盘 收盘 最高 最低 成交量 成交额`` with volume in *lots*
  (手, 100 shares);
* Sina daily bars (``stock_zh_a_daily``): English columns
  ``date open high low close volume amount`` with volume already in
  shares.

The cumulative adjustment factor is derived per day as
``hfq_close / raw_close`` from the *same* endpoint, which by
construction matches the source's own forward/backward adjustment
arithmetic (hfq = raw × factor; qfq = raw × factor / factor_ref).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Mapping

import pandas as pd

from ...errors import ConfigurationError
from ...schema import (
    BAR_COLUMNS,
    CALENDAR_COLUMNS,
    CORPORATE_ACTION_COLUMNS,
    INSTRUMENT_COLUMNS,
    SUSPENSION_COLUMNS,
    Dataset,
    daily_ts,
)
from ...symbols import (
    infer_board,
    infer_exchange,
    symbol_universe_filter,
    to_canonical_symbol,
    to_source_code,
)
from ..base import FetchRequest
from .client import AkShareClient, FixtureAkShareClient, LiveAkShareClient

logger = logging.getLogger("pulsar_data.akshare")

__all__ = ["AkShareSourceAdapter"]


def _pick(frame: pd.DataFrame, *candidates: str) -> str | None:
    """Return the first candidate column present in ``frame``."""
    lowered = {str(column).strip().lower(): column for column in frame.columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    # substring fallback (e.g. 除权除息日 vs 除权除息日期)
    for candidate in candidates:
        for lowered_name, original in lowered.items():
            if candidate.lower() in lowered_name:
                return original
    return None


def _source_code_text(value: object) -> str:
    """Normalize any source-side code representation to a comparable string.

    Handles numeric codes that lost their leading zeros (``1`` ->
    ``000001``), float artifacts (``1.0``), and prefixed (``sh600519``)
    or suffixed (``600519.SH``) forms; returns a bare zero-padded code
    when possible.
    """
    text = str(value).strip().upper()
    if len(text) == 8 and text[:2] in {"SH", "SZ", "BJ"} and text[2:].isdigit():
        text = text[2:]
    if "." in text:
        head, _, tail = text.partition(".")
        if head.isdigit() and (tail in {"SH", "SZ", "BJ"} or tail.isdigit()):
            text = head  # 600519.SH suffix, or float artifacts like "1.0"
    if text.isdigit():
        return text.zfill(6)
    return text


def _info_wide(info: pd.DataFrame) -> dict[str, object]:
    """Pivot an item/value instrument-info frame into ``{item: value}``.

    ``stock_individual_info_em`` returns a two-column frame whose rows
    are labelled ``股票代码``/``上市时间``/``总股本``/...; the row order
    is not guaranteed, so match by label.
    """
    columns = list(info.columns)
    if len(columns) < 2:
        return {}
    key_column, value_column = columns[0], columns[1]
    wide: dict[str, object] = {}
    for _, row in info.iterrows():
        key = str(row[key_column]).strip()
        if key:
            wide[key] = row[value_column]
    return wide


def _parse_list_date(value: object) -> object:
    """Parse a listing date in ``YYYYMMDD`` or ``YYYY-MM-DD`` form."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    parsed = pd.to_datetime(str(value).strip().replace("-", ""), format="%Y%m%d", errors="coerce")
    return parsed if pd.notna(parsed) else pd.NaT


class AkShareSourceAdapter:
    """``SourceAdapter`` implementation for the akshare free data source."""

    source_id = "akshare"

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        config = dict(config or {})
        fixture_dir = config.get("fixture_dir")
        if fixture_dir:
            self.client: AkShareClient = FixtureAkShareClient(fixture_dir)
        else:
            self.client = LiveAkShareClient(
                min_interval=float(config.get("min_interval", 0.6)),
                retries=int(config.get("retries", 5)),
            )

    # ------------------------------------------------------------- fetch_raw
    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        if dataset is Dataset.BARS_1D:
            if request.symbol is None:
                raise ConfigurationError("bars_1d fetch requires request.symbol")
            code = to_source_code(request.symbol)
            raw, hfq = self.client.daily_bars_pair(code, request.start, request.end)
            return self._merge_raw_hfq(raw, hfq)
        if dataset is Dataset.CALENDAR:
            return self.client.trade_dates()
        if dataset is Dataset.CORPORATE_ACTIONS:
            if request.symbol is None:
                raise ConfigurationError("corporate_actions fetch requires request.symbol")
            code = to_source_code(request.symbol)
            dividends = self.client.dividend_detail(code)
            rights = self.client.rights_detail(code)
            frames = [frame for frame in (dividends, rights) if not frame.empty]
            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True)
        if dataset is Dataset.INSTRUMENTS:
            universe = self.client.universe()
            if request.symbol is None:
                return universe
            # single-symbol mode: attach listing metadata (best effort)
            code = to_source_code(request.symbol)
            code_column = _pick(universe, "代码", "code")
            if code_column is None:
                return universe
            matches = universe[
                universe[code_column].map(_source_code_text) == code
            ].copy()
            info = self.client.instrument_info(code)
            if not matches.empty and not info.empty:
                wide = _info_wide(info)
                for key, value in wide.items():
                    matches[key] = value
            return matches if not matches.empty else universe.iloc[0:0]
        if dataset is Dataset.SUSPENSIONS:
            return self.client.suspensions(request.start)
        raise ConfigurationError(f"dataset {dataset!r} not supported by akshare adapter")

    @staticmethod
    def _merge_raw_hfq(raw: pd.DataFrame, hfq: pd.DataFrame) -> pd.DataFrame:
        if raw.empty:
            return raw
        if hfq.empty:
            raise ConfigurationError("upstream returned bars without hfq series")
        date_column = _pick(raw, "日期", "date")
        hfq_date_column = _pick(hfq, "日期", "date")
        hfq_close_column = _pick(hfq, "收盘", "close")
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
        if dataset is Dataset.CORPORATE_ACTIONS:
            return self._normalize_corporate_actions(raw, request)
        if dataset is Dataset.INSTRUMENTS:
            return self._normalize_instruments(raw, request)
        if dataset is Dataset.SUSPENSIONS:
            return self._normalize_suspensions(raw, request)
        raise ConfigurationError(f"dataset {dataset!r} not supported by akshare adapter")

    # -- bars -----------------------------------------------------------
    def _normalize_bars(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        symbol = to_canonical_symbol(request.symbol or "")
        if raw is None or raw.empty:
            return pd.DataFrame(columns=list(BAR_COLUMNS))
        date_column = _pick(raw, "日期", "date")
        mapping = {
            "open": _pick(raw, "开盘", "open"),
            "close": _pick(raw, "收盘", "close"),
            "high": _pick(raw, "最高", "high"),
            "low": _pick(raw, "最低", "low"),
            "volume": _pick(raw, "成交量", "volume"),
            "amount": _pick(raw, "成交额", "amount"),
        }
        missing = [name for name, column in mapping.items() if column is None] + (
            [] if date_column else ["date"]
        )
        if missing:
            raise ConfigurationError(
                f"cannot map upstream bar columns {list(raw.columns)}; missing {missing}"
            )
        sina_shape = str(date_column).lower() == "date"
        frame = pd.DataFrame(
            {
                "symbol": symbol,
                "ts": pd.to_datetime(raw[date_column]).map(daily_ts),
                "open": pd.to_numeric(raw[mapping["open"]], errors="coerce"),
                "high": pd.to_numeric(raw[mapping["high"]], errors="coerce"),
                "low": pd.to_numeric(raw[mapping["low"]], errors="coerce"),
                "close": pd.to_numeric(raw[mapping["close"]], errors="coerce"),
                # Eastmoney reports volume in lots (手); Sina in shares.
                "volume": pd.to_numeric(raw[mapping["volume"]], errors="coerce")
                * (1.0 if sina_shape else 100.0),
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
            logger.warning("bars %s: %d rows outside [%s, %s] dropped", symbol, dropped, request.start, request.end)
        return (
            frame.loc[window]
            .sort_values("ts")
            .reset_index(drop=True)[list(BAR_COLUMNS)]
        )

    # -- calendar ---------------------------------------------------------
    def _normalize_calendar(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        column = _pick(raw, "trade_date", "日期", "date")
        if column is None:
            raise ConfigurationError(f"calendar frame shape not recognized: {list(raw.columns)}")
        frame = pd.DataFrame({"trade_date": pd.to_datetime(raw[column], errors="coerce").dt.date})
        frame = frame.dropna().drop_duplicates().sort_values("trade_date").reset_index(drop=True)
        in_window = frame["trade_date"].between(request.start, request.end)
        return frame.loc[in_window].reset_index(drop=True)[list(CALENDAR_COLUMNS)]

    # -- corporate actions ------------------------------------------
    def _normalize_corporate_actions(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        symbol = to_canonical_symbol(request.symbol or "")
        if raw is None or raw.empty:
            return pd.DataFrame(columns=list(CORPORATE_ACTION_COLUMNS))

        ex_column = _pick(raw, "除权除息日", "除权日", "ex_date")
        if ex_column is None:
            raise ConfigurationError(
                f"corporate-action frame shape not recognized: {list(raw.columns)}"
            )
        rows: list[dict[str, object]] = []

        progress_column = _pick(raw, "进度")
        status = (
            raw[progress_column].astype(str).str.contains("实施", na=False)
            if progress_column is not None
            else pd.Series(True, index=raw.index)
        )
        bonus_column = _pick(raw, "送股")
        conversion_column = _pick(raw, "转增")
        cash_column = _pick(raw, "派息")
        plan_column = _pick(raw, "配股方案")
        price_column = _pick(raw, "配股价格")
        for _, row in raw.loc[status].iterrows():
            ex_date = pd.to_datetime(row.get(ex_column), errors="coerce")
            if pd.isna(ex_date):
                continue
            cash = float(row[cash_column]) / 10 if cash_column is not None and pd.notna(row.get(cash_column)) else 0.0
            bonus = float(row[bonus_column]) / 10 if bonus_column is not None and pd.notna(row.get(bonus_column)) else 0.0
            bonus += (
                float(row[conversion_column]) / 10
                if conversion_column is not None and pd.notna(row.get(conversion_column))
                else 0.0
            )
            rights_ratio = (
                float(row[plan_column]) / 10
                if plan_column is not None and pd.notna(row.get(plan_column))
                else 0.0
            )
            rights_price = (
                float(row[price_column])
                if price_column is not None and pd.notna(row.get(price_column)) and rights_ratio > 0
                else None
            )
            if cash <= 0 and bonus <= 0 and rights_ratio <= 0:
                continue
            description = f"10派{cash * 10:g}元 送{bonus * 10:g}股 配{rights_ratio * 10:g}股"
            rows.append(
                {
                    "symbol": symbol,
                    "ex_date": ex_date.date(),
                    "cash_dividend_per_share": cash,
                    "bonus_share_ratio": bonus,
                    "rights_issue_ratio": rights_ratio,
                    "rights_issue_price": rights_price,
                    "description": description,
                }
            )
        if not rows:
            return pd.DataFrame(columns=list(CORPORATE_ACTION_COLUMNS))
        frame = pd.DataFrame(rows)
        return (
            frame.sort_values("ex_date").reset_index(drop=True)[list(CORPORATE_ACTION_COLUMNS)]
        )

    # -- instruments ------------------------------------------------------
    def _normalize_instruments(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        code_column = _pick(raw, "代码", "code")
        name_column = _pick(raw, "名称", "name")
        if code_column is None or name_column is None:
            raise ConfigurationError(
                f"universe frame shape not recognized: {list(raw.columns)}"
            )
        list_date_column = _pick(raw, "上市时间")
        shares_column = _pick(raw, "总股本")
        rows: list[dict[str, object]] = []
        seen: set[str] = set()
        for _, row in raw.iterrows():
            try:
                symbol = to_canonical_symbol(_source_code_text(row[code_column]))
            except Exception:  # noqa: BLE001 - universe rows may carry odd codes
                continue
            if symbol in seen or not symbol_universe_filter(symbol):
                continue
            seen.add(symbol)
            name = str(row[name_column])
            list_date = (
                _parse_list_date(row[list_date_column]) if list_date_column is not None else pd.NaT
            )
            shares = (
                pd.to_numeric(row[shares_column], errors="coerce")
                if shares_column is not None
                else None
            )
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "exchange": infer_exchange(symbol).value,
                    "board": infer_board(symbol).value,
                    "is_st": "ST" in name.upper(),
                    "status": "listed",
                    "list_date": list_date,
                    "delist_date": pd.NaT,
                    "shares_outstanding": None if shares is None or pd.isna(shares) else float(shares),
                }
            )
        frame = pd.DataFrame(rows, columns=list(INSTRUMENT_COLUMNS))
        return frame.sort_values("symbol").reset_index(drop=True)

    # -- suspensions ---------------------------------------------------
    def _normalize_suspensions(self, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        if raw is None or raw.empty:
            return pd.DataFrame(columns=list(SUSPENSION_COLUMNS))
        code_column = _pick(raw, "代码", "code")
        start_column = _pick(raw, "停牌时间")
        end_column = _pick(raw, "停牌截止时间")
        expected_column = _pick(raw, "预计复牌时间")
        reason_column = _pick(raw, "停牌原因")
        if code_column is None or start_column is None:
            raise ConfigurationError(
                f"suspension frame shape not recognized: {list(raw.columns)}"
            )
        rows: list[dict[str, object]] = []
        for _, row in raw.iterrows():
            start = pd.to_datetime(row.get(start_column), errors="coerce")
            if pd.isna(start) or not (request.start <= start.date() <= request.end):
                continue
            end = pd.to_datetime(row.get(end_column), errors="coerce") if end_column is not None else pd.NaT
            if pd.isna(end) and expected_column is not None:
                expected = pd.to_datetime(row.get(expected_column), errors="coerce")
                # 预计复牌时间 is the *resumption* day; suspension covers up to the day before.
                if pd.notna(expected):
                    end = expected - timedelta(days=1)
            if pd.isna(end):
                end = start
            if end.date() > request.end:
                end = pd.Timestamp(request.end)
            try:
                symbol = to_canonical_symbol(str(row[code_column]))
            except Exception:  # noqa: BLE001
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "start_date": start.date(),
                    "end_date": end.date(),
                    "reason": str(row.get(reason_column)) if reason_column is not None else "",
                }
            )
        if not rows:
            return pd.DataFrame(columns=list(SUSPENSION_COLUMNS))
        frame = pd.DataFrame(rows).drop_duplicates(
            subset=["symbol", "start_date", "end_date"]
        )
        return frame.sort_values(["symbol", "start_date"]).reset_index(drop=True)[
            list(SUSPENSION_COLUMNS)
        ]


def _register() -> None:
    from ..registry import register_adapter

    @register_adapter("akshare")
    def _factory(config: Mapping[str, object] | None = None) -> AkShareSourceAdapter:
        return AkShareSourceAdapter(config)


_register()
