"""Upstream clients for the akshare adapter.

:class:`AkShareClient` is the narrow interface the adapter talks to;
two implementations exist:

* :class:`LiveAkShareClient` — calls akshare (Eastmoney endpoints
  first, Sina as fallback) under the egress guard, with per-source
  rate limiting, retry/backoff and a circuit breaker;
* :class:`FixtureAkShareClient` — replays raw frames recorded by
  ``scripts/record_fixtures.py`` from a directory, so the whole
  pipeline runs offline (CI, airplanes, air-gapped hosts).

Both return *raw, source-shaped* frames; translation to the canonical
schema happens in the adapter's ``normalize`` step.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Protocol

import pandas as pd

from ...errors import ConfigurationError, FetchError
from ...netguard import install_egress_guard
from ...ratelimit import CircuitBreaker, RateLimiter, with_retry

logger = logging.getLogger("pulsar_data.akshare")

__all__ = ["AkShareClient", "LiveAkShareClient", "FixtureAkShareClient"]


class AkShareClient(Protocol):
    """Raw upstream access the akshare adapter needs."""

    def trade_dates(self) -> pd.DataFrame: ...
    def universe(self) -> pd.DataFrame: ...
    def daily_bars_pair(self, code: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(raw_frame, hfq_frame)`` for one symbol from ONE endpoint.

        Both frames must come from the same source so the derived
        adjustment factor keeps a single anchor.
        """
    def daily_bars_qfq(self, code: str, start: date, end: date) -> pd.DataFrame:
        """Source-side forward-adjusted bars (verification only)."""
    def dividend_detail(self, code: str) -> pd.DataFrame: ...
    def rights_detail(self, code: str) -> pd.DataFrame: ...
    def suspensions(self, on_date: date) -> pd.DataFrame: ...
    def instrument_info(self, code: str) -> pd.DataFrame: ...


class LiveAkShareClient:
    """akshare-backed client: Eastmoney primary, Sina fallback, guarded egress."""

    def __init__(
        self,
        *,
        min_interval: float = 0.6,
        retries: int = 5,
        breaker_threshold: int = 8,
        breaker_cooldown: float = 120.0,
    ) -> None:
        self._limiter = RateLimiter(min_interval)
        self._retries = retries
        self._breaker = CircuitBreaker(breaker_threshold, breaker_cooldown)
        self._ak = None

    # ------------------------------------------------------------------ util
    def _module(self):
        if self._ak is None:
            try:
                import akshare  # heavy import: only when actually fetching
            except ImportError as exc:  # pragma: no cover - depends on extras
                raise ConfigurationError(
                    "akshare is not installed; install pulsar-data[akshare] "
                    "or point the adapter at a fixture directory"
                ) from exc
            self._ak = akshare
        return self._ak

    def _call(self, description: str, fn, /, *args, **kwargs):
        if self._breaker.is_open:
            raise FetchError(
                f"circuit breaker open for akshare upstream; cooling down ({description})"
            )
        self._limiter.wait()
        try:
            with install_egress_guard():
                result = with_retry(
                    lambda: fn(*args, **kwargs),
                    retries=self._retries,
                    retry_on=(ConnectionError, TimeoutError, OSError, ValueError),
                )
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()
        return result

    # ------------------------------------------------------------- endpoints
    def trade_dates(self) -> pd.DataFrame:
        return self._call("trade calendar", self._module().tool_trade_date_hist_sina)

    def universe(self) -> pd.DataFrame:
        ak = self._module()
        try:
            frame = self._call("universe (eastmoney)", ak.stock_zh_a_spot_em)
            return frame[["代码", "名称"]]
        except Exception as exc:
            logger.warning("eastmoney universe failed (%r); falling back to sina", exc)
            frame = self._call("universe (sina)", ak.stock_zh_a_spot)
            return frame[["代码", "名称"]]

    @staticmethod
    def _em_bars(ak, code: str, start: date, end: date, adjust: str) -> pd.DataFrame:
        return ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust=adjust,
        )

    @staticmethod
    def _sina_bars(ak, code_with_prefix: str, start: date, end: date, adjust: str) -> pd.DataFrame:
        return ak.stock_zh_a_daily(
            symbol=code_with_prefix,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust=adjust,
        )

    def _sina_symbol(self, code: str) -> str:
        prefix = "sh" if code.startswith(("6", "9")) else ("bj" if code.startswith(("4", "8")) else "sz")
        return f"{prefix}{code}"

    def daily_bars_pair(self, code: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        ak = self._module()
        try:
            raw = self._call(f"bars {code} raw (eastmoney)", self._em_bars, ak, code, start, end, "")
            hfq = self._call(f"bars {code} hfq (eastmoney)", self._em_bars, ak, code, start, end, "hfq")
            return raw, hfq
        except Exception as exc:
            logger.warning(
                "eastmoney bars for %s failed (%r); falling back to sina", code, exc
            )
        raw = self._call(
            f"bars {code} raw (sina)", self._sina_bars, ak, self._sina_symbol(code), start, end, ""
        )
        hfq = self._call(
            f"bars {code} hfq (sina)", self._sina_bars, ak, self._sina_symbol(code), start, end, "hfq"
        )
        return raw, hfq

    def daily_bars_qfq(self, code: str, start: date, end: date) -> pd.DataFrame:
        ak = self._module()
        try:
            return self._call(f"bars {code} qfq (eastmoney)", self._em_bars, ak, code, start, end, "qfq")
        except Exception as exc:
            logger.warning("eastmoney qfq for %s failed (%r); falling back to sina", code, exc)
        return self._call(
            f"bars {code} qfq (sina)", self._sina_bars, ak, self._sina_symbol(code), start, end, "qfq"
        )

    def dividend_detail(self, code: str) -> pd.DataFrame:
        return self._call(
            f"dividends {code}",
            self._module().stock_history_dividend_detail,
            symbol=code,
            indicator="分红",
        )

    def rights_detail(self, code: str) -> pd.DataFrame:
        try:
            return self._call(
                f"rights {code}",
                self._module().stock_history_dividend_detail,
                symbol=code,
                indicator="配股",
            )
        except Exception as exc:
            logger.warning("rights detail for %s unavailable (%r); using empty frame", code, exc)
            return pd.DataFrame()

    def suspensions(self, on_date: date) -> pd.DataFrame:
        return self._call(
            f"suspensions {on_date}", self._module().stock_tfp_em, date=on_date.strftime("%Y%m%d")
        )

    def instrument_info(self, code: str) -> pd.DataFrame:
        return self._call(f"info {code}", self._module().stock_individual_info_em, symbol=code)


class FixtureAkShareClient:
    """Replay recorded raw frames from a fixture directory (fully offline)."""

    def __init__(self, fixture_dir: str | Path) -> None:
        self.root = Path(fixture_dir)
        if not self.root.is_dir():
            raise ConfigurationError(f"fixture directory not found: {self.root}")
        manifest = self.root / "manifest.json"
        self.manifest: dict = (
            json.loads(manifest.read_text(encoding="utf-8")) if manifest.exists() else {}
        )

    def _read(self, relative: str) -> pd.DataFrame:
        path = self.root / relative
        if not path.exists():
            logger.debug("fixture missing %s -> empty frame", relative)
            return pd.DataFrame()
        try:
            return pd.read_csv(path, dtype={"代码": str, "股票代码": str})
        except pd.errors.EmptyDataError:
            # e.g. an empty rights-issue history recorded as a bare newline
            return pd.DataFrame()

    @staticmethod
    def _window(frame: pd.DataFrame, date_column: str, start: date, end: date) -> pd.DataFrame:
        if frame.empty or date_column not in frame.columns:
            return frame
        dates = pd.to_datetime(frame[date_column], errors="coerce")
        keep = dates.dt.date.between(start, end) & dates.notna()
        return frame.loc[keep]

    def trade_dates(self) -> pd.DataFrame:
        return self._read("calendar.csv")

    def universe(self) -> pd.DataFrame:
        return self._read("universe.csv")

    def daily_bars_pair(self, code: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        symbol = self._canonical(code)
        raw = self._window(self._read(f"bars/{symbol}.raw.csv"), "日期", start, end)
        if raw.empty:
            raw = self._window(self._read(f"bars/{symbol}.raw.csv"), "date", start, end)
        hfq = self._window(self._read(f"bars/{symbol}.hfq.csv"), "日期", start, end)
        if hfq.empty:
            hfq = self._window(self._read(f"bars/{symbol}.hfq.csv"), "date", start, end)
        return raw, hfq

    def daily_bars_qfq(self, code: str, start: date, end: date) -> pd.DataFrame:
        symbol = self._canonical(code)
        frame = self._window(self._read(f"bars/{symbol}.qfq.csv"), "日期", start, end)
        if frame.empty:
            frame = self._window(self._read(f"bars/{symbol}.qfq.csv"), "date", start, end)
        return frame

    def dividend_detail(self, code: str) -> pd.DataFrame:
        return self._read(f"dividends/{self._canonical(code)}.csv")

    def rights_detail(self, code: str) -> pd.DataFrame:
        return self._read(f"rights/{self._canonical(code)}.csv")

    def suspensions(self, on_date: date) -> pd.DataFrame:
        return self._read("suspensions.csv")

    def instrument_info(self, code: str) -> pd.DataFrame:
        return self._read(f"instrument_info/{self._canonical(code)}.csv")

    @staticmethod
    def _canonical(code: str) -> str:
        """Fixture files are keyed by canonical symbol (``SH600519``)."""
        from ...symbols import to_canonical_symbol

        return to_canonical_symbol(code)
