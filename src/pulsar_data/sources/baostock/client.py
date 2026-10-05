"""Upstream client for the baostock adapter (free, credential-less backup source).

baostock is a socket-protocol SDK: every session must ``login()`` before
querying and ``logout()`` afterwards.  :class:`BaostockSession`
encapsulates that lifecycle (lazy anonymous login, explicit logout,
thread-safe), so callers never see a raw ``bs.*`` call.

:class:`LiveBaostockClient` funnels every query through the same
reliability plumbing as the akshare client — per-source rate limiting,
retry with exponential backoff, and a consecutive-failure circuit
breaker — and reuses :func:`pulsar_data.netguard.validate_url` to check
the server host (scheme, forbidden names, resolved addresses) *before*
the socket session is established.  baostock carries no credentials:
login is anonymous and this module never reads secrets.

The client also exposes minute-frequency bars (``frequency="5" | "15" |
"30" | "60"``) and raw adjustment-factor events for callers that need
them; the adapter currently normalizes the daily frequency into the
canonical ``bars_1d`` schema (minute bars land with the ``bars_1min``
dataset, 二期).
"""

from __future__ import annotations

import logging
import threading
from datetime import date
from typing import Callable, Protocol

import pandas as pd

from ...errors import ConfigurationError, EgressViolation, FetchError
from ...netguard import validate_url
from ...ratelimit import CircuitBreaker, RateLimiter, with_retry

logger = logging.getLogger("pulsar_data.baostock")

__all__ = [
    "BaostockClient",
    "BaostockSession",
    "LiveBaostockClient",
    "to_baostock_code",
    "from_baostock_code",
]

#: Default baostock server (socket protocol on port 80).
DEFAULT_SERVER_HOST = "www.baostock.com"
DEFAULT_SERVER_PORT = 80

#: adjustflag semantics in baostock: 3 = 不复权 (raw), 1 = 后复权, 2 = 前复权.
ADJUST_FLAG_RAW = "3"
ADJUST_FLAG_HFQ = "1"

#: Field list for k-data queries (raw and hfq alike).
_K_DATA_FIELDS = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"

#: Exception types a baostock call may raise transiently.
_TRANSIENT = (ConnectionError, TimeoutError, OSError, ValueError)


def to_baostock_code(symbol: str) -> str:
    """Canonical ``SH600519`` -> baostock ``sh.600519``."""
    from ...symbols import to_canonical_symbol

    canonical = to_canonical_symbol(symbol)
    return f"{canonical[:2].lower()}.{canonical[2:]}"


def from_baostock_code(code: str) -> str:
    """baostock ``sh.600519`` -> canonical ``SH600519``."""
    from ...symbols import to_canonical_symbol

    return to_canonical_symbol(str(code).strip().replace(".", "", 1))


class BaostockSession:
    """Encapsulates the baostock login/logout socket session lifecycle.

    The SDK keeps one process-global connection; this wrapper makes the
    lifecycle explicit and re-entrant: ``ensure_logged_in()`` before any
    query, ``logout()`` when done (or via ``close()`` / context
    manager).  A new query after logout transparently logs in again.
    The host is egress-validated once per login attempt.
    """

    def __init__(
        self,
        module_factory: Callable[[], object],
        *,
        host: str = DEFAULT_SERVER_HOST,
        port: int = DEFAULT_SERVER_PORT,
        resolver: Callable[[str], object] | None = None,
    ) -> None:
        self._module_factory = module_factory
        self._host = host
        self._port = port
        self._resolver = resolver
        self._lock = threading.RLock()
        self._logged_in = False
        self.logins = 0
        self.logouts = 0

    @property
    def logged_in(self) -> bool:
        return self._logged_in

    def _validate_egress(self) -> None:
        """Reuse the shared netguard check on the socket target.

        baostock speaks a raw socket protocol rather than requests, so
        the request-level guard cannot see it; validating the resolved
        host before connecting keeps the same security baseline
        (http scheme analog, no loopback/private/reserved targets).
        """
        validate_url(f"http://{self._host}/", resolver=self._resolver)

    def ensure_logged_in(self) -> object:
        """Return the baostock module with an active anonymous session."""
        with self._lock:
            if self._logged_in:
                module = self._module_factory()
                return module
            self._validate_egress()
            module = self._module_factory()
            result = module.login()
            error_code = str(getattr(result, "error_code", "1"))
            if error_code != "0":
                raise FetchError(
                    f"baostock login failed: {getattr(result, 'error_msg', 'unknown error')}"
                )
            self._logged_in = True
            self.logins += 1
            logger.debug("baostock session established to %s:%s", self._host, self._port)
            return module

    def logout(self) -> None:
        with self._lock:
            if not self._logged_in:
                return
            module = self._module_factory()
            try:
                module.logout()
            finally:
                self._logged_in = False
                self.logouts += 1

    def close(self) -> None:
        self.logout()

    def __enter__(self) -> "BaostockSession":
        self.ensure_logged_in()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.logout()


class BaostockClient(Protocol):
    """Raw upstream access the baostock adapter needs."""

    def daily_bars_pair(self, code: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(raw_frame, hfq_frame)`` for one symbol (daily, unadjusted + 后复权)."""
    def minute_bars(self, code: str, start: date, end: date, frequency: str) -> pd.DataFrame: ...
    def adjust_factor_events(self, code: str, start: date, end: date) -> pd.DataFrame: ...
    def trade_dates(self, start: date, end: date) -> pd.DataFrame: ...
    def close(self) -> None: ...


class LiveBaostockClient:
    """baostock-backed client: session-scoped, rate-limited, circuit-broken."""

    def __init__(
        self,
        *,
        min_interval: float = 0.5,
        retries: int = 4,
        breaker_threshold: int = 5,
        breaker_cooldown: float = 120.0,
        host: str = DEFAULT_SERVER_HOST,
        port: int = DEFAULT_SERVER_PORT,
        module_factory: Callable[[], object] | None = None,
        resolver: Callable[[str], object] | None = None,
    ) -> None:
        self._limiter = RateLimiter(min_interval)
        self._retries = retries
        self._breaker = CircuitBreaker(breaker_threshold, breaker_cooldown)
        self._session = BaostockSession(
            module_factory or self._import_baostock,
            host=host,
            port=port,
            resolver=resolver,
        )

    # ------------------------------------------------------------------ util
    @staticmethod
    def _import_baostock() -> object:
        try:
            import baostock  # heavy import: only when actually querying
        except ImportError as exc:  # pragma: no cover - depends on extras
            raise ConfigurationError(
                "baostock is not installed; pip install baostock "
                "(the module is only needed for live fetching)"
            ) from exc
        return baostock

    def login(self) -> object:
        """Open (or reuse) the anonymous session; returns the baostock module."""
        return self._session.ensure_logged_in()

    def logout(self) -> None:
        self._session.logout()

    @property
    def session(self) -> BaostockSession:
        return self._session

    def close(self) -> None:
        self.logout()

    def __enter__(self) -> "LiveBaostockClient":
        self.login()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _query(self, description: str, fn, /, *args, **kwargs) -> pd.DataFrame:
        """Run one SDK query: breaker + limiter + retry, result -> DataFrame."""
        if self._breaker.is_open:
            raise FetchError(
                f"circuit breaker open for baostock upstream; cooling down ({description})"
            )
        self._limiter.wait()
        try:
            self._session.ensure_logged_in()

            def _run() -> pd.DataFrame:
                result_set = fn(*args, **kwargs)
                return _result_to_frame(result_set)

            frame = with_retry(_run, retries=self._retries, retry_on=_TRANSIENT)
        except EgressViolation:
            # a forbidden destination is a configuration/security problem,
            # not a transient failure — never counted against the breaker
            raise
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()
        return frame

    # ------------------------------------------------------------- endpoints
    def daily_bars_pair(self, code: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
        raw = self._query(
            f"bars {code} raw",
            _query_history_k_data_plus,
            self._session,
            code,
            start,
            end,
            "d",
            ADJUST_FLAG_RAW,
        )
        hfq = self._query(
            f"bars {code} hfq",
            _query_history_k_data_plus,
            self._session,
            code,
            start,
            end,
            "d",
            ADJUST_FLAG_HFQ,
        )
        return raw, hfq

    def minute_bars(self, code: str, start: date, end: date, frequency: str = "5") -> pd.DataFrame:
        if frequency not in {"5", "15", "30", "60"}:
            raise ConfigurationError(f"unsupported minute frequency {frequency!r}")
        return self._query(
            f"bars {code} {frequency}min",
            _query_history_k_data_plus,
            self._session,
            code,
            start,
            end,
            frequency,
            ADJUST_FLAG_RAW,
        )

    def adjust_factor_events(self, code: str, start: date, end: date) -> pd.DataFrame:
        return self._query(
            f"adjust factors {code}",
            _query_adjust_factor,
            self._session,
            code,
            start,
            end,
        )

    def trade_dates(self, start: date, end: date) -> pd.DataFrame:
        return self._query(
            "trade calendar", _query_trade_dates, self._session, start, end
        )


# ----------------------------------------------------------------------
# Thin SDK call helpers: keep the baostock calling convention in one place
# ----------------------------------------------------------------------
def _result_to_frame(result_set: object) -> pd.DataFrame:
    """Drain a baostock ResultData into a DataFrame (or raise FetchError)."""
    error_code = str(getattr(result_set, "error_code", "1"))
    if error_code != "0":
        raise FetchError(
            f"baostock query failed: {getattr(result_set, 'error_msg', 'unknown error')} "
            f"(code {error_code})"
        )
    rows: list[list[str]] = []
    while getattr(result_set, "next", lambda: False)():
        rows.append(result_set.get_row_data())
    fields = list(getattr(result_set, "fields", []) or [])
    return pd.DataFrame(rows, columns=fields)


def _query_history_k_data_plus(
    session: BaostockSession,
    code: str,
    start: date,
    end: date,
    frequency: str,
    adjustflag: str,
) -> object:
    module = session.ensure_logged_in()
    fields = _K_DATA_FIELDS if frequency == "d" else (
        "date,time,code,open,high,low,close,volume,amount,adjustflag"
    )
    return module.query_history_k_data_plus(
        code,
        fields,
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
        frequency=frequency,
        adjustflag=adjustflag,
    )


def _query_adjust_factor(session: BaostockSession, code: str, start: date, end: date) -> object:
    module = session.ensure_logged_in()
    return module.query_adjust_factor(
        code, start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d")
    )


def _query_trade_dates(session: BaostockSession, start: date, end: date) -> object:
    module = session.ensure_logged_in()
    return module.query_trade_dates(
        start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d")
    )
