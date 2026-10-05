"""Realtime snapshot collectors over public free quote endpoints.

Design mapping (Pulsar data-source matrix): Sina and Eastmoney public
quote interfaces are the free realtime-snapshot sources.  Both are plain
HTTP GET endpoints with no credentials — nothing secret is stored or
logged.

Security and reliability reuse the D1 framework verbatim:

* every collector issues requests through
  :class:`pulsar_data.netguard.SafeHTTPSession`, so scheme and target-host
  validation (http/https only; loopback, private, link-local, reserved,
  multicast and unspecified addresses rejected — on the *resolved*
  addresses, not the hostname string) happens before any bytes leave the
  process;
* each source owns a :class:`pulsar_data.ratelimit.RateLimiter` (minimum
  interval between upstream requests) and a
  :class:`pulsar_data.ratelimit.CircuitBreaker` (consecutive-failure
  tripping with cooldown), mirroring the D1 akshare client;
* :class:`FailoverQuoteSource` routes polls primary → fallback and records
  degradation when it has to switch (the realtime form of the design's
  source failover rule).

Upstream truth per source (both verified against live responses,
2026-10-05):

* Sina ``hq.sinajs.cn/list=...`` — one GBK-encoded ``var hq_str_...``
  line per symbol, ~34 comma-separated fields for A-share stocks:
  0 name, 1 open, 2 prev close, 3 last, 4 high, 5 low, 6 bid1 price,
  7 ask1 price, 8 cumulative volume (shares), 9 cumulative amount (CNY),
  10..19 five bid levels as (volume, price) pairs, 20..29 five ask levels
  as (volume, price) pairs, 30 date (yyyy-mm-dd), 31 time (HH:MM:SS),
  32 status.  Fields 6/7 repeat the bid1/ask1 prices of 11/21 — the
  parser requires that repetition as a structural guard against
  non-stock lines (indices carry a different layout).  A ``Referer``
  header is mandatory since 2022.  Suspended / unknown symbols come back
  as empty strings — the collector simply omits them and the dispatcher
  raises a MISSING marker.
* Eastmoney ``push2.eastmoney.com/api/qt/ulist.np/get`` — batch JSON
  quotes by ``secid`` (``1.600519`` Shanghai, ``0.000001`` Shenzhen /
  Beijing).  With ``fltt=2&invt=2`` numeric fields arrive as decimals:
  ``f2`` last price, ``f5`` volume in lots of 100 shares, ``f6`` amount
  in CNY, ``f12`` code, ``f13`` market (1 = Shanghai, 0 = Shenzhen /
  Beijing); ``f2 == "-"`` marks a quote without a last price (suspended).
  Deliberately *no five-level book* and no per-quote clock: Eastmoney's
  book/timestamp field numbering drifts between endpoint versions
  (``f86`` is a ratio on ``ulist``, not a timestamp), and a crossed book
  or a wrong clock is worse than none — the contract allows empty
  ``bids``/``asks``, snapshots carry the poll time as ``ts``, and Sina
  (whose layout has been stable for a decade) remains primary with the
  true quote clock.

Both implementations are synchronous and stateless between polls; all
parsing is pure so CI can drive it from canned payloads.
"""

from __future__ import annotations

import json
import logging
import re
import time as _time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Protocol, Sequence
from zoneinfo import ZoneInfo

import requests
from pulsar_contracts import QuoteLevel

from ..errors import ConfigurationError, EgressViolation, FetchError
from ..netguard import SafeHTTPSession
from ..ratelimit import CircuitBreaker, RateLimiter, with_retry
from ..symbols import to_canonical_symbol

logger = logging.getLogger("pulsar_data.realtime")

__all__ = [
    "RawSnapshot",
    "SnapshotSource",
    "SinaQuoteSource",
    "EastmoneyQuoteSource",
    "FailoverQuoteSource",
    "Degradation",
    "build_default_source",
]

_SHANGHAI = ZoneInfo("Asia/Shanghai")

#: Sina prefixes per canonical exchange prefix (lowercase, wire form).
_SINA_PREFIX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}

#: Eastmoney market ids per canonical exchange prefix.
_EM_MARKET = {"SH": "1", "SZ": "0", "BJ": "0"}

_SINA_LINE_RE = re.compile(r'var\s+hq_str_(?P<key>[a-z]{2}\d{6})\s*=\s*"(?P<body>[^"]*)"\s*;')

_SINA_URL = "https://hq.sinajs.cn/list={codes}"
_SINA_HEADERS = {
    "Referer": "https://finance.sina.com.cn",
    "User-Agent": "pulsar-data/0.1 (market-data research)",
}

_EM_URL = (
    "https://push2.eastmoney.com/api/qt/ulist.np/get"
    "?fltt=2&invt=2&fields=f2%2Cf5%2Cf6%2Cf12%2Cf13%2Cf124&secids={secids}"
)

_EM_CODE_RE = re.compile(r"\d{6}")

#: Verified Sina stock layout (see module docstring): last/volume/amount on
#: 3/8/9, book as (volume, price) pairs on bids 10..19 and asks 20..29.
_SINA_LAST_FIELD = 3
_SINA_VOLUME_FIELD = 8
_SINA_AMOUNT_FIELD = 9
_SINA_BIDS_START = 10
_SINA_ASKS_START = 20
_SINA_DATE_FIELD = 30
_SINA_TIME_FIELD = 31
#: Structural stock-line guard: bid1/ask1 prices repeat on 6/7 and 11/21.
_SINA_BID1_PREVIEW = 6
_SINA_ASK1_PREVIEW = 7


@dataclass(frozen=True)
class RawSnapshot:
    """One normalized quote, source-agnostic, ready for the dispatcher.

    ``ts`` is timezone-aware Asia/Shanghai; ``volume`` is cumulative day
    volume in shares and ``amount`` cumulative turnover in CNY (both match
    the ``Snapshot`` contract fields).
    """

    symbol: str
    ts: datetime
    last_price: float
    volume: float
    amount: float
    bids: tuple[QuoteLevel, ...] = ()
    asks: tuple[QuoteLevel, ...] = ()


class SnapshotSource(Protocol):
    """A pollable realtime quote source (internal contract)."""

    name: str

    def poll(self, symbols: Sequence[str]) -> dict[str, RawSnapshot]:
        """Return the quotes found for ``symbols`` (canonical form) now.

        Symbols the source has no usable quote for are simply absent from
        the result — turning absence into markers is the dispatcher's job,
        never the collector's.  Transport-level failure raises
        :class:`~pulsar_data.errors.FetchError`.
        """
        ...


def _canonical_symbols(symbols: Iterable[str]) -> list[str]:
    """Canonicalize (and de-duplicate, order-preserving) ``symbols``."""
    return list(dict.fromkeys(to_canonical_symbol(s) for s in symbols))


def _levels(pairs: Iterable[tuple[float, float]]) -> tuple[QuoteLevel, ...]:
    """Build book levels from (price, volume) pairs, dropping empty ones."""
    return tuple(QuoteLevel(price=price, volume=volume) for price, volume in pairs if price > 0)


def _float(value: Any) -> float | None:
    """Best-effort float conversion; ``None`` when the field is unusable."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class _GuardedHttpMixin:
    """Shared limiter / breaker / guarded-session plumbing for collectors."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        min_interval: float = 0.5,
        breaker_threshold: int = 5,
        breaker_cooldown: float = 60.0,
        timeout: float = 5.0,
        retries: int = 1,
    ) -> None:
        self._session = session if session is not None else SafeHTTPSession()
        self._limiter = RateLimiter(min_interval)
        self._breaker = CircuitBreaker(breaker_threshold, breaker_cooldown)
        self._timeout = timeout
        self._retries = max(1, retries)

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        """One rate-limited, guarded, at-most-``retries`` GET.

        Unlike the batch backfill path (D1's ``with_retry`` sleeps between
        attempts) a realtime poll must never block for long: the poll loop
        itself is the retry, so the default here is a single attempt and
        consecutive failures trip the circuit breaker instead.
        """
        source = getattr(self, "name", type(self).__name__)
        if self._breaker.is_open:
            raise FetchError(f"circuit breaker open for {source} upstream; cooling down")
        self._limiter.wait()
        try:
            response = with_retry(
                lambda: self._session.get(url, timeout=self._timeout, **kwargs),
                retries=self._retries,
            )
        except EgressViolation:
            # security signal: never masked behind a generic fetch failure
            self._breaker.record_failure()
            raise
        except Exception as exc:
            self._breaker.record_failure()
            raise FetchError(f"quote poll failed: {exc!r}") from exc
        self._breaker.record_success()
        if response.status_code != 200:
            self._breaker.record_failure()
            raise FetchError(f"quote poll returned HTTP {response.status_code}")
        return response


class SinaQuoteSource(_GuardedHttpMixin):
    """Sina public batch quote endpoint (five-level book, per-quote clock)."""

    name = "sina"

    def poll(self, symbols: Sequence[str]) -> dict[str, RawSnapshot]:
        wanted = _canonical_symbols(symbols)
        if not wanted:
            return {}
        url = _SINA_URL.format(codes=",".join(f"{_SINA_PREFIX[s[:2]]}{s[2:]}" for s in wanted))
        response = self._get(url, headers=dict(_SINA_HEADERS))
        return self.parse(response.content, wanted)

    # ------------------------------------------------------------------ parse
    @staticmethod
    def parse(body: bytes, wanted: Sequence[str]) -> dict[str, RawSnapshot]:
        """Parse a raw Sina response into snapshots for ``wanted`` symbols.

        Public and pure so tests run against canned payloads offline.
        Sina encodes GBK; unknown / suspended symbols arrive as empty
        strings and are left out of the result.
        """
        wanted_set = set(wanted)
        found: dict[str, RawSnapshot] = {}
        text = body.decode("gbk", errors="replace")
        for match in _SINA_LINE_RE.finditer(text):
            symbol = to_canonical_symbol(match.group("key"))
            if symbol not in wanted_set or symbol in found:
                continue
            snapshot = _parse_sina_fields(symbol, match.group("body").split(","))
            if snapshot is not None:
                found[symbol] = snapshot
        return found


def _parse_sina_fields(symbol: str, fields: list[str]) -> RawSnapshot | None:
    """One Sina stock line → :class:`RawSnapshot`; ``None`` if unusable."""
    if len(fields) <= _SINA_TIME_FIELD:
        return None  # not a stock line (indices carry a different layout)
    # structural guard: for stocks the bid1/ask1 preview (6/7) repeats the
    # first book pair (11/21) verbatim; non-stock lines never satisfy this
    if (
        fields[_SINA_BID1_PREVIEW] != fields[_SINA_BIDS_START + 1]
        or fields[_SINA_ASK1_PREVIEW] != fields[_SINA_ASKS_START + 1]
    ):
        return None
    last_price = _float(fields[_SINA_LAST_FIELD])
    volume = _float(fields[_SINA_VOLUME_FIELD])
    amount = _float(fields[_SINA_AMOUNT_FIELD])
    if last_price is None or last_price <= 0 or volume is None or amount is None:
        return None  # suspended / pre-open / malformed: no usable quote
    try:
        ts = datetime.strptime(
            f"{fields[_SINA_DATE_FIELD]} {fields[_SINA_TIME_FIELD]}", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=_SHANGHAI)
    except ValueError:
        return None
    try:
        # Sina book layout: (volume, price) pairs — bids on 10..19, asks on 20..29
        bids = _levels(
            (float(fields[_SINA_BIDS_START + 2 * i + 1]), float(fields[_SINA_BIDS_START + 2 * i]))
            for i in range(5)
        )
        asks = _levels(
            (float(fields[_SINA_ASKS_START + 2 * i + 1]), float(fields[_SINA_ASKS_START + 2 * i]))
            for i in range(5)
        )
    except ValueError:
        return None
    return RawSnapshot(
        symbol=symbol,
        ts=ts,
        last_price=last_price,
        volume=volume,
        amount=amount,
        bids=bids,
        asks=asks,
    )


class EastmoneyQuoteSource(_GuardedHttpMixin):
    """Eastmoney public batch quote endpoint (book-less fallback source)."""

    name = "eastmoney"

    def __init__(self, *, clock: Callable[[], datetime] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._clock = clock or (lambda: datetime.now(tz=_SHANGHAI))

    def poll(self, symbols: Sequence[str]) -> dict[str, RawSnapshot]:
        wanted = _canonical_symbols(symbols)
        if not wanted:
            return {}
        secids = ",".join(f"{_EM_MARKET[s[:2]]}.{s[2:]}" for s in wanted)
        response = self._get(_EM_URL.format(secids=secids))
        return self.parse(response.text, wanted, poll_ts=self._clock())

    # ------------------------------------------------------------------ parse
    @staticmethod
    def parse(
        text: str, wanted: Sequence[str], *, poll_ts: datetime
    ) -> dict[str, RawSnapshot]:
        """Parse an ``ulist.np/get`` JSON body into snapshots (pure, offline-testable).

        ``poll_ts`` is the (Asia/Shanghai) time the poll was issued; it
        becomes each snapshot's ``ts`` because this endpoint's field set
        carries no reliable per-quote clock.
        """
        wanted_set = set(wanted)
        found: dict[str, RawSnapshot] = {}
        try:
            payload = json.loads(text)
            rows = payload["data"]["diff"] or []
        except (ValueError, KeyError, TypeError):
            raise FetchError("eastmoney response is not a usable quote payload") from None
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = _em_row_symbol(row)
            if symbol is None or symbol not in wanted_set or symbol in found:
                continue
            snapshot = _em_row_snapshot(symbol, row, poll_ts)
            if snapshot is not None:
                found[symbol] = snapshot
        return found


def _em_row_symbol(row: dict[str, Any]) -> str | None:
    """Rebuild the canonical symbol from Eastmoney ``f12`` (code) + ``f13`` (market)."""
    code = row.get("f12")
    market = row.get("f13")
    if not isinstance(code, str) or not _EM_CODE_RE.fullmatch(code):
        return None
    if market == 1:
        return f"SH{code}"
    if code.startswith(("43", "83", "87", "88", "92")):
        return f"BJ{code}"
    return f"SZ{code}"


def _em_row_snapshot(symbol: str, row: dict[str, Any], poll_ts: datetime) -> RawSnapshot | None:
    """One ``ulist`` row → :class:`RawSnapshot`; ``None`` when unusable.

    ``f2`` arrives as ``"-"`` for quotes without a last price — that is
    the suspended case and maps to absence (a MISSING marker upstream).
    ``f5`` is in lots of 100 shares, ``f6`` in CNY.  ``f124`` (update
    time) is used as ``ts`` when the endpoint provides it as a plausible
    unix timestamp; otherwise the poll time stands in — this field set
    has no reliable per-quote clock (see module docstring).
    """
    last_price = _float(row.get("f2"))
    volume_lots = _float(row.get("f5"))
    amount = _float(row.get("f6"))
    if last_price is None or last_price <= 0 or volume_lots is None or amount is None:
        return None
    ts = poll_ts
    raw_ts = _float(row.get("f124"))
    if raw_ts is not None and raw_ts > 1e9:  # a plausible unix-seconds stamp
        if raw_ts > 1e12:  # milliseconds
            raw_ts /= 1000.0
        ts = datetime.fromtimestamp(raw_ts, tz=_SHANGHAI)
    return RawSnapshot(
        symbol=symbol,
        ts=ts,
        last_price=last_price,
        volume=volume_lots * 100.0,
        amount=amount,
    )


@dataclass(frozen=True)
class Degradation:
    """One source-failover event: ``from_source`` failed, ``to_source`` took over."""

    at: float
    from_source: str
    to_source: str
    reason: str


class FailoverQuoteSource:
    """Route polls primary → fallback and record every degradation.

    The realtime counterpart of the integration design's source routing:
    sources are tried in declared order; a source whose circuit breaker is
    open, or whose poll raises, is skipped in favour of the next one.  A
    :class:`Degradation` record is appended on every skip/switch and
    logged, so failover is observable.  When every source fails the last
    error propagates as :class:`~pulsar_data.errors.FetchError` and the
    dispatcher turns it into MISSING markers — never an exception towards
    the consumer.
    """

    def __init__(self, sources: Sequence[SnapshotSource], *, history_limit: int = 100) -> None:
        if not sources:
            raise ConfigurationError("FailoverQuoteSource needs at least one source")
        self.sources: list[SnapshotSource] = list(sources)
        self.degradations: list[Degradation] = []
        self._history_limit = history_limit
        self.last_source: str | None = None

    def poll(self, symbols: Sequence[str]) -> dict[str, RawSnapshot]:
        last_error: Exception | None = None
        for index, source in enumerate(self.sources):
            breaker = getattr(source, "_breaker", None)
            if breaker is not None and breaker.is_open:
                self._record(index, "circuit breaker open", skipped=True)
                continue
            try:
                result = source.poll(symbols)
            except Exception as exc:  # noqa: BLE001 - routing must survive any source error
                last_error = exc
                self._record(index, f"poll failed: {exc!r}", skipped=False)
                continue
            self.last_source = source.name
            return result
        raise FetchError(
            f"all {len(self.sources)} quote sources failed; last error: {last_error!r}"
        )

    def _record(self, index: int, reason: str, *, skipped: bool) -> None:
        source = self.sources[index]
        fallback = self.sources[index + 1].name if index + 1 < len(self.sources) else "<none>"
        self.degradations.append(
            Degradation(
                at=_time.monotonic(),
                from_source=source.name,
                to_source=fallback,
                reason=("skipped: " if skipped else "") + reason,
            )
        )
        if self._history_limit > 0 and len(self.degradations) > self._history_limit:
            del self.degradations[: len(self.degradations) - self._history_limit]
        logger.warning(
            "realtime source %s %s (%s); next: %s",
            source.name,
            "skipped" if skipped else "failed",
            reason,
            fallback,
        )


def build_default_source(**source_kwargs: Any) -> FailoverQuoteSource:
    """The declared source routing for the realtime channel: Sina → Eastmoney."""
    return FailoverQuoteSource(
        [SinaQuoteSource(**source_kwargs), EastmoneyQuoteSource(**source_kwargs)]
    )
