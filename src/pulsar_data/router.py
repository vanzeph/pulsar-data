"""Primary/backup source routing with automatic degradation (主备路由与降级).

Design mapping — 适配器框架与归一化 §3 and 安全与可靠性 §限流与退避:

* 「主备路由：配置按序声明 sources，主源失败（限流、超时、断流）
  自动降级备源，并记录降级事件」— :class:`SourceRouter` walks the
  configured source order, fails one call over to the next candidate,
  and records every degradation as a structured :class:`DegradationEvent`.
* 「限流与退避：适配器内置每源限速与指数退避；连续失败熔断并降级备源」
  — rate limiting and per-call retry/backoff live *inside* each live
  adapter client (see :mod:`pulsar_data.ratelimit`, reused by both the
  akshare and baostock clients); the router adds the cross-cutting part:
  consecutive-failure counting per source and a circuit-style trip that
  skips a dead upstream entirely for a cooldown window instead of paying
  the timeout on every call (same semantics as
  :class:`~pulsar_data.ratelimit.CircuitBreaker`, with an injectable
  clock so cooldown behavior is deterministically testable offline).

The router itself implements the :class:`~pulsar_data.sources.base.SourceAdapter`
protocol, so it can be handed to ``run_ingestion`` /
:class:`~pulsar_data.backfill.BackfillRunner` /
:class:`~pulsar_data.incremental.IncrementalRunner` like any single
source.  ``normalize`` is delegated to the exact source that produced
the raw frame (``fetch_raw`` is immediately followed by ``normalize``
in the fixed pipeline, so remembering the serving source is safe).
``source_id`` therefore reports the *serving* source: rows, lake
partitions and watermarks stay attributed to the source that actually
delivered them (per-source watermarks are the design's answer to
multi-source bookkeeping).

Degradation events are appended to
``<lake>/_meta/degradation_events.jsonl`` (one JSON object per line) so
the drill 「主源故障注入演练自动降级并完成当日增量，降级事件留痕」
has a durable, machine-readable trail.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping, Sequence

import pandas as pd

from .errors import FetchError, PulsarDataError
from .schema import Dataset
from .sources.base import FetchRequest, SourceAdapter

__all__ = [
    "DegradationEvent",
    "DegradationLog",
    "SourceRouter",
]


def _is_routable_failure(exc: BaseException) -> bool:
    """Failures that justify failing over to the next source.

    Network-shaped exceptions plus every :class:`PulsarDataError`
    (fetch failures after retries, quality gates, unsupported datasets,
    egress violations) — a dead or unusable primary must never block the
    daily increment.  Unrelated programming errors propagate untouched.
    """
    return isinstance(exc, (PulsarDataError, ConnectionError, TimeoutError, OSError))


@dataclass(frozen=True)
class DegradationEvent:
    """One structured degradation record (降级事件留痕)."""

    occurred_at: str
    from_source: str
    to_source: str | None
    dataset: str
    symbol: str | None
    reason: str
    consecutive_failures: int
    tripped: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class DegradationLog:
    """Append-only JSONL trail of degradation events under ``_meta``."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    @classmethod
    def for_lake(cls, lake_root: str | Path) -> "DegradationLog":
        """Standard location: ``<lake_root>/_meta/degradation_events.jsonl``."""
        return cls(Path(lake_root) / "_meta" / "degradation_events.jsonl")

    def record(self, event: DegradationEvent) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(event.to_dict(), ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def read(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        text = self.path.read_text(encoding="utf-8")
        return [json.loads(line) for line in text.splitlines() if line.strip()]


@dataclass
class _SourceState:
    adapter: SourceAdapter
    consecutive_failures: int = 0
    tripped_at: float | None = None

    @property
    def source_id(self) -> str:
        return self.adapter.source_id


class SourceRouter:
    """Failover façade over an ordered list of source adapters.

    Every call attempts the sources in *configuration* order (the
    「按序声明」 order never drifts at runtime).  A source whose call
    raises a routable failure is skipped for the remainder of *that*
    call (per-call failover) and its consecutive-failure counter grows;
    once the counter reaches ``failure_threshold`` the source is
    *tripped* — excluded from every later call with zero upstream cost
    until ``cooldown`` seconds have passed (half-open: one probe attempt
    afterwards, an immediate re-trip on failure).  A successful call
    resets the counter.  Every failover writes a
    :class:`DegradationEvent`; a silent skip of a tripped source writes
    none (the trip event already marked it).
    """

    def __init__(
        self,
        sources: Sequence[SourceAdapter | tuple[str, SourceAdapter]],
        *,
        failure_threshold: int = 3,
        cooldown: float = 300.0,
        event_log: DegradationLog | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not sources:
            raise FetchError("SourceRouter needs at least one source")
        self._states: list[_SourceState] = []
        seen: set[str] = set()
        for entry in sources:
            adapter = entry[1] if isinstance(entry, tuple) else entry
            source_id = adapter.source_id
            if source_id in seen:
                raise FetchError(f"duplicate source id {source_id!r} in router order")
            seen.add(source_id)
            self._states.append(_SourceState(adapter))
        self.failure_threshold = max(1, failure_threshold)
        self.cooldown = max(0.0, cooldown)
        self.event_log = event_log
        self.clock = clock
        self.degradations: list[DegradationEvent] = []
        self._serving: _SourceState | None = None

    # ------------------------------------------------------------- factory
    @classmethod
    def from_config(
        cls,
        source_configs: Sequence[Mapping[str, object]],
        *,
        failure_threshold: int = 3,
        cooldown: float = 300.0,
        event_log: DegradationLog | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> "SourceRouter":
        """Build a router from ``[{"id": "akshare", ...}, {"id": "baostock"}]``.

        Each entry is resolved through the adapter registry, so the
        source order and per-source tuning are pure configuration — the
        「配置按序声明 sources」 clause.  Extra keys in each entry are
        passed to that adapter's factory as its config.
        """
        from .sources import get_adapter

        adapters: list[SourceAdapter] = []
        for entry in source_configs:
            config = dict(entry)
            if "id" not in config:
                raise FetchError("every router source config needs an 'id'")
            source_id = str(config.pop("id"))
            adapters.append(get_adapter(source_id, config))
        return cls(
            adapters,
            failure_threshold=failure_threshold,
            cooldown=cooldown,
            event_log=event_log,
            clock=clock,
        )

    # ------------------------------------------------------------ accessors
    @property
    def source_ids(self) -> list[str]:
        return [state.source_id for state in self._states]

    @property
    def active_source_id(self) -> str:
        """First source in configuration order that is not tripped right now."""
        for state in self._states:
            if not self._trip_active(state):
                return state.source_id
        return self._states[0].source_id

    @property
    def serving_source_id(self) -> str | None:
        """Source that served the latest successful fetch (None before any)."""
        return self._serving.source_id if self._serving is not None else None

    @property
    def source_id(self) -> str:
        """Adapter-protocol id: the source that served the latest fetch.

        Used by the ingestion pipeline to attribute lake writes and
        watermarks to the *serving* source; before any fetch it is the
        active source.
        """
        return self.serving_source_id or self.active_source_id

    def is_tripped(self, source_id: str) -> bool:
        return self._trip_active(self._state_for(source_id))

    def _state_for(self, source_id: str) -> _SourceState:
        for state in self._states:
            if state.source_id == source_id:
                return state
        raise KeyError(source_id)

    # ---------------------------------------------------------------- trips
    def _trip_active(self, state: _SourceState) -> bool:
        if state.tripped_at is None:
            return False
        if self.clock() - state.tripped_at >= self.cooldown:
            state.tripped_at = None  # half-open: allow one probe attempt
            return False
        return True

    def _candidates(self) -> list[_SourceState]:
        return [state for state in self._states if not self._trip_active(state)]

    def _record_failure(self, state: _SourceState) -> bool:
        """Count one failure; return True when this failure tripped the source."""
        state.consecutive_failures += 1
        if state.consecutive_failures >= self.failure_threshold:
            state.tripped_at = self.clock()
            return True
        return False

    def _record_success(self, state: _SourceState) -> None:
        state.consecutive_failures = 0
        state.tripped_at = None
        self._serving = state

    def _emit(
        self,
        failed: _SourceState,
        to_source: str | None,
        dataset: Dataset,
        request: FetchRequest,
        exc: BaseException,
        tripped: bool,
    ) -> None:
        event = DegradationEvent(
            occurred_at=datetime.now().isoformat(timespec="milliseconds"),
            from_source=failed.source_id,
            to_source=to_source,
            dataset=dataset.value,
            symbol=request.symbol,
            reason=f"{type(exc).__name__}: {exc}"[:500],
            consecutive_failures=failed.consecutive_failures,
            tripped=tripped,
        )
        self.degradations.append(event)
        if self.event_log is not None:
            self.event_log.record(event)

    # ---------------------------------------------------------- adapter API
    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        candidates = self._candidates()
        if not candidates:
            raise FetchError(
                f"no routable source for {dataset.value}"
                f"{'/' + request.symbol if request.symbol else ''}: "
                f"all sources tripped, cooling down"
            )
        failures: list[str] = []
        for position, state in enumerate(candidates):
            try:
                raw = state.adapter.fetch_raw(dataset, request)
            except Exception as exc:
                if not _is_routable_failure(exc):
                    raise
                tripped = self._record_failure(state)
                following = candidates[position + 1] if position + 1 < len(candidates) else None
                self._emit(
                    state,
                    following.source_id if following is not None else None,
                    dataset,
                    request,
                    exc,
                    tripped,
                )
                failures.append(f"{state.source_id}: {type(exc).__name__}: {exc}")
                continue
            self._record_success(state)
            return raw
        raise FetchError(
            f"all sources failed for {dataset.value}"
            f"{'/' + request.symbol if request.symbol else ''}: "
            + "; ".join(failures)
        )

    def normalize(
        self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest
    ) -> pd.DataFrame:
        state = self._serving if self._serving is not None else self._states[0]
        return state.adapter.normalize(dataset, raw, request)

    # ----------------------------------------------------------- lifecycle
    def reset_breakers(self) -> None:
        """Clear failure counters and trips (e.g. at the start of a new day's run)."""
        for state in self._states:
            state.consecutive_failures = 0
            state.tripped_at = None
