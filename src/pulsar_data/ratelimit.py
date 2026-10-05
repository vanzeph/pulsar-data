"""Per-source rate limiting, retry with exponential backoff, and circuit breaking.

Every live adapter funnels its upstream calls through :class:`RateLimiter`
(min interval between requests) and :func:`with_retry` (exponential
backoff on transient failures).  A :class:`CircuitBreaker` trips after
N consecutive failures and stays open for a cooldown window, so a dead
upstream cannot stall a whole backfill run.
"""

from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

from .errors import FetchError

__all__ = ["RateLimiter", "with_retry", "CircuitBreaker"]

T = TypeVar("T")

#: Exception types considered transient (worth retrying).
_TRANSIENT = (ConnectionError, TimeoutError, OSError)


class RateLimiter:
    """Enforce a minimum interval between upstream requests."""

    def __init__(self, min_interval: float = 0.5) -> None:
        self.min_interval = max(0.0, min_interval)
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        due = self._last + self.min_interval
        if now < due:
            time.sleep(due - now)
        self._last = time.monotonic()


def with_retry(
    fn: Callable[[], T],
    *,
    retries: int = 4,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retry_on: tuple[type[BaseException], ...] = _TRANSIENT,
) -> T:
    """Call ``fn`` retrying transient failures with exponential backoff + jitter."""
    last: BaseException | None = None
    for attempt in range(max(1, retries)):
        try:
            return fn()
        except retry_on as exc:  # noqa: PERF203
            last = exc
            if attempt == retries - 1:
                break
            delay = min(max_delay, base_delay * (2**attempt)) + random.random()
            time.sleep(delay)
    raise FetchError(f"upstream call failed after {retries} attempts: {last!r}") from last


class CircuitBreaker:
    """Trip open after ``threshold`` consecutive failures; auto-close after cooldown."""

    def __init__(self, threshold: int = 5, cooldown: float = 60.0) -> None:
        self.threshold = threshold
        self.cooldown = cooldown
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at >= self.cooldown:
            self._opened_at = None
            self._failures = 0
            return False
        return True

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.threshold:
            self._opened_at = time.monotonic()
