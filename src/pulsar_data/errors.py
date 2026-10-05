"""Exception hierarchy for pulsar-data."""

from __future__ import annotations

__all__ = [
    "PulsarDataError",
    "EgressViolation",
    "FetchError",
    "QualityViolation",
    "LakeError",
    "ConfigurationError",
    "DataNotAvailable",
]


class PulsarDataError(Exception):
    """Base class for every error raised by pulsar-data."""


class EgressViolation(PulsarDataError):
    """An outbound request targeted a forbidden destination.

    Raised before any bytes hit the wire: the URL scheme was not
    http/https, or the target host resolved (or was named) to a
    loopback / private / reserved / link-local / multicast address.
    """


class FetchError(PulsarDataError):
    """An upstream fetch failed after retries (network, upstream 5xx, timeout)."""


class QualityViolation(PulsarDataError):
    """Normalized data failed a hard quality gate and must not enter the lake."""


class LakeError(PulsarDataError):
    """A data-lake structural problem (missing table, unreadable partition, ...)."""


class ConfigurationError(PulsarDataError):
    """Invalid configuration passed to an adapter or the CLI."""


class DataNotAvailable(PulsarDataError):
    """A read-side request cannot be served completely from the lake.

    Raised (never silently truncated) when a requested bar range has
    unexplained missing trading days, or the requested dataset/symbol
    is absent from the lake entirely.
    """
