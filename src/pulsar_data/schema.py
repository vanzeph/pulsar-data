"""Canonical lake schemas shared by every source adapter.

Each dataset has exactly one canonical column set; ``normalize`` output
must match it exactly (names, order, dtypes are enforced by
:mod:`pulsar_data.quality` before anything is written to the lake).

Conventions (from the Pulsar architecture baseline):

* timestamps are timezone-aware ``Asia/Shanghai``; a daily bar carries
  ``00:00`` of its trading day (left-closed interval start);
* the lake stores raw prices plus a cumulative ``adjust_factor``;
  forward/backward adjustment is derived at query time;
* ``volume`` is in shares, ``amount`` in CNY.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from typing import Final

import pandas as pd
from pulsar_contracts import SHANGHAI_TZ

__all__ = [
    "Dataset",
    "BAR_COLUMNS",
    "CALENDAR_COLUMNS",
    "CORPORATE_ACTION_COLUMNS",
    "INSTRUMENT_COLUMNS",
    "SUSPENSION_COLUMNS",
    "WATERMARK_COLUMNS",
    "daily_ts",
    "ts_to_date",
]

#: Canonical column lists per dataset, in write order.
BAR_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjust_factor",
    "quality",
)
CALENDAR_COLUMNS: Final[tuple[str, ...]] = ("trade_date",)
CORPORATE_ACTION_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "ex_date",
    "cash_dividend_per_share",
    "bonus_share_ratio",
    "rights_issue_ratio",
    "rights_issue_price",
    "description",
)
INSTRUMENT_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "name",
    "exchange",
    "board",
    "is_st",
    "status",
    "list_date",
    "delist_date",
    "shares_outstanding",
)
SUSPENSION_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "start_date",
    "end_date",
    "reason",
)
WATERMARK_COLUMNS: Final[tuple[str, ...]] = (
    "source",
    "dataset",
    "partition",
    "rows",
    "synced_through",
    "updated_at",
)


class Dataset(str, enum.Enum):
    """Lake datasets an adapter can serve through fetch_raw/normalize."""

    BARS_1D = "bars_1d"
    CALENDAR = "calendar"
    CORPORATE_ACTIONS = "corporate_actions"
    INSTRUMENTS = "instruments"
    SUSPENSIONS = "suspensions"


def daily_ts(value: date | datetime | str | pd.Timestamp) -> pd.Timestamp:
    """Return ``value`` as a tz-aware Asia/Shanghai timestamp at midnight.

    This is the canonical ``ts`` of a daily bar: the left-closed start
    of the trading day.
    """
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(SHANGHAI_TZ)
    else:
        ts = ts.tz_localize(SHANGHAI_TZ)
    return ts.normalize()


def ts_to_date(ts: pd.Timestamp) -> date:
    """Convert a (possibly tz-aware) bar timestamp to its trading-day date."""
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(SHANGHAI_TZ)
    return stamp.tz_convert(SHANGHAI_TZ).date()
