"""Canonical lake schemas shared by every source adapter.

Each dataset has exactly one canonical column set; ``normalize`` output
must match it exactly (names, order, dtypes are enforced by
:mod:`pulsar_data.quality` before anything is written to the lake).

Conventions (from the Pulsar architecture baseline):

* timestamps are timezone-aware ``Asia/Shanghai``; a daily bar carries
  ``00:00`` of its trading day (left-closed interval start); a minute
  bar carries the left-closed start of its intraday interval
  (``09:30`` opens the first 5-minute bar of the day);
* the lake stores raw prices plus a cumulative ``adjust_factor``;
  forward/backward adjustment is derived at query time;
* ``volume`` is in shares, ``amount`` in CNY.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from typing import Final

import pandas as pd
from pulsar_contracts import SHANGHAI_TZ, Freq

from .errors import ConfigurationError

__all__ = [
    "Dataset",
    "BAR_COLUMNS",
    "CALENDAR_COLUMNS",
    "CORPORATE_ACTION_COLUMNS",
    "INSTRUMENT_COLUMNS",
    "SUSPENSION_COLUMNS",
    "WATERMARK_COLUMNS",
    "QUALITY_VALUES",
    "BAR_DATASETS",
    "MINUTE_DATASETS",
    "dataset_for_freq",
    "freq_minutes",
    "bars_per_trading_day",
    "DATASET_MINUTES",
    "SESSION_STARTS",
    "TRADING_MINUTES_PER_DAY",
    "daily_ts",
    "minute_ts",
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

#: Allowed values of the per-row ``quality`` mark column (可信度落地).
#: ``ok`` — landed through the normal pipeline with no anomaly attached;
#: ``backfilled`` — (re)written by a historical backfill or gap repair;
#: ``suspect`` — flagged by a cross-source mismatch or other quality event.
QUALITY_VALUES: Final[tuple[str, ...]] = ("ok", "backfilled", "suspect")


class Dataset(str, enum.Enum):
    """Lake datasets an adapter can serve through fetch_raw/normalize."""

    BARS_1D = "bars_1d"
    BARS_5MIN = "bars_5min"
    BARS_15MIN = "bars_15min"
    BARS_30MIN = "bars_30min"
    BARS_60MIN = "bars_60min"
    CALENDAR = "calendar"
    CORPORATE_ACTIONS = "corporate_actions"
    INSTRUMENTS = "instruments"
    SUSPENSIONS = "suspensions"


#: Every dataset carrying the canonical bar column set.
BAR_DATASETS: Final[frozenset[Dataset]] = frozenset(
    {
        Dataset.BARS_1D,
        Dataset.BARS_5MIN,
        Dataset.BARS_15MIN,
        Dataset.BARS_30MIN,
        Dataset.BARS_60MIN,
    }
)

#: The minute-granular bar datasets, thinnest first (downsampling source order).
MINUTE_DATASETS: Final[tuple[Dataset, ...]] = (
    Dataset.BARS_5MIN,
    Dataset.BARS_15MIN,
    Dataset.BARS_30MIN,
    Dataset.BARS_60MIN,
)

#: Contract frequency -> lake dataset (one partition family per granularity).
_FREQ_TO_DATASET: Final[dict[Freq, Dataset]] = {
    Freq.MINUTE_5: Dataset.BARS_5MIN,
    Freq.MINUTE_15: Dataset.BARS_15MIN,
    Freq.MINUTE_30: Dataset.BARS_30MIN,
    Freq.MINUTE_60: Dataset.BARS_60MIN,
    Freq.DAILY: Dataset.BARS_1D,
}

#: Minute datasets -> bar duration in minutes (public: the quality gates and
#: the query layer's downsampler both need the grid).
DATASET_MINUTES: Final[dict[Dataset, int]] = {
    Dataset.BARS_5MIN: 5,
    Dataset.BARS_15MIN: 15,
    Dataset.BARS_30MIN: 30,
    Dataset.BARS_60MIN: 60,
}


def dataset_for_freq(freq: Freq) -> Dataset:
    """Lake dataset serving ``freq`` (one ``bars_<freq>`` family per granularity).

    ``1m`` has no source today (baostock serves 5/15/30/60 only), so asking
    for it is a configuration error, not a missing-data condition.
    """
    try:
        return _FREQ_TO_DATASET[freq]
    except KeyError:
        raise ConfigurationError(
            f"no lake dataset serves freq {freq!r}; minute sources provide "
            "5m/15m/30m/60m (1m has no source yet)"
        ) from None


def freq_minutes(freq: Freq) -> int:
    """Bar duration of ``freq`` in minutes (minute freqs only)."""
    dataset = dataset_for_freq(freq)
    return DATASET_MINUTES[dataset]


#: A-share continuous sessions, minute-of-day, left-closed (09:30 and 13:00).
SESSION_STARTS: Final[tuple[int, ...]] = (9 * 60 + 30, 13 * 60)
#: Total continuous-trading minutes per day (240: 09:30-11:30 + 13:00-15:00).
TRADING_MINUTES_PER_DAY: Final[int] = 240


def bars_per_trading_day(freq: Freq) -> int:
    """Bars a complete session carries at ``freq`` (48 @ 5m, 16 @ 15m, ...)."""
    return TRADING_MINUTES_PER_DAY // freq_minutes(freq)


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


def minute_ts(value: str | datetime | pd.Timestamp, *, minutes: int) -> pd.Timestamp:
    """Normalize one minute-bar timestamp to the lake's left-closed convention.

    baostock labels minute bars by their interval **end** (``time`` column,
    e.g. ``20260928093500000`` for the 09:30-09:35 bar), while the lake
    stores interval starts like everywhere else; ``minutes`` is the bar
    duration, so the returned timestamp is ``value - minutes``.  Naive
    input is interpreted as Asia/Shanghai wall time.
    """
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert(SHANGHAI_TZ)
    else:
        stamp = stamp.tz_localize(SHANGHAI_TZ)
    return stamp - pd.Timedelta(minutes=minutes)


def ts_to_date(ts: pd.Timestamp) -> date:
    """Convert a (possibly tz-aware) bar timestamp to its trading-day date."""
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(SHANGHAI_TZ)
    return stamp.tz_convert(SHANGHAI_TZ).date()
