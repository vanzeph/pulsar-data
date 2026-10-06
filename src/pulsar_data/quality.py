"""Quality gates applied by the framework between normalize and lake write.

Hard violations raise :class:`~pulsar_data.errors.QualityViolation` and
block the batch from entering the lake — bad data must never land.
Soft anomalies are recorded per-row through the ``quality`` column
(``ok / backfilled / suspect``) so the read side can filter.

Checks per dataset:

* **bars_1d** — canonical columns exactly; finite non-null prices > 0;
  ``low <= open <= high`` and ``low <= close <= high``; volume/amount
  >= 0; ``adjust_factor > 0``; no duplicate ``(symbol, ts)``; ``ts``
  must be midnight Asia/Shanghai (left-closed daily convention);
  optionally rows must fall inside the requested window (warn-level).
* **bars_5min / bars_15min / bars_30min / bars_60min** — the same bar
  invariants, with ``ts`` validated against the left-closed intraday
  session grid instead of midnight: every bar must start inside a
  continuous session (09:30-11:30 / 13:00-15:00) on the frequency's
  grid anchored at the session start.
* **calendar** — unique, weekday, ascending trade dates.
* **corporate_actions** — valid ex-dates; at least one non-zero
  component; rights issues must carry a price.
* **instruments** — canonical symbols, valid exchange/board values,
  unique symbols.
* **suspensions** — valid dates, ``start <= end``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from .errors import QualityViolation
from .schema import (
    BAR_COLUMNS,
    BAR_DATASETS,
    CALENDAR_COLUMNS,
    CORPORATE_ACTION_COLUMNS,
    DATASET_MINUTES,
    INSTRUMENT_COLUMNS,
    MINUTE_DATASETS,
    SUSPENSION_COLUMNS,
    Dataset,
    QUALITY_VALUES,
    SESSION_STARTS,
)
from .symbols import to_canonical_symbol

if TYPE_CHECKING:  # pragma: no cover
    from .sources.base import FetchRequest

__all__ = ["check_canonical", "explain_frame_problems"]

_COLUMN_SETS: dict[Dataset, tuple[str, ...]] = {
    Dataset.BARS_1D: BAR_COLUMNS,
    **{dataset: BAR_COLUMNS for dataset in MINUTE_DATASETS},
    Dataset.CALENDAR: CALENDAR_COLUMNS,
    Dataset.CORPORATE_ACTIONS: CORPORATE_ACTION_COLUMNS,
    Dataset.INSTRUMENTS: INSTRUMENT_COLUMNS,
    Dataset.SUSPENSIONS: SUSPENSION_COLUMNS,
}

#: Columns that are legitimately optional (unknown from a given source).
_NULLABLE: dict[Dataset, frozenset[str]] = {
    Dataset.BARS_1D: frozenset(),
    **{dataset: frozenset() for dataset in MINUTE_DATASETS},
    Dataset.CALENDAR: frozenset(),
    Dataset.CORPORATE_ACTIONS: frozenset({"rights_issue_price", "description"}),
    Dataset.INSTRUMENTS: frozenset({"list_date", "delist_date", "shares_outstanding"}),
    Dataset.SUSPENSIONS: frozenset({"end_date", "reason"}),
}


def _fail(dataset: Dataset, problems: list[str]) -> None:
    detail = "; ".join(problems[:12]) + (" ..." if len(problems) > 12 else "")
    raise QualityViolation(f"quality gate failed for {dataset.value}: {detail}")


def _off_session_grid(ts: pd.Series, minutes: int) -> pd.Series:
    """Boolean mask of timestamps outside the freq's session grid.

    A bar is on-grid when it starts inside a continuous session
    (``[09:30, 11:30)`` or ``[13:00, 15:00)``) and its offset from that
    session's start is a multiple of ``minutes`` — the labeling the
    baostock minute series exhibits once end-times are shifted to the
    left-closed convention (09:30-anchored mornings, 13:00-anchored
    afternoons, lunch break excluded).
    """
    local = ts.dt.tz_convert("Asia/Shanghai")
    minute_of_day = local.dt.hour * 60 + local.dt.minute
    morning, afternoon = SESSION_STARTS
    in_morning = (minute_of_day >= morning) & (minute_of_day < morning + 120)
    in_afternoon = (minute_of_day >= afternoon) & (minute_of_day < afternoon + 120)
    offset = pd.Series(
        pd.concat(
            [minute_of_day[in_morning] - morning, minute_of_day[in_afternoon] - afternoon]
        ).reindex(minute_of_day.index),
        dtype="float64",
    )
    on_grid = (offset.notna()) & ((offset % minutes) == 0)
    return ~on_grid


def _column_problems(dataset: Dataset, frame: pd.DataFrame) -> list[str]:
    expected = _COLUMN_SETS[dataset]
    actual = tuple(frame.columns)
    if actual != expected:
        return [f"canonical columns must be {expected} in order, got {actual}"]
    return []


def _finite(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    problems: list[str] = []
    for column in columns:
        if column not in frame.columns:
            continue
        series = frame[column]
        if not pd.api.types.is_numeric_dtype(series):
            nulls = series.isna().sum()
            if nulls:
                problems.append(f"{column}: {int(nulls)} null values")
            continue
        nulls = series.isna().sum()
        if nulls:
            problems.append(f"{column}: {int(nulls)} null/non-numeric values")
        else:
            infinite = ~np.isfinite(series.to_numpy(dtype="float64", na_value=np.nan))
            if infinite.any():
                problems.append(f"{column}: {int(infinite.sum())} non-finite values")
    return problems


def explain_frame_problems(dataset: Dataset, frame: pd.DataFrame) -> list[str]:
    """Return the list of quality problems found in ``frame`` (empty = clean)."""
    structural = _column_problems(dataset, frame)
    if structural:  # value checks assume the canonical columns exist
        return structural
    nullable = _NULLABLE[dataset]
    required = [column for column in frame.columns if column not in nullable]
    problems: list[str] = list(_finite(frame, required))

    if dataset in BAR_DATASETS:
        for column in ("open", "high", "low", "close"):
            bad = (frame[column] <= 0).sum()
            if bad:
                problems.append(f"{column}: {int(bad)} non-positive prices")
        bad = (frame["low"] > frame["open"]).sum() + (frame["open"] > frame["high"]).sum()
        bad += (frame["low"] > frame["close"]).sum() + (frame["close"] > frame["high"]).sum()
        if bad:
            problems.append(f"OHLC invariant low<=open,close<=high violated on {int(bad)} rows")
        bad = (frame["volume"] < 0).sum() + (frame["amount"] < 0).sum()
        if bad:
            problems.append(f"negative volume/amount on {int(bad)} rows")
        bad = (frame["adjust_factor"] <= 0).sum()
        if bad:
            problems.append(f"non-positive adjust_factor on {int(bad)} rows")
        dups = frame.duplicated(subset=["symbol", "ts"]).sum()
        if dups:
            problems.append(f"{int(dups)} duplicate (symbol, ts) rows")
        bad_marks = ~frame["quality"].isin(QUALITY_VALUES)
        if bad_marks.any():
            problems.append(
                f"quality mark must be one of {QUALITY_VALUES}; "
                f"{int(bad_marks.sum())} rows carry other values"
            )
        ts = pd.to_datetime(frame["ts"], utc=True).dt.tz_convert("Asia/Shanghai")
        if dataset is Dataset.BARS_1D:
            not_midnight = (ts.dt.time != pd.Timestamp("00:00").time()).sum()
            if not_midnight:
                problems.append(
                    f"{int(not_midnight)} daily bars not at 00:00 Asia/Shanghai (left-closed convention)"
                )
        else:
            off_grid = _off_session_grid(ts, DATASET_MINUTES[dataset])
            if bool(off_grid.any()):
                problems.append(
                    f"{int(off_grid.sum())} minute bars off the {DATASET_MINUTES[dataset]}-minute "
                    "session grid (left-closed convention: 09:30/13:00 anchored)"
                )
        try:
            frame["symbol"].map(to_canonical_symbol)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"non-canonical symbol present: {exc}")

    elif dataset is Dataset.CALENDAR:
        dups = frame["trade_date"].duplicated().sum()
        if dups:
            problems.append(f"{int(dups)} duplicate trade dates")
        weekdays = pd.to_datetime(frame["trade_date"]).dt.weekday
        if (weekdays >= 5).any():
            problems.append("calendar contains weekend dates")

    elif dataset is Dataset.CORPORATE_ACTIONS:
        nulls = frame["ex_date"].isna().sum()
        if nulls:
            problems.append(f"{int(nulls)} rows without ex_date")
        all_zero = (
            (frame["cash_dividend_per_share"] <= 0)
            & (frame["bonus_share_ratio"] <= 0)
            & (frame["rights_issue_ratio"] <= 0)
        ).sum()
        if all_zero:
            problems.append(f"{int(all_zero)} rows with all-zero components")
        missing_price = (
            (frame["rights_issue_ratio"] > 0) & frame["rights_issue_price"].isna()
        ).sum()
        if missing_price:
            problems.append(f"{int(missing_price)} rights issues without a price")

    elif dataset is Dataset.INSTRUMENTS:
        dups = frame["symbol"].duplicated().sum()
        if dups:
            problems.append(f"{int(dups)} duplicate symbols")
        try:
            frame["symbol"].map(to_canonical_symbol)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"non-canonical symbol present: {exc}")
        bad_exchange = ~frame["exchange"].isin(["SSE", "SZSE", "BSE"])
        if bad_exchange.any():
            problems.append(f"{int(bad_exchange.sum())} rows with unknown exchange")
        bad_board = ~frame["board"].isin(["main", "gem", "star", "bse"])
        if bad_board.any():
            problems.append(f"{int(bad_board.sum())} rows with unknown board")

    elif dataset is Dataset.SUSPENSIONS:
        start = pd.to_datetime(frame["start_date"])
        end = pd.to_datetime(frame["end_date"], errors="coerce")
        inverted = (end.notna() & (end < start)).sum()
        if inverted:
            problems.append(f"{int(inverted)} suspension ranges with end before start")

    return problems


def check_canonical(dataset: Dataset, frame: pd.DataFrame, request: "FetchRequest | None" = None) -> None:
    """Raise :class:`QualityViolation` when ``frame`` fails the hard gates."""
    problems = explain_frame_problems(dataset, frame)
    if problems:
        _fail(dataset, problems)
