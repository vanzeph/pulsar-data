"""Query-time price-adjustment derivation (AdjustMode).

The lake stores raw prices plus one cumulative ``adjust_factor`` per
bar (hfq-style: 后复权价 = raw × factor, matching how the akshare
adapter derives the factor from the source's own adjusted close).
Forward/backward adjusted prices are **derived at query time** and
never stored, so a factor revision rewrites nothing on disk.

Anchoring convention (deterministic per query result):

* ``RAW`` — prices returned exactly as stored.
* ``FORWARD`` (前复权) — anchor is each symbol's *last* bar in the
  frame: ``adjusted = raw × factor / factor_at_last_bar``.  The newest
  bar keeps its raw price; earlier bars are restated downward for
  corporate actions that happened after them.
* ``BACKWARD`` (后复权) — anchor is each symbol's *first* bar in the
  frame: ``adjusted = raw × factor / factor_at_first_bar``.  The
  earliest bar keeps its raw price; later bars are restated upward.

Because the anchor is picked inside the queried window, a query is
fully determined by (symbols, window, lake content) — the same window
always yields the same adjusted prices regardless of how much other
history the lake holds.  Both modes preserve the total-return ratio
between any two days of the same symbol.

``volume`` and ``amount`` are never scaled (share-count changes live
in the factor) and the stored ``adjust_factor`` column is passed
through untouched so consumers can undo or re-anchor the derivation.
"""

from __future__ import annotations

import pandas as pd
from pulsar_contracts import AdjustMode

from .schema import BAR_COLUMNS

__all__ = ["derive_adjusted", "anchor_factors"]

#: Price columns scaled by the factor ratio.
PRICE_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close")


def anchor_factors(frame: pd.DataFrame, mode: AdjustMode) -> pd.Series:
    """Per-row anchor factor: the symbol's first (BACKWARD) / last (FORWARD) factor.

    ``RAW`` anchors at 1.0 (identity).  The returned series is aligned
    to ``frame``'s index.
    """
    if mode is AdjustMode.RAW:
        return pd.Series(1.0, index=frame.index, dtype="float64")
    ordered = frame.sort_values(["symbol", "ts"], kind="stable")
    if mode is AdjustMode.BACKWARD:
        anchor = ordered.groupby("symbol", sort=False)["adjust_factor"].transform("first")
    else:  # AdjustMode.FORWARD
        anchor = ordered.groupby("symbol", sort=False)["adjust_factor"].transform("last")
    return anchor.reindex(frame.index)


def derive_adjusted(frame: pd.DataFrame, mode: AdjustMode) -> pd.DataFrame:
    """Return a copy of ``frame`` with OHLC restated per ``mode``.

    ``frame`` must carry the canonical bar columns; an empty frame is
    returned as-is (canonical column order preserved).
    """
    missing = [column for column in BAR_COLUMNS if column not in frame.columns]
    if missing:
        raise KeyError(f"adjustment derivation requires canonical bar columns; missing {missing}")
    if mode not in (AdjustMode.RAW, AdjustMode.FORWARD, AdjustMode.BACKWARD):
        raise ValueError(f"unsupported AdjustMode: {mode!r}")
    out = frame.copy()
    if out.empty or mode is AdjustMode.RAW:
        return out[list(BAR_COLUMNS)]
    anchor = anchor_factors(out, mode)
    ratio = out["adjust_factor"].astype("float64") / anchor
    for column in PRICE_COLUMNS:
        out[column] = out[column].astype("float64") * ratio
    return out[list(BAR_COLUMNS)]
