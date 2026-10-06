"""The ``MarketDataPort`` read side: deterministic lake reads.

:class:`LakeMarketDataPort` implements the port contract from
``pulsar_contracts`` on top of the local lake:

* ``fetch_bars`` = DuckDB query (:mod:`pulsar_data.query`) + query-time
  adjustment derivation (:mod:`pulsar_data.adjust`) — exactly the
  composition the integration design prescribes — for daily **and**
  minute frequencies (``5m/15m/30m/60m`` read their ``bars_<freq>``
  families, downsampling from a finer stored family when needed);
* ``list_instruments`` / ``fetch_corporate_actions`` / ``calendar`` map
  lake rows onto the immutable contract objects;
* ``subscribe`` (realtime snapshots for Paper/Live) belongs to the D5
  realtime-feed task and deliberately raises ``NotImplementedError``.

Reads never mutate the lake and never return silently-truncated data:
a requested window whose trading days are missing without explanation
(before listing / after coverage / suspension are explained) raises
:class:`~pulsar_data.errors.DataNotAvailable`.  Minute reads apply the
same rule one level finer: every in-coverage trading day must carry
the full session bar count at the requested granularity.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd
from pulsar_contracts import (
    AdjustMode,
    Board,
    CorporateAction,
    Exchange,
    Freq,
    Instrument,
    InstrumentStatus,
    MarketDataPort,
    Snapshot,
    Subscription,
)

from .adjust import derive_adjusted
from .backfill import suspension_days
from .errors import DataNotAvailable
from .lake import DataLake
from .query import LakeQuery
from .schema import BAR_COLUMNS, TRADING_MINUTES_PER_DAY, freq_minutes
from .symbols import to_canonical_symbol

__all__ = ["LakeMarketDataPort"]


def _conforms_to_contract(port: "LakeMarketDataPort") -> MarketDataPort:
    """Structural-conformance anchor: the class satisfies the protocol."""
    return port


class LakeMarketDataPort:
    """Read-side :class:`MarketDataPort` over one local lake directory."""

    def __init__(self, lake: DataLake | str | Path, *, query: LakeQuery | None = None) -> None:
        self.lake = lake if isinstance(lake, DataLake) else DataLake(lake)
        self.query = query if query is not None else LakeQuery(self.lake.root)

    # ------------------------------------------------------------ universe
    def list_instruments(self, as_of: date) -> list[Instrument]:
        """Instruments listed as of ``as_of`` (rows without a list date are skipped)."""
        frame = self.query.instruments()
        out: list[Instrument] = []
        for _, row in frame.iterrows():
            if pd.isna(row.get("list_date")):
                continue
            list_date = pd.Timestamp(row["list_date"]).date()
            if list_date > as_of:
                continue  # not listed yet
            delist = row.get("delist_date")
            delist_date = pd.Timestamp(delist).date() if pd.notna(delist) else None
            if delist_date is not None and delist_date < as_of:
                continue  # already delisted
            shares = row.get("shares_outstanding")
            out.append(
                Instrument(
                    symbol=to_canonical_symbol(row["symbol"]),
                    name=str(row.get("name") or ""),
                    exchange=Exchange(str(row["exchange"])),
                    board=Board(str(row["board"])),
                    is_st=bool(row.get("is_st", False)),
                    status=InstrumentStatus(str(row.get("status") or "listed")),
                    list_date=list_date,
                    delist_date=delist_date,
                    shares_outstanding=(
                        float(shares) if shares is not None and pd.notna(shares) else None
                    ),
                )
            )
        return out

    # ------------------------------------------------------------ bars
    def fetch_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        freq: Freq,
        adjust: AdjustMode,
    ) -> pd.DataFrame:
        """Bars for ``symbols`` in ``[start, end]`` at ``freq``, restated per ``adjust``.

        Returns the canonical bar columns — one row per symbol and
        trading day for ``1d``, per symbol and intraday interval for the
        minute frequencies; inclusive on both ends.  In-range trading
        days missing without explanation (listing window, suspension —
        and for minute freqs, partially filled sessions) raise instead
        of returning partial data.
        """
        if not symbols:
            return pd.DataFrame(columns=list(BAR_COLUMNS))
        if end < start:
            raise ValueError(f"fetch_bars window is inverted: [{start}, {end}]")
        wanted = [to_canonical_symbol(symbol) for symbol in symbols]
        bars = self.query.bars(wanted, start, end, freq=freq)
        if freq is Freq.DAILY:
            self._assert_complete(wanted, start, end, bars)
        else:
            self._assert_minute_complete(wanted, start, end, bars, freq)
        return derive_adjusted(bars, adjust)

    # ------------------------------------------------------------ reference
    def fetch_corporate_actions(self, symbol: str) -> list[CorporateAction]:
        """All corporate actions on record for ``symbol``."""
        canonical = to_canonical_symbol(symbol)
        frame = self.query.corporate_actions([canonical])
        out: list[CorporateAction] = []
        for _, row in frame.iterrows():
            price = row.get("rights_issue_price")
            out.append(
                CorporateAction(
                    symbol=to_canonical_symbol(row["symbol"]),
                    ex_date=pd.Timestamp(row["ex_date"]).date(),
                    cash_dividend_per_share=_number(row["cash_dividend_per_share"]),
                    bonus_share_ratio=_number(row["bonus_share_ratio"]),
                    rights_issue_ratio=_number(row["rights_issue_ratio"]),
                    rights_issue_price=(
                        float(price) if price is not None and pd.notna(price) else None
                    ),
                    description=str(row.get("description") or ""),
                )
            )
        return out

    def calendar(self, start: date, end: date) -> list[date]:
        """Trading days in ``[start, end]``, ascending."""
        frame = self.query.calendar(start, end)
        return [pd.Timestamp(value).date() for value in frame["trade_date"]]

    # ------------------------------------------------------------ realtime
    def subscribe(
        self, symbols: list[str], on_snapshot: Callable[[Snapshot], None]
    ) -> Subscription:
        """Realtime snapshots are D5 scope (realtime feed); not implemented here."""
        raise NotImplementedError(
            "MarketDataPort.subscribe (realtime snapshot streaming for Paper/Live) "
            "is delivered by the D5 realtime-feed task; this port only serves "
            "deterministic lake reads"
        )

    # ------------------------------------------------------------ internals
    def _assert_complete(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        bars: pd.DataFrame,
    ) -> None:
        """Raise when in-range trading days are missing without explanation.

        Explained absences mirror the lake completeness taxonomy: days
        before the symbol's first bar (pre-IPO / coverage start), after
        its last bar (coverage end), and suspended days.  Anything else
        is an unexplained gap and fails the read.
        """
        trading_days = self.calendar(start, end)
        if not trading_days:
            return  # nothing could be missing; caller gets an empty frame
        if bars.empty:
            raise DataNotAvailable(
                f"no bars in lake for {list(symbols)} within [{start}, {end}]"
            )
        bars = bars.copy()
        bars["trade_date"] = pd.to_datetime(bars["ts"], utc=True).dt.tz_convert(
            "Asia/Shanghai"
        ).dt.date
        suspended = suspension_days(self.lake, symbols)
        present: dict[str, set[date]] = {
            symbol: set(group["trade_date"]) for symbol, group in bars.groupby("symbol")
        }
        problems: list[str] = []
        for symbol in symbols:
            days = present.get(symbol)
            if not days:
                problems.append(f"{symbol}: no bars in lake within [{start}, {end}]")
                continue
            first, last = min(days), max(days)
            excused = suspended.get(symbol, set())
            missing = [
                day
                for day in trading_days
                if day not in days and not (day < first or day > last or day in excused)
            ]
            if missing:
                shown = ", ".join(day.isoformat() for day in missing[:5])
                more = "" if len(missing) <= 5 else f" (+{len(missing) - 5} more)"
                problems.append(f"{symbol}: {len(missing)} unexplained missing days [{shown}{more}]")
        if problems:
            raise DataNotAvailable(
                "requested bar range is incomplete in the lake; "
                "refusing to return partial data — " + "; ".join(problems)
            )

    def _assert_minute_complete(
        self,
        symbols: Sequence[str],
        start: date,
        end: date,
        bars: pd.DataFrame,
        freq: Freq,
    ) -> None:
        """Minute counterpart of :meth:`_assert_complete`.

        Every trading day inside a symbol's coverage window must carry
        the full session bar count at ``freq``; suspended days and the
        pre-listing / after-coverage ranges are explained absences, the
        same taxonomy the daily check applies one level coarser.
        """
        trading_days = self.calendar(start, end)
        if not trading_days:
            return
        if bars.empty:
            raise DataNotAvailable(
                f"no minute bars in lake for {list(symbols)} within [{start}, {end}]"
            )
        expected = TRADING_MINUTES_PER_DAY // freq_minutes(freq)
        bars = bars.copy()
        bars["trade_date"] = pd.to_datetime(bars["ts"], utc=True).dt.tz_convert(
            "Asia/Shanghai"
        ).dt.date
        suspended = suspension_days(self.lake, symbols)
        per_day: dict[str, dict[date, int]] = {
            symbol: group.groupby("trade_date").size().to_dict()
            for symbol, group in bars.groupby("symbol")
        }
        problems: list[str] = []
        for symbol in symbols:
            days = per_day.get(symbol)
            if not days:
                problems.append(f"{symbol}: no minute bars in lake within [{start}, {end}]")
                continue
            first, last = min(days), max(days)
            excused = suspended.get(symbol, set())
            short = [
                (day, days.get(day, 0))
                for day in trading_days
                if not (day in excused or day < first or day > last)
                and days.get(day, 0) < expected
            ]
            if short:
                shown = ", ".join(
                    f"{day.isoformat()}({expected - have} missing)" for day, have in short[:3]
                )
                more = "" if len(short) <= 3 else f" (+{len(short) - 3} more days)"
                problems.append(
                    f"{symbol}: {len(short)} incomplete/missing session day(s) at "
                    f"{freq} [{shown}{more}]"
                )
        if problems:
            raise DataNotAvailable(
                "requested minute-bar range is incomplete in the lake; "
                "refusing to return partial data — " + "; ".join(problems)
            )


def _number(value: object) -> float:
    """Coerce a nullable numeric cell to ``float`` (None/NaN -> 0.0)."""
    if value is None or pd.isna(value):
        return 0.0
    return float(value)
