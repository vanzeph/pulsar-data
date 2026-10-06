"""Record raw baostock minute-bar responses as offline test fixtures.

Run manually on a machine with network access (requires
``pip install baostock``); nothing here runs in CI:

    python scripts/record_minute_fixtures.py --out tests/fixtures/baostock

The script dumps the *raw* frames exactly as the live client returns
them (``query_history_k_data_plus`` at minute frequencies plus the
full-history ``query_adjust_factor`` walk) for:

* the SSE/SZSE trading calendar over the whole recorded span,
* 5-minute bars for the fixed 20-symbol sample domain: a contiguous
  recent window (full sessions, the primary zero-gap fixture) and the
  first trading week of each sampled past year (the cross-year
  fixtures; years before the service's minute-coverage start simply
  record no rows and land in the manifest),
* 15/30/60-minute bars for one symbol over the recent window (the
  other minute families),
* per-symbol cumulative adjustment factors (full history, so the
  as-of join in the adapter reproduces exactly).

It also measures, per recorded symbol-year, the real Parquet footprint
after a full normalize → lake write — the input of the full-market
disk-size estimate in ``README.md``.  Everything the offline
test-suite and the ``--fixture-dir`` CLI mode read is produced here;
see ``tests/fixtures/baostock/manifest.json`` for provenance.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pulsar_data.lake import DataLake
from pulsar_data.schema import Dataset
from pulsar_data.sources.baostock import BaostockSourceAdapter
from pulsar_data.sources.baostock.client import LiveBaostockClient, to_baostock_code
from pulsar_data.sources.base import FetchRequest

#: Same diverse 20-symbol domain as the akshare fixture recording.
SAMPLE_SYMBOLS = [
    "SH600519", "SZ000001", "SH601318", "SH600036", "SZ000858",
    "SH600276", "SZ300750", "SZ300059", "SH688981", "SH688111",
    "SZ002415", "SZ002594", "SH601899", "SZ000651", "SH603288",
    "SZ300760", "SH688012", "SH601127", "SZ003816", "SZ301536",
]

#: Cross-year windows recorded per symbol (first trading week of June).
SAMPLE_YEARS = (2016, 2018, 2020, 2022, 2024, 2026)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("tests/fixtures/baostock"))
    parser.add_argument(
        "--recent-start", type=date.fromisoformat, default=date.fromisoformat("2026-09-14"),
        help="contiguous recent window start (full sessions; the primary zero-gap fixture)",
    )
    parser.add_argument(
        "--recent-end", type=date.fromisoformat, default=date.fromisoformat("2026-09-30"),
    )
    parser.add_argument("--pause", type=float, default=0.4, help="seconds between upstream calls")
    args = parser.parse_args()

    out = args.out
    (out / "minute").mkdir(parents=True, exist_ok=True)
    (out / "adjust_factors").mkdir(parents=True)

    client = LiveBaostockClient(min_interval=args.pause, retries=3)
    windows: list[tuple[date, date]] = [
        (args.recent_start, args.recent_end),
        *[(date(year, 6, 1), date(year, 6, 7)) for year in SAMPLE_YEARS],
    ]
    calendar_start = min(start for start, _ in windows)
    calendar_end = max(end for _, end in windows)

    manifest: dict[str, object] = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "source": "baostock query_history_k_data_plus (raw), query_adjust_factor, query_trade_dates",
        "symbols": SAMPLE_SYMBOLS,
        "recent_window": [args.recent_start.isoformat(), args.recent_end.isoformat()],
        "sample_years": list(SAMPLE_YEARS),
        "windows": {},       # symbol -> {label: [start, end]}
        "coverage": {},      # symbol -> earliest/latest dated 5m row recorded
        "rows": {},          # symbol -> {freq: rows}
        "disk_measure": {},  # symbol -> {year: {rows, parquet_bytes}}
    }

    calendar = client.trade_dates(calendar_start, calendar_end)
    calendar.to_csv(out / "calendar.csv", index=False)
    time.sleep(args.pause)

    measure_lake = DataLake(out / ".disk-measure-lake")

    for index, symbol in enumerate(SAMPLE_SYMBOLS, start=1):
        code = to_baostock_code(symbol)
        factors = client.adjust_factor_events(code, date(1990, 1, 1), calendar_end)
        factors.to_csv(out / "adjust_factors" / f"{symbol}.csv", index=False)
        time.sleep(args.pause)

        combined: dict[int, pd.DataFrame] = {}
        coverage_first: str | None = None
        coverage_last: str | None = None
        for start, end in windows:
            label = "recent" if start == args.recent_start else f"y{start.year}"
            frame = client.minute_bars(code, start, end, "5")
            if not frame.empty:
                combined.setdefault(5, []).append(frame)
                first, last = frame["date"].iloc[0], frame["date"].iloc[-1]
                coverage_first = first if coverage_first is None else min(coverage_first, first)
                coverage_last = last if coverage_last is None else max(coverage_last, last)
            manifest["windows"].setdefault(symbol, {})[label] = [
                start.isoformat(), end.isoformat(), int(len(frame)),
            ]
            time.sleep(args.pause)
        five = (
            pd.concat(combined[5], ignore_index=True).drop_duplicates(subset=["date", "time"])
            if combined.get(5)
            else pd.DataFrame()
        )
        if not five.empty:
            five = five.sort_values(["date", "time"]).reset_index(drop=True)
        five.to_csv(out / "minute" / f"{symbol}.5.csv", index=False)
        manifest["coverage"][symbol] = [coverage_first, coverage_last]
        manifest["rows"][symbol] = {"5": int(len(five))}

        # disk measurement: replay the freshly written fixture through the
        # real normalize -> quality -> lake pipeline (fully offline) and
        # measure the resulting Parquet partition bytes
        if not five.empty:
            from pulsar_data.sources.baostock.client import FixtureBaostockClient
            from pulsar_data.sources.base import run_ingestion

            replay = BaostockSourceAdapter(client=FixtureBaostockClient(out))
            request = FetchRequest(
                Dataset.BARS_5MIN,
                date.fromisoformat(five["date"].iloc[0]),
                date.fromisoformat(five["date"].iloc[-1]),
                symbol=symbol,
            )
            run_ingestion(replay, request, measure_lake)
            for year in sorted({int(d[:4]) for d in five["date"]}):
                rows = int((five["date"].str[:4] == str(year)).sum())
                partition = (
                    measure_lake.root / Dataset.BARS_5MIN.value
                    / f"symbol={symbol}" / f"year={year}" / "part.parquet"
                )
                manifest["disk_measure"].setdefault(symbol, {})[str(year)] = {
                    "rows": rows,
                    "parquet_bytes": partition.stat().st_size if partition.exists() else 0,
                }

        # the other minute families for one symbol only
        if symbol == "SH600519":
            for frequency in ("15", "30", "60"):
                frames = []
                for start, end in windows[:1]:  # recent window only
                    frame = client.minute_bars(code, start, end, frequency)
                    frames.append(frame)
                    time.sleep(args.pause)
                frame = (
                    pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date", "time"])
                    .sort_values(["date", "time"])
                    .reset_index(drop=True)
                )
                frame.to_csv(out / "minute" / f"{symbol}.{frequency}.csv", index=False)
                manifest["rows"][symbol][frequency] = int(len(frame))

        print(f"[{index}/{len(SAMPLE_SYMBOLS)}] {symbol}: rows={manifest['rows'][symbol]}", flush=True)

    client.logout()
    import shutil

    shutil.rmtree(out / ".disk-measure-lake", ignore_errors=True)
    (out / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(f"fixtures written to {out}")


if __name__ == "__main__":
    main()
