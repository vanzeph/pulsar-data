"""Record raw akshare API responses as offline test fixtures.

Run manually on a machine with network access to the upstream data
endpoints (Sina / Eastmoney); requires ``pip install -e .[akshare]``:

    python scripts/record_fixtures.py --out tests/fixtures/akshare \
        --start 2024-01-01 --end 2024-12-31

The script dumps the *raw* frames exactly as the live client returns
them (Eastmoney first, Sina fallback, exactly like production) for:

* the SSE/SZSE trading calendar,
* the A-share universe snapshot,
* daily bars (raw + hfq + qfq, all from one endpoint per symbol so the
  adjustment-factor anchor stays consistent),
* dividend and rights-issue detail for the sample,
* suspension records covering the window,
* per-symbol listing metadata for the sample.

Everything the offline test-suite and the ``--fixture-dir`` CLI mode
read is produced here, so CI never touches the network.  See
``tests/fixtures/akshare/manifest.json`` for provenance of a given
recording.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pulsar_data.errors import FetchError
from pulsar_data.sources.akshare.client import LiveAkShareClient

# Fixed, deliberately diverse sample: main board SH/SZ, GEM, STAR,
# different sectors, one 2024 IPO, dividend payers.
SAMPLE_SYMBOLS = [
    "SH600519",  # Kweichow Moutai (cash dividends 2024-06 / 2024-12)
    "SZ000001",  # Ping An Bank (main board SZ)
    "SH601318",  # China Ping An
    "SH600036",  # China Merchants Bank
    "SZ000858",  # Wuliangye
    "SH600276",  # Hengrui Medicine
    "SZ300750",  # CATL (GEM)
    "SZ300059",  # East Money (GEM)
    "SH688981",  # SMIC (STAR)
    "SH688111",  # Kingsoft Office (STAR)
    "SZ002415",  # Hikvision (SZ main)
    "SZ002594",  # BYD
    "SH601899",  # Zijin Mining
    "SZ000651",  # Gree Electric
    "SH603288",  # Haitian Flavouring
    "SZ300760",  # Mindray Medical (GEM)
    "SH688012",  # AMEC (STAR)
    "SH601127",  # Seres
    "SZ003816",  # CGN Power
    "SZ301536",  # 2024 IPO (GEM, listed 2024-03) — exercises pre-IPO gaps
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("tests/fixtures/akshare"))
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--pause", type=float, default=0.5, help="seconds between symbols")
    args = parser.parse_args()

    out = args.out
    (out / "bars").mkdir(parents=True, exist_ok=True)
    (out / "dividends").mkdir(parents=True, exist_ok=True)
    (out / "rights").mkdir(parents=True, exist_ok=True)
    (out / "instrument_info").mkdir(parents=True, exist_ok=True)

    client = LiveAkShareClient(min_interval=0.3, retries=2)

    def exists(path: Path) -> bool:
        return path.exists() and path.stat().st_size > 0

    recorded: dict[str, object] = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "window": [args.start.isoformat(), args.end.isoformat()],
        "symbols": SAMPLE_SYMBOLS,
    }

    # --- calendar -------------------------------------------------------
    if not exists(out / "calendar.csv"):
        client.trade_dates().to_csv(out / "calendar.csv", index=False)
    print(f"calendar: {len(pd.read_csv(out / 'calendar.csv'))} trade dates")

    # --- universe ---------------------------------------------------------
    if not exists(out / "universe.csv"):
        client.universe().to_csv(out / "universe.csv", index=False)
    universe = pd.read_csv(out / "universe.csv", dtype=str)
    recorded["universe_source"] = "eastmoney spot with sina fallback (same code path as production)"
    print(f"universe: {len(universe)} rows")

    # --- suspensions --------------------------------------------------
    if not exists(out / "suspensions.csv"):
        client.suspensions(args.start).to_csv(out / "suspensions.csv", index=False)
    susp = pd.read_csv(out / "suspensions.csv")
    print(f"suspensions on/after {args.start}: {len(susp)} rows")

    # --- bars / corporate actions / listing info --------------------------
    failures: list[str] = []
    for symbol in SAMPLE_SYMBOLS:
        code = symbol[2:]
        try:
            if not exists(out / f"bars/{symbol}.raw.csv") or not exists(
                out / f"bars/{symbol}.hfq.csv"
            ):
                raw, hfq = client.daily_bars_pair(code, args.start, args.end)
                raw.to_csv(out / f"bars/{symbol}.raw.csv", index=False)
                hfq.to_csv(out / f"bars/{symbol}.hfq.csv", index=False)
            if not exists(out / f"bars/{symbol}.qfq.csv"):
                client.daily_bars_qfq(code, args.start, args.end).to_csv(
                    out / f"bars/{symbol}.qfq.csv", index=False
                )
            if not exists(out / f"dividends/{symbol}.csv"):
                client.dividend_detail(code).to_csv(out / f"dividends/{symbol}.csv", index=False)
            if not exists(out / f"rights/{symbol}.csv"):
                client.rights_detail(code).to_csv(out / f"rights/{symbol}.csv", index=False)
            if not exists(out / f"instrument_info/{symbol}.csv"):
                try:
                    client.instrument_info(code).to_csv(
                        out / f"instrument_info/{symbol}.csv", index=False
                    )
                except FetchError:
                    # eastmoney listing-info endpoint unavailable: derive the
                    # 上市时间 row from the full Sina trading history (the
                    # first trading day IS the listing day) and record that.
                    ak = client._module()  # noqa: SLF001 - recording helper
                    full = ak.stock_zh_a_daily(symbol=client._sina_symbol(code))  # noqa: SLF001
                    listed = str(pd.to_datetime(full["date"]).min().date()).replace("-", "")
                    pd.DataFrame(
                        {"item": ["上市时间"], "value": [listed]}
                    ).to_csv(out / f"instrument_info/{symbol}.csv", index=False)
            print(f"recorded {symbol}")
        except FetchError as exc:
            print(f"FAILED {symbol}: {exc}")
            failures.append(symbol)
        time.sleep(args.pause)

    recorded["failed_symbols"] = failures
    (out / "manifest.json").write_text(
        json.dumps(recorded, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("manifest written")
    if failures:
        raise SystemExit(f"{len(failures)} symbols failed: {failures}")


if __name__ == "__main__":
    main()
