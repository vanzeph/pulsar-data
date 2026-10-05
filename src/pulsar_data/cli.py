"""Command-line interface: ``pulsar-data backfill|verify|sources``.

The backfill command works in two modes:

* live — fetches through the registered adapter (requires network and
  the akshare extra);
* offline — ``--fixture-dir`` replays recorded raw frames through the
  exact same normalize → quality → lake pipeline, so CI and air-gapped
  hosts can exercise backfill end to end.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from .backfill import BackfillRunner, suspension_days
from .errors import PulsarDataError
from .lake import DataLake
from .sources import get_adapter, list_adapters

logger = logging.getLogger("pulsar_data.cli")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pulsar-data",
        description="Pulsar data integration: backfill, verify, sources.",
    )
    parser.add_argument("--log-level", default="INFO", help="logging level (default INFO)")
    sub = parser.add_subparsers(dest="command", required=True)

    backfill = sub.add_parser("backfill", help="backfill history into the local lake")
    backfill.add_argument("--source", default="akshare", help="registered adapter id")
    backfill.add_argument("--lake", default="./data/lake", help="lake root directory")
    backfill.add_argument("--start", type=_parse_date, required=True)
    backfill.add_argument("--end", type=_parse_date, required=True)
    target = backfill.add_mutually_exclusive_group()
    target.add_argument("--symbols", help="comma-separated canonical symbols (SH600519,...)")
    target.add_argument(
        "--universe-file", help="CSV/text file with one symbol (or code) per line"
    )
    target.add_argument(
        "--all", action="store_true", help="discover the full universe from the source"
    )
    backfill.add_argument("--limit", type=int, help="cap symbol count (sampling)")
    backfill.add_argument(
        "--fixture-dir", help="replay recorded raw frames from this directory (offline)"
    )
    backfill.add_argument("--report", help="write the JSON quality report here")
    backfill.add_argument(
        "--fail-on-gaps", action="store_true", help="exit non-zero on unexplained missing bars"
    )
    backfill.add_argument(
        "--no-corporate-actions", action="store_true", help="skip corporate-action ingestion"
    )
    backfill.add_argument("--no-suspensions", action="store_true", help="skip suspension records")
    backfill.add_argument(
        "--enrich-instruments",
        action="store_true",
        help="per-symbol listing metadata (extra upstream calls)",
    )
    backfill.add_argument(
        "--force", action="store_true", help="re-fetch symbols already synced (ignore watermarks)"
    )
    backfill.add_argument(
        "--min-interval", type=float, default=0.6, help="min seconds between upstream calls"
    )

    verify = sub.add_parser("verify", help="check lake completeness against the calendar")
    verify.add_argument("--lake", default="./data/lake")
    verify.add_argument("--start", type=_parse_date, required=True)
    verify.add_argument("--end", type=_parse_date, required=True)
    verify.add_argument("--symbols", help="comma-separated canonical symbols (default: all)")

    sub.add_parser("sources", help="list registered source adapters")
    return parser


def _symbols_from_cli(args: argparse.Namespace) -> list[str]:
    if getattr(args, "symbols", None):
        return [item.strip() for item in args.symbols.split(",") if item.strip()]
    if getattr(args, "universe_file", None):
        text = Path(args.universe_file).read_text(encoding="utf-8")
        return [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    return []


def cmd_backfill(args: argparse.Namespace) -> int:
    config: dict[str, object] = {"min_interval": args.min_interval}
    if args.fixture_dir:
        config["fixture_dir"] = args.fixture_dir
    adapter = get_adapter(args.source, config)
    lake = DataLake(args.lake)
    runner = BackfillRunner(
        adapter,
        lake,
        include_corporate_actions=not args.no_corporate_actions,
        include_suspensions=not args.no_suspensions,
        enrich_instruments=args.enrich_instruments,
        force=args.force,
    )
    symbols = _symbols_from_cli(args)
    report = runner.run(symbols, args.start, args.end, limit=args.limit)

    if args.report:
        report.to_json(args.report)
        print(f"report written to {args.report}")

    total_cells = sum(sum(counts.values()) for counts in report.completeness.values())
    print(
        f"source={report.source} window=[{report.start}, {report.end}] "
        f"symbols={len(report.symbols)} bars_rows={report.bars_rows} "
        f"ca_rows={report.corporate_action_rows} calendar_rows={report.calendar_rows} "
        f"suspension_rows={report.suspension_rows}"
    )
    print(f"failed={len(report.failed_symbols)} skipped={len(report.skipped_symbols)}")
    if report.failed_symbols:
        for symbol, reason in list(report.failed_symbols.items())[:10]:
            print(f"  FAILED {symbol}: {reason}")
    for symbol, counts in sorted(report.completeness.items()):
        flags = {key: value for key, value in counts.items() if key != "ok" and value}
        suffix = f" {flags}" if flags else ""
        print(f"  {symbol}: ok={counts.get('ok', 0)}{suffix}")
    print(
        f"completeness cells={total_cells} unexplained_gaps={report.unexplained_gaps}"
    )
    if args.fail_on_gaps and report.unexplained_gaps:
        print("FAIL: unexplained missing bars remain", file=sys.stderr)
        return 1
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    lake = DataLake(args.lake)
    symbols = (
        [item.strip() for item in args.symbols.split(",") if item.strip()]
        if args.symbols
        else None
    )
    completeness = lake.completeness(
        start=args.start,
        end=args.end,
        symbols=symbols,
        suspension_days=suspension_days(lake, symbols or []),
    )
    gaps = 0
    for symbol, counts in sorted(completeness.items()):
        gaps += counts.get("gap", 0)
        flags = {key: value for key, value in counts.items() if key != "ok" and value}
        suffix = f" {flags}" if flags else ""
        print(f"  {symbol}: ok={counts.get('ok', 0)}{suffix}")
    print(f"symbols={len(completeness)} unexplained_gaps={gaps}")
    if gaps:
        print("FAIL: unexplained missing bars remain", file=sys.stderr)
        return 1
    return 0


def cmd_sources(_: argparse.Namespace) -> int:
    for source_id in list_adapters():
        print(source_id)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    handlers = {
        "backfill": cmd_backfill,
        "verify": cmd_verify,
        "sources": cmd_sources,
    }
    try:
        return handlers[args.command](args)
    except PulsarDataError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
