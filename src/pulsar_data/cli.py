"""Command-line interface: ``pulsar-data backfill|verify|update|repair|quality|sources|snapshots``.

The backfill command works in two modes:

* live — fetches through the registered adapter (requires network and
  the akshare extra);
* offline — ``--fixture-dir`` replays recorded raw frames through the
  exact same normalize → quality → lake pipeline, so CI and air-gapped
  hosts can exercise backfill end to end.

``update`` is the watermark-driven daily incremental (日终增量) entry:
per-symbol windows resume from each symbol's bars watermark and merge
into existing partitions, so re-running the same ``--end`` is
idempotent. Scheduling (cron or pulsar-app) lives outside this package.

``backfill --freq 5m|15m|30m|60m`` is the minute-granular entry: bars
land in their ``bars_<freq>`` partition family (fetched per
symbol-year, calendar still ingested from the same source), the
universe comes from ``--symbols`` / ``--universe-file`` / ``--all``
(the last falls back to the lake's instruments snapshot for sources
without universe discovery), and the post-run report classifies each
trading day by its full session bar count. Minute sources: baostock.

``repair`` closes the quality loop: it detects calendar-basis gaps
(unexplained missing bars) and executes the resulting backfill tasks —
whole-partition overwrite writes that are idempotent and resumable.
``quality`` prints/writes the partition-level quality-mark summary
(ok / backfilled / suspect).

``snapshots`` is the snapshot-accumulation daemon family (快照流积累):
``collect`` runs the realtime channel into the ``snapshots`` partition
family, ``start``/``stop`` manage it detached, ``status`` reports
watermarks/gaps/disk, and ``archive`` runs the retention pass (raw →
downsampled ``snapshots_1m``).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from pulsar_contracts import Freq

from .backfill import BackfillRunner, suspension_days
from .errors import PulsarDataError
from .lake import DataLake
from .sources import get_adapter, list_adapters

logger = logging.getLogger("pulsar_data.cli")


def _parse_freq(value: str) -> Freq:
    try:
        return Freq(value)
    except ValueError:
        choices = ", ".join(freq.value for freq in Freq)
        raise argparse.ArgumentTypeError(
            f"unknown freq {value!r}; choose one of: {choices}"
        ) from None


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
        "--freq",
        type=_parse_freq,
        default=Freq.DAILY,
        help="bar granularity: 1d (default) or 5m/15m/30m/60m minute families",
    )
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
    verify.add_argument(
        "--freq",
        type=_parse_freq,
        default=Freq.DAILY,
        help="verify the daily bars (1d, default) or a minute family (5m/15m/30m/60m)",
    )

    repair = sub.add_parser(
        "repair",
        help="detect calendar-basis gaps and backfill them (whole-partition, idempotent)",
    )
    repair.add_argument("--source", default="akshare", help="registered adapter id")
    repair.add_argument("--lake", default="./data/lake")
    repair.add_argument("--start", type=_parse_date, required=True)
    repair.add_argument("--end", type=_parse_date, required=True)
    repair.add_argument("--symbols", help="comma-separated canonical symbols (default: all)")
    repair.add_argument("--fixture-dir", help="replay recorded raw frames from this directory (offline)")
    repair.add_argument("--report", help="write the JSON repair report here")
    repair.add_argument("--task-list", help="write the detected backfill task list here (JSON)")
    repair.add_argument(
        "--force", action="store_true", help="re-execute tasks already done in a previous run"
    )
    repair.add_argument("--min-interval", type=float, default=0.6, help="min seconds between upstream calls")

    quality = sub.add_parser(
        "quality", help="partition-level quality report (ok / backfilled / suspect counts and day lists)"
    )
    quality.add_argument("--lake", default="./data/lake")
    quality.add_argument("--symbols", help="comma-separated canonical symbols (default: all)")
    quality.add_argument("--report", help="write the JSON quality report here")

    update = sub.add_parser(
        "update", help="watermark-driven daily incremental update (idempotent, re-runnable)"
    )
    update.add_argument("--source", default="akshare", help="registered adapter id")
    update.add_argument("--lake", default="./data/lake", help="lake root directory")
    update.add_argument("--end", type=_parse_date, required=True, help="sync through this date")
    update.add_argument(
        "--initial-start",
        type=_parse_date,
        help="window start for symbols/datasets with no watermark yet (required on a fresh lake)",
    )
    target = update.add_mutually_exclusive_group()
    target.add_argument("--symbols", help="comma-separated canonical symbols (default: lake universe)")
    target.add_argument("--universe-file", help="file with one symbol per line")
    update.add_argument("--fixture-dir", help="replay recorded raw frames from this directory (offline)")
    update.add_argument("--report", help="write the JSON report here")
    update.add_argument("--no-corporate-actions", action="store_true", help="skip corporate actions")
    update.add_argument("--no-suspensions", action="store_true", help="skip suspension records")
    update.add_argument("--min-interval", type=float, default=0.6, help="min seconds between upstream calls")

    sub.add_parser("sources", help="list registered source adapters")

    from .snapshots.cli import register_parser as _register_snapshots

    _register_snapshots(sub)
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
        freq=args.freq,
    )
    symbols = _symbols_from_cli(args)
    report = runner.run(symbols, args.start, args.end, limit=args.limit)

    if args.report:
        report.to_json(args.report)
        print(f"report written to {args.report}")

    total_cells = sum(sum(counts.values()) for counts in report.completeness.values())
    print(
        f"source={report.source} freq={report.freq} window=[{report.start}, {report.end}] "
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
    from .schema import dataset_for_freq

    lake = DataLake(args.lake)
    symbols = (
        [item.strip() for item in args.symbols.split(",") if item.strip()]
        if args.symbols
        else None
    )
    if args.freq is Freq.DAILY:
        completeness = lake.completeness(
            start=args.start,
            end=args.end,
            symbols=symbols,
            suspension_days=suspension_days(lake, symbols or []),
        )
        missing_bars: dict[str, dict[str, int]] = {}
    else:
        completeness, missing_bars = lake.minute_completeness(
            dataset=dataset_for_freq(args.freq),
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
        detail = ""
        if symbol in missing_bars:
            first = next(iter(missing_bars[symbol].items()))
            detail = f" e.g. {first[0]} missing {first[1]} bars"
        print(f"  {symbol}: ok={counts.get('ok', 0)}{suffix}{detail}")
    print(f"freq={args.freq.value} symbols={len(completeness)} unexplained_gaps={gaps}")
    if gaps:
        print("FAIL: unexplained missing bars remain", file=sys.stderr)
        return 1
    return 0


def cmd_sources(_: argparse.Namespace) -> int:
    for source_id in list_adapters():
        print(source_id)
    return 0


def _cmd_snapshots(args: argparse.Namespace) -> int:
    from .snapshots.cli import run as run_snapshots

    return run_snapshots(args)


def cmd_repair(args: argparse.Namespace) -> int:
    import json

    from .gapfill import BackfillTaskExecutor, detect_backfill_tasks

    config: dict[str, object] = {"min_interval": args.min_interval}
    if args.fixture_dir:
        config["fixture_dir"] = args.fixture_dir
    adapter = get_adapter(args.source, config)
    lake = DataLake(args.lake)
    symbols = (
        [item.strip() for item in args.symbols.split(",") if item.strip()]
        if args.symbols
        else None
    )
    tasks = detect_backfill_tasks(lake, start=args.start, end=args.end, symbols=symbols)
    if args.task_list:
        path = Path(args.task_list)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                [task.to_dict() for task in tasks], ensure_ascii=False, indent=2, sort_keys=True
            ),
            encoding="utf-8",
        )
        print(f"task list written to {args.task_list}")
    print(f"detected {len(tasks)} backfill task(s)")
    for task in tasks[:20]:
        days = ", ".join(day.isoformat() for day in task.missing_days[:5])
        more = "" if len(task.missing_days) <= 5 else f" (+{len(task.missing_days) - 5} more)"
        print(f"  {task.task_id}: {len(task.missing_days)} gap day(s) [{days}{more}]")
    if len(tasks) > 20:
        print(f"  ... (+{len(tasks) - 20} more tasks)")

    if not tasks:
        return 0
    executor = BackfillTaskExecutor(adapter, lake)
    report = executor.execute(tasks, force=args.force)
    if args.report:
        report.to_json(args.report)
        print(f"report written to {args.report}")
    print(
        f"source={report.source} tasks={len(report.results)} done={len(report.done)} "
        f"failed={len(report.failed)} skipped={len(report.skipped)} "
        f"rows_written={report.rows_written}"
    )
    for item in report.failed[:10]:
        print(f"  FAILED {item.task.task_id}: {item.detail}")
    if report.failed:
        print("FAIL: gap repair left unexplained missing bars", file=sys.stderr)
        return 1
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    from .quality_report import build_quality_report

    lake = DataLake(args.lake)
    symbols = (
        [item.strip() for item in args.symbols.split(",") if item.strip()]
        if args.symbols
        else None
    )
    report = build_quality_report(lake, symbols=symbols)
    if args.report:
        report.to_json(args.report)
        print(f"report written to {args.report}")
    totals = report.totals
    print(
        f"partitions={len(report.partitions)} rows={report.rows} "
        f"ok={totals.get('ok', 0)} backfilled={totals.get('backfilled', 0)} "
        f"suspect={totals.get('suspect', 0)}"
    )
    for item in report.partitions:
        marks = {
            mark: count for mark, count in item.counts.items() if mark != "ok" and count
        }
        suffix = f" {marks}" if marks else ""
        print(f"  {item.partition}: rows={item.rows} ok={item.counts.get('ok', 0)}{suffix}")
    return 0


def cmd_update(args: argparse.Namespace) -> int:
    from .incremental import IncrementalRunner

    config: dict[str, object] = {"min_interval": args.min_interval}
    if args.fixture_dir:
        config["fixture_dir"] = args.fixture_dir
    adapter = get_adapter(args.source, config)
    lake = DataLake(args.lake)
    runner = IncrementalRunner(
        adapter,
        lake,
        initial_start=args.initial_start,
        include_corporate_actions=not args.no_corporate_actions,
        include_suspensions=not args.no_suspensions,
    )
    symbols = _symbols_from_cli(args) or None
    report = runner.run(args.end, symbols=symbols)

    if args.report:
        report.to_json(args.report)
        print(f"report written to {args.report}")

    print(
        f"source={report.source} end={report.end} symbols={len(report.symbols)} "
        f"bars_rows={report.bars_rows} ca_rows={report.corporate_action_rows} "
        f"calendar_rows={report.calendar_rows}"
    )
    print(f"failed={len(report.failed_symbols)} skipped={len(report.skipped_symbols)}")
    if report.failed_symbols:
        for symbol, reason in list(report.failed_symbols.items())[:10]:
            print(f"  FAILED {symbol}: {reason}")
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
        "update": cmd_update,
        "repair": cmd_repair,
        "quality": cmd_quality,
        "sources": cmd_sources,
        "snapshots": _cmd_snapshots,
    }
    try:
        return handlers[args.command](args)
    except PulsarDataError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
