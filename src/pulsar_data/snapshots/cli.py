"""CLI for the snapshot collect daemon: ``pulsar-data snapshots <cmd>``.

* ``collect`` — run the accumulation daemon in the foreground (graceful
  SIGTERM/SIGINT shutdown with a final flush); the piece `pulsar-app`,
  systemd or a supervisor would run;
* ``start`` / ``stop`` — daemon lifecycle around ``collect`` (detached
  session, pidfile under ``<lake>/_meta``, log to
  ``<lake>/_meta/collect.log``);
* ``status`` — running check, per-symbol persisted watermark tail, gap
  summary (session gaps + recent missing markers), and the disk
  watermark report against the policy thresholds;
* ``archive`` — one retention pass: raw partitions past
  ``raw_retention_days`` are downsampled into ``snapshots_1m`` and
  reclaimed (idempotent; re-runnable from cron).

The accumulation policy comes from ``--config`` (JSON) refined by the
inline flags; whatever the daemon runs with is persisted under
``<lake>/_meta/snapshot_policy.json`` so later ``status``/``archive``
invocations need no arguments.  The full-market universe always requires
the explicit ``--accept-full-market`` (or
``acknowledge_full_market: true``) — the ~1 GB/day budget is never a
silent default.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

from ..errors import PulsarDataError
from ..lake import DataLake
from .archive import archive_expired
from .daemon import SnapshotCollectorDaemon, collect_status
from .policy import DEFAULT_POLICY_FILENAME, SnapshotPolicy
from .store import SnapshotStore

logger = logging.getLogger("pulsar_data.snapshots.cli")

__all__ = ["register_parser", "run"]

_PID_FILENAME = "collect.pid"
_LOG_FILENAME = "collect.log"


# ---------------------------------------------------------------- policy IO
def _persisted_policy_path(lake: DataLake) -> Path:
    return lake.root / "_meta" / DEFAULT_POLICY_FILENAME


def persist_policy(lake: DataLake, policy: SnapshotPolicy) -> None:
    """Record the running policy so status/archive reproduce it verbatim."""
    path = _persisted_policy_path(lake)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(policy.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def _load_policy(lake: DataLake, args: argparse.Namespace) -> SnapshotPolicy:
    """Config file → previously persisted policy → bare defaults, then flags."""
    if getattr(args, "config", None):
        policy = SnapshotPolicy.from_json_file(args.config)
    else:
        persisted = _persisted_policy_path(lake)
        policy = None
        if persisted.exists():
            try:
                policy = SnapshotPolicy.from_mapping(
                    json.loads(persisted.read_text(encoding="utf-8"))
                )
            except ValueError as exc:
                raise PulsarDataError(
                    f"persisted snapshot policy {persisted} is unreadable: {exc}; "
                    "pass --config to override"
                ) from exc
        if policy is None:
            policy = SnapshotPolicy(symbols=())
    policy = _apply_overrides(policy, args)
    policy.validate()
    return policy


def _apply_overrides(policy: SnapshotPolicy, args: argparse.Namespace) -> SnapshotPolicy:
    """Layer the CLI flags over the base policy (flags win)."""
    values = policy.to_dict()
    symbols = getattr(args, "symbols", None)
    if symbols:
        values["symbols"] = symbols
    universe_file = getattr(args, "universe_file", None)
    if universe_file:
        text = Path(universe_file).read_text(encoding="utf-8")
        values["symbols"] = [
            line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")
        ]
    if getattr(args, "all", False):
        values["full_market"] = True
    if getattr(args, "accept_full_market", False):
        values["acknowledge_full_market"] = True
    for flag, key in (
        ("poll_interval", "poll_interval_s"),
        ("min_sample_interval", "min_sample_interval_s"),
    ):
        value = getattr(args, flag, None)
        if value is not None:
            values[key] = value
    return SnapshotPolicy.from_mapping(values)


# ------------------------------------------------------------------ parser
def register_parser(sub: argparse._SubParsersAction) -> None:  # noqa: SLF001
    """Wire the ``snapshots`` command group onto the main parser."""
    group = sub.add_parser(
        "snapshots",
        help="snapshot-stream accumulation: collect daemon, status, retention archive",
    )
    nested = group.add_subparsers(dest="snapshots_command", required=True)

    collect = nested.add_parser("collect", help="run the collect daemon in the foreground")
    _add_policy_args(collect, with_runtime=True)

    start = nested.add_parser("start", help="start the collect daemon detached (pidfile + log)")
    _add_policy_args(start, with_runtime=True)

    stop = nested.add_parser("stop", help="stop the daemon started via `snapshots start`")
    stop.add_argument("--lake", default="./data/lake")
    stop.add_argument("--timeout", type=float, default=30.0, help="seconds to wait for a clean stop")

    status = nested.add_parser("status", help="watermarks, gaps, and the disk report")
    _add_policy_args(status)
    status.add_argument("--days", type=int, default=7, help="recent days for missing-marker counts")
    status.add_argument("--report", help="write the JSON status here")

    archive = nested.add_parser("archive", help="retention pass: downsample + reclaim raw partitions")
    _add_policy_args(archive)
    archive.add_argument("--today", type=date.fromisoformat, help="override today (tests/dry ops)")
    archive.add_argument("--report", help="write the JSON archive report here")


def _add_policy_args(parser: argparse.ArgumentParser, *, with_runtime: bool = False) -> None:
    parser.add_argument("--lake", default="./data/lake", help="lake root directory")
    parser.add_argument("--config", help="snapshot policy JSON file")
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--symbols", help="comma-separated canonical symbols (watchlist)")
    target.add_argument("--universe-file", help="file with one symbol per line")
    target.add_argument("--all", action="store_true", help="full market (lake instruments)")
    parser.add_argument(
        "--accept-full-market",
        action="store_true",
        help="explicitly acknowledge the ~1GB/day full-market disk budget (required with --all)",
    )
    parser.add_argument("--poll-interval", type=float, help="seconds between polls (policy override)")
    parser.add_argument(
        "--min-sample-interval",
        type=float,
        help="sampling cap in seconds: at most one quote per symbol per interval (policy override)",
    )
    if with_runtime:
        parser.add_argument(
            "--max-duration",
            type=float,
            help="stop the collector after this many seconds (default: run until signalled)",
        )
        parser.add_argument("--pidfile", help="pidfile to write while running")


# ---------------------------------------------------------------- handlers
def _symbols_list(args: argparse.Namespace) -> list[str] | None:
    raw = getattr(args, "symbols", None)
    if not raw:
        return None
    return [item.strip() for item in raw.split(",") if item.strip()]


def _pidfile_for(args: argparse.Namespace, lake: DataLake) -> Path:
    return Path(args.pidfile) if getattr(args, "pidfile", None) else lake.root / "_meta" / _PID_FILENAME


def cmd_collect(args: argparse.Namespace) -> int:
    lake = DataLake(args.lake)
    policy = _load_policy(lake, args)
    daemon = SnapshotCollectorDaemon(lake, policy)
    persist_policy(lake, policy)

    pidfile = _pidfile_for(args, lake)
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    pidfile.write_text(str(os.getpid()), encoding="utf-8")

    stop_requested: list[signal.Signals | None] = [None]

    def _handle(signum: int, _frame: object) -> None:  # pragma: no cover - signal path
        stop_requested[0] = signal.Signals(signum)

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    daemon.start()
    print(
        f"collecting {len(daemon.symbols)} symbol(s) into {lake.root / 'snapshots'} "
        f"(session {daemon.session_id}, poll {policy.poll_interval_s}s)",
        flush=True,
    )
    deadline = time.monotonic() + args.max_duration if args.max_duration else None
    try:
        while stop_requested[0] is None:
            if deadline is not None and time.monotonic() >= deadline:
                break
            time.sleep(0.2)
    finally:
        reason = stop_requested[0].name if stop_requested[0] else "duration"
        daemon.stop()
        pidfile.unlink(missing_ok=True)
    stats = daemon.stats
    print(
        f"stopped ({reason}): cycles={stats['cycles']} rows={stats['rows_written']} "
        f"missing={stats['missing']} late={stats['late']} thinned={stats['thinned']} "
        f"flush_errors={stats['flush_errors']} gaps_recorded={stats['session_gaps_recorded']}"
    )
    return 0 if stats["flush_errors"] == 0 else 1


def cmd_start(args: argparse.Namespace) -> int:
    lake = DataLake(args.lake)
    pidfile = _pidfile_for(args, lake)
    if pidfile.exists():
        try:
            os.kill(int(pidfile.read_text().strip()), 0)
            print(f"collector already running (pidfile {pidfile})")
            return 0
        except (ValueError, ProcessLookupError, PermissionError):
            pidfile.unlink(missing_ok=True)  # stale

    log_path = lake.root / "_meta" / _LOG_FILENAME
    log_path.parent.mkdir(parents=True, exist_ok=True)
    argv = [sys.executable, "-m", "pulsar_data.cli", "snapshots", "collect",
            "--lake", str(args.lake)]
    if args.config:
        argv += ["--config", args.config]
    if args.symbols:
        argv += ["--symbols", args.symbols]
    if args.universe_file:
        argv += ["--universe-file", args.universe_file]
    if args.all:
        argv += ["--all"]
    if args.accept_full_market:
        argv += ["--accept-full-market"]
    if args.poll_interval is not None:
        argv += ["--poll-interval", str(args.poll_interval)]
    if args.min_sample_interval is not None:
        argv += ["--min-sample-interval", str(args.min_sample_interval)]
    if args.max_duration is not None:
        argv += ["--max-duration", str(args.max_duration)]
    argv += ["--pidfile", str(pidfile)]
    with log_path.open("ab") as log:
        process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    print(f"collector started detached (pid {process.pid}, log {log_path}, pidfile {pidfile})")
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    lake = DataLake(args.lake)
    pidfile = lake.root / "_meta" / _PID_FILENAME
    if not pidfile.exists():
        print(f"no pidfile at {pidfile}; collector not running (or started with a custom --pidfile)")
        return 0
    pid = int(pidfile.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pidfile.unlink(missing_ok=True)
        print(f"pid {pid} not running; removed stale pidfile")
        return 0
    deadline = time.monotonic() + max(1.0, args.timeout)
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.2)
    else:
        log_path = lake.root / "_meta" / _LOG_FILENAME
        print(f"pid {pid} still alive after {args.timeout}s; check {log_path}", file=sys.stderr)
        return 1
    pidfile.unlink(missing_ok=True)
    print(f"collector pid {pid} stopped cleanly")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    lake = DataLake(args.lake)
    policy = _load_policy(lake, args)
    store = SnapshotStore(lake)
    payload = collect_status(store, policy, days=args.days)
    pidfile = lake.root / "_meta" / _PID_FILENAME
    running = False
    pid: int | None = None
    if pidfile.exists():
        try:
            pid = int(pidfile.read_text().strip())
            os.kill(pid, 0)
            running = True
        except (ValueError, ProcessLookupError, PermissionError):
            running = False
    payload["running"] = running
    payload["pid"] = pid if running else None
    if args.report:
        Path(args.report).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(f"report written to {args.report}")
    disk = payload["disk"]
    print(
        f"running={running} session={payload['last_session']} updated_at={payload['updated_at']}"
    )
    print(
        f"partitions raw={payload['raw_partitions']} archive={payload['archive_partitions']} "
        f"bytes raw={payload['raw_bytes']} archive={payload['archive_bytes']} "
        f"(~{disk['bytes_per_day']}/day, headroom ~{disk['days_headroom']}d)"
    )
    markers = payload["missing_markers_recent"]
    gaps = payload["session_gaps"]
    print(
        f"gaps: session_gaps={len(gaps)} missing_markers({args.days}d)="
        f"{sum(markers.values())}"
    )
    for record in gaps[-5:]:
        print(f"  session_gap {record['symbol']} {record['from_ts']} → {record['to_ts']}")
    for symbol, count in sorted(markers.items()):
        print(f"  missing {symbol}: {count}")
    alerts = payload["alerts"]
    for record in alerts[-5:]:
        print(f"  alert {record.get('type')} at {record.get('at')}")
    if disk["warn"]:
        for reason in disk["reasons"]:
            print(f"DISK WARN: {reason}", file=sys.stderr)
    return 0


def cmd_archive(args: argparse.Namespace) -> int:
    lake = DataLake(args.lake)
    policy = _load_policy(lake, args)
    report = archive_expired(lake, policy, today=args.today)
    if args.report:
        Path(args.report).write_text(
            json.dumps(
                {
                    "raw_partitions_archived": report.raw_partitions_archived,
                    "raw_rows_aggregated": report.raw_rows_aggregated,
                    "archive_rows_written": report.archive_rows_written,
                    "raw_bytes_reclaimed": report.raw_bytes_reclaimed,
                    "archive_partitions_pruned": report.archive_partitions_pruned,
                    "skipped": report.skipped,
                    "failures": report.failures,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        print(f"report written to {args.report}")
    print(
        f"archived={report.raw_partitions_archived} raw_rows={report.raw_rows_aggregated} "
        f"archive_rows={report.archive_rows_written} reclaimed_bytes={report.raw_bytes_reclaimed} "
        f"pruned_archives={report.archive_partitions_pruned} "
        f"skipped={len(report.skipped)} failed={len(report.failures)}"
    )
    return 0


def run(args: argparse.Namespace) -> int:
    """Dispatch one ``snapshots`` subcommand."""
    handlers = {
        "collect": cmd_collect,
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "archive": cmd_archive,
    }
    return handlers[args.snapshots_command](args)
