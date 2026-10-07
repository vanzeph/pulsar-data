"""CLI surface of the snapshot daemon: guard, collect, status, archive, stop.

The collect path is exercised in-process against a monkeypatched (offline)
quote source; the stop path signals a real short-lived subprocess so the
signal handling is genuinely tested without any network.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from pulsar_data.cli import main as cli_main
from pulsar_data.lake import DataLake
from pulsar_data.realtime.collector import RawSnapshot
from pulsar_data.realtime.events import EventKind, StreamEvent
from pulsar_data.schema import Dataset
from pulsar_data.snapshots import SnapshotStore

_SH = ZoneInfo("Asia/Shanghai")


class RepeatingSource:
    """A quote source that always answers — good for the foreground loop."""

    name = "repeating"
    last_source = "repeating"

    def poll(self, symbols):
        now = datetime.now(tz=_SH)
        return {
            symbol: RawSnapshot(
                symbol=symbol, ts=now, last_price=10.0, volume=100.0, amount=1000.0
            )
            for symbol in symbols
        }


def _write_one_event(lake: DataLake, *, day: str = "2026-10-05") -> None:
    ts = datetime.fromisoformat(f"{day}T09:30:00+08:00")
    event = StreamEvent(kind=EventKind.MISSING, symbol="SH600519", seq=1, ts=ts)
    store = SnapshotStore(lake)
    store.write_events(
        [SnapshotStore.event_to_row(event, session_id="cli-test", source_name="test")],
        session_id="cli-test",
    )


class TestCollectGuard:
    def test_full_market_flag_requires_explicit_acceptance(self, tmp_path, capsys):
        code = cli_main(
            ["snapshots", "collect", "--lake", str(tmp_path / "lake"), "--all", "--max-duration", "0.1"]
        )
        assert code == 2
        assert "acknowledge" in capsys.readouterr().err

    def test_watchlist_collect_runs_offline_and_writes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "pulsar_data.snapshots.daemon.build_default_source", lambda **kw: RepeatingSource()
        )
        lake_dir = tmp_path / "lake"
        code = cli_main(
            [
                "snapshots", "collect", "--lake", str(lake_dir),
                "--symbols", "SH600519,SZ000001",
                "--poll-interval", "0.2", "--max-duration", "1.5",
            ]
        )
        assert code == 0
        lake = DataLake(lake_dir)
        frame = lake.read(Dataset.SNAPSHOTS)
        assert not frame.empty
        assert set(frame["symbol"]) == {"SH600519", "SZ000001"}
        # the running policy was persisted for later status/archive runs
        persisted = json.loads((lake.root / "_meta" / "snapshot_policy.json").read_text())
        assert persisted["symbols"] == ["SH600519", "SZ000001"]


class TestStatus:
    def test_status_reports_state_gaps_and_disk(self, tmp_path, capsys):
        lake = DataLake(tmp_path / "lake")
        _write_one_event(lake)
        policy_file = tmp_path / "policy.json"
        policy_file.write_text(
            json.dumps({"symbols": ["SH600519"], "min_sample_interval_s": 0}), encoding="utf-8"
        )
        report = tmp_path / "status.json"
        code = cli_main(
            [
                "snapshots", "status", "--lake", str(lake.root),
                "--config", str(policy_file), "--report", str(report),
            ]
        )
        assert code == 0
        payload = json.loads(report.read_text())
        assert payload["raw_partitions"] == 1
        assert payload["running"] is False
        out = capsys.readouterr().out
        assert "partitions raw=1" in out

    def test_status_without_any_policy_is_a_configuration_error(self, tmp_path, capsys):
        code = cli_main(["snapshots", "status", "--lake", str(tmp_path / "fresh-lake")])
        assert code == 2


class TestArchiveCli:
    def test_archive_via_cli_uses_persisted_policy(self, tmp_path, capsys):
        lake = DataLake(tmp_path / "lake")
        ts = datetime.fromisoformat("2026-09-01T09:30:00+08:00")
        from pulsar_contracts import Snapshot

        event = StreamEvent(
            kind=EventKind.SNAPSHOT,
            symbol="SH600519",
            seq=1,
            ts=ts,
            snapshot=Snapshot(
                symbol="SH600519", ts=ts, seq=1, last_price=10.0, volume=1.0, amount=10.0
            ),
        )
        store = SnapshotStore(lake)
        store.write_events(
            [SnapshotStore.event_to_row(event, session_id="cli", source_name="t")],
            session_id="cli",
        )
        (lake.root / "_meta" / "snapshot_policy.json").write_text(
            json.dumps({"symbols": ["SH600519"], "raw_retention_days": 14}), encoding="utf-8"
        )
        report = tmp_path / "archive.json"
        code = cli_main(
            [
                "snapshots", "archive", "--lake", str(lake.root),
                "--today", "2026-10-05", "--report", str(report),
            ]
        )
        assert code == 0
        payload = json.loads(report.read_text())
        assert payload["raw_partitions_archived"] == 1
        assert not (lake.root / "snapshots" / "symbol=SH600519" / "date=2026-09-01").exists()
        assert "archived=1" in capsys.readouterr().out


class TestStartStop:
    def test_start_spawns_detached_collect_with_correct_argv(self, tmp_path, monkeypatch):
        captured: dict = {}

        class FakeProc:
            pid = 424242

            def poll(self):
                return 0

        def _fake_popen(argv, **kwargs):
            captured["argv"] = argv
            captured["kwargs"] = kwargs
            return FakeProc()

        monkeypatch.setattr("pulsar_data.snapshots.cli.subprocess.Popen", _fake_popen)
        lake_dir = tmp_path / "lake"
        code = cli_main(
            [
                "snapshots", "start", "--lake", str(lake_dir),
                "--symbols", "SH600519", "--poll-interval", "3",
            ]
        )
        assert code == 0
        argv = captured["argv"]
        assert argv[:5] == [sys.executable, "-m", "pulsar_data.cli", "snapshots", "collect"]
        assert "--symbols" in argv and "SH600519" in argv
        assert argv[argv.index("--poll-interval") + 1] == "3.0"
        assert captured["kwargs"]["start_new_session"] is True
        assert "--pidfile" in argv

    def test_stop_signals_a_real_process(self, tmp_path, capsys):
        lake = DataLake(tmp_path / "lake")
        # spawn the sleeper as a *grandchild* so it is not our direct child:
        # direct children of the test process linger as zombies (kill(pid,0)
        # keeps succeeding) which would fake a "still alive" collector
        launcher = subprocess.Popen(
            [
                sys.executable, "-c",
                "import subprocess, sys; p = subprocess.Popen("
                "[sys.executable, '-c', 'import time; time.sleep(60)']); "
                "print(p.pid, flush=True); p.wait()",
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            sleeper_pid = int(launcher.stdout.readline().strip())
        finally:
            pass
        pidfile = lake.root / "_meta" / "collect.pid"
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text(str(sleeper_pid), encoding="utf-8")
        try:
            code = cli_main(["snapshots", "stop", "--lake", str(lake.root), "--timeout", "10"])
            assert code == 0
            assert not pidfile.exists()
            assert "stopped cleanly" in capsys.readouterr().out
        finally:
            launcher.kill()
            launcher.wait(timeout=5)

    def test_stop_with_stale_pidfile_cleans_up(self, tmp_path, capsys):
        lake = DataLake(tmp_path / "lake")
        pidfile = lake.root / "_meta" / "collect.pid"
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text("999999999", encoding="utf-8")
        code = cli_main(["snapshots", "stop", "--lake", str(lake.root)])
        assert code == 0
        assert not pidfile.exists()
        assert "stale" in capsys.readouterr().out
