"""Snapshot accumulation policy: validation and the full-market guard."""

from __future__ import annotations

import json

import pytest

from pulsar_data.errors import ConfigurationError, LakeError
from pulsar_data.lake import DataLake
from pulsar_data.schema import INSTRUMENT_COLUMNS, Dataset
from pulsar_data.snapshots import SnapshotPolicy


class TestFullMarketGuard:
    def test_full_market_requires_explicit_acknowledgement(self):
        with pytest.raises(ConfigurationError, match="acknowledge"):
            SnapshotPolicy.from_mapping({"full_market": True})

    def test_full_market_with_acknowledgement_is_valid(self):
        policy = SnapshotPolicy.from_mapping(
            {"full_market": True, "acknowledge_full_market": True}
        )
        assert policy.full_market is True

    def test_empty_watchlist_without_full_market_is_rejected(self):
        with pytest.raises(ConfigurationError, match="watchlist"):
            SnapshotPolicy.from_mapping({})

    def test_guard_fires_before_any_collection_happens(self):
        """The guard is a load-time property, not a first-poll surprise."""
        with pytest.raises(ConfigurationError):
            SnapshotPolicy.from_mapping({"symbols": [], "full_market": True})


class TestValidation:
    def test_unknown_key_rejected(self):
        with pytest.raises(ConfigurationError, match="unknown snapshot policy keys"):
            SnapshotPolicy.from_mapping({"symbols": ["SH600519"], "universe": "all"})

    def test_non_positive_intervals_rejected(self):
        for key, value in (
            ("poll_interval_s", 0),
            ("flush_interval_s", -1),
            ("raw_retention_days", 0),
            ("archive_interval_s", 0),
            ("reconnect_gap_threshold_s", -5),
        ):
            with pytest.raises(ConfigurationError):
                SnapshotPolicy.from_mapping({"symbols": ["SH600519"], key: value})

    def test_disk_thresholds_validated(self):
        with pytest.raises(ConfigurationError):
            SnapshotPolicy.from_mapping(
                {"symbols": ["SH600519"], "disk_warn_free_percent": 0}
            )
        with pytest.raises(ConfigurationError):
            SnapshotPolicy.from_mapping(
                {"symbols": ["SH600519"], "disk_warn_used_bytes": -1}
            )

    def test_symbols_canonicalized_from_string(self):
        policy = SnapshotPolicy.from_mapping({"symbols": "600519, sz000001"})
        assert policy.symbols == ("SH600519", "SZ000001")

    def test_from_json_file_roundtrip(self, tmp_path):
        path = tmp_path / "policy.json"
        path.write_text(
            json.dumps({"symbols": ["SH600519"], "min_sample_interval_s": 5}), encoding="utf-8"
        )
        policy = SnapshotPolicy.from_json_file(path)
        assert policy.symbols == ("SH600519",)
        assert policy.min_sample_interval_s == 5

    def test_from_json_file_unreadable(self, tmp_path):
        with pytest.raises(ConfigurationError):
            SnapshotPolicy.from_json_file(tmp_path / "missing.json")


class TestResolveSymbols:
    def test_watchlist_returns_policy_symbols(self, tmp_path):
        lake = DataLake(tmp_path)
        policy = SnapshotPolicy(symbols=["SH600519", "SZ000001"])
        assert policy.resolve_symbols(lake) == ("SH600519", "SZ000001")

    def test_full_market_resolves_from_lake_instruments(self, tmp_path):
        import pandas as pd

        lake = DataLake(tmp_path)
        frame = pd.DataFrame(
            {
                "symbol": ["SZ000001", "SH600519"],
                "name": ["平安银行", "贵州茅台"],
                "exchange": ["SZ", "SH"],
                "board": ["main", "main"],
                "is_st": [False, False],
                "status": ["listed", "listed"],
                "list_date": ["1991-04-03", "2001-08-27"],
                "delist_date": [None, None],
                "shares_outstanding": [1.0, 1.0],
            }
        )[list(INSTRUMENT_COLUMNS)]
        lake.write(Dataset.INSTRUMENTS, frame, source="test")
        policy = SnapshotPolicy(
            full_market=True, acknowledge_full_market=True, symbols=("SH600519",)
        )
        # the lake universe wins over the watchlist under full_market
        assert policy.resolve_symbols(lake) == ("SH600519", "SZ000001")

    def test_full_market_without_instruments_is_a_lake_error(self, tmp_path):
        lake = DataLake(tmp_path)
        policy = SnapshotPolicy(full_market=True, acknowledge_full_market=True)
        with pytest.raises(LakeError, match="instruments"):
            policy.resolve_symbols(lake)
