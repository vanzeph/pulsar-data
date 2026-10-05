"""Registry behavior and third-party adapter extension (framework untouched)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from pulsar_data.errors import ConfigurationError, QualityViolation
from pulsar_data.lake import DataLake
from pulsar_data.schema import BAR_COLUMNS, Dataset, daily_ts
from pulsar_data.sources import (
    FetchRequest,
    SourceAdapter,
    get_adapter,
    list_adapters,
    register_adapter,
    run_ingestion,
)


def test_akshare_registered():
    assert "akshare" in list_adapters()


def test_unknown_adapter_rejected():
    with pytest.raises(ConfigurationError, match="unknown data source"):
        get_adapter("does-not-exist")


def test_duplicate_id_rejected():
    @register_adapter("dup-test")
    def build(config=None):
        return _MockAdapter()

    with pytest.raises(ConfigurationError, match="already registered"):
        @register_adapter("dup-test")
        def build_again(config=None):
            return _MockAdapter()


class _MockAdapter:
    """Example third-party adapter: registers, ingests, zero framework edits."""

    source_id = "mock"

    def __init__(self, config=None) -> None:
        self.config = config or {}

    def fetch_raw(self, dataset: Dataset, request: FetchRequest) -> pd.DataFrame:
        if dataset is not Dataset.BARS_1D:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "d": ["2024-01-02", "2024-01-03"],
                "o": [10.0, 10.5],
                "h": [11.0, 10.8],
                "l": [9.8, 10.2],
                "c": [10.8, 10.4],
                "v": [100.0, 200.0],
                "notional": [1080.0, 2080.0],
            }
        )

    def normalize(self, dataset: Dataset, raw: pd.DataFrame, request: FetchRequest) -> pd.DataFrame:
        if dataset is not Dataset.BARS_1D:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "symbol": request.symbol,
                "ts": [daily_ts(value) for value in raw["d"]],
                "open": raw["o"],
                "high": raw["h"],
                "low": raw["l"],
                "close": raw["c"],
                "volume": raw["v"],
                "amount": raw["notional"],
                "adjust_factor": 1.0,
                "quality": "ok",
            }
        )[list(BAR_COLUMNS)]


def test_mock_adapter_registers_and_ingests(lake: DataLake):
    @register_adapter("mock")
    def build(config=None):
        return _MockAdapter(config)

    adapter = get_adapter("mock", {"greeting": "hi"})
    assert adapter.config == {"greeting": "hi"}
    result = run_ingestion(
        adapter,
        FetchRequest(Dataset.BARS_1D, date(2024, 1, 1), date(2024, 1, 31), "SH600519"),
        lake,
    )
    assert result.rows == 2
    stored = lake.read(Dataset.BARS_1D)
    assert len(stored) == 2
    assert set(stored["quality"]) == {"ok"}


def test_framework_quality_gate_applies_to_any_adapter(lake: DataLake):
    """A sloppy third-party adapter is stopped by the framework gate, not trusted."""

    class _SloppyAdapter(_MockAdapter):
        def normalize(self, dataset, raw, request):
            frame = super().normalize(dataset, raw, request)
            frame.loc[0, "high"] = 9.0  # below close -> OHLC violation
            return frame

    sloppy = _SloppyAdapter()
    assert isinstance(sloppy, SourceAdapter)  # runtime-checkable protocol
    with pytest.raises(QualityViolation, match="OHLC"):
        run_ingestion(
            sloppy,
            FetchRequest(Dataset.BARS_1D, date(2024, 1, 1), date(2024, 1, 31), "SH600519"),
            lake,
        )
    # nothing landed
    assert lake.read(Dataset.BARS_1D).empty
