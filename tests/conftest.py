"""Shared test fixtures and helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "akshare"


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    if not FIXTURES.is_dir():
        pytest.skip("akshare fixtures not recorded")
    return FIXTURES


@pytest.fixture()
def lake(tmp_path):
    from pulsar_data.lake import DataLake

    return DataLake(tmp_path / "lake")
