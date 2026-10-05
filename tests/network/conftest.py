"""Marker registration for the optional live smoke tests.

The ``network`` marker is registered here (instead of the shared
``pyproject.toml``) so this task touches no configuration another
parallel task might be editing.  Tests under ``tests/network/`` are
skipped unless ``PULSAR_RUN_NETWORK_TESTS=1`` is set.
"""

from __future__ import annotations

import os


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "network: hits the real public quote endpoints; run explicitly with "
        "PULSAR_RUN_NETWORK_TESTS=1",
    )


def pytest_collection_modifyitems(config, items):
    if os.environ.get("PULSAR_RUN_NETWORK_TESTS") == "1":
        return
    import pytest

    skip = pytest.mark.skip(reason="network smoke disabled by default; set PULSAR_RUN_NETWORK_TESTS=1")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip)
