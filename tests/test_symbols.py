"""Canonical symbol conversion and exchange/board inference."""

from __future__ import annotations

import pytest

from pulsar_data.errors import ConfigurationError
from pulsar_data.symbols import (
    infer_board,
    infer_exchange,
    symbol_universe_filter,
    to_canonical_symbol,
    to_source_code,
)
from pulsar_contracts import Board, Exchange


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("600519", "SH600519"),
        ("600519.SH", "SH600519"),
        ("sh600519", "SH600519"),
        ("SH600519", "SH600519"),
        ("000001", "SZ000001"),
        ("000001.SZ", "SZ000001"),
        ("sz000001", "SZ000001"),
        ("300750", "SZ300750"),
        ("688111", "SH688111"),
        ("920000", "BJ920000"),
        ("430047", "BJ430047"),
        ("bj920000", "BJ920000"),
    ],
)
def test_to_canonical_symbol(raw, canonical):
    assert to_canonical_symbol(raw) == canonical


@pytest.mark.parametrize(
    ("symbol", "exchange", "board"),
    [
        ("SH600519", Exchange.SSE, Board.MAIN),
        ("SH601127", Exchange.SSE, Board.MAIN),
        ("SH688981", Exchange.SSE, Board.STAR),
        ("SZ000001", Exchange.SZSE, Board.MAIN),
        ("SZ002415", Exchange.SZSE, Board.MAIN),
        ("SZ300750", Exchange.SZSE, Board.GEM),
        ("BJ920000", Exchange.BSE, Board.BSE),
    ],
)
def test_inference(symbol, exchange, board):
    assert infer_exchange(symbol) is exchange
    assert infer_board(symbol) is board


def test_to_source_code():
    assert to_source_code("SH600519") == "600519"
    with pytest.raises(ConfigurationError):
        to_source_code("600519")


def test_b_shares_excluded_from_universe():
    assert not symbol_universe_filter("SH900901")
    assert not symbol_universe_filter("SZ200596")
    assert symbol_universe_filter("SH600519")
    assert symbol_universe_filter("SZ300750")


def test_garbage_rejected():
    for bad in ("", "HELLOWORLD", "60051", "600519.XX", "sh60051"):
        with pytest.raises(ConfigurationError):
            to_canonical_symbol(bad)
