"""Canonical Pulsar symbol handling and exchange/board inference.

A canonical A-share symbol is the two-letter exchange prefix followed by
the six-digit code: ``SH600519``, ``SZ000001``, ``BJ920000``.  Upstream
sources use bare codes (``600519``), prefixed codes (``sh600519``) or
suffixed codes (``600519.SH``); this module converts between them and
infers exchange and listing board from the code itself.
"""

from __future__ import annotations

import re

from pulsar_contracts import Board, Exchange

from .errors import ConfigurationError

__all__ = [
    "to_canonical_symbol",
    "to_source_code",
    "infer_exchange",
    "infer_board",
    "symbol_universe_filter",
]

_CODE_RE = re.compile(r"^(\d{6})$")

#: canonical two-letter prefix <-> contract ``Exchange`` enum value.
_PREFIX_TO_EXCHANGE = {"SH": Exchange.SSE, "SZ": Exchange.SZSE, "BJ": Exchange.BSE}
_EXCHANGE_TO_PREFIX = {exchange: prefix for prefix, exchange in _PREFIX_TO_EXCHANGE.items()}


def _split(symbol: str) -> tuple[str, str]:
    """Split any accepted input form into ``(exchange_prefix_or_"", digits)``."""
    text = symbol.strip().upper()
    if not text:
        raise ConfigurationError(f"empty symbol: {symbol!r}")
    if "." in text:  # 600519.SH / 000001.SZ / 430047.BJ
        code, _, suffix = text.partition(".")
        if _CODE_RE.match(code) and suffix in {"SH", "SZ", "BJ"}:
            return suffix, code
    if text[:2] in {"SH", "SZ", "BJ"} and _CODE_RE.match(text[2:]):
        return text[:2], text[2:]
    if _CODE_RE.match(text):
        return "", text
    raise ConfigurationError(f"unrecognized A-share symbol: {symbol!r}")


def infer_exchange(code: str) -> Exchange:
    """Infer the exchange from a bare six-digit code (or canonical symbol)."""
    _, digits = _split(code)
    if digits.startswith(("60", "68", "90")):
        return Exchange.SSE
    if digits.startswith(("00", "30", "20")):
        return Exchange.SZSE
    if digits.startswith(("43", "83", "87", "88", "92")):
        return Exchange.BSE
    raise ConfigurationError(f"cannot infer exchange for code {code!r}")


def infer_board(code: str) -> Board:
    """Infer the listing board from a bare six-digit code (or canonical symbol)."""
    _, digits = _split(code)
    prefix2 = digits[:2]
    if prefix2 == "68":
        return Board.STAR
    if prefix2 == "30":
        return Board.GEM
    if prefix2 in {"43", "83", "87", "88", "92"}:
        return Board.BSE
    if prefix2 in {"60", "00", "20", "90"}:
        return Board.MAIN
    raise ConfigurationError(f"cannot infer board for code {code!r}")


def to_canonical_symbol(symbol: str) -> str:
    """Convert any accepted input form to the canonical ``XX######`` form."""
    prefix, digits = _split(symbol)
    if prefix:
        return f"{prefix}{digits}"
    exchange = infer_exchange(digits)
    return f"{_EXCHANGE_TO_PREFIX[exchange]}{digits}"


def to_source_code(symbol: str) -> str:
    """Convert a canonical symbol to the bare six-digit code used by sources."""
    prefix, digits = _split(symbol)
    if not prefix:
        raise ConfigurationError(f"symbol {symbol!r} carries no exchange prefix")
    return digits


def symbol_universe_filter(symbol: str) -> bool:
    """True for A-share symbols (excludes SH/SZ B-shares: 900xxx / 200xxx)."""
    prefix, digits = _split(symbol)
    if digits.startswith(("900", "200")):
        return False
    try:
        infer_exchange(f"{prefix}{digits}" if prefix else digits)
    except ConfigurationError:
        return False
    return True
