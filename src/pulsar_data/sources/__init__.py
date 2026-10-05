"""Adapter framework: contract, registry, and built-in adapters."""

from .base import FetchRequest, IngestionResult, SourceAdapter, run_ingestion
from .registry import AdapterFactory, get_adapter, list_adapters, register_adapter

# Importing the akshare adapter registers it under "akshare", the
# baostock adapter under "baostock".  Both SDKs are imported lazily
# inside their live clients, so this stays cheap (and offline-safe).
from . import akshare as _akshare  # noqa: F401  (side effect: registration)
from . import baostock as _baostock  # noqa: F401  (side effect: registration)

__all__ = [
    "AdapterFactory",
    "FetchRequest",
    "IngestionResult",
    "SourceAdapter",
    "get_adapter",
    "list_adapters",
    "register_adapter",
    "run_ingestion",
]
