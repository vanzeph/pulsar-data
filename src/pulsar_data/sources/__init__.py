"""Adapter framework: contract, registry, and built-in adapters."""

from .base import FetchRequest, IngestionResult, SourceAdapter, run_ingestion
from .registry import AdapterFactory, get_adapter, list_adapters, register_adapter

# Importing the akshare adapter registers it under "akshare".  The
# akshare SDK itself is imported lazily inside the live client, so this
# stays cheap (and offline-safe).
from . import akshare as _akshare  # noqa: F401  (side effect: registration)

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
