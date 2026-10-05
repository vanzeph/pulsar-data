"""pulsar-data: pluggable source adapters, normalization, local market-data lake.

Read side: :class:`LakeQuery` (DuckDB over Parquet), :func:`derive_adjusted`
(query-time forward/backward adjustment) and :class:`LakeMarketDataPort`
(the ``MarketDataPort`` fetch_*/calendar/list_instruments implementation).
Write side: :class:`BackfillRunner` (historical) and
:class:`IncrementalRunner` (watermark-driven daily increments, idempotent).
"""

from .adjust import derive_adjusted
from .errors import (
    ConfigurationError,
    DataNotAvailable,
    EgressViolation,
    FetchError,
    LakeError,
    PulsarDataError,
    QualityViolation,
)
from .incremental import IncrementalRunner
from .lake import DataLake
from .port import LakeMarketDataPort
from .query import LakeQuery
from .schema import Dataset
from .sources import (
    FetchRequest,
    SourceAdapter,
    get_adapter,
    list_adapters,
    register_adapter,
    run_ingestion,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "ConfigurationError",
    "DataLake",
    "DataNotAvailable",
    "Dataset",
    "EgressViolation",
    "FetchError",
    "FetchRequest",
    "IncrementalRunner",
    "LakeError",
    "LakeMarketDataPort",
    "LakeQuery",
    "PulsarDataError",
    "QualityViolation",
    "SourceAdapter",
    "derive_adjusted",
    "get_adapter",
    "list_adapters",
    "register_adapter",
    "run_ingestion",
]
