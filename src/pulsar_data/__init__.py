"""pulsar-data: pluggable source adapters, normalization, local market-data lake."""

from .errors import (
    ConfigurationError,
    EgressViolation,
    FetchError,
    LakeError,
    PulsarDataError,
    QualityViolation,
)
from .lake import DataLake
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
    "Dataset",
    "EgressViolation",
    "FetchError",
    "FetchRequest",
    "LakeError",
    "PulsarDataError",
    "QualityViolation",
    "SourceAdapter",
    "get_adapter",
    "list_adapters",
    "register_adapter",
    "run_ingestion",
]
