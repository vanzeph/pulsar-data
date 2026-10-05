"""Adapter registry: source id -> factory.

A new data source registers itself with :func:`register_adapter` and
becomes instantiable by id from configuration alone — the framework,
lake and CLI stay untouched.
"""

from __future__ import annotations

from typing import Callable, Mapping

from ..errors import ConfigurationError
from .base import SourceAdapter

__all__ = ["AdapterFactory", "register_adapter", "get_adapter", "list_adapters"]

#: An adapter factory builds a ready-to-use adapter from a plain config mapping.
AdapterFactory = Callable[[Mapping[str, object] | None], SourceAdapter]

_REGISTRY: dict[str, AdapterFactory] = {}


def register_adapter(
    source_id: str,
) -> Callable[[AdapterFactory], AdapterFactory]:
    """Class/function decorator declaring an adapter under ``source_id``."""

    def decorator(factory: AdapterFactory) -> AdapterFactory:
        if source_id in _REGISTRY and _REGISTRY[source_id] is not factory:
            raise ConfigurationError(f"adapter id {source_id!r} is already registered")
        _REGISTRY[source_id] = factory
        return factory

    return decorator


def get_adapter(source_id: str, config: Mapping[str, object] | None = None) -> SourceAdapter:
    """Instantiate the adapter registered under ``source_id``."""
    try:
        factory = _REGISTRY[source_id]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise ConfigurationError(
            f"unknown data source {source_id!r}; registered adapters: {known}"
        ) from None
    return factory(dict(config) if config else None)


def list_adapters() -> list[str]:
    """Ids of every registered adapter."""
    return sorted(_REGISTRY)
