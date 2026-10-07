"""Accumulation policy for the snapshot collect daemon (策略可配).

The design fixes four configurable knobs for long-term snapshot
accumulation — the symbol universe (default: an explicit watchlist; the
full market at roughly 1 GB/day must be *explicitly* acknowledged), the
sampling-frequency cap, the local raw retention, and the downsample
archive rule — plus disk-watermark alerting.  :class:`SnapshotPolicy`
is that policy: one immutable, strictly-validated value object loaded
from a mapping (JSON config file or CLI flags).

The full-market guard is deliberately loud: a policy that asks for the
whole market without ``acknowledge_full_market`` is a
:class:`~pulsar_data.errors.ConfigurationError`, not a silent default —
the disk-budget footgun the design calls out.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Mapping

from ..errors import ConfigurationError, LakeError
from ..schema import Dataset
from ..symbols import to_canonical_symbol

logger = logging.getLogger("pulsar_data.snapshots.policy")

__all__ = ["SnapshotPolicy", "DEFAULT_POLICY_FILENAME"]

#: Conventional policy filename under the lake's ``_meta`` directory.
DEFAULT_POLICY_FILENAME = "snapshot_policy.json"

#: Rough full-market disk footprint the design budget calls out
#: (~5000 symbols × ~4800 three-second cycles per session ≈ 1 GB/day of
#: Parquet).  Used for status projections only, never for enforcement.
FULL_MARKET_BYTES_PER_DAY = 1_000_000_000


@dataclass(frozen=True)
class SnapshotPolicy:
    """The complete, validated accumulation policy of one collect daemon.

    All durations are seconds; ``symbols`` is canonicalized on load.
    ``full_market`` resolves the universe from the lake's ingested
    instruments snapshot at daemon start (offline, no upstream call).
    """

    #: Explicit watchlist (canonical symbols).  Ignored when ``full_market``.
    symbols: tuple[str, ...] = ()
    #: Collect every instrument in the lake's instruments snapshot.
    full_market: bool = False
    #: Required explicit opt-in for ``full_market`` (≈1 GB/day disk budget).
    acknowledge_full_market: bool = False
    #: Seconds between polls of the realtime channel (D5 semantics).
    poll_interval_s: float = 3.0
    #: Sampling-frequency cap: persist at most one quote per symbol per this
    #: many seconds; quotes arriving faster land as ``thinned`` marker rows
    #: (0 disables thinning — every cycle is persisted with data).
    min_sample_interval_s: float = 0.0
    #: Buffer flush cadence and size trigger.
    flush_interval_s: float = 5.0
    flush_rows: int = 5000
    #: Raw ``snapshots`` partitions strictly older than this many days are
    #: downsampled into ``snapshots_1m`` and then reclaimed.
    raw_retention_days: int = 14
    #: Aggregation interval of the downsampled archive (e.g. 3s → 60s).
    archive_interval_s: float = 60.0
    #: Reclaim archived ``snapshots_1m`` partitions older than this many
    #: days (0 = keep archives forever; they are tiny by comparison).
    archive_retention_days: int = 0
    #: Downtime longer than this between persisted state and a new session
    #: (same trade date) is recorded as an explicit session gap.
    reconnect_gap_threshold_s: float = 10.0
    #: Alert when the lake volume's free space drops below this percentage.
    disk_warn_free_percent: float = 10.0
    #: Alert when the snapshots family exceeds this many bytes (None: off).
    disk_warn_used_bytes: int | None = None
    #: Extra free-form note carried into status output (optional).
    note: str = ""

    # ------------------------------------------------------------- loading
    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "SnapshotPolicy":
        """Build and validate a policy from a plain mapping.

        Unknown keys are rejected (typos must not silently widen the
        universe or the disk budget); every numeric bound is checked, and
        the full-market guard runs before anything else.
        """
        known = {f.name for f in fields(cls)}
        unknown = set(mapping) - known
        if unknown:
            raise ConfigurationError(
                f"unknown snapshot policy keys: {sorted(unknown)}; "
                f"known keys: {sorted(known)}"
            )
        values = dict(mapping)
        symbols = values.get("symbols") or ()
        if isinstance(symbols, str):
            symbols = [part.strip() for part in symbols.split(",") if part.strip()]
        try:
            values["symbols"] = tuple(to_canonical_symbol(s) for s in symbols)
        except Exception as exc:  # noqa: BLE001 - surface as configuration error
            raise ConfigurationError(f"invalid symbols in snapshot policy: {exc}") from exc
        policy = cls(**values)
        policy.validate()
        return policy

    @classmethod
    def from_json_file(cls, path: str | Path) -> "SnapshotPolicy":
        """Load a policy from a JSON file (the ``--config`` CLI input)."""
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ConfigurationError(f"cannot read snapshot policy {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ConfigurationError(f"snapshot policy {path} must be a JSON object")
        return cls.from_mapping(payload)

    # ----------------------------------------------------------- validation
    def validate(self) -> None:
        """Raise :class:`ConfigurationError` on any inconsistent value."""
        # the explicit full-market guard (never a silent default)
        if self.full_market and not self.acknowledge_full_market:
            raise ConfigurationError(
                "full-market snapshot collection writes ~1 GB/day; acknowledge the "
                "disk budget explicitly (acknowledge_full_market=true in the policy "
                "JSON, or --accept-full-market on the CLI) or keep the default "
                "watchlist universe"
            )
        if not self.full_market and not self.symbols:
            raise ConfigurationError(
                "snapshot policy needs a non-empty watchlist (symbols=...) unless "
                "full_market=true is explicitly acknowledged"
            )
        positive = {
            "poll_interval_s": self.poll_interval_s,
            "flush_interval_s": self.flush_interval_s,
            "raw_retention_days": self.raw_retention_days,
            "archive_interval_s": self.archive_interval_s,
            "reconnect_gap_threshold_s": self.reconnect_gap_threshold_s,
        }
        for name, value in positive.items():
            if not value > 0:
                raise ConfigurationError(f"snapshot policy {name} must be > 0, got {value!r}")
        for name, value in {
            "min_sample_interval_s": self.min_sample_interval_s,
            "flush_rows": self.flush_rows,
        }.items():
            if value < 0:
                raise ConfigurationError(f"snapshot policy {name} must be >= 0, got {value!r}")
        if self.flush_rows == 0:
            raise ConfigurationError("snapshot policy flush_rows must be >= 1")
        if self.archive_retention_days < 0:
            raise ConfigurationError(
                f"snapshot policy archive_retention_days must be >= 0, got {self.archive_retention_days!r}"
            )
        if not 0 < self.disk_warn_free_percent <= 100:
            raise ConfigurationError(
                f"snapshot policy disk_warn_free_percent must be in (0, 100], "
                f"got {self.disk_warn_free_percent!r}"
            )
        if self.disk_warn_used_bytes is not None and self.disk_warn_used_bytes <= 0:
            raise ConfigurationError(
                f"snapshot policy disk_warn_used_bytes must be > 0 when set, "
                f"got {self.disk_warn_used_bytes!r}"
            )

    # -------------------------------------------------------------- getters
    def resolve_symbols(self, lake: Any) -> tuple[str, ...]:
        """The concrete canonical universe this policy collects.

        Watchlist policies return their (already canonicalized) symbols;
        full-market policies read the lake's instruments snapshot — the
        universe the daily bar pipeline already maintains, so no extra
        upstream call is ever made for universe discovery.
        """
        if not self.full_market:
            return tuple(self.symbols)
        frame = lake.read(Dataset.INSTRUMENTS)
        if frame.empty:
            raise LakeError(
                "full-market snapshot collection needs the instruments dataset in "
                "the lake; run `pulsar-data backfill --all` first or use an explicit "
                "watchlist"
            )
        return tuple(sorted(to_canonical_symbol(s) for s in frame["symbol"]))

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly view for status output and state files."""
        payload = asdict(self)
        payload["symbols"] = list(self.symbols)
        return payload
