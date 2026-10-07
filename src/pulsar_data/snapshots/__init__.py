"""Snapshot-stream accumulation (快照流积累): the collect daemon family.

Task SNAP1 of the data-integration design.  A daemon reuses the D5
realtime subscription channel (Sina primary + Eastmoney fallback, guarded
egress, failover, per-symbol monotonic ``seq`` with late/missing marker
semantics — consumed unchanged) and accumulates every cycle into the
lake's new ``snapshots`` partition family (symbol × day, atomic writes,
watermarks), so Paper/Live-grade quotes become a long-term local research
asset.  Everything is policy-driven
(:class:`~pulsar_data.snapshots.policy.SnapshotPolicy`): the symbol
universe (watchlist by default; the ~1 GB/day full market needs explicit
acknowledgment), a sampling-frequency cap, raw retention, and the
downsample archive rule (e.g. 3s → 1m) executed by
:func:`~pulsar_data.snapshots.archive.archive_expired`.  Restarts reload
the persisted watermark state and ledger every downtime gap explicitly —
never a silent frame drop.  Everything lands on local disk first; zero
cloud dependency.
"""

from .archive import ArchiveReport, aggregate_snapshots, archive_expired
from .daemon import SnapshotCollectorDaemon, collect_status
from .policy import SnapshotPolicy
from .store import SnapshotStore

__all__ = [
    "ArchiveReport",
    "SnapshotCollectorDaemon",
    "SnapshotPolicy",
    "SnapshotStore",
    "aggregate_snapshots",
    "archive_expired",
    "collect_status",
]
