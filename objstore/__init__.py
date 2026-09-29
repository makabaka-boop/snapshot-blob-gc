"""本地内容寻址对象仓库：不可变快照、SQLite 元数据与两阶段垃圾回收。"""

from .exceptions import (
    DuplicateGCRunError,
    InternalConsistencyError,
    InvalidObjectIdError,
    LeaseNotFoundError,
    ObjectNotFoundError,
    RepositoryError,
    SnapshotNotFoundError,
    UnknownGCRunError,
)
from .clocks import Clock, SystemClock
from .hooks import GCHooks
from .repo import GCMarkResult, GCSweepResult, Repository, SnapshotInfo, LeaseInfo
from .diagnostics import ConsistencyReport, check_consistency

__all__ = [
    "Clock",
    "SystemClock",
    "ConsistencyReport",
    "DuplicateGCRunError",
    "GCHooks",
    "GCMarkResult",
    "GCSweepResult",
    "InternalConsistencyError",
    "InvalidObjectIdError",
    "LeaseInfo",
    "LeaseNotFoundError",
    "ObjectNotFoundError",
    "Repository",
    "RepositoryError",
    "SnapshotInfo",
    "SnapshotNotFoundError",
    "UnknownGCRunError",
    "check_consistency",
]
