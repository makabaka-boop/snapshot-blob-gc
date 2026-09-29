"""本地对象仓库。

公开 API 见 :class:`local_repo.repository.ObjectRepository`。
"""

from local_repo.exceptions import (
    CorruptionError,
    LeaseNotFound,
    ObjectNotFound,
    SnapshotNotFound,
)
from local_repo.repository import GCReport, LeaseInfo, ObjectRepository

__all__ = [
    "ObjectRepository",
    "LeaseInfo",
    "GCReport",
    "ObjectNotFound",
    "SnapshotNotFound",
    "LeaseNotFound",
    "CorruptionError",
]
