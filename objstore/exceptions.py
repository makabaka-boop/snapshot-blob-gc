"""仓库操作的确定性异常类型。

所有异常都对应一个确定的、可断言的结果：不存在、非法参数或内部不变量被破坏。
"""

from __future__ import annotations


class RepositoryError(Exception):
    """所有仓库异常的基类。"""


class InvalidObjectIdError(RepositoryError):
    """对象 id 不是 64 位小写十六进制 SHA-256 摘要。"""


class ObjectNotFoundError(RepositoryError):
    """引用的对象在元数据中不存在。

    :ivar object_id: 缺失对象的 id（单个引用接口会填充）。
    :ivar object_ids: 一批引用中所有缺失对象的 id（已排序）。
    """

    def __init__(
        self,
        object_id: str | None = None,
        object_ids: "list[str] | None" = None,
    ) -> None:
        self.object_id = object_id
        self.object_ids = sorted(object_ids or ([object_id] if object_id else []))
        detail = self.object_ids[0] if len(self.object_ids) == 1 else ""
        if len(self.object_ids) > 1:
            detail = f"{len(self.object_ids)} 个对象不存在: {self.object_ids}"
        super().__init__(f"对象不存在: {detail}" if detail else "对象不存在")


class SnapshotNotFoundError(RepositoryError):
    """快照不存在（从未发布或已删除）。"""


class LeaseNotFoundError(RepositoryError):
    """导出租约不存在（从未授予、已过期清理或已撤销）。"""


class UnknownGCRunError(RepositoryError):
    """传入的回收运行 id 不存在。"""


class DuplicateGCRunError(RepositoryError):
    """回收运行 id 冲突（正常的 hex 摘要 id 下不会发生）。"""


class InternalConsistencyError(RepositoryError):
    """数据库与对象目录之间的不变量被破坏。"""
