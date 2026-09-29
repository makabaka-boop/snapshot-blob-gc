"""数据库与对象目录之间的一致性检查（测试在每一步后调用）。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from .repo import Repository


@dataclass
class ConsistencyReport:
    """一致性核对结果。``violations`` 为空即一致。"""

    violations: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def raise_if_bad(self) -> None:
        if self.violations:
            raise AssertionError("仓库一致性被破坏:\n  - " + "\n  - ".join(self.violations))


def check_consistency(repo: Repository) -> ConsistencyReport:
    """核对元数据与对象目录的双向一一对应及引用完整性。

    正常 API 操作、两阶段回收的每个边界，以及崩溃后重试完成之后，
    本检查都应当返回空报告。
    """

    violations: list[str] = []
    pending_file_ids: set[str] = set()
    conn = repo._connect()
    try:
        db_ids = {r[0] for r in conn.execute("SELECT id FROM objects").fetchall()}

        # 外键约束本身由 PRAGMA foreign_keys 保证，这里再显式核对引用关系。
        for table, col in ("snapshot_objects", "object_id"), ("lease_objects", "object_id"):
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM {table} WHERE {col} NOT IN"
                " (SELECT id FROM objects)"
            ).fetchall()
            for r in rows:
                violations.append(f"{table} 引用了不存在的对象: {r[0]}")

        # 回收审计行：reclaimed 的对象必须真的不再有数据库行
        # （数据库绝不能指向已删除文件）。
        rows = conn.execute(
            "SELECT object_id FROM gc_candidates WHERE state = 'reclaimed'"
            " AND object_id IN (SELECT id FROM objects)"
        ).fetchall()
        for r in rows:
            violations.append(f"候选标记为 reclaimed，但对象行仍存在: {r[0]}")

        # 崩溃可能发生在“元数据删除已提交、文件尚未 unlink”之间：
        # 这些文件没有任何数据库引用，是合法的待清理文件，不算损坏，
        # 下次 sweep 重试会补删。
        pending_file_ids = {
            r[0]
            for r in conn.execute(
                "SELECT object_id FROM gc_candidates WHERE state = 'reclaimed'"
            ).fetchall()
        }

        # 所有标记完成的回收运行：候选对象要么存活（被救回），要么 reclaimed。
        rows = conn.execute(
            "SELECT gc.run_id, gc.object_id FROM gc_candidates gc"
            " JOIN gc_runs r ON r.id = gc.run_id"
            " WHERE gc.state = 'candidate' AND r.status = 'swept'"
        ).fetchall()
        for r in rows:
            violations.append(
                f"回收运行 {r['run_id']} 已完成但候选仍未处理: {r['object_id']}"
            )
    finally:
        conn.close()

    # 目录侧：枚举对象文件，与数据库双向比对。
    disk_ids: set[str] = set()
    for shard in sorted(repo.objects_dir.iterdir()):
        if shard.name.startswith(".") or not shard.is_dir():
            if shard.is_file() and shard.name.startswith(".tmp-"):
                violations.append(f"残留临时文件: {shard}")
            continue
        for f in sorted(shard.iterdir()):
            if f.name.startswith(".tmp-"):
                violations.append(f"残留临时文件: {f}")
                continue
            if not f.is_file():
                violations.append(f"对象目录中存在非文件条目: {f}")
                continue
            oid = shard.name + f.name
            if len(oid) != 64:
                violations.append(f"对象文件名无法构成合法 id: {f}")
                continue
            disk_ids.add(oid)

    for oid in sorted(db_ids - disk_ids):
        violations.append(f"数据库引用了已删除/缺失的对象文件: {oid}")
    for oid in sorted((disk_ids - db_ids) - pending_file_ids):
        violations.append(f"对象文件存在但数据库没有记录（孤儿文件）: {oid}")

    return ConsistencyReport(violations=violations)
