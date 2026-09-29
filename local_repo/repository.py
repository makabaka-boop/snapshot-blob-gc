"""本地对象仓库：Python API、SQLite 元数据、两阶段垃圾回收。

存储布局（root 目录）::

    root/
      repo.sqlite3          # SQLite 元数据
      objects/ab/abcdef...  # 按散列前两字符分目录的内容寻址对象
      tmp/                  # put_object 的落盘暂存目录
      graveyard/            # GC 第二阶段“先移除、后物理删除”的过渡目录

根引用与 GC 安全模型
--------------------
* 快照是不可变的对象集合。``snapshots.rows`` 保存快照 -> 对象的根引用。
  任何已存在的快照（包括发布前的草稿）都保护其对象；删除快照才撤销
  根引用。这样保证数据库永远不会指向已删除文件。
* 已发布快照只是多了 ``published=1`` 标记（用于“发布”这一确定状态与
  并发发布幂等）；导出租约（``export_leases``，未到期）同样是根。
* 垃圾回收分两个持久化阶段：

  1. **标记候选**：在一个 IMMEDIATE 事务中扫描当前不可达对象，写入
     ``gc_runs/gc_candidates`` 并提交。提交后候选即冻结。
  2. **复核后删除文件**：逐个候选重新在数据库中复核可达性；只有仍
     不可达的对象，才在同一事务里删除其元数据行，事务提交后再把文件
     移动到 graveyard，最后物理删除。两阶段之间（甚至两次复核之间）
     若新快照或新租约重新引用候选对象，删除自动放弃。

* 任何文件的物理删除都发生在“对应数据库变更已提交之后”；任何数据库
  行的提交都保证对应文件仍在 ``objects/`` 中。崩溃后重开仓库，启动
  恢复（``_reconcile``）会把 graveyard 中“数据库仍引用”的文件还原，
  并把“数据库已无引用”的残留文件清掉——因此中断后可以安全重试。
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

from local_repo.exceptions import (
    CorruptionError,
    LeaseNotFound,
    ObjectNotFound,
    SnapshotNotFound,
)

OID_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_oid(oid: str) -> str:
    if not isinstance(oid, str) or not OID_RE.match(oid):
        raise ValueError(f"非法对象 ID（应为 64 位小写十六进制）: {oid!r}")
    return oid


@dataclass(frozen=True)
class LeaseInfo:
    """导出租约视图。"""

    id: str
    oid: str
    expires_at: float

    @property
    def is_live(self) -> bool:
        """租约是否尚未到期（以租约自身的到期时刻为准）。"""
        return time.time() < self.expires_at

    def is_live_at(self, now: float) -> bool:
        """在可控时钟 ``now`` 下租约是否尚未到期。"""
        return now < self.expires_at


@dataclass(frozen=True)
class GCReport:
    """一次（可能是恢复执行的）垃圾回收结果。"""

    run_id: str
    resumed: bool
    """本次调用是否接管了一个中断的 GC 运行。"""
    candidates: int
    """标记阶段冻结的候选对象数量。"""
    deleted: int
    """复核仍不可达、最终删除元数据与文件的对象数量。"""
    kept: int
    """因被新快照/未到期租约重新引用而放弃删除的候选数量。"""
    orphans_removed: int
    """清理的无元数据游离文件数量。"""

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "resumed": self.resumed,
            "candidates": self.candidates,
            "deleted": self.deleted,
            "kept": self.kept,
            "orphans_removed": self.orphans_removed,
        }


_SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    oid       TEXT PRIMARY KEY,
    size      INTEGER NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id         TEXT PRIMARY KEY,
    published  INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    published_at REAL
);

CREATE TABLE IF NOT EXISTS snapshot_rows (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    oid         TEXT NOT NULL REFERENCES objects(oid),
    seq         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_snapshot_rows_oid ON snapshot_rows(oid);

CREATE TABLE IF NOT EXISTS export_leases (
    id         TEXT PRIMARY KEY,
    oid        TEXT NOT NULL REFERENCES objects(oid),
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_export_leases_oid ON export_leases(oid);
CREATE INDEX IF NOT EXISTS idx_export_leases_expires ON export_leases(expires_at);

CREATE TABLE IF NOT EXISTS gc_runs (
    id        TEXT PRIMARY KEY,
    state     TEXT NOT NULL CHECK (state IN ('marking', 'sweeping', 'done')),
    created_at REAL NOT NULL,
    finished_at REAL
);

CREATE TABLE IF NOT EXISTS gc_candidates (
    run_id TEXT NOT NULL REFERENCES gc_runs(id) ON DELETE CASCADE,
    oid    TEXT NOT NULL,
    state  TEXT NOT NULL DEFAULT 'pending'
               CHECK (state IN ('pending', 'deleted', 'kept')),
    PRIMARY KEY (run_id, oid)
);
CREATE INDEX IF NOT EXISTS idx_gc_candidates_state
    ON gc_candidates(run_id, state);
"""

# 可达性判定：对象被任何快照行引用，或被未到期租约引用。
_REACHABLE_CTE = """
WITH reachable AS (
    SELECT sr.oid
      FROM snapshot_rows sr
    UNION
    SELECT l.oid
      FROM export_leases l
     WHERE l.expires_at > :now
)
"""


class ObjectRepository:
    """单进程、线程安全的本地对象仓库。

    参数:
        root: 仓库目录，不存在会被创建。
        clock: 可注入时钟，返回当前时间（秒），默认为 :func:`time.time`。
        on_after_mark: 阶段屏障回调。新 GC 运行完成“标记候选”提交后、
            第二阶段开始前被调用（锁外），参数为 ``(run_id, 候选列表)``。
            测试可在其中发布新快照/新建租约以复现两阶段竞争。
        on_after_delete: 每删除一个对象后（锁外）以 ``(run_id, oid)``
            调用，用于在第二阶段中途制造竞争。
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        clock: Callable[[], float] | None = None,
        on_after_mark: Callable[[str, list[str]], None] | None = None,
        on_after_delete: Callable[[str, str], None] | None = None,
    ) -> None:
        self.root = Path(root)
        self.objects_dir = self.root / "objects"
        self.tmp_dir = self.root / "tmp"
        self.graveyard_dir = self.root / "graveyard"
        for d in (self.root, self.objects_dir, self.tmp_dir, self.graveyard_dir):
            d.mkdir(parents=True, exist_ok=True)

        self._clock = clock or time.time
        self._on_after_mark = on_after_mark
        self._on_after_delete = on_after_delete

        # 单一连接 + 可重入锁：所有数据库变更都在 BEGIN IMMEDIATE
        # 事务中完成，文件移动/删除严格安排在事务提交之后。
        self._lock = threading.RLock()
        self._gc_lock = threading.Lock()
        self._conn = sqlite3.connect(
            self.root / "repo.sqlite3",
            isolation_level=None,  # autocommit；事务边界由我们显式管理
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = FULL")
        self._conn.executescript(_SCHEMA)

        # 启动恢复：处理上一进程崩溃留下的 graveyard、游离对象文件与
        # 暂存文件，使后续任何操作都建立在一致状态之上。
        with self._lock:
            self._reconcile_graveyard()
            self._sweep_orphan_files()
            self._purge_tmp()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            with contextlib.suppress(Exception):
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._conn.close()

    def __enter__(self) -> "ObjectRepository":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextlib.contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """串行化的 IMMEDIATE 写事务。"""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def now(self) -> float:
        return self._clock()

    def _object_path(self, oid: str) -> Path:
        return self.objects_dir / oid[:2] / oid

    # ------------------------------------------------------------------
    # 对象 API
    # ------------------------------------------------------------------

    def put_object(self, data: bytes) -> str:
        """写入对象，返回其内容寻址 ID（SHA-256 十六进制）。

        相同内容是幂等的：重复写入返回同一 OID，不改变引用计数。
        """
        if isinstance(data, str):
            raise TypeError("data 必须是 bytes，不能是 str")
        mv = memoryview(data)
        oid = hashlib.sha256(mv).hexdigest()
        size = mv.nbytes
        now = self.now()

        tmp_path = self.tmp_dir / f"put-{uuid.uuid4().hex}"
        # 先把数据完整落到暂存文件（fsync），崩溃也只会留下游离 tmp 文件。
        with open(tmp_path, "wb") as f:
            f.write(mv)
            f.flush()
            os.fsync(f.fileno())

        final_path = self._object_path(oid)
        with self._lock:
            # 关键序次：先让文件在 objects/ 中就位（fsync），再提交元数据。
            # 这样最坏的崩溃后果只是“有文件、无行”的游离文件（重开时
            # 清除），绝不会出现“有行、无文件”的悬挂引用。
            existing = self._conn.execute(
                "SELECT 1 FROM objects WHERE oid = ?", (oid,)
            ).fetchone()
            if existing is not None:
                self._ensure_object_present(oid)
                reused = True
            else:
                final_path.parent.mkdir(parents=True, exist_ok=True)
                os.replace(tmp_path, final_path)
                self._fsync_parent(final_path.parent)
                with self._tx() as conn:
                    inserted = conn.execute(
                        "INSERT OR IGNORE INTO objects(oid, size, created_at) "
                        "VALUES (?, ?, ?)",
                        (oid, size, now),
                    ).rowcount
                if inserted == 0:
                    # 理论上同锁下不会发生；防御并发进程：校验既有文件
                    # 并丢弃暂存文件。
                    self._ensure_object_present(oid)
                    reused = True
                else:
                    reused = False

        if reused:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
        return oid

    def get_object(self, oid: str) -> bytes:
        """读取对象内容。不存在抛 :class:`ObjectNotFound`。"""
        _validate_oid(oid)
        with self._lock:
            row = self._conn.execute(
                "SELECT oid FROM objects WHERE oid = ?", (oid,)
            ).fetchone()
            if row is None:
                raise ObjectNotFound(oid)
            self._ensure_object_present(oid)
            path = self._object_path(oid)
            data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != oid:
            raise CorruptionError(f"对象内容与散列不符: {oid}")
        return data

    def has_object(self, oid: str) -> bool:
        _validate_oid(oid)
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM objects WHERE oid = ?", (oid,)
            ).fetchone()
            return row is not None

    def list_objects(self) -> list[str]:
        with self._lock:
            return [
                r[0]
                for r in self._conn.execute(
                    "SELECT oid FROM objects ORDER BY oid"
                ).fetchall()
            ]

    def _ensure_object_present(self, oid: str) -> None:
        """数据库行存在时，文件必须存在且内容正确，否则属于损坏。"""
        path = self._object_path(oid)
        if not path.is_file():
            raise CorruptionError(f"数据库引用的对象文件缺失: {oid}")

    @staticmethod
    def _fsync_parent(d: Path) -> None:
        try:
            fd = os.open(d, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            # 某些文件系统不支持目录 fsync；不影响正确性。
            pass

    # ------------------------------------------------------------------
    # 快照 API（不可变根引用）
    # ------------------------------------------------------------------

    def create_snapshot(self, oids: Sequence[str]) -> str:
        """由一组已存在对象创建不可变快照（草稿态），返回快照 ID。

        草稿快照同样持有根引用：删除它（:meth:`delete_snapshot`）才
        撤销引用，因此数据库永远不会指向被 GC 删掉的文件。
        重复 OID 会被去重，顺序按传入顺序保留。
        """
        oids = self._dedupe_oids(oids)
        snap_id = uuid.uuid4().hex
        now = self.now()
        with self._lock, self._tx() as conn:
            for oid in oids:
                row = conn.execute(
                    "SELECT 1 FROM objects WHERE oid = ?", (oid,)
                ).fetchone()
                if row is None:
                    raise ObjectNotFound(oid)
            conn.execute(
                "INSERT INTO snapshots(id, published, created_at) "
                "VALUES (?, 0, ?)",
                (snap_id, now),
            )
            conn.executemany(
                "INSERT INTO snapshot_rows(snapshot_id, oid, seq) "
                "VALUES (?, ?, ?)",
                [(snap_id, oid, seq) for seq, oid in enumerate(oids)],
            )
        return snap_id

    def publish_snapshot(self, snapshot_id: str) -> bool:
        """发布快照。幂等：重复发布返回 ``False``，并发发布结果确定。

        快照不存在抛 :class:`SnapshotNotFound`。返回 ``True`` 表示
        本次调用完成了 草稿 -> 已发布 的状态翻转。
        """
        with self._lock:
            self._require_snapshot(snapshot_id)
            now = self.now()
            with self._tx() as conn:
                cur = conn.execute(
                    "UPDATE snapshots SET published = 1, published_at = ? "
                    "WHERE id = ? AND published = 0",
                    (now, snapshot_id),
                )
                changed = cur.rowcount == 1
        return changed

    def is_published(self, snapshot_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT published FROM snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
            if row is None:
                raise SnapshotNotFound(snapshot_id)
            return bool(row[0])

    def read_snapshot(self, snapshot_id: str) -> list[str]:
        """返回快照引用的对象 OID 列表（按创建时的顺序）。"""
        with self._lock:
            self._require_snapshot(snapshot_id)
            rows = self._conn.execute(
                "SELECT oid FROM snapshot_rows WHERE snapshot_id = ? "
                "ORDER BY seq",
                (snapshot_id,),
            ).fetchall()
        return [r[0] for r in rows]

    def list_snapshots(self, include_drafts: bool = True) -> list[str]:
        with self._lock:
            if include_drafts:
                rows = self._conn.execute(
                    "SELECT id FROM snapshots ORDER BY created_at, id"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id FROM snapshots WHERE published = 1 "
                    "ORDER BY created_at, id"
                ).fetchall()
        return [r[0] for r in rows]

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """删除快照即撤销其根引用（快照不可变，无内容修改接口）。

        幂等：快照不存在返回 ``False``；存在并删除返回 ``True``。
        对象文件本身由后续垃圾回收处理。
        """
        with self._lock, self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM snapshots WHERE id = ?", (snapshot_id,)
            )
            return cur.rowcount == 1

    def _require_snapshot(self, snapshot_id: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT id, published FROM snapshots WHERE id = ?", (snapshot_id,)
        ).fetchone()
        if row is None:
            raise SnapshotNotFound(snapshot_id)
        return row

    @staticmethod
    def _dedupe_oids(oids: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for oid in oids:
            _validate_oid(oid)
            if oid not in seen:
                seen.add(oid)
                result.append(oid)
        return result

    # ------------------------------------------------------------------
    # 导出租约 API（未到期租约是对象的根引用）
    # ------------------------------------------------------------------

    def add_lease(self, oid: str, ttl: float) -> str:
        """为对象创建一个 ``ttl`` 秒后到期的导出租约，返回租约 ID。

        租约在创建时刻即引用并保护对象。对象不存在抛
        :class:`ObjectNotFound`；``ttl`` 允许为 0 或负数（即已到期，
        下一次 GC 标记会清除）。
        """
        _validate_oid(oid)
        if ttl != ttl or ttl in (float("inf"), float("-inf")):
            raise ValueError("ttl 必须是有限数值")
        lease_id = uuid.uuid4().hex
        now = self.now()
        expires_at = now + ttl
        with self._lock, self._tx() as conn:
            row = conn.execute(
                "SELECT 1 FROM objects WHERE oid = ?", (oid,)
            ).fetchone()
            if row is None:
                raise ObjectNotFound(oid)
            conn.execute(
                "INSERT INTO export_leases(id, oid, created_at, expires_at) "
                "VALUES (?, ?, ?, ?)",
                (lease_id, oid, now, expires_at),
            )
        return lease_id

    def get_lease(self, lease_id: str) -> LeaseInfo:
        """查询租约。租约不存在（含已到期被 GC 清除）抛
        :class:`LeaseNotFound`。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT id, oid, expires_at FROM export_leases WHERE id = ?",
                (lease_id,),
            ).fetchone()
        if row is None:
            raise LeaseNotFound(lease_id)
        return LeaseInfo(id=row["id"], oid=row["oid"], expires_at=row["expires_at"])

    def release_lease(self, lease_id: str) -> bool:
        """提前释放租约（撤销根引用）。幂等，返回租约是否存在。"""
        with self._lock, self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM export_leases WHERE id = ?", (lease_id,)
            )
            return cur.rowcount == 1

    def list_leases(self) -> list[LeaseInfo]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, oid, expires_at FROM export_leases "
                "ORDER BY expires_at, id"
            ).fetchall()
        return [
            LeaseInfo(id=r["id"], oid=r["oid"], expires_at=r["expires_at"])
            for r in rows
        ]

    def _purge_expired_leases(self, conn: sqlite3.Connection, now: float) -> int:
        return conn.execute(
            "DELETE FROM export_leases WHERE expires_at <= ?", (now,)
        ).rowcount

    # ------------------------------------------------------------------
    # 垃圾回收
    # ------------------------------------------------------------------

    def active_gc_run(self) -> str | None:
        """返回未完成 GC 运行的 ID（中断后待恢复），无则 ``None``。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM gc_runs WHERE state != 'done'"
            ).fetchone()
        return row[0] if row else None

    def gc_collect(self) -> GCReport:
        """执行一次两阶段垃圾回收；线程内串行化，可安全重试。

        若存在上一进程/上一调用中断的未完成运行，则接管它并继续
        第二阶段（候选集合不重新标记，保证“重复回收”结果确定）。
        """
        # 同一进程内，GC 彼此串行，避免两个运行交错。
        with self._gc_lock:
            return self._gc_collect_locked()

    def _gc_collect_locked(self) -> GCReport:
        with self._lock:
            # 崩溃恢复先行：数据库引用的 graveyard 文件必须先还原。
            self._reconcile_graveyard()
            self._purge_tmp()

            now = self.now()
            run_row = self._conn.execute(
                "SELECT id FROM gc_runs WHERE state != 'done' LIMIT 1"
            ).fetchone()

            if run_row is None:
                run_id = uuid.uuid4().hex
                with self._tx() as conn:
                    conn.execute(
                        "INSERT INTO gc_runs(id, state, created_at) "
                        "VALUES (?, 'marking', ?)",
                        (run_id, now),
                    )
                    self._purge_expired_leases(conn, now)
                    # 阶段一：标记候选 = 标记时刻不可达的对象。
                    conn.execute(
                        _REACHABLE_CTE
                        + "INSERT INTO gc_candidates(run_id, oid) "
                        "SELECT :run_id, o.oid FROM objects o "
                        "WHERE o.oid NOT IN (SELECT oid FROM reachable) "
                        "ORDER BY o.oid",
                        {"now": now, "run_id": run_id},
                    )
                    conn.execute(
                        "UPDATE gc_runs SET state = 'sweeping' WHERE id = ?",
                        (run_id,),
                    )
                resumed = False
            else:
                run_id = run_row["id"]
                resumed = True

            # 重新从数据库读取冻结的候选（可能来自被接管的旧运行）。
            candidates = [
                r[0]
                for r in self._conn.execute(
                    "SELECT oid FROM gc_candidates WHERE run_id = ? ORDER BY oid",
                    (run_id,),
                ).fetchall()
            ]

        # ===== 阶段屏障（锁外）=====
        # 新运行的标记候选已提交持久化，但一个文件都还没删。
        # 此处新快照/新租约可以重新引用任意候选对象。
        if not resumed and self._on_after_mark is not None:
            self._on_after_mark(run_id, list(candidates))

        deleted: list[str] = []
        kept: list[str] = []
        deleted_now: str | None = None

        # ===== 阶段二：逐个复核后删除 =====
        for oid in candidates:
            action = "skip"
            with self._lock:
                now = self.now()
                state_row = self._conn.execute(
                    "SELECT state FROM gc_candidates WHERE run_id = ? AND oid = ?",
                    (run_id, oid),
                ).fetchone()
                if state_row is None or state_row["state"] != "pending":
                    action = "skip"  # 断点续跑时已处理过
                elif not self._object_path(oid).is_file():
                    # 数据库有行但文件缺失属于损坏。
                    raise CorruptionError(f"数据库引用的对象文件缺失: {oid}")
                else:
                    with self._tx() as conn:
                        # 复核前以当前时刻清理到期租约，使可达性判定确定。
                        self._purge_expired_leases(conn, now)
                        reachable = conn.execute(
                            _REACHABLE_CTE
                            + "SELECT 1 FROM reachable WHERE oid = :oid",
                            {"now": now, "oid": oid},
                        ).fetchone()
                        if reachable is not None:
                            # 两阶段之间被新快照/未到期租约重新引用：
                            # 删除必须放弃，候选标记为 kept，文件原样保留。
                            conn.execute(
                                "UPDATE gc_candidates SET state = 'kept' "
                                "WHERE run_id = ? AND oid = ?",
                                (run_id, oid),
                            )
                            action = "kept"
                        else:
                            # 复核仍不可达：在同一事务中删除元数据行。
                            conn.execute(
                                "DELETE FROM objects WHERE oid = ?", (oid,)
                            )
                            conn.execute(
                                "UPDATE gc_candidates SET state = 'deleted' "
                                "WHERE run_id = ? AND oid = ?",
                                (run_id, oid),
                            )
                            action = "delete_ready"

                    # 事务已提交、仍持锁：现在把文件移到 graveyard。
                    # put 的新行只可能在文件移动之后提交（同锁），因此
                    # 不可能把“数据库已引用的文件”移走。
                    if action == "delete_ready":
                        path = self._object_path(oid)
                        gy_dir = self.graveyard_dir / run_id
                        gy_dir.mkdir(parents=True, exist_ok=True)
                        os.replace(path, gy_dir / oid)
                        deleted.append(oid)
                        deleted_now = oid
                    elif action == "kept":
                        kept.append(oid)

            # 第二阶段中途屏障（锁外）：后续候选仍会被逐个复核。
            if action == "delete_ready" and self._on_after_delete is not None:
                self._on_after_delete(run_id, deleted_now)

        # 游离文件（有文件、无元数据）的处理放在所有候选复核之后。
        # 先在事务中把“done”与候选集合一并提交；提交后、持锁移动
        # 文件。崩溃恢复时：事务未提交则文件还在 objects/（由下次
        # GC 重新发现）；事务已提交则 graveyard 中的残留会在重开时
        # 按“数据库无引用”清除。
        orphans = 0
        with self._lock:
            now = self.now()
            with self._tx() as conn:
                self._purge_expired_leases(conn, now)
                orphan_paths: list[Path] = []
                for shard in sorted(self.objects_dir.iterdir()):
                    if not shard.is_dir() or not re.fullmatch(
                        r"[0-9a-f]{2}", shard.name
                    ):
                        continue
                    for p in sorted(shard.iterdir()):
                        if not p.is_file() or not OID_RE.match(p.name):
                            continue
                        row = conn.execute(
                            "SELECT 1 FROM objects WHERE oid = ?", (p.name,)
                        ).fetchone()
                        if row is None:
                            orphan_paths.append(p)
                conn.execute(
                    "UPDATE gc_runs SET state = 'done', finished_at = ? WHERE id = ?",
                    (now, run_id),
                )

            # 提交完成（同进程同锁，期间没有任何 put 能插入元数据）：
            # 移动游离文件到本次运行的 graveyard。
            gy_dir = self.graveyard_dir / run_id
            for p in orphan_paths:
                if not p.is_file():
                    continue
                # 双重确认：此时行仍不存在（锁保护，无并发写入）。
                row = self._conn.execute(
                    "SELECT 1 FROM objects WHERE oid = ?", (p.name,)
                ).fetchone()
                if row is not None:
                    continue
                gy_dir.mkdir(parents=True, exist_ok=True)
                os.replace(p, gy_dir / p.name)
                orphans += 1

            # 运行结束：graveyard 中所有文件（本运行删除的对象与游离
            # 文件）现在数据库都不再引用，物理删除是安全的。
            run_gy = self.graveyard_dir / run_id
            if run_gy.exists():
                shutil.rmtree(run_gy, ignore_errors=True)

            # 清理空分片目录。
            for shard in self.objects_dir.iterdir():
                if shard.is_dir() and not any(shard.iterdir()):
                    with contextlib.suppress(OSError):
                        shard.rmdir()

        return GCReport(
            run_id=run_id,
            resumed=resumed,
            candidates=len(candidates),
            deleted=len(deleted),
            kept=len(kept),
            orphans_removed=orphans,
        )

    # ------------------------------------------------------------------
    # 崩溃恢复
    # ------------------------------------------------------------------

    def _reconcile_graveyard(self) -> None:
        """重开仓库时调和 graveyard（必须在持有 ``_lock`` 时调用）。

        两阶段删除的崩溃点只有两个：

        * 事务提交前崩溃：元数据行仍在，文件可能已被移到 graveyard
          ——必须还原回 objects/，否则数据库会指向缺失文件。
        * 事务提交后、物理删除前崩溃：元数据行已删——直接删除残留文件。
        """
        if not self.graveyard_dir.exists():
            return
        for run_dir in sorted(self.graveyard_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            for p in sorted(run_dir.rglob("*")):
                if not p.is_file():
                    continue
                oid = p.name
                if not OID_RE.match(oid):
                    # 非对象文件直接清除。
                    with contextlib.suppress(OSError):
                        p.unlink()
                    continue
                row = self._conn.execute(
                    "SELECT 1 FROM objects WHERE oid = ?", (oid,)
                ).fetchone()
                if row is not None:
                    dest = self._object_path(oid)
                    if dest.exists():
                        # 数据库行在、objects 与 graveyard 各有一份：
                        # 内容寻址保证一致，删除 graveyard 副本。
                        p.unlink()
                    else:
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(p, dest)
                else:
                    # 数据库已无引用：残留可安全物理删除。
                    p.unlink()
            # 清空后移除目录树。
            shutil.rmtree(run_dir, ignore_errors=True)

    def _sweep_orphan_files(self) -> int:
        """删除 objects/ 下“有文件、无元数据”的游离对象。

        仓库为单进程设计：打开仓库时不存在并发写入方，游离文件只可能
        来自 put_object 在“文件就位、元数据提交”之前的崩溃——它从未被
        任何快照或租约引用，删除是安全的，且让重开后立即恢复一致状态。
        必须在持有 ``_lock`` 时调用。
        """
        removed = 0
        for shard in sorted(self.objects_dir.iterdir()):
            if not shard.is_dir() or not re.fullmatch(r"[0-9a-f]{2}", shard.name):
                continue
            for p in sorted(shard.iterdir()):
                if not p.is_file() or not OID_RE.match(p.name):
                    continue
                row = self._conn.execute(
                    "SELECT 1 FROM objects WHERE oid = ?", (p.name,)
                ).fetchone()
                if row is None:
                    p.unlink()
                    removed += 1
            if shard.is_dir() and not any(shard.iterdir()):
                with contextlib.suppress(OSError):
                    shard.rmdir()
        return removed


    def _purge_tmp(self) -> None:
        """清除上次进程崩溃残留的 put 暂存文件（从未被引用）。"""
        if not self.tmp_dir.exists():
            return
        for p in self.tmp_dir.iterdir():
            if p.is_file() and p.name.startswith("put-"):
                with contextlib.suppress(OSError):
                    p.unlink()

    # ------------------------------------------------------------------
    # 一致性自检（供测试与巡检使用）
    # ------------------------------------------------------------------

    def assert_consistent(self, *, gc_in_progress: bool = False) -> None:
        """核对数据库与文件目录的一致性，违反不变量时抛
        :class:`CorruptionError`。

        不变量：

        1. 数据库中每个对象在 ``objects/`` 下都有内容正确的文件；
        2. ``objects/`` 下的每个对象文件在数据库中都有对应行
           （``gc_in_progress=True`` 时，允许已从 objects/ 移出但
           尚未物理删除的文件短暂停留在 graveyard）；
        3. 快照行、租约引用的 OID 都存在；
        4. GC 未运行期间 graveyard 必须为空。

        无论是否处于 GC 中途，“数据库引用的文件必须存在”这一最关键
        不变量始终被检查。
        """
        with self._lock:
            db_oids = {
                r[0]
                for r in self._conn.execute("SELECT oid FROM objects").fetchall()
            }
            file_oids: set[str] = set()
            for shard in self.objects_dir.iterdir():
                if not shard.is_dir():
                    continue
                for p in shard.iterdir():
                    if p.is_file():
                        if not OID_RE.match(p.name):
                            raise CorruptionError(f"对象目录出现非法文件名: {p}")
                        file_oids.add(p.name)

            missing_files = db_oids - file_oids
            if missing_files:
                raise CorruptionError(
                    f"数据库引用但文件缺失: {sorted(missing_files)}"
                )
            extra_files = file_oids - db_oids
            if extra_files:
                raise CorruptionError(
                    f"文件存在但数据库无引用（GC 外不应出现）: {sorted(extra_files)}"
                )
            for oid in db_oids:
                data = self._object_path(oid).read_bytes()
                if hashlib.sha256(data).hexdigest() != oid:
                    raise CorruptionError(f"对象内容与散列不符: {oid}")

            for table, col in (("snapshot_rows", "oid"), ("export_leases", "oid")):
                bad = {
                    r[0]
                    for r in self._conn.execute(
                        f"SELECT DISTINCT {col} FROM {table} "
                        f"WHERE {col} NOT IN (SELECT oid FROM objects)"
                    ).fetchall()
                }
                if bad:
                    raise CorruptionError(f"{table} 引用了不存在的对象: {sorted(bad)}")

            if not gc_in_progress and self.graveyard_dir.exists():
                leftovers = [
                    p
                    for p in self.graveyard_dir.rglob("*")
                    if p.is_file()
                ]
                if leftovers:
                    raise CorruptionError(
                        f"仓库打开状态下 graveyard 非空: {leftovers}"
                    )
