"""仓库核心实现。

不变量（每个已提交状态、以及崩溃恢复后的重试都必须满足）：

1. ``objects`` 中的每一行在 ``objects/`` 目录下都有对应文件，且内容哈希等于 id。
2. 快照/租约引用的每个对象都存在（外键 ``ON DELETE RESTRICT`` 强制）。
3. 一个对象只有在“当前没有任何已发布快照或未到期租约引用它”时才会被删除。

垃圾回收分两阶段：

* ``gc_mark``：在单个事务里计算存活集（快照 ∪ 未到期租约），把其余对象
  作为候选写入 ``gc_candidates``，不删除任何文件。
* ``gc_sweep``：对每个候选在一个 **IMMEDIATE 写事务** 里重新复核。复核通过后，
  **先在该事务中删除元数据（objects 行、候选标记为 reclaimed）并提交，随后才
  删除文件**。这样崩溃只会留下“数据库不再引用、但文件尚未删除”的待清理文件
  （由 gc_candidates 记录、重试时补删），绝不会出现“数据库指向已删除文件”，
  也绝不会删掉数据库仍引用的文件。

两阶段之间（或扫描其他候选时）获得新引用的候选转为 ``rescued``；
在删除窗口并发到达的发布会因行已不可见而确定性地收到对象不存在错误。
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from types import TracebackType
from typing import Iterator

from .clocks import Clock, SystemClock, to_epoch_micros
from .exceptions import (
    DuplicateGCRunError,
    InternalConsistencyError,
    InvalidObjectIdError,
    LeaseNotFoundError,
    ObjectNotFoundError,
    SnapshotNotFoundError,
    UnknownGCRunError,
)
from .hooks import GCHooks, NullHooks

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")
_OID_RE = re.compile(r"^[0-9a-f]{64}$")
_BUSY_TIMEOUT_SECONDS = 30.0


def _validate_oid(oid: str) -> str:
    if not isinstance(oid, str) or not _OID_RE.fullmatch(oid):
        raise InvalidObjectIdError(f"非法对象 id: {oid!r}（应为 64 位小写十六进制）")
    return oid


def _normalize_oid_list(object_ids) -> list[str]:
    """校验、去重并排序一组对象 id（快照/租约是集合而非序列）。"""

    if isinstance(object_ids, (str, bytes)):
        raise TypeError("object_ids 必须是字符串 id 的可迭代对象，而不是单个字符串")
    unique = {_validate_oid(oid) for oid in object_ids}
    return sorted(unique)


def _snapshot_content_id(object_ids: list[str]) -> str:
    """快照 id 由其成员集合确定性派生：相同集合得到相同快照。"""

    h = hashlib.sha256()
    for oid in object_ids:
        h.update(b"snap\x00")
        h.update(bytes.fromhex(oid))
        h.update(b"\x00")
    return h.hexdigest()


@dataclass(frozen=True)
class SnapshotInfo:
    id: str
    object_ids: list[str] = field(default_factory=list)
    created_at_epoch_micros: int = 0


@dataclass(frozen=True)
class LeaseInfo:
    id: str
    object_ids: list[str] = field(default_factory=list)
    created_at_epoch_micros: int = 0
    expires_at_epoch_micros: int = 0

    def is_expired(self, now_epoch_micros: int) -> bool:
        return self.expires_at_epoch_micros <= now_epoch_micros


@dataclass(frozen=True)
class GCMarkResult:
    run_id: str
    candidate_ids: list[str]


@dataclass(frozen=True)
class GCSweepResult:
    run_id: str
    reclaimed: list[str]
    rescued: list[str]


class Repository:
    """本地内容寻址对象仓库。

    :param root: 仓库根目录（不存在会自动创建），元数据在 ``root/meta.sqlite3``，
        对象文件在 ``root/objects/xx/yyyy...``。
    :param clock: 时间源，测试可注入可控时钟。
    :param hooks: 回收阶段钩子（屏障/崩溃注入用），不影响正确性。
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        clock: Clock | None = None,
        hooks: GCHooks | None = None,
    ) -> None:
        self.root = Path(root)
        self.objects_dir = self.root / "objects"
        self.db_path = self.root / "meta.sqlite3"
        self.clock: Clock = clock or SystemClock()
        self.hooks: GCHooks = hooks or NullHooks()
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_db()

    # ------------------------------------------------------------------ 基础

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None：完全由我们自己控制 BEGIN/COMMIT，
        # 从而保证“复核+删文件+删行”处于同一个 IMMEDIATE 事务。
        conn = sqlite3.connect(
            self.db_path,
            timeout=_BUSY_TIMEOUT_SECONDS,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {int(_BUSY_TIMEOUT_SECONDS * 1000)}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _initialize_db(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
        finally:
            conn.close()

    @contextlib.contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(Exception):
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _now(self) -> int:
        return to_epoch_micros(self.clock.now())

    def _path_for(self, object_id: str) -> Path:
        return self.objects_dir / object_id[:2] / object_id[2:]

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    @staticmethod
    def _hash_file(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def _write_object_file(self, object_id: str, data: bytes) -> None:
        path = self._path_for(object_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, path)
            self._fsync_dir(path.parent)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise

    # ------------------------------------------------------------- 对象操作

    def put(self, data: bytes) -> str:
        """写入一个对象，返回其内容 id（SHA-256）。重复写入是幂等的。"""

        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data 必须是 bytes")
        data = bytes(data)
        object_id = hashlib.sha256(data).hexdigest()
        path = self._path_for(object_id)
        now = self._now()
        with self._tx() as conn:
            exists = conn.execute(
                "SELECT 1 FROM objects WHERE id = ?", (object_id,)
            ).fetchone()
            if exists is None:
                if not path.exists():
                    self._write_object_file(object_id, data)
                elif self._hash_file(path) != object_id:
                    # 文件存在但内容与内容 id 不符：外部损坏，绝不覆盖信任。
                    raise InternalConsistencyError(
                        f"对象文件内容与 id 不符: {object_id}"
                    )
                conn.execute(
                    "INSERT INTO objects(id, size, created_at) VALUES (?, ?, ?)",
                    (object_id, len(data), now),
                )
        return object_id

    def get(self, object_id: str) -> bytes:
        """读取对象内容；对象不存在抛 :class:`ObjectNotFoundError`。"""

        _validate_oid(object_id)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT size FROM objects WHERE id = ?", (object_id,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise ObjectNotFoundError(object_id)
        path = self._path_for(object_id)
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            raise InternalConsistencyError(
                f"数据库引用了缺失的对象文件: {object_id}"
            ) from None
        if len(data) != row["size"] or hashlib.sha256(data).hexdigest() != object_id:
            raise InternalConsistencyError(f"对象文件损坏: {object_id}")
        return data

    def exists(self, object_id: str) -> bool:
        _validate_oid(object_id)
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM objects WHERE id = ?", (object_id,)
            ).fetchone()
        finally:
            conn.close()
        return row is not None

    def list_objects(self) -> list[str]:
        conn = self._connect()
        try:
            return [
                r[0]
                for r in conn.execute("SELECT id FROM objects ORDER BY id").fetchall()
            ]
        finally:
            conn.close()

    # ------------------------------------------------------------- 快照操作

    def publish_snapshot(self, object_ids, *, snapshot_id: str | None = None) -> str:
        """发布不可变快照，使其引用的对象成为存活根。

        相同成员集合得到相同的快照 id，重复发布幂等。引用不存在的对象会抛
        :class:`ObjectNotFoundError`，且不写入任何内容。
        """

        oids = _normalize_oid_list(object_ids)
        sid = snapshot_id or _snapshot_content_id(oids)
        now = self._now()
        with self._tx() as conn:
            row = conn.execute(
                "SELECT 1 FROM snapshots WHERE id = ?", (sid,)
            ).fetchone()
            if row is not None:
                return sid
            missing = self._missing_object_ids(conn, oids)
            if missing:
                raise ObjectNotFoundError(object_ids=missing)
            for oid in oids:
                if not self._path_for(oid).exists():
                    # 行存在但文件缺失属于损坏；添加根引用必须失败而不是制造悬空引用。
                    raise InternalConsistencyError(
                        f"数据库引用了缺失的对象文件: {oid}"
                    )
            conn.execute(
                "INSERT INTO snapshots(id, created_at) VALUES (?, ?)", (sid, now)
            )
            conn.executemany(
                "INSERT INTO snapshot_objects(snapshot_id, object_id) VALUES (?, ?)",
                [(sid, oid) for oid in oids],
            )
        return sid

    def delete_snapshot(self, snapshot_id: str) -> None:
        """删除快照（仅撤销根引用，不删除任何对象）。

        快照不存在时抛 :class:`SnapshotNotFoundError`，结果确定。
        """

        with self._tx() as conn:
            cur = conn.execute(
                "DELETE FROM snapshots WHERE id = ?", (snapshot_id,)
            )
            if cur.rowcount == 0:
                raise SnapshotNotFoundError(f"快照不存在: {snapshot_id}")

    def get_snapshot(self, snapshot_id: str) -> SnapshotInfo:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT id, created_at FROM snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
            if row is None:
                raise SnapshotNotFoundError(f"快照不存在: {snapshot_id}")
            members = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM snapshot_objects WHERE snapshot_id = ?"
                    " ORDER BY object_id",
                    (snapshot_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
        return SnapshotInfo(
            id=row["id"],
            object_ids=members,
            created_at_epoch_micros=row["created_at"],
        )

    def list_snapshots(self) -> list[str]:
        conn = self._connect()
        try:
            return [
                r[0]
                for r in conn.execute("SELECT id FROM snapshots ORDER BY id").fetchall()
            ]
        finally:
            conn.close()

    # ------------------------------------------------------------- 租约操作

    def grant_lease(
        self,
        object_ids,
        *,
        ttl: timedelta | None = None,
        lease_id: str | None = None,
    ) -> str:
        """为一组对象授予带到期时间的导出租约；未到期租约保住其对象。

        ``ttl`` 默认为一小时。租约在 ``now + ttl`` 时刻到期（到期时刻本身不再
        保活）。引用不存在的对象抛 :class:`ObjectNotFoundError`，不写入任何内容。
        """

        oids = _normalize_oid_list(object_ids)
        ttl = timedelta(hours=1) if ttl is None else ttl
        lid = lease_id or uuid.uuid4().hex
        now = self._now()
        expires_at = now + int(ttl.total_seconds() * 1_000_000)
        with self._tx() as conn:
            dup = conn.execute(
                "SELECT 1 FROM leases WHERE id = ?", (lid,)
            ).fetchone()
            if dup is not None:
                raise DuplicateGCRunError(f"租约 id 已存在: {lid}")
            missing = self._missing_object_ids(conn, oids)
            if missing:
                raise ObjectNotFoundError(object_ids=missing)
            for oid in oids:
                if not self._path_for(oid).exists():
                    raise InternalConsistencyError(
                        f"数据库引用了缺失的对象文件: {oid}"
                    )
            conn.execute(
                "INSERT INTO leases(id, created_at, expires_at) VALUES (?, ?, ?)",
                (lid, now, expires_at),
            )
            conn.executemany(
                "INSERT INTO lease_objects(lease_id, object_id) VALUES (?, ?)",
                [(lid, oid) for oid in oids],
            )
        return lid

    def revoke_lease(self, lease_id: str) -> bool:
        """提前撤销租约。返回是否真的撤销了一个存在的租约（重复撤销返回 False）。"""

        with self._tx() as conn:
            cur = conn.execute("DELETE FROM leases WHERE id = ?", (lease_id,))
            return cur.rowcount > 0

    def get_lease(self, lease_id: str) -> LeaseInfo:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT id, created_at, expires_at FROM leases WHERE id = ?",
                (lease_id,),
            ).fetchone()
            if row is None:
                raise LeaseNotFoundError(f"租约不存在: {lease_id}")
            members = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM lease_objects WHERE lease_id = ?"
                    " ORDER BY object_id",
                    (lease_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
        return LeaseInfo(
            id=row["id"],
            object_ids=members,
            created_at_epoch_micros=row["created_at"],
            expires_at_epoch_micros=row["expires_at"],
        )

    def list_leases(self, *, include_expired: bool = True) -> list[str]:
        sql = "SELECT id FROM leases"
        params: tuple = ()
        if not include_expired:
            sql += " WHERE expires_at > ?"
            params = (self._now(),)
        sql += " ORDER BY id"
        conn = self._connect()
        try:
            return [r[0] for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    # --------------------------------------------------------------- 垃圾回收

    def gc_mark(self, *, run_id: str | None = None) -> GCMarkResult:
        """第一阶段：标记候选。只在数据库中记录候选，不触碰任何对象文件。

        标记开始前会先清理已到期租约（与标记在同一事务中，时钟即时取值）。
        """

        run_id = run_id or uuid.uuid4().hex
        self.hooks.mark_begin(run_id)
        now = self._now()
        with self._tx() as conn:
            dup = conn.execute(
                "SELECT 1 FROM gc_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if dup is not None:
                raise DuplicateGCRunError(f"回收运行 id 已存在: {run_id}")
            self._prune_expired_leases(conn, now)
            all_objects = {
                r[0] for r in conn.execute("SELECT id FROM objects").fetchall()
            }
            live = self._live_object_ids(conn, now)
            candidates = sorted(all_objects - live)
            conn.execute(
                "INSERT INTO gc_runs(id, started_at, status) VALUES (?, ?, 'marked')",
                (run_id, now),
            )
            conn.executemany(
                "INSERT INTO gc_candidates(run_id, object_id, state, marked_at)"
                " VALUES (?, ?, 'candidate', ?)",
                [(run_id, oid, now) for oid in candidates],
            )
            # 仍在未提交的标记事务内：此处崩溃注入（如 SIGKILL）会让整笔
            # 标记随事务回滚，数据库回到标记前状态。
            self.hooks.mark_before_commit(run_id, candidates)
        self.hooks.mark_end(run_id, candidates)
        return GCMarkResult(run_id=run_id, candidate_ids=candidates)

    def gc_sweep(self, run_id: str) -> GCSweepResult:
        """第二阶段：逐个复核候选并删除确认无引用者。

        复核依据的“当前”语义是**标记时刻**：标记时仍有效的租约在复核时继续
        有效（租约可以在窗口内到期，但这属于下一次回收的处理范围），从而保证
        回收绝不会删除“标记时仍被根引用”的对象。快照引用没有时间限制，始终保活。

        每个候选在独立的 IMMEDIATE 写事务中完成复核与元数据删除。
        两阶段之间获得新快照/租约引用的候选转为 ``rescued``。
        进程中断后重复调用本方法是安全的幂等重试。
        """

        conn = self._connect()
        try:
            run = conn.execute(
                "SELECT status, started_at FROM gc_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise UnknownGCRunError(f"回收运行不存在: {run_id}")
            mark_time = run["started_at"]
            marked = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM gc_candidates WHERE run_id = ?"
                    " ORDER BY object_id",
                    (run_id,),
                ).fetchall()
            ]
        finally:
            conn.close()

        self.hooks.sweep_begin(run_id, marked)

        for oid in marked:
            self._sweep_one(run_id, oid, mark_time)

        finish_now = self._now()
        with self._tx() as conn:
            conn.execute(
                "UPDATE gc_runs SET status = 'swept', finished_at = ? WHERE id = ?",
                (finish_now, run_id),
            )
            reclaimed = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM gc_candidates"
                    " WHERE run_id = ? AND state = 'reclaimed' ORDER BY object_id",
                    (run_id,),
                ).fetchall()
            ]
            rescued = [
                r[0]
                for r in conn.execute(
                    "SELECT object_id FROM gc_candidates"
                    " WHERE run_id = ? AND state = 'rescued' ORDER BY object_id",
                    (run_id,),
                ).fetchall()
            ]
        return GCSweepResult(run_id=run_id, reclaimed=reclaimed, rescued=rescued)

    def gc(self, *, run_id: str | None = None) -> GCSweepResult:
        """便捷方法：顺序执行标记与复核，返回复核结果。"""

        mark = self.gc_mark(run_id=run_id)
        return self.gc_sweep(mark.run_id)

    def _sweep_one(self, run_id: str, object_id: str, mark_time: int) -> None:
        """复核并处理一个候选。安全重试：任何中途崩溃都可重入。

        提交状态下删除顺序严格为「先删元数据并提交，再删文件」：

        * 崩溃在提交之前：事务整体回滚，文件、行、候选都保持原状，重试即可；
        * 崩溃在提交之后、删文件之前：行与引用已不存在，只剩一个由 reclaimed
          候选记录跟踪的待清理文件，重试时补删，任何 API 都无法再引用它。
        """

        reclaimed_at = self._now()
        pending_file = False
        fire_after_hook = False
        with self._tx() as conn:
            candidate = conn.execute(
                "SELECT state FROM gc_candidates WHERE run_id = ? AND object_id = ?",
                (run_id, object_id),
            ).fetchone()
            if candidate is None:
                return
            if candidate["state"] == "reclaimed":
                # 上一轮元数据已提交、文件可能还没删（或已删）。
                row_exists = conn.execute(
                    "SELECT 1 FROM objects WHERE id = ?", (object_id,)
                ).fetchone()
                if row_exists is not None:
                    # 正常路径不可能出现：reclaimed 的对象行不应存在。
                    raise InternalConsistencyError(
                        f"reclaimed 候选仍有对象行: {object_id}"
                    )
                pending_file = self._path_for(object_id).exists()
                fire_after_hook = pending_file  # 重试补删完成后视为一次回收完成
            elif candidate["state"] == "candidate":
                obj_row = conn.execute(
                    "SELECT 1 FROM objects WHERE id = ?", (object_id,)
                ).fetchone()
                if obj_row is None:
                    # 行已不在：直接落审计状态，文件若存在则在事务外补删。
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'reclaimed',"
                        " reclaimed_at = ? WHERE run_id = ? AND object_id = ?",
                        (reclaimed_at, run_id, object_id),
                    )
                    pending_file = self._path_for(object_id).exists()
                    fire_after_hook = True
                elif self._is_live(conn, object_id, mark_time):
                    # 被快照引用（始终保活），或被在标记时刻仍未到期的租约引用，
                    # 又或两阶段之间/本次扫描期间获得的新根引用：一律救回。
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'rescued' WHERE run_id = ?"
                        " AND object_id = ?",
                        (run_id, object_id),
                    )
                else:
                    # 复核通过：钩子在写锁持有期间触发，因此并发发布会阻塞到
                    # 「行删除 + reclaimed 落库」提交完成；提交后该对象对任何
                    # 新事务都不可见，发布确定性地收到对象不存在错误。
                    self.hooks.before_reclaim(run_id, object_id)
                    # 外键 RESTRICT 是最后一道闸：若存在引用，提交直接失败。
                    conn.execute("DELETE FROM objects WHERE id = ?", (object_id,))
                    conn.execute(
                        "UPDATE gc_candidates SET state = 'reclaimed',"
                        " reclaimed_at = ? WHERE run_id = ? AND object_id = ?",
                        (reclaimed_at, run_id, object_id),
                    )
                    pending_file = self._path_for(object_id).exists()
                    fire_after_hook = True
            # rescued 候选：什么都不做。

        # 事务已提交（或候选已被救回）；现在才删除不再被任何元数据引用的文件。
        # 崩溃在这里只会留下待清理文件，重试本方法时按 reclaimed 分支补删。
        if pending_file:
            path = self._path_for(object_id)
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
        if fire_after_hook:
            self.hooks.after_reclaim(run_id, object_id)

    # --------------------------------------------------------------- 内部查询

    @staticmethod
    def _missing_object_ids(conn: sqlite3.Connection, oids: list[str]) -> list[str]:
        if not oids:
            return []
        placeholders = ",".join("?" * len(oids))
        present = {
            r[0]
            for r in conn.execute(
                f"SELECT id FROM objects WHERE id IN ({placeholders})", oids
            ).fetchall()
        }
        return [oid for oid in oids if oid not in present]

    @staticmethod
    def _prune_expired_leases(conn: sqlite3.Connection, now: int) -> None:
        # 级联删除 lease_objects；必须在 IMMEDIATE 事务内调用。
        conn.execute("DELETE FROM leases WHERE expires_at <= ?", (now,))

    @staticmethod
    def _is_live(conn: sqlite3.Connection, object_id: str, now: int) -> bool:
        row = conn.execute(
            """
            SELECT 1 FROM snapshot_objects WHERE object_id = ?
            UNION ALL
            SELECT 1 FROM lease_objects lo
            JOIN leases l ON l.id = lo.lease_id
            WHERE lo.object_id = ? AND l.expires_at > ?
            LIMIT 1
            """,
            (object_id, object_id, now),
        ).fetchone()
        return row is not None

    @staticmethod
    def _live_object_ids(conn: sqlite3.Connection, now: int) -> set[str]:
        live = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT object_id FROM snapshot_objects"
            ).fetchall()
        }
        live.update(
            r[0]
            for r in conn.execute(
                """
                SELECT DISTINCT lo.object_id
                FROM lease_objects lo
                JOIN leases l ON l.id = lo.lease_id
                WHERE l.expires_at > ?
                """,
                (now,),
            ).fetchall()
        )
        return live

    # ------------------------------------------------------------- 资源清理

    def close(self) -> None:
        """目前所有连接均为短连接，此方法仅为调用方提供对称的生命周期接口。"""

    def __enter__(self) -> "Repository":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
