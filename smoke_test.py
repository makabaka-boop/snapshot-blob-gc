"""零依赖冒烟测试：在没有 pytest 的环境中验证核心行为与不变量。

与 tests/ 中的 pytest 用例覆盖同一组场景，但只使用断言。
正式验收仍以 pytest（Docker verify 服务）为准。
"""

import hashlib
import os
import shutil
import sqlite3
import tempfile
import threading
import traceback
from pathlib import Path

from local_repo import (
    CorruptionError,
    LeaseNotFound,
    ObjectNotFound,
    ObjectRepository,
    SnapshotNotFound,
)

PASS = 0
FAIL = 0


def run(name, fn):
    global PASS, FAIL
    d = Path(tempfile.mkdtemp())
    try:
        fn(d)
        PASS += 1
        print(f"  ok   {name}")
    except Exception:
        FAIL += 1
        print(f"  FAIL {name}")
        traceback.print_exc()
    finally:
        shutil.rmtree(d, ignore_errors=True)


class Clock:
    def __init__(self):
        self.t = 1000.0
        self.l = threading.Lock()

    def __call__(self):
        with self.l:
            return self.t

    def advance(self, s):
        with self.l:
            self.t += s


def H(b):
    return hashlib.sha256(b).hexdigest()


# ---- 基础 ----------------------------------------------------------------

def t_put_get(d):
    r = ObjectRepository(d)
    a = r.put_object(b"hello")
    assert a == H(b"hello")
    assert r.get_object(a) == b"hello"
    assert r.put_object(b"hello") == a
    assert r.list_objects() == [a]
    try:
        r.get_object(H(b"x"))
        assert False
    except ObjectNotFound:
        pass
    r.assert_consistent()
    r.close()


def t_snapshot(d):
    r = ObjectRepository(d)
    a = r.put_object(b"a")
    s = r.create_snapshot([a, a])
    assert r.read_snapshot(s) == [a]
    assert not r.is_published(s)
    assert r.publish_snapshot(s) is True
    assert r.publish_snapshot(s) is False
    assert r.delete_snapshot(s) is True
    assert r.delete_snapshot(s) is False
    assert r.get_object(a) == b"a"  # 删除快照不删文件
    try:
        r.read_snapshot(s)
        assert False
    except SnapshotNotFound:
        pass
    r.close()


def t_leases(d):
    c = Clock()
    r = ObjectRepository(d, clock=c)
    a = r.put_object(b"a")
    l = r.add_lease(a, 100)
    assert r.get_lease(l).is_live_at(c())
    c.advance(100)
    assert not r.get_lease(l).is_live
    assert r.release_lease(l) is True
    assert r.release_lease(l) is False
    try:
        r.get_lease(l)
        assert False
    except LeaseNotFound:
        pass
    r.close()


# ---- GC ------------------------------------------------------------------

def t_gc_basic(d):
    r = ObjectRepository(d)
    keep = r.put_object(b"keep")
    junk = r.put_object(b"junk")
    s = r.create_snapshot([keep])
    r.publish_snapshot(s)
    rep = r.gc_collect()
    assert rep.deleted == 1 and rep.kept == 0
    assert set(r.list_objects()) == {keep}
    assert not r.has_object(junk)
    rep2 = r.gc_collect()
    assert rep2.run_id != rep.run_id and rep2.deleted == 0
    r.close()


def t_gc_draft_protects(d):
    r = ObjectRepository(d)
    a = r.put_object(b"d")
    r.create_snapshot([a])
    assert r.gc_collect().deleted == 0
    assert r.get_object(a) == b"d"
    r.close()


def t_gc_lease_expiry(d):
    c = Clock()
    r = ObjectRepository(d, clock=c)
    a = r.put_object(b"e")
    r.add_lease(a, 10)
    assert r.gc_collect().deleted == 0
    c.advance(10)
    assert r.gc_collect().deleted == 1
    r.close()


def t_gc_rescue_snapshot(d):
    r = ObjectRepository(d)
    a = r.put_object(b"rescue")

    def after_mark(run_id, cands):
        assert cands == [a]
        s = r.create_snapshot([a])
        r.publish_snapshot(s)

    r._on_after_mark = after_mark
    rep = r.gc_collect()
    assert rep.candidates == 1 and rep.kept == 1 and rep.deleted == 0
    assert r.get_object(a) == b"rescue"
    r.close()


def t_gc_rescue_lease(d):
    c = Clock()
    r = ObjectRepository(d, clock=c)
    a = r.put_object(b"rescue2")
    r._on_after_mark = lambda rid, cs: r.add_lease(a, 100)
    rep = r.gc_collect()
    assert rep.kept == 1 and rep.deleted == 0
    r.close()


def t_gc_mid_sweep_lease(d):
    c = Clock()
    r = ObjectRepository(d, clock=c)
    a, b, cc = r.put_object(b"aaaa"), r.put_object(b"bbbb"), r.put_object(b"cccc")
    fires = []

    def after_delete(rid, oid):
        fires.append(oid)
        r.add_lease(cc, 100)

    r._on_after_delete = after_delete
    rep = r.gc_collect()
    assert rep.deleted == 2 and rep.kept == 1
    assert r.has_object(cc) and not r.has_object(a) and not r.has_object(b)
    assert len(fires) == 2
    r.close()


def t_threaded_publish(d):
    c = Clock()
    r = ObjectRepository(d, clock=c)
    a = r.put_object(b"x")
    snap = r.create_snapshot([a])
    r.delete_snapshot(snap)
    barrier, proceed, outcome = threading.Event(), threading.Event(), {}

    def pub():
        barrier.wait(5)
        ns = r.create_snapshot([a])
        outcome["p"] = r.publish_snapshot(ns)
        proceed.set()

    def after_mark(rid, cs):
        barrier.set()
        proceed.wait(5)

    t = threading.Thread(target=pub)
    t.start()
    r._on_after_mark = after_mark
    rep = r.gc_collect()
    t.join(5)
    assert outcome["p"] is True
    assert rep.kept == 1 and rep.deleted == 0
    assert r.has_object(a)
    r.assert_consistent()
    r.close()


# ---- 崩溃恢复 ------------------------------------------------------------

def t_crash_after_mark(d):
    c = Clock()
    r = ObjectRepository(d, clock=c)
    a = r.put_object(b"o")
    rid_holder = {}

    def after_mark(run_id, cands):
        rid_holder["id"] = run_id
        r.close()
        raise RuntimeError("crash")

    r._on_after_mark = after_mark
    try:
        r.gc_collect()
    except RuntimeError:
        pass
    r2 = ObjectRepository(d, clock=c)
    assert r2.active_gc_run() == rid_holder["id"]
    r2.assert_consistent()
    rep = r2.gc_collect()
    assert rep.resumed and rep.deleted == 1
    assert not r2.has_object(a)
    r2.assert_consistent()
    r2.close()


def t_crash_precommit_restore(d):
    r = ObjectRepository(d)
    a = r.put_object(b"p")
    snap = r.create_snapshot([a])
    run_id = "rr1"
    conn = sqlite3.connect(d / "repo.sqlite3", isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("INSERT INTO gc_runs(id, state, created_at) VALUES (?, 'sweeping', 1)",
                 (run_id,))
    conn.execute("DELETE FROM snapshots WHERE id=?", (snap,))
    conn.execute("INSERT INTO gc_candidates(run_id, oid) VALUES (?,?)", (run_id, a))
    conn.close()
    gy = d / "graveyard" / run_id
    gy.mkdir(parents=True)
    (d / "objects" / a[:2] / a).replace(gy / a)
    r.close()

    r2 = ObjectRepository(d)
    assert r2.has_object(a) and r2.get_object(a) == b"p"
    r2.assert_consistent()
    rep = r2.gc_collect()
    assert rep.resumed and rep.deleted == 1
    r2.close()


def t_crash_postcommit_cleanup(d):
    r = ObjectRepository(d)
    a = r.put_object(b"x")
    run_id = "rr2"
    conn = sqlite3.connect(d / "repo.sqlite3", isolation_level=None)
    conn.execute("INSERT INTO gc_runs(id, state, created_at) VALUES (?, 'sweeping', 1)",
                 (run_id,))
    conn.execute("DELETE FROM objects WHERE oid=?", (a,))
    conn.execute("INSERT INTO gc_candidates(run_id, oid, state) VALUES (?,?,'deleted')",
                 (run_id, a))
    conn.close()
    gy = d / "graveyard" / run_id
    gy.mkdir(parents=True)
    (d / "objects" / a[:2] / a).replace(gy / a)
    r.close()

    r2 = ObjectRepository(d)
    assert not r2.has_object(a)
    assert not (d / "graveyard" / run_id).exists()
    rep = r2.gc_collect()
    assert rep.resumed and rep.deleted == 0
    r2.assert_consistent()
    r2.close()


def t_put_orphan_cleanup_on_open(d):
    r = ObjectRepository(d)
    data = b"nocommit"
    oid = H(data)
    shard = d / "objects" / oid[:2]
    shard.mkdir(parents=True)
    (shard / oid).write_bytes(data)
    r.close()
    r2 = ObjectRepository(d)
    assert not r2.has_object(oid)
    r2.assert_consistent()
    r2.close()


def t_external_corruption(d):
    r = ObjectRepository(d)
    a = r.put_object(b"t")
    r.create_snapshot([a])
    (d / "objects" / a[:2] / a).unlink()
    try:
        r.assert_consistent()
        assert False
    except CorruptionError:
        pass
    r.close()


def t_orphan_sweep(d):
    r = ObjectRepository(d)
    g = H(b"ghost")
    shard = d / "objects" / g[:2]
    shard.mkdir(parents=True)
    (shard / g).write_bytes(b"ghost")
    kept = r.put_object(b"real")
    s = r.create_snapshot([kept])
    r.publish_snapshot(s)
    rep = r.gc_collect()
    assert rep.orphans_removed == 1 and rep.deleted == 0
    assert r.has_object(kept)
    r.assert_consistent()
    assert os.listdir(d / "graveyard") == []
    r.close()


def main():
    tests = [
        ("put/get/content-address/idempotent", t_put_get),
        ("snapshot publish/delete semantics", t_snapshot),
        ("lease lifecycle", t_leases),
        ("gc basic + second run", t_gc_basic),
        ("draft snapshot protects", t_gc_draft_protects),
        ("lease expiry boundary", t_gc_lease_expiry),
        ("rescue by snapshot at barrier", t_gc_rescue_snapshot),
        ("rescue by lease at barrier", t_gc_rescue_lease),
        ("lease during mid-sweep saves later candidate", t_gc_mid_sweep_lease),
        ("threaded concurrent publish", t_threaded_publish),
        ("crash after mark -> resume", t_crash_after_mark),
        ("crash pre-commit -> restore", t_crash_precommit_restore),
        ("crash post-commit -> cleanup", t_crash_postcommit_cleanup),
        ("put orphan cleaned at reopen", t_put_orphan_cleanup_on_open),
        ("external corruption detected", t_external_corruption),
        ("orphan file sweep", t_orphan_sweep),
    ]
    print(f"Running {len(tests)} smoke checks...")
    for name, fn in tests:
        run(name, fn)
    print(f"\n{PASS} passed, {FAIL} failed")
    raise SystemExit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
