"""阶段屏障测试：两阶段之间的重新引用必须令删除放弃。

这些测试不依赖真实线程竞态（竞态无法稳定复现），而是用
``on_after_mark`` / ``on_after_delete`` 屏障在确定的时刻注入动作，
并在每个动作之后核对数据库与文件目录的一致性。
"""

import hashlib

from local_repo import ObjectNotFound


def oid(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_new_snapshot_between_mark_and_sweep_saves_object(make_repo):
    repo = make_repo()
    a = repo.put_object(b"rescued-by-snapshot")

    def after_mark(run_id, candidates):
        # 标记后、任何删除前：新快照重新引用候选对象。
        assert candidates == [a]
        snap = repo.create_snapshot([a])
        repo.publish_snapshot(snap)
        repo.assert_consistent()  # 屏障处数据库/文件仍完全一致

    repo._on_after_mark = after_mark
    report = repo.gc_collect()
    repo._on_after_mark = None  # 屏障只对本次运行生效
    assert report.candidates == 1
    assert report.kept == 1
    assert report.deleted == 0
    assert repo.has_object(a)
    assert repo.get_object(a) == b"rescued-by-snapshot"
    repo.assert_consistent()

    # 快照仍在时再次回收：依然不能删。
    assert repo.gc_collect().deleted == 0
    # 删除快照后回收：对象才被删除。
    repo.delete_snapshot(repo.list_snapshots()[0])
    assert repo.gc_collect().deleted == 1
    assert not repo.has_object(a)


def test_new_lease_between_phases_saves_object(make_repo, clock):
    repo = make_repo()
    a = repo.put_object(b"rescued-by-lease")

    def after_mark(run_id, candidates):
        repo.add_lease(a, ttl=100)  # 未到期租约重新引用
        repo.assert_consistent()

    repo._on_after_mark = after_mark
    report = repo.gc_collect()
    repo._on_after_mark = None
    assert report.deleted == 0
    assert report.kept == 1
    assert repo.get_object(a) == b"rescued-by-lease"


def test_lease_added_after_first_delete_still_saves_later_candidates(make_repo):
    """第二阶段是逐个复核的：屏障后新租约只救得了后续候选。"""
    repo = make_repo()
    a = repo.put_object(b"aaaa")
    b = repo.put_object(b"bbbb")
    c = repo.put_object(b"cccc")
    c_oid = c
    fired = {"n": 0}

    def after_delete(run_id, deleted_oid):
        fired["n"] += 1
        repo.assert_consistent(gc_in_progress=True)
        # 第一个被删的对象之后给 c 加租约（b 继续删除，c 必须保留）。
        repo.add_lease(c_oid, ttl=100)

    repo._on_after_delete = after_delete
    report = repo.gc_collect()
    repo._on_after_delete = None
    assert fired["n"] == 2  # a、b 被删；c 被救，所以回调只触发两次
    assert not repo.has_object(a)
    assert not repo.has_object(b)
    assert repo.has_object(c)
    assert repo.get_object(c) == b"cccc"
    assert report.deleted == 2
    assert report.kept == 1

    # 租约到期后再回收：c 随之被删除（候选集合在新运行中重新计算）。
    repo._clock.advance(100)
    assert repo.gc_collect().deleted == 1


def test_rescue_then_release_allows_next_collection(make_repo, clock):
    repo = make_repo()
    a = repo.put_object(b"flipflop")

    def after_mark(run_id, candidates):
        lease = repo.add_lease(a, ttl=100)
        repo.assert_consistent()
        after_mark.lease = lease

    repo._on_after_mark = after_mark
    report = repo.gc_collect()
    assert report.kept == 1
    repo._on_after_mark = None
    repo.release_lease(after_mark.lease)
    # 下一次 GC 重新标记：对象此时真的不可达。
    assert repo.gc_collect().deleted == 1
    assert not repo.has_object(a)


def test_concurrent_publish_and_gc_from_thread(make_repo):
    """真实线程版：发布线程在阶段屏障上与 GC 同步，结果确定。"""
    import threading

    repo = make_repo()
    a = repo.put_object(b"threaded")
    snap_id = repo.create_snapshot([a])
    repo.delete_snapshot(snap_id)  # 初始不可达

    barrier = threading.Event()
    proceed = threading.Event()
    outcome = {}

    def publisher():
        # 等待屏障：在“标记已提交、复核未开始”的确定时刻建立根引用并
        # 发布，精确复现两阶段之间的并发发布。
        barrier.wait(timeout=5)
        new_snap = repo.create_snapshot([a])
        outcome["published"] = repo.publish_snapshot(new_snap)
        repo.assert_consistent()
        proceed.set()

    def after_mark(run_id, candidates):
        assert candidates == [a]
        barrier.set()            # 放行发布线程
        assert proceed.wait(5)   # 等待发布完成后再进入复核
        repo.assert_consistent()

    t = threading.Thread(target=publisher)
    t.start()
    repo._on_after_mark = after_mark
    report = repo.gc_collect()
    repo._on_after_mark = None
    t.join(timeout=5)
    assert not t.is_alive()
    assert outcome["published"] is True
    assert report.kept == 1
    assert report.deleted == 0
    assert repo.has_object(a)


def test_object_put_after_mark_is_not_collected(make_repo):
    """标记之后新 put 的对象根本不在候选集里，绝不能被误删。"""
    repo = make_repo()
    old = repo.put_object(b"old")

    def after_mark(run_id, candidates):
        assert candidates == [old]
        new = repo.put_object(b"brand-new")
        snap = repo.create_snapshot([new])
        repo.publish_snapshot(snap)
        repo.assert_consistent()

    repo._on_after_mark = after_mark
    report = repo.gc_collect()
    assert report.deleted == 1  # 只有 old
    new_oid = hashlib.sha256(b"brand-new").hexdigest()
    assert repo.get_object(new_oid) == b"brand-new"
    repo.assert_consistent()
