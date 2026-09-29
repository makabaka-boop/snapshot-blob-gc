"""两阶段垃圾回收：标记、复核、租约到期、游离文件、重复回收。"""

import hashlib

import pytest

from local_repo import ObjectNotFound


def oid(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_gc_deletes_unreferenced_objects(repo):
    a = repo.put_object(b"unreferenced")
    report = repo.gc_collect()
    assert report.candidates == 1
    assert report.deleted == 1
    assert report.kept == 0
    assert not repo.has_object(a)
    with pytest.raises(ObjectNotFound):
        repo.get_object(a)
    repo.assert_consistent()


def test_gc_empty_repository(repo):
    report = repo.gc_collect()
    assert (report.candidates, report.deleted, report.kept) == (0, 0, 0)
    assert not report.resumed


def test_gc_keeps_published_snapshot_objects(repo, clock):
    a = repo.put_object(b"published")
    b = repo.put_object(b"garbage")
    s = repo.create_snapshot([a])
    repo.publish_snapshot(s)
    report = repo.gc_collect()
    assert report.deleted == 1
    assert set(repo.list_objects()) == {a}
    assert repo.get_object(a) == b"published"
    repo.assert_consistent()


def test_draft_snapshot_also_protects_objects(repo):
    # 草稿快照同样持有根引用：数据库永远不指向已删除文件。
    a = repo.put_object(b"draft")
    repo.create_snapshot([a])  # 不发布
    report = repo.gc_collect()
    assert report.deleted == 0
    assert repo.get_object(a) == b"draft"
    repo.assert_consistent()


def test_deleting_snapshot_allows_collection(repo):
    a = repo.put_object(b"a")
    b = repo.put_object(b"b")
    s = repo.create_snapshot([a, b])
    repo.delete_snapshot(s)  # 撤销根引用
    report = repo.gc_collect()
    assert report.deleted == 2
    assert repo.list_objects() == []


def test_shared_object_kept_while_any_root_remains(repo):
    a = repo.put_object(b"shared")
    s1 = repo.create_snapshot([a])
    s2 = repo.create_snapshot([a])
    repo.delete_snapshot(s1)
    report = repo.gc_collect()
    assert report.deleted == 0  # s2 仍引用
    repo.delete_snapshot(s2)
    report = repo.gc_collect()
    assert report.deleted == 1
    assert not repo.has_object(a)


def test_live_lease_protects_object(repo, clock):
    a = repo.put_object(b"leased")
    repo.add_lease(a, ttl=100)
    report = repo.gc_collect()
    assert report.deleted == 0
    assert repo.get_object(a) == b"leased"


def test_expired_lease_is_purged_and_object_collected(repo, clock):
    a = repo.put_object(b"expired-export")
    lease = repo.add_lease(a, ttl=10)
    clock.advance(10)  # 到达到期边界
    assert not repo.get_lease(lease).is_live
    report = repo.gc_collect()
    assert report.deleted == 1
    assert report.kept == 0
    assert not repo.has_object(a)
    repo.assert_consistent()


def test_lease_expiring_between_mark_and_sweep_is_collected(repo, clock):
    """租约在标记时尚有效（对象不进候选），屏障内到期：

    本次运行按冻结候选执行，不会越界删除非候选对象；下一次 GC
    重新标记时租约已被清除，对象得到确定性回收。
    """
    a = repo.put_object(b"boundary")
    repo.add_lease(a, ttl=10)

    def after_mark(run_id, candidates):
        assert candidates == []  # 标记时租约仍有效
        clock.advance(10)        # 两阶段之间到期

    repo._on_after_mark = after_mark
    report = repo.gc_collect()
    repo._on_after_mark = None
    assert report.candidates == 0
    assert report.deleted == 0
    assert repo.has_object(a)  # 本次运行绝不越界删除

    # 新一次 GC：标记时租约已到期，对象被回收。
    report2 = repo.gc_collect()
    assert report2.deleted == 1
    assert not repo.has_object(a)


def test_second_gc_is_new_empty_run(repo):
    repo.put_object(b"x")
    r1 = repo.gc_collect()
    assert r1.deleted == 1
    assert repo.active_gc_run() is None
    r2 = repo.gc_collect()
    assert r2.run_id != r1.run_id
    assert not r2.resumed
    assert (r2.candidates, r2.deleted) == (0, 0)


def test_object_referenced_only_by_expired_then_released(repo, clock):
    a = repo.put_object(b"a")
    lease = repo.add_lease(a, ttl=1000)
    repo.release_lease(lease)
    report = repo.gc_collect()
    assert report.deleted == 1


def test_mixed_roots(repo, clock):
    keep1 = repo.put_object(b"in-published")
    keep2 = repo.put_object(b"in-draft")
    keep3 = repo.put_object(b"in-lease")
    junk = repo.put_object(b"junk")

    s = repo.create_snapshot([keep1])
    repo.publish_snapshot(s)
    repo.create_snapshot([keep2])
    repo.add_lease(keep3, ttl=100)

    report = repo.gc_collect()
    assert report.deleted == 1
    assert set(repo.list_objects()) == {keep1, keep2, keep3}
    assert not repo.has_object(junk)
    repo.assert_consistent()


def test_orphan_files_are_swept_at_gc(make_repo, tmp_path, clock):
    # 手工制造一个“有文件、无元数据”的游离对象（模拟外部残留）。
    import os

    repo = make_repo()
    ghost = oid(b"ghost")
    shard = repo.objects_dir / ghost[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / ghost).write_bytes(b"ghost")
    # 注意：游离文件内容不要求等于其名，sweep 只按文件名识别后删除。
    # 这里内容恰好一致。
    assert ghost == hashlib.sha256(b"ghost").hexdigest()

    kept = repo.put_object(b"real")
    s = repo.create_snapshot([kept])  # real 必须有根引用，否则它本身也是候选
    repo.publish_snapshot(s)
    report = repo.gc_collect()
    assert report.orphans_removed == 1
    assert report.deleted == 0
    assert not (repo.objects_dir / ghost[:2] / ghost).exists()
    assert repo.has_object(kept)
    repo.assert_consistent()
    assert os.listdir(repo.graveyard_dir) == []
