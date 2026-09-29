"""对象、快照、租约 API 的确定性行为测试。"""

import hashlib

import pytest

from local_repo import (
    LeaseNotFound,
    ObjectNotFound,
    ObjectRepository,
    SnapshotNotFound,
)


def oid(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# 对象
# ---------------------------------------------------------------------------

def test_put_returns_content_address(repo):
    a = repo.put_object(b"hello")
    assert a == oid(b"hello")
    repo.assert_consistent()


def test_put_is_idempotent_for_same_content(repo):
    a = repo.put_object(b"data")
    b = repo.put_object(b"data")
    assert a == b
    assert repo.list_objects() == [a]
    repo.assert_consistent()


def test_put_distinct_contents(repo):
    a = repo.put_object(b"a")
    b = repo.put_object(b"b")
    assert a != b
    assert set(repo.list_objects()) == {a, b}


def test_get_object_roundtrip(repo):
    a = repo.put_object(b"payload")
    assert repo.get_object(a) == b"payload"
    assert repo.has_object(a)


def test_get_missing_object_raises(repo):
    missing = oid(b"never written")
    assert not repo.has_object(missing)
    with pytest.raises(ObjectNotFound):
        repo.get_object(missing)


def test_get_object_bad_id_format(repo):
    with pytest.raises(ValueError):
        repo.get_object("../etc/passwd")
    with pytest.raises(ValueError):
        repo.get_object("abc")


def test_put_rejects_str(repo):
    with pytest.raises(TypeError):
        repo.put_object("not bytes")  # type: ignore[arg-type]


def test_create_snapshot_rejects_unknown_object(repo):
    with pytest.raises(ObjectNotFound):
        repo.create_snapshot([oid(b"missing")])


# ---------------------------------------------------------------------------
# 快照：不可变、发布幂等、删除撤销引用
# ---------------------------------------------------------------------------

def test_snapshot_immutable_and_readable(repo):
    a = repo.put_object(b"a")
    b = repo.put_object(b"b")
    s = repo.create_snapshot([a, b, a])  # 重复项去重，保留顺序
    assert repo.read_snapshot(s) == [a, b]
    assert not repo.is_published(s)


def test_publish_snapshot_is_idempotent(repo):
    s = repo.create_snapshot([repo.put_object(b"x")])
    assert repo.publish_snapshot(s) is True
    assert repo.publish_snapshot(s) is False
    assert repo.is_published(s)
    assert repo.list_snapshots(include_drafts=False) == [s]
    repo.assert_consistent()


def test_publish_and_read_missing_snapshot(repo):
    with pytest.raises(SnapshotNotFound):
        repo.publish_snapshot("deadbeef")
    with pytest.raises(SnapshotNotFound):
        repo.read_snapshot("deadbeef")
    with pytest.raises(SnapshotNotFound):
        repo.is_published("deadbeef")


def test_delete_snapshot_revokes_root_but_keeps_object_until_gc(repo):
    a = repo.put_object(b"a")
    s = repo.create_snapshot([a])
    assert repo.delete_snapshot(s) is True
    # 幂等：重复删除结果确定。
    assert repo.delete_snapshot(s) is False
    # 删除快照只是撤销根引用；对象在 GC 前仍完好可读。
    assert repo.has_object(a)
    assert repo.get_object(a) == b"a"
    with pytest.raises(SnapshotNotFound):
        repo.read_snapshot(s)
    repo.assert_consistent()


def test_snapshot_survives_reopen(tmp_path, clock):
    r1 = ObjectRepository(tmp_path, clock=clock)
    a = r1.put_object(b"persist")
    s = r1.create_snapshot([a])
    r1.publish_snapshot(s)
    r1.close()

    r2 = ObjectRepository(tmp_path, clock=clock)
    try:
        assert r2.read_snapshot(s) == [a]
        assert r2.is_published(s)
        assert r2.get_object(a) == b"persist"
        r2.assert_consistent()
    finally:
        r2.close()


# ---------------------------------------------------------------------------
# 租约
# ---------------------------------------------------------------------------

def test_lease_protects_object(repo, clock):
    a = repo.put_object(b"export-me")
    lease = repo.add_lease(a, ttl=100)
    info = repo.get_lease(lease)
    assert info.oid == a
    assert info.is_live_at(clock.now)
    assert info.is_live_at(clock.now + 99)
    assert not info.is_live_at(clock.now + 100)  # 到期判定为闭区间


def test_lease_on_missing_object(repo):
    with pytest.raises(ObjectNotFound):
        repo.add_lease(oid(b"missing"), ttl=10)


def test_release_lease_is_idempotent(repo):
    a = repo.put_object(b"a")
    lease = repo.add_lease(a, ttl=100)
    assert repo.release_lease(lease) is True
    assert repo.release_lease(lease) is False
    with pytest.raises(LeaseNotFound):
        repo.get_lease(lease)


def test_get_missing_lease(repo):
    with pytest.raises(LeaseNotFound):
        repo.get_lease("nope")


def test_zero_and_negative_ttl_allowed(repo):
    a = repo.put_object(b"a")
    l0 = repo.add_lease(a, ttl=0)
    assert not repo.get_lease(l0).is_live
    lneg = repo.add_lease(a, ttl=-5)
    assert not repo.get_lease(lneg).is_live
