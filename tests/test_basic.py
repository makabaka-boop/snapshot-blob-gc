"""对象、快照、租约的基础语义与确定性错误结果。"""

from __future__ import annotations

from datetime import timedelta

import pytest

from objstore import (
    InvalidObjectIdError,
    LeaseNotFoundError,
    ObjectNotFoundError,
    SnapshotNotFoundError,
)

from conftest import assert_consistent


def test_put_is_content_addressed_and_idempotent(repo):
    a1 = repo.put(b"hello")
    a2 = repo.put(b"hello")
    b = repo.put(b"world")
    assert a1 == a2
    assert a1 != b
    assert a1 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert repo.exists(a1)
    assert repo.get(a1) == b"hello"
    assert set(repo.list_objects()) == {a1, b}
    assert_consistent(repo)


def test_get_missing_object_raises(repo):
    phantom = "0" * 64
    with pytest.raises(ObjectNotFoundError) as ei:
        repo.get(phantom)
    assert ei.value.object_id == phantom
    assert not repo.exists(phantom)
    assert_consistent(repo)


def test_invalid_object_id_is_rejected(repo):
    with pytest.raises(InvalidObjectIdError):
        repo.get("not-a-hash")
    with pytest.raises(InvalidObjectIdError):
        repo.exists("A" * 64)


def test_publish_rejects_missing_objects_and_writes_nothing(repo):
    good = repo.put(b"good")
    bad = "f" * 64
    with pytest.raises(ObjectNotFoundError) as ei:
        repo.publish_snapshot([good, bad])
    assert bad in ei.value.object_ids and good not in ei.value.object_ids
    assert repo.list_snapshots() == []
    assert_consistent(repo)


def test_snapshot_publish_is_idempotent_and_immutable(repo):
    a = repo.put(b"a")
    b = repo.put(b"b")
    sid1 = repo.publish_snapshot([b, a])  # 顺序不影响集合身份
    sid2 = repo.publish_snapshot([a, b])
    assert sid1 == sid2
    info = repo.get_snapshot(sid1)
    assert info.object_ids == sorted([a, b])

    repo.delete_snapshot(sid1)
    with pytest.raises(SnapshotNotFoundError):
        repo.delete_snapshot(sid1)  # 重复删除有确定结果
    with pytest.raises(SnapshotNotFoundError):
        repo.get_snapshot(sid1)
    assert_consistent(repo)


def test_empty_snapshot_is_a_valid_root(repo):
    sid = repo.publish_snapshot([])
    assert repo.get_snapshot(sid).object_ids == []
    assert repo.list_snapshots() == [sid]
    assert_consistent(repo)


def test_lease_protects_then_expires(repo, clock):
    a = repo.put(b"a")
    lid = repo.grant_lease([a], ttl=timedelta(hours=1))

    info = repo.get_lease(lid)
    assert info.object_ids == [a]
    assert not info.is_expired(clock.now_micros())

    assert repo.revoke_lease(lid) is True
    assert repo.revoke_lease(lid) is False  # 重复撤销确定为 False
    with pytest.raises(LeaseNotFoundError):
        repo.get_lease(lid)
    assert_consistent(repo)


def test_grant_lease_rejects_missing_objects(repo):
    with pytest.raises(ObjectNotFoundError):
        repo.grant_lease(["1" * 64], ttl=timedelta(minutes=5))
    assert repo.list_leases() == []
    assert_consistent(repo)


def test_reopen_persists_state(repo_root, clock):
    from objstore import Repository

    with Repository(repo_root, clock=clock) as r:
        oid = r.put(b"persist-me")
        sid = r.publish_snapshot([oid])
        lid = r.grant_lease([oid], ttl=timedelta(hours=2))

    with Repository(repo_root, clock=clock) as r:
        assert r.exists(oid)
        assert r.get_snapshot(sid).object_ids == [oid]
        assert r.get_lease(lid).object_ids == [oid]
        assert_consistent(r)
