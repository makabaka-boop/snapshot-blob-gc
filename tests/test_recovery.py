"""进程中断（崩溃）恢复测试：重开仓库后可安全重试，且绝不破坏不变量。

崩溃点通过直接操作 SQLite 与 graveyard 目录来模拟，不依赖真正 kill。
"""

import sqlite3

from local_repo import CorruptionError, ObjectRepository


def _raw_conn(root):
    conn = sqlite3.connect(root / "repo.sqlite3", isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _gy_file(repo, run_id, oid):
    return repo.graveyard_dir / run_id / oid


def test_crash_after_mark_resumes_without_remark(make_repo, tmp_path, clock):
    # 第一次运行：在标记完成的屏障内直接关库，模拟阶段一提交后崩溃。
    repo = make_repo()
    a = repo.put_object(b"orphaned")
    captured = {}

    def after_mark(run_id, candidates):
        captured["run_id"] = run_id
        captured["candidates"] = candidates
        repo.skip_teardown_consistency = True
        repo.close()
        raise RuntimeError("simulated crash after mark")

    repo._on_after_mark = after_mark
    try:
        repo.gc_collect()
    except RuntimeError:
        pass

    # 重开：存在未完成运行；不重新标记，直接接管复核。
    repo2 = make_repo(tmp_path)
    assert repo2.active_gc_run() == captured["run_id"]
    repo2.assert_consistent()  # 对象文件完好，数据库引用完好
    assert repo2.has_object(a)

    report = repo2.gc_collect()
    assert report.resumed is True
    assert report.run_id == captured["run_id"]
    assert report.candidates == 1
    assert report.deleted == 1
    assert not repo2.has_object(a)
    repo2.assert_consistent()


def test_crash_before_delete_commit_restores_graveyard(make_repo, tmp_path):
    """事务提交前崩溃：文件可能已进 graveyard，元数据行仍在 => 还原。"""
    repo = make_repo()
    a = repo.put_object(b"precious")
    snap = repo.create_snapshot([a])

    # 手工建立“sweeping 中”的运行与候选（模拟阶段一已提交）。
    run_id = "crash-run-1"
    conn = _raw_conn(repo.root)
    conn.execute("INSERT INTO gc_runs(id, state, created_at) VALUES (?, 'sweeping', 1)",
                 (run_id,))
    # 候选实际不可达：先撤销快照引用，但保持 objects 行。
    conn.execute("DELETE FROM snapshots WHERE id = ?", (snap,))
    conn.execute("INSERT INTO gc_candidates(run_id, oid) VALUES (?, ?)", (run_id, a))
    conn.close()

    # 模拟“已移动文件到 graveyard、但 DELETE objects 尚未提交”的最坏序次。
    src = repo.objects_dir / a[:2] / a
    gy = repo.graveyard_dir / run_id
    gy.mkdir(parents=True, exist_ok=True)
    src.replace(gy / a)
    repo.skip_teardown_consistency = True
    repo.close()

    # 重开：恢复逻辑必须把文件还原（数据库仍引用它）。
    repo2 = make_repo(tmp_path)
    assert repo2.has_object(a)
    assert repo2.get_object(a) == b"precious"
    repo2.assert_consistent()
    # 注意：快照已撤销，恢复后重试 GC 会正常回收它。
    report = repo2.gc_collect()
    assert report.resumed is True
    assert report.deleted == 1
    assert not repo2.has_object(a)


def test_crash_after_delete_commit_removes_graveyard_leftover(make_repo, tmp_path):
    """事务提交后、物理删除前崩溃：元数据已删 => 残留文件被清除。"""
    repo = make_repo()
    a = repo.put_object(b"doomed")
    run_id = "crash-run-2"
    conn = _raw_conn(repo.root)
    conn.execute("INSERT INTO gc_runs(id, state, created_at) VALUES (?, 'sweeping', 1)",
                 (run_id,))
    conn.execute("DELETE FROM objects WHERE oid = ?", (a,))
    conn.execute(
        "INSERT INTO gc_candidates(run_id, oid, state) VALUES (?, ?, 'deleted')",
        (run_id, a),
    )
    conn.close()

    gy = repo.graveyard_dir / run_id
    gy.mkdir(parents=True, exist_ok=True)
    (repo.objects_dir / a[:2] / a).replace(gy / a)
    repo.skip_teardown_consistency = True
    repo.close()

    repo2 = make_repo(tmp_path)
    assert not repo2.has_object(a)
    assert not (repo2.graveyard_dir / run_id).exists()
    # 旧运行里候选都已处理；接管后收尾为 done，不删任何额外对象。
    report = repo2.gc_collect()
    assert report.resumed is True
    assert report.deleted == 0
    assert report.candidates == 1
    repo2.assert_consistent()


def test_resumed_run_rescues_candidate_referenced_after_crash(make_repo, tmp_path):
    """崩溃后、重试前若新快照重新引用候选，恢复后的复核必须放弃删除。"""
    repo = make_repo()
    a = repo.put_object(b"saved-after-crash")
    captured = {}

    def after_mark(run_id, candidates):
        captured["run_id"] = run_id
        repo.skip_teardown_consistency = True
        repo.close()
        raise RuntimeError("simulated crash")

    repo._on_after_mark = after_mark
    try:
        repo.gc_collect()
    except RuntimeError:
        pass

    # 重开后、恢复 GC 之前发布引用该对象的新快照。
    repo2 = make_repo(tmp_path)
    s = repo2.create_snapshot([a])
    repo2.publish_snapshot(s)
    report = repo2.gc_collect()
    assert report.resumed is True
    assert report.kept == 1
    assert report.deleted == 0
    assert repo2.get_object(a) == b"saved-after-crash"
    repo2.assert_consistent()


def test_put_crash_before_commit_leaves_safe_orphan_cleaned_at_reopen(
    make_repo, tmp_path
):
    """put 在“文件就位、元数据提交”前崩溃：重开后游离文件被清除。"""
    repo = make_repo()
    import hashlib

    data = b"never-committed"
    oid = hashlib.sha256(data).hexdigest()
    shard = repo.objects_dir / oid[:2]
    shard.mkdir(parents=True, exist_ok=True)
    (shard / oid).write_bytes(data)  # 无 objects 行
    repo.skip_teardown_consistency = True
    repo.close()

    repo2 = make_repo(tmp_path)
    assert not repo2.has_object(oid)
    assert not (repo2.objects_dir / oid[:2] / oid).exists()
    repo2.assert_consistent()


def test_external_file_deletion_is_detected_as_corruption(make_repo):
    """外部直接删掉数据库仍引用的文件：读取/GC 必须确定性报损坏。"""
    repo = make_repo()
    a = repo.put_object(b"tampered")
    repo.create_snapshot([a])
    p = repo.objects_dir / a[:2] / a
    p.unlink()  # 外部破坏，绕过 API
    repo.skip_teardown_consistency = True
    with __import__("pytest").raises(CorruptionError):
        repo.assert_consistent()
    with __import__("pytest").raises(CorruptionError):
        repo.get_object(a)


def test_repeated_gc_after_interruptions_is_deterministic(make_repo, tmp_path):
    """连续两次“标记后崩溃 + 重试”：最终集合与首次正常 GC 完全一致。"""
    repo = make_repo()
    keep = repo.put_object(b"keep")
    junk1 = repo.put_object(b"junk1")
    junk2 = repo.put_object(b"junk2")
    s = repo.create_snapshot([keep])
    repo.publish_snapshot(s)

    def crash(run_id, candidates):
        repo.skip_teardown_consistency = True
        repo.close()
        raise RuntimeError("crash")

    repo._on_after_mark = crash
    for _ in range(2):
        try:
            repo.gc_collect()
        except RuntimeError:
            pass
        repo = make_repo(tmp_path)
        repo._on_after_mark = crash
    # 最后一次不再崩溃，无回调正常完成。
    repo._on_after_mark = None
    report = repo.gc_collect()
    assert not report.resumed  # 上一次中断的运行已在上次重试中完成
    assert set(repo.list_objects()) == {keep}
    assert not repo.has_object(junk1)
    assert not repo.has_object(junk2)
    repo.assert_consistent()
