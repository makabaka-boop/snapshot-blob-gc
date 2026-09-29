"""进程中断后的安全重试：在真实子进程里 SIGKILL，随后用新进程复核。"""

from __future__ import annotations

import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from objstore import ObjectNotFoundError, Repository
from objstore.clocks import SystemClock

from conftest import assert_consistent

WORKER = Path(__file__).with_name("crash_worker.py")


def _run_crash_worker(root: Path, phase: str, run_id: str) -> None:
    proc = subprocess.run(
        [sys.executable, str(WORKER), str(root), phase, run_id],
        capture_output=True,
        text=True,
        timeout=60,
    )
    # SIGKILL → 返回码 -9；这正是我们要制造的中断。
    assert proc.returncode == -9, (
        f"工作进程应被 SIGKILL 杀死，实际返回码 {proc.returncode}\n"
        f"stdout={proc.stdout}\nstderr={proc.stderr}"
    )


@pytest.mark.parametrize(
    "phase",
    ["after_mark_commit", "before_delete_commit", "after_delete_commit"],
)
def test_sweep_retry_after_kill(tmp_path, phase):
    root = tmp_path / "repo"
    run_id = "crash-run-1"

    with Repository(root, clock=SystemClock()) as repo:
        victim = repo.put(b"victim-bytes")
        kept = repo.put(b"kept-bytes")
        repo.publish_snapshot([kept])
        repo.grant_lease([kept], ttl=timedelta(days=1))
        assert_consistent(repo)

    _run_crash_worker(root, phase, run_id)

    # 中断后用一个全新进程/连接打开，必须仍能核对一致性并重试完成回收。
    with Repository(root, clock=SystemClock()) as repo:
        assert_consistent(repo)
        assert repo.exists(kept)
        if phase == "after_delete_commit":
            # 元数据删除已提交：对象已不可见；文件可能还在但无人引用。
            assert not repo.exists(victim)
            with pytest.raises(ObjectNotFoundError):
                repo.publish_snapshot([victim])
        else:
            # after_mark_commit：候选落库但什么都没删；
            # before_delete_commit：删除事务整体回滚，一切如旧。
            assert repo.exists(victim)

        result = repo.gc_sweep(run_id)
        assert result.reclaimed == [victim]
        assert not repo.exists(victim)
        assert repo.exists(kept)
        assert_consistent(repo)

        # 再来一轮回收：稳定无候选，文件目录无残留。
        second = repo.gc()
        assert second.reclaimed == []
        assert_consistent(repo)
        objects_dir = root / "objects"
        leftovers = [
            p for shard in objects_dir.iterdir() for p in shard.iterdir()
            if not p.name.startswith(".")
        ]
        assert len(leftovers) == 1  # 仅剩 kept


def test_mark_retry_after_kill_before_commit(tmp_path):
    root = tmp_path / "repo"
    run_id = "crash-mark-1"
    with Repository(root, clock=SystemClock()) as repo:
        repo.put(b"orphan-ish")

    _run_crash_worker(root, "mark_before_commit", run_id)

    with Repository(root, clock=SystemClock()) as repo:
        assert_consistent(repo)
        # 事务未提交：运行不存在，同样的 run_id 可安全重新使用。
        mark = repo.gc_mark(run_id=run_id)
        assert mark.run_id == run_id
        result = repo.gc_sweep(run_id)
        assert len(result.reclaimed) == 1
        assert_consistent(repo)


def test_pending_file_after_commit_crash_never_dangles(tmp_path):
    """元数据删除已提交、文件未删的崩溃窗口：绝不允许再产生引用。"""

    root = tmp_path / "repo"
    with Repository(root, clock=SystemClock()) as repo:
        victim = repo.put(b"dangling-check")
    _run_crash_worker(root, "after_delete_commit", "run-x")

    with Repository(root, clock=SystemClock()) as repo:
        assert_consistent(repo)
        # 对象行已不存在：任何引用尝试都得到确定的“对象不存在”，
        # 数据库不可能指向那个待清理文件。
        with pytest.raises(ObjectNotFoundError):
            repo.publish_snapshot([victim])
        assert repo.list_snapshots() == []

        # 重试 sweep：补删待清理文件，结果确定。
        result = repo.gc_sweep("run-x")
        assert result.reclaimed == [victim]
        with pytest.raises(ObjectNotFoundError):
            repo.grant_lease([victim], ttl=timedelta(minutes=1))
        assert_consistent(repo)
