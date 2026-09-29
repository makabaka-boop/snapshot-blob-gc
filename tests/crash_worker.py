"""一次性崩溃工作进程：在指定回收阶段被 SIGKILL，用于测试“进程中断后安全重试”。

用法:
    python crash_worker.py <repo_root> <phase> <run_id>

phase:
    mark_before_commit    标记事务提交前被杀（整笔标记回滚）
    after_mark_commit     标记事务提交后立即被杀（候选已落库，文件未动）
    before_delete_commit  复核通过、持有写锁、删除元数据之前被杀（事务随进程回滚）
    after_delete_commit   元数据删除已提交、对象文件尚未 unlink 时被杀
                          （留下无任何引用的待清理文件）
"""

from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from objstore.clocks import SystemClock
from objstore.hooks import NullHooks
from objstore.repo import Repository


class KillBeforeDelete(NullHooks):
    def before_reclaim(self, run_id: str, object_id: str) -> None:
        os.kill(os.getpid(), signal.SIGKILL)


class KillBeforeMarkCommit(NullHooks):
    def mark_before_commit(self, run_id: str, candidate_ids: list[str]) -> None:
        os.kill(os.getpid(), signal.SIGKILL)


def _kill_at_unlink() -> None:
    original_unlink = Path.unlink

    def unlink(self, *args, **kwargs):
        # sweep 复核提交后唯一的 unlink 调用就是删除该候选对象文件。
        os.kill(os.getpid(), signal.SIGKILL)

    Path.unlink = unlink  # type: ignore[assignment]


def main() -> int:
    root, phase, run_id = sys.argv[1], sys.argv[2], sys.argv[3]
    hooks: NullHooks = NullHooks()
    if phase == "before_delete_commit":
        hooks = KillBeforeDelete()
    elif phase == "after_delete_commit":
        _kill_at_unlink()
    elif phase == "mark_before_commit":
        hooks = KillBeforeMarkCommit()

    repo = Repository(root, clock=SystemClock(), hooks=hooks)

    mark = repo.gc_mark(run_id=run_id)
    if phase == "after_mark_commit":
        os.kill(os.getpid(), signal.SIGKILL)

    repo.gc_sweep(mark.run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
