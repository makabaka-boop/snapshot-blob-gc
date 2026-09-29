"""pytest 共享装置：可控时钟与带阶段屏障的回收钩子。"""

from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path

import pytest

from objstore.clocks import Clock, from_epoch_micros, to_epoch_micros
from objstore.diagnostics import check_consistency
from objstore.hooks import NullHooks
from objstore.repo import Repository


class FakeClock(Clock):
    """冻结/可步进时钟，初始固定在一个 UTC 时刻，全程加锁。"""

    def __init__(self, start_epoch_micros: int | None = None) -> None:
        if start_epoch_micros is None:
            # 固定基准：2026-01-01T00:00:00Z
            start_epoch_micros = 1_767_225_600 * 1_000_000
        self._now = start_epoch_micros
        self._lock = threading.Lock()

    def now(self):
        with self._lock:
            return from_epoch_micros(self._now)

    def now_micros(self) -> int:
        with self._lock:
            return self._now

    def set(self, value) -> None:
        if hasattr(value, "timestamp"):
            value = to_epoch_micros(value)
        with self._lock:
            self._now = int(value)

    def advance(self, delta: timedelta | int) -> int:
        if isinstance(delta, timedelta):
            micros = int(delta.total_seconds() * 1_000_000)
        else:
            micros = int(delta)
        with self._lock:
            self._now += micros
            return self._now


class ScriptedHooks(NullHooks):
    """记录事件并在各阶段执行注入回调的钩子（回调可为屏障）。"""

    def __init__(
        self,
        *,
        on_mark_begin=None,
        on_mark_before_commit=None,
        on_mark_end=None,
        on_sweep_begin=None,
        on_before_reclaim=None,
        on_after_reclaim=None,
    ) -> None:
        self._cbs = {
            "mark_begin": on_mark_begin,
            "mark_before_commit": on_mark_before_commit,
            "mark_end": on_mark_end,
            "sweep_begin": on_sweep_begin,
            "before_reclaim": on_before_reclaim,
            "after_reclaim": on_after_reclaim,
        }
        self._lock = threading.Lock()
        self.events: list[tuple[str, str]] = []

    def _fire(self, name: str, run_id: str, object_id: str = "") -> None:
        with self._lock:
            self.events.append((name, object_id))
        cb = self._cbs[name]
        if cb is not None:
            cb(run_id, object_id)

    def mark_begin(self, run_id: str) -> None:
        self._fire("mark_begin", run_id)

    def mark_before_commit(self, run_id: str, candidate_ids: list[str]) -> None:
        with self._lock:
            self.events.append(("mark_before_commit", ",".join(candidate_ids)))
        cb = self._cbs["mark_before_commit"]
        if cb is not None:
            cb(run_id, candidate_ids)

    def mark_end(self, run_id: str, candidate_ids: list[str]) -> None:
        with self._lock:
            self.events.append(("mark_end", ",".join(candidate_ids)))
        cb = self._cbs["mark_end"]
        if cb is not None:
            cb(run_id, candidate_ids)

    def sweep_begin(self, run_id: str, candidate_ids: list[str]) -> None:
        with self._lock:
            self.events.append(("sweep_begin", ",".join(candidate_ids)))
        cb = self._cbs["sweep_begin"]
        if cb is not None:
            cb(run_id, candidate_ids)

    def before_reclaim(self, run_id: str, object_id: str) -> None:
        self._fire("before_reclaim", run_id, object_id)

    def after_reclaim(self, run_id: str, object_id: str) -> None:
        self._fire("after_reclaim", run_id, object_id)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    return root


@pytest.fixture
def repo(repo_root: Path, clock: FakeClock) -> Repository:
    r = Repository(repo_root, clock=clock)
    yield r
    check_consistency(r).raise_if_bad()
    r.close()


def assert_consistent(repo: Repository) -> None:
    """供测试在每个关键步骤后显式核对。"""

    check_consistency(repo).raise_if_bad()
