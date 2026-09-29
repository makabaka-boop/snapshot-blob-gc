"""pytest 公共夹具：可控时钟与每步一致性核对。"""

import threading

import pytest

from local_repo.repository import ObjectRepository


class FakeClock:
    """线程安全的可控时钟。

    时间只能向前推进（``advance``），保证“过期”是单调确定的事件。
    """

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("时钟不能回拨")
        with self._lock:
            self._now += seconds
            return self._now

    @property
    def now(self) -> float:
        return self()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def make_repo(tmp_path, clock):
    """构造仓库的工厂；默认在每个仓库上挂一致性自检。

    每个返回仓库的夹具都会在 GC 后以及测试结束时调用
    ``assert_consistent()``；测试中也可随时手动调用。
    """

    opened: list[ObjectRepository] = []

    def _make(root=None, on_after_mark=None, on_after_delete=None):
        # 默认根目录就是 tmp_path，保证崩溃测试用同一目录重开。
        root = root or tmp_path
        repo = ObjectRepository(
            root,
            clock=clock,
            on_after_mark=on_after_mark,
            on_after_delete=on_after_delete,
        )
        opened.append(repo)
        return repo

    yield _make

    for r in opened:
        try:
            # 测试可把仓库标记为“故意制造的崩溃现场”，跳过收尾自检；
            # 重新打开的恢复仓库仍必须通过自检。
            if not getattr(r, "skip_teardown_consistency", False):
                r.assert_consistent()
        finally:
            r.close()


@pytest.fixture
def repo(make_repo):
    return make_repo()
