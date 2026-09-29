"""极简 pytest 垫片——仅用于本沙箱无 pytest 可装时本地自检。

支持本仓库测试用到的极小子集：raises、fixture、mark.parametrize、
tmp_path/monkeypatch 内置装置、test_* 函数收集。不是 pytest 的替代品，
验收以 Docker verify 服务中的真实 pytest 为准。
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
import tempfile
import traceback
import types
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def raises(exc_type):
    class Holder:
        value = None

    holder = Holder()
    try:
        yield holder
    except exc_type as exc:  # type: ignore[misc]
        holder.value = exc
    except BaseException as exc:  # noqa: BLE001
        raise AssertionError(
            f"期望 {exc_type!r}，实际抛出 {type(exc).__name__}: {exc}"
        ) from exc
    else:
        raise AssertionError(f"期望抛出 {exc_type!r}，但没有任何异常")


class _Mark:
    def parametrize(self, argnames, argvalues):
        names = [n.strip() for n in argnames.split(",")]

        def decorate(fn):
            cases = []
            for value in argvalues:
                values = value if isinstance(value, tuple) else (value,)
                cases.append(dict(zip(names, values)))
            fn._param_cases = cases
            return fn

        return decorate


mark = _Mark()


def fixture(fn=None, **kwargs):
    def decorate(f):
        f._is_fixture = True
        return f

    return decorate if fn is None else decorate(fn)


def _load_module(path: Path, name: str, sys_path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    spec.loader.exec_module(module)
    return module


class Scope:
    def __init__(self, conftest, root):
        self.values = {"tmp_path": Path(tempfile.mkdtemp(prefix="shim-pytest-"))}
        self.conftest = conftest
        self.root = root
        self._teardowns = []

    def teardown(self):
        for fn in reversed(self._teardowns):
            fn()

    def get(self, name, stack=None):
        stack = stack or set()
        if name in self.values:
            return self.values[name]
        if hasattr(self.conftest, name):
            if name in stack:
                raise RuntimeError(f"fixture 循环: {stack}")
            fn = getattr(self.conftest, name)
            params = self._params(fn, stack | {name})
            produced = fn(**params)
            if inspect.isgenerator(produced):
                gen = produced
                value = next(gen)
                self._teardowns.append(lambda g=gen: self._close(g))
            else:
                value = produced
            self.values[name] = value
            return value
        raise RuntimeError(f"未知 fixture: {name}")

    @staticmethod
    def _close(gen):
        try:
            next(gen)
        except StopIteration:
            pass

    def _params(self, fn, stack):
        sig = inspect.signature(fn)
        kwargs = {}
        for pname in sig.parameters:
            kwargs[pname] = self.get(pname, stack)
        return kwargs


def run(tests_dir: Path) -> int:
    root = tests_dir.parent
    conftest = _load_module(tests_dir / "conftest.py", "conftest", str(root))
    passed = failed = 0
    failures = []

    for path in sorted(tests_dir.glob("test_*.py")):
        module = _load_module(path, f"tests_{path.stem}", str(root))
        for attr in sorted(dir(module)):
            if not attr.startswith("test_"):
                continue
            fn = getattr(module, attr)
            if not callable(fn):
                continue
            cases = getattr(fn, "_param_cases", [{}])
            for case in cases:
                scope = Scope(conftest, root)
                label = attr + (
                    f"[{list(case.values())[0]!r}]" if len(case) == 1 else ""
                )
                try:
                    kwargs = dict(case)
                    sig = inspect.signature(fn)
                    for pname in sig.parameters:
                        if pname not in kwargs:
                            kwargs[pname] = scope.get(pname)
                    fn(**kwargs)
                except Exception:  # noqa: BLE001
                    failed += 1
                    failures.append((label, traceback.format_exc()))
                    print(f"FAIL {label}")
                else:
                    passed += 1
                    print(f"PASS {label}")
                finally:
                    scope.teardown()

    print(f"\n{passed} passed, {failed} failed")
    for label, tb in failures:
        print("=" * 70)
        print(label)
        print(tb)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run(Path(__file__).resolve().parent.parent / "tests"))
