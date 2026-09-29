"""本地自检入口（沙箱无真实 pytest 时使用）：python tools/run_tests.py。"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent / "shim"))
sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402  （在无真实 pytest 的环境中解析到本地垫片）

sys.exit(pytest.run(ROOT / "tests"))
