"""可控时钟抽象。

租约是否到期只依赖时钟给出的“当前时间”，因此测试可以注入冻结/步进时钟，
无需真实睡眠即可复现“标记后到期”“标记后发布”等竞争。
"""

from __future__ import annotations

from datetime import datetime, timezone


class Clock:
    """时钟接口：返回时区感知的 UTC ``datetime``。"""

    def now(self) -> datetime:  # pragma: no cover - 接口方法
        raise NotImplementedError


class SystemClock(Clock):
    """墙上时钟。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def to_epoch_micros(dt: datetime) -> int:
    """把时区感知时间转成自 Unix 纪元起的整数微秒。"""

    if dt.tzinfo is None:
        raise ValueError("时间必须带时区信息")
    return int(dt.timestamp() * 1_000_000)


def from_epoch_micros(value: int) -> datetime:
    """整数微秒转回 UTC 时区感知时间。"""

    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)
