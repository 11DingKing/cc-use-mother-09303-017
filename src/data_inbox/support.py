"""时间与标识工具，集中处理以便测试可重放。"""
from __future__ import annotations

import itertools
import uuid
from datetime import datetime, timezone


class Clock:
    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Sequence:
    """批次/行序号生成器。"""

    def __init__(self, start: int = 1) -> None:
        self._it = itertools.count(start)

    def next(self) -> int:
        return next(self._it)
