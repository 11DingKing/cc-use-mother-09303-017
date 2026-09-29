"""时间来源，便于测试中注入确定时钟。"""
from __future__ import annotations

from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")
