"""批次、数据行状态与去向枚举。"""
from __future__ import annotations

from enum import Enum


class BatchStatus(str, Enum):
    """批次生命周期状态，对应契约中的接收/映射/校验/发布/撤回。"""

    REGISTERED = "已登记"
    VALIDATED = "核验完成"
    PUBLISHING = "发布中"
    PUBLISHED = "已发布"
    WITHDRAWN = "已撤回"


class RowStatus(str, Enum):
    """单行状态机：接收 → 映射 → 核验通过/已隔离 → 已发布/已撤回。"""

    RECEIVED = "已接收"
    MAPPED = "已映射"
    VALID = "核验通过"
    QUARANTINED = "已隔离"
    PUBLISHED = "已发布"
    WITHDRAWN = "已撤回"


class Destination(str, Enum):
    """每一行当前所处的物理去向，保证“行有所处”。"""

    STAGING = "暂存区"
    QUARANTINE = "隔离区"
    OFFICIAL = "正式数据"
    WITHDRAWN = "撤回归档"
