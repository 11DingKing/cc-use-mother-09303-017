"""领域状态枚举与服务层返回的只读收据结构。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PartyRole(str, Enum):
    SUBMITTER = "合作院校填报员"
    COORDINATOR = "数据协调员"
    COMPILER = "报告编制人员"


class BatchStatus(str, Enum):
    """提交批次（一次文件上传）的生命周期状态。"""

    RECEIVED = "接收"
    PARSE_FAILED = "解析失败"
    VALIDATED = "校验完成"
    PARTIALLY_PUBLISHED = "部分发布"
    PUBLISHED = "已发布"
    WITHDRAWN = "已撤回"


class RowStatus(str, Enum):
    """逐行状态：合格与不合格严格分流。"""

    VALID = "合格"
    QUARANTINED = "隔离"
    PUBLISHED = "已发布"
    WITHDRAWN = "已撤回"


class PublicationStatus(str, Enum):
    """一次原子发布尝试的状态。失败发布保留 FAILED 记录但不产生正式数据。"""

    PENDING = "待提交"
    COMMITTED = "已提交"
    FAILED = "已回滚"
    WITHDRAWN = "已撤回"


class OfficialStatus(str, Enum):
    ACTIVE = "生效中"
    WITHDRAWN = "已撤回"


@dataclass
class ReceiveReceipt:
    """文件接收回执：先落指纹，再给结论。"""

    submission_id: str
    fingerprint: str
    status: str
    submitter_id: str
    mapping_version: str
    filename: str
    total_rows: int = 0
    valid_rows: int = 0
    quarantined_rows: int = 0
    unmapped_columns: list[str] = field(default_factory=list)
    parse_error: str | None = None
    duplicate_of_submission_id: str | None = None

    @property
    def is_duplicate(self) -> bool:
        return self.duplicate_of_submission_id is not None


@dataclass
class CorrectionResult:
    """受控修订结果：修订后立即重新校验。"""

    row_id: str
    accepted: bool
    status: str
    errors: list[dict]
    revision_id: str
    valid_rows: int
    quarantined_rows: int


@dataclass
class PublishReceipt:
    publication_id: str
    submission_id: str
    seq: int
    scope: str
    status: str
    published_rows: int
    committed_at: str | None = None
    failure_reason: str | None = None
