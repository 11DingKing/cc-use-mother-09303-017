"""收件服务异常类型。"""
from __future__ import annotations


class InboxError(Exception):
    """收件服务领域异常基类。"""


class UnknownMappingError(InboxError):
    """引用了不存在的字段映射版本。"""

    def __init__(self, version: str) -> None:
        super().__init__(f"未知的字段映射版本：{version}")
        self.version = version


class MappingRetiredError(InboxError):
    """映射版本已停用，不能用于新批次（历史批次仍按旧版解释）。"""

    def __init__(self, version: str) -> None:
        super().__init__(f"字段映射版本已停用：{version}")
        self.version = version


class EmptySubmissionError(InboxError):
    """文件没有任何数据行。"""


class DuplicateSubmissionError(InboxError):
    """文件指纹与既有批次重复。"""

    def __init__(self, filename: str, existing_batch_id: str) -> None:
        super().__init__(
            f"文件 {filename} 与已接收批次 {existing_batch_id} 指纹重复"
        )
        self.filename = filename
        self.existing_batch_id = existing_batch_id


class BatchNotFound(InboxError):
    def __init__(self, batch_id: str) -> None:
        super().__init__(f"批次不存在：{batch_id}")
        self.batch_id = batch_id


class RowNotFound(InboxError):
    def __init__(self, row_id: str) -> None:
        super().__init__(f"数据行不存在：{row_id}")
        self.row_id = row_id


class RevisionError(InboxError):
    """受控修订被拒绝的基类。"""


class PublishConflictError(InboxError):
    """发布并发冲突或当前状态不可发布。"""


class BatchNotPublishableError(PublishConflictError):
    pass


class AlreadyPublishedError(PublishConflictError):
    pass


class ConcurrentPublishError(PublishConflictError):
    pass


class PublishFailedError(InboxError):
    """发布过程中下沉失败，批次已整体回滚。"""

    def __init__(self, batch_id: str, reason: str) -> None:
        super().__init__(f"批次 {batch_id} 发布失败并已回滚：{reason}")
        self.batch_id = batch_id
        self.reason = reason


class WithdrawalNotAllowedError(InboxError):
    pass
