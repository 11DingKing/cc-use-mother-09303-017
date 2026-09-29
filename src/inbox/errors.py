"""收件服务的领域错误。"""
from __future__ import annotations


class InboxError(Exception):
    """所有收件服务错误的基类。"""


class NotFound(InboxError):
    """参与方、映射版本、批次或数据行不存在。"""


class InvalidMappingSpec(InboxError):
    """字段映射版本定义不合法。"""


class BadFileFormat(InboxError):
    """上传文件无法按映射版本声明的格式解析。"""


class PermissionDenied(InboxError):
    """当前角色无权执行该操作。"""


class InvalidState(InboxError):
    """实体当前状态不允许该操作（例如撤回一个未发布的批次）。"""


class RowNotEditable(InboxError):
    """只有隔离中的行允许受控修订。"""


class NothingToPublish(InboxError):
    """批次中没有核验通过、等待发布的行。"""


class PublicationBusy(InboxError):
    """发布闸门正被其他发布者占用，等待超时。"""

    def __init__(self, scope: str) -> None:
        super().__init__(f"发布范围 {scope} 正在发布中")
        self.scope = scope


class PublishConflict(InboxError):
    """正式数据中已存在相同业务键，整批发布必须回滚。"""

    def __init__(self, business_keys: list[str]) -> None:
        super().__init__("业务键与正式数据冲突：" + "、".join(business_keys))
        self.business_keys = business_keys
