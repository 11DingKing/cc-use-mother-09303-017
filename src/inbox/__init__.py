"""合作项目数据收件箱完整收件服务。

模块划分：

- model / errors：领域状态、收据结构与错误。
- fingerprint / parsing / mapping / rules：接收、映射与逐行校验。
- database / repository：SQLite 持久化与事务边界。
- service：应用服务（接收、修订、发布、撤回、逐行谱系查询）。
"""
from .errors import (
    BadFileFormat,
    InboxError,
    InvalidMappingSpec,
    InvalidState,
    NothingToPublish,
    NotFound,
    PermissionDenied,
    PublishConflict,
    PublicationBusy,
    RowNotEditable,
)
from .model import (
    BatchStatus,
    OfficialStatus,
    PartyRole,
    PublicationStatus,
    RowStatus,
)
from .service import InboxService

__all__ = [
    "InboxService",
    "InboxError",
    "NotFound",
    "InvalidMappingSpec",
    "BadFileFormat",
    "PermissionDenied",
    "InvalidState",
    "RowNotEditable",
    "NothingToPublish",
    "PublicationBusy",
    "PublishConflict",
    "PartyRole",
    "BatchStatus",
    "RowStatus",
    "PublicationStatus",
    "OfficialStatus",
]
