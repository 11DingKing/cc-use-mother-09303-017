"""批次、数据行、修订记录与不可变来源台账。"""
from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


def _freeze(value: Any) -> Any:
    """递归冻结台账载荷，杜绝事件写入后被篡改。"""
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, set):
        return frozenset(_freeze(v) for v in value)
    return value

from .enums import BatchStatus, Destination, RowStatus
from .mapping import RuleViolation


@dataclass
class Revision:
    """一次受控修订：谁、为什么、改了什么、核验结果如何。"""

    revision_no: int
    editor: str
    reason: str
    changes: dict[str, str]
    violations: tuple[RuleViolation, ...]
    passed: bool


@dataclass
class DataRow:
    row_id: str
    batch_id: str
    line_no: int
    raw: dict[str, str]
    """提交方原始格式数据（谱系留底）。"""
    mapped: dict[str, str]
    """按批次映射版本解释后的标准字段。"""
    status: RowStatus
    destination: Destination
    violations: tuple[RuleViolation, ...] = ()
    warnings: tuple[RuleViolation, ...] = ()
    revisions: list[Revision] = field(default_factory=list)

    def whereabouts(self) -> dict[str, Any]:
        """说明这一行当前的状态与去向。"""
        return {
            "row_id": self.row_id,
            "batch_id": self.batch_id,
            "line_no": self.line_no,
            "status": self.status.value,
            "destination": self.destination.value,
            "revision_count": len(self.revisions),
            "violations": [
                {"rule_id": v.rule_id, "field": v.field, "message": v.message}
                for v in self.violations
            ],
        }


@dataclass
class Batch:
    batch_id: str
    fingerprint: str
    filename: str
    submitter: str
    mapping_version: str
    received_at: str
    status: BatchStatus = BatchStatus.REGISTERED
    rows: dict[str, DataRow] = field(default_factory=dict)
    version: int = 0
    """乐观版本号：每次状态流转自增，供并发发布做条件更新。"""
    published_at: str | None = None
    published_by: str | None = None
    withdrawn_at: str | None = None
    withdraw_reason: str | None = None

    def rows_by_destination(self, destination: Destination) -> list[DataRow]:
        return [r for r in self.rows.values() if r.destination == destination]

    def counts(self) -> dict[str, int]:
        valid = len(self.rows_by_destination(Destination.STAGING))
        quarantined = len(self.rows_by_destination(Destination.QUARANTINE))
        published = len(self.rows_by_destination(Destination.OFFICIAL))
        withdrawn = len(self.rows_by_destination(Destination.WITHDRAWN))
        return {
            "total": len(self.rows),
            "valid_staging": valid,
            "quarantined": quarantined,
            "published": published,
            "withdrawn": withdrawn,
        }


@dataclass(frozen=True)
class LedgerEntry:
    seq: int
    timestamp: str
    event_type: str
    payload: dict[str, Any]


class Ledger:
    """只增不改的来源谱系台账。

    发布失败、重复拒收、撤回、换版、修订等一切关键动作都在此留痕，
    任何状态流转都不允许改写或删除既有条目。
    """

    def __init__(self) -> None:
        self._entries: list[LedgerEntry] = []

    def append(
        self,
        clock: Any,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> LedgerEntry:
        entry = LedgerEntry(
            seq=len(self._entries) + 1,
            timestamp=clock.now(),
            event_type=event_type,
            payload=_freeze(dict(payload or {})),
        )
        self._entries.append(entry)
        return entry

    def all(self) -> tuple[LedgerEntry, ...]:
        return tuple(self._entries)

    def for_batch(self, batch_id: str) -> tuple[LedgerEntry, ...]:
        return tuple(
            e for e in self._entries if e.payload.get("batch_id") == batch_id
        )

    def for_row(self, row_id: str) -> tuple[LedgerEntry, ...]:
        return tuple(
            e for e in self._entries if e.payload.get("row_id") == row_id
        )

    def event_types(self) -> list[str]:
        return [e.event_type for e in self._entries]
