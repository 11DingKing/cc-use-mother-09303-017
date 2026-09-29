"""字段映射版本与逐行适用规则。

各国院校上传格式不同：每个映射版本描述“提交方列名 → 标准字段”，
并携带该版本适用的核验规则。映射可换版，停用的版本不能用于新批次，
但历史批次始终按其登记时的映射版本解释，保证谱系可重放。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Pattern

from .errors import UnknownMappingError


class Severity(str, Enum):
    ERROR = "错误"          # 不合格：行进隔离区
    WARNING = "提示"        # 不阻断发布，仅记录


@dataclass(frozen=True)
class RuleViolation:
    rule_id: str
    field: str
    message: str
    severity: Severity


@dataclass(frozen=True)
class FieldMapping:
    """一个字段映射版本。"""

    version: str
    submitter: str
    columns: dict[str, str]
    """提交方原始列名 → 标准字段名。"""
    rules: tuple["Rule", ...]
    retired: bool = False
    note: str = ""

    def map_row(self, raw: dict[str, str]) -> dict[str, str]:
        """把一行原始数据映射为标准字段；缺失列得到空串。"""
        return {
            standard: (raw.get(source) or "").strip()
            for source, standard in self.columns.items()
        }


@dataclass(frozen=True)
class Rule:
    """逐行核验规则：对映射后的标准字段值进行判定。"""

    rule_id: str
    field: str
    check: Callable[[str], bool]
    message: str
    severity: Severity = Severity.ERROR

    def evaluate(self, mapped: dict[str, str]) -> RuleViolation | None:
        value = mapped.get(self.field, "")
        if self.check(value):
            return None
        return RuleViolation(self.rule_id, self.field, self.message, self.severity)


# ---- 常用规则构造器 -------------------------------------------------------

def required(rule_id: str, field_name: str, label: str = "不能为空") -> Rule:
    return Rule(rule_id, field_name, lambda v: bool(v), label)


def regex(rule_id: str, field_name: str, pattern: Pattern[str], label: str) -> Rule:
    return Rule(
        rule_id, field_name, lambda v: bool(pattern.fullmatch(v or "")), label
    )


def numeric(rule_id: str, field_name: str, label: str = "必须为数字") -> Rule:
    def _is_number(v: str) -> bool:
        try:
            float(v)
            return True
        except ValueError:
            return False

    return Rule(rule_id, field_name, _is_number, label)


def allowed_values(
    rule_id: str, field_name: str, values: frozenset[str], label: str | None = None
) -> Rule:
    text = label or f"取值必须属于：{sorted(values)}"
    return Rule(rule_id, field_name, lambda v: v in values, text)


# ---- 映射版本登记册 -------------------------------------------------------

@dataclass
class MappingRegistry:
    _mappings: dict[str, FieldMapping] = field(default_factory=dict)

    def register(self, mapping: FieldMapping) -> None:
        self._mappings[mapping.version] = mapping

    def get(self, version: str) -> FieldMapping:
        try:
            return self._mappings[version]
        except KeyError:
            raise UnknownMappingError(version) from None

    def retire(self, version: str) -> FieldMapping:
        """停用某版本：对象不可变，以替换实例实现。"""
        current = self.get(version)
        retired = FieldMapping(
            version=current.version,
            submitter=current.submitter,
            columns=current.columns,
            rules=current.rules,
            retired=True,
            note=current.note,
        )
        self._mappings[version] = retired
        return retired

    def versions(self) -> list[str]:
        return sorted(self._mappings)
