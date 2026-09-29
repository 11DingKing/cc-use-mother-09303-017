"""逐行校验规则。

规则按映射版本声明的标准字段逐行执行；每条规则返回结构化错误码，
不抛异常、不丢行。规则集合自身带版本号（RULE_SET_VERSION），
批次记录校验时所用规则版本，保证来源谱系可复现。
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from .mapping import BUSINESS_KEY_FIELDS, STANDARD_FIELDS

RULE_SET_VERSION = "rules-2026.1"

_RECORD_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{2,31}$")
_SCHOOL_CODE_RE = re.compile(r"^[0-9]{5}$")
_PROGRAM_CODE_RE = re.compile(r"^[0-9]{4}$")
_STUDENT_ID_RE = re.compile(r"^[0-9A-Za-z]{6,20}$")
_YEAR_MIN, _YEAR_MAX = 2000, 2100
_SCORE_MIN, _SCORE_MAX = 0.0, 100.0
_NAME_MAX_LEN = 60


@dataclass(frozen=True)
class FieldViolation:
    field: str  # 标准字段名（中文）
    code: str
    message: str

    def as_dict(self) -> dict:
        return {"field": self.field, "code": self.code, "message": self.message}


def validate_row(values: dict[str, str]) -> list[FieldViolation]:
    """对映射后的标准字段值执行全部适用规则，返回所有违规。"""
    violations: list[FieldViolation] = []

    def require(field: str) -> str:
        value = values.get(field, "")
        if not value:
            violations.append(FieldViolation(STANDARD_FIELDS[field], "required", "必填字段为空"))
        return value

    record_code = require("record_code")
    school_code = require("school_code")
    program_code = require("program_code")
    student_name = require("student_name")
    student_id = require("student_id")
    report_year_raw = require("report_year")
    score_raw = values.get("score", "")  # 成绩允许缺考为空

    if record_code and not _RECORD_CODE_RE.match(record_code):
        violations.append(
            FieldViolation(STANDARD_FIELDS["record_code"], "format", "须为 3-32 位字母、数字或连字符，且以字母数字开头")
        )
    if school_code and not _SCHOOL_CODE_RE.match(school_code):
        violations.append(FieldViolation(STANDARD_FIELDS["school_code"], "format", "须为 5 位数字"))
    if program_code and not _PROGRAM_CODE_RE.match(program_code):
        violations.append(FieldViolation(STANDARD_FIELDS["program_code"], "format", "须为 4 位数字"))
    if student_name:
        if len(student_name) > _NAME_MAX_LEN:
            violations.append(FieldViolation(STANDARD_FIELDS["student_name"], "length", f"姓名不能超过 {_NAME_MAX_LEN} 个字符"))
        elif student_name.strip() != student_name:
            violations.append(FieldViolation(STANDARD_FIELDS["student_name"], "format", "姓名首尾不能有空白"))
    if student_id and not _STUDENT_ID_RE.match(student_id):
        violations.append(FieldViolation(STANDARD_FIELDS["student_id"], "format", "须为 6-20 位字母或数字"))

    if report_year_raw:
        try:
            year = int(report_year_raw)
            if not _YEAR_MIN <= year <= _YEAR_MAX:
                violations.append(
                    FieldViolation(STANDARD_FIELDS["report_year"], "range", f"年度须在 {_YEAR_MIN}-{_YEAR_MAX} 之间")
                )
        except ValueError:
            violations.append(FieldViolation(STANDARD_FIELDS["report_year"], "format", "年度须为整数"))

    if score_raw:
        try:
            score = float(score_raw)
            if not _SCORE_MIN <= score <= _SCORE_MAX:
                violations.append(
                    FieldViolation(STANDARD_FIELDS["score"], "range", f"成绩须在 {_SCORE_MIN}-{_SCORE_MAX} 之间")
                )
        except ValueError:
            violations.append(FieldViolation(STANDARD_FIELDS["score"], "format", "成绩须为数值"))

    # 业务键字段不允许重复：同一批次内同业务键只接受首行，其余隔离
    # （批次内去重在 repository/service 层处理，此处只保证字段齐全）
    return violations


def business_key(values: dict[str, str]) -> str:
    """拼接业务键。调用前应保证必填校验已过，键字段缺失时用空串占位。"""
    return "|".join(values.get(f, "") for f in BUSINESS_KEY_FIELDS)
