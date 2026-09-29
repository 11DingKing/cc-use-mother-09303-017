"""字段映射版本。

各国院校上传格式不同，映射版本把「源字段名」翻译成统一的标准字段名。
映射本身有版本号、适用于哪个提交方、针对哪种文件格式；历史版本不可变，
换版即新建版本，批次永远绑定接收时的映射版本（来源谱系）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256

from .errors import InvalidMappingSpec
from .parsing import SUPPORTED_FORMATS

# 统一标准字段（所有院校格式最终映射到这套字段）
STANDARD_FIELDS: dict[str, str] = {
    "record_code": "记录编号",
    "school_code": "院校代码",
    "program_code": "项目代码",
    "student_name": "学生姓名",
    "student_id": "学生证件号",
    "score": "考核成绩",
    "report_year": "报告年度",
}
BUSINESS_KEY_FIELDS = ("school_code", "program_code", "student_id", "report_year")
# 中文标准字段名 -> 内部字段键
FIELD_NAME_TO_KEY = {name: key for key, name in STANDARD_FIELDS.items()}


@dataclass(frozen=True)
class MappingVersion:
    version_id: str
    submitter_id: str  # "*" 表示对所有提交方适用
    file_format: str
    column_mapping: dict[str, str]  # 源字段名 -> 标准字段名
    created_at: str

    @property
    def spec_fingerprint(self) -> str:
        """映射规格指纹：同样的列映射内容得到同样的指纹。"""
        payload = json.dumps(
            {
                "submitter_id": self.submitter_id,
                "file_format": self.file_format,
                "column_mapping": dict(sorted(self.column_mapping.items())),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_mapping(
    version_id: str,
    submitter_id: str,
    file_format: str,
    column_mapping: dict[str, str],
    created_at: str,
) -> MappingVersion:
    """构造并校验一个映射版本。"""
    if not version_id or not version_id.strip():
        raise InvalidMappingSpec("映射版本号不能为空")
    if file_format not in SUPPORTED_FORMATS:
        raise InvalidMappingSpec(f"不支持的文件格式：{file_format}")
    if not column_mapping:
        raise InvalidMappingSpec("列映射不能为空")
    unknown = {target for target in column_mapping.values() if target not in FIELD_NAME_TO_KEY}
    if unknown:
        raise InvalidMappingSpec("映射到未知标准字段：" + "、".join(sorted(unknown)))
    sources = [src.strip() for src in column_mapping]
    if any(not src for src in sources):
        raise InvalidMappingSpec("源字段名不能为空")
    if len(sources) != len(set(sources)):
        raise InvalidMappingSpec("源字段名不能重复映射")
    targets = list(column_mapping.values())
    dup_targets = {t for t in targets if targets.count(t) > 1}
    if dup_targets:
        raise InvalidMappingSpec("多个源字段映射到同一标准字段：" + "、".join(sorted(dup_targets)))
    missing_key_fields = [
        STANDARD_FIELDS[f] for f in BUSINESS_KEY_FIELDS if STANDARD_FIELDS[f] not in targets
    ]
    if missing_key_fields:
        raise InvalidMappingSpec("映射必须覆盖业务键字段：" + "、".join(missing_key_fields))
    return MappingVersion(
        version_id=version_id.strip(),
        submitter_id=submitter_id.strip(),
        file_format=file_format.lower(),
        column_mapping=dict(column_mapping),
        created_at=created_at,
    )


def apply_mapping(
    mapping: MappingVersion, source_columns: list[str], source_values: dict[str, str]
) -> tuple[dict[str, str], list[str]]:
    """把一行源数据翻译成标准字段。

    返回 (标准字段值字典, 未被映射的源列名列表)。未映射列不参与校验，
    但会记录在接收回执中，避免静默丢列。
    """
    mapped: dict[str, str] = {}
    for source_name, target_name in mapping.column_mapping.items():
        mapped[FIELD_NAME_TO_KEY[target_name]] = source_values.get(source_name, "")
    unmapped = [col for col in source_columns if col not in mapping.column_mapping]
    return mapped, unmapped
