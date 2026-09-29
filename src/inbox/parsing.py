"""多格式文件解析。

支持院校常见的三种上传格式，全部解析为统一的「原始行」序列：

- csv：逗号分隔，首行表头，UTF-8（兼容 UTF-8-SIG）。
- tsv：制表符分隔，首行表头。
- json：顶层为对象数组，对象键即源字段名。

解析器只负责切分结构，不做任何业务校验，也不丢弃列。
"""
from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass

from .errors import BadFileFormat

CSV = "csv"
TSV = "tsv"
JSON = "json"
SUPPORTED_FORMATS = frozenset({CSV, TSV, JSON})


@dataclass
class RawRow:
    """文件中的一行原始数据。"""

    line_no: int  # 从 1 开始的源文件行号（JSON 为元素序号）
    source_values: dict[str, str]


@dataclass
class ParsedFile:
    columns: list[str]
    rows: list[RawRow]


def parse_file(content: bytes, fmt: str) -> ParsedFile:
    """按声明格式解析字节内容。

    解析失败统一抛 BadFileFormat，绝不把错误行静默入库。
    """
    fmt = fmt.lower()
    if fmt not in SUPPORTED_FORMATS:
        raise BadFileFormat(f"不支持的文件格式：{fmt}（支持 {sorted(SUPPORTED_FORMATS)}）")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BadFileFormat("文件不是有效的 UTF-8 文本") from exc
    if fmt == JSON:
        return _parse_json(text)
    return _parse_delimited(text, "\t" if fmt == TSV else ",")


def _parse_delimited(text: str, delimiter: str) -> ParsedFile:
    try:
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        rows = list(reader)
    except csv.Error as exc:
        raise BadFileFormat(f"分隔格式解析失败：{exc}") from exc
    while rows and not any(cell.strip() for cell in rows[-1]):
        rows.pop()  # 容忍末尾空行
    if not rows:
        raise BadFileFormat("文件为空，缺少表头")
    header = [cell.strip() for cell in rows[0]]
    if not header or any(not name for name in header):
        raise BadFileFormat("表头存在空列名")
    if len(set(header)) != len(header):
        raise BadFileFormat("表头列名重复：" + "、".join(sorted({n for n in header if header.count(n) > 1})))
    raw_rows: list[RawRow] = []
    for offset, cells in enumerate(rows[1:], start=2):
        if not any(cell.strip() for cell in cells):
            continue  # 跳过空行，但不计入数据行
        if len(cells) != len(header):
            raise BadFileFormat(f"第 {offset} 行列数（{len(cells)}）与表头（{len(header)}）不一致")
        values = {name: cells[i].strip() for i, name in enumerate(header)}
        raw_rows.append(RawRow(line_no=offset, source_values=values))
    return ParsedFile(columns=header, rows=raw_rows)


def _parse_json(text: str) -> ParsedFile:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise BadFileFormat(f"JSON 解析失败：第 {exc.lineno} 行 {exc.msg}") from exc
    if not isinstance(data, list):
        raise BadFileFormat("JSON 顶层必须是对象数组")
    columns: list[str] = []
    raw_rows: list[RawRow] = []
    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise BadFileFormat(f"JSON 第 {index} 个元素不是对象")
        for key in item:
            if key not in columns:
                columns.append(key)
        values = {key: ("" if value is None else str(value)).strip() for key, value in item.items()}
        raw_rows.append(RawRow(line_no=index, source_values=values))
    if not raw_rows:
        raise BadFileFormat("JSON 数组为空，缺少数据行")
    return ParsedFile(columns=columns, rows=raw_rows)
