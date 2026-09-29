"""文件指纹与多格式解析。

当前内置 CSV（各国院校常见导出格式），解析层只负责把字节内容变成
“原始行列”，不做任何字段解释——解释交给映射版本，保证换版后原始
数据仍可按新版重新映射。
"""
from __future__ import annotations

import csv
import hashlib
import io


def fingerprint(content: bytes) -> str:
    """文件指纹：sha256，用于重复文件识别。"""
    return hashlib.sha256(content).hexdigest()


def parse_csv(content: bytes) -> list[dict[str, str]]:
    """解析带表头的 CSV，返回原始行（键为提交方列名）。"""
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return []
    rows: list[dict[str, str]] = []
    for raw in reader:
        rows.append({(k or "").strip(): (v or "").strip() for k, v in raw.items()})
    return rows
