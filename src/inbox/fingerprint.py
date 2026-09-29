"""文件指纹：重复文件判定的唯一依据。"""
from __future__ import annotations

import hashlib


def sha256_fingerprint(content: bytes) -> str:
    """计算上传字节内容的 SHA-256 指纹。"""
    return hashlib.sha256(content).hexdigest()
