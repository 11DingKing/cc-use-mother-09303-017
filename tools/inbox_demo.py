"""收件服务端到端演示：多格式接收 → 逐行核验隔离 → 受控修订 →
原子发布（含失败回滚）→ 撤回，并打印每行状态去向与来源谱系。

运行：python3 tools/inbox_demo.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from data_inbox import (
    Destination,
    FieldMapping,
    InboxService,
    MappingRegistry,
    PublishFailedError,
    Severity,
    allowed_values,
    numeric,
    required,
)
from data_inbox.mapping import Rule


def build_service() -> InboxService:
    registry = MappingRegistry()
    rules = (
        required("R001", "school_code", "院校代码不能为空"),
        required("R003", "program_name", "项目名称不能为空"),
        numeric("R004", "student_count", "学生人数必须为数字"),
        allowed_values("R005", "status", frozenset({"在读", "毕业", "休学"})),
        Rule("W001", "program_name", lambda v: len(v) <= 30,
             "项目名称超过30字", Severity.WARNING),
    )
    registry.register(FieldMapping(
        version="map-cn-v1", submitter="中国合作院校",
        columns={"院校代码": "school_code", "项目名称": "program_name",
                 "学生数": "student_count", "状态": "status"},
        rules=rules,
    ))
    return InboxService(registry)


def main() -> None:
    service = build_service()
    content = (
        "院校代码,项目名称,学生数,状态\n"
        "CN1001,联合培养硕士,42,在读\n"          # 合格
        "CN1002,暑期学分项目,十八,在读\n"        # 人数非数字 → 隔离
        "CN1003,交换生项目,7,失联\n"            # 状态非法 → 隔离
    ).encode("utf-8")

    batch = service.receive(
        filename="华东合作大学_9月批次.csv",
        content=content,
        submitter="华东合作大学",
        mapping_version="map-cn-v1",
    )
    print(f"批次 {batch.batch_id} 接收完成：{batch.counts()}")

    for row in batch.rows.values():
        info = service.row_trace(row.row_id)
        tail = (
            "违规=" + ",".join(v["rule_id"] for v in info["violations"])
            if info["violations"] else "合格"
        )
        print(f"  行{info['line_no']}: {info['status']} → {info['destination']}（{tail}）")

    # 受控修订两行问题数据
    for line_no, changes, reason in (
        (3, {"student_count": "18"}, "与院校确认人数为18"),
        (4, {"status": "在读"}, "院校更正在读状态"),
    ):
        row = [r for r in batch.rows.values() if r.line_no == line_no][0]
        service.revise_row(row.row_id, editor="数据协调员", reason=reason, changes=changes)

    # 第一次发布：模拟正式库失败 → 必须整体回滚
    service.official_store().fail_next_push("正式数据维护中")
    try:
        service.publish(batch.batch_id, by="报告编制人员")
    except PublishFailedError as exc:
        print(f"发布失败已回滚：{exc.reason}；正式数据现存 {service.official_store().count()} 行")

    # 重试发布：3 行全部一次性进入正式数据
    service.publish(batch.batch_id, by="报告编制人员")
    print(f"发布成功，正式数据现存 {service.official_store().count()} 行；"
          f"批次状态={batch.status.value}")

    # 重复文件：改文件名也会被指纹拦下
    try:
        service.receive(
            filename="改名重传.csv", content=content,
            submitter="华东合作大学", mapping_version="map-cn-v1",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"重复文件被拒收：{exc}")

    # 来源谱系
    lineage = service.batch_lineage(batch.batch_id)
    print("来源谱系事件：")
    for event in lineage["events"]:
        print(f"  {event['seq']:>2}. {event['event_type']} {event['payload']}")

    # 撤回：连带从正式数据移除
    service.withdraw(batch.batch_id, by="数据协调员", reason="院校要求截止前撤换")
    print(f"撤回后正式数据 {service.official_store().count()} 行，"
          f"行去向集合={ {r.destination.value for r in batch.rows.values()} }")


if __name__ == "__main__":
    main()
