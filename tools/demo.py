"""端到端演示：两所格式不同的院校上传，合格数据发布、问题行隔离修订。

运行：python3 tools/demo.py
数据落在内存库，不写磁盘，仅用于走查流程。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inbox import InboxService, PartyRole, PublishConflict  # noqa: E402

# 甲国 A 校：英文表头 CSV
EN_MAPPING = {
    "record_id": "记录编号",
    "school": "院校代码",
    "program": "项目代码",
    "name": "学生姓名",
    "id_no": "学生证件号",
    "score": "考核成绩",
    "year": "报告年度",
}
# 乙国 B 校：中文表头 TSV
CN_MAPPING = {
    "记录编号": "记录编号",
    "院校代码": "院校代码",
    "项目代码": "项目代码",
    "学生姓名": "学生姓名",
    "学生证件号": "学生证件号",
    "考核成绩": "考核成绩",
    "报告年度": "报告年度",
}


def main() -> None:
    svc = InboxService(":memory:")
    svc.register_party("school-a", "甲国A院校", PartyRole.SUBMITTER)
    svc.register_party("school-b", "乙国B院校", PartyRole.SUBMITTER)
    svc.register_party("coord", "李协调", PartyRole.COORDINATOR)
    svc.register_party("compiler", "王编制", PartyRole.COMPILER)
    svc.register_mapping("a-csv-v1", "school-a", "csv", EN_MAPPING)
    svc.register_mapping("b-tsv-v1", "school-b", "tsv", CN_MAPPING)

    # A 校文件：1 条合格、1 条成绩越界
    csv_content = (
        "record_id,school,program,name,id_no,score,year\n"
        "R-001,10001,2026,Alice,P111111,88.5,2026\n"
        "R-002,10001,2026,Bob,P222222,150,2026\n"
    ).encode("utf-8")
    receipt_a = svc.receive_file(
        "school-a", "a.csv", csv_content, mapping_version_id="a-csv-v1"
    )
    print(f"[接收] A 校批次 {receipt_a.submission_id}：指纹 {receipt_a.fingerprint[:12]}…"
          f" 合格 {receipt_a.valid_rows} 隔离 {receipt_a.quarantined_rows}")

    # 重复上传同一文件 → 标记重复，不重复入库
    dup = svc.receive_file("school-a", "a-copy.csv", csv_content, mapping_version_id="a-csv-v1")
    print(f"[查重] 重复文件指向原批次：{dup.duplicate_of_submission_id}")

    # B 校文件：TSV 格式、中文表头，1 条合格
    tsv_content = (
        "记录编号\t院校代码\t项目代码\t学生姓名\t学生证件号\t考核成绩\t报告年度\n"
        "R-201\t20002\t2026\t乙同学\tQ999999\t55\t2026\n"
    ).encode("utf-8")
    receipt_b = svc.receive_file(
        "school-b", "b.tsv", tsv_content, mapping_version_id="b-tsv-v1"
    )
    print(f"[接收] B 校批次 {receipt_b.submission_id}：TSV 映射后合格 {receipt_b.valid_rows} 行")

    # 协调员修订 A 校隔离行
    overview = svc.submission_overview(receipt_a.submission_id)
    bad = next(r for r in overview["rows"] if r["status"] == "隔离")
    result = svc.correct_row(bad["row_id"], "coord", {"考核成绩": "90"})
    print(f"[修订] 行 {bad['line_no']} 重新校验：{result.status}"
          f"（留痕记录 {result.revision_id}）")

    # 一次性原子发布两个批次
    pub_a = svc.publish(receipt_a.submission_id, "compiler")
    pub_b = svc.publish(receipt_b.submission_id, "compiler")
    print(f"[发布] A 批次 {pub_a.publication_id} 入库 {pub_a.published_rows} 行；"
          f"B 批次 {pub_b.publication_id} 入库 {pub_b.published_rows} 行")

    # 用相同业务键再发一批 → 整批回滚，不留半成品
    clash = (
        "record_id,school,program,name,id_no,score,year\n"
        "R-301,10001,2026,Alice-X,P111111,30,2026\n"
    ).encode("utf-8")
    clash_receipt = svc.receive_file(
        "school-a", "a2.csv", clash, mapping_version_id="a-csv-v1"
    )
    try:
        svc.publish(clash_receipt.submission_id, "compiler")
    except PublishConflict as exc:
        print(f"[回滚] 冲突发布被拒：{exc}")

    # 撤回 A 批次：正式数据与行状态同步迁移，谱系指针完整
    withdrawal = svc.withdraw(receipt_a.submission_id, "coord")
    trace = svc.trace_row(overview["rows"][0]["row_id"])
    print(f"[撤回] {withdrawal.publication_id}；行去向：{trace['destination']['official_status']}，"
          f"撤回记录：{trace['destination']['withdrawal_publication_id']}")
    print("[谱系] 正式数据版本链：")
    print(json.dumps(svc.official_history("default", "10001|2026|P111111|2026"),
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
