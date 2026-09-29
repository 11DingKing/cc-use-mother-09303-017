"""收件服务端到端回归测试。

覆盖契约四条不变量：多格式映射、隔离错误、原子发布、来源谱系，
以及重复文件、撤回批次、映射换版、多人并发发布等场景。
"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inbox import (
    InboxService,
    InvalidState,
    NothingToPublish,
    PartyRole,
    PermissionDenied,
    PublicationBusy,
    PublishConflict,
    RowNotEditable,
)

# 两所院校使用不同格式的表头；remark 是院校附加列，刻意不映射，
# 接收回执应把它列为「未映射列」而不是静默丢弃
CSV_MAPPING_V1 = {
    "record_id": "记录编号",
    "school": "院校代码",
    "program": "项目代码",
    "name": "学生姓名",
    "id_no": "学生证件号",
    "score": "考核成绩",
    "year": "报告年度",
}
# 文件表头比映射多一列 remark
CSV_HEADER_WITH_EXTRA = [
    "record_id", "school", "program", "name", "id_no", "score", "year", "remark",
]
CSV_MAPPING_V2 = {
    "记录编号": "记录编号",
    "院校代码": "院校代码",
    "项目代码": "项目代码",
    "学生姓名": "学生姓名",
    "学生证件号": "学生证件号",
    "考核成绩": "考核成绩",
    "报告年度": "报告年度",
}


def csv_text(header: list[str], rows: list[list[str]]) -> bytes:
    lines = [",".join(header)]
    lines.extend(",".join(cells) for cells in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


GOOD_ROWS = [
    ["R-001", "10001", "2026", "Alice", "P111111", "88.5", "2026"],
    ["R-002", "10001", "2026", "Bob", "P222222", "91", "2026"],
]
BAD_ROWS = [
    # 院校代码不是 5 位、成绩越界
    ["R-003", "999", "2026", "Carol", "P333333", "150", "2026"],
    # 证件号格式错误 + 缺姓名
    ["R-004", "10001", "2026", "", "X", "70", "2026"],
]


class InboxTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = InboxService(db_path=":memory:")
        self.svc.register_party("school-a", "甲国A院校", PartyRole.SUBMITTER)
        self.svc.register_party("school-b", "乙国B院校", PartyRole.SUBMITTER)
        self.svc.register_party("coord-1", "李协调", PartyRole.COORDINATOR)
        self.svc.register_party("compiler-1", "王编制", PartyRole.COMPILER)
        self.svc.register_mapping("csv-en-v1", "school-a", "csv", CSV_MAPPING_V1)

    def tearDown(self) -> None:
        self.svc.close()

    def _upload(self, rows: list[list[str]], *, mapping: str = "csv-en-v1"):
        padded = [row + ["NA"] for row in rows]
        return self.svc.receive_file(
            "school-a", "batch.csv", csv_text(CSV_HEADER_WITH_EXTRA, padded),
            mapping_version_id=mapping,
        )


class ReceiveAndMappingTest(InboxTestBase):
    def test_good_and_bad_rows_split_on_receive(self) -> None:
        receipt = self._upload([*GOOD_ROWS, *BAD_ROWS])
        self.assertEqual(receipt.status, "校验完成")
        self.assertEqual(receipt.total_rows, 4)
        self.assertEqual(receipt.valid_rows, 2)
        self.assertEqual(receipt.quarantined_rows, 2)
        # 未映射列（remark）在回执中可见，不静默丢失
        self.assertIn("remark", receipt.unmapped_columns)
        overview = self.svc.submission_overview(receipt.submission_id)
        statuses = {r["line_no"]: r["status"] for r in overview["rows"]}
        self.assertEqual(statuses, {2: "合格", 3: "合格", 4: "隔离", 5: "隔离"})
        bad = next(r for r in overview["rows"] if r["line_no"] == 4)
        codes = {e["code"] for e in bad["errors"]}
        self.assertEqual(codes, {"format", "range"})

    def test_fingerprint_recorded_before_validation(self) -> None:
        content = csv_text(list(CSV_MAPPING_V1.keys())[:7], GOOD_ROWS)
        receipt = self.svc.receive_file(
            "school-a", "f.csv", content, mapping_version_id="csv-en-v1"
        )
        overview = self.svc.submission_overview(receipt.submission_id)
        self.assertEqual(len(overview["fingerprint"]), 64)
        self.assertEqual(overview["mapping_version"], "csv-en-v1")
        self.assertEqual(overview["rule_set_version"], "rules-2026.1")

    def test_duplicate_file_is_flagged_not_reprocessed(self) -> None:
        content = csv_text(list(CSV_MAPPING_V1.keys())[:7], GOOD_ROWS)
        first = self.svc.receive_file(
            "school-a", "first.csv", content, mapping_version_id="csv-en-v1"
        )
        second = self.svc.receive_file(
            "school-a", "again.csv", content, mapping_version_id="csv-en-v1"
        )
        self.assertTrue(second.is_duplicate)
        self.assertEqual(second.duplicate_of_submission_id, first.submission_id)
        # 重复批次不产生任何数据行
        self.assertEqual(
            len(self.svc.submission_overview(second.submission_id)["rows"]), 0
        )
        # 同一文件由不同提交方上传不算重复
        self.svc.register_mapping("csv-b-v1", "school-b", "csv", CSV_MAPPING_V1)
        other = self.svc.receive_file(
            "school-b", "first.csv", content, mapping_version_id="csv-b-v1"
        )
        self.assertFalse(other.is_duplicate)

    def test_unparseable_file_never_enters_row_store(self) -> None:
        receipt = self.svc.receive_file(
            "school-a",
            "broken.csv",
            "record_id,school\nR-001,10001,EXTRA\n".encode("utf-8"),
            mapping_version_id="csv-en-v1",
        )
        self.assertEqual(receipt.status, "解析失败")
        self.assertIn("列数", receipt.parse_error)
        overview = self.svc.submission_overview(receipt.submission_id)
        self.assertEqual(overview["counts"]["total"], 0)
        # 指纹与提交方仍保留，谱系可查
        self.assertEqual(len(overview["fingerprint"]), 64)

    def test_reupload_of_same_broken_bytes_is_not_flagged_duplicate(self) -> None:
        broken = "record_id,school\nR-001,10001,EXTRA\n".encode("utf-8")
        first = self.svc.receive_file(
            "school-a", "broken.csv", broken, mapping_version_id="csv-en-v1"
        )
        self.assertEqual(first.status, "解析失败")
        # 同一份坏字节重传：应重新尝试解析并再次标记失败，而不是被当成重复文件
        retry = self.svc.receive_file(
            "school-a", "broken-again.csv", broken, mapping_version_id="csv-en-v1"
        )
        self.assertEqual(retry.status, "解析失败")
        self.assertFalse(retry.is_duplicate)
        self.assertIsNone(retry.duplicate_of_submission_id)
        self.assertNotEqual(first.submission_id, retry.submission_id)

    def test_duplicate_business_keys_within_batch_are_quarantined(self) -> None:
        dup_rows = GOOD_ROWS + [
            ["R-005", "10001", "2026", "Alice Twin", "P111111", "60", "2026"]
        ]
        receipt = self._upload(dup_rows)
        self.assertEqual(receipt.valid_rows, 2)
        self.assertEqual(receipt.quarantined_rows, 1)
        overview = self.svc.submission_overview(receipt.submission_id)
        twin = next(r for r in overview["rows"] if r["line_no"] == 4)
        self.assertEqual(twin["errors"][0]["code"], "duplicate_in_batch")

    def test_multi_format_schools_use_own_mappings(self) -> None:
        # B 院校直接用中文表头 CSV
        self.svc.register_mapping("csv-cn-v1", "school-b", "csv", CSV_MAPPING_V2)
        cn_header = list(CSV_MAPPING_V2.keys())
        content = csv_text(
            cn_header,
            [["R-101", "20002", "2026", "乙同学", "Q999999", "55", "2026"]],
        )
        receipt = self.svc.receive_file(
            "school-b", "cn.csv", content, mapping_version_id="csv-cn-v1"
        )
        self.assertEqual(receipt.valid_rows, 1)
        # TSV 与 JSON 格式同样可解析
        tsv = ("记录编号\t院校代码\t项目代码\t学生姓名\t学生证件号\t考核成绩\t报告年度\n"
               "R-102\t20002\t2026\t丙同学\tQ888888\t44\t2026\n").encode("utf-8")
        self.svc.register_mapping("tsv-cn-v1", "school-b", "tsv", CSV_MAPPING_V2)
        tsv_receipt = self.svc.receive_file(
            "school-b", "cn.tsv", tsv, mapping_version_id="tsv-cn-v1"
        )
        self.assertEqual(tsv_receipt.valid_rows, 1)
        payload = json.dumps(
            [
                {
                    "记录编号": "R-103",
                    "院校代码": "20002",
                    "项目代码": "2026",
                    "学生姓名": "丁同学",
                    "学生证件号": "Q777777",
                    "考核成绩": "33",
                    "报告年度": "2026",
                }
            ]
        ).encode("utf-8")
        self.svc.register_mapping("json-cn-v1", "school-b", "json", CSV_MAPPING_V2)
        json_receipt = self.svc.receive_file(
            "school-b", "cn.json", payload, mapping_version_id="json-cn-v1"
        )
        self.assertEqual(json_receipt.valid_rows, 1)


class CorrectionTest(InboxTestBase):
    def test_coordinator_can_fix_quarantined_row(self) -> None:
        receipt = self._upload([GOOD_ROWS[0], BAD_ROWS[0]])
        overview = self.svc.submission_overview(receipt.submission_id)
        bad_row = next(r for r in overview["rows"] if r["status"] == "隔离")
        result = self.svc.correct_row(
            bad_row["row_id"],
            "coord-1",
            {"院校代码": "10002", "考核成绩": "90"},
        )
        self.assertTrue(result.accepted)
        self.assertEqual(result.status, "合格")
        self.assertEqual(result.valid_rows, 2)
        self.assertEqual(result.quarantined_rows, 0)
        trace = self.svc.submission_overview(receipt.submission_id)
        fixed = next(r for r in trace["rows"] if r["row_id"] == bad_row["row_id"])
        self.assertEqual(fixed["status"], "合格")

    def test_failed_correction_stays_quarantined_and_is_recorded(self) -> None:
        receipt = self._upload([BAD_ROWS[0]])
        row = self.svc.submission_overview(receipt.submission_id)["rows"][0]
        result = self.svc.correct_row(
            row["row_id"], "coord-1", {"考核成绩": "still-bad"}
        )
        self.assertFalse(result.accepted)
        self.assertEqual(result.status, "隔离")
        lineage = self.svc.trace_row(row["row_id"])
        self.assertEqual(len(lineage["revisions"]), 1)
        self.assertFalse(lineage["revisions"][0]["accepted"])
        self.assertEqual(lineage["revisions"][0]["changed_fields"], ["考核成绩"])
        self.assertEqual(lineage["revisions"][0]["editor_id"], "coord-1")
        # 再改一次，修订序号递增
        self.svc.correct_row(
            row["row_id"], "coord-1", {"院校代码": "10002", "考核成绩": "90"}
        )
        lineage = self.svc.trace_row(row["row_id"])
        self.assertEqual([rv["seq"] for rv in lineage["revisions"]], [1, 2])
        self.assertTrue(lineage["revisions"][1]["accepted"])

    def test_only_quarantined_rows_are_editable(self) -> None:
        receipt = self._upload(GOOD_ROWS)
        row = self.svc.submission_overview(receipt.submission_id)["rows"][0]
        with self.assertRaises(RowNotEditable):
            self.svc.correct_row(row["row_id"], "coord-1", {"考核成绩": "10"})

    def test_submitter_cannot_correct(self) -> None:
        receipt = self._upload([BAD_ROWS[0]])
        row = self.svc.submission_overview(receipt.submission_id)["rows"][0]
        with self.assertRaises(PermissionDenied):
            self.svc.correct_row(row["row_id"], "school-a", {"考核成绩": "10"})

    def test_correction_duplicate_key_kept_quarantined(self) -> None:
        rows = [
            ["R-001", "10001", "2026", "Alice", "P111111", "88", "2026"],
            # 第二行成绩越界进入隔离
            ["R-002", "10001", "2026", "Bob", "P222222", "150", "2026"],
        ]
        receipt = self._upload(rows)
        target = self.svc.submission_overview(receipt.submission_id)["rows"][1]
        # 成绩修好，但证件号改成与第一行相同的业务键 → 仍隔离
        result = self.svc.correct_row(
            target["row_id"], "coord-1", {"考核成绩": "77", "学生证件号": "P111111"}
        )
        self.assertFalse(result.accepted)
        self.assertTrue(any(e["code"] == "duplicate_in_batch" for e in result.errors))
        # 证件号保持不冲突后通过
        result = self.svc.correct_row(
            target["row_id"], "coord-1", {"学生证件号": "P333333"}
        )
        self.assertTrue(result.accepted)
        self.assertEqual(result.status, "合格")


class PublicationTest(InboxTestBase):
    def test_valid_rows_publish_atomically(self) -> None:
        receipt = self._upload([*GOOD_ROWS, *BAD_ROWS])
        publish = self.svc.publish(receipt.submission_id, "compiler-1")
        self.assertEqual(publish.status, "已提交")
        self.assertEqual(publish.published_rows, 2)
        overview = self.svc.submission_overview(receipt.submission_id)
        self.assertEqual(overview["status"], "部分发布")
        # 合格行进入正式数据，问题行仍隔离
        destinations = {r["line_no"]: r["status"] for r in overview["rows"]}
        self.assertEqual(destinations[2], "已发布")
        self.assertEqual(destinations[3], "已发布")
        self.assertEqual(destinations[4], "隔离")
        self.assertEqual(destinations[5], "隔离")
        trace = self.svc.trace_row(overview["rows"][0]["row_id"])
        self.assertEqual(trace["destination"]["official_status"], "生效中")
        self.assertEqual(trace["destination"]["publication_id"], publish.publication_id)
        self.assertEqual(trace["current_status"], "已发布")

    def test_fix_then_second_publication_completes_batch(self) -> None:
        receipt = self._upload([GOOD_ROWS[0], BAD_ROWS[0]])
        first = self.svc.publish(receipt.submission_id, "compiler-1")
        self.assertEqual(first.seq, 1)
        overview = self.svc.submission_overview(receipt.submission_id)
        bad = next(r for r in overview["rows"] if r["status"] == "隔离")
        self.svc.correct_row(bad["row_id"], "coord-1", {"院校代码": "10002", "考核成绩": "90"})
        second = self.svc.publish(receipt.submission_id, "compiler-1")
        self.assertEqual(second.seq, 2)
        self.assertEqual(second.published_rows, 1)
        self.assertEqual(
            self.svc.submission_overview(receipt.submission_id)["status"], "已发布"
        )

    def test_conflicting_publication_rolls_back_with_no_half_data(self) -> None:
        first_receipt = self._upload(
            [["R-001", "10001", "2026", "Alice", "P111111", "88", "2026"]]
        )
        self.svc.publish(first_receipt.submission_id, "compiler-1")

        # 第二个批次含相同业务键 + 另一组合法行
        second_receipt = self._upload(
            [
                ["R-101", "10001", "2026", "Alice Again", "P111111", "30", "2026"],
                ["R-102", "10001", "2026", "Zoe", "P555555", "66", "2026"],
            ]
        )
        with self.assertRaises(PublishConflict) as ctx:
            self.svc.publish(second_receipt.submission_id, "compiler-1")
        self.assertIn("10001|2026|P111111|2026", ctx.exception.business_keys)

        overview = self.svc.submission_overview(second_receipt.submission_id)
        # 整批回滚：两行仍是合格，没有一条半成品正式数据
        self.assertTrue(all(r["status"] == "合格" for r in overview["rows"]))
        self.assertEqual(overview["status"], "校验完成")
        failed_pub = next(p for p in overview["publications"] if p["status"] == "已回滚")
        self.assertEqual(failed_pub["published_count"], 0)
        self.assertIsNotNone(failed_pub["failure_reason"])

        # 冲突行修订后可重新发布；当前先验证正式数据只有第一批次的 1 条，
        # 第二批次任何一行（含无关的 R-102）都未留下半成品
        dup_key = "10001|2026|P111111|2026"
        active = self.svc.official_history("default", dup_key)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["source_submission_id"], first_receipt.submission_id)
        self.assertEqual(
            self.svc.official_history("default", "10001|2026|P555555|2026"), []
        )

    def test_publish_with_no_valid_rows_rejected(self) -> None:
        receipt = self._upload(BAD_ROWS)
        with self.assertRaises(NothingToPublish):
            self.svc.publish(receipt.submission_id, "compiler-1")

    def test_submitter_cannot_publish_and_compiler_cannot_receive(self) -> None:
        receipt = self._upload(GOOD_ROWS)
        with self.assertRaises(PermissionDenied):
            self.svc.publish(receipt.submission_id, "school-a")
        content = csv_text(list(CSV_MAPPING_V1.keys())[:7], GOOD_ROWS)
        with self.assertRaises(PermissionDenied):
            self.svc.receive_file(
                "compiler-1", "x.csv", content, mapping_version_id="csv-en-v1"
            )


class WithdrawalTest(InboxTestBase):
    def test_withdraw_pulled_batch_marks_everything(self) -> None:
        receipt = self._upload(GOOD_ROWS)
        publish = self.svc.publish(receipt.submission_id, "compiler-1")
        withdrawal = self.svc.withdraw(receipt.submission_id, "coord-1")
        self.assertEqual(withdrawal.status, "已撤回")
        overview = self.svc.submission_overview(receipt.submission_id)
        self.assertEqual(overview["status"], "已撤回")
        self.assertTrue(all(r["status"] == "已撤回" for r in overview["rows"]))
        statuses = [p["status"] for p in overview["publications"]]
        self.assertEqual(statuses, ["已提交", "已撤回"])
        # 正式数据行带撤回谱系指针
        trace = self.svc.trace_row(overview["rows"][0]["row_id"])
        self.assertEqual(trace["destination"]["official_status"], "已撤回")
        self.assertIsNotNone(trace["destination"]["withdrawn_at"])
        history = self.svc.official_history(
            "default", "10001|2026|P111111|2026"
        )
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["withdrawal_publication_id"], withdrawal.publication_id)
        # 未发布批次不可撤回
        fresh = self._upload([BAD_ROWS[0]])
        with self.assertRaises(InvalidState):
            self.svc.withdraw(fresh.submission_id, "coord-1")
        # 已撤回批次不可重复撤回
        with self.assertRaises(InvalidState):
            self.svc.withdraw(receipt.submission_id, "coord-1")

    def test_withdraw_partial_batch_keeps_unpublished_rows(self) -> None:
        receipt = self._upload([*GOOD_ROWS, *BAD_ROWS])
        self.svc.publish(receipt.submission_id, "compiler-1")
        self.svc.withdraw(receipt.submission_id, "compiler-1")
        overview = self.svc.submission_overview(receipt.submission_id)
        # 两条发布行被撤回，两条问题行仍隔离，批次回到校验完成
        self.assertEqual(overview["status"], "校验完成")
        remaining = {r["line_no"]: r["status"] for r in overview["rows"]}
        self.assertEqual(remaining[2], "已撤回")
        self.assertEqual(remaining[3], "已撤回")
        self.assertEqual(remaining[4], "隔离")
        self.assertEqual(remaining[5], "隔离")


class MappingVersionTest(InboxTestBase):
    def test_mapping_upgrade_binds_new_batch_to_new_version(self) -> None:
        old_content = csv_text(list(CSV_MAPPING_V1.keys())[:7], GOOD_ROWS)
        old = self.svc.receive_file(
            "school-a", "old.csv", old_content, mapping_version_id="csv-en-v1"
        )
        # 换版：新建 v2（中文表头），停用 v1；历史批次谱系不变
        self.svc.register_mapping("csv-en-v2", "school-a", "csv", CSV_MAPPING_V2)
        self.svc.retire_mapping("csv-en-v1")
        new_content = csv_text(list(CSV_MAPPING_V2.keys()), [
            ["R-201", "10001", "2026", "新同学", "P666666", "77", "2026"]
        ])
        new = self.svc.receive_file(
            "school-a", "new.csv", new_content, mapping_version_id="csv-en-v2"
        )
        self.assertEqual(new.valid_rows, 1)
        self.assertEqual(
            self.svc.submission_overview(old.submission_id)["mapping_version"],
            "csv-en-v1",
        )
        self.assertEqual(
            self.svc.submission_overview(new.submission_id)["mapping_version"],
            "csv-en-v2",
        )
        old_trace = self.svc.trace_row(
            self.svc.submission_overview(old.submission_id)["rows"][0]["row_id"]
        )
        self.assertEqual(old_trace["mapping_version"], "csv-en-v1")

    def test_same_spec_twice_rejected(self) -> None:
        from inbox import InvalidMappingSpec

        with self.assertRaises(InvalidMappingSpec):
            self.svc.register_mapping("csv-en-v1-dup", "school-a", "csv", CSV_MAPPING_V1)


class ConcurrentPublishTest(unittest.TestCase):
    """多人并发发布：同范围经闸门串行化，失败不留半成品。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "inbox.db")
        bootstrap = InboxService(self.db_path)
        bootstrap.register_party("school-a", "A院校", PartyRole.SUBMITTER)
        bootstrap.register_party("c1", "编制一", PartyRole.COMPILER)
        bootstrap.register_party("c2", "编制二", PartyRole.COMPILER)
        bootstrap.register_mapping("m1", "school-a", "csv", CSV_MAPPING_V1)
        bootstrap.close()
        self.header = list(CSV_MAPPING_V1.keys())[:7]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _prepare_batch(self, rows: list[list[str]]) -> str:
        svc = InboxService(self.db_path)
        try:
            receipt = svc.receive_file(
                "school-a", "b.csv", csv_text(self.header, rows), mapping_version_id="m1"
            )
            return receipt.submission_id
        finally:
            svc.close()

    def test_concurrent_disjoint_batches_both_commit(self) -> None:
        b1 = self._prepare_batch(
            [["R-001", "10001", "2026", "A1", "P111111", "80", "2026"]]
        )
        b2 = self._prepare_batch(
            [["R-002", "10001", "2026", "A2", "P222222", "81", "2026"]]
        )
        results: dict[str, object] = {}

        def run(name: str, compiler: str, submission: str) -> None:
            svc = InboxService(self.db_path)
            try:
                results[name] = svc.publish(submission, compiler, wait_timeout=30)
            except Exception as exc:  # noqa: BLE001 - 记录到结果字典
                results[name] = exc
            finally:
                svc.close()

        t1 = threading.Thread(target=run, args=("t1", "c1", b1))
        t2 = threading.Thread(target=run, args=("t2", "c2", b2))
        t1.start(); t2.start(); t1.join(); t2.join()
        receipts = list(results.values())
        self.assertEqual(len(receipts), 2)
        self.assertTrue(all(hasattr(r, "publication_id") for r in receipts))

        check = InboxService(self.db_path)
        try:
            for key in ("10001|2026|P111111|2026", "10001|2026|P222222|2026"):
                history = check.official_history("default", key)
                active = [h for h in history if h["status"] == "生效中"]
                self.assertEqual(len(active), 1, key)
        finally:
            check.close()

    def test_concurrent_conflicting_batches_one_commits_one_rolls_back(self) -> None:
        b1 = self._prepare_batch(
            [["R-001", "10001", "2026", "A1", "P111111", "80", "2026"]]
        )
        b2 = self._prepare_batch(
            [
                ["R-010", "10001", "2026", "A1X", "P111111", "40", "2026"],
                ["R-011", "10001", "2026", "A2", "P222222", "41", "2026"],
            ]
        )
        results: dict[str, object] = {}
        errors: dict[str, BaseException] = {}

        def run(name: str, compiler: str, submission: str) -> None:
            svc = InboxService(self.db_path)
            try:
                results[name] = svc.publish(submission, compiler, wait_timeout=30)
            except BaseException as exc:  # noqa: BLE001
                errors[name] = exc
            finally:
                svc.close()

        t1 = threading.Thread(target=run, args=("t1", "c1", b1))
        t2 = threading.Thread(target=run, args=("t2", "c2", b2))
        t1.start(); t2.start(); t1.join(); t2.join()

        # 恰好一方成功，一方冲突回滚（谁先抢到闸门不确定）；
        # 绝不可能出现两条生效中同键记录
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(next(iter(errors.values())), PublishConflict)

        check = InboxService(self.db_path)
        try:
            dup_key = "10001|2026|P111111|2026"
            active = [h for h in check.official_history("default", dup_key) if h["status"] == "生效中"]
            self.assertEqual(len(active), 1)
            # 失败方整批回滚：R-011 也不能偷偷入库
            other = [
                h for h in check.official_history("default", "10001|2026|P222222|2026")
                if h["status"] == "生效中"
            ]
            # 若 b1 获胜：b2 整批回滚，P222222 不在正式数据中（共 1 条）；
            # 若 b2 获胜：其两行都入库（共 2 条）。任何情况下生效中同键记录恰为一条。
            winner_count = len(other)
            self.assertIn(winner_count, (0, 1))
            expected_total = 2 if winner_count == 1 else 1
            total_active = check.conn.execute(
                "SELECT COUNT(*) AS c FROM official_records WHERE status='生效中'"
            ).fetchone()["c"]
            self.assertEqual(total_active, expected_total)
        finally:
            check.close()

    def test_concurrent_publish_same_batch_does_not_double_publish(self) -> None:
        submission = self._prepare_batch(
            [["R-001", "10001", "2026", "A1", "P111111", "80", "2026"]]
        )
        outcomes: list[object] = []

        def run(compiler: str) -> None:
            svc = InboxService(self.db_path)
            try:
                outcomes.append(("ok", svc.publish(submission, compiler, wait_timeout=30)))
            except BaseException as exc:  # noqa: BLE001
                outcomes.append(("err", exc))
            finally:
                svc.close()

        t1 = threading.Thread(target=run, args=("c1",))
        t2 = threading.Thread(target=run, args=("c2",))
        t1.start(); t2.start(); t1.join(); t2.join()
        oks = [o for o in outcomes if o[0] == "ok"]
        errs = [o for o in outcomes if o[0] == "err"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(errs), 1)
        self.assertIsInstance(errs[0][1], (InvalidState, NothingToPublish))


class LineageTest(InboxTestBase):
    def test_every_row_explains_its_status_and_destination(self) -> None:
        receipt = self._upload([GOOD_ROWS[0], BAD_ROWS[1]])
        overview = self.svc.submission_overview(receipt.submission_id)
        good_row, bad_row = overview["rows"]
        good_trace = self.svc.trace_row(good_row["row_id"])
        self.assertEqual(good_trace["filename"], "batch.csv")
        self.assertEqual(len(good_trace["fingerprint"]), 64)
        self.assertEqual(good_trace["submitter_id"], "school-a")
        self.assertEqual(good_trace["source_values"]["record_id"], "R-001")
        self.assertEqual(good_trace["mapped_values"]["record_code"], "R-001")
        self.assertIsNone(good_trace["destination"])

        self.svc.publish(receipt.submission_id, "compiler-1")
        good_trace = self.svc.trace_row(good_row["row_id"])
        self.assertEqual(good_trace["destination"]["official_status"], "生效中")
        self.assertEqual(good_trace["destination"]["scope"], "default")

        bad_trace = self.svc.trace_row(bad_row["row_id"])
        self.assertEqual(bad_trace["current_status"], "隔离")
        self.assertTrue(bad_trace["errors"])
        self.assertIsNone(bad_trace["destination"])

    def test_busy_gate_when_timeout_is_zero(self) -> None:
        receipt = self._upload(GOOD_ROWS)
        # 手工塞入一个持有中的闸门
        from inbox.clock import utc_now_iso
        from datetime import datetime, timedelta

        now = datetime.fromisoformat(utc_now_iso())
        future = (now + timedelta(seconds=30)).isoformat(timespec="microseconds")
        with self.svc.conn:
            self.svc.conn.execute(
                "INSERT INTO publish_gates(scope, holder_publication_id, acquired_at, lease_expires_at)"
                " VALUES ('default','pub_hold',?,?)",
                (utc_now_iso(), future),
            )
        with self.assertRaises(PublicationBusy):
            self.svc.publish(receipt.submission_id, "compiler-1", wait_timeout=0)


if __name__ == "__main__":
    unittest.main()
