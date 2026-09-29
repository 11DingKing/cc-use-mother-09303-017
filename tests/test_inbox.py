"""收件服务端到端测试：覆盖接收、隔离、修订、换版、原子发布、
撤回、重复文件、并发发布、失败回滚与来源谱系。"""
from __future__ import annotations

import re
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from data_inbox import (
    BatchStatus,
    Destination,
    ConcurrentPublishError,
    DuplicateSubmissionError,
    EmptySubmissionError,
    FieldMapping,
    InboxService,
    MappingRegistry,
    MappingRetiredError,
    OfficialDataStore,
    PublishConflictError,
    PublishFailedError,
    RevisionError,
    RowStatus,
    Severity,
    WithdrawalNotAllowedError,
    allowed_values,
    numeric,
    regex,
    required,
)
from data_inbox.support import Clock


class FixedClock(Clock):
    def __init__(self) -> None:
        self.t = 0

    def now(self) -> str:
        self.t += 1
        return f"2026-09-29T00:00:{self.t:02d}Z"


def make_registry() -> MappingRegistry:
    """两国院校两种列名格式，映射到同一套标准字段。"""
    registry = MappingRegistry()
    common_rules = (
        required("R001", "school_code", "院校代码不能为空"),
        regex("R002", "school_code", re.compile(r"[A-Z]{2}\d{4}"), "院校代码格式应为2位字母+4位数字"),
        required("R003", "program_name", "项目名称不能为空"),
        numeric("R004", "student_count", "学生人数必须为数字"),
        allowed_values("R005", "status", frozenset({"在读", "毕业", "休学"})),
    )
    registry.register(
        FieldMapping(
            version="map-cn-v1",
            submitter="中国合作院校",
            columns={
                "院校代码": "school_code",
                "项目名称": "program_name",
                "学生数": "student_count",
                "状态": "status",
            },
            rules=common_rules,
        )
    )
    registry.register(
        FieldMapping(
            version="map-uk-v1",
            submitter="英国合作院校",
            columns={
                "Institution Code": "school_code",
                "Programme": "program_name",
                "Students": "student_count",
                "State": "status",
            },
            rules=common_rules,
        )
    )
    registry.register(
        FieldMapping(
            version="map-cn-v2",
            submitter="中国合作院校",
            columns={
                "代码": "school_code",
                "项目名称": "program_name",
                "学生数": "student_count",
                "在读状态": "status",
            },
            rules=common_rules,
            note="院校改了两列列名",
        )
    )
    return registry


CN_CSV = """院校代码,项目名称,学生数,状态
CN1001,联合培养硕士,42,在读
CN1002,暑期学分项目,十八,在读
CN99,联合培养博士,5,在读
CN1004,,12,毕业
CN1005,交换生项目,7,失联
"""


class ReceiveAndValidateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = InboxService(make_registry(), clock=FixedClock())

    def test_fingerprint_submitter_mapping_version_recorded_first(self) -> None:
        batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        lineage = self.service.batch_lineage(batch.batch_id)
        self.assertEqual(len(lineage["fingerprint"]), 64)  # sha256
        self.assertEqual(lineage["submitter"], "华东合作大学")
        self.assertEqual(lineage["mapping_version"], "map-cn-v1")
        # 接收事件携带三类来源信息
        received = [e for e in lineage["events"] if e["event_type"] == "file.received"][0]
        self.assertEqual(received["payload"]["fingerprint"], lineage["fingerprint"])

    def test_good_rows_staging_bad_rows_quarantined_not_whole_package_reject(self) -> None:
        batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        counts = batch.counts()
        self.assertEqual(counts["total"], 5)
        self.assertEqual(counts["valid_staging"], 1)   # 仅第1行全合格
        self.assertEqual(counts["quarantined"], 4)
        self.assertEqual(batch.status, BatchStatus.VALIDATED)

        bad = [
            r for r in batch.rows.values() if r.destination == Destination.QUARANTINE
        ]
        # 每个隔离行都带具体违规规则
        reasons = {r.line_no: {v.rule_id for v in r.violations} for r in bad}
        self.assertEqual(reasons[3], {"R004"})        # 十八 非数字
        self.assertEqual(reasons[4], {"R002"})        # CN99 代码格式错
        self.assertEqual(reasons[5], {"R003"})        # 项目名称空
        self.assertEqual(reasons[6], {"R005"})        # 状态取值非法

    def test_different_country_format_maps_to_same_standard_fields(self) -> None:
        uk_csv = (
            "Institution Code,Programme,Students,State\r\n"
            "UK2001,Joint Master,31,在读\r\n"
        ).encode("utf-8")
        batch = self.service.receive(
            filename="uk.csv",
            content=uk_csv,
            submitter="英伦大学",
            mapping_version="map-uk-v1",
        )
        row = next(iter(batch.rows.values()))
        self.assertEqual(row.mapped["school_code"], "UK2001")
        self.assertEqual(row.destination, Destination.STAGING)

    def test_empty_file_rejected_before_persist(self) -> None:
        with self.assertRaises(EmptySubmissionError):
            self.service.receive(
                filename="empty.csv",
                content="院校代码,项目名称\n".encode("utf-8"),
                submitter="x",
                mapping_version="map-cn-v1",
            )

    def test_retired_mapping_rejected_for_new_batch(self) -> None:
        registry = make_registry()
        registry.retire("map-cn-v1")
        service = InboxService(registry, clock=FixedClock())
        with self.assertRaises(MappingRetiredError):
            service.receive(
                filename="cn.csv",
                content=CN_CSV.encode("utf-8"),
                submitter="华东合作大学",
                mapping_version="map-cn-v1",
            )

    def test_duplicate_file_rejected_and_lineage_kept(self) -> None:
        kwargs = dict(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        first = self.service.receive(**kwargs)
        with self.assertRaises(DuplicateSubmissionError) as ctx:
            self.service.receive(**kwargs)
        self.assertEqual(ctx.exception.existing_batch_id, first.batch_id)
        types = self.service.ledger().event_types()
        self.assertEqual(types.count("file.duplicate_rejected"), 1)
        # 同内容改文件名/提交方仍然是重复文件（以字节指纹为准）
        with self.assertRaises(DuplicateSubmissionError):
            self.service.receive(
                filename="renamed.csv",
                content=CN_CSV.encode("utf-8"),
                submitter="另一个院校",
                mapping_version="map-cn-v1",
            )


class ControlledRevisionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = InboxService(make_registry(), clock=FixedClock())
        self.batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )

    def test_revision_passes_moves_row_to_staging(self) -> None:
        row = [r for r in self.batch.rows.values() if r.line_no == 3][0]
        fixed = self.service.revise_row(
            row.row_id,
            editor="协调员王",
            reason="与院校电话确认学生数为18",
            changes={"student_count": "18"},
        )
        self.assertEqual(fixed.destination, Destination.STAGING)
        self.assertEqual(fixed.status, RowStatus.VALID)
        self.assertEqual(len(fixed.revisions), 1)
        self.assertTrue(fixed.revisions[0].passed)

    def test_failed_revision_keeps_row_quarantined_and_logged(self) -> None:
        row = [r for r in self.batch.rows.values() if r.line_no == 3][0]
        still_bad = self.service.revise_row(
            row.row_id,
            editor="协调员王",
            reason="尝试修订",
            changes={"student_count": "十八人"},
        )
        self.assertEqual(still_bad.destination, Destination.QUARANTINE)
        self.assertFalse(still_bad.revisions[0].passed)
        # 修订履历累积
        self.service.revise_row(
            row.row_id, editor="协调员王", reason="再次修订", changes={"student_count": "20"}
        )
        self.assertEqual(len(still_bad.revisions), 2)
        self.assertEqual(still_bad.destination, Destination.STAGING)

    def test_revision_requires_editor_and_reason(self) -> None:
        row = [r for r in self.batch.rows.values() if r.line_no == 3][0]
        with self.assertRaises(RevisionError):
            self.service.revise_row(row.row_id, editor="", reason="x", changes={"student_count": "1"})
        with self.assertRaises(RevisionError):
            self.service.revise_row(row.row_id, editor="王", reason="", changes={"student_count": "1"})

    def test_staging_row_cannot_be_revised(self) -> None:
        row = [r for r in self.batch.rows.values() if r.line_no == 2][0]
        with self.assertRaises(RevisionError):
            self.service.revise_row(
                row.row_id, editor="王", reason="x", changes={"program_name": "新名"}
            )


class AtomicPublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sink = OfficialDataStore()
        self.service = InboxService(make_registry(), clock=FixedClock(), sink=self.sink)
        self.batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )

    def test_only_valid_rows_publish_atomically(self) -> None:
        self.service.publish(self.batch.batch_id, by="报告编制员李")
        self.assertEqual(self.sink.count(), 1)  # 只有1行核验通过
        self.assertEqual(self.batch.status, BatchStatus.PUBLISHED)
        entry = self.sink.all()[0]
        self.assertEqual(entry.data["school_code"], "CN1001")
        self.assertEqual(entry.submitter, "华东合作大学")
        self.assertEqual(entry.mapping_version, "map-cn-v1")
        # 隔离行原封不动保留，批次仍可在修订合格后再次发布
        self.assertEqual(self.batch.counts()["quarantined"], 4)

        row3 = [r for r in self.batch.rows.values() if r.line_no == 3][0]
        self.service.revise_row(
            row3.row_id, editor="王", reason="确认", changes={"student_count": "18"}
        )
        self.service.publish(self.batch.batch_id, by="李")
        self.assertEqual(self.sink.count(), 2)
        self.assertEqual(row3.destination, Destination.OFFICIAL)

    def test_nothing_published_when_no_valid_rows(self) -> None:
        all_bad = "院校代码,项目名称,学生数,状态\nX, ,x,奇怪\n".encode("utf-8")
        batch = self.service.receive(
            filename="bad.csv",
            content=all_bad,
            submitter="某院校",
            mapping_version="map-cn-v1",
        )
        with self.assertRaises(Exception):
            self.service.publish(batch.batch_id, by="李")
        self.assertEqual(self.sink.count(), 0)

    def test_failed_push_rolls_back_with_no_half_product(self) -> None:
        self.sink.fail_next_push("正式库磁盘故障")
        with self.assertRaises(PublishFailedError):
            self.service.publish(self.batch.batch_id, by="李")
        # 正式数据无半成品
        self.assertEqual(self.sink.count(), 0)
        # 批次状态回滚、行仍在暂存区，可重试
        self.assertEqual(self.batch.status, BatchStatus.VALIDATED)
        row = [r for r in self.batch.rows.values() if r.destination == Destination.STAGING]
        self.assertEqual(len(row), 1)
        self.service.publish(self.batch.batch_id, by="李")
        self.assertEqual(self.sink.count(), 1)
        # 失败事件与后续成功事件都在谱系中
        types = [
            e["event_type"]
            for e in self.service.batch_lineage(self.batch.batch_id)["events"]
        ]
        self.assertIn("batch.publish_failed", types)
        self.assertIn("batch.published", types)

    def test_optimistic_version_conflict_rejected(self) -> None:
        stale = self.batch.version
        # 有人先对批次做了修订，改变了版本
        row3 = [r for r in self.batch.rows.values() if r.line_no == 3][0]
        self.service.revise_row(
            row3.row_id, editor="王", reason="确认", changes={"student_count": "18"}
        )
        with self.assertRaises(ConcurrentPublishError):
            self.service.publish(self.batch.batch_id, by="李", expected_version=stale)
        self.assertEqual(self.sink.count(), 0)
        # 用最新版本可以发布
        self.service.publish(
            self.batch.batch_id, by="李", expected_version=self.batch.version
        )
        self.assertEqual(self.sink.count(), 2)

    def test_concurrent_publishers_serialized_without_duplication(self) -> None:
        # 先把全部行修到合格，制造“多人同时点发布”的场景
        fixes = {3: ("student_count", "18"), 4: ("school_code", "CN1003"),
                 5: ("program_name", "补录项目"), 6: ("status", "在读")}
        for line_no, (field_name, value) in fixes.items():
            row = [r for r in self.batch.rows.values() if r.line_no == line_no][0]
            self.service.revise_row(
                row.row_id, editor="王", reason="补录修订", changes={field_name: value}
            )
        self.assertEqual(self.sink.count(), 0)

        barrier = threading.Barrier(2)
        results: list[BaseException | str] = []

        def publish(name: str) -> None:
            barrier.wait()
            try:
                self.service.publish(self.batch.batch_id, by=name)
                results.append("ok")
            except BaseException as exc:  # noqa: BLE001
                results.append(exc)

        t1 = threading.Thread(target=publish, args=("发布员甲",))
        t2 = threading.Thread(target=publish, args=("发布员乙",))
        t1.start(); t2.start(); t1.join(); t2.join()

        self.assertEqual(self.sink.count(), 5)            # 无重复下沉
        self.assertEqual(results.count("ok"), 1)
        other = [r for r in results if r != "ok"][0]
        self.assertIsInstance(other, PublishConflictError)

    def test_warning_severity_does_not_quarantine(self) -> None:
        from data_inbox import Rule
        registry = make_registry()
        registry.register(
            FieldMapping(
                version="map-warn-v1",
                submitter="试验院校",
                columns={"代码": "school_code", "名": "program_name",
                         "人数": "student_count", "态": "status"},
                rules=(
                    Rule("W001", "program_name", lambda v: len(v) <= 20,
                         "名称过长", Severity.WARNING),
                    required("R001", "school_code"),
                    required("R003", "program_name"),
                    numeric("R004", "student_count"),
                    allowed_values("R005", "status", frozenset({"在读", "毕业", "休学"})),
                ),
            )
        )
        service = InboxService(registry, clock=FixedClock())
        batch = service.receive(
            filename="w.csv",
            content="代码,名,人数,态\nAB1234,一个特别特别特别特别特别特别特别长的项目名称,9,在读\n".encode("utf-8"),
            submitter="试验院校",
            mapping_version="map-warn-v1",
        )
        row = next(iter(batch.rows.values()))
        self.assertEqual(row.destination, Destination.STAGING)
        self.assertEqual(row.warnings[0].rule_id, "W001")


class MappingChangeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = InboxService(make_registry(), clock=FixedClock())

    def test_change_mapping_version_reinterprets_raw_and_revalidates(self) -> None:
        # 文件按 v1 列名上传，但登记时误用……这里用 v2 列名格式的文件演示换版
        new_format = "代码,项目名称,学生数,在读状态\nCN1001,联合培养硕士,42,在读\n"
        # 先以 v1 接收：v1 不认识新列名 → 全部映射为空 → 隔离
        batch = self.service.receive(
            filename="cn2.csv",
            content=new_format.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        row = next(iter(batch.rows.values()))
        self.assertEqual(row.destination, Destination.QUARANTINE)
        self.assertEqual(row.mapped["school_code"], "")
        # 原始数据始终保留
        self.assertEqual(row.raw["代码"], "CN1001")

        # 换版到 v2 后按新列名重新解释 → 核验通过
        self.service.change_mapping(batch.batch_id, "map-cn-v2", requester="协调员赵")
        self.assertEqual(batch.mapping_version, "map-cn-v2")
        self.assertEqual(row.mapped["school_code"], "CN1001")
        self.assertEqual(row.destination, Destination.STAGING)

        events = self.service.batch_lineage(batch.batch_id)["events"]
        changed = [e for e in events if e["event_type"] == "mapping.version_changed"][0]
        self.assertEqual(changed["payload"]["old_version"], "map-cn-v1")
        self.assertEqual(changed["payload"]["new_version"], "map-cn-v2")

    def test_cannot_change_mapping_after_publish(self) -> None:
        batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        self.service.publish(batch.batch_id, by="李")
        with self.assertRaises(WithdrawalNotAllowedError):
            self.service.change_mapping(batch.batch_id, "map-cn-v2", requester="赵")


class WithdrawTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sink = OfficialDataStore()
        self.service = InboxService(make_registry(), clock=FixedClock(), sink=self.sink)

    def test_withdraw_unpublished_batch_archives_rows(self) -> None:
        batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        self.service.withdraw(batch.batch_id, by="赵", reason="院校通知数据有误主动撤回")
        self.assertEqual(batch.status, BatchStatus.WITHDRAWN)
        self.assertTrue(all(r.destination == Destination.WITHDRAWN for r in batch.rows.values()))
        with self.assertRaises(WithdrawalNotAllowedError):
            self.service.withdraw(batch.batch_id, by="赵", reason="再撤一次")

    def test_withdraw_published_batch_removes_from_official(self) -> None:
        batch = self.service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        self.service.publish(batch.batch_id, by="李")
        self.assertEqual(self.sink.count(), 1)
        self.service.withdraw(batch.batch_id, by="赵", reason="报告截止前撤换批次")
        self.assertEqual(self.sink.count(), 0)
        lineage = self.service.batch_lineage(batch.batch_id)
        ev = [e for e in lineage["events"] if e["event_type"] == "batch.withdrawn"][0]
        self.assertEqual(ev["payload"]["removed_from_official"], 1)


class LineageAndWhereaboutsTest(unittest.TestCase):
    def test_every_row_explains_its_status_and_destination(self) -> None:
        service = InboxService(make_registry(), clock=FixedClock())
        batch = service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        bad_row = [r for r in batch.rows.values() if r.line_no == 3][0]
        service.revise_row(
            bad_row.row_id, editor="王", reason="第一次没改对", changes={"student_count": "错"}
        )
        service.revise_row(
            bad_row.row_id, editor="王", reason="确认18人", changes={"student_count": "18"}
        )
        trace = service.row_trace(bad_row.row_id)
        self.assertEqual(trace["status"], RowStatus.VALID.value)
        self.assertEqual(trace["destination"], Destination.STAGING.value)
        self.assertEqual(len(trace["revisions"]), 2)
        self.assertEqual(trace["revisions"][0]["reason"], "第一次没改对")
        # 谱系事件覆盖：隔离 → 修订失败 → 修订通过
        event_types = [e["event_type"] for e in trace["events"]]
        self.assertEqual(event_types[0], "row.quarantined")
        self.assertEqual(event_types.count("row.revised"), 2)

    def test_ledger_is_append_only_and_immutable_payloads(self) -> None:
        service = InboxService(make_registry(), clock=FixedClock())
        batch = service.receive(
            filename="cn.csv",
            content=CN_CSV.encode("utf-8"),
            submitter="华东合作大学",
            mapping_version="map-cn-v1",
        )
        service.publish(batch.batch_id, by="李")
        # 谱系与行迹查询结果必须可 JSON 序列化（供接口直接返回）
        import json
        json.dumps(service.batch_lineage(batch.batch_id), ensure_ascii=False)
        any_row = next(iter(batch.rows.values()))
        json.dumps(service.row_trace(any_row.row_id), ensure_ascii=False)
        entries = service.ledger().all()
        seqs = [e.seq for e in entries]
        self.assertEqual(seqs, list(range(1, len(entries) + 1)))  # 连续编号
        first = entries[0]
        with self.assertRaises(Exception):
            first.payload["batch_id"] = "tampered"  # type: ignore[misc]
        # frozen 数据类拒绝改写
        self.assertNotIn("tampered", str(entries[0].payload))


if __name__ == "__main__":
    unittest.main()
