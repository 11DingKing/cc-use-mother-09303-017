"""合作项目数据收件服务。

链路：登记文件指纹/提交方/映射版本 → 多格式映射 → 逐行核验分流
（暂存区 / 隔离区）→ 受控修订 → 核验通过部分一次性原子发布 →
撤回与谱系追溯。并发安全、失败回滚均在本层保证。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable

from .enums import BatchStatus, Destination, RowStatus
from .errors import (
    BatchNotFound,
    BatchNotPublishableError,
    ConcurrentPublishError,
    DuplicateSubmissionError,
    EmptySubmissionError,
    MappingRetiredError,
    PublishFailedError,
    RevisionError,
    RowNotFound,
    WithdrawalNotAllowedError,
)
from .mapping import MappingRegistry, RuleViolation, Severity
from .models import Batch, DataRow, Ledger, Revision
from .parsing import fingerprint, parse_csv
from .support import Clock, Sequence, new_id


# ---- 事件名称（来源谱系台账中的 event_type） ------------------------------

EV_FILE_DUPLICATE = "file.duplicate_rejected"
EV_FILE_RECEIVED = "file.received"
EV_ROW_QUARANTINED = "row.quarantined"
EV_ROW_REVISED = "row.revised"
EV_MAPPING_CHANGED = "mapping.version_changed"
EV_PUBLISH_STARTED = "batch.publish_started"
EV_PUBLISH_REJECTED = "batch.publish_rejected"
EV_PUBLISHED = "batch.published"
EV_PUBLISH_FAILED = "batch.publish_failed"
EV_WITHDRAWN = "batch.withdrawn"


@dataclass(frozen=True)
class OfficialEntry:
    """进入正式数据的最小不可变记录，带来源指针。"""

    row_id: str
    batch_id: str
    submitter: str
    mapping_version: str
    line_no: int
    data: dict[str, str]


class OfficialDataStore:
    """正式数据下沉目标。

    push_all 要么整批成功、要么不写入任何一行，模拟事务边界；
    测试可令其在提交前失败以验证发布回滚不留半成品。
    """

    def __init__(self) -> None:
        self._rows: list[OfficialEntry] = []
        self._fail_next: str | None = None
        self._lock = threading.Lock()

    def fail_next_push(self, reason: str) -> None:
        self._fail_next = reason

    def push_all(self, entries: list[OfficialEntry]) -> None:
        with self._lock:
            if self._fail_next is not None:
                reason, self._fail_next = self._fail_next, None
                raise RuntimeError(reason)
            # 先完成全部入栈准备，再一次性可见：失败时 self._rows 不被触碰。
            self._rows.extend(entries)

    def remove_batch(self, batch_id: str) -> int:
        with self._lock:
            before = len(self._rows)
            self._rows = [e for e in self._rows if e.batch_id != batch_id]
            return before - len(self._rows)

    def all(self) -> tuple[OfficialEntry, ...]:
        with self._lock:
            return tuple(self._rows)

    def count(self) -> int:
        with self._lock:
            return len(self._rows)


class InboxService:
    def __init__(
        self,
        mappings: MappingRegistry,
        clock: Clock | None = None,
        sink: OfficialDataStore | None = None,
    ) -> None:
        self._mappings = mappings
        self._clock = clock or Clock()
        self._sink = sink or OfficialDataStore()
        self._batches: dict[str, Batch] = {}
        self._fingerprints: dict[str, str] = {}
        self._ledger = Ledger()
        self._seq = Sequence()
        self._publish_lock = threading.Lock()

    # ---- 接收 -------------------------------------------------------------

    def receive(
        self,
        *,
        filename: str,
        content: bytes,
        submitter: str,
        mapping_version: str,
        parser: Callable[[bytes], list[dict[str, str]]] = parse_csv,
    ) -> Batch:
        """接收一个文件：先存指纹与来源信息，再映射、逐行核验分流。

        指纹重复时整包拒收（但既有批次不受影响），并在台账留痕。
        """
        fp = fingerprint(content)

        existing = self._fingerprints.get(fp)
        if existing is not None:
            self._ledger.append(
                self._clock,
                EV_FILE_DUPLICATE,
                {
                    "filename": filename,
                    "fingerprint": fp,
                    "submitter": submitter,
                    "existing_batch_id": existing,
                },
            )
            raise DuplicateSubmissionError(filename, existing)

        mapping = self._mappings.get(mapping_version)
        if mapping.retired:
            raise MappingRetiredError(mapping_version)

        raw_rows = parser(content)
        if not raw_rows:
            raise EmptySubmissionError("文件不包含任何数据行")

        # 指纹、提交方、映射版本在任何行处理之前先落定。
        batch = Batch(
            batch_id=new_id("B"),
            fingerprint=fp,
            filename=filename,
            submitter=submitter,
            mapping_version=mapping_version,
            received_at=self._clock.now(),
            status=BatchStatus.VALIDATED,
        )
        self._batches[batch.batch_id] = batch
        self._fingerprints[fp] = batch.batch_id

        for line_no, raw in enumerate(raw_rows, start=2):  # 行号含表头，从 2 起
            self._classify_row(batch, raw, line_no, mapping)

        self._ledger.append(
            self._clock,
            EV_FILE_RECEIVED,
            {
                "batch_id": batch.batch_id,
                "filename": filename,
                "fingerprint": fp,
                "submitter": submitter,
                "mapping_version": mapping_version,
                **batch.counts(),
            },
        )
        return batch

    def _classify_row(self, batch: Batch, raw: dict[str, str], line_no: int, mapping) -> None:
        mapped = mapping.map_row(raw)
        violations, warnings = self._evaluate(mapping, mapped)
        row = DataRow(
            row_id=new_id("R"),
            batch_id=batch.batch_id,
            line_no=line_no,
            raw=dict(raw),
            mapped=mapped,
            status=RowStatus.VALID if not violations else RowStatus.QUARANTINED,
            destination=(
                Destination.STAGING if not violations else Destination.QUARANTINE
            ),
            violations=tuple(violations),
            warnings=tuple(warnings),
        )
        batch.rows[row.row_id] = row
        if violations:
            self._ledger.append(
                self._clock,
                EV_ROW_QUARANTINED,
                {
                    "batch_id": batch.batch_id,
                    "row_id": row.row_id,
                    "line_no": line_no,
                    "violations": [
                        {"rule_id": v.rule_id, "field": v.field, "message": v.message}
                        for v in violations
                    ],
                },
            )

    @staticmethod
    def _evaluate(mapping, mapped: dict[str, str]):
        errors: list[RuleViolation] = []
        warnings: list[RuleViolation] = []
        for rule in mapping.rules:
            violation = rule.evaluate(mapped)
            if violation is None:
                continue
            if violation.severity == Severity.ERROR:
                errors.append(violation)
            else:
                warnings.append(violation)
        return errors, warnings

    # ---- 受控修订 ---------------------------------------------------------

    def revise_row(
        self,
        row_id: str,
        *,
        editor: str,
        reason: str,
        changes: dict[str, str],
    ) -> DataRow:
        """对隔离行做受控修订：必须给出修改人与理由，修订后重新核验。

        只有隔离区中的行允许修订；修订通过则进入暂存区，否则继续隔离，
        无论结果如何都留下修订记录与台账事件。
        """
        row = self._get_row(row_id)
        if row.destination != Destination.QUARANTINE:
            raise RevisionError(
                f"行 {row_id} 当前位于{row.destination.value}，只有隔离区行允许受控修订"
            )
        if not editor or not reason:
            raise RevisionError("受控修订必须提供修改人与修订理由")
        if not changes:
            raise RevisionError("修订内容不能为空")

        batch = self._batches[row.batch_id]
        mapping = self._mappings.get(batch.mapping_version)
        revised_mapped = {**row.mapped, **{k: v.strip() for k, v in changes.items()}}
        violations, warnings = self._evaluate(mapping, revised_mapped)
        record = Revision(
            revision_no=len(row.revisions) + 1,
            editor=editor,
            reason=reason,
            changes=dict(changes),
            violations=tuple(violations),
            passed=not violations,
        )
        row.revisions.append(record)
        row.mapped = revised_mapped
        row.violations = tuple(violations)
        row.warnings = tuple(warnings)
        if not violations:
            row.status = RowStatus.VALID
            row.destination = Destination.STAGING
        batch.version += 1
        self._ledger.append(
            self._clock,
            EV_ROW_REVISED,
            {
                "batch_id": batch.batch_id,
                "row_id": row_id,
                "revision_no": record.revision_no,
                "editor": editor,
                "reason": reason,
                "passed": record.passed,
                "violations": [
                    {"rule_id": v.rule_id, "field": v.field} for v in violations
                ],
            },
        )
        return row

    # ---- 映射换版 ---------------------------------------------------------

    def change_mapping(self, batch_id: str, new_version: str, *, requester: str) -> Batch:
        """对未发布批次换用新的字段映射版本并重新核验全部行。

        原始数据始终保留，换版只是按新版重新解释；去向按新核验结果重分，
        旧版→新版进入台账谱系。
        """
        batch = self._get(batch_id)
        if batch.status in (BatchStatus.PUBLISHING, BatchStatus.PUBLISHED, BatchStatus.WITHDRAWN):
            raise WithdrawalNotAllowedError(
                f"批次 {batch_id} 当前为{batch.status.value}，不能更换映射版本"
            )
        new_mapping = self._mappings.get(new_version)
        if new_mapping.retired:
            raise MappingRetiredError(new_version)
        old_version = batch.mapping_version
        if old_version == new_version:
            return batch

        batch.mapping_version = new_version
        for row in batch.rows.values():
            mapped = new_mapping.map_row(row.raw)
            violations, warnings = self._evaluate(new_mapping, mapped)
            row.mapped = mapped
            row.violations = tuple(violations)
            row.warnings = tuple(warnings)
            if violations:
                row.status = RowStatus.QUARANTINED
                row.destination = Destination.QUARANTINE
            else:
                row.status = RowStatus.VALID
                row.destination = Destination.STAGING
        batch.version += 1
        self._ledger.append(
            self._clock,
            EV_MAPPING_CHANGED,
            {
                "batch_id": batch_id,
                "old_version": old_version,
                "new_version": new_version,
                "requester": requester,
                **batch.counts(),
            },
        )
        return batch

    # ---- 原子发布 ---------------------------------------------------------

    def publish(
        self,
        batch_id: str,
        *,
        by: str,
        expected_version: int | None = None,
    ) -> Batch:
        """把暂存区中核验通过的行一次性发布到正式数据。

        - 全局发布互斥锁 + PUBLISHING 状态守卫串行化多人并发发布；
        - expected_version 提供乐观并发条件，过期则拒绝；
        - 下沉失败时状态与行去向整体回滚，正式数据不留半成品。
        """
        with self._publish_lock:
            batch = self._get(batch_id)
            if batch.status == BatchStatus.WITHDRAWN:
                raise BatchNotPublishableError(f"批次 {batch_id} 已撤回，不能发布")
            if batch.status == BatchStatus.PUBLISHING:
                raise ConcurrentPublishError(f"批次 {batch_id} 正在被其他人发布")
            # 已发布批次允许再次发布后续修订合格的行（每次发布本身保持原子）。
            if expected_version is not None and batch.version != expected_version:
                self._ledger.append(
                    self._clock,
                    EV_PUBLISH_REJECTED,
                    {
                        "batch_id": batch_id,
                        "by": by,
                        "reason": "stale_version",
                        "expected_version": expected_version,
                        "actual_version": batch.version,
                    },
                )
                raise ConcurrentPublishError(
                    f"批次版本已变化（期望 {expected_version}，实际 {batch.version}）"
                )

            valid_rows = batch.rows_by_destination(Destination.STAGING)
            if not valid_rows:
                raise BatchNotPublishableError("没有核验通过的行可发布")

            entries = [
                OfficialEntry(
                    row_id=r.row_id,
                    batch_id=batch.batch_id,
                    submitter=batch.submitter,
                    mapping_version=batch.mapping_version,
                    line_no=r.line_no,
                    data=dict(r.mapped),
                )
                for r in valid_rows
            ]

            previous_status = batch.status
            batch.status = BatchStatus.PUBLISHING
            batch.version += 1
            self._ledger.append(
                self._clock,
                EV_PUBLISH_STARTED,
                {
                    "batch_id": batch_id,
                    "by": by,
                    "expected_version": expected_version,
                    "valid_count": len(entries),
                },
            )

            try:
                self._sink.push_all(entries)
            except Exception as exc:  # 下沉失败：整体回滚，不留半成品
                batch.status = previous_status
                batch.version += 1
                self._ledger.append(
                    self._clock,
                    EV_PUBLISH_FAILED,
                    {"batch_id": batch_id, "by": by, "reason": str(exc)},
                )
                raise PublishFailedError(batch_id, str(exc)) from exc

            for row in valid_rows:
                row.status = RowStatus.PUBLISHED
                row.destination = Destination.OFFICIAL
            batch.status = BatchStatus.PUBLISHED
            batch.published_at = self._clock.now()
            batch.published_by = by
            batch.version += 1
            self._ledger.append(
                self._clock,
                EV_PUBLISHED,
                {
                    "batch_id": batch_id,
                    "by": by,
                    "published_count": len(entries),
                    "quarantined_remaining": batch.counts()["quarantined"],
                },
            )
            return batch

    # ---- 撤回 -------------------------------------------------------------

    def withdraw(self, batch_id: str, *, by: str, reason: str) -> Batch:
        """撤回整个批次：未发布则归档暂存/隔离行；已发布则连带从正式数据移除。"""
        batch = self._get(batch_id)
        if batch.status == BatchStatus.WITHDRAWN:
            raise WithdrawalNotAllowedError(f"批次 {batch_id} 已撤回")
        if batch.status == BatchStatus.PUBLISHING:
            raise WithdrawalNotAllowedError(f"批次 {batch_id} 正在发布中，暂不能撤回")
        if not reason:
            raise WithdrawalNotAllowedError("撤回必须说明理由")

        removed = 0
        if batch.status == BatchStatus.PUBLISHED:
            removed = self._sink.remove_batch(batch_id)

        for row in batch.rows.values():
            if row.destination in (Destination.STAGING, Destination.QUARANTINE, Destination.OFFICIAL):
                row.status = RowStatus.WITHDRAWN
                row.destination = Destination.WITHDRAWN
        batch.status = BatchStatus.WITHDRAWN
        batch.withdrawn_at = self._clock.now()
        batch.withdraw_reason = reason
        batch.version += 1
        self._ledger.append(
            self._clock,
            EV_WITHDRAWN,
            {
                "batch_id": batch_id,
                "by": by,
                "reason": reason,
                "removed_from_official": removed,
            },
        )
        return batch

    # ---- 查询：状态、去向与谱系 ------------------------------------------

    @staticmethod
    def _event_dict(event) -> dict:
        def plain(value):
            from collections.abc import Mapping as MappingT
            if isinstance(value, MappingT):
                return {k: plain(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [plain(v) for v in value]
            return value

        return {
            "seq": event.seq,
            "timestamp": event.timestamp,
            "event_type": event.event_type,
            "payload": plain(event.payload),
        }

    def batch(self, batch_id: str) -> Batch:
        return self._get(batch_id)

    def row(self, row_id: str) -> DataRow:
        return self._get_row(row_id)

    def row_trace(self, row_id: str) -> dict:
        """给出一行的当前状态、去向、修订履历与完整来源事件。"""
        row = self._get_row(row_id)
        return {
            **row.whereabouts(),
            "revisions": [
                {
                    "revision_no": rev.revision_no,
                    "editor": rev.editor,
                    "reason": rev.reason,
                    "changes": rev.changes,
                    "passed": rev.passed,
                }
                for rev in row.revisions
            ],
            "events": [self._event_dict(e) for e in self._ledger.for_row(row_id)],
        }

    def batch_lineage(self, batch_id: str) -> dict:
        batch = self._get(batch_id)
        return {
            "batch_id": batch_id,
            "fingerprint": batch.fingerprint,
            "filename": batch.filename,
            "submitter": batch.submitter,
            "mapping_version": batch.mapping_version,
            "status": batch.status.value,
            "version": batch.version,
            "counts": batch.counts(),
            "events": [self._event_dict(e) for e in self._ledger.for_batch(batch_id)],
        }

    def ledger(self) -> Ledger:
        return self._ledger

    def official_store(self) -> OfficialDataStore:
        return self._sink

    # ---- 内部 -------------------------------------------------------------

    def _get(self, batch_id: str) -> Batch:
        try:
            return self._batches[batch_id]
        except KeyError:
            raise BatchNotFound(batch_id) from None

    def _get_row(self, row_id: str) -> DataRow:
        for batch in self._batches.values():
            if row_id in batch.rows:
                return batch.rows[row_id]
        raise RowNotFound(row_id)
