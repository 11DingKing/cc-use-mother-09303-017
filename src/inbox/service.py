"""收件应用服务：领域流程的唯一入口。

所有写操作在显式事务中完成；发布额外通过 publish_gates 闸门做跨连接互斥。
失败路径要么回滚（不留半成品），要么只留下 FAILED 发布记录与原始批次，
绝不产生半条正式数据。
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Callable

from .clock import utc_now_iso
from .database import InboxRepository, connect, initialize_db
from .errors import (
    BadFileFormat,
    InvalidMappingSpec,
    InvalidState,
    NotFound,
    NothingToPublish,
    PermissionDenied,
    PublicationBusy,
    PublishConflict,
    RowNotEditable,
)
from .fingerprint import sha256_fingerprint
from .mapping import (
    FIELD_NAME_TO_KEY,
    MappingVersion,
    apply_mapping,
    build_mapping,
)
from .model import (
    BatchStatus,
    CorrectionResult,
    OfficialStatus,
    PartyRole,
    PublicationStatus,
    PublishReceipt,
    ReceiveReceipt,
    RowStatus,
)
from .parsing import SUPPORTED_FORMATS, parse_file
from .rules import RULE_SET_VERSION, business_key, validate_row

# 发布闸门租约：持有者崩溃后，超过该时长允许他人接管
_GATE_LEASE_SECONDS = 30.0


class InboxService:
    def __init__(
        self,
        db_path: str = ":memory:",
        *,
        clock: Callable[[], str] = utc_now_iso,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.conn = connect(db_path)
        initialize_db(self.conn)
        self.repo = InboxRepository(self.conn)
        self.clock = clock
        self._sleep = sleeper
        self._monotonic = monotonic

    def close(self) -> None:
        self.conn.close()

    # ---------- 参与方 ----------

    def register_party(self, party_id: str, name: str, role: PartyRole | str) -> None:
        role_value = role.value if isinstance(role, PartyRole) else str(role)
        if role_value not in {r.value for r in PartyRole}:
            raise PermissionDenied(f"未知角色：{role_value}")
        with self.conn:
            self.repo.insert_party(party_id, name, role_value, self.clock())

    def _party(self, party_id: str) -> sqlite3.Row:
        party = self.repo.get_party(party_id)
        if party is None:
            raise NotFound(f"参与方不存在：{party_id}")
        return party

    def _require_role(self, party_id: str, roles: set[str]) -> sqlite3.Row:
        party = self._party(party_id)
        if party["role"] not in roles:
            raise PermissionDenied(f"角色 {party['role']} 无权执行此操作")
        return party

    # ---------- 映射版本 ----------

    def register_mapping(
        self,
        version_id: str,
        submitter_id: str,
        file_format: str,
        column_mapping: dict[str, str],
    ) -> MappingVersion:
        if submitter_id != "*":
            self._party(submitter_id)
        mapping = build_mapping(
            version_id, submitter_id, file_format, column_mapping, self.clock()
        )
        try:
            with self.conn:
                self.repo.insert_mapping(mapping, mapping.spec_fingerprint)
        except sqlite3.IntegrityError as exc:
            raise InvalidMappingSpec(
                f"提交方 {submitter_id} 已存在相同规格的映射版本"
            ) from exc
        return mapping

    def retire_mapping(self, version_id: str) -> None:
        if self.repo.get_mapping(version_id) is None:
            raise NotFound(f"映射版本不存在：{version_id}")
        with self.conn:
            self.repo.retire_mapping(version_id, self.clock())

    def _resolve_mapping(
        self, submitter_id: str, file_format: str | None, mapping_version_id: str | None
    ) -> MappingVersion:
        if mapping_version_id is not None:
            row = self.repo.get_mapping(mapping_version_id)
            if row is None:
                raise NotFound(f"映射版本不存在：{mapping_version_id}")
            if row["submitter_id"] not in ("*", submitter_id):
                raise PermissionDenied("该映射版本不属于此提交方")
            if file_format is not None and row["file_format"] != file_format:
                raise BadFileFormat(
                    f"声明格式 {file_format} 与映射版本格式 {row['file_format']} 不一致"
                )
            return self._row_to_mapping(row)
        if file_format is None:
            raise BadFileFormat("未指定映射版本时必须声明文件格式")
        row = self.repo.find_latest_mapping(submitter_id, file_format)
        if row is None:
            raise NotFound(f"提交方 {submitter_id} 没有适用于 {file_format} 的映射版本")
        return self._row_to_mapping(row)

    @staticmethod
    def _row_to_mapping(row: sqlite3.Row) -> MappingVersion:
        return MappingVersion(
            version_id=row["version_id"],
            submitter_id=row["submitter_id"],
            file_format=row["file_format"],
            column_mapping=json.loads(row["column_mapping"]),
            created_at=row["created_at"],
        )

    # ---------- 接收 ----------

    def receive_file(
        self,
        submitter_id: str,
        filename: str,
        content: bytes,
        *,
        declared_format: str | None = None,
        mapping_version_id: str | None = None,
        scope: str = "default",
    ) -> ReceiveReceipt:
        """接收一个上传文件。

        固定顺序：先算指纹并查重 → 定位映射版本 → 解析 → 逐行映射校验入库。
        任何阶段失败都给出去向明确的回执，已接收的指纹记录始终保留。
        """
        self._require_role(submitter_id, {PartyRole.SUBMITTER.value})
        if declared_format is not None and declared_format.lower() not in SUPPORTED_FORMATS:
            raise BadFileFormat(f"不支持的文件格式：{declared_format}")

        fingerprint = sha256_fingerprint(content)
        submission_id = "sub_" + uuid.uuid4().hex
        now = self.clock()

        # 映射版本在解析前确定；解析失败也保留接收记录（谱系）
        mapping = self._resolve_mapping(submitter_id, declared_format, mapping_version_id)
        fmt = mapping.file_format

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            duplicate = self.repo.find_active_submission_by_fingerprint(fingerprint, submitter_id)
            if duplicate is not None:
                self.repo.insert_submission(
                    submission_id=submission_id,
                    fingerprint=fingerprint,
                    filename=filename,
                    submitter_id=submitter_id,
                    mapping_version_id=mapping.version_id,
                    file_format=fmt,
                    rule_set_version=RULE_SET_VERSION,
                    scope=scope,
                    status=BatchStatus.RECEIVED.value,
                    total_rows=0,
                    valid_rows=0,
                    quarantined_rows=0,
                    duplicate_of_submission_id=duplicate["submission_id"],
                    parse_error=None,
                    received_at=now,
                )
                self.conn.commit()
                return ReceiveReceipt(
                    submission_id=submission_id,
                    fingerprint=fingerprint,
                    status=BatchStatus.RECEIVED.value,
                    submitter_id=submitter_id,
                    mapping_version=mapping.version_id,
                    filename=filename,
                    duplicate_of_submission_id=duplicate["submission_id"],
                )

            try:
                parsed = parse_file(content, fmt)
            except BadFileFormat as exc:
                # 整包无法解析：登记接收记录并标记解析失败，不产生任何数据行
                self.repo.insert_submission(
                    submission_id=submission_id,
                    fingerprint=fingerprint,
                    filename=filename,
                    submitter_id=submitter_id,
                    mapping_version_id=mapping.version_id,
                    file_format=fmt,
                    rule_set_version=RULE_SET_VERSION,
                    scope=scope,
                    status=BatchStatus.PARSE_FAILED.value,
                    total_rows=0,
                    valid_rows=0,
                    quarantined_rows=0,
                    duplicate_of_submission_id=None,
                    parse_error=str(exc),
                    received_at=now,
                )
                self.conn.commit()
                return ReceiveReceipt(
                    submission_id=submission_id,
                    fingerprint=fingerprint,
                    status=BatchStatus.PARSE_FAILED.value,
                    submitter_id=submitter_id,
                    mapping_version=mapping.version_id,
                    filename=filename,
                    parse_error=str(exc),
                )

            # 先落批次：文件指纹、提交方、映射版本、规则版本与范围
            self.repo.insert_submission(
                submission_id=submission_id,
                fingerprint=fingerprint,
                filename=filename,
                submitter_id=submitter_id,
                mapping_version_id=mapping.version_id,
                file_format=fmt,
                rule_set_version=RULE_SET_VERSION,
                scope=scope,
                status=BatchStatus.VALIDATED.value,
                total_rows=len(parsed.rows),
                valid_rows=0,
                quarantined_rows=0,
                duplicate_of_submission_id=None,
                parse_error=None,
                received_at=now,
            )

            # 再逐行映射、校验、分流入库
            unmapped_columns: set[str] = set()
            seen_keys: set[str] = set()
            valid_count = quarantined_count = 0
            for raw in parsed.rows:
                mapped, unmapped = apply_mapping(mapping, parsed.columns, raw.source_values)
                unmapped_columns.update(unmapped)
                violations = validate_row(mapped)
                key = business_key(mapped) if not violations else ""
                if key:
                    if key in seen_keys:
                        violations = [
                            *violations,
                            _DuplicateInBatch(),
                        ]
                        key = ""
                    else:
                        seen_keys.add(key)
                status = RowStatus.VALID if not violations else RowStatus.QUARANTINED
                if status is RowStatus.VALID:
                    valid_count += 1
                else:
                    quarantined_count += 1
                self.repo.insert_row(
                    row_id="row_" + uuid.uuid4().hex,
                    submission_id=submission_id,
                    line_no=raw.line_no,
                    status=status.value,
                    source_values=json.dumps(raw.source_values, ensure_ascii=False),
                    mapped_values=json.dumps(mapped, ensure_ascii=False),
                    errors=json.dumps([v.as_dict() for v in violations], ensure_ascii=False),
                    business_key=key,
                    revision_count=0,
                    created_at=now,
                    updated_at=now,
                )

            self.repo.update_submission_counts(
                submission_id, len(parsed.rows), valid_count, quarantined_count
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

        return ReceiveReceipt(
            submission_id=submission_id,
            fingerprint=fingerprint,
            status=BatchStatus.VALIDATED.value,
            submitter_id=submitter_id,
            mapping_version=mapping.version_id,
            filename=filename,
            total_rows=len(parsed.rows),
            valid_rows=valid_count,
            quarantined_rows=quarantined_count,
            unmapped_columns=sorted(unmapped_columns),
        )

    # ---------- 受控修订 ----------

    def correct_row(
        self, row_id: str, editor_id: str, updates: dict[str, str]
    ) -> CorrectionResult:
        """对隔离行做受控修订：只接受标准字段的修改，修订后立即重新校验。

        只有「隔离」状态的行可改；每次修订（无论是否通过）都留痕。
        """
        self._require_role(editor_id, {PartyRole.COORDINATOR.value})
        if not updates:
            raise RowNotEditable("修订内容为空")
        unknown = [name for name in updates if name not in FIELD_NAME_TO_KEY]
        if unknown:
            raise RowNotEditable("不能修改非标准字段：" + "、".join(unknown))

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.repo.get_row(row_id)
            if row is None:
                raise NotFound(f"数据行不存在：{row_id}")
            if row["status"] != RowStatus.QUARANTINED.value:
                raise RowNotEditable(f"行当前状态为 {row['status']}，只有隔离行允许修订")

            old_values = json.loads(row["mapped_values"])
            errors_before = json.loads(row["errors"])
            new_values = dict(old_values)
            changed_fields: list[str] = []
            for field_name, new_text in updates.items():
                key = FIELD_NAME_TO_KEY[field_name]
                new_text = "" if new_text is None else str(new_text).strip()
                if new_values.get(key, "") != new_text:
                    changed_fields.append(field_name)
                new_values[key] = new_text

            violations = validate_row(new_values)
            key = business_key(new_values) if not violations else ""
            if key:
                clash = self.conn.execute(
                    """SELECT 1 FROM rows
                       WHERE submission_id=? AND status IN (?,?) AND business_key=?
                         AND row_id<>? LIMIT 1""",
                    (
                        row["submission_id"],
                        RowStatus.VALID.value,
                        RowStatus.PUBLISHED.value,
                        key,
                        row_id,
                    ),
                ).fetchone()
                if clash is not None:
                    violations.append(_DuplicateInBatch())
                    key = ""

            accepted = not violations
            new_status = RowStatus.VALID if accepted else RowStatus.QUARANTINED
            now = self.clock()
            seq = row["revision_count"] + 1
            errors_after = [v.as_dict() for v in violations]
            revision_id = "rev_" + uuid.uuid4().hex
            self.repo.insert_revision(
                revision_id=revision_id,
                row_id=row_id,
                seq=seq,
                editor_id=editor_id,
                changed_fields=json.dumps(changed_fields, ensure_ascii=False),
                old_values=json.dumps(old_values, ensure_ascii=False),
                new_values=json.dumps(new_values, ensure_ascii=False),
                errors_before=json.dumps(errors_before, ensure_ascii=False),
                errors_after=json.dumps(errors_after, ensure_ascii=False),
                accepted=1 if accepted else 0,
                created_at=now,
            )
            self.repo.update_row(
                row_id,
                mapped_values=json.dumps(new_values, ensure_ascii=False),
                errors=json.dumps(errors_after, ensure_ascii=False),
                status=new_status.value,
                business_key=key,
                revision_count=seq,
                updated_at=now,
            )
            counts = self._recount(row["submission_id"])
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

        return CorrectionResult(
            row_id=row_id,
            accepted=accepted,
            status=new_status.value,
            errors=errors_after,
            revision_id=revision_id,
            valid_rows=counts[0],
            quarantined_rows=counts[1],
        )

    def _recount(self, submission_id: str) -> tuple[int, int]:
        valid = len(self.repo.list_rows(submission_id, RowStatus.VALID.value))
        quarantined = len(self.repo.list_rows(submission_id, RowStatus.QUARANTINED.value))
        self.repo.update_submission_counts(
            submission_id,
            total=valid + quarantined
            + len(self.repo.list_rows(submission_id, RowStatus.PUBLISHED.value))
            + len(self.repo.list_rows(submission_id, RowStatus.WITHDRAWN.value)),
            valid=valid,
            quarantined=quarantined,
        )
        return valid, quarantined

    # ---------- 原子发布 ----------

    def publish(
        self,
        submission_id: str,
        publisher_id: str,
        *,
        wait_timeout: float = 10.0,
    ) -> PublishReceipt:
        """把批次内全部「合格」行一次性写入正式数据。

        - 同范围发布经闸门串行化（多人并发安全）。
        - 正式数据中存在同业务键时整批回滚，仅保留 FAILED 记录。
        """
        self._require_role(publisher_id, {PartyRole.COMPILER.value})
        submission = self._submission(submission_id)
        if submission["status"] not in (
            BatchStatus.VALIDATED.value,
            BatchStatus.PARTIALLY_PUBLISHED.value,
        ):
            raise InvalidState(f"批次状态为 {submission['status']}，不可发布")
        scope = submission["scope"]

        publication_id = "pub_" + uuid.uuid4().hex
        if not self._acquire_gate(scope, publication_id, wait_timeout):
            raise PublicationBusy(scope)

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            eligible = self.repo.list_rows(submission_id, RowStatus.VALID.value)
            seq = self.repo.next_publication_seq(submission_id)
            now = self.clock()
            if not eligible:
                self.conn.commit()
                raise NothingToPublish("批次中没有核验通过、等待发布的行")

            row_ids = [r["row_id"] for r in eligible]
            keys = [r["business_key"] for r in eligible]

            # 发布记录先行（PENDING）：正式数据行外键引用它，失败也留得下谱系
            self.repo.insert_publication(
                publication_id=publication_id,
                submission_id=submission_id,
                scope=scope,
                seq=seq,
                status=PublicationStatus.PENDING.value,
                published_count=0,
                row_ids=json.dumps(row_ids, ensure_ascii=False),
                initiated_by=publisher_id,
                failure_reason=None,
                created_at=now,
                committed_at=None,
            )

            conflicts = self.repo.active_official_keys(scope, keys)
            if conflicts:
                reason = "业务键与正式数据冲突：" + "、".join(conflicts)
                self.repo.update_publication_result(
                    publication_id, PublicationStatus.FAILED.value, 0, reason, None
                )
                self.conn.commit()
                raise PublishConflict(conflicts)

            for row in eligible:
                self.repo.insert_official(
                    official_id="off_" + uuid.uuid4().hex,
                    scope=scope,
                    business_key=row["business_key"],
                    values_json=row["mapped_values"],
                    status=OfficialStatus.ACTIVE.value,
                    source_submission_id=submission_id,
                    source_row_id=row["row_id"],
                    source_publication_id=publication_id,
                    published_at=now,
                    withdrawn_at=None,
                    withdrawal_publication_id=None,
                )
            self.repo.mark_rows_status(row_ids, RowStatus.PUBLISHED.value, now)
            self.repo.update_publication_result(
                publication_id,
                PublicationStatus.COMMITTED.value,
                len(eligible),
                None,
                now,
            )
            remaining_valid = len(self.repo.list_rows(submission_id, RowStatus.VALID.value))
            remaining_quarantined = len(
                self.repo.list_rows(submission_id, RowStatus.QUARANTINED.value)
            )
            new_status = (
                BatchStatus.PUBLISHED.value
                if remaining_valid == 0 and remaining_quarantined == 0
                else BatchStatus.PARTIALLY_PUBLISHED.value
            )
            self.repo.update_submission_status(submission_id, new_status)
            self.repo.update_submission_counts(
                submission_id,
                total=submission["total_rows"],
                valid=remaining_valid,
                quarantined=remaining_quarantined,
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            self._safe_release_gate(scope, publication_id)
            raise
        self._safe_release_gate(scope, publication_id)

        return PublishReceipt(
            publication_id=publication_id,
            submission_id=submission_id,
            seq=seq,
            scope=scope,
            status=PublicationStatus.COMMITTED.value,
            published_rows=len(eligible),
            committed_at=now,
        )

    def _acquire_gate(self, scope: str, publication_id: str, wait_timeout: float) -> bool:
        deadline = self._monotonic() + wait_timeout
        while True:
            now_text = self.clock()
            lease_text = _iso_add_seconds(now_text, _GATE_LEASE_SECONDS)
            with self.conn:
                acquired = self.repo.try_acquire_gate(scope, publication_id, now_text, lease_text)
            if acquired:
                return True
            if self._monotonic() >= deadline:
                return False
            self._sleep(0.02)

    def _safe_release_gate(self, scope: str, publication_id: str) -> None:
        try:
            with self.conn:
                self.repo.release_gate(scope, publication_id)
        except sqlite3.Error:
            pass

    # ---------- 撤回 ----------

    def withdraw(self, submission_id: str, actor_id: str) -> PublishReceipt:
        """撤回批次已进入正式数据的全部行（同范围与发布互斥）。"""
        self._require_role(
            actor_id,
            {PartyRole.COORDINATOR.value, PartyRole.COMPILER.value},
        )
        submission = self._submission(submission_id)
        if submission["status"] not in (
            BatchStatus.PUBLISHED.value,
            BatchStatus.PARTIALLY_PUBLISHED.value,
        ):
            raise InvalidState(f"批次状态为 {submission['status']}，无可撤回的发布")
        scope = submission["scope"]

        withdrawal_id = "wd_" + uuid.uuid4().hex
        if not self._acquire_gate(scope, withdrawal_id, wait_timeout=10.0):
            raise PublicationBusy(scope)

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            active_official = [
                item
                for item in self.repo.list_official_by_submission(submission_id)
                if item["status"] == OfficialStatus.ACTIVE.value
            ]
            if not active_official:
                self.conn.commit()
                raise InvalidState("该批次没有生效中的正式数据")
            seq = self.repo.next_publication_seq(submission_id)
            now = self.clock()
            official_ids = [item["official_id"] for item in active_official]
            row_ids = [item["source_row_id"] for item in active_official]
            # 撤回记录先行，正式数据的撤回谱系指针才能引用它
            self.repo.insert_publication(
                publication_id=withdrawal_id,
                submission_id=submission_id,
                scope=scope,
                seq=seq,
                status=PublicationStatus.WITHDRAWN.value,
                published_count=0,
                row_ids=json.dumps(row_ids, ensure_ascii=False),
                initiated_by=actor_id,
                failure_reason=None,
                created_at=now,
                committed_at=now,
            )
            self.repo.mark_official_withdrawn(official_ids, now, withdrawal_id)
            self.repo.mark_rows_status(row_ids, RowStatus.WITHDRAWN.value, now)
            valid, quarantined = self._recount(submission_id)
            if valid == 0 and quarantined == 0:
                self.repo.update_submission_status(submission_id, BatchStatus.WITHDRAWN.value)
            else:
                # 仍有合格/隔离行：批次回到校验完成，可继续修订后发布
                self.repo.update_submission_status(submission_id, BatchStatus.VALIDATED.value)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            self._safe_release_gate(scope, withdrawal_id)
            raise
        self._safe_release_gate(scope, withdrawal_id)

        return PublishReceipt(
            publication_id=withdrawal_id,
            submission_id=submission_id,
            seq=seq,
            scope=scope,
            status=PublicationStatus.WITHDRAWN.value,
            published_rows=0,
            committed_at=now,
        )

    # ---------- 查询：每一行都能说明状态与去向 ----------

    def _submission(self, submission_id: str) -> sqlite3.Row:
        submission = self.repo.get_submission(submission_id)
        if submission is None:
            raise NotFound(f"批次不存在：{submission_id}")
        return submission

    def submission_overview(self, submission_id: str) -> dict:
        submission = self._submission(submission_id)
        rows = self.repo.list_rows(submission_id)
        publications = self.repo.list_publications(submission_id)
        mapping_row = self.repo.get_mapping(submission["mapping_version_id"])
        return {
            "submission_id": submission_id,
            "filename": submission["filename"],
            "fingerprint": submission["fingerprint"],
            "submitter_id": submission["submitter_id"],
            "mapping_version": submission["mapping_version_id"],
            "mapping_spec_fingerprint": mapping_row["spec_fingerprint"] if mapping_row else None,
            "file_format": submission["file_format"],
            "rule_set_version": submission["rule_set_version"],
            "scope": submission["scope"],
            "status": submission["status"],
            "parse_error": submission["parse_error"],
            "duplicate_of": submission["duplicate_of_submission_id"],
            "received_at": submission["received_at"],
            "counts": {
                "total": submission["total_rows"],
                "valid": submission["valid_rows"],
                "quarantined": submission["quarantined_rows"],
            },
            "rows": [
                {
                    "row_id": r["row_id"],
                    "line_no": r["line_no"],
                    "status": r["status"],
                    "business_key": r["business_key"],
                    "revision_count": r["revision_count"],
                    "errors": json.loads(r["errors"]),
                }
                for r in rows
            ],
            "publications": [
                {
                    "publication_id": p["publication_id"],
                    "seq": p["seq"],
                    "status": p["status"],
                    "published_count": p["published_count"],
                    "failure_reason": p["failure_reason"],
                    "created_at": p["created_at"],
                    "committed_at": p["committed_at"],
                }
                for p in publications
            ],
        }

    def trace_row(self, row_id: str) -> dict:
        """逐行谱系：来源文件、映射版本、校验结论、修订链、正式数据去向。"""
        row = self.repo.get_row(row_id)
        if row is None:
            raise NotFound(f"数据行不存在：{row_id}")
        submission = self._submission(row["submission_id"])
        official = self.repo.get_official_by_row(row_id)
        publication = None
        if official is not None:
            publication = self.conn.execute(
                "SELECT * FROM publications WHERE publication_id=?",
                (official["source_publication_id"],),
            ).fetchone()
        revisions = self.repo.list_revisions(row_id)
        return {
            "row_id": row_id,
            "submission_id": row["submission_id"],
            "filename": submission["filename"],
            "fingerprint": submission["fingerprint"],
            "submitter_id": submission["submitter_id"],
            "line_no": row["line_no"],
            "current_status": row["status"],
            "source_values": json.loads(row["source_values"]),
            "mapped_values": json.loads(row["mapped_values"]),
            "errors": json.loads(row["errors"]),
            "business_key": row["business_key"],
            "mapping_version": submission["mapping_version_id"],
            "rule_set_version": submission["rule_set_version"],
            "revisions": [
                {
                    "seq": rv["seq"],
                    "editor_id": rv["editor_id"],
                    "changed_fields": json.loads(rv["changed_fields"]),
                    "old_values": json.loads(rv["old_values"]),
                    "new_values": json.loads(rv["new_values"]),
                    "errors_after": json.loads(rv["errors_after"]),
                    "accepted": bool(rv["accepted"]),
                    "created_at": rv["created_at"],
                }
                for rv in revisions
            ],
            "destination": None
            if official is None
            else {
                "official_id": official["official_id"],
                "scope": official["scope"],
                "business_key": official["business_key"],
                "official_status": official["status"],
                "published_at": official["published_at"],
                "publication_id": official["source_publication_id"],
                "publication_status": publication["status"] if publication else None,
                "withdrawn_at": official["withdrawn_at"],
                "withdrawal_publication_id": official["withdrawal_publication_id"],
            },
        }

    def official_history(self, scope: str, business_key: str) -> list[dict]:
        """某业务键在正式数据中的完整版本链（发布/撤回谱系）。"""
        history = self.repo.list_official_history(scope, business_key)
        return [
            {
                "official_id": item["official_id"],
                "status": item["status"],
                "values": json.loads(item["values_json"]),
                "source_submission_id": item["source_submission_id"],
                "source_row_id": item["source_row_id"],
                "publication_id": item["source_publication_id"],
                "withdrawal_publication_id": item["withdrawal_publication_id"],
                "published_at": item["published_at"],
                "withdrawn_at": item["withdrawn_at"],
            }
            for item in history
        ]


def _iso_add_seconds(iso_text: str, seconds: float) -> str:
    from datetime import datetime, timedelta

    dt = datetime.fromisoformat(iso_text)
    return (dt + timedelta(seconds=seconds)).isoformat(timespec="microseconds")


class _DuplicateInBatch:
    """与 FieldViolation 同形的轻量占位，避免循环导入。"""

    def __init__(self) -> None:
        self.field = "业务键"
        self.code = "duplicate_in_batch"
        self.message = "同一批次内业务键重复"

    def as_dict(self) -> dict:
        return {"field": self.field, "code": self.code, "message": self.message}
