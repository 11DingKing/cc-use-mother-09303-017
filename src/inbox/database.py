"""SQLite 持久化：表结构与仓储。

设计要点：

- official_records 只追加不覆盖：每次发布插入新版本行，部分唯一索引保证
  同一业务键在同一发布范围内至多一条「生效中」记录；撤回只改状态。
- publish_gates 是显式的发布闸门：同一范围同时只允许一个发布者，
  带租约防止持有者崩溃后死锁。
- 批次与行永不物理删除，撤回只做状态迁移，来源谱系完整保留。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS parties (
    party_id   TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    role       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mapping_versions (
    version_id        TEXT PRIMARY KEY,
    submitter_id      TEXT NOT NULL,
    file_format       TEXT NOT NULL,
    column_mapping    TEXT NOT NULL,
    spec_fingerprint  TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    retired_at        TEXT,
    UNIQUE (submitter_id, spec_fingerprint)
);

CREATE TABLE IF NOT EXISTS submissions (
    submission_id               TEXT PRIMARY KEY,
    fingerprint                 TEXT NOT NULL,
    filename                    TEXT NOT NULL,
    submitter_id                TEXT NOT NULL REFERENCES parties(party_id),
    mapping_version_id          TEXT NOT NULL REFERENCES mapping_versions(version_id),
    file_format                 TEXT NOT NULL,
    rule_set_version            TEXT NOT NULL,
    scope                       TEXT NOT NULL,
    status                      TEXT NOT NULL,
    total_rows                  INTEGER NOT NULL DEFAULT 0,
    valid_rows                  INTEGER NOT NULL DEFAULT 0,
    quarantined_rows            INTEGER NOT NULL DEFAULT 0,
    duplicate_of_submission_id  TEXT REFERENCES submissions(submission_id),
    parse_error                 TEXT,
    received_at                 TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_dedup
    ON submissions(fingerprint, submitter_id)
    WHERE duplicate_of_submission_id IS NULL AND status <> '解析失败';

CREATE TABLE IF NOT EXISTS rows (
    row_id          TEXT PRIMARY KEY,
    submission_id   TEXT NOT NULL REFERENCES submissions(submission_id),
    line_no         INTEGER NOT NULL,
    status          TEXT NOT NULL,
    source_values   TEXT NOT NULL,
    mapped_values   TEXT NOT NULL,
    errors          TEXT NOT NULL,
    business_key    TEXT NOT NULL DEFAULT '',
    revision_count  INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (submission_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_rows_submission ON rows(submission_id);
CREATE INDEX IF NOT EXISTS idx_rows_status ON rows(submission_id, status);

CREATE TABLE IF NOT EXISTS row_revisions (
    revision_id      TEXT PRIMARY KEY,
    row_id           TEXT NOT NULL REFERENCES rows(row_id),
    seq              INTEGER NOT NULL,
    editor_id        TEXT NOT NULL REFERENCES parties(party_id),
    changed_fields   TEXT NOT NULL,
    old_values       TEXT NOT NULL,
    new_values       TEXT NOT NULL,
    errors_before    TEXT NOT NULL,
    errors_after     TEXT NOT NULL,
    accepted         INTEGER NOT NULL,
    created_at       TEXT NOT NULL,
    UNIQUE (row_id, seq)
);

CREATE TABLE IF NOT EXISTS publications (
    publication_id   TEXT PRIMARY KEY,
    submission_id    TEXT NOT NULL REFERENCES submissions(submission_id),
    scope            TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    status           TEXT NOT NULL,
    published_count  INTEGER NOT NULL DEFAULT 0,
    row_ids          TEXT NOT NULL,
    initiated_by     TEXT NOT NULL REFERENCES parties(party_id),
    failure_reason   TEXT,
    created_at       TEXT NOT NULL,
    committed_at     TEXT,
    UNIQUE (submission_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_publications_scope ON publications(scope, status);

CREATE TABLE IF NOT EXISTS official_records (
    official_id                  TEXT PRIMARY KEY,
    scope                        TEXT NOT NULL,
    business_key                 TEXT NOT NULL,
    values_json                  TEXT NOT NULL,
    status                       TEXT NOT NULL,
    source_submission_id         TEXT NOT NULL REFERENCES submissions(submission_id),
    source_row_id                TEXT NOT NULL REFERENCES rows(row_id),
    source_publication_id        TEXT NOT NULL REFERENCES publications(publication_id),
    published_at                 TEXT NOT NULL,
    withdrawn_at                 TEXT,
    withdrawal_publication_id    TEXT REFERENCES publications(publication_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_official_active
    ON official_records(scope, business_key) WHERE status = '生效中';
CREATE INDEX IF NOT EXISTS idx_official_source ON official_records(source_submission_id);

CREATE TABLE IF NOT EXISTS publish_gates (
    scope                 TEXT PRIMARY KEY,
    holder_publication_id TEXT NOT NULL,
    acquired_at           TEXT NOT NULL,
    lease_expires_at      TEXT NOT NULL
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """打开（必要时创建）收件库连接。"""
    path = Path(db_path)
    if path.parent != Path(".") and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def initialize_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


class InboxRepository:
    """薄仓储：只做存取，事务由应用服务控制。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # ---- 参与方 ----

    def insert_party(self, party_id: str, name: str, role: str, created_at: str) -> None:
        self.conn.execute(
            "INSERT INTO parties(party_id, name, role, created_at) VALUES (?,?,?,?)",
            (party_id, name, role, created_at),
        )

    def get_party(self, party_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM parties WHERE party_id=?", (party_id,)).fetchone()

    # ---- 映射版本 ----

    def insert_mapping(self, mapping, spec_fingerprint: str) -> None:
        self.conn.execute(
            """INSERT INTO mapping_versions
               (version_id, submitter_id, file_format, column_mapping, spec_fingerprint, created_at)
               VALUES (?,?,?,?,?,?)""",
            (
                mapping.version_id,
                mapping.submitter_id,
                mapping.file_format,
                json.dumps(mapping.column_mapping, ensure_ascii=False),
                spec_fingerprint,
                mapping.created_at,
            ),
        )

    def get_mapping(self, version_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM mapping_versions WHERE version_id=?", (version_id,)
        ).fetchone()

    def find_latest_mapping(self, submitter_id: str, file_format: str) -> sqlite3.Row | None:
        """适用版本：提交方专用版本优先，其次全局版本，取最新未停用者。"""
        return self.conn.execute(
            """SELECT * FROM mapping_versions
               WHERE file_format=? AND retired_at IS NULL
                 AND submitter_id IN ('*', ?)
               ORDER BY (submitter_id=?) DESC, created_at DESC, version_id DESC
               LIMIT 1""",
            (file_format, submitter_id, submitter_id),
        ).fetchone()

    def retire_mapping(self, version_id: str, retired_at: str) -> None:
        self.conn.execute(
            "UPDATE mapping_versions SET retired_at=? WHERE version_id=? AND retired_at IS NULL",
            (retired_at, version_id),
        )

    # ---- 提交批次 ----

    def find_active_submission_by_fingerprint(
        self, fingerprint: str, submitter_id: str
    ) -> sqlite3.Row | None:
        """查重只认真正接收成功的原始批次：重复登记与解析失败不参与。"""
        return self.conn.execute(
            """SELECT * FROM submissions
               WHERE fingerprint=? AND submitter_id=?
                 AND duplicate_of_submission_id IS NULL
                 AND status <> '解析失败'""",
            (fingerprint, submitter_id),
        ).fetchone()

    def insert_submission(self, **fields) -> None:
        self.conn.execute(
            """INSERT INTO submissions
               (submission_id, fingerprint, filename, submitter_id, mapping_version_id,
                file_format, rule_set_version, scope, status, total_rows, valid_rows,
                quarantined_rows, duplicate_of_submission_id, parse_error, received_at)
               VALUES (:submission_id,:fingerprint,:filename,:submitter_id,:mapping_version_id,
                :file_format,:rule_set_version,:scope,:status,:total_rows,:valid_rows,
                :quarantined_rows,:duplicate_of_submission_id,:parse_error,:received_at)""",
            fields,
        )

    def get_submission(self, submission_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM submissions WHERE submission_id=?", (submission_id,)
        ).fetchone()

    def update_submission_status(self, submission_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE submissions SET status=? WHERE submission_id=?",
            (status, submission_id),
        )

    def update_submission_counts(self, submission_id: str, total: int, valid: int, quarantined: int) -> None:
        self.conn.execute(
            """UPDATE submissions SET total_rows=?, valid_rows=?, quarantined_rows=?
               WHERE submission_id=?""",
            (total, valid, quarantined, submission_id),
        )

    # ---- 数据行 ----

    def insert_row(self, **fields) -> None:
        self.conn.execute(
            """INSERT INTO rows
               (row_id, submission_id, line_no, status, source_values, mapped_values,
                errors, business_key, revision_count, created_at, updated_at)
               VALUES (:row_id,:submission_id,:line_no,:status,:source_values,:mapped_values,
                :errors,:business_key,:revision_count,:created_at,:updated_at)""",
            fields,
        )

    def get_row(self, row_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM rows WHERE row_id=?", (row_id,)).fetchone()

    def list_rows(self, submission_id: str, status: str | None = None) -> list[sqlite3.Row]:
        if status is None:
            result = self.conn.execute(
                "SELECT * FROM rows WHERE submission_id=? ORDER BY line_no", (submission_id,)
            )
        else:
            result = self.conn.execute(
                "SELECT * FROM rows WHERE submission_id=? AND status=? ORDER BY line_no",
                (submission_id, status),
            )
        return list(result)

    def update_row(self, row_id: str, *, mapped_values: str, errors: str, status: str,
                   business_key: str, revision_count: int, updated_at: str) -> None:
        self.conn.execute(
            """UPDATE rows SET mapped_values=?, errors=?, status=?, business_key=?,
               revision_count=?, updated_at=? WHERE row_id=?""",
            (mapped_values, errors, status, business_key, revision_count, updated_at, row_id),
        )

    def insert_revision(self, **fields) -> None:
        self.conn.execute(
            """INSERT INTO row_revisions
               (revision_id, row_id, seq, editor_id, changed_fields, old_values, new_values,
                errors_before, errors_after, accepted, created_at)
               VALUES (:revision_id,:row_id,:seq,:editor_id,:changed_fields,:old_values,:new_values,
                :errors_before,:errors_after,:accepted,:created_at)""",
            fields,
        )

    def list_revisions(self, row_id: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM row_revisions WHERE row_id=? ORDER BY seq", (row_id,)
            )
        )

    # ---- 发布与正式数据 ----

    def try_acquire_gate(self, scope: str, publication_id: str, now: str, lease_expires_at: str) -> bool:
        """尝试占用发布闸门。返回 False 表示闸门被有效租约占用。"""
        self.conn.execute(
            """INSERT INTO publish_gates(scope, holder_publication_id, acquired_at, lease_expires_at)
               VALUES (?,?,?,?)
               ON CONFLICT(scope) DO UPDATE SET
                 holder_publication_id=excluded.holder_publication_id,
                 acquired_at=excluded.acquired_at,
                 lease_expires_at=excluded.lease_expires_at
               WHERE publish_gates.lease_expires_at < ?""",
            (scope, publication_id, now, lease_expires_at, now),
        )
        holder = self.conn.execute(
            "SELECT holder_publication_id FROM publish_gates WHERE scope=?", (scope,)
        ).fetchone()
        return holder is not None and holder["holder_publication_id"] == publication_id

    def release_gate(self, scope: str, publication_id: str) -> None:
        self.conn.execute(
            "DELETE FROM publish_gates WHERE scope=? AND holder_publication_id=?",
            (scope, publication_id),
        )

    def next_publication_seq(self, submission_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM publications WHERE submission_id=?",
            (submission_id,),
        ).fetchone()
        return int(row["next_seq"])

    def insert_publication(self, **fields) -> None:
        self.conn.execute(
            """INSERT INTO publications
               (publication_id, submission_id, scope, seq, status, published_count, row_ids,
                initiated_by, failure_reason, created_at, committed_at)
               VALUES (:publication_id,:submission_id,:scope,:seq,:status,:published_count,:row_ids,
                :initiated_by,:failure_reason,:created_at,:committed_at)""",
            fields,
        )

    def update_publication_result(self, publication_id: str, status: str, published_count: int,
                                  failure_reason: str | None, committed_at: str | None) -> None:
        self.conn.execute(
            """UPDATE publications SET status=?, published_count=?, failure_reason=?, committed_at=?
               WHERE publication_id=?""",
            (status, published_count, failure_reason, committed_at, publication_id),
        )

    def list_publications(self, submission_id: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM publications WHERE submission_id=? ORDER BY seq", (submission_id,)
            )
        )

    def active_official_keys(self, scope: str, business_keys: list[str]) -> list[str]:
        if not business_keys:
            return []
        marks = ",".join("?" for _ in business_keys)
        rows = self.conn.execute(
            f"""SELECT business_key FROM official_records
                WHERE scope=? AND status='生效中' AND business_key IN ({marks})""",
            [scope, *business_keys],
        ).fetchall()
        return [row["business_key"] for row in rows]

    def insert_official(self, **fields) -> None:
        self.conn.execute(
            """INSERT INTO official_records
               (official_id, scope, business_key, values_json, status, source_submission_id,
                source_row_id, source_publication_id, published_at, withdrawn_at,
                withdrawal_publication_id)
               VALUES (:official_id,:scope,:business_key,:values_json,:status,:source_submission_id,
                :source_row_id,:source_publication_id,:published_at,:withdrawn_at,
                :withdrawal_publication_id)""",
            fields,
        )

    def get_official_by_row(self, row_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM official_records WHERE source_row_id=? ORDER BY published_at DESC",
            (row_id,),
        ).fetchone()

    def list_official_by_submission(self, submission_id: str, status: str | None = None) -> list[sqlite3.Row]:
        if status is None:
            result = self.conn.execute(
                "SELECT * FROM official_records WHERE source_submission_id=? ORDER BY published_at",
                (submission_id,),
            )
        else:
            result = self.conn.execute(
                "SELECT * FROM official_records WHERE source_submission_id=? AND status=? ORDER BY published_at",
                (submission_id, status),
            )
        return list(result)

    def list_official_history(self, scope: str, business_key: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """SELECT * FROM official_records WHERE scope=? AND business_key=?
                   ORDER BY published_at, official_id""",
                (scope, business_key),
            )
        )

    def mark_official_withdrawn(self, official_ids: list[str], withdrawn_at: str,
                                withdrawal_publication_id: str) -> None:
        marks = ",".join("?" for _ in official_ids)
        self.conn.execute(
            f"""UPDATE official_records SET status='已撤回', withdrawn_at=?,
                withdrawal_publication_id=? WHERE official_id IN ({marks})""",
            [withdrawn_at, withdrawal_publication_id, *official_ids],
        )

    def mark_rows_status(self, row_ids: list[str], status: str, updated_at: str) -> None:
        marks = ",".join("?" for _ in row_ids)
        self.conn.execute(
            f"UPDATE rows SET status=?, updated_at=? WHERE row_id IN ({marks})",
            [status, updated_at, *row_ids],
        )
