"""vocabulary_quiz DB에 다유형 퀴즈(파일럿) 테이블을 추가한다.

migrate_vocabulary_quiz_add_qa_and_quiz_tables.py와 같은 패턴(실행 전 백업,
멱등 CREATE IF NOT EXISTS) - 다만 백업은 파일 복사 대신 sqlite3.Connection.backup()
API를 우선 사용한다(운영 DB 백업 절차에 맞춤). 기존 vocabulary_contents/items/
review_samples/qa_batches/quiz_sessions/quiz_attempts 데이터는 전혀 건드리지
않는다(순수 추가만).

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_vocabulary_quiz_add_multiformat_tables.py
"""
from __future__ import annotations

import io
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocabulary_quiz.db import get_db_path  # noqa: E402

_NEW_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_multiformat_items (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id                  TEXT NOT NULL UNIQUE,
    item_type                TEXT NOT NULL CHECK (item_type IN
                              ('MEANING_CHOICE','WORD_FROM_DEFINITION','CONTEXT_MEANING',
                               'CONTEXT_CLOZE','MATCH_WORD_MEANING')),
    source_content_id        TEXT REFERENCES vocabulary_contents(content_id),
    source_content_ids_json  TEXT,
    sense_id                 TEXT,
    sense_ids_json           TEXT,
    lemma                    TEXT,
    pos                      TEXT,
    prompt                   TEXT NOT NULL,
    options_json             TEXT,
    correct_option           INTEGER CHECK (correct_option IS NULL OR correct_option BETWEEN 1 AND 4),
    answer_payload_json      TEXT NOT NULL,
    explanation              TEXT,
    cognitive_level          INTEGER,
    qa_flags_json            TEXT,
    generator_version        TEXT,
    source_version           TEXT NOT NULL,
    is_active                INTEGER NOT NULL DEFAULT 1,
    created_at                TEXT DEFAULT (datetime('now')),
    updated_at                TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_vmi_item_type ON vocabulary_multiformat_items(item_type);
CREATE INDEX IF NOT EXISTS idx_vmi_source_content ON vocabulary_multiformat_items(source_content_id);
CREATE INDEX IF NOT EXISTS idx_vmi_source_version ON vocabulary_multiformat_items(source_version);

CREATE TABLE IF NOT EXISTS vocabulary_multiformat_import_batches (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    version                 TEXT NOT NULL,
    source_filename          TEXT,
    source_sha256            TEXT,
    seed                      INTEGER,
    selected_words            INTEGER,
    started_at                TEXT,
    completed_at               TEXT,
    status                     TEXT NOT NULL DEFAULT 'PENDING'
                               CHECK (status IN ('PENDING','DRY_RUN_OK','DRY_RUN_FAILED','COMPLETED','FAILED')),
    item_count                 INTEGER,
    item_type_counts_json       TEXT,
    inserted_count               INTEGER,
    updated_count                 INTEGER,
    unchanged_count                INTEGER,
    validation_result               TEXT,
    notes                            TEXT
);
CREATE INDEX IF NOT EXISTS idx_mf_batches_version ON vocabulary_multiformat_import_batches(version);

CREATE TABLE IF NOT EXISTS vocabulary_multiformat_sessions (
    id               TEXT PRIMARY KEY,
    user_id          TEXT NOT NULL,
    source_version   TEXT NOT NULL,
    item_types_json  TEXT,
    question_count   INTEGER NOT NULL,
    correct_count    INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed')),
    started_at       TEXT NOT NULL,
    completed_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_mf_sessions_user ON vocabulary_multiformat_sessions(user_id, status);

CREATE TABLE IF NOT EXISTS vocabulary_multiformat_responses (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id                TEXT NOT NULL REFERENCES vocabulary_multiformat_sessions(id) ON DELETE CASCADE,
    item_id                    TEXT NOT NULL REFERENCES vocabulary_multiformat_items(item_id),
    order_index                 INTEGER NOT NULL,
    item_type                    TEXT NOT NULL,
    submitted_payload_json        TEXT,
    is_correct                     INTEGER,
    correct_count                   INTEGER,
    total_count                      INTEGER,
    answered_at                       TEXT,
    UNIQUE (session_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_mf_responses_session ON vocabulary_multiformat_responses(session_id, order_index);

CREATE VIEW IF NOT EXISTS v_vmf_item_type_counts AS
SELECT source_version, item_type, COUNT(*) AS n
FROM vocabulary_multiformat_items
GROUP BY source_version, item_type;
"""

_NEW_OBJECTS = [
    "vocabulary_multiformat_items",
    "vocabulary_multiformat_import_batches",
    "vocabulary_multiformat_sessions",
    "vocabulary_multiformat_responses",
]

_EXISTING_TABLES_TO_VERIFY = [
    "vocabulary_contents", "vocabulary_items", "vocabulary_review_samples",
    "vocabulary_qa_batches", "vocabulary_quiz_sessions", "vocabulary_quiz_attempts",
]


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def _backup_via_sqlite_api(db_path: Path) -> Path:
    """sqlite3.Connection.backup()으로 일관성 있는 스냅샷을 뜬다(파일 복사보다
    권장 - 쓰기 도중이어도 SQLite가 페이지 단위로 안전하게 복사한다)."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return backup_path


def main() -> int:
    db_path = get_db_path()
    if not db_path.exists():
        print(f"치명적 오류: {db_path} 가 없습니다.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        print(f"치명적 오류: 마이그레이션 전 integrity_check 실패: {integrity}", file=sys.stderr)
        conn.close()
        return 1

    before = _existing_tables(conn)
    to_create = set(_NEW_OBJECTS) - before

    if not to_create:
        print("추가할 테이블 없음 - 전부 이미 존재함 (멱등)")
        conn.close()
        return 0

    backup_path = _backup_via_sqlite_api(db_path)
    print(f"백업 완료(sqlite3.Connection.backup API): {backup_path}")

    conn.executescript(_NEW_TABLES_SQL)
    conn.commit()

    after = _existing_tables(conn)
    for t in _NEW_OBJECTS:
        print(f"  {t}: {'생성됨' if t in after else '오류(생성 안 됨)'}")
        assert t in after, f"{t} 생성 실패"

    # 기존 테이블 행 수는 이 마이그레이션으로 절대 변하면 안 된다 - 안전 확인
    for t in _EXISTING_TABLES_TO_VERIFY:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  (기존){t}: {n}건 (변경 없어야 함)")

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"integrity_check(마이그레이션 후): {integrity_after}")
    assert integrity_after == "ok", "마이그레이션 후 integrity_check 실패"

    conn.close()
    print("마이그레이션 완료")
    print(f"복구 명령: cp {backup_path} {db_path}  (또는 서버에서 동일 경로로 복사)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
