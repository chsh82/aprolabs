"""vocabulary_quiz DB에 QA 배치 이력 + 퀴즈 세션/응답 테이블을 추가한다.

app/literacy/migrations/003_review_fields.py와 같은 패턴 - 실행 전 백업,
멱등(이미 있는 테이블은 건너뜀). idiom.db/기존 vocabulary_contents/items/
review_samples 데이터는 전혀 건드리지 않는다(순수 추가만).

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_vocabulary_quiz_add_qa_and_quiz_tables.py
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocabulary_quiz.db import get_db_path  # noqa: E402

_NEW_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_qa_batches (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    version                TEXT NOT NULL,
    reviewed_sample_count  INTEGER NOT NULL,
    pass_count             INTEGER NOT NULL,
    revise_count           INTEGER NOT NULL,
    exclude_count          INTEGER NOT NULL,
    critical_error_count   INTEGER NOT NULL,
    major_error_count      INTEGER NOT NULL,
    minor_error_count      INTEGER NOT NULL,
    approved_for_rnd_quiz  INTEGER NOT NULL DEFAULT 0,
    approved_for_public    INTEGER NOT NULL DEFAULT 0,
    reviewed_at            TEXT NOT NULL,
    notes                  TEXT,
    created_at             TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_qa_batches_version ON vocabulary_qa_batches(version);

CREATE TABLE IF NOT EXISTS vocabulary_quiz_sessions (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    source_version  TEXT NOT NULL,
    question_count  INTEGER NOT NULL,
    correct_count   INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed')),
    started_at      TEXT NOT NULL,
    completed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_quiz_sessions_user ON vocabulary_quiz_sessions(user_id, status);

CREATE TABLE IF NOT EXISTS vocabulary_quiz_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id       TEXT NOT NULL REFERENCES vocabulary_quiz_sessions(id) ON DELETE CASCADE,
    item_id          TEXT NOT NULL,
    order_index      INTEGER NOT NULL,
    selected_option  INTEGER CHECK (selected_option BETWEEN 1 AND 4),
    correct_option   INTEGER NOT NULL CHECK (correct_option BETWEEN 1 AND 4),
    is_correct       INTEGER,
    answered_at      TEXT,
    UNIQUE (session_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_quiz_attempts_session ON vocabulary_quiz_attempts(session_id, order_index);
"""


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def main() -> int:
    db_path = get_db_path()
    if not db_path.exists():
        print(f"치명적 오류: {db_path} 가 없습니다.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    before = _existing_tables(conn)
    to_create = {"vocabulary_qa_batches", "vocabulary_quiz_sessions", "vocabulary_quiz_attempts"} - before

    if not to_create:
        print("추가할 테이블 없음 - 전부 이미 존재함 (멱등)")
        conn.close()
        return 0

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    shutil.copy2(db_path, backup_path)
    print(f"백업 완료: {backup_path}")

    conn.executescript(_NEW_TABLES_SQL)
    conn.commit()

    after = _existing_tables(conn)
    for t in ["vocabulary_qa_batches", "vocabulary_quiz_sessions", "vocabulary_quiz_attempts"]:
        print(f"  {t}: {'생성됨' if t in after else '오류(생성 안 됨)'}")
        assert t in after, f"{t} 생성 실패"

    # 기존 테이블 행 수는 이 마이그레이션으로 절대 변하면 안 된다 - 안전 확인
    for t in ["vocabulary_contents", "vocabulary_items", "vocabulary_review_samples", "vocabulary_import_batches"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  (기존){t}: {n}건 (변경 없어야 함)")

    conn.close()
    print("마이그레이션 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
