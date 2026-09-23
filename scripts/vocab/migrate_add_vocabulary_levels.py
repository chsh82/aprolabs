"""vocabulary_quiz DB에 어휘 레벨(자동 후보) 테이블을 추가한다.

migrate_vocabulary_quiz_add_multiformat_tables.py와 같은 패턴 - 순수 신규
테이블 추가라 SQLite ALTER TABLE 제약(CHECK 변경 불가)에 안 걸린다. 실행
전 sqlite3.Connection.backup() API로 백업, 멱등(CREATE TABLE IF NOT EXISTS).
기존 vocabulary_contents/items/multiformat_items 등은 전혀 건드리지 않는다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_add_vocabulary_levels.py
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

_NEW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_content_levels (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id          TEXT NOT NULL REFERENCES vocabulary_contents(content_id),
    vocab_level         INTEGER NOT NULL CHECK (vocab_level BETWEEN 0 AND 6),
    target_grade_band   TEXT,
    level_score         REAL,
    level_confidence    REAL CHECK (level_confidence IS NULL OR (level_confidence BETWEEN 0 AND 1)),
    level_status        TEXT NOT NULL CHECK (level_status IN ('PROVISIONAL_AUTO', 'REVIEW_BOUNDARY')),
    boundary_flag       INTEGER NOT NULL DEFAULT 0,
    level_source        TEXT,
    level_version       TEXT NOT NULL,
    level_reason_json   TEXT,
    is_active           INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now')),
    UNIQUE (content_id, level_version)
);
CREATE INDEX IF NOT EXISTS idx_vcl_content_id ON vocabulary_content_levels(content_id);
CREATE INDEX IF NOT EXISTS idx_vcl_level_version ON vocabulary_content_levels(level_version);
CREATE INDEX IF NOT EXISTS idx_vcl_vocab_level ON vocabulary_content_levels(vocab_level);
"""

_EXISTING_TABLES_TO_VERIFY = [
    "vocabulary_contents", "vocabulary_items", "vocabulary_review_samples",
    "vocabulary_multiformat_items", "vocabulary_multiformat_sessions", "vocabulary_multiformat_responses",
]


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def _backup_via_sqlite_api(db_path: Path) -> Path:
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
    if "vocabulary_content_levels" in before:
        print("추가할 테이블 없음 - vocabulary_content_levels 이미 존재함 (멱등)")
        conn.close()
        return 0

    backup_path = _backup_via_sqlite_api(db_path)
    print(f"백업 완료(sqlite3.Connection.backup API): {backup_path}")

    counts_before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in _EXISTING_TABLES_TO_VERIFY}

    conn.executescript(_NEW_TABLE_SQL)
    conn.commit()

    after = _existing_tables(conn)
    print(f"vocabulary_content_levels: {'생성됨' if 'vocabulary_content_levels' in after else '오류(생성 안 됨)'}")
    assert "vocabulary_content_levels" in after, "vocabulary_content_levels 생성 실패"

    for t, before_count in counts_before.items():
        after_count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  (기존){t}: 전 {before_count} -> 후 {after_count} {'OK' if before_count == after_count else '!! 불일치'}")
        assert before_count == after_count, f"{t} 행 수가 변경됨 - 예상치 못한 부작용"

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"integrity_check(마이그레이션 후): {integrity_after}")
    assert integrity_after == "ok", "마이그레이션 후 integrity_check 실패"

    conn.close()
    print("마이그레이션 완료")
    print(f"복구 명령: cp {backup_path} {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
