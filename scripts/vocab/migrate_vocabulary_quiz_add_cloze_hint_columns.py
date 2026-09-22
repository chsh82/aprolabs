"""vocabulary_multiformat_responses에 문맥빈칸(CONTEXT_CLOZE) 초성 힌트/재시도용
컬럼(attempt_count, hint_used)을 추가한다.

migrate_vocabulary_quiz_add_multiformat_tables.py와 같은 패턴(실행 전
sqlite3.Connection.backup() API로 백업, 멱등). SQLite는 ALTER TABLE ADD COLUMN만
지원하고 컬럼 존재 여부를 직접 못 물어보므로 PRAGMA table_info로 확인한다.
기존 데이터(문항/세션/응답 행 수)는 전혀 건드리지 않는다 - 신규 컬럼은 전부
DEFAULT 0으로 채워진다(과거 응답은 재시도/힌트 없이 완료된 것으로 취급).

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_vocabulary_quiz_add_cloze_hint_columns.py
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

_NEW_COLUMNS = {
    "attempt_count": "INTEGER NOT NULL DEFAULT 0",
    "hint_used": "INTEGER NOT NULL DEFAULT 0",
}


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


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

    existing = _existing_columns(conn, "vocabulary_multiformat_responses")
    to_add = {c: t for c, t in _NEW_COLUMNS.items() if c not in existing}

    if not to_add:
        print("추가할 컬럼 없음 - 전부 이미 존재함 (멱등)")
        conn.close()
        return 0

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    print(f"백업 완료(sqlite3.Connection.backup API): {backup_path}")

    before_count = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_responses").fetchone()[0]

    for col, coltype in to_add.items():
        conn.execute(f"ALTER TABLE vocabulary_multiformat_responses ADD COLUMN {col} {coltype}")
        print(f"  {col} 컬럼 추가됨")
    conn.commit()

    after_columns = _existing_columns(conn, "vocabulary_multiformat_responses")
    for col in _NEW_COLUMNS:
        assert col in after_columns, f"{col} 추가 실패"

    after_count = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_responses").fetchone()[0]
    print(f"응답 행 수 불변 확인: 전 {before_count} -> 후 {after_count}")
    assert before_count == after_count, "응답 행 수가 변경됨 - 예상치 못한 부작용"

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"integrity_check(마이그레이션 후): {integrity_after}")
    assert integrity_after == "ok", "마이그레이션 후 integrity_check 실패"

    conn.close()
    print("마이그레이션 완료")
    print(f"복구 명령: cp {backup_path} {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
