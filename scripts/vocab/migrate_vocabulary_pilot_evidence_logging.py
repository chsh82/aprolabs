"""Closed Pilot behavioral evidence logging columns for vocabulary multiformat runtime.

Adds cohort/tester flags to sessions and exposure/answer snapshots to responses.
Idempotent, backs up the target DB first, and preserves all existing rows.

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_vocabulary_pilot_evidence_logging.py
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

_SESSION_COLUMNS = {
    "student_cohort_id": "TEXT",
    "is_internal_tester": "INTEGER NOT NULL DEFAULT 1",
    "is_verified_student": "INTEGER NOT NULL DEFAULT 0",
}

_RESPONSE_COLUMNS = {
    "sense_id": "TEXT",
    "service_level": "INTEGER",
    "response_time_ms": "INTEGER",
    "attempt_no": "INTEGER",
    "presented_at": "TEXT",
    "item_status_at_exposure": "TEXT",
    "content_release_version": "TEXT",
}


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _backup(db_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-pilot-evidence-{timestamp}")
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
        conn.close()
        print(f"치명적 오류: 마이그레이션 전 integrity_check 실패: {integrity}", file=sys.stderr)
        return 1

    session_existing = _columns(conn, "vocabulary_multiformat_sessions")
    response_existing = _columns(conn, "vocabulary_multiformat_responses")
    session_to_add = {c: t for c, t in _SESSION_COLUMNS.items() if c not in session_existing}
    response_to_add = {c: t for c, t in _RESPONSE_COLUMNS.items() if c not in response_existing}

    if not session_to_add and not response_to_add:
        print("추가할 컬럼 없음 - pilot evidence logging columns already exist (멱등)")
        conn.close()
        return 0

    before_sessions = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_sessions").fetchone()[0]
    before_responses = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_responses").fetchone()[0]
    backup_path = _backup(db_path)
    print(f"백업 완료(sqlite3.Connection.backup API): {backup_path}")

    for col, coltype in session_to_add.items():
        conn.execute(f"ALTER TABLE vocabulary_multiformat_sessions ADD COLUMN {col} {coltype}")
        print(f"  vocabulary_multiformat_sessions.{col} 컬럼 추가됨")
    for col, coltype in response_to_add.items():
        conn.execute(f"ALTER TABLE vocabulary_multiformat_responses ADD COLUMN {col} {coltype}")
        print(f"  vocabulary_multiformat_responses.{col} 컬럼 추가됨")
    conn.commit()

    after_sessions = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_sessions").fetchone()[0]
    after_responses = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_responses").fetchone()[0]
    print(f"세션 행 수 불변 확인: 전 {before_sessions} -> 후 {after_sessions}")
    print(f"응답 행 수 불변 확인: 전 {before_responses} -> 후 {after_responses}")
    assert before_sessions == after_sessions, "세션 행 수가 변경됨"
    assert before_responses == after_responses, "응답 행 수가 변경됨"

    final_session_cols = _columns(conn, "vocabulary_multiformat_sessions")
    final_response_cols = _columns(conn, "vocabulary_multiformat_responses")
    missing = sorted(set(_SESSION_COLUMNS) - final_session_cols) + sorted(set(_RESPONSE_COLUMNS) - final_response_cols)
    assert not missing, f"컬럼 추가 실패: {missing}"

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"integrity_check(마이그레이션 후): {integrity_after}")
    assert integrity_after == "ok", "마이그레이션 후 integrity_check 실패"
    conn.close()
    print("마이그레이션 완료")
    print(f"복구 명령: cp {backup_path} {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
