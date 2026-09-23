"""vocabulary_multiformat_sessions에 metadata_json 컬럼을 추가한다.

관리자 레벨별 출제(v1)가 선택 조건(selected_vocab_level, confidence_mode,
level_version, candidate_count 등)을 저장할 범용 필드가 기존 세션 테이블에
없어서 추가한다 - 기존 item_types_json은 단순 리스트 형태라 용도를 바꾸면
그 값을 리스트로 읽는 기존 코드가 깨진다.

nullable TEXT 컬럼 추가라 CHECK 제약이 없고 SQLite ALTER TABLE ADD COLUMN
으로 충분하다(테이블 재구성 불필요). 기존 행은 전부 NULL로 남고, 결과
화면에서 "레벨 미지정"으로 표시한다 - 데이터 손실이나 의미 변경 없음.

실행 전 sqlite3.Connection.backup() API로 백업, 멱등(컬럼 존재 확인 후
스킵). 기존 세션/응답 데이터는 전혀 건드리지 않는다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_add_session_metadata.py
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


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
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

    if "metadata_json" in _columns(conn, "vocabulary_multiformat_sessions"):
        print("추가할 컬럼 없음 - metadata_json 이미 존재함 (멱등)")
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

    before_sessions = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_sessions").fetchone()[0]
    before_responses = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_responses").fetchone()[0]

    conn.execute("ALTER TABLE vocabulary_multiformat_sessions ADD COLUMN metadata_json TEXT")
    conn.commit()

    after_cols = _columns(conn, "vocabulary_multiformat_sessions")
    print(f"metadata_json 컬럼: {'추가됨' if 'metadata_json' in after_cols else '오류(추가 안 됨)'}")
    assert "metadata_json" in after_cols, "metadata_json 컬럼 추가 실패"

    after_sessions = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_sessions").fetchone()[0]
    after_responses = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_responses").fetchone()[0]
    print(f"  vocabulary_multiformat_sessions: 전 {before_sessions} -> 후 {after_sessions} "
          f"{'OK' if before_sessions == after_sessions else '!! 불일치'}")
    print(f"  vocabulary_multiformat_responses: 전 {before_responses} -> 후 {after_responses} "
          f"{'OK' if before_responses == after_responses else '!! 불일치'}")
    assert before_sessions == after_sessions and before_responses == after_responses, \
        "마이그레이션 중 기존 세션/응답 행 수가 변경됨"

    null_count = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_sessions WHERE metadata_json IS NULL"
    ).fetchone()[0]
    print(f"  기존 세션 metadata_json IS NULL: {null_count}건 (전부 NULL이어야 함, 총 {after_sessions}건)")
    assert null_count == after_sessions, "일부 기존 세션의 metadata_json이 NULL이 아님(예상치 못한 값)"

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"integrity_check(마이그레이션 후): {integrity_after}")
    assert integrity_after == "ok", "마이그레이션 후 integrity_check 실패"

    conn.close()
    print("마이그레이션 완료")
    print(f"복구 명령: cp {backup_path} {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
