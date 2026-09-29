# -*- coding: utf-8 -*-
"""tier1(31건) 공식 등급 검토 화면의 판정 저장용 테이블
(vocabulary_official_grade_judgments)을 추가한다 - 순수 추가형(append-only),
vocab_level/문항/매니페스트/student_exposure/public_ready는 전혀 건드리지
않는다. 적재 직후 이 테이블은 항상 0행이다(사람이 실제로 화면에서 판정을
저장해야만 행이 생긴다 - 이 스크립트가 어떤 행도 자동으로 채우지 않는다).

migrate_add_official_grade_reference.py와 완전히 같은 안전 패턴
(db_path_guard.py로 APP_ENV=research + VOCABULARY_QUIZ_DB_PATH 실존을
sqlite3.connect 전에 확인, SQLite Backup API 백업 + 복원 가능성 검증,
멱등 CREATE TABLE IF NOT EXISTS).

실행:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/migrate_add_official_grade_judgments.py [--database <같은경로>]
"""
from __future__ import annotations

import argparse
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

from scripts.vocab.db_path_guard import DbPathGuardError, connect_rw, guard_db_path  # noqa: E402

_NEW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_official_grade_judgments (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id                      TEXT NOT NULL REFERENCES vocabulary_official_grade_reference(content_id),
    judgment                        TEXT NOT NULL CHECK (judgment IN ('기본 레벨 조정', '현재 유지', '뜻 확인', '보류')),
    rationale                       TEXT,
    reviewer_user_id                TEXT NOT NULL,
    reviewer_email                  TEXT,
    reviewed_at                     TEXT NOT NULL DEFAULT (datetime('now')),
    source_data_version_at_review   TEXT NOT NULL,
    created_at                      TEXT DEFAULT (datetime('now'))
)
"""

_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_official_grade_judgments_content_id "
    "ON vocabulary_official_grade_judgments(content_id)"
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=None, help="VOCABULARY_QUIZ_DB_PATH와 반드시 일치해야 함(교차 확인용)")
    args = parser.parse_args()

    try:
        db_path = guard_db_path(args.database)
    except DbPathGuardError as e:
        print(str(e))
        sys.exit(1)
    print("가드 통과, 대상 DB:", db_path)

    src = connect_rw(db_path)
    cur = src.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vocabulary_official_grade_reference'")
    if cur.fetchone() is None:
        print("선행 조건 실패: vocabulary_official_grade_reference 테이블이 없습니다 - "
              "먼저 migrate_add_official_grade_reference.py + apply_official_grade_reference.py --apply를 실행하세요")
        src.close()
        sys.exit(1)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_migrate_official_grade_judgments_{ts}"
    dst = sqlite3.connect(backup_path)
    with dst:
        src.backup(dst)
    dst.close()
    print("백업 생성:", backup_path)

    verify = sqlite3.connect(backup_path)
    integrity = verify.execute("PRAGMA integrity_check").fetchone()[0]
    verify.close()
    if integrity != "ok":
        print("백업 무결성 실패 - 중단:", integrity)
        src.close()
        sys.exit(1)
    print("백업 무결성 확인: ok")

    cur.execute(_NEW_TABLE_SQL)
    cur.execute(_INDEX_SQL)
    src.commit()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vocabulary_official_grade_judgments'")
    exists = cur.fetchone() is not None
    cur.execute("SELECT COUNT(*) FROM vocabulary_official_grade_judgments")
    row_count = cur.fetchone()[0]
    print("vocabulary_official_grade_judgments 테이블 존재 확인:", exists, "/ 현재 행수:", row_count)
    src.close()

    if not exists:
        print("마이그레이션 실패")
        sys.exit(1)
    if row_count != 0:
        print("경고: 신규 테이블인데 0행이 아닙니다 - 예상 밖 상태")
        sys.exit(1)
    print("마이그레이션 완료(멱등 - 재실행해도 안전, 0행 확인됨 - 자동 채움 없음)")


if __name__ == "__main__":
    main()
