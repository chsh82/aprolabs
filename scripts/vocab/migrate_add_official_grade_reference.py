# -*- coding: utf-8 -*-
"""국립국어원 공식 어휘 등급을 담을 순수 참조 테이블을 추가한다.

migrate_add_vocabulary_levels.py와 같은 패턴 - 신규 테이블 추가만이라
SQLite ALTER TABLE 제약에 걸리지 않는다. 실행 전 sqlite3.Connection.backup()
API로 백업, 멱등(CREATE TABLE IF NOT EXISTS). 기존 vocabulary_contents/
vocabulary_content_levels/vocabulary_multiformat_items 등은 전혀 건드리지
않는다 - 이 테이블은 vocabulary_contents.content_id를 FK로만 참조하는
읽기 전용 분석/참조 데이터다. 어떤 서빙 코드도 이 테이블을 아직 읽지
않는다(이번 작업 범위 밖).

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_add_official_grade_reference.py
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
CREATE TABLE IF NOT EXISTS vocabulary_official_grade_reference (
    content_id                     TEXT PRIMARY KEY REFERENCES vocabulary_contents(content_id),
    official_grade                 TEXT,      -- '1'..'5' 또는 NULL(다중후보/매칭없음/표제어만일치 - 절대 추정 금지)
    proposed_base_level            TEXT,      -- 'L0'..'L2', '경계(L3~L4)', 또는 NULL
    proposed_base_level_note       TEXT,
    match_type                     TEXT NOT NULL CHECK (match_type IN (
                                        '단일일치', '단일일치_동형이의주의', '다중후보', '매칭없음', '표제어만일치'
                                    )),
    standard_homonym_number        INTEGER,
    exception_reason                TEXT,
    exception_reason_secondary      TEXT,
    priority_tier                  INTEGER,   -- NULL 또는 1/2/3/4(예외 823건에만)
    review_status                   TEXT NOT NULL,  -- 기계 제안 상태값 - 사람 승인 아님
    human_approval_status           TEXT,      -- 이번 적재는 항상 NULL - 향후 사람 검수용 예약 컬럼
    current_vocab_level_snapshot   INTEGER,   -- 적재 시점 vocab_level 스냅샷(드리프트 탐지용, 자동 갱신 안 됨)
    current_level_status_snapshot  TEXT,
    level_source                    TEXT,
    source_report_seq              INTEGER NOT NULL,
    source_file_sha256              TEXT NOT NULL,
    source_version_label            TEXT NOT NULL,
    computed_at                     TEXT NOT NULL,
    computed_by_script              TEXT NOT NULL
)
"""

_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_official_grade_ref_review_status "
    "ON vocabulary_official_grade_reference(review_status)"
)


def main() -> None:
    db_path = get_db_path()
    print("대상 DB:", db_path)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_migrate_official_grade_ref_{ts}"
    src = sqlite3.connect(db_path)
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

    cur = src.cursor()
    cur.execute(_NEW_TABLE_SQL)
    cur.execute(_INDEX_SQL)
    src.commit()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vocabulary_official_grade_reference'")
    exists = cur.fetchone() is not None
    print("vocabulary_official_grade_reference 테이블 존재 확인:", exists)
    src.close()

    if not exists:
        print("마이그레이션 실패")
        sys.exit(1)
    print("마이그레이션 완료(멱등 - 재실행해도 안전)")


if __name__ == "__main__":
    main()
