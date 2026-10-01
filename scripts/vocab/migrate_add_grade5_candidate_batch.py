# -*- coding: utf-8 -*-
"""공식 5등급(중1~3, 경계 L3~L4 신호) 신규 어휘 확장 배치용 2개 테이블을
추가한다 - vocabulary_grade5_candidate_batch(스테이징, content_id 아닌
candidate_id가 PK), vocabulary_grade5_candidate_judgments(append-only
판정, 선택지 L3/L4/경계 유지/제외).

reports/nikl_grade5_candidate_list_and_review_design_20261001.md의 설계를
그대로 구현한다 - tier1(vocabulary_official_grade_reference)과 완전히
별도 테이블이다: 이번 100건은 아직 vocabulary_contents에 content_id가
없는 "신규 후보"라서 같은 키로 tier1 테이블에 넣을 수 없다(구조가 다른
검수 대상). vocabulary_contents/vocabulary_content_levels/문항/매니페스트는
이 테이블 어디에서도 FK로도 값으로도 건드리지 않는다 - 완전히 독립.

migrate_add_official_grade_reference.py와 같은 패턴 - CREATE TABLE IF NOT
EXISTS(멱등), 실행 전 SQLite Backup API 백업, db_path_guard로 APP_ENV=
research + VOCABULARY_QUIZ_DB_PATH(파일 실존) + (선택)--database 일치를
sqlite3.connect 호출 전에 전부 강제한다.

실행:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/migrate_add_grade5_candidate_batch.py [--database <같은경로>]
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

_BATCH_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_grade5_candidate_batch (
    candidate_id            TEXT PRIMARY KEY,  -- 결정적 파생값(어휘|품사|동형번호|소스해시) - content_id 아님, row-order 비의존
    batch_no                INTEGER NOT NULL,  -- 1(이번 100건), 2, 3...
    lemma                   TEXT NOT NULL,
    pos                     TEXT NOT NULL,
    homonym_number          INTEGER,
    official_meaning_short  TEXT,              -- 공식 자료 원문 의미(읽기 전용 표시 - 이 스크립트가 작성하지 않음)
    specialized_domain_flag INTEGER NOT NULL,  -- 전문어 분야 위험
    polysemy_risk_flag      INTEGER NOT NULL,  -- 다의어 위험
    proper_noun_risk_flag   INTEGER NOT NULL,  -- 고유명사 위험(이번 배치는 전부 0 - 공식자료에 해당 태그 없음)
    selection_reason        TEXT,
    official_grade          TEXT NOT NULL,     -- '5' 고정(이번 배치)
    proposed_level_note     TEXT NOT NULL,     -- '경계(L3~L4)' 고정 - 추가근거 없이 L3/L4 단정 안 함
    source_report_seq       INTEGER NOT NULL,
    source_file_sha256      TEXT NOT NULL,
    computed_at              TEXT NOT NULL,
    computed_by_script       TEXT NOT NULL
)
"""

_JUDGMENT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_grade5_candidate_judgments (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id                    TEXT NOT NULL REFERENCES vocabulary_grade5_candidate_batch(candidate_id),
    judgment                        TEXT NOT NULL CHECK (judgment IN ('L3','L4','경계 유지','제외')),
    rationale                       TEXT,
    reviewer_user_id                TEXT NOT NULL,
    reviewer_email                  TEXT,
    reviewed_at                     TEXT NOT NULL DEFAULT (datetime('now')),
    source_data_version_at_review   TEXT NOT NULL,  -- tier1과 동일한 staleness 해시 패턴
    submission_token                 TEXT,            -- 이중 클릭/중복 제출 방지(같은 토큰 재제출은 무시)
    created_at                       TEXT DEFAULT (datetime('now'))
)
"""

_INDEX_SQL_1 = (
    "CREATE INDEX IF NOT EXISTS idx_grade5_candidate_batch_batch_no "
    "ON vocabulary_grade5_candidate_batch(batch_no)"
)
_INDEX_SQL_2 = (
    "CREATE INDEX IF NOT EXISTS idx_grade5_candidate_judgments_candidate_id "
    "ON vocabulary_grade5_candidate_judgments(candidate_id)"
)
# submission_token 재제출 감지를 위한 부분 유니크 인덱스(토큰이 있는 행에만 적용 -
# 과거/다른 경로로 들어올 수 있는 NULL 토큰 행은 중복 판단에서 제외)
_INDEX_SQL_3 = (
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_grade5_candidate_judgments_token "
    "ON vocabulary_grade5_candidate_judgments(candidate_id, submission_token) "
    "WHERE submission_token IS NOT NULL"
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

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_migrate_grade5_candidate_batch_{ts}"
    src = connect_rw(db_path)
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
    cur.execute(_BATCH_TABLE_SQL)
    cur.execute(_JUDGMENT_TABLE_SQL)
    cur.execute(_INDEX_SQL_1)
    cur.execute(_INDEX_SQL_2)
    cur.execute(_INDEX_SQL_3)
    src.commit()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
        "('vocabulary_grade5_candidate_batch','vocabulary_grade5_candidate_judgments')"
    )
    found = {r[0] for r in cur.fetchall()}
    print("생성된 테이블:", found)
    src.close()

    if found != {"vocabulary_grade5_candidate_batch", "vocabulary_grade5_candidate_judgments"}:
        print("마이그레이션 실패")
        sys.exit(1)
    print("마이그레이션 완료(멱등 - 재실행해도 안전)")


if __name__ == "__main__":
    main()
