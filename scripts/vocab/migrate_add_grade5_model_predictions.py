# -*- coding: utf-8 -*-
"""Gemini(모델) 레벨 제안 저장용 테이블 1개를 추가한다 -
vocabulary_grade5_candidate_model_predictions.

사람 판정(vocabulary_grade5_candidate_judgments)과 완전히 분리된 별도
테이블이다 - 검수 화면 어디에서도 이 테이블을 읽어 렌더링하지 않는다
(사람에게 자동 제안·근거를 보여주지 않는다는 사용자 지시, app/vocabulary_
quiz/routers/grade5_candidate_review.py를 한 글자도 건드리지 않았다).

migrate_add_grade5_candidate_batch.py와 같은 패턴 - CREATE TABLE IF NOT
EXISTS(멱등), 실행 전 SQLite Backup API 백업, db_path_guard로 APP_ENV=
research + VOCABULARY_QUIZ_DB_PATH(파일 실존) + (선택)--database 일치를
sqlite3.connect 호출 전에 전부 강제한다.

실행:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/migrate_add_grade5_model_predictions.py [--database <같은경로>]
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

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vocabulary_grade5_candidate_model_predictions (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id           TEXT NOT NULL REFERENCES vocabulary_grade5_candidate_batch(candidate_id),
    model_name             TEXT NOT NULL,
    model_version          TEXT NOT NULL,
    prompt_template_hash   TEXT NOT NULL,
    predicted_judgment     TEXT,              -- L3/L4/경계 유지/검토 필요, 오류 시 NULL
    predicted_reason       TEXT,
    grounded               INTEGER,
    borderline             INTEGER,
    api_error              INTEGER NOT NULL DEFAULT 0,
    error_detail           TEXT,
    computed_at            TEXT NOT NULL
)
"""
_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_grade5_model_predictions_candidate_id "
    "ON vocabulary_grade5_candidate_model_predictions(candidate_id)"
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=None)
    args = parser.parse_args()

    try:
        db_path = guard_db_path(args.database)
    except DbPathGuardError as e:
        print(str(e))
        sys.exit(1)
    print("가드 통과, 대상 DB:", db_path)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_migrate_grade5_model_predictions_{ts}"
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
    cur.execute(_TABLE_SQL)
    cur.execute(_INDEX_SQL)
    src.commit()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='vocabulary_grade5_candidate_model_predictions'"
    )
    exists = cur.fetchone() is not None
    print("vocabulary_grade5_candidate_model_predictions 테이블 존재 확인:", exists)
    src.close()


if __name__ == "__main__":
    main()
