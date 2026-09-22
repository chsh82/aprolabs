"""vocabulary_multiformat_items에 CROSSWORD(십자말) 유형 지원을 추가한다.

두 가지를 바꿔야 한다:
  1. public_payload_json 컬럼 추가 - 이건 ALTER TABLE ADD COLUMN으로 충분하다.
  2. item_type CHECK 제약조건에 'CROSSWORD'를 추가 - SQLite는 ALTER TABLE로
     기존 CHECK 제약조건을 바꿀 수 없으므로, 테이블을 새로 만들어 데이터를
     옮기고 이름을 바꾸는 표준 절차(SQLite 공식 문서의 "12단계 절차")를 쓴다.

기존 행의 데이터는 전혀 잃지 않는다 - INSERT INTO new SELECT * FROM old로
그대로 복사하고, public_payload_json은 기존 행에서 NULL로 남는다(다음
import_multiformat_quiz.py --apply 재실행이 자동으로 채워 넣는다 - 값이
바뀌므로 이번엔 "unchanged"가 아니라 "updated"로 잡힌다).

실행 전 sqlite3.Connection.backup() API로 스냅샷을 뜨고, 실행 후
integrity_check + foreign_key_check + 행 수 불변을 검증한다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/migrate_vocabulary_quiz_add_crossword_support.py
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

_OLD_COLUMNS = [
    "id", "item_id", "item_type", "source_content_id", "source_content_ids_json",
    "sense_id", "sense_ids_json", "lemma", "pos", "prompt", "options_json",
    "correct_option", "answer_payload_json", "explanation", "cognitive_level",
    "qa_flags_json", "generator_version", "source_version", "is_active",
    "created_at", "updated_at",
]

_NEW_TABLE_SQL = """
CREATE TABLE vocabulary_multiformat_items_new (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id                  TEXT NOT NULL UNIQUE,
    item_type                TEXT NOT NULL CHECK (item_type IN
                              ('MEANING_CHOICE','WORD_FROM_DEFINITION','CONTEXT_MEANING',
                               'CONTEXT_CLOZE','MATCH_WORD_MEANING','CROSSWORD')),
    source_content_id        TEXT REFERENCES vocabulary_contents(content_id),
    source_content_ids_json  TEXT,
    sense_id                 TEXT,
    sense_ids_json           TEXT,
    lemma                    TEXT,
    pos                      TEXT,
    prompt                   TEXT NOT NULL,
    options_json             TEXT,
    correct_option           INTEGER CHECK (correct_option IS NULL OR correct_option BETWEEN 1 AND 4),
    public_payload_json      TEXT,
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
"""

_INDEX_SQL = """
CREATE INDEX idx_vmi_item_type ON vocabulary_multiformat_items(item_type);
CREATE INDEX idx_vmi_source_content ON vocabulary_multiformat_items(source_content_id);
CREATE INDEX idx_vmi_source_version ON vocabulary_multiformat_items(source_version);
"""

# v_vmf_item_type_counts 뷰가 이 테이블을 참조하므로 테이블을 DROP하기 전에
# 뷰부터 지워야 한다(안 그러면 "no such table" 오류) - 재구성 후 원래 정의 그대로 재생성한다.
_VIEW_SQL = """
CREATE VIEW v_vmf_item_type_counts AS
SELECT source_version, item_type, COUNT(*) AS n
FROM vocabulary_multiformat_items
GROUP BY source_version, item_type;
"""


def _column_names(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _already_migrated(conn: sqlite3.Connection) -> bool:
    cols = _column_names(conn, "vocabulary_multiformat_items")
    if "public_payload_json" not in cols:
        return False
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='vocabulary_multiformat_items'"
    ).fetchone()
    return row is not None and "CROSSWORD" in row[0]


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

    if _already_migrated(conn):
        print("이미 마이그레이션됨 (public_payload_json 컬럼 + CROSSWORD 허용 확인) - 멱등")
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

    counts_before = {}
    for t in ["vocabulary_multiformat_items", "vocabulary_contents", "vocabulary_items",
              "vocabulary_review_samples", "vocabulary_multiformat_sessions", "vocabulary_multiformat_responses"]:
        counts_before[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]

    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DROP VIEW IF EXISTS v_vmf_item_type_counts")
        conn.execute(_NEW_TABLE_SQL)
        cols = ", ".join(_OLD_COLUMNS)
        conn.execute(
            f"INSERT INTO vocabulary_multiformat_items_new ({cols}) "
            f"SELECT {cols} FROM vocabulary_multiformat_items"
        )
        conn.execute("DROP TABLE vocabulary_multiformat_items")
        conn.execute("ALTER TABLE vocabulary_multiformat_items_new RENAME TO vocabulary_multiformat_items")
        for stmt in _INDEX_SQL.strip().split("\n"):
            if stmt.strip():
                conn.execute(stmt)
        conn.execute(_VIEW_SQL)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")

    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    print(f"foreign_key_check: {'위반 없음' if not fk_violations else f'위반 {len(fk_violations)}건'}")
    assert not fk_violations, f"foreign_key_check 위반 발견: {fk_violations}"

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"integrity_check(마이그레이션 후): {integrity_after}")
    assert integrity_after == "ok", "마이그레이션 후 integrity_check 실패"

    for t, before in counts_before.items():
        after = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: 전 {before} -> 후 {after} {'OK' if before == after else '!! 불일치'}")
        assert before == after, f"{t} 행 수가 변경됨 - 예상치 못한 부작용"

    cols_after = _column_names(conn, "vocabulary_multiformat_items")
    print(f"public_payload_json 컬럼 존재: {'public_payload_json' in cols_after}")
    assert "public_payload_json" in cols_after

    conn.close()
    print("마이그레이션 완료 - item_type CHECK에 CROSSWORD 추가됨, public_payload_json 컬럼 추가됨")
    print(f"복구 명령: cp {backup_path} {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
