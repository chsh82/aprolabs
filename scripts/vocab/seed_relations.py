"""relation(confusable) 시드 적재 - SQL 파일을 그대로 실행한다.

혼동쌍은 자동 생성하지 않는다(docs/vocab/MISSING.md 참고 - 의미가 비슷하거나
헷갈리기 쉬운 성어 쌍을 아는 건 자동화가 어렵다). SQL 파일은 사람이 직접
검수해서 만든 쌍 목록이고, 이 스크립트는 그 파일을 실행만 한다 - 쌍 목록
자체를 여기서 만들지 않는다.

seed_relations*.sql은 `INSERT OR IGNORE` + relation의
UNIQUE(idiom_a, idiom_b, rel_type) 제약을 쓰므로 재실행해도 안전하다(멱등).

실행 (인자를 안 주면 seed_relations.sql):
    python scripts/vocab/seed_relations.py
    python scripts/vocab/seed_relations.py seed_relations_batch2.sql
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser(description="relation(confusable) 시드 SQL 적재")
    parser.add_argument("sql_file", nargs="?", default="seed_relations.sql",
                         help="scripts/vocab/ 기준 파일명 또는 절대경로 (기본: seed_relations.sql)")
    args = parser.parse_args()

    sql_path = Path(args.sql_file)
    if not sql_path.is_absolute():
        sql_path = SCRIPT_DIR / sql_path

    conn = sqlite3.connect(get_db_path())
    try:
        before = conn.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
        conn.executescript(sql_path.read_text(encoding="utf-8"))
        after = conn.execute("SELECT COUNT(*) FROM relation").fetchone()[0]

        n_with_pair = conn.execute(
            """
            SELECT COUNT(DISTINCT x.idiom_id) FROM (
                SELECT idiom_a AS idiom_id FROM relation WHERE rel_type='confusable'
                UNION
                SELECT idiom_b AS idiom_id FROM relation WHERE rel_type='confusable'
            ) x
            JOIN idiom i ON i.idiom_id = x.idiom_id
            WHERE i.status = 'published'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    print(f"relation 적재 전: {before}건, 적재 후: {after}건 (신규 {after - before}건)")
    print(f"published 중 혼동쌍을 가진 표제어: {n_with_pair}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())
