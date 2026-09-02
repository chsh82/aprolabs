"""meaning/level_min/idiom_hanja(4자) 세 조건을 모두 만족하는 idiom을
status='published'로 승격한다. 출제 가능 풀(v_pool_by_level)에 실제로
문항을 낼 수 있는 항목만 올리기 위한 게이트다.

실행:
    python scripts/vocab/promote_published.py --dry-run   # 저장 없이 대상만 출력
    python scripts/vocab/promote_published.py              # 실제 승격
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

from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402

_QUALIFYING_QUERY = """
    SELECT i.idiom_id, i.headword, i.status, i.level_min
    FROM idiom i
    WHERE i.meaning IS NOT NULL
      AND i.level_min IS NOT NULL
      AND (SELECT COUNT(*) FROM idiom_hanja h WHERE h.idiom_id = i.idiom_id) = 4
"""


def find_targets(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(_QUALIFYING_QUERY + " AND i.status != 'published'").fetchall()


def run(dry_run: bool) -> list[tuple]:
    conn = sqlite3.connect(get_vocab_db_path())
    try:
        targets = find_targets(conn)
        if not dry_run:
            conn.executemany(
                "UPDATE idiom SET status = 'published' WHERE idiom_id = ?",
                [(t[0],) for t in targets],
            )
            conn.commit()
        return targets
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="published 승격")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 대상만 출력")
    args = parser.parse_args()

    targets = run(dry_run=args.dry_run)
    print(f"승격 대상: {len(targets)}건")
    for idiom_id, headword, status, level_min in targets[:10]:
        print(f"  {headword} (idiom_id={idiom_id}, 현재 status={status}, level_min={level_min})")
    if len(targets) > 10:
        print(f"  ... ({len(targets) - 10}건 더)")

    if args.dry_run:
        print("--dry-run - DB에 저장하지 않음.")
    else:
        print(f"완료: {len(targets)}건 published로 승격")
    return 0


if __name__ == "__main__":
    sys.exit(main())
