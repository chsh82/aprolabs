"""검수 UI(Phase 6)용 컬럼 추가.

docs/literacy/06-검수UI.md 3절 참고.

- quiz_items.note TEXT - 검수 메모(자유 입력)
- quiz_items.reject_reason TEXT - X 판정 사유 코드(집계 대상이라 note와
  분리한 별도 컬럼으로 둔다 - schema 자료의 "소분류"를 note 텍스트에 넣었다가
  나중에 정규식 파싱을 해야 했던 전례가 있어서, 집계할 값은 컬럼으로 둔다)
- quiz_items.reviewed_at DATETIME
- terms.reviewed_at DATETIME

실행 전 `data/literacy.db`를 백업한다. 멱등 - 이미 있는 컬럼은 건너뛴다.

실행:
    python app/literacy/migrations/003_review_fields.py
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.literacy.db import DB_PATH  # noqa: E402

_COLUMNS_TO_ADD = [
    ("quiz_items", "note", "TEXT"),
    ("quiz_items", "reject_reason", "TEXT"),
    ("quiz_items", "reviewed_at", "DATETIME"),
    ("terms", "reviewed_at", "DATETIME"),
]


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def _backup(db_path: Path) -> Path | None:
    if not db_path.exists():
        return None
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def main() -> int:
    if not DB_PATH.exists():
        print(f"치명적 오류: {DB_PATH} 가 없습니다.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")

    to_add = [(t, c, ty) for t, c, ty in _COLUMNS_TO_ADD if not _has_column(conn, t, c)]
    already = [(t, c) for t, c, _ in _COLUMNS_TO_ADD if (t, c) not in [(t2, c2) for t2, c2, _ in to_add]]

    if already:
        for t, c in already:
            print(f"{t}.{c} 컬럼이 이미 존재함 - 건너뜀")

    if to_add:
        backup_path = _backup(DB_PATH)
        if backup_path:
            print(f"백업 완료: {backup_path}")
        for table, column, coltype in to_add:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
            print(f"{table}.{column} ({coltype}) 컬럼 추가 완료")
        conn.commit()
    else:
        print("추가할 컬럼 없음 - 전부 이미 존재함")

    print("\n=== 재실행 안전성 확인 ===")
    for table, column, _ in _COLUMNS_TO_ADD:
        exists = _has_column(conn, table, column)
        print(f"  {table}.{column}: {'존재' if exists else '없음(오류)'}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
