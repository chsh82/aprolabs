"""hanja 테이블에 stroke_count 컬럼 추가.

calc_level.py가 hanja_score(획수 평균 기반 임시 규칙)를 계산하려면 획수
데이터가 필요한데, data/vocab/schema.sql의 hanja 테이블에는 애초에
stroke_count가 없었다(literacy DB의 Hanja 모델에는 있었지만 vocab
schema.sql에는 빠졌던 것). scripts/vocab/populate_hanja_strokes.py가
이 컬럼을 Unicode Unihan 데이터로 채운다.

실행:
    python app/vocab/migrations/002_add_hanja_stroke_count.py   # 멱등 - 이미 있으면 스킵
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path  # noqa: E402


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def main() -> int:
    conn = sqlite3.connect(get_db_path())

    if _has_column(conn, "hanja", "stroke_count"):
        print("hanja.stroke_count 컬럼이 이미 존재함 - 아무것도 하지 않음")
        conn.close()
        return 0

    conn.execute("ALTER TABLE hanja ADD COLUMN stroke_count INTEGER")
    conn.commit()
    print("hanja.stroke_count 컬럼 추가 완료 (값은 비어 있음 - "
          "scripts/vocab/populate_hanja_strokes.py로 채울 것)")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
