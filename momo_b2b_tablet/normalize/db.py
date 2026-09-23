"""momo_book_db/momo_book.db 읽기 전용 연결.

SPEC §1 설계원칙 2: "원본 DB는 수정하지 않는다." - 이 모듈은 SELECT만 한다.
momo_b2b_tablet은 momo_book_db와 별개 디렉터리이지만(사용자 지시), 원본
교재 데이터는 그대로 momo_book_db/momo_book.db를 읽는다 - 데이터를 복제하지
않는다.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = REPO_ROOT / "momo_book_db" / "momo_book.db"


def get_connection() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"momo_book.db가 없습니다: {DB_PATH}")
    # 읽기 전용 모드로 연다 - 실수로 원본을 건드리는 코드가 있어도 여기서 막힌다.
    uri = f"file:{DB_PATH.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn
