"""문해력 DB에 `level`(0~6) 컬럼 추가 + 기존 데이터 백필.

docs/literacy/04-스키마리딩어휘적재.md 1·3절 참고. `grade_level`(1~12,
초1~고3)은 실제로는 없는 정밀도라 판단하고, 2개 학년을 묶은 `level`
(0~6)을 새 기본 기준으로 도입한다. **`grade_level`은 지우지 않는다.**

레벨 대응표:
    level 0 = 초1~2 (grade_level 1,2)
    level 1 = 초3~4 (grade_level 3,4)
    level 2 = 초5~6 (grade_level 5,6)
    level 3 = 중1~2 (grade_level 7,8)
    level 4 = 중3    (grade_level 9)
    level 5 = 고1~2 (grade_level 10,11)
    level 6 = 고3    (grade_level 12)

백필 대상:
    source='momo-textbook' (821건) - 위 표로 grade_level -> level 변환
    source='krdict'         (2,884건) - level은 NULL로 둔다(등급 정보 없음)

실행 전 `data/literacy.db`를 `data/literacy.db.bak-YYYYMMDD-HHMMSS`로
백업한다(커밋 안 함 - `*.db.bak*`가 이미 .gitignore에 있어 별도 규칙 불필요).

실행:
    python app/literacy/migrations/002_add_level.py   # 멱등 - 이미 적용됐으면 스킵
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

# grade_level -> level 매핑. 8까지만 실제 데이터에 존재하지만(검증 완료),
# 표 전체를 그대로 둔다 - 나중에 9~12에 해당하는 자료가 들어와도 그대로 맞는다.
_GRADE_TO_LEVEL = {
    1: 0, 2: 0,
    3: 1, 4: 1,
    5: 2, 6: 2,
    7: 3, 8: 3,
    9: 4,
    10: 5, 11: 5,
    12: 6,
}


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
        print(f"치명적 오류: {DB_PATH} 가 없습니다. 001_initial.py를 먼저 실행하세요.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")

    already_has_column = _has_column(conn, "terms", "level")

    if already_has_column:
        # 컬럼이 이미 있어도 백필 자체는 WHERE level IS NULL 조건이라 재실행 안전.
        # 다만 이미 적용된 상태에서 다시 백업을 뜰 필요는 없으므로 백업은 건너뛴다.
        print("terms.level 컬럼이 이미 존재함 - 컬럼 추가는 건너뜀")
    else:
        backup_path = _backup(DB_PATH)
        if backup_path:
            print(f"백업 완료: {backup_path}")
        conn.execute("ALTER TABLE terms ADD COLUMN level INTEGER")
        conn.commit()
        print("terms.level 컬럼 추가 완료")

    conn.execute("CREATE INDEX IF NOT EXISTS ix_terms_level ON terms(level)")
    conn.commit()

    updated = 0
    for grade, level in _GRADE_TO_LEVEL.items():
        cur = conn.execute(
            """
            UPDATE terms SET level = ?
            WHERE source = 'momo-textbook' AND grade_level = ? AND level IS NULL
            """,
            (level, grade),
        )
        updated += cur.rowcount
    conn.commit()
    print(f"momo-textbook 백필: {updated}건 (이미 채워져 있던 행은 재적용 안 함)")

    krdict_null = conn.execute(
        "SELECT COUNT(*) FROM terms WHERE source = 'krdict' AND level IS NULL"
    ).fetchone()[0]
    krdict_total = conn.execute("SELECT COUNT(*) FROM terms WHERE source = 'krdict'").fetchone()[0]
    print(f"krdict: level NULL {krdict_null}/{krdict_total}건 (전부 NULL이어야 정상)")

    grade_level_preserved = conn.execute(
        "SELECT COUNT(*) FROM terms WHERE source = 'momo-textbook' AND grade_level IS NOT NULL"
    ).fetchone()[0]
    print(f"momo-textbook grade_level 보존 확인: {grade_level_preserved}건 (821건이어야 함)")

    print("\nlevel별 분포 (momo-textbook):")
    for row in conn.execute(
        "SELECT level, COUNT(*) FROM terms WHERE source = 'momo-textbook' GROUP BY level ORDER BY level"
    ):
        print(f"  {row}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
