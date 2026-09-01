"""문해력 DB(`data/literacy.db`) 초기 스키마 생성.

실행:
    python app/literacy/migrations/001_initial.py            # 생성(멱등 - 이미 있으면 스킵)
    python app/literacy/migrations/001_initial.py --drop     # 기존 테이블 삭제 후 재생성(확인 프롬프트)

기존 데이터가 있는 상태에서 --drop 없이 재실행해도 아무 데이터도 지우지 않는다
(SQLAlchemy Base.metadata.create_all은 없는 테이블만 만든다).
"""
from __future__ import annotations

import sys
from pathlib import Path

# app/literacy/migrations/001_initial.py -> ... -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import inspect, text  # noqa: E402

from app.literacy.db import Base, DB_PATH, engine  # noqa: E402
from app.literacy import models  # noqa: E402,F401  - 모델 등록을 위해 import만 필요


def _confirm_drop() -> bool:
    answer = input("계속하려면 DROP을 입력하세요: ")
    return answer.strip() == "DROP"


def _print_table_counts() -> None:
    with engine.connect() as conn:
        for table in Base.metadata.sorted_tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table.name}")).scalar()
            print(f"  {table.name}: {count}건")


def main() -> int:
    drop = "--drop" in sys.argv

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if drop:
        print(f"'--drop' 지정됨 - {DB_PATH} 의 기존 테이블을 전부 삭제하고 재생성합니다.")
        if not _confirm_drop():
            print("확인 문자열이 일치하지 않아 취소합니다.")
            return 1
        Base.metadata.drop_all(bind=engine)
        print("기존 테이블 삭제 완료")

    inspector = inspect(engine)
    existing_before = set(inspector.get_table_names())
    expected_tables = set(Base.metadata.tables.keys())

    if not drop and expected_tables <= existing_before:
        print(f"이미 전부 존재함 - 아무것도 하지 않음: {sorted(expected_tables)}")
        print("\n현재 테이블별 행 수:")
        _print_table_counts()
        return 0

    Base.metadata.create_all(bind=engine)

    inspector = inspect(engine)
    existing_after = set(inspector.get_table_names())
    created = sorted(expected_tables - existing_before)

    if created:
        print(f"새로 생성된 테이블: {created}")
    else:
        print("새로 생성된 테이블 없음(이미 전부 존재)")

    missing = expected_tables - existing_after
    if missing:
        print(f"경고: 생성 후에도 없는 테이블: {sorted(missing)}", file=sys.stderr)
        return 1

    print(f"\nDB 파일: {DB_PATH}")
    print("현재 테이블별 행 수:")
    _print_table_counts()
    return 0


if __name__ == "__main__":
    sys.exit(main())
