"""어휘 게임 DB(`data/vocab/idiom.db`)에 `attempt` 테이블 추가.

idiom/inclusion_evidence/hanja/idiom_hanja/example/relation/topic_link/
corpus_hit 8개는 data/vocab/schema.sql로 이미 생성되어 있다. attempt는
HANDOFF.md 5절("학습 기록")에서 스키마에 없던 것으로 지적된 테이블 -
여기서 처음 추가한다.

Base.metadata.create_all은 없는 테이블만 만들고 기존 8개는 그대로
둔다(SQLAlchemy가 이미 존재하는 테이블을 건드리지 않음) - 이미 있는
데이터를 지우지 않는다.

실행:
    python app/vocab/migrations/001_add_attempt.py            # 생성(멱등 - 이미 있으면 스킵)
    python app/vocab/migrations/001_add_attempt.py --drop     # 기존 테이블 전부 삭제 후 재생성(확인 프롬프트)
"""
from __future__ import annotations

import sys
from pathlib import Path

# app/vocab/migrations/001_add_attempt.py -> ... -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy import inspect, text  # noqa: E402

from app.vocab.db import Base, DB_PATH, engine  # noqa: E402
from app.vocab import models  # noqa: E402,F401  - 모델 등록을 위해 import만 필요


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

    if not DB_PATH.exists():
        print(f"치명적 오류: {DB_PATH} 가 없습니다. data/vocab/schema.sql을 먼저 적용하세요.", file=sys.stderr)
        return 1

    if drop:
        print(f"'--drop' 지정됨 - {DB_PATH} 의 기존 테이블을 전부 삭제하고 재생성합니다.")
        print("주의: schema.sql로 만든 8개 테이블(현재 비어 있음)도 함께 삭제됩니다.")
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
