"""신규 관계 테이블 `vocabulary_content_literacy_links` 마이그레이션.

`vocabulary_contents`에 nullable `literacy_term_id` 단일 컬럼을 추가하는 안은
3단계 dry-run에서 속수무책·혼비백산 1:N 반례가 실제로 확인되어 폐기됐다
(reports/schema_reading_phase3_dryrun_20260924.md 4-1절). 대신 별도 관계
테이블을 최소 범위로 추가한다. literacy.db는 별도 SQLite 파일이라 SQL FK를
걸 수 없으므로 literacy_term_id는 정수 컬럼으로만 두고(교차 DB 참조),
애플리케이션 레벨에서 별도로 존재 여부를 검증한다(verify 스크립트 참고).

스키마:
    vocabulary_content_literacy_links(
        id, content_id (FK -> vocabulary_contents.content_id),
        literacy_term_id (literacy.db terms.id, 교차 DB 참조, SQL FK 불가),
        literacy_source, literacy_headword (연결 시점 스냅샷, drift 감지용),
        link_status, link_method, evidence,
        created_at, updated_at,
        UNIQUE(content_id, literacy_term_id)
    )

student_exposure/public_ready 관련 컬럼이 전혀 없다 - 이 테이블은 어떤
학생 노출 경로에도 연결되어 있지 않다(앱 코드가 아직 참조하지 않음).

하드 가드: --env-file에서 APP_ENV=research를 직접 읽어 확인하고, --db-path의
basename이 'vocabulary_quiz_research.db'가 아니면 무조건 중단한다.
기본은 dry-run(DDL만 출력, 실행 안 함) - --apply를 줘야 실제로 CREATE TABLE을
실행한다. CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS라 재실행해도
안전(멱등).
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS vocabulary_content_literacy_links (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id         TEXT NOT NULL REFERENCES vocabulary_contents(content_id),
        literacy_term_id   INTEGER NOT NULL,
        literacy_source    TEXT NOT NULL,
        literacy_headword  TEXT NOT NULL,
        link_status        TEXT NOT NULL DEFAULT 'CANDIDATE'
                            CHECK (link_status IN ('CANDIDATE','APPROVED','REJECTED')),
        link_method        TEXT NOT NULL,
        evidence           TEXT,
        created_at         TEXT DEFAULT (datetime('now')),
        updated_at         TEXT DEFAULT (datetime('now')),
        UNIQUE (content_id, literacy_term_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vcll_content_id ON vocabulary_content_literacy_links(content_id)",
    "CREATE INDEX IF NOT EXISTS idx_vcll_literacy_term_id ON vocabulary_content_literacy_links(literacy_term_id)",
]


def read_app_env(env_file: Path) -> str | None:
    if not env_file.exists():
        return None
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("APP_ENV="):
            return line.split("=", 1)[1].strip()
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--apply", action="store_true", help="실제로 CREATE TABLE 실행(기본은 dry-run)")
    args = ap.parse_args()

    app_env = read_app_env(args.env_file)
    print(f"GATE 1: APP_ENV={app_env!r}")
    if app_env != "research":
        print("GATE 1 FAIL: APP_ENV != research - 중단, 아무것도 실행하지 않음")
        return 1

    print(f"GATE 2: db-path basename={args.db_path.name!r}")
    if args.db_path.name != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: research DB가 아님 - 중단")
        return 1

    if not args.apply:
        print("\n--- DRY RUN (실행 안 함) ---")
        for stmt in DDL_STATEMENTS:
            print(stmt.strip())
        print("--- dry-run 끝. 실제 적용하려면 --apply 추가 ---")
        return 0

    print("\nGATE 통과 - 실제 CREATE TABLE 실행")
    conn = sqlite3.connect(str(args.db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    with conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(stmt)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='vocabulary_content_literacy_links'"
    ).fetchall()]
    print(f"테이블 생성 확인: {tables}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
