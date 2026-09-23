"""4단계(literacy 링크 적용)를 되돌리는 롤백 스크립트.

`vq_research_backup.py`가 apply 직전에 만든 백업 파일로 research DB를
완전히 되돌린다. `vocabulary_content_literacy_links` 테이블 생성과 142건
삽입 외에는 아무것도 건드리지 않았으므로(phase4 보고서 6절 체크섬으로
증명됨), 이 백업 하나로 그 이전 상태로 정확히 복원된다.

기본은 dry-run(무엇을 할지만 출력, 아무것도 바꾸지 않음). 실제 복원은
--confirm을 명시해야 실행된다.

하드 가드: --env-file에서 APP_ENV=research 직접 확인, --db-path/--backup-path
basename이 각각 'vocabulary_quiz_research.db'/그 백업 파일 패턴이 아니면 중단.

사용(dry-run):
    python3 rollback_literacy_link_apply.py \\
        --env-file /home/chsh82/aprolabs/.env \\
        --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \\
        --backup-path /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase4-pre-migration-20260923-214414

실제 복원:
    (위 명령에 --confirm 추가)
"""
from __future__ import annotations

import argparse
import datetime
import shutil
import sqlite3
import sys
from pathlib import Path


def read_app_env(env_file: Path) -> str | None:
    if not env_file.exists():
        return None
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("APP_ENV="):
            return line.split("=", 1)[1].strip()
    return None


def integrity_check(db_path: Path) -> tuple[bool, str]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    result = conn.execute("PRAGMA integrity_check").fetchall()
    conn.close()
    ok = len(result) == 1 and result[0][0] == "ok"
    return ok, (result[0][0] if result else str(result))


def table_row_counts(db_path: Path, tables: list[str]) -> dict[str, int]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    counts = {}
    for t in tables:
        try:
            counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            counts[t] = None  # 테이블 없음(예: 링크 테이블은 롤백 후 사라져야 정상)
    conn.close()
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--backup-path", type=Path, required=True)
    ap.add_argument("--confirm", action="store_true", help="실제로 복원 실행(없으면 dry-run)")
    args = ap.parse_args()

    app_env = read_app_env(args.env_file)
    print(f"GATE 1: env-file={args.env_file} APP_ENV={app_env!r}")
    if app_env != "research":
        print("GATE 1 FAIL: APP_ENV != research - 중단")
        return 1
    print("GATE 1 PASS")

    print(f"GATE 2: db-path basename={args.db_path.name!r}")
    if args.db_path.name != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: db-path가 research DB가 아님 - 중단")
        return 1
    if not args.db_path.exists():
        print(f"GATE 2 FAIL: db-path가 존재하지 않음: {args.db_path}")
        return 1
    print("GATE 2 PASS")

    print(f"GATE 3: backup-path={args.backup_path}")
    if not args.backup_path.exists():
        print("GATE 3 FAIL: 백업 파일이 존재하지 않음 - 중단")
        return 1
    if ".bak-" not in args.backup_path.name:
        print("GATE 3 FAIL: 백업 파일 이름 패턴('.bak-' 포함)이 아님 - 잘못된 파일을 복원할 위험, 중단")
        return 1
    backup_ok, backup_status = integrity_check(args.backup_path)
    print(f"  백업 PRAGMA integrity_check: {backup_status} ({'PASS' if backup_ok else 'FAIL'})")
    if not backup_ok:
        print("GATE 3 FAIL: 백업 파일 자체가 손상됨 - 이 백업으로는 복원하지 말 것")
        return 1
    print("GATE 3 PASS")

    tables = ["vocabulary_contents", "vocabulary_content_levels", "vocabulary_content_literacy_links"]
    print("\n현재 DB 상태:", table_row_counts(args.db_path, tables))
    print("백업 DB 상태:", table_row_counts(args.backup_path, tables))

    if not args.confirm:
        print("\n--- DRY RUN: 아래를 실행할 예정 (--confirm 없이는 아무것도 바꾸지 않음) ---")
        print(f"  1) 현재 DB를 안전 백업: {args.db_path} -> {args.db_path}.rollback-before-restore-<timestamp>")
        print(f"  2) 백업으로 복원: {args.backup_path} -> {args.db_path}")
        print("  3) 복원 후 PRAGMA integrity_check 재확인")
        return 0

    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    safety_copy = args.db_path.parent / f"{args.db_path.name}.rollback-before-restore-{ts}"
    print(f"\n1) 복구 전 현재 상태 안전 백업: {safety_copy}")
    shutil.copy2(args.db_path, safety_copy)

    print(f"2) 백업으로 복원: {args.backup_path} -> {args.db_path}")
    shutil.copy2(args.backup_path, args.db_path)

    ok, status = integrity_check(args.db_path)
    print(f"3) 복원 후 PRAGMA integrity_check: {status} ({'PASS' if ok else 'FAIL'})")
    print("복원 후 DB 상태:", table_row_counts(args.db_path, tables))

    print(f"\nSAFETY_COPY={safety_copy}")
    print(f"RESTORED_FROM={args.backup_path}")
    print(f"INTEGRITY_OK={ok}")
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
