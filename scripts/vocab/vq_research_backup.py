"""research vocabulary_quiz DB 마이그레이션 전 백업 (SQLite Backup API 사용).

반드시 research 서버에서 실행한다(로컬에는 이 DB가 없음). 표준 라이브러리
(sqlite3, argparse, hashlib)만 사용 - 별도 의존성 없음.

하드 가드: --env-file로 지정한 .env 파일에서 APP_ENV=research를 직접 읽어
확인하고, --db-path의 basename이 'vocabulary_quiz_research.db'가 아니면
무조건 중단한다. 둘 중 하나라도 실패하면 백업을 만들지 않는다.

사용:
    python3 vq_research_backup.py \\
        --env-file /home/chsh82/aprolabs/.env \\
        --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \\
        --backup-dir /home/chsh82/aprolabs_data/vocabulary_quiz/backups \\
        --label phase4-pre-migration
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import sqlite3
import sys
from pathlib import Path

CORE_TABLES = ["vocabulary_contents", "vocabulary_content_levels"]


def read_app_env(env_file: Path) -> str | None:
    if not env_file.exists():
        return None
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("APP_ENV="):
            return line.split("=", 1)[1].strip()
    return None


def table_row_counts(db_path: Path) -> dict[str, int]:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    counts = {}
    for t in CORE_TABLES:
        counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    conn.close()
    return counts


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--backup-dir", type=Path, required=True)
    ap.add_argument("--label", default="pre-migration")
    args = ap.parse_args()

    app_env = read_app_env(args.env_file)
    print(f"GATE 1: env-file={args.env_file} APP_ENV={app_env!r}")
    if app_env != "research":
        print("GATE 1 FAIL: APP_ENV != research - 백업/이후 어떤 쓰기도 하지 않고 중단")
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

    args.backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = args.backup_dir / f"vocabulary_quiz_research.db.bak-{args.label}-{ts}"

    print(f"\nSQLite Backup API로 백업 생성: {args.db_path} -> {backup_path}")
    src_uri = f"file:{args.db_path}?mode=ro"
    src = sqlite3.connect(src_uri, uri=True)
    src.execute("PRAGMA query_only=ON")
    dst = sqlite3.connect(str(backup_path))
    with dst:
        src.backup(dst)
    dst.close()
    src.close()
    print("백업 완료")

    print("\n무결성/복원 가능성 검증")
    src_counts = table_row_counts(args.db_path)
    bkp_counts = table_row_counts(backup_path)
    print(f"  원본 행수: {src_counts}")
    print(f"  백업 행수: {bkp_counts}")
    row_counts_match = src_counts == bkp_counts

    bkp_conn = sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True)
    bkp_conn.execute("PRAGMA query_only=ON")
    integrity = bkp_conn.execute("PRAGMA integrity_check").fetchall()
    fk_check = bkp_conn.execute("PRAGMA foreign_key_check").fetchall()
    bkp_conn.close()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"  백업 PRAGMA integrity_check: {integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"  백업 PRAGMA foreign_key_check 위반: {len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    backup_sha256 = file_sha256(backup_path)
    print(f"  백업 파일 SHA-256: {backup_sha256}")
    print(f"  백업 파일 크기: {backup_path.stat().st_size} bytes")

    all_pass = row_counts_match and integrity_ok and fk_ok
    print(f"\n최종: {'PASS' if all_pass else 'FAIL'}")
    print(f"BACKUP_PATH={backup_path}")
    print(f"BACKUP_SHA256={backup_sha256}")
    print(f"ROW_COUNTS_MATCH={row_counts_match}")
    print(f"INTEGRITY_OK={integrity_ok}")
    print(f"FK_OK={fk_ok}")
    return 0 if all_pass else 2


if __name__ == "__main__":
    sys.exit(main())
