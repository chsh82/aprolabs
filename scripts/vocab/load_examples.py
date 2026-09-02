"""example 테이블에 CSV로 용례/상황문을 적재한다.

CSV 컬럼: idiom_id, sentence, context_type, grade_band, source
(schema.sql의 CHECK 제약대로 context_type은 situation/essay/literature/
media/dialogue 중 하나여야 한다 - situation4 형식은 그중 'situation'을 쓴다)

같은 idiom_id에 같은 context_type이 이미 있으면 건너뛴다. situation4는
idiom당 상황문이 여러 개일 필요가 없어서(퀴즈 하나에 하나만 쓴다) 이
조합으로 중복을 막는다 - 성격이 다른 문맥(예: situation과 essay)은
각각 따로 들어갈 수 있다.

실행:
    python scripts/vocab/load_examples.py <csv경로> --dry-run
    python scripts/vocab/load_examples.py <csv경로>
"""
from __future__ import annotations

import argparse
import csv
import io
import sqlite3
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path  # noqa: E402

REQUIRED_COLUMNS = {"idiom_id", "sentence", "context_type", "grade_band", "source"}
VALID_CONTEXT_TYPES = {"situation", "essay", "literature", "media", "dialogue"}


def read_rows(csv_path: Path) -> list[dict]:
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV에 필요한 컬럼이 없습니다: {sorted(missing)}")
        rows = list(reader)

    invalid_types = {r["context_type"] for r in rows} - VALID_CONTEXT_TYPES
    if invalid_types:
        raise ValueError(
            f"context_type에 허용되지 않는 값이 있습니다: {sorted(invalid_types)} "
            f"(허용값: {sorted(VALID_CONTEXT_TYPES)})"
        )
    return rows


def find_targets(conn: sqlite3.Connection, rows: list[dict]) -> tuple[list[dict], list[dict]]:
    targets, skipped = [], []
    for row in rows:
        exists = conn.execute(
            "SELECT 1 FROM example WHERE idiom_id=? AND context_type=?",
            (int(row["idiom_id"]), row["context_type"]),
        ).fetchone()
        (skipped if exists else targets).append(row)
    return targets, skipped


def run(csv_path: Path, dry_run: bool) -> dict:
    rows = read_rows(csv_path)
    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        targets, skipped = find_targets(conn, rows)
        if not dry_run:
            for row in targets:
                conn.execute(
                    "INSERT INTO example (idiom_id, sentence, context_type, grade_band, source) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        int(row["idiom_id"]), row["sentence"], row["context_type"],
                        row["grade_band"] or None, row["source"] or None,
                    ),
                )
            conn.commit()
        return {"total": len(rows), "targets": targets, "skipped": skipped}
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="example 테이블에 CSV 적재")
    parser.add_argument("csv_path", type=Path, help="idiom_id,sentence,context_type,grade_band,source 컬럼을 가진 CSV")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 대상만 출력")
    args = parser.parse_args()

    result = run(args.csv_path, dry_run=args.dry_run)
    print(f"CSV 총 {result['total']}행")
    print(f"적재 대상: {len(result['targets'])}건")
    print(f"건너뜀(같은 idiom_id+context_type 이미 있음): {len(result['skipped'])}건")
    for row in result["skipped"]:
        print(f"  skip: idiom_id={row['idiom_id']} context_type={row['context_type']}")

    if args.dry_run:
        print("--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
    else:
        print(f"완료: {len(result['targets'])}건 적재")
    return 0


if __name__ == "__main__":
    sys.exit(main())
