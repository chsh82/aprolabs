"""hanja.stroke_count를 scripts/vocab/hanja_strokes_unihan.tsv(Unicode Unihan
Database kTotalStrokes, 2026-09-03 조회)로 채운다. 1회성 - 이후 calc_level.py는
DB에 저장된 이 값만 쓰고 외부 조회를 다시 하지 않는다.

실행:
    python scripts/vocab/populate_hanja_strokes.py --dry-run   # 저장 없이 대상만 출력
    python scripts/vocab/populate_hanja_strokes.py              # 실제 채움
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402

LOOKUP_PATH = Path(__file__).resolve().parent / "hanja_strokes_unihan.tsv"


def load_lookup() -> dict[str, int]:
    lookup = {}
    with open(LOOKUP_PATH, encoding="utf-8") as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            char, _codepoint, stroke_count = line.rstrip("\n").split("\t")
            lookup[char] = int(stroke_count)
    return lookup


def run(dry_run: bool) -> dict:
    lookup = load_lookup()
    conn = sqlite3.connect(get_vocab_db_path())
    try:
        rows = conn.execute("SELECT char, stroke_count FROM hanja").fetchall()
        targets = []
        missing_in_lookup = []
        for char, current in rows:
            if current is not None:
                continue
            if char not in lookup:
                missing_in_lookup.append(char)
                continue
            targets.append((char, lookup[char]))

        if not dry_run:
            for char, stroke_count in targets:
                conn.execute("UPDATE hanja SET stroke_count = ? WHERE char = ?", (stroke_count, char))
            conn.commit()

        return {"total": len(rows), "targets": targets, "missing_in_lookup": missing_in_lookup}
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="hanja.stroke_count 채우기")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 대상만 출력")
    args = parser.parse_args()

    result = run(dry_run=args.dry_run)
    print(f"hanja 총 {result['total']}건 중 채울 대상 {len(result['targets'])}건, "
          f"lookup에 없음 {len(result['missing_in_lookup'])}건")
    if result["missing_in_lookup"]:
        print(f"  lookup에 없는 글자: {result['missing_in_lookup']}")

    if args.dry_run:
        print("--dry-run - DB에 저장하지 않음.")
    else:
        print(f"완료: {len(result['targets'])}건 채움")
    return 0


if __name__ == "__main__":
    sys.exit(main())
