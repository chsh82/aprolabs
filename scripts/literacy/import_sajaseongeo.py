"""사자성어(raw/sajaseongeo/의 PDF 2개, sajaseongeo_parser.merge_sources() 결과)
적재.

병합 규칙(sajaseongeo_parser.py 참고): 정의/한자/출제정보는 PDF2 우선,
글자별 훈음 분해는 PDF1에서만. 225건 중 156건(PDF1에 있는 것)에만 훈음이
붙고, 69건(PDF2에만 있는 것)은 훈음 없이 한자 원어만 저장된다.

정의가 아예 없는 항목(원본 PDF 자체의 누락, 6건 확인됨 - 예: "금상첨화")은
definition=NULL, review_status='보류'로 저장한다(schemareading-schema의
빈 정의 처리와 같은 원칙 - 채우려 하지 않고 보류로 남긴다).

level은 비워둔다(NULL) - auto_review_level.py로 별도 자동 배정한다.

실행:
    python import_sajaseongeo.py --dry-run   # 저장 없이 결과만 출력
    python import_sajaseongeo.py             # 실제 적재
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sajaseongeo_parser import MergedEntry, merge_sources  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

SOURCE = "sajaseongeo-pdf"
LICENSE = "PDF 정리자료(평가원+EBS/수능 언어영역 기출)"


def _get_or_create_hanja(conn: sqlite3.Connection, character: str, meaning_reading: str) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO hanja (character, meaning_reading) VALUES (?, ?)",
        (character, meaning_reading),
    )
    row = conn.execute("SELECT id FROM hanja WHERE character = ?", (character,)).fetchone()
    return row[0]


def _insert_example_if_new(conn: sqlite3.Connection, term_id: int, source: str, sentence: str) -> None:
    exists = conn.execute(
        "SELECT 1 FROM examples WHERE term_id = ? AND source = ? AND sentence = ?",
        (term_id, source, sentence),
    ).fetchone()
    if not exists:
        conn.execute(
            "INSERT INTO examples (term_id, sentence, source) VALUES (?, ?, ?)",
            (term_id, sentence, source),
        )


def run(dry_run: bool) -> dict:
    merged, stats = merge_sources()

    if dry_run:
        return {"merged": merged, "stats": stats}

    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA foreign_keys=ON")

    cur = conn.execute(
        "INSERT INTO collection_runs (source, started_at, status) VALUES (?, ?, 'running')",
        (SOURCE, now),
    )
    run_id = cur.lastrowid
    conn.commit()

    inserted = 0
    no_definition = 0
    hanja_linked = 0
    try:
        for e in merged:
            definition = e.definitions[0] if e.definitions else None
            review_status = "검수전" if definition else "보류"
            if not definition:
                no_definition += 1

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO terms
                    (category, headword, origin, definition, level, grade_source,
                     source, license, external_id, collected_at, review_status, note)
                VALUES ('사자성어', ?, ?, ?, NULL, 'auto', ?, ?, ?, ?, ?, ?)
                """,
                (e.headword, e.hanja, definition, SOURCE, LICENSE, e.headword, now,
                 review_status, e.note or None),
            )
            if cur.rowcount == 0:
                continue  # 이미 있음(재실행) - 건너뜀
            inserted += 1

            term_id = conn.execute(
                "SELECT id FROM terms WHERE source = ? AND external_id = ?",
                (SOURCE, e.headword),
            ).fetchone()[0]

            for i, d in enumerate(e.definitions[1:], start=2):
                _insert_example_if_new(conn, term_id, f"sajaseongeo-sense-{i}", d)
            for r in e.related:
                _insert_example_if_new(conn, term_id, "sajaseongeo-related", r)

            if e.chars:
                hanja_linked += 1
                for position, c in enumerate(e.chars):
                    hanja_id = _get_or_create_hanja(conn, c.character, c.meaning_reading)
                    conn.execute(
                        "INSERT OR IGNORE INTO term_hanja (term_id, hanja_id, position) VALUES (?, ?, ?)",
                        (term_id, hanja_id, position),
                    )

        conn.commit()
        conn.execute(
            """
            UPDATE collection_runs
            SET finished_at = ?, fetched_count = ?, inserted_count = ?, status = 'success'
            WHERE id = ?
            """,
            (datetime.now().isoformat(sep=" ", timespec="seconds"), len(merged), inserted, run_id),
        )
        conn.commit()
    except Exception as ex:
        conn.execute(
            "UPDATE collection_runs SET finished_at = ?, status = 'failed', error = ? WHERE id = ?",
            (datetime.now().isoformat(sep=" ", timespec="seconds"), str(ex), run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()

    return {
        "stats": stats, "inserted": inserted, "no_definition": no_definition,
        "hanja_linked": hanja_linked,
    }


def print_dry_run_report(result: dict) -> None:
    merged: list[MergedEntry] = result["merged"]
    stats = result["stats"]

    print("=== 1. 소스별 건수 ===")
    print(f"  PDF1 원본 엔트리: {stats['pdf1_raw']}건 (고유 표제어 {stats['pdf1_unique']}건)")
    print(f"  PDF2 고유 표제어: {stats['pdf2_unique']}건")
    print(f"  교집합: {stats['both']}건 / PDF1만: {stats['pdf1_only']}건 / PDF2만: {stats['pdf2_only']}건")
    print(f"  최종 병합 고유 사자성어: {stats['merged_total']}건")

    no_chars = [e for e in merged if not e.chars]
    no_def = [e for e in merged if not e.definitions]
    print(f"\n=== 2. 글자별 훈음 없음(PDF2만 있는 것) {len(no_chars)}건 ===")
    print(f"\n=== 3. 정의 없음(원본 PDF 자체 누락) {len(no_def)}건 ===")
    for e in no_def:
        print(f"  {e.headword}({e.hanja})")

    print(f"\n=== 4. 샘플 10건 ===")
    for e in merged[:10]:
        breakdown = ", ".join(f"{c.character}({c.meaning_reading})" for c in e.chars) or "훈음 없음"
        print(f"  {e.headword}({e.hanja}) [{breakdown}]")
        for d in e.definitions:
            print(f"    정의: {d}")
        print(f"    note: {e.note}")


def main() -> int:
    parser = argparse.ArgumentParser(description="사자성어 PDF 2개 병합 적재")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 결과만 출력")
    args = parser.parse_args()

    result = run(dry_run=args.dry_run)

    if args.dry_run:
        print_dry_run_report(result)
        print("\n--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
        return 0

    stats = result["stats"]
    print(f"사자성어 신규 적재: {result['inserted']}건 (병합 대상 {stats['merged_total']}건)")
    print(f"정의 없음(보류 처리): {result['no_definition']}건")
    print(f"글자별 훈음 연결됨: {result['hanja_linked']}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())
