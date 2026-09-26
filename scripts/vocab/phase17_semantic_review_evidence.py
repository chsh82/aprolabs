"""phase17 의미 재검토용 증거 수집 스크립트 (읽기 전용, 재실행 가능).

**주의**: 이 스크립트는 SEMANTIC_PASS/HOLD 판정을 자동으로 내리지 않는다.
그 판정은 사람(에이전트)이 아래 증거를 직접 읽고 국어 의미를 판단한
결과이며, `data/import/schema_reading_phase17_quiz_pilot_semantic_review_*.csv`의
`semantic_verdict`/`semantic_reason` 컬럼에 그 판단과 근거가 그대로 남아있다.
이 스크립트는 그 판단에 필요한 "재료"(literacy.db 원문 정의, 동형이의
표제어 존재 여부)만 기계적으로 모아서 사람이 재검토할 때 같은 증거를 다시
볼 수 있게 한다 - 판단 자체를 재현하지 않는다(재현 불가능한 영역).

사용법:
    python scripts/vocab/phase17_semantic_review_evidence.py \\
        --literacy-db data/literacy.db \\
        --quiz-csv data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path


def fetch_term(con: sqlite3.Connection, term_id: str) -> dict | None:
    row = con.execute(
        "SELECT id, headword, pos, definition, source FROM terms WHERE id=?",
        (term_id,),
    ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "headword": row[1], "pos": row[2], "definition": row[3], "source": row[4]}


def fetch_homonym_rows(con: sqlite3.Connection, headword: str, exclude_id: str) -> list[dict]:
    rows = con.execute(
        "SELECT id, headword, pos, definition, source FROM terms WHERE headword=? AND id != ?",
        (headword, exclude_id),
    ).fetchall()
    return [{"id": r[0], "headword": r[1], "pos": r[2], "definition": r[3], "source": r[4]} for r in rows]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--literacy-db", type=Path, required=True)
    ap.add_argument("--quiz-csv", type=Path, required=True)
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.literacy_db}?mode=ro", uri=True)
    con.execute("PRAGMA query_only=ON")

    with open(args.quiz_csv, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    seen_terms: dict[str, dict] = {}
    for row in rows:
        term_id = row["literacy_term_id"]
        if term_id in seen_terms:
            continue
        term = fetch_term(con, term_id)
        if term is None:
            seen_terms[term_id] = {"error": f"term_id={term_id} not found in literacy.db"}
            continue
        homonyms = fetch_homonym_rows(con, term["headword"], term_id)
        seen_terms[term_id] = {
            "literacy_term_id": term_id,
            "db_headword": term["headword"],
            "db_pos": term["pos"],
            "db_definition": term["definition"],
            "db_source": term["source"],
            "same_headword_other_rows": homonyms,
        }

    for term_id, evidence in seen_terms.items():
        print(json.dumps(evidence, ensure_ascii=False))

    print(f"\n총 {len(seen_terms)}개 고유 literacy_term_id에 대한 증거 수집 완료(문항 {len(rows)}건 기준).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
