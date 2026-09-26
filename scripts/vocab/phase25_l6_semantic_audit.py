# -*- coding: utf-8 -*-
"""Phase25 항목1/2 - 읽기 전용 재실행 스크립트.

DB에는 전혀 쓰지 않는다(연구 서버 SELECT만, literacy.db도 읽기 전용). 이 스크립트가
하는 일은 두 가지 기계적 검증뿐이다 - 실제 PASS/HOLD 의미 판정과 근거는 사람이
직접 내용을 읽고 결과표(schema_reading_phase25_l6_50_semantic_audit_20260927.csv,
schema_reading_phase25_l6_v35_dryrun_20260927.csv)에 기록했다:

  1) 적재된 L6 50건: canonical_definition이 literacy.db 원천 definition과 정확히
     일치하는지(자구 변경 여부)를 재확인하고, 기존 vocabulary_contents(5,820건)
     중 표기가 1글자 차이(hamming distance 1)이거나 포함 관계(길이차 1, 부분
     문자열)인 "근접 후보"를 자동으로 뽑아 사람이 놓친 근접 유의어 쌍을 찾는
     실마리로 삼는다(완결/완료, 결여/결점, 타당성/부당성처럼 이미 기록된 쌍
     외에 개략/개괄, 총합/총량, 합리적/논리적을 이 방식으로 추가 발견했다).
  2) phase23의 잔여 V NEW_CANDIDATE 35건이 이미 적재된 50건·기존 5,820건과
     literacy_term_id·lemma 어느 쪽으로도 겹치지 않는지 재확인한다.

사용:
    python3 scripts/vocab/phase25_l6_semantic_audit.py \\
        --literacy-db data/literacy.db \\
        --research-db-copy <research db 사본 경로>
(둘 다 로컬 사본/원본을 읽기 전용으로만 연다. 서버 원본을 직접 지정해도 무방하다 -
읽기만 하므로 안전하다.)
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LOADED_50_CSV = REPO_ROOT / "data/import/schema_reading_phase24_l6_core_final_20260927.csv"
CANDIDATES_116_CSV = REPO_ROOT / "data/import/schema_reading_phase23_l6_candidates_20260926.csv"
EXCLUDE_SOURCE_VERSION = "schema_reading_literacy_l6_manual_v1"


def hamming1(a: str, b: str) -> bool:
    if len(a) != len(b):
        return False
    return sum(1 for x, y in zip(a, b) if x != y) == 1


def near(a: str, b: str) -> bool:
    if a == b:
        return False
    if hamming1(a, b):
        return True
    if abs(len(a) - len(b)) == 1:
        shorter, longer = (a, b) if len(a) < len(b) else (b, a)
        if shorter in longer:
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--literacy-db", type=Path, required=True)
    ap.add_argument("--research-db-copy", type=Path, required=True)
    args = ap.parse_args()

    with open(LOADED_50_CSV, encoding="utf-8-sig") as f:
        loaded50 = list(csv.DictReader(f))
    assert len(loaded50) == 50, f"기대와 다른 행 수: {len(loaded50)}"

    lit_conn = sqlite3.connect(f"file:{args.literacy_db}?mode=ro", uri=True)
    lit_conn.execute("PRAGMA query_only=ON")
    term_ids = [r["literacy_term_id"] for r in loaded50]
    qmarks = ",".join("?" * len(term_ids))
    src_rows = lit_conn.execute(
        f"SELECT id, definition FROM terms WHERE id IN ({qmarks})", term_ids
    ).fetchall()
    src_def_by_id = {str(i): d for i, d in src_rows}
    lit_conn.close()

    mismatches = []
    for r in loaded50:
        src_def = src_def_by_id.get(r["literacy_term_id"])
        if src_def != r["canonical_definition"]:
            mismatches.append((r["literacy_term_id"], r["lemma"], src_def, r["canonical_definition"]))
    print(f"[1a] canonical_definition == literacy.db 원문 불일치: {len(mismatches)}건")
    for m in mismatches:
        print("  ", m)

    conn = sqlite3.connect(f"file:{args.research_db_copy}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    existing = conn.execute(
        "SELECT content_id, lemma FROM vocabulary_contents WHERE source_version != ?",
        (EXCLUDE_SOURCE_VERSION,),
    ).fetchall()
    conn.close()

    print(f"\n[1b] 근접 후보(hamming-1 또는 부분문자열, 길이차<=1) - 50건 대상:")
    total_flagged = 0
    for r in loaded50:
        lemma = r["lemma"]
        cands = [(cid, el) for cid, el in existing if near(lemma, el)]
        if cands:
            total_flagged += 1
            print(f"  {lemma} ({r['content_id']}) <-> " + ", ".join(f"{el}[{cid}]" for cid, el in cands))
    print(f"  근접 후보가 있는 행: {total_flagged}/50")

    with open(CANDIDATES_116_CSV, encoding="utf-8-sig") as f:
        candidates116 = list(csv.DictReader(f))
    remaining_v = [
        r for r in candidates116
        if r["final_classification"] == "NEW_CANDIDATE" and r["v_s"] == "V"
        and r.get("proposed_first_l6_batch") != "Y"
    ]
    print(f"\n[2] 잔여 V NEW_CANDIDATE: {len(remaining_v)}건 (기대값 35)")
    loaded_term_ids = {r["literacy_term_id"] for r in loaded50}
    loaded_lemmas = {r["lemma"] for r in loaded50}
    existing_lemmas = {el for _, el in existing}
    overlap_termid = [r["literacy_term_id"] for r in remaining_v if r["literacy_term_id"] in loaded_term_ids]
    overlap_lemma = [r["headword"] for r in remaining_v
                      if r["headword"] in loaded_lemmas or r["headword"] in existing_lemmas]
    print(f"  loaded-50과 term_id 중복: {len(overlap_termid)}건 {overlap_termid}")
    print(f"  loaded-50/existing-5820과 lemma 중복: {len(overlap_lemma)}건 {overlap_lemma}")

    ok = not mismatches and len(remaining_v) == 35 and not overlap_termid and not overlap_lemma
    print(f"\n{'[PASS]' if ok else '[FAIL]'} 기계적 재검증 종합")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
