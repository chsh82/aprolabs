#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase31: L6 원천·표기·동형이의 예외 정리 - 읽기 전용(SELECT만). literacy.db도
vocabulary_quiz DB(제공 시)도 전혀 쓰지 않는다. 577건의 상태·레벨·공개
플래그는 이 스크립트가 손대는 대상이 아니다(조회만).

항목1: 사회 L6 원문(정의 보유 34행)과 AI 미태그 32건을 external_id 기준으로
       행별 대조해 차이 2건의 정확한 행선을 밝힌다.
항목2: 동형이의 위험 4건을 원천 표제어/정의/분야와 대조하고, vocabulary_quiz
       DB(연구 서버 사본, --vq-db로 지정 시)에 이미 로드돼 있는지까지 확인한다.
항목3: 표기 의심 3건(별의 진화/르 샤를리에/핌비)의 원본 값·현재 DB 값을
       분리해서 기록한다(수정 제안은 별도 dry-run 표로만 남기고 적용하지 않음).
항목4: 소분류 재태깅 후보 5건의 원본 XLSX 대분류/중분류/소분류/주차 원값을
       그대로 대조해 변경 전/후(제안) 표를 만든다.
항목5: 과학·사회(법외) L6에 필요한 "정의 근거" 요건 충족 범위를 수치로 낸다.

사용:
    python3 scripts/vocab/phase31_l6_reconciliation.py \\
        --literacy-db data/literacy.db \\
        --out-dir data/import \\
        [--vq-db <vocabulary_quiz_research.db 사본, 선택>]
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))
from schemareading_parser import iter_schema_entries  # noqa: E402

HOMONYM_PAIRS = [
    # (L6 AI태그 term_id, 다른 literacy.db term_id)
    ("6805", "5320"),  # 시차
    ("6864", "4911"),  # 분화
    ("5991", "4130"),  # 교환
    ("5956", "3467"),  # 공약
]
RECAT_CANDIDATES = ["6014", "6015", "6016", "6017", "6029"]
SPELLING_CANDIDATES = {
    "6817": ("별의 진화", "가시구름", "가스구름"),
    "6960": ("르 샤를리에 원리", "르 샤를리에", "르샤틀리에"),
    "5900": ("핌비", "핌비", "핌피"),
}


def load_terms_by_id(conn: sqlite3.Connection, ids: list[str]) -> dict[str, dict]:
    qmarks = ",".join("?" for _ in ids)
    cols = ["id", "headword", "definition", "pos", "sense_category", "subject_category",
            "grade_level", "source", "external_id", "note", "level"]
    rows = conn.execute(f"SELECT {','.join(cols)} FROM terms WHERE id IN ({qmarks})", ids).fetchall()
    return {str(r[0]): dict(zip(cols, r)) for r in rows}


def item1_reconcile_34_vs_32(conn: sqlite3.Connection) -> list[dict]:
    with_def_l6_social = [e for e in iter_schema_entries() if e.sheet_key == "social" and e.level == 6 and e.definition]
    rows_out = []
    for e in with_def_l6_social:
        eid = f"social-{e.row_no}"
        own = conn.execute(
            "SELECT id, level, external_id, note FROM terms WHERE external_id=? AND source='schemareading-schema'",
            (eid,),
        ).fetchone()
        if own:
            status = "OWN_L6_TERM"
            resolved_id, resolved_level, resolved_note = own[0], own[1], own[3]
        else:
            # 나선형 반복으로 낮은 레벨에 흡수됐는지 확인(같은 시트, 같은 headword, note에 "나선형 반복: L6")
            absorbed = conn.execute(
                "SELECT id, level, external_id, note FROM terms "
                "WHERE headword=? AND source='schemareading-schema' AND note LIKE '%나선형 반복%'",
                (e.headword,),
            ).fetchall()
            if absorbed:
                status = "ABSORBED_INTO_LOWER_LEVEL"
                resolved_id, resolved_level, resolved_note = absorbed[0][0], absorbed[0][1], absorbed[0][3]
            else:
                status = "UNRESOLVED"
                resolved_id = resolved_level = resolved_note = None
        rows_out.append({
            "xlsx_row_no": e.row_no, "xlsx_headword": e.headword, "xlsx_external_id_L6": eid,
            "status": status, "resolved_term_id": resolved_id, "resolved_level": resolved_level,
            "resolved_note": resolved_note,
        })
    return rows_out


def item2_homonym_check(conn: sqlite3.Connection, vq_conn: sqlite3.Connection | None) -> list[dict]:
    all_ids = [a for pair in HOMONYM_PAIRS for a in pair]
    terms = load_terms_by_id(conn, all_ids)
    out = []
    for l6_id, other_id in HOMONYM_PAIRS:
        l6 = terms[l6_id]
        other = terms[other_id]
        vq_status = "NOT_CHECKED(no --vq-db given)"
        if vq_conn is not None:
            rows = vq_conn.execute(
                "SELECT content_id, lemma, canonical_definition, source_version FROM vocabulary_contents WHERE lemma=?",
                (l6["headword"],),
            ).fetchall()
            vq_status = "NOT_LOADED" if not rows else f"LOADED:{rows}"
        out.append({
            "headword": l6["headword"],
            "l6_term_id": l6_id, "l6_definition": l6["definition"], "l6_subject": l6["subject_category"],
            "l6_sense": l6["sense_category"], "l6_level": l6["level"],
            "other_term_id": other_id, "other_definition": other["definition"],
            "other_subject": other["subject_category"], "other_source": other["source"],
            "other_level": other["level"],
            "vocabulary_quiz_status": vq_status,
        })
    return out


def item3_spelling_dryrun(conn: sqlite3.Connection) -> list[dict]:
    terms = load_terms_by_id(conn, list(SPELLING_CANDIDATES.keys()))
    out = []
    for tid, (headword, current_form, proposed_form) in SPELLING_CANDIDATES.items():
        t = terms[tid]
        field = "definition" if tid == "6817" else "headword"
        out.append({
            "term_id": tid, "headword": t["headword"], "field": field,
            "original_xlsx_value_note": "원본 XLSX에 정의 없음(구조적, 대조 불가)" if field == "definition"
                                         else f"원본 XLSX 표제어 = 현재 DB와 동일({t['headword']!r}) - AI가 표제어를 지어낸 것이 아님",
            "current_db_value": t["definition"] if field == "definition" else t["headword"],
            "proposed_value": proposed_form,
            "proposed_basis": (
                "일반 지구과학 지식 - '가시구름'은 표준 용어가 아니며 '가스구름'(성운)의 "
                "오기로 추정(원문 정의가 없어 100% 확정 불가, 사람 확인 필요)"
                if tid == "6817" else
                "표준 교과서 표기 대조(화학) - '르샤틀리에'가 통용 표기"
                if tid == "6960" else
                "표준 교과서 표기 대조(사회) - '핌피'(PIMFY)가 통용 표기"
            ),
            "applied": False,
        })
    return out


def item4_recategorize_dryrun(conn: sqlite3.Connection) -> list[dict]:
    eid_to_entry = {f"{e.sheet_key}-{e.row_no}": e for e in iter_schema_entries()}
    terms = load_terms_by_id(conn, RECAT_CANDIDATES)
    out = []
    proposal = {
        "6014": "민법(물권)", "6015": "민법(물권)", "6016": "민법(물권)", "6017": "민법(물권)",
        "6029": "민형사공통절차",
    }
    for tid in RECAT_CANDIDATES:
        t = terms[tid]
        e = eid_to_entry.get(t["external_id"])
        out.append({
            "term_id": tid, "headword": t["headword"],
            "xlsx_subject_category(대분류,원본)": e.subject_category if e else None,
            "xlsx_sub_category(소분류,원본)": e.sub_category if e else None,
            "xlsx_week(주차,원본)": e.week if e else None,
            "current_db_note_subcategory": t["note"],
            "proposed_subcategory": proposal[tid],
            "basis": (
                "원본 XLSX 소분류 컬럼 자체가 '상법'/'형법'으로 되어 있음(수입 과정의 "
                "오류가 아니라 원 교육과정 설계자의 분류) - 정의 내용은 물권법/민형사공통 "
                "절차 개념이라 재태깅 후보로 제안하나, 원 저작자 확인 없이 이번 단계에서 "
                "적용하지 않음"
            ),
            "applied": False,
        })
    return out


def item5_coverage_requirements(conn: sqlite3.Connection) -> dict:
    l6_entries = [e for e in iter_schema_entries() if e.level == 6]
    by_subject: dict[str, dict] = {}
    for e in l6_entries:
        key = e.subject_category or "(무분류)"
        d = by_subject.setdefault(key, {"total": 0, "with_def": 0, "with_subject_meta": 0})
        d["total"] += 1
        if e.definition:
            d["with_def"] += 1
        if e.subject_category and e.sense_category and e.week:
            d["with_subject_meta"] += 1

    ai_tagged = conn.execute(
        "SELECT COUNT(*) FROM terms WHERE level=6 AND source='schemareading-schema' "
        "AND note LIKE '%[AI 자동 생성 뜻풀이]%'"
    ).fetchone()[0]
    grade_evidence = conn.execute(
        "SELECT COUNT(*) FROM terms WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema') "
        "AND grade_level IS NOT NULL"
    ).fetchone()[0]
    total_vs = conn.execute(
        "SELECT COUNT(*) FROM terms WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema')"
    ).fetchone()[0]

    return {
        "by_subject_xlsx_coverage": by_subject,
        "ai_tagged_l6_schema_count": ai_tagged,
        "individual_grade_evidence_count": grade_evidence,
        "total_l6_vs_count": total_vs,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--literacy-db", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--vq-db", type=Path, default=None)
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.literacy_db}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    vq_conn = None
    if args.vq_db:
        vq_conn = sqlite3.connect(f"file:{args.vq_db}?mode=ro", uri=True)
        vq_conn.execute("PRAGMA query_only=ON")

    r1 = item1_reconcile_34_vs_32(conn)
    print(f"\n[항목1] 사회 L6 원문 정의보유 34행 재대조: {len(r1)}건")
    from collections import Counter
    print("  status 분포:", dict(Counter(r["status"] for r in r1)))
    assert len(r1) == 34
    assert Counter(r["status"] for r in r1)["OWN_L6_TERM"] == 32
    assert Counter(r["status"] for r in r1)["ABSORBED_INTO_LOWER_LEVEL"] == 2
    assert Counter(r["status"] for r in r1).get("UNRESOLVED", 0) == 0

    r2 = item2_homonym_check(conn, vq_conn)
    print(f"\n[항목2] 동형이의 4건 대조 완료")
    for r in r2:
        print(f"  {r['headword']}: vq_status={r['vocabulary_quiz_status']}")

    r3 = item3_spelling_dryrun(conn)
    print(f"\n[항목3] 표기 의심 {len(r3)}건 dry-run 제안 작성(적용 없음)")

    r4 = item4_recategorize_dryrun(conn)
    print(f"\n[항목4] 소분류 재태깅 후보 {len(r4)}건 dry-run 제안 작성(적용 없음)")

    r5 = item5_coverage_requirements(conn)
    print(f"\n[항목5] 정의 근거 커버리지:")
    for subj, d in r5["by_subject_xlsx_coverage"].items():
        pct = d["with_def"] / d["total"] * 100 if d["total"] else 0
        print(f"  {subj}: 전체 {d['total']}, 정의보유 {d['with_def']}({pct:.1f}%), 분야메타 완비 {d['with_subject_meta']}")
    print(f"  개별 학년 근거 보유: {r5['individual_grade_evidence_count']}/{r5['total_l6_vs_count']}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    with open(args.out_dir / "schema_reading_phase31_item1_34vs32_20260927.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(r1[0].keys()))
        w.writeheader(); w.writerows(r1)
    with open(args.out_dir / "schema_reading_phase31_item2_homonym_20260927.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(r2[0].keys()))
        w.writeheader(); w.writerows(r2)
    with open(args.out_dir / "schema_reading_phase31_item3_spelling_dryrun_20260927.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(r3[0].keys()))
        w.writeheader(); w.writerows(r3)
    with open(args.out_dir / "schema_reading_phase31_item4_recategorize_dryrun_20260927.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(r4[0].keys()))
        w.writeheader(); w.writerows(r4)
    with open(args.out_dir / "schema_reading_phase31_item5_coverage_20260927.json", "w", encoding="utf-8") as f:
        json.dump(r5, f, ensure_ascii=False, indent=1)

    print(f"\n저장 완료: {args.out_dir}")
    conn.close()
    if vq_conn:
        vq_conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
