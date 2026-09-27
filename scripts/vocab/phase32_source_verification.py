#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase32 항목2/3/4: 선정된 20건에 대해 독립 자료 대조 결과를 병합하고,
근거가 확보된 항목에만 학생용 뜻풀이·예문 초안을 작성해 최종 판정표를
만든다. 읽기 전용 - 입력 CSV/JSON만 읽는다. literacy.db·vocabulary_quiz DB
어디에도 쓰지 않는다.

독립 자료 대조 자체(자료명·발행주체·위치·확인일·지지근거·재사용조건 확보)는
이 스크립트가 자동으로 하지 않는다 - 실제 웹 조사는 사람(호출 세션)이
WebSearch/WebFetch로 직접 수행하고, 그 결과를 SOURCE_FINDINGS 딕셔너리에
기록한다(가짜 출처를 스크립트가 만들어내지 않도록 자료 자체는 항상 사람이
채워 넣게 구조화). 이 스크립트는 그 결과를 병합해:
  1. 근거 미확보(source_found=False) 항목은 자동으로 HOLD 처리(뜻풀이·
     예문을 작성하지 않음)
  2. 판정이 CONFLICT이거나 원천 뜻의 범위가 불명확(scope_unclear=True)한
     항목도 HOLD 처리
  3. MATCH/PARTIAL_MATCH이고 범위가 명확한 항목만 학생용 뜻풀이·예문
     초안을 작성(독립 자료의 표현을 우선 반영, AI 정의는 참고만)
  4. L6=고2~3 학년 적합성은 뜻 근거와 완전히 별도 컬럼(grade_fit)으로
     처리 - 개별 학년 근거가 없으므로 전부 POLICY_MAPPING_ONLY 고정
  5. review_status는 이 단계가 전혀 건드리지 않는다(사람 검수 완료로
     바꾸지 않음 - 애초에 literacy.db에 쓰지 않으므로 구조적으로 불가능)

사용:
    python3 scripts/vocab/phase32_source_verification.py \\
        --selected data/import/schema_reading_phase32_selected_20_20260927.csv \\
        --sources data/import/schema_reading_phase32_source_findings_20260927.json \\
        --out data/import/schema_reading_phase32_verdicts_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

REQUIRED_SOURCE_FIELDS = [
    "source_found", "material_name", "publisher", "location", "accessed_date",
    "supporting_quote", "reuse_terms", "comparison_verdict", "verdict_reason",
    "scope_unclear",
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--selected", type=Path, required=True)
    ap.add_argument("--sources", type=Path, required=True)
    ap.add_argument("--drafts", type=Path, default=None,
                     help="term_id -> {student_definition, example_sentence, example_target_form} JSON(선택)")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with open(args.selected, encoding="utf-8-sig") as f:
        selected = list(csv.DictReader(f))
    with open(args.sources, encoding="utf-8") as f:
        sources = json.load(f)
    drafts = {}
    if args.drafts and args.drafts.exists():
        drafts = json.loads(args.drafts.read_text(encoding="utf-8"))

    missing = [r["term_id"] for r in selected if r["term_id"] not in sources]
    if missing:
        raise SystemExit(f"source_findings에 없는 term_id 존재(20건 전부 기록 필요): {missing}")

    out_rows = []
    for r in selected:
        tid = r["term_id"]
        src = sources[tid]
        for field in REQUIRED_SOURCE_FIELDS:
            if field not in src:
                raise SystemExit(f"{tid}: source_findings에 필수 필드 {field!r} 누락")

        if not src["source_found"]:
            final_status = "HOLD"
            hold_reason = "NO_RELIABLE_SOURCE_FOUND - 근거 미확보"
        elif src["comparison_verdict"] == "CONFLICT":
            final_status = "HOLD"
            hold_reason = f"CONFLICT - {src['verdict_reason']}"
        elif src.get("scope_unclear"):
            final_status = "HOLD"
            hold_reason = f"SCOPE_UNCLEAR - {src['verdict_reason']}"
        elif src["comparison_verdict"] in ("MATCH", "PARTIAL_MATCH"):
            final_status = "DRAFT_READY"
            hold_reason = ""
        else:
            final_status = "HOLD"
            hold_reason = f"UNRECOGNIZED_VERDICT({src['comparison_verdict']})"

        draft = drafts.get(tid, {})
        out_rows.append({
            "term_id": tid, "headword": r["headword"], "pilot_group": r["pilot_group"],
            "subject_category": r["subject_category"], "sense_category": r["sense_category"],
            "note_subcategory": r["note_subcategory"], "note_week": r["note_week"],
            "ai_definition": r["ai_definition"],
            "material_name": src["material_name"], "publisher": src["publisher"],
            "location": src["location"], "accessed_date": src["accessed_date"],
            "supporting_quote": src["supporting_quote"], "reuse_terms": src["reuse_terms"],
            "comparison_verdict": src["comparison_verdict"], "verdict_reason": src["verdict_reason"],
            "scope_unclear": src.get("scope_unclear", False),
            "final_status": final_status, "hold_reason": hold_reason,
            "student_definition_draft": draft.get("student_definition", "") if final_status == "DRAFT_READY" else "",
            "example_sentence_draft": draft.get("example_sentence", "") if final_status == "DRAFT_READY" else "",
            "grade_fit": "POLICY_MAPPING_ONLY",
            "grade_fit_note": (
                "grade_level 컬럼 NULL(개별 학년 근거 없음) - L6=고2~3은 정책 매핑에만 "
                "의존. 뜻 근거 확보 여부와 무관하게 항상 이 값(별도 판정)."
            ),
            "expert_review_status": "DRAFT_NOT_REVIEWED",
        })

    from collections import Counter
    dist = Counter(o["final_status"] for o in out_rows)
    verdict_dist = Counter(o["comparison_verdict"] for o in out_rows)
    print(f"최종 상태 분포: {dict(dist)}")
    print(f"비교 판정 분포: {dict(verdict_dist)}")
    n_source_found = sum(1 for o in out_rows if sources[o["term_id"]]["source_found"])
    print(f"근거 확보 건수: {n_source_found}/{len(out_rows)}")
    n_draft_ready = dist.get("DRAFT_READY", 0)
    print(f"뜻풀이·예문 초안 작성 대상: {n_draft_ready}건")
    if n_draft_ready:
        missing_draft = [o["term_id"] for o in out_rows if o["final_status"] == "DRAFT_READY" and not o["student_definition_draft"]]
        if missing_draft:
            print(f"[경고] DRAFT_READY인데 초안이 비어 있는 항목: {missing_draft} (--drafts로 채워야 함)")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
