#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase33 항목2/3/4/5: 신규 선정 50건 + phase32 DRAFT_READY 15건 재검증을
병합해 최종 판정표를 만든다. 읽기 전용 - 입력 CSV/JSON만 읽는다. literacy.db·
vocabulary_quiz DB 어디에도 쓰지 않는다.

phase32_source_verification.py와의 차이(이번 단계 지시사항 반영):
  1. 판정값에 CANNOT_DETERMINE 대신 INSUFFICIENT_SOURCE 사용
  2. 위키백과 단독 근거는 이번 단계부터 자동 HOLD(출처부족) - phase32는
     위키백과를 "최후 수단"으로 허용했으나(예: DNA 중합효소=MATCH), 이번
     지시("위키백과 단독 근거는... HOLD로 두라")에 따라 더 엄격하게 적용
  3. HOLD 사유를 4종(출처부족/뜻충돌/동형이의/표기문제)으로 분류
  3b. 조사 진행 단계를 3개 상태로 분리 기록: search_result_found(검색 결과
      발견) / access_and_meaning_confirmed(접근해서 원문 인용까지 확인) /
      draft_eligible(초안 작성 가능=DRAFT_READY) - "찾았다"와 "실제로 열어서
      확인했다"와 "초안을 써도 된다"를 서로 다른 단계로 구분
  4. phase32 DRAFT_READY 15건을 이 새 규칙으로 재실행(재조사 아님 - 이미
     기록된 source_findings에 위키백과 단독 여부만 추가 판정)

독립 자료 조사 자체는 이 스크립트가 하지 않는다 - WebSearch/WebFetch는
사람(호출 세션·서브에이전트)이 직접 수행하고 SOURCE_FINDINGS류 JSON에
기록한다(가짜 출처를 스크립트가 만들어내지 않도록).

사용:
    python3 scripts/vocab/phase33_source_verification.py \\
        --selected data/import/schema_reading_phase33_selected_50_20260927.csv \\
        --sources data/import/schema_reading_phase33_source_findings_20260927.json \\
        --reverify-ids 6904,6856,6867,6756,6820,6962,6993,7027,7049,5749,5794,5834,5845,5884,5968 \\
        --reverify-selected data/import/schema_reading_phase32_selected_20_20260927.csv \\
        --reverify-sources data/import/schema_reading_phase33_reverify15_source_findings_20260927.json \\
        --drafts data/import/schema_reading_phase33_drafts_20260927.json \\
        --out data/import/schema_reading_phase33_verdicts_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

REQUIRED_SOURCE_FIELDS = [
    "source_found", "material_name", "publisher", "location", "accessed_date",
    "supporting_quote", "reuse_terms", "comparison_verdict", "verdict_reason",
    "scope_unclear", "wikipedia_sole_source", "homonym_risk", "orthography_issue",
]

VALID_VERDICTS = {"MATCH", "PARTIAL_MATCH", "CONFLICT", "INSUFFICIENT_SOURCE"}


def classify(src: dict) -> tuple[str, str]:
    """(final_status, hold_category) 반환. hold_category는 final_status=='HOLD'일 때만 의미 있음."""
    if src["comparison_verdict"] not in VALID_VERDICTS:
        return "HOLD", f"UNRECOGNIZED_VERDICT({src['comparison_verdict']})"
    if not src["source_found"]:
        return "HOLD", "출처부족"
    if src.get("wikipedia_sole_source"):
        return "HOLD", "출처부족"
    if src["comparison_verdict"] == "INSUFFICIENT_SOURCE":
        return "HOLD", "출처부족"
    if src["comparison_verdict"] == "CONFLICT":
        return "HOLD", "뜻충돌"
    if src.get("homonym_risk"):
        return "HOLD", "동형이의"
    if src.get("orthography_issue"):
        return "HOLD", "표기문제"
    if src.get("scope_unclear"):
        return "HOLD", "뜻충돌"
    if src["comparison_verdict"] in ("MATCH", "PARTIAL_MATCH"):
        return "DRAFT_READY", ""
    return "HOLD", "출처부족"


def build_rows(selected: list[dict], sources: dict, drafts: dict, batch_label: str,
               previous_status_default: str = "") -> list[dict]:
    missing = [r["term_id"] for r in selected if r["term_id"] not in sources]
    if missing:
        raise SystemExit(f"[{batch_label}] source_findings에 없는 term_id: {missing}")

    out_rows = []
    for r in selected:
        tid = r["term_id"]
        src = sources[tid]
        for field in REQUIRED_SOURCE_FIELDS:
            if field not in src:
                raise SystemExit(f"[{batch_label}] {tid}: source_findings에 필수 필드 {field!r} 누락")

        final_status, hold_category = classify(src)
        draft = drafts.get(tid, {})
        search_result_found = bool(src["source_found"])
        access_and_meaning_confirmed = search_result_found and bool(src.get("supporting_quote", "").strip())
        draft_eligible = final_status == "DRAFT_READY"
        out_rows.append({
            "term_id": tid, "headword": r["headword"], "batch": batch_label,
            "search_result_found": search_result_found,
            "access_and_meaning_confirmed": access_and_meaning_confirmed,
            "draft_eligible": draft_eligible,
            "subject_category": r["subject_category"], "sense_category": r["sense_category"],
            "note_subcategory": r["note_subcategory"], "note_week": r["note_week"],
            "ai_definition": r.get("ai_definition", ""),
            "headword_form": r.get("headword_form", ""),
            "material_name": src["material_name"], "publisher": src["publisher"],
            "location": src["location"], "accessed_date": src["accessed_date"],
            "supporting_quote": src["supporting_quote"], "reuse_terms": src["reuse_terms"],
            "comparison_verdict": src["comparison_verdict"], "verdict_reason": src["verdict_reason"],
            "scope_unclear": src.get("scope_unclear", False),
            "wikipedia_sole_source": src.get("wikipedia_sole_source", False),
            "homonym_risk": src.get("homonym_risk", False),
            "orthography_issue": src.get("orthography_issue", False),
            "final_status": final_status, "hold_category": hold_category,
            "previous_status": previous_status_default,
            "status_changed": bool(previous_status_default) and previous_status_default != final_status,
            "student_definition_draft": draft.get("student_definition", "") if final_status == "DRAFT_READY" else "",
            "example_sentence_draft": draft.get("example_sentence", "") if final_status == "DRAFT_READY" else "",
            "grade_fit": "POLICY_MAPPING_ONLY",
            "grade_fit_note": (
                "grade_level 컬럼 NULL(개별 학년 근거 없음) - L6=고2~3은 정책 매핑에만 "
                "의존(phase23/30/31/32와 동일, 뜻 근거와 완전히 독립적으로 고정)."
            ),
            "expert_review_status": "DRAFT_NOT_REVIEWED",
        })
    return out_rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--selected", type=Path, required=True)
    ap.add_argument("--sources", type=Path, required=True)
    ap.add_argument("--reverify-ids", type=str, default="")
    ap.add_argument("--reverify-selected", type=Path, default=None)
    ap.add_argument("--reverify-sources", type=Path, default=None)
    ap.add_argument("--drafts", type=Path, default=None)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with open(args.selected, encoding="utf-8-sig") as f:
        selected_50 = list(csv.DictReader(f))
    with open(args.sources, encoding="utf-8") as f:
        sources_50 = json.load(f)
    drafts = {}
    if args.drafts and args.drafts.exists():
        drafts = json.loads(args.drafts.read_text(encoding="utf-8"))

    rows = build_rows(selected_50, sources_50, drafts, "new_50")

    if args.reverify_ids:
        reverify_ids = [x.strip() for x in args.reverify_ids.split(",") if x.strip()]
        with open(args.reverify_selected, encoding="utf-8-sig") as f:
            reverify_pool = {r["term_id"]: r for r in csv.DictReader(f)}
        reverify_selected = []
        for tid in reverify_ids:
            r = reverify_pool[tid]
            reverify_selected.append({
                "term_id": tid, "headword": r["headword"],
                "subject_category": r["subject_category"], "sense_category": r["sense_category"],
                "note_subcategory": r["note_subcategory"], "note_week": r["note_week"],
                "ai_definition": r["ai_definition"], "headword_form": "",
            })
        with open(args.reverify_sources, encoding="utf-8") as f:
            sources_reverify = json.load(f)
        rows.extend(build_rows(reverify_selected, sources_reverify, drafts, "reverify_15",
                                previous_status_default="DRAFT_READY"))

    from collections import Counter
    dist = Counter((o["batch"], o["final_status"]) for o in rows)
    verdict_dist = Counter((o["batch"], o["comparison_verdict"]) for o in rows)
    hold_cat_dist = Counter(o["hold_category"] for o in rows if o["final_status"] == "HOLD")
    wiki_sole = [o["term_id"] for o in rows if o["wikipedia_sole_source"]]
    changed = [(o["term_id"], o["headword"], o["previous_status"], o["final_status"]) for o in rows if o["status_changed"]]
    print(f"최종 상태 분포(배치별): {dict(dist)}")
    print(f"비교 판정 분포(배치별): {dict(verdict_dist)}")
    print(f"HOLD 사유 분포: {dict(hold_cat_dist)}")
    print(f"위키백과 단독 근거로 강제 HOLD된 항목: {wiki_sole}")
    print(f"phase32 대비 판정이 바뀐 항목: {changed}")
    n_draft_ready = sum(1 for o in rows if o["final_status"] == "DRAFT_READY")
    print(f"뜻풀이·예문 초안 작성 대상: {n_draft_ready}건")
    missing_draft = [o["term_id"] for o in rows if o["final_status"] == "DRAFT_READY" and not o["student_definition_draft"]]
    if missing_draft:
        print(f"[경고] DRAFT_READY인데 초안이 비어 있는 항목: {missing_draft}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
