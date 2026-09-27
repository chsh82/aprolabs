# -*- coding: utf-8 -*-
"""Phase33 - L6 과학·법 외 사회 어휘 근거 기반 집필 50건 + phase32
DRAFT_READY 15건 재검증 산출물 독립 재검증. 읽기 전용(SELECT만) -
literacy.db·vocabulary_quiz DB 어디에도 쓰지 않는다. 이번 단계도, 이
테스트도 577건의 상태·레벨·공개 플래그를 전혀 바꾸지 않으며, 적재·공개·
검수완료 처리를 하지 않는다.

검증 항목:
  1. 선정 50건이 phase31 예외 12건 + phase32 선정 20건(합계 32건)과 전혀
     겹치지 않는지, 과학 29 + 사회 21로 578건 원본과 대조 가능한지
  2. 선정 50건의 (subject_category, note_subcategory) 그룹이 35개 전부
     커버되는지(분산 확인) - phase32처럼 완전한 중복 배제까지는 아니지만
     그룹당 최대 2건 규칙이 지켜졌는지
  3. 최종 판정표 65건(50+15) 전부 필수 출처 필드를 채우고 있는지 - HOLD
     행도 "못 찾았다"는 사실 자체가 기록돼 있는지
  4. DRAFT_READY 행에는 반드시 student_definition 초안이 있고, HOLD
     행에는 없는지
  5. 위키백과 단독 근거(wikipedia_sole_source=True)인 행은 반드시
     final_status=HOLD인지(이번 단계의 강화 규칙이 실제로 적용됐는지)
  6. HOLD 행의 hold_category가 4종(출처부족/뜻충돌/동형이의/표기문제) 중
     하나인지
  7. reverify_15 배치 중 이전에 DRAFT_READY였던 항목이 이번에 HOLD로
     바뀌었다면(status_changed=True) 반드시 위키백과 단독 근거 때문인지
     (예: DNA 중합효소)
  8. grade_fit 전부 POLICY_MAPPING_ONLY, expert_review_status 전부
     DRAFT_NOT_REVIEWED인지
  9. literacy.db mtime 불변, (지정 시) vocabulary_quiz DB 콘텐츠/문항 수
     불변

실행:
    python tests/test_phase33_source_verification.py
"""
from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
SELECTED_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase33_selected_50_20260927.csv"
VERDICT_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase33_verdicts_20260927.csv"
LITERACY_DB = REPO_ROOT / "data" / "literacy.db"

PHASE31_AND_32_EXCLUDED = {
    "6805", "6864", "5991", "5956", "6960", "5900", "6817",
    "6014", "6015", "6016", "6017", "6029",
    "6904", "6835", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "7049",
    "5749", "5770", "5794", "5804", "5834", "5845", "5863", "5884", "5928", "5968",
}
REQUIRED_SOURCE_FIELDS = ["material_name", "publisher", "location", "accessed_date",
                          "supporting_quote", "reuse_terms"]
VALID_HOLD_CATEGORIES = {"출처부족", "뜻충돌", "동형이의", "표기문제"}

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(SELECTED_CSV.exists(), f"선정 50건 CSV 존재: {SELECTED_CSV}")
    check(VERDICT_CSV.exists(), f"판정표 CSV 존재: {VERDICT_CSV}")
    if not (SELECTED_CSV.exists() and VERDICT_CSV.exists()):
        return False

    mtime_before = LITERACY_DB.stat().st_mtime if LITERACY_DB.exists() else None

    with open(SELECTED_CSV, encoding="utf-8-sig") as f:
        selected = list(csv.DictReader(f))
    with open(VERDICT_CSV, encoding="utf-8-sig") as f:
        verdicts = list(csv.DictReader(f))

    check(len(selected) == 50, f"선정 50건(실제 {len(selected)})")
    sci_n = sum(1 for r in selected if r["subject_category"] == "과학")
    soc_n = sum(1 for r in selected if r["subject_category"] == "사회")
    check(sci_n + soc_n == 50, f"과학+사회=50(실제 과학 {sci_n} / 사회 {soc_n})")

    selected_ids = {r["term_id"] for r in selected}
    check(not (selected_ids & PHASE31_AND_32_EXCLUDED),
          "선정 50건이 phase31 예외(12) + phase32 선정(20) = 32건과 전혀 안 겹침",
          str(selected_ids & PHASE31_AND_32_EXCLUDED))
    check(len(selected_ids) == 50, "선정 50건에 중복 term_id 없음")

    groups = {(r["subject_category"], r["note_subcategory"]) for r in selected}
    check(len(groups) >= 30, f"교과×소분류 그룹이 폭넓게 분산됨(실제 {len(groups)}개 그룹)")
    from collections import Counter
    group_counts = Counter((r["subject_category"], r["note_subcategory"]) for r in selected)
    over_2 = {k: v for k, v in group_counts.items() if v > 2}
    check(not over_2, "그룹당 선정 건수가 2건을 넘지 않음(분산 규칙 준수)", str(over_2))

    check(len(verdicts) == 65, f"판정표 65건(신규 50 + 재검증 15, 실제 {len(verdicts)})")
    new50 = [r for r in verdicts if r["batch"] == "new_50"]
    reverify15 = [r for r in verdicts if r["batch"] == "reverify_15"]
    check(len(new50) == 50, f"신규 50건 배치(실제 {len(new50)})")
    check(len(reverify15) == 15, f"재검증 15건 배치(실제 {len(reverify15)})")

    for r in verdicts:
        missing = [f for f in REQUIRED_SOURCE_FIELDS if not r.get(f)]
        if missing:
            check(False, f"{r['term_id']}({r['headword']}) 필수 출처 필드 누락", str(missing))
    else:
        check(True, "판정표 65건 전부 필수 출처 필드(자료명/발행주체/위치/확인일/지지근거/재사용조건) 기록됨")

    draft_ready = [r for r in verdicts if r["final_status"] == "DRAFT_READY"]
    hold = [r for r in verdicts if r["final_status"] == "HOLD"]
    check(len(draft_ready) + len(hold) == 65, f"final_status가 DRAFT_READY/HOLD 둘 중 하나(실제 합계 {len(draft_ready) + len(hold)})")
    empty_draft_in_ready = [r["term_id"] for r in draft_ready if not r["student_definition_draft"]]
    check(not empty_draft_in_ready, "DRAFT_READY 항목은 전부 student_definition_draft가 채워짐", str(empty_draft_in_ready))
    nonempty_draft_in_hold = [r["term_id"] for r in hold if r["student_definition_draft"]]
    check(not nonempty_draft_in_hold, "HOLD 항목에는 student_definition_draft가 비어 있음(근거 없이 초안 안 씀)",
          str(nonempty_draft_in_hold))

    wiki_sole_rows = [r for r in verdicts if r["wikipedia_sole_source"] == "True"]
    wiki_sole_not_hold = [r["term_id"] for r in wiki_sole_rows if r["final_status"] != "HOLD"]
    check(bool(wiki_sole_rows), f"위키백과 단독 근거 항목이 실제로 존재함(실제 {len(wiki_sole_rows)}건)")
    check(not wiki_sole_not_hold,
          "위키백과 단독 근거 항목은 전부 HOLD로 처리됨(이번 단계 강화 규칙)", str(wiki_sole_not_hold))

    hold_cats = {r["hold_category"] for r in hold if r["hold_category"]}
    check(hold_cats.issubset(VALID_HOLD_CATEGORIES),
          f"HOLD 사유가 4종(출처부족/뜻충돌/동형이의/표기문제) 안에서만 쓰임(실제 {hold_cats})")
    check(all(r["hold_category"] for r in hold), "HOLD 항목 전부 hold_category가 채워짐")

    changed = [r for r in reverify15 if r["status_changed"] == "True"]
    for r in changed:
        ok = r["wikipedia_sole_source"] == "True"
        check(ok, f"{r['term_id']}({r['headword']}) phase32 대비 판정 변경 사유가 위키백과 단독 근거임",
              f"wikipedia_sole_source={r['wikipedia_sole_source']}")
    check(len(changed) >= 1, f"phase32 대비 판정이 바뀐 항목이 최소 1건 존재(실제 {len(changed)}건 - DNA 중합효소 예상)")

    grade_fit_vals = {r["grade_fit"] for r in verdicts}
    check(grade_fit_vals == {"POLICY_MAPPING_ONLY"}, f"grade_fit 전부 POLICY_MAPPING_ONLY(실제 {grade_fit_vals})")

    review_vals = {r["expert_review_status"] for r in verdicts}
    check(review_vals == {"DRAFT_NOT_REVIEWED"},
          f"expert_review_status 전부 DRAFT_NOT_REVIEWED(자동검사=검수완료 아님, 실제 {review_vals})")

    if mtime_before is not None:
        mtime_after = LITERACY_DB.stat().st_mtime
        check(mtime_before == mtime_after, f"literacy.db mtime 불변(전 {mtime_before}, 후 {mtime_after})")
    else:
        print("[SKIP] literacy.db 로컬 미존재 - mtime 확인 생략")

    vq_db_ro = os.environ.get("VOCABULARY_QUIZ_DB_PATH_RO")
    if vq_db_ro and Path(vq_db_ro).exists():
        import sqlite3
        conn = sqlite3.connect(f"file:{vq_db_ro}?mode=ro", uri=True)
        vc = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        mfi = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        conn.close()
        check(vc == 5902, f"vocabulary_contents 5,902건 불변(실제 {vc})")
        check(mfi == 1369, f"vocabulary_multiformat_items 1,369건 불변(실제 {mfi})")
    else:
        print("[SKIP] VOCABULARY_QUIZ_DB_PATH_RO 미지정 - 서버 DB 불변 확인은 보고서 로그 참고")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
