# -*- coding: utf-8 -*-
"""Phase32 - L6 과학·법 외 사회 어휘 근거 기반 집필 파일럿 산출물 독립
재검증. 읽기 전용(SELECT만) - literacy.db·vocabulary_quiz DB 어디에도
쓰지 않는다. 이번 단계도, 이 테스트도 577건의 상태·레벨·공개 플래그를
전혀 바꾸지 않으며, 적재·공개·검수완료 처리를 하지 않는다.

검증 항목:
  1. 선정 20건이 phase31의 제외 목록(동형이의 4·표기 2·정의오류 1·
     소분류 5 = 12건)과 전혀 겹치지 않는지
  2. 선정 20건이 과학 10 + 법외 사회 10이고, 교과×주차 조합이 전부 서로
     달라(분산) 같은 소단원에서 2건 이상 뽑히지 않았는지
  3. 최종 판정표의 모든 행이 필수 출처 필드(자료명·발행주체·위치·확인일·
     지지근거·재사용조건)를 채우고 있는지 - 근거 미확보(HOLD)로 표시된
     행도 "근거를 못 찾았다"는 사실 자체가 기록돼 있어야 함(빈 칸으로
     얼버무리지 않았는지)
  4. DRAFT_READY(뜻풀이 초안 작성 대상)인 행에는 반드시 student_definition
     초안이 채워져 있고, HOLD인 행에는 채워져 있지 않은지(근거 없는 항목에
     초안을 쓰지 않았다는 것의 기계적 재확인)
  5. grade_fit이 전부 POLICY_MAPPING_ONLY인지(뜻 근거 확보 여부와 무관하게
     학년 근거는 별도라는 것의 재확인)
  6. expert_review_status가 전부 DRAFT_NOT_REVIEWED인지(자동검사 통과를
     사람 검수 완료로 바꾸지 않았다는 것의 재확인)
  7. literacy.db mtime 불변, (지정 시) vocabulary_quiz DB 콘텐츠/문항 수 불변

실행:
    python tests/test_phase32_source_verification.py
"""
from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
SELECTED_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase32_selected_20_20260927.csv"
VERDICT_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase32_verdicts_20260927.csv"
LITERACY_DB = REPO_ROOT / "data" / "literacy.db"

PHASE31_EXCLUDED = {
    "6805", "6864", "5991", "5956", "6960", "5900", "6817",
    "6014", "6015", "6016", "6017", "6029",
}
REQUIRED_SOURCE_FIELDS = ["material_name", "publisher", "location", "accessed_date",
                          "supporting_quote", "reuse_terms"]

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(SELECTED_CSV.exists(), f"선정 20건 CSV 존재: {SELECTED_CSV}")
    check(VERDICT_CSV.exists(), f"판정표 CSV 존재: {VERDICT_CSV}")
    if not (SELECTED_CSV.exists() and VERDICT_CSV.exists()):
        return False

    mtime_before = LITERACY_DB.stat().st_mtime

    with open(SELECTED_CSV, encoding="utf-8-sig") as f:
        selected = list(csv.DictReader(f))
    with open(VERDICT_CSV, encoding="utf-8-sig") as f:
        verdicts = list(csv.DictReader(f))

    check(len(selected) == 20, f"선정 20건(실제 {len(selected)})")
    sci_n = sum(1 for r in selected if r["pilot_group"] == "science")
    soc_n = sum(1 for r in selected if r["pilot_group"] == "social_nonlaw")
    check(sci_n == 10 and soc_n == 10, f"과학 10 + 법외 사회 10(실제 {sci_n}/{soc_n})")

    selected_ids = {r["term_id"] for r in selected}
    check(not (selected_ids & PHASE31_EXCLUDED),
          "선정 20건이 phase31 제외 목록(동형이의4/표기2/정의오류1/소분류5)과 전혀 안 겹침",
          str(selected_ids & PHASE31_EXCLUDED))

    groups = [(r["sense_category"], r["note_week"]) for r in selected]
    check(len(set(groups)) == 20, f"교과×주차 조합 20개 전부 서로 다름(분산 성공, 실제 고유 {len(set(groups))}개)")

    check(len(verdicts) == 20, f"판정표 20건(실제 {len(verdicts)})")
    for r in verdicts:
        missing = [f for f in REQUIRED_SOURCE_FIELDS if not r.get(f)]
        if missing:
            check(False, f"{r['term_id']}({r['headword']}) 필수 출처 필드 누락", str(missing))
    else:
        check(True, "판정표 20건 전부 필수 출처 필드(자료명/발행주체/위치/확인일/지지근거/재사용조건) 기록됨")

    draft_ready = [r for r in verdicts if r["final_status"] == "DRAFT_READY"]
    hold = [r for r in verdicts if r["final_status"] == "HOLD"]
    check(len(draft_ready) + len(hold) == 20, f"final_status가 DRAFT_READY/HOLD 둘 중 하나(실제 합계 {len(draft_ready)+len(hold)})")
    empty_draft_in_ready = [r["term_id"] for r in draft_ready if not r["student_definition_draft"]]
    check(not empty_draft_in_ready, "DRAFT_READY 항목은 전부 student_definition_draft가 채워짐", str(empty_draft_in_ready))
    nonempty_draft_in_hold = [r["term_id"] for r in hold if r["student_definition_draft"]]
    check(not nonempty_draft_in_hold, "HOLD 항목에는 student_definition_draft가 비어 있음(근거 없이 초안 안 씀)",
          str(nonempty_draft_in_hold))

    grade_fit_vals = {r["grade_fit"] for r in verdicts}
    check(grade_fit_vals == {"POLICY_MAPPING_ONLY"}, f"grade_fit 전부 POLICY_MAPPING_ONLY(실제 {grade_fit_vals})")

    review_vals = {r["expert_review_status"] for r in verdicts}
    check(review_vals == {"DRAFT_NOT_REVIEWED"},
          f"expert_review_status 전부 DRAFT_NOT_REVIEWED(자동검사=검수완료 아님, 실제 {review_vals})")

    mtime_after = LITERACY_DB.stat().st_mtime
    check(mtime_before == mtime_after, f"literacy.db mtime 불변(전 {mtime_before}, 후 {mtime_after})")

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
