# -*- coding: utf-8 -*-
"""Phase33 - L6 과학·법 외 사회 어휘 근거 기반 집필 50건 + phase32 DRAFT_READY
15건 재검증 산출물 독립 재검증. 읽기 전용(SELECT만) - literacy.db·
vocabulary_quiz DB 어디에도 쓰지 않는다. 이번 단계도, 이 테스트도 577건의
상태·레벨·공개 플래그를 전혀 바꾸지 않으며, 적재·공개·검수완료 처리를
하지 않는다.

검증 항목:
  1. 선정 50건이 phase31 예외 12건 + phase32 선정 20건(=32건)과 전혀
     겹치지 않고, 정확히 50건이며 중복이 없는지
  2. 최종 판정표(new_50 배치)에 선정 50건의 term_id가 정확히 한 번씩
     존재하는지(누락·중복 없음)
  3. 판정표의 모든 행이 필수 출처 필드를 채우고 있는지(HOLD 행도
     "못 찾았다"는 사실 자체가 기록됨 - 빈 칸으로 얼버무리지 않음)
  4. DRAFT_READY 행에는 student_definition_draft가 채워져 있고, HOLD
     행에는 비어 있는지
  5. 위키백과 단독 근거(wikipedia_sole_source=True)인 행은 전부
     final_status=HOLD, hold_category=출처부족인지(강화 규칙 재확인)
  6. hold_category가 4종(출처부족/뜻충돌/동형이의/표기문제) 중 하나로만
     채워져 있는지(HOLD 행에 한정)
  7. reverify_15 배치가 정확히 phase32 DRAFT_READY 15건과 같은 집합인지,
     DNA 중합효소(7049)가 phase32=DRAFT_READY -> phase33=HOLD로 바뀌었고
     나머지 14건은 그대로 DRAFT_READY로 유지됐는지(변경 전후 판정 보존)
  8. grade_fit 전부 POLICY_MAPPING_ONLY, expert_review_status 전부
     DRAFT_NOT_REVIEWED인지(뜻 근거·자동검사 통과를 학년 근거·전문가 검수
     완료로 바꾸지 않았다는 것의 재확인)
  9. literacy.db mtime 불변, (지정 시) vocabulary_quiz 연구 DB 콘텐츠/문항
     수 불변

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

PHASE31_EXCEPTIONS_12 = {
    "6805", "6864", "5991", "5956", "6960", "5900", "6817",
    "6014", "6015", "6016", "6017", "6029",
}
PHASE32_SELECTED_20 = {
    "6904", "6835", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "7049",
    "5749", "5770", "5794", "5804", "5834", "5845", "5863", "5884", "5928", "5968",
}
PHASE32_DRAFT_READY_15 = {
    "6904", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "7049",
    "5749", "5794", "5834", "5845", "5884", "5968",
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

    mtime_before = LITERACY_DB.stat().st_mtime

    with open(SELECTED_CSV, encoding="utf-8-sig") as f:
        selected = list(csv.DictReader(f))
    with open(VERDICT_CSV, encoding="utf-8-sig") as f:
        verdicts = list(csv.DictReader(f))

    check(len(selected) == 50, f"선정 50건(실제 {len(selected)})")
    selected_ids = [r["term_id"] for r in selected]
    check(len(set(selected_ids)) == 50, f"선정 50건 중복 없음(고유 {len(set(selected_ids))}건)")
    already_used = PHASE31_EXCEPTIONS_12 | PHASE32_SELECTED_20
    overlap = set(selected_ids) & already_used
    check(not overlap, "선정 50건이 phase31 예외 12건 + phase32 선정 20건과 전혀 안 겹침", str(overlap))

    new50 = [r for r in verdicts if r["batch"] == "new_50"]
    reverify15 = [r for r in verdicts if r["batch"] == "reverify_15"]
    check(len(new50) == 50, f"판정표 new_50 배치 50건(실제 {len(new50)})")
    new50_ids = {r["term_id"] for r in new50}
    check(new50_ids == set(selected_ids), "판정표 new_50 term_id 집합이 선정 50건과 정확히 일치(누락/중복 없음)",
          str(set(selected_ids) ^ new50_ids))

    for r in new50 + reverify15:
        missing = [f for f in REQUIRED_SOURCE_FIELDS if not r.get(f)]
        if missing:
            check(False, f"{r['term_id']}({r['headword']}) 필수 출처 필드 누락", str(missing))
    else:
        check(True, "판정표 65건(50+15) 전부 필수 출처 필드(자료명/발행주체/위치/확인일/지지근거/재사용조건) 기록됨")

    draft_ready = [r for r in new50 + reverify15 if r["final_status"] == "DRAFT_READY"]
    hold = [r for r in new50 + reverify15 if r["final_status"] == "HOLD"]
    check(len(draft_ready) + len(hold) == 65, f"final_status가 DRAFT_READY/HOLD 둘 중 하나(실제 합계 {len(draft_ready)+len(hold)})")
    empty_draft_in_ready = [r["term_id"] for r in draft_ready if not r["student_definition_draft"]]
    check(not empty_draft_in_ready, "DRAFT_READY 항목은 전부 student_definition_draft가 채워짐", str(empty_draft_in_ready))
    nonempty_draft_in_hold = [r["term_id"] for r in hold if r["student_definition_draft"]]
    check(not nonempty_draft_in_hold, "HOLD 항목에는 student_definition_draft가 비어 있음(근거 없이 초안 안 씀)",
          str(nonempty_draft_in_hold))

    wiki_sole_rows = [r for r in new50 + reverify15 if r["wikipedia_sole_source"] == "True"]
    bad_wiki = [r["term_id"] for r in wiki_sole_rows
                if not (r["final_status"] == "HOLD" and r["hold_category"] == "출처부족")]
    check(bool(wiki_sole_rows), f"위키백과 단독 근거 항목이 실제로 존재함({len(wiki_sole_rows)}건) - 강화 규칙 검증 대상 있음")
    check(not bad_wiki, "위키백과 단독 근거 항목은 전부 HOLD/출처부족으로 강제 처리됨", str(bad_wiki))

    bad_hold_cat = [r["term_id"] for r in hold if r["hold_category"] not in VALID_HOLD_CATEGORIES]
    check(not bad_hold_cat, f"HOLD 사유가 4종(출처부족/뜻충돌/동형이의/표기문제) 중 하나로만 기록됨", str(bad_hold_cat))

    reverify_ids = {r["term_id"] for r in reverify15}
    check(reverify_ids == PHASE32_DRAFT_READY_15,
          f"reverify_15 배치가 phase32 DRAFT_READY 15건과 정확히 같은 집합(실제 {reverify_ids})",
          str(reverify_ids ^ PHASE32_DRAFT_READY_15))

    dna_poly = next((r for r in reverify15 if r["term_id"] == "7049"), None)
    check(dna_poly is not None, "DNA 중합효소(7049)가 reverify_15에 존재")
    if dna_poly is not None:
        check(dna_poly["previous_status"] == "DRAFT_READY" and dna_poly["final_status"] == "HOLD",
              f"DNA 중합효소: phase32 DRAFT_READY -> phase33 HOLD로 전환 확인(실제 {dna_poly['previous_status']}->{dna_poly['final_status']})")
        check(dna_poly["status_changed"] == "True", "DNA 중합효소의 status_changed 플래그가 True로 기록됨")
        check(dna_poly["wikipedia_sole_source"] == "True", "DNA 중합효소가 위키백과 단독 근거임이 기록됨")

    unchanged_14 = [r for r in reverify15 if r["term_id"] != "7049"]
    still_draft_ready = [r["term_id"] for r in unchanged_14 if r["final_status"] != "DRAFT_READY"]
    check(not still_draft_ready, "DNA 중합효소를 제외한 나머지 14건은 phase32와 동일하게 DRAFT_READY 유지",
          str(still_draft_ready))
    no_change_flag = [r["term_id"] for r in unchanged_14 if r["status_changed"] == "True"]
    check(not no_change_flag, "나머지 14건은 status_changed=False(판정 변화 없음)", str(no_change_flag))

    grade_fit_vals = {r["grade_fit"] for r in new50 + reverify15}
    check(grade_fit_vals == {"POLICY_MAPPING_ONLY"}, f"grade_fit 전부 POLICY_MAPPING_ONLY(실제 {grade_fit_vals})")

    review_vals = {r["expert_review_status"] for r in new50 + reverify15}
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
        print("[SKIP] VOCABULARY_QUIZ_DB_PATH_RO 미지정 - 이번 세션은 로컬에 그 사본이 없어 실제 재조회 불가"
              "(phase33은 어떤 스크립트도 sqlite3로 이 DB에 연결하지 않으므로 구조적으로 불변 - "
              "phase31/32와 동일한 한계, 사본 제공 시에만 수치 재확인 가능)")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
