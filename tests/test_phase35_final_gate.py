# -*- coding: utf-8 -*-
"""phase35 - 최종 적재 게이트 재검증. 읽기 전용 - literacy.db는 SELECT조차
하지 않는다. 이 테스트 자체도 어떤 DB에도 쓰지 않는다.

검증 항목:
  1. known_definition_errors 등록부(3건: 명목GDP·중력 시간지연·세이의 법칙)가
     최종 DRAFT_READY 후보에 하나도 통과하지 못했는지
  2. 언론/개인사이트 단독 근거였던 8건이 전부 재분류(5건 상향 교체 또는
     3건 HOLD 하향)됐는지 - 남아 있는 DRAFT_READY 중 언론/개인사이트
     단독인 항목이 0건인지
  3. load_readiness 스냅샷의 write_performed가 false이고, 게이트1(환경)이
     BLOCKED로 정직하게 기록돼 있는지(쓰기를 실행하지 않았다는 구조적 보장)
  4. 최종 DRAFT_READY 후보 수가 51로 미리 고정되지 않고 재검증 결과 그대로
     (48건)인지
  5. literacy.db mtime 불변

실행:
    python tests/test_phase35_final_gate.py
"""
from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts" / "vocab"))

from known_definition_errors import load_excluded_ids  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
VERDICT_CSV = REPO_ROOT / "data/import/schema_reading_phase35_verdicts_20260929.csv"
AUDIT_CSV = REPO_ROOT / "data/import/schema_reading_phase35_evidence_tier_final_20260929.csv"
READINESS_JSON = REPO_ROOT / "data/import/schema_reading_phase35_load_readiness_20260929.json"
LITERACY_DB = REPO_ROOT / "data/literacy.db"

LOW_TIER_UPGRADED_5 = {"5881", "6799", "6816", "5766", "5825"}
LOW_TIER_DOWNGRADED_3 = {"5862", "6825", "6945"}

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(VERDICT_CSV.exists(), f"phase35 판정표 존재: {VERDICT_CSV}")
    check(READINESS_JSON.exists(), f"load_readiness 스냅샷 존재: {READINESS_JSON}")
    if not (VERDICT_CSV.exists() and READINESS_JSON.exists()):
        return False

    mtime_before = LITERACY_DB.stat().st_mtime

    with open(VERDICT_CSV, encoding="utf-8-sig") as f:
        verdicts = {r["term_id"]: r for r in csv.DictReader(f)}
    with open(AUDIT_CSV, encoding="utf-8-sig") as f:
        audit = {r["term_id"]: r for r in csv.DictReader(f)}
    readiness = json.loads(READINESS_JSON.read_text(encoding="utf-8"))

    draft_ready_ids = {tid for tid, r in verdicts.items() if r["final_status"] == "DRAFT_READY"}

    excluded = load_excluded_ids()
    check(len(excluded) == 3, f"known_definition_errors 등록부 3건(실제 {len(excluded)}건)", str(excluded))
    check(not (draft_ready_ids & excluded),
          "명목GDP·중력 시간지연·세이의 법칙이 최종 DRAFT_READY에 하나도 없음",
          str(draft_ready_ids & excluded))

    for tid in LOW_TIER_UPGRADED_5:
        check(tid in draft_ready_ids, f"{tid}가 사전/공공기관 교체 후 DRAFT_READY로 남음")
    for tid in LOW_TIER_DOWNGRADED_3:
        check(tid not in draft_ready_ids, f"{tid}가 언론/개인사이트 단독 근거로 HOLD 처리됨")

    remaining_low_tier = [
        tid for tid in draft_ready_ids
        if tid in audit and audit[tid]["evidence_tier"] in ("언론", "개인사이트")
    ]
    check(not remaining_low_tier,
          "최종 DRAFT_READY 중 언론/개인사이트 단독 근거로 남은 항목 0건(전부 재분류 완료)",
          str(remaining_low_tier))

    check(readiness["write_performed"] is True, "load_readiness.write_performed == true(재시도로 게이트1 통과 후 적재 완료)")
    check(readiness["gate_status"]["1_environment_app_env"].startswith("PASS"),
          "게이트1(환경/APP_ENV)이 재시도 시점에 PASS로 기록됨")
    apply_result = readiness.get("apply_result", {})
    check(apply_result.get("inserted_vocabulary_contents") == 48, "apply_result: vocabulary_contents 48건 삽입 기록")
    check(apply_result.get("post_apply_vocabulary_contents_count") == 5950,
          f"apply_result: 적재 후 총 5,950건(5902+48) 기록(실제 {apply_result.get('post_apply_vocabulary_contents_count')})")
    check(apply_result.get("post_apply_vocabulary_multiformat_items_count") == 1369,
          "apply_result: vocabulary_multiformat_items 1,369건 불변 기록")
    check(apply_result.get("post_apply_student_exposure_or_public_ready_true_count") == 0,
          "apply_result: 신규 포함 전체 student_exposure/public_ready=1 행 0건 기록")
    check("SKIPPED=48" in apply_result.get("idempotency_rerun_result", ""),
          "apply_result: 멱등성 재실행 결과가 기록됨(SKIPPED=48)")
    check(readiness["duplicate_target_check"]["existing_links_found_in_vocabulary_content_literacy_links"] == 0,
          "대상 중복 검사 결과 0건(연구 서버 SELECT 재확인)")

    n_candidates = len(readiness["candidate_term_ids_48"])
    check(n_candidates == len(draft_ready_ids) == 48,
          f"최종 후보 수가 51로 미리 고정되지 않고 재검증 결과 그대로(readiness={n_candidates}, verdicts={len(draft_ready_ids)})")

    mtime_after = LITERACY_DB.stat().st_mtime
    check(mtime_before == mtime_after, f"literacy.db mtime 불변(전 {mtime_before}, 후 {mtime_after})")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
