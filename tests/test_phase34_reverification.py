# -*- coding: utf-8 -*-
"""phase34 - DRAFT_READY 54건 URL 전수 재확인 산출물 독립 재검증. 읽기
전용(SELECT만) - literacy.db·vocabulary_quiz DB 어디에도 쓰지 않는다.
577건의 상태·레벨·공개 플래그를 전혀 바꾸지 않으며, 적재·상태 전환·
문항 생성·push·배포를 하지 않는다.

검증 항목:
  1. URL 감사표(54건)가 phase33 최초 DRAFT_READY 54건과 정확히 같은
     집합인지
  2. 메인 세션 재검증으로 새로 HOLD 전환된 3건(정당정치·세이의 법칙·
     반응 속도와 농도)이 실제로 phase34 verdicts에서 HOLD인지, 그리고
     student_definition_draft가 비어 있는지
  3. 위키백과 단독 근거 HOLD 9건(신규 8 + DNA 중합효소 1)이 이번 단계에서
     그대로 HOLD 유지됐는지(독립 근거가 새로 확보되지 않는 한 유지하라는
     지시 재확인)
  4. 원 URL이 깨져 표준국어대사전으로 교체된 5건이 여전히 DRAFT_READY이고
     location이 stdict.korean.go.kr로 바뀌었는지
  5. 명목GDP(5800)가 known_definition_errors 등록부와 최종 verdicts 양쪽
     모두에서 일관되게 HOLD로 처리되는지
  6. 최종 수치: DRAFT_READY/HOLD, 근거 등급별 분포, URL 확인 실패 건수,
     의미 충돌(CONFLICT) 건수가 감사표에서 정확히 집계되는지
  7. literacy.db mtime 불변

실행:
    python tests/test_phase34_reverification.py
"""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts" / "vocab"))

from known_definition_errors import load_excluded_ids  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
AUDIT_CSV = REPO_ROOT / "data/import/schema_reading_phase34_url_audit_20260928.csv"
VERDICT_CSV = REPO_ROOT / "data/import/schema_reading_phase34_verdicts_20260928.csv"
LITERACY_DB = REPO_ROOT / "data/literacy.db"

ORIGINAL_DRAFT_READY_54 = {
    "5881", "5954", "5980", "6881", "5924", "5945", "7025", "7045", "6832", "6851",
    "6990", "7009", "5747", "5766", "5844", "5862", "6799", "6816", "5817", "6863",
    "6880", "6784", "6938", "7017", "5825", "6777", "5775", "7053", "6763", "6825",
    "6945", "6968", "6918", "5839", "5786", "5876", "6956", "5796", "5868", "5950",
    "6904", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "5749", "5794",
    "5834", "5845", "5884", "5968",
}
NEW_HOLD_3 = {"5954", "5817", "6968"}
WIKI_SOLE_HOLD_9 = {"5915", "6901", "6974", "6989", "6798", "6924", "6858", "6908", "5920"}
REPLACED_5 = {"7045", "5747", "6938", "6918", "6956"}

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(AUDIT_CSV.exists(), f"URL 감사표 존재: {AUDIT_CSV}")
    check(VERDICT_CSV.exists(), f"재검증 판정표 존재: {VERDICT_CSV}")
    if not (AUDIT_CSV.exists() and VERDICT_CSV.exists()):
        return False

    mtime_before = LITERACY_DB.stat().st_mtime

    with open(AUDIT_CSV, encoding="utf-8-sig") as f:
        audit = list(csv.DictReader(f))
    with open(VERDICT_CSV, encoding="utf-8-sig") as f:
        verdicts = {r["term_id"]: r for r in csv.DictReader(f)}

    audit_ids = {r["term_id"] for r in audit}
    check(audit_ids == ORIGINAL_DRAFT_READY_54,
          f"URL 감사표가 phase33 최초 DRAFT_READY 54건과 정확히 일치(실제 {len(audit_ids)}건)",
          str(audit_ids ^ ORIGINAL_DRAFT_READY_54))

    for tid in NEW_HOLD_3:
        r = verdicts.get(tid)
        check(r is not None and r["final_status"] == "HOLD",
              f"{tid}가 메인 세션 재검증으로 HOLD 전환됨", str(r["final_status"] if r else "MISSING"))
        check(r is not None and not r["student_definition_draft"],
              f"{tid}(HOLD)의 student_definition_draft가 비어 있음")

    for tid in WIKI_SOLE_HOLD_9:
        r = verdicts.get(tid)
        check(r is not None and r["final_status"] == "HOLD" and r["wikipedia_sole_source"] == "True",
              f"위키백과 단독 근거 {tid}가 이번 단계에서도 HOLD 유지", str(r))

    r_dna = verdicts.get("7049")
    check(r_dna is not None and r_dna["final_status"] == "HOLD",
          "DNA 중합효소(7049)도 위키백과 단독 근거로 HOLD 유지")

    for tid in REPLACED_5:
        r = verdicts.get(tid)
        check(r is not None and r["final_status"] == "DRAFT_READY",
              f"{tid}는 원 URL 교체 후에도 DRAFT_READY 유지", str(r["final_status"] if r else "MISSING"))
        check(r is not None and "stdict.korean.go.kr" in r["location"],
              f"{tid}의 location이 표준국어대사전 직접 URL로 교체됨", str(r["location"] if r else "MISSING"))

    r_5800 = verdicts.get("5800")
    check(r_5800 is not None and r_5800["final_status"] == "HOLD",
          "명목GDP(5800)가 최종 판정표에서도 HOLD")
    check("5800" in load_excluded_ids(), "명목GDP(5800)가 known_definition_errors 등록부에도 존재(이중 안전장치)")

    tier_counts: dict[str, int] = {}
    access_counts: dict[str, int] = {}
    for r in audit:
        tier_counts[r["evidence_tier"]] = tier_counts.get(r["evidence_tier"], 0) + 1
        access_counts[r["access_status"]] = access_counts.get(r["access_status"], 0) + 1
    check(sum(tier_counts.values()) == 54, f"근거 등급 분포 합계 54(실제 {sum(tier_counts.values())})", str(tier_counts))
    check(access_counts.get("FAILED", 0) == 1, f"URL 확인 실패(대체 자료도 못 찾음) 1건(실제 {access_counts.get('FAILED', 0)})")
    check(access_counts.get("REPLACED", 0) == 5, f"원 URL 실패 후 교체 5건(실제 {access_counts.get('REPLACED', 0)})")

    conflict_ids = [r["term_id"] for r in verdicts.values() if r["comparison_verdict"] == "CONFLICT"]
    check(len(conflict_ids) == 3,
          f"의미 충돌(CONFLICT) 3건(명목GDP·중력 시간지연·세이의 법칙, 실제 {len(conflict_ids)}건)", str(conflict_ids))

    mtime_after = LITERACY_DB.stat().st_mtime
    check(mtime_before == mtime_after, f"literacy.db mtime 불변(전 {mtime_before}, 후 {mtime_after})")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
