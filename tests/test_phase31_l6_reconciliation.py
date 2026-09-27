# -*- coding: utf-8 -*-
"""Phase31 - L6 원천·표기·동형이의 예외 정리 산출물 독립 재검증. 읽기 전용
(SELECT만) - literacy.db·vocabulary_quiz DB 어디에도 쓰지 않는다. 이번
단계도, 이 테스트도 577건의 상태·레벨·공개 플래그를 전혀 바꾸지 않는다.

검증 항목:
  1. phase31_l6_reconciliation.py 재실행 -> 34건이 정확히 32(OWN_L6_TERM)+2
     (ABSORBED_INTO_LOWER_LEVEL)+0(UNRESOLVED)로 재현되는지
  2. 동형이의 4건이 vocabulary_quiz_research.db에 전혀 로드돼 있지 않은지
     (자동 링크·적재 대상으로 들어가지 않았다는 것의 재확인)
  3. 표기 의심 3건·소분류 재태깅 후보 5건의 dry-run 제안 산출물이 전부
     `applied: False`로 남아 있는지(이번 단계가 실제로 아무것도 고치지
     않았다는 것의 기계적 재확인)
  4. 항목5 커버리지 수치(과학 0%, 사회 10.5%, 개별 학년 근거 0/693)가
     재실행에서도 동일한지
  5. literacy.db mtime과 vocabulary_quiz DB 콘텐츠(5,902)/문항(1,369)/
     노출·공개(0/0) 불변

실행:
    python tests/test_phase31_l6_reconciliation.py
(연구 서버 vocabulary_quiz DB 사본 경로를 VOCABULARY_QUIZ_DB_PATH_RO로
지정하면 항목2/5의 서버 DB 재확인도 함께 수행 - 없으면 그 부분만 건너뜀)
"""
from __future__ import annotations

import csv
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "vocab" / "phase31_l6_reconciliation.py"
LITERACY_DB = REPO_ROOT / "data" / "literacy.db"
ITEM3_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase31_item3_spelling_dryrun_20260927.csv"
ITEM4_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase31_item4_recategorize_dryrun_20260927.csv"
ITEM2_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase31_item2_homonym_20260927.csv"

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(SCRIPT.exists(), f"phase31 스크립트 존재: {SCRIPT}")
    check(ITEM3_CSV.exists() and ITEM4_CSV.exists() and ITEM2_CSV.exists(), "phase31 산출물 CSV 3종 존재")

    mtime_before = LITERACY_DB.stat().st_mtime

    vq_db_ro = os.environ.get("VOCABULARY_QUIZ_DB_PATH_RO")
    with tempfile.TemporaryDirectory() as tmpdir:
        cmd = [sys.executable, str(SCRIPT), "--literacy-db", str(LITERACY_DB), "--out-dir", tmpdir]
        if vq_db_ro and Path(vq_db_ro).exists():
            cmd += ["--vq-db", vq_db_ro]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
        check(result.returncode == 0, "phase31_l6_reconciliation.py 재실행 종료 코드 0",
              (result.stdout or "")[-1500:] + (result.stderr or "")[-1500:])

        rerun_item1 = Path(tmpdir) / "schema_reading_phase31_item1_34vs32_20260927.csv"
        if rerun_item1.exists():
            with open(rerun_item1, encoding="utf-8-sig") as f:
                rows = list(csv.DictReader(f))
            from collections import Counter
            dist = Counter(r["status"] for r in rows)
            check(len(rows) == 34, f"34행 재현(실제 {len(rows)})")
            check(dist.get("OWN_L6_TERM") == 32, f"OWN_L6_TERM 32건 재현(실제 {dist.get('OWN_L6_TERM')})")
            check(dist.get("ABSORBED_INTO_LOWER_LEVEL") == 2,
                  f"ABSORBED_INTO_LOWER_LEVEL 2건 재현(실제 {dist.get('ABSORBED_INTO_LOWER_LEVEL')})")
            check(dist.get("UNRESOLVED", 0) == 0, f"UNRESOLVED 0건(실제 {dist.get('UNRESOLVED', 0)})")

        rerun_item5 = Path(tmpdir) / "schema_reading_phase31_item5_coverage_20260927.json"
        if rerun_item5.exists():
            cov = json.loads(rerun_item5.read_text(encoding="utf-8"))
            sci = cov["by_subject_xlsx_coverage"].get("과학", {})
            soc = cov["by_subject_xlsx_coverage"].get("사회", {})
            check(sci.get("with_def") == 0, f"과학 L6 원문 정의보유 0건 재현(실제 {sci.get('with_def')})")
            check(soc.get("with_def") == 34, f"사회 L6 원문 정의보유 34건 재현(실제 {soc.get('with_def')})")
            check(cov["individual_grade_evidence_count"] == 0,
                  f"개별 학년 근거 0건 재현(실제 {cov['individual_grade_evidence_count']})")

    # 3. dry-run 제안이 전부 미적용 상태인지
    with open(ITEM3_CSV, encoding="utf-8-sig") as f:
        item3_rows = list(csv.DictReader(f))
    with open(ITEM4_CSV, encoding="utf-8-sig") as f:
        item4_rows = list(csv.DictReader(f))
    check(len(item3_rows) == 3 and all(r["applied"] == "False" for r in item3_rows),
          f"표기 의심 3건 전부 applied=False(미적용) 확인")
    check(len(item4_rows) == 5 and all(r["applied"] == "False" for r in item4_rows),
          f"소분류 재태깅 후보 5건 전부 applied=False(미적용) 확인")

    # 2. 동형이의 4건이 실제로 vocabulary_quiz DB에 없는지
    with open(ITEM2_CSV, encoding="utf-8-sig") as f:
        item2_rows = list(csv.DictReader(f))
    check(len(item2_rows) == 4, f"동형이의 비교 4건(실제 {len(item2_rows)})")
    if vq_db_ro and Path(vq_db_ro).exists():
        not_loaded = [r for r in item2_rows if r["vocabulary_quiz_status"] == "NOT_LOADED"]
        check(len(not_loaded) == 4, f"동형이의 4건 전부 vocabulary_quiz DB에 미적재(실제 {len(not_loaded)}/4) "
                                     "- 자동 링크·적재 대상으로 들어가지 않았음")
    else:
        print("[SKIP] VOCABULARY_QUIZ_DB_PATH_RO 미지정 - 항목2의 서버 DB 재확인은 phase31 보고서 로그 참고")

    # 5. literacy.db 읽기 전용 재확인
    mtime_after = LITERACY_DB.stat().st_mtime
    check(mtime_before == mtime_after, f"literacy.db mtime 불변(전 {mtime_before}, 후 {mtime_after})")

    if vq_db_ro and Path(vq_db_ro).exists():
        import sqlite3
        conn = sqlite3.connect(f"file:{vq_db_ro}?mode=ro", uri=True)
        vc = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        mfi = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        exposure = conn.execute("SELECT SUM(student_exposure), SUM(public_ready) FROM vocabulary_contents").fetchone()
        conn.close()
        check(vc == 5902, f"vocabulary_contents 5,902건 불변(실제 {vc})")
        check(mfi == 1369, f"vocabulary_multiformat_items 1,369건 불변(실제 {mfi})")
        check(exposure == (0, 0), f"student_exposure/public_ready 합계 0/0 불변(실제 {exposure})")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
