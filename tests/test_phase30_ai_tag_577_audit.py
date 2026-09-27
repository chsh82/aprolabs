# -*- coding: utf-8 -*-
"""Phase30 - L6 AI 태그 577건 감사 산출물 독립 재검증. 읽기 전용(SELECT만) -
literacy.db·vocabulary_quiz DB 어디에도 쓰지 않는다. DB 적재나 검수 완료
처리는 이 테스트도, phase30의 어떤 스크립트도 하지 않는다.

검증 항목:
  1. 577건 재추출이 결정론적으로 재현되는지(같은 코드 재실행 -> 같은 term_id
     집합, 같은 V/S 분포)
  2. 100건 표본이 문서화된 seed(577)로 재현되는지(재실행 -> 동일 term_id 집합)
  3. 의미 판정표가 101건(표본 100 + V 1) 전부를 빠짐없이 커버하고 5종
     판정값 중 하나만 쓰는지, 목표 수량을 채우려 판정을 생략한 흔적(빈 값)이
     없는지
  4. literacy.db가 이번 단계 동안 정말 읽기 전용이었는지(mtime 불변)
  5. vocabulary_quiz 연구 서버 DB의 콘텐츠(5,902)·문항(1,369) 수가 이번
     단계 동안 불변인지(원격 SSH가 가능한 환경에서만 - 없으면 건너뜀)

실행:
    python tests/test_phase30_ai_tag_577_audit.py
"""
from __future__ import annotations

import csv
import io
import os
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
EXTRACT_SCRIPT = REPO_ROOT / "scripts" / "vocab" / "phase30_extract_ai_tag_577.py"
SAMPLE_SCRIPT = REPO_ROOT / "scripts" / "vocab" / "phase30_stratified_sample.py"
FULL_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase30_ai_tag_577_20260927.csv"
SAMPLE_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase30_s_sample_20260927.csv"
VERDICT_CSV = REPO_ROOT / "data" / "import" / "schema_reading_phase30_semantic_verdicts_20260927.csv"
LITERACY_DB = REPO_ROOT / "data" / "literacy.db"

VALID_VERDICTS = {"SOURCE_SUPPORTED", "SENSE_AMBIGUOUS", "DEFINITION_MISMATCH",
                  "EXAMPLE_PROBLEM", "INSUFFICIENT_SOURCE"}

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(FULL_CSV.exists(), f"577건 결과 CSV 존재: {FULL_CSV}")
    check(SAMPLE_CSV.exists(), f"100건 표본 CSV 존재: {SAMPLE_CSV}")
    check(VERDICT_CSV.exists(), f"판정표 CSV 존재: {VERDICT_CSV}")
    if not (FULL_CSV.exists() and SAMPLE_CSV.exists() and VERDICT_CSV.exists()):
        return False

    mtime_before = LITERACY_DB.stat().st_mtime

    # 1. 577건 재추출 재현성
    with tempfile.TemporaryDirectory() as tmpdir:
        rerun_csv = Path(tmpdir) / "rerun_577.csv"
        result = subprocess.run(
            [sys.executable, str(EXTRACT_SCRIPT), "--literacy-db", str(LITERACY_DB), "--out", str(rerun_csv)],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120,
        )
        check(result.returncode == 0, "phase30_extract_ai_tag_577.py 재실행 종료 코드 0",
              result.stdout[-1000:] + result.stderr[-1000:])
        if rerun_csv.exists():
            with open(FULL_CSV, encoding="utf-8-sig") as f:
                orig_ids = sorted(r["term_id"] for r in csv.DictReader(f))
            with open(rerun_csv, encoding="utf-8-sig") as f:
                rerun_rows = list(csv.DictReader(f))
            rerun_ids = sorted(r["term_id"] for r in rerun_rows)
            check(orig_ids == rerun_ids, f"577건 term_id 집합 재현됨(재실행 {len(rerun_ids)}건)",
                  f"차이: {set(orig_ids) ^ set(rerun_ids)}")
            v_n = sum(1 for r in rerun_rows if r["source"] == "schemareading-tooldict")
            s_n = sum(1 for r in rerun_rows if r["source"] == "schemareading-schema")
            check(v_n == 1 and s_n == 576, f"재실행 V/S 분포 1/576(실제 {v_n}/{s_n})")
            n_src_def = sum(1 for r in rerun_rows if r["xlsx_definition_present"] == "True")
            check(n_src_def == 0, f"재실행에서도 원천 XLSX 정의 보유 0건 재확인(실제 {n_src_def})")

        # 2. 100건 표본 재현성(같은 seed)
        rerun_sample_csv = Path(tmpdir) / "rerun_sample.csv"
        result2 = subprocess.run(
            [sys.executable, str(SAMPLE_SCRIPT), "--input", str(FULL_CSV), "--out", str(rerun_sample_csv),
             "--max-sample", "100", "--seed", "577"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=60,
        )
        check(result2.returncode == 0, "phase30_stratified_sample.py 재실행 종료 코드 0",
              result2.stdout[-1000:] + result2.stderr[-1000:])
        if rerun_sample_csv.exists():
            with open(SAMPLE_CSV, encoding="utf-8-sig") as f:
                orig_sample_ids = sorted(r["term_id"] for r in csv.DictReader(f))
            with open(rerun_sample_csv, encoding="utf-8-sig") as f:
                rerun_sample_ids = sorted(r["term_id"] for r in csv.DictReader(f))
            check(orig_sample_ids == rerun_sample_ids,
                  f"100건 표본 term_id 집합이 seed=577로 정확히 재현됨(재실행 {len(rerun_sample_ids)}건)",
                  f"차이: {set(orig_sample_ids) ^ set(rerun_sample_ids)}")

    # 3. 판정표 완전성
    with open(VERDICT_CSV, encoding="utf-8-sig") as f:
        verdicts = list(csv.DictReader(f))
    check(len(verdicts) == 101, f"판정표가 정확히 101건(표본 100 + V 1)(실제 {len(verdicts)})")
    empty_verdicts = [v["term_id"] for v in verdicts if not v.get("semantic_verdict")]
    check(not empty_verdicts, f"빈 판정 0건(목표 수량을 채우려 판정을 생략한 흔적 없음)", str(empty_verdicts))
    invalid = [v["term_id"] for v in verdicts if v["semantic_verdict"] not in VALID_VERDICTS]
    check(not invalid, f"판정값이 5종 중 하나가 아닌 건 0건", str(invalid))
    check(any(v["term_id"] == "5022" for v in verdicts), "V 항목(5022, 이면적)이 판정표에 전수 포함됨")

    dist = Counter(v["semantic_verdict"] for v in verdicts)
    print(f"의미 판정 분포(표본): {dict(dist)}")
    check(dist.get("SOURCE_SUPPORTED", 0) == 0,
          "SOURCE_SUPPORTED 판정 0건임을 재확인(577건 전수에 원천 정의가 없으므로 "
          "구조적으로 SOURCE_SUPPORTED가 나올 수 없음 - 문자열 일치로 통과시키지 "
          "않았다는 것을 이 사실 자체가 뒷받침)")

    grade_fit_vals = {v["grade_fit"] for v in verdicts}
    check(grade_fit_vals == {"POLICY_MAPPING_ONLY"},
          f"grade_fit이 전부 POLICY_MAPPING_ONLY(개별 학년 근거 없음)(실제 {grade_fit_vals})")

    # 4. literacy.db 읽기 전용 재확인
    mtime_after = LITERACY_DB.stat().st_mtime
    check(mtime_before == mtime_after, f"literacy.db mtime 불변(전 {mtime_before}, 후 {mtime_after})")

    # 5. 연구 서버 vocabulary_quiz DB 불변(SSH 가능한 환경에서만)
    vq_db_path = os.environ.get("VOCABULARY_QUIZ_DB_PATH_RO")
    if vq_db_path and Path(vq_db_path).exists():
        import sqlite3
        conn = sqlite3.connect(f"file:{vq_db_path}?mode=ro", uri=True)
        vc = conn.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        mfi = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        conn.close()
        check(vc == 5902, f"vocabulary_contents 5,902건 불변(실제 {vc})")
        check(mfi == 1369, f"vocabulary_multiformat_items 1,369건 불변(실제 {mfi})")
    else:
        print("[SKIP] VOCABULARY_QUIZ_DB_PATH_RO 미지정 - 연구 서버 DB 불변 확인은 "
              "phase30 보고서의 별도 SSH 재조회 로그 참고")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
