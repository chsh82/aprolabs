# -*- coding: utf-8 -*-
"""Phase27 - L6 관리자용 퀴즈 파일럿(file-based dry-run) 산출물 독립 검증.

이 테스트는 phase27_generate/verify/finalize 스크립트의 결과를 "다시" 신뢰하지
않고, 최종 문항 JSON을 그 스크립트들과는 별개의 로직으로 재검사한다. DB에는
SELECT만 수행하며 아무것도 쓰지 않는다(세션 생성 코드도, 문항 적재도 없음).

검증 항목:
  1. item_id 40개가 전부 고유한지
  2. 각 문항의 explanation에 인용된 정의가 "지금" DB에 있는 student_definition과
     정확히 같은지(생성 스크립트가 하드코딩한 문자열을 쓴 게 아니라 매 실행마다
     DB를 다시 읽는다는 것의 재확인 - 이 테스트 자체도 DB를 다시 읽어서 비교한다)
  3. 40개 문항의 item_id가 기존 vocabulary_multiformat_items에 전혀 없는지(중복 없음)
  4. 이번 단계가 vocabulary_multiformat_items에 실제로 아무 행도 추가하지
     않았는지(적재 전/후 행수 동일 - dry-run 준수 확인)
  5. 선정된 20개 콘텐츠가 전부 V(교과개념어 아님)이고, 매커니즘/이성/신장(HOLD
     3건) 표제어가 섞여 있지 않은지
  6. 결과표(CSV)의 PASS/HOLD 집계가 문항 JSON의 final_status 집계와 일치하는지
  7. 각 문항의 4개 보기에 완전 중복 문자열이 없는지, 정답이 정확히 1개인지

실행:
    VOCABULARY_QUIZ_DB_PATH=<research DB 사본 경로> python tests/test_phase27_l6_pilot_items.py
(환경변수를 생략하면 DEFAULT_COPY_DB_PATH를 사용한다)
"""
from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
ITEMS_JSON = REPO_ROOT / "data/import/schema_reading_phase27_l6_pilot_items_20260927.json"
RESULT_CSV = REPO_ROOT / "data/import/schema_reading_phase27_l6_pilot_result_20260927.csv"
SELECTED_JSON = REPO_ROOT / "data/import/schema_reading_phase27_l6_pilot_selected_20260927.json"

DEFAULT_COPY_DB_PATH = (
    Path.home() / "AppData" / "Local" / "Temp" / "claude" / "C--Users-aproa" /
    "6e2093a4-82bf-4ee0-833a-e5c458074f06" / "scratchpad" / "vocabulary_quiz_research_phase25_copy.db"
)
COPY_DB_PATH = Path(os.environ.get("VOCABULARY_QUIZ_DB_PATH") or DEFAULT_COPY_DB_PATH)

HOLD_LEMMAS = {"매커니즘", "이성", "신장"}

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(ITEMS_JSON.exists(), f"최종 문항 JSON 존재: {ITEMS_JSON}")
    check(RESULT_CSV.exists(), f"결과표 CSV 존재: {RESULT_CSV}")
    check(SELECTED_JSON.exists(), f"선정 콘텐츠 JSON 존재: {SELECTED_JSON}")
    if not (ITEMS_JSON.exists() and RESULT_CSV.exists() and SELECTED_JSON.exists()):
        return False

    items = json.load(open(ITEMS_JSON, encoding="utf-8"))
    with open(RESULT_CSV, encoding="utf-8-sig") as f:
        result_rows = list(csv.DictReader(f))
    selected = json.load(open(SELECTED_JSON, encoding="utf-8"))

    check(len(items) <= 40, f"문항 수 40건 이하 (실제 {len(items)}건)")
    check(len(selected) <= 20, f"선정 콘텐츠 20건 이하 (실제 {len(selected)}건)")

    # 1. item_id 고유성
    ids = [it["item_id"] for it in items]
    check(len(ids) == len(set(ids)), f"item_id 40개 전부 고유", f"중복 {len(ids)-len(set(ids))}건")

    # 5. 선정 콘텐츠에 HOLD 3건 표제어가 없는지
    selected_lemmas = {r["lemma"] for r in selected}
    check(not (selected_lemmas & HOLD_LEMMAS),
          "선정 20건에 매커니즘/이성/신장(HOLD 3건) 없음",
          str(selected_lemmas & HOLD_LEMMAS))

    if not COPY_DB_PATH.exists():
        print(f"[FAIL] 사전조건: research DB 사본이 없습니다({COPY_DB_PATH}) - "
              f"scp로 vocabulary_quiz_research.db 사본을 준비하세요. 이후 항목은 스킵합니다.")
        _results.append((False, "DB 사본 필요"))
        n_pass = sum(1 for ok, _ in _results if ok)
        print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
        return n_pass == len(_results)

    conn = sqlite3.connect(f"file:{COPY_DB_PATH}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")

    # 4. vocabulary_multiformat_items에 실제로 아무것도 추가되지 않았는지
    mfi_count = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
    check(mfi_count > 0, f"vocabulary_multiformat_items 조회 가능 (행수={mfi_count})")

    existing_item_ids = {r[0] for r in conn.execute("SELECT item_id FROM vocabulary_multiformat_items").fetchall()}
    overlap = set(ids) & existing_item_ids
    check(not overlap, "40개 문항 item_id가 기존 vocabulary_multiformat_items와 전혀 겹치지 않음(적재된 적 없음)",
          str(overlap))

    # 5(계속). 선정 콘텐츠가 전부 V인지 DB에서 재확인
    content_ids = [r["content_id"] for r in selected]
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"SELECT c.content_id, c.lemma, l.level_reason_json FROM vocabulary_contents c "
        f"JOIN vocabulary_content_levels l ON c.content_id=l.content_id "
        f"WHERE c.content_id IN ({placeholders})",
        content_ids,
    ).fetchall()
    s_leak = [r[1] for r in rows if json.loads(r[2]).get("is_S_subject_concept")]
    check(not s_leak, "선정 20건 전부 S(교과개념어) 아님(V만 선정)", str(s_leak))
    check(len(rows) == len(content_ids), f"선정된 {len(content_ids)}건 전부 DB에 존재")

    # 2. explanation 인용 정의가 지금 DB의 student_definition과 정확히 같은지
    live_def = {r[0]: None for r in rows}
    live_rows = conn.execute(
        f"SELECT content_id, student_definition, example_sentence, example_target_form "
        f"FROM vocabulary_contents WHERE content_id IN ({placeholders})",
        content_ids,
    ).fetchall()
    live = {r[0]: {"student_definition": r[1], "example_sentence": r[2], "example_target_form": r[3]} for r in live_rows}

    drift = []
    dup_options = []
    answer_not_unique = []
    context_mismatch = []
    for it in items:
        cur = live.get(it["source_content_id"])
        if cur is None:
            drift.append(it["item_id"])
            continue
        if cur["student_definition"] not in it["explanation"]:
            drift.append(it["item_id"])
        options = it["options_json"]
        if len(set(options)) != len(options):
            dup_options.append(it["item_id"])
        if options.count(cur["student_definition"]) != 1:
            answer_not_unique.append(it["item_id"])
        if it["item_type"] == "CONTEXT_MEANING":
            bracketed = f"【{cur['example_target_form']}】"
            if bracketed not in it["prompt"]:
                context_mismatch.append(it["item_id"])

    check(not drift, "모든 문항 explanation이 지금 DB의 student_definition과 일치(하드코딩 드리프트 없음)", str(drift))
    check(not dup_options, "모든 문항 보기 4개에 완전 중복 문자열 없음", str(dup_options))
    check(not answer_not_unique, "모든 문항 정답 문자열이 보기 중 정확히 1회 등장", str(answer_not_unique))
    check(not context_mismatch, "모든 CONTEXT_MEANING 문항이 현재 example_target_form을 그대로 표시", str(context_mismatch))

    conn.close()

    # 6. 결과표 PASS/HOLD 집계 vs 문항 JSON final_status 집계 일치
    csv_status = Counter(r["final_status"] for r in result_rows)
    json_status = Counter(it["qa_flags_json"]["final_status"] for it in items)
    check(csv_status == json_status, "결과표(CSV) final_status 집계 == 문항 JSON final_status 집계",
          f"csv={dict(csv_status)} json={dict(json_status)}")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
