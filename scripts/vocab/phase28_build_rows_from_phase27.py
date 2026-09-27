#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase28 항목2: phase27 최종 문항 JSON(40건)을 vocabulary_multiformat_items
INSERT용 행(rows-json)으로 변환하면서, 적재 전 dry-run 검증을 전부 통과해야만
파일을 만든다. 읽기 전용(SELECT만) - DB에 아무것도 쓰지 않는다.

dry-run 검증:
  1. 입력이 정확히 40건인지(20 content x 2 유형) - 아니면 즉시 중단, 파일을
     만들지 않는다
  2. 유형별 정확히 20/20인지
  3. item_id 40개가 서로 중복 없는지 + 기존 vocabulary_multiformat_items와
     전혀 겹치지 않는지
  4. 문항 "내용" 중복이 없는지 - (item_type, prompt, options_json 정렬본)
     조합이 40건 모두 서로 달라야 함(생성 로직 버그로 같은 문항이 두 번
     만들어지는 사고 방지)
  5. content_id 연결: 40건 전부 source_content_id가 실제 vocabulary_contents에
     있는 값을 가리키는지
  6. 정답/공개 payload 분리: public_payload_json에는 정답 인덱스가 들어있지
     않고(보기 목록만), answer_payload_json에는 정답 인덱스만 들어있는지
     (클라이언트에 정답이 새 나가지 않는 구조를 파일 단계에서부터 보장)

위 6개를 전부 통과해야 rows-json을 저장한다. 하나라도 실패하면 아무 파일도
만들지 않고 종료 코드 1을 반환한다.

사용:
    python3 scripts/vocab/phase28_build_rows_from_phase27.py \\
        --db-path <research db, 읽기 전용> \\
        --items-json data/import/schema_reading_phase27_l6_pilot_items_20260927.json \\
        --out data/import/schema_reading_phase28_l6_pilot_quiz_rows_20260927.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

NEW_SOURCE_VERSION = "schema_reading_l6_pilot_dryrun_v1"
GENERATOR_VERSION = "schema_reading_phase27_l6_pilot_v1"
PHASE27_REPORT = "reports/schema_reading_phase27_l6_pilot_dryrun_20260927.md"
PHASE28_REPORT = "reports/schema_reading_phase28_l6_pilot_apply_20260927.md"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--items-json", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    items = json.load(open(args.items_json, encoding="utf-8"))
    ok = True

    print(f"입력 문항 수: {len(items)} (기대값 40)")
    if len(items) != 40:
        print("GATE FAIL: 입력이 40건이 아님 - rows-json을 만들지 않고 중단")
        return 1

    from collections import Counter
    type_counts = Counter(it["item_type"] for it in items)
    print(f"유형별 건수: {dict(type_counts)} (기대값 MEANING_CHOICE=20, CONTEXT_MEANING=20)")
    if type_counts.get("MEANING_CHOICE") != 20 or type_counts.get("CONTEXT_MEANING") != 20:
        print("GATE FAIL: 유형별 20/20이 아님 - 중단")
        return 1

    item_ids = [it["item_id"] for it in items]
    if len(set(item_ids)) != 40:
        print("GATE FAIL: 입력 자체에 item_id 중복 있음 - 중단")
        return 1

    content_dup_keys = [(it["item_type"], it["prompt"], tuple(sorted(it["options_json"]))) for it in items]
    if len(set(content_dup_keys)) != 40:
        print("GATE FAIL: 문항 내용(유형+프롬프트+보기 집합)이 완전히 같은 중복 문항 존재 - 중단")
        return 1
    print("문항 내용 중복 0건 확인")

    conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")

    existing_ids = {r[0] for r in conn.execute("SELECT item_id FROM vocabulary_multiformat_items").fetchall()}
    collide = set(item_ids) & existing_ids
    print(f"기존 vocabulary_multiformat_items와 item_id 충돌: {len(collide)}건")
    if collide:
        print("GATE FAIL:", collide)
        ok = False

    content_ids = sorted({it["source_content_id"] for it in items})
    placeholders = ",".join("?" for _ in content_ids)
    found = {r[0] for r in conn.execute(
        f"SELECT content_id FROM vocabulary_contents WHERE content_id IN ({placeholders})", content_ids
    ).fetchall()}
    missing = [c for c in content_ids if c not in found]
    print(f"content_id 연결(40건 전부 유효한 vocabulary_contents를 가리킴): 누락 {len(missing)}건 {missing}")
    if missing:
        ok = False

    conn.close()
    if not ok:
        print("\nGATE FAIL: 위 문제로 rows-json을 만들지 않고 중단")
        return 1

    rows = []
    for it in items:
        options = it["options_json"]
        correct_option = it["correct_option"]
        public_payload = {"options": options}
        answer_payload = {"correct_option": correct_option}

        # 6. 정답/공개 payload 분리 재확인(빌드 시점에 우리가 만든 값 스스로 검증)
        assert "correct_option" not in public_payload
        assert set(answer_payload.keys()) == {"correct_option"}

        qa = it["qa_flags_json"]
        qa_flags_entry = {
            "flag": "PHASE28_L6_ADMIN_PILOT_REVIEW",
            "source_version_isolation": (
                f"source_version deliberately set to {NEW_SOURCE_VERSION} (not 2.1.29, and not "
                "schema_reading_l4l5_pilot_dryrun_v1) so this L6 batch is structurally excluded "
                "from general/mixed-mode selection, level-mode selection, and the existing L4/L5 "
                "admin pilot mode (app/vocabulary_quiz/routers/multiformat.py hardcoded "
                "SOURCE_VERSION/PILOT_SOURCE_VERSION checks) until a future deliberate migration "
                "changes it"
            ),
            "literacy_term_id": qa["literacy_term_id"],
            "content_source_version": qa["content_source_version"],
            "vocab_level": 6,
            "l6_grade_caveat": qa["l6_grade_caveat"],
            "expert_review_status": qa["expert_review_status"],
            "caution": qa["caution"],
            "auto_validation_status": qa["auto_validation_status"],
            "auto_validation_issues": qa["auto_validation_issues"],
            "semantic_verdict": qa["semantic_verdict"],
            "semantic_reason": qa["semantic_reason"],
            "wrong_option_reasons": qa["wrong_option_reasons"],
            "phase27_report": PHASE27_REPORT,
            "phase28_report": PHASE28_REPORT,
        }

        rows.append({
            "item_id": it["item_id"],
            "item_type": it["item_type"],
            "source_content_id": it["source_content_id"],
            "source_content_ids_json": None,
            "sense_id": None,
            "sense_ids_json": None,
            "lemma": it["lemma"],
            "pos": it["pos"],
            "prompt": it["prompt"],
            "options_json": json.dumps(options, ensure_ascii=False),
            "correct_option": correct_option,
            "public_payload_json": json.dumps(public_payload, ensure_ascii=False),
            "answer_payload_json": json.dumps(answer_payload, ensure_ascii=False),
            "explanation": it["explanation"],
            "cognitive_level": None,
            "qa_flags_json": json.dumps([qa_flags_entry], ensure_ascii=False),
            "generator_version": GENERATOR_VERSION,
            "source_version": NEW_SOURCE_VERSION,
        })

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1)
    print(f"\n[PASS] dry-run 전체 통과, rows-json {len(rows)}건 저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
