"""phase17 결과표(40행, SEMANTIC_PASS 40/40)를 phase18_apply_quiz_pilot.py가 읽는
rows-json(vocabulary_multiformat_items INSERT용 행 목록)으로 변환한다.

입력 CSV 컬럼 그대로 사용 - 재조사·재생성하지 않고 phase16/phase17이 이미 확정한
값(prompt/options_json/correct_option/answer_payload_json/explanation/
wrong_option_reasons_json/source_version 등)을 그대로 옮긴다. qa_flags_json에는
phase16/17 산출물 경로와 검토 근거를 그대로 보존해, 적재 후에도 각 문항이 어떤
근거로 통과했는지 DB에서 바로 추적할 수 있게 한다.

실행:
    python scripts/vocab/phase18_build_rows_from_phase17.py \\
        --csv data/import/schema_reading_phase17_quiz_pilot_semantic_review_20260926.csv \\
        --out data/import/schema_reading_phase18_quiz_pilot_rows_20260926.json
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

EXPECTED_SOURCE_VERSION = "schema_reading_l4l5_pilot_dryrun_v1"


def build_row(r: dict) -> dict:
    options = json.loads(r["options_json"])
    answer_payload = json.loads(r["answer_payload_json"])
    wrong_reasons = json.loads(r["wrong_option_reasons_json"])
    qa_flag_entry = {
        "flag": "PHASE18_QUIZ_PILOT_ADMIN_REVIEW",
        "source_version_isolation": (
            "source_version deliberately set to schema_reading_l4l5_pilot_dryrun_v1 "
            "(not 2.1.29) so this batch is structurally excluded from both mixed-mode "
            "and level-mode admin quiz selection (app/vocabulary_quiz/routers/"
            "multiformat.py SOURCE_VERSION hardcode) until a future deliberate "
            "migration changes it"
        ),
        "literacy_term_id": r["literacy_term_id"],
        "vocab_level": r["vocab_level"],
        "expert_review_status": r["expert_review_status"],
        "l5_grade_caveat": r["l5_grade_caveat"] or None,
        "auto_validation_status": r["auto_validation_status"],
        "auto_validation_issues": r["auto_validation_issues"],
        "semantic_verdict": r["semantic_verdict"],
        "semantic_reason": r["semantic_reason"],
        "hold_fix_suggestion": r["hold_fix_suggestion"] or None,
        "wrong_option_reasons": wrong_reasons,
        "phase16_report": "reports/schema_reading_phase16_quiz_pilot_dryrun_20260925.md",
        "phase17_report": "reports/schema_reading_phase17_quiz_pilot_semantic_review_20260926.md",
    }
    return {
        "item_id": r["item_id"],
        "item_type": r["item_type"],
        "source_content_id": r["source_content_id"],
        "sense_id": None,
        "sense_ids_json": None,
        "source_content_ids_json": None,
        "lemma": r["lemma"],
        "pos": r["pos"],
        "prompt": r["prompt"],
        "options_json": json.dumps(options, ensure_ascii=False),
        "correct_option": int(r["correct_option"]),
        "public_payload_json": json.dumps({"options": options}, ensure_ascii=False),
        "answer_payload_json": json.dumps(answer_payload, ensure_ascii=False),
        "explanation": r["explanation"],
        "cognitive_level": None,
        "qa_flags_json": json.dumps([qa_flag_entry], ensure_ascii=False),
        "generator_version": "schema_reading_phase16_phase17_pilot_v1",
        "source_version": r["source_version"],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.csv, encoding="utf-8-sig")))
    out = [build_row(r) for r in rows]

    assert len(out) == 40, f"기대 40건, 실제 {len(out)}건"
    assert all(r["source_version"] == EXPECTED_SOURCE_VERSION for r in out), "source_version 불일치 행 존재"
    assert len(set(r["item_id"] for r in out)) == len(out), "item_id 중복 존재"

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"{len(out)}건 -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
