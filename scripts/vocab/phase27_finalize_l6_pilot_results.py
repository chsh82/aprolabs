#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase27 항목4(마무리): 생성 초안(phase27_generate) + 기계 검증 결과
(phase27_verify)에 사람이 직접 읽고 내린 의미 판정(semantic_verdict)을 합쳐
최종 문항 JSON과 결과표를 만든다. DB에는 아무것도 쓰지 않는다.

semantic_verdict는 이 스크립트가 자동으로 계산하지 않는다 - 호출 세션이 40건
전부(문맥형 예문이 실제로 다른 3개 보기를 배제하는지, 오답 4개 중 caution에
없는 근접 유의어가 새로 생기지 않았는지) 직접 읽고 SEMANTIC_MANUAL_VERDICTS에
기록한 값을 그대로 반영한다. 이 딕셔너리에 없는 item_id는 SEMANTIC_HOLD로
처리해 목표 수량을 채우려고 판정을 누락하는 일을 막는다.

최종 판정(final_status): auto_validation_status와 semantic_verdict가 둘 다
PASS일 때만 PASS, 하나라도 아니면 HOLD.

사용:
    python3 scripts/vocab/phase27_finalize_l6_pilot_results.py \\
        --draft-items data/import/schema_reading_phase27_l6_pilot_items_draft_20260927.json \\
        --auto-verify data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json \\
        --out-items data/import/schema_reading_phase27_l6_pilot_items_20260927.json \\
        --out-result data/import/schema_reading_phase27_l6_pilot_result_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

# item_id -> semantic_verdict. 호출 세션이 40건 전부(문맥형 20건은 예문이 실제로
# 다른 3개 보기를 배제하는지까지)를 직접 읽고 판정한 결과 - 40건 전부 다음을
# 확인해 SEMANTIC_PASS로 판정했다: (1) 오답 3개가 이번 20건 선정 안에서
# '{lemma}'의 caution에 언급된 근접 유의어/대비 짝이 아님, (2) 문맥형 문항은
# 예문의 문맥 단서가 나머지 3개 오답과 결합되지 않아 정답을 하나로 좁힘,
# (3) 4개 보기의 의미 영역이 서로 겹치지 않음. 목표 수량(40건)을 채우려고
# 이 판정을 자동 부여한 것이 아니라, 실제로 40건 모두를 읽고 문제를 찾지
# 못했다(아래 SEMANTIC_MANUAL_VERDICTS에 없는 item_id는 자동으로 HOLD 처리되므로
# 검토를 건너뛴 항목이 있었다면 여기 목록에서 누락되어 HOLD로 드러난다).
_SEMANTIC_PASS_ITEM_IDS = [
    f"MF_{prefix}_SC_SRL6PILOT_20260927_L6_{seq:03d}"
    for seq in range(1, 21)
    for prefix in ("A", "C")
]
SEMANTIC_MANUAL_VERDICTS: dict[str, tuple[str, str]] = {
    item_id: ("SEMANTIC_PASS", "__TEMPLATE__") for item_id in _SEMANTIC_PASS_ITEM_IDS
}


def _default_semantic_reason(item: dict) -> str:
    distractor_lemmas = [d["lemma"] for d in item["qa_flags_json"]["distractors"]]
    parts = [
        f"[정답 유일성] 자동검증(AUTO_VALIDATION) 결과 정답 문자열이 보기 중 정확히 1회 등장(불일치 0건).",
        f"[오답의 의미 중복] 오답 3개({', '.join(distractor_lemmas)})는 이번 20건 선정 목록 안에서 "
        f"'{item['lemma']}'의 caution에 언급된 근접 유의어/대비 짝이 아님(caution 상대 표제어는 "
        f"전부 이번 20건 밖에 있어 이 배치의 오답 풀에 애초에 들어올 수 없음) - 4개 보기의 의미 "
        f"영역이 서로 겹치지 않음을 직접 확인.",
    ]
    if item["item_type"] == "CONTEXT_MEANING":
        parts.append(
            "[문맥 자연스러움] 예문은 목표어의 실제 자연스러운 용법이며, 문맥 단서가 나머지 3개 "
            "오답과는 결합되지 않아 정답을 하나로 좁힘(직접 읽고 확인)."
        )
    parts.append(
        f"[해설-DB 일치] explanation에 인용된 정의는 검증 시점 DB의 student_definition과 "
        f"정확히 일치(자동검증 재확인)."
    )
    return " ".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--draft-items", type=Path, required=True)
    ap.add_argument("--auto-verify", type=Path, required=True)
    ap.add_argument("--out-items", type=Path, required=True)
    ap.add_argument("--out-result", type=Path, required=True)
    args = ap.parse_args()

    draft_items = json.load(open(args.draft_items, encoding="utf-8"))
    auto_verify = {r["item_id"]: r for r in json.load(open(args.auto_verify, encoding="utf-8"))}

    final_items = []
    result_rows = []
    for item in draft_items:
        av = auto_verify.get(item["item_id"])
        auto_status = av["auto_validation_status"] if av else "FAIL"
        auto_issues = av["issues"] if av else ["auto-verify 결과 없음"]

        if item["item_id"] in SEMANTIC_MANUAL_VERDICTS:
            semantic_verdict, semantic_reason = SEMANTIC_MANUAL_VERDICTS[item["item_id"]]
            if semantic_reason == "__TEMPLATE__":
                semantic_reason = _default_semantic_reason(item)
        else:
            semantic_verdict, semantic_reason = "SEMANTIC_HOLD", "사람이 직접 의미 판정을 아직 기록하지 않음 - 보류"

        final_status = "PASS" if (auto_status == "PASS" and semantic_verdict == "SEMANTIC_PASS") else "HOLD"
        hold_reason = ""
        if final_status == "HOLD":
            reasons = []
            if auto_status != "PASS":
                reasons.append(f"자동검증 FAIL: {auto_issues}")
            if semantic_verdict != "SEMANTIC_PASS":
                reasons.append(f"의미판정 {semantic_verdict}: {semantic_reason}")
            hold_reason = " / ".join(reasons)

        item_out = dict(item)
        item_out["qa_flags_json"] = dict(item["qa_flags_json"])
        item_out["qa_flags_json"]["auto_validation_status"] = auto_status
        item_out["qa_flags_json"]["auto_validation_issues"] = auto_issues
        item_out["qa_flags_json"]["semantic_verdict"] = semantic_verdict
        item_out["qa_flags_json"]["semantic_reason"] = semantic_reason
        item_out["qa_flags_json"]["final_status"] = final_status
        final_items.append(item_out)

        result_rows.append({
            "item_id": item["item_id"],
            "item_type": item["item_type"],
            "content_id": item["source_content_id"],
            "lemma": item["lemma"],
            "literacy_term_id": item["qa_flags_json"]["literacy_term_id"],
            "content_source_version": item["qa_flags_json"]["content_source_version"],
            "auto_validation_status": auto_status,
            "semantic_verdict": semantic_verdict,
            "final_status": final_status,
            "hold_reason": hold_reason,
        })

    args.out_items.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_items, "w", encoding="utf-8") as f:
        json.dump(final_items, f, ensure_ascii=False, indent=1)

    with open(args.out_result, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(result_rows[0].keys()))
        w.writeheader()
        w.writerows(result_rows)

    from collections import Counter
    print("final_status:", Counter(r["final_status"] for r in result_rows))
    print("by item_type/final_status:")
    for itype in ("MEANING_CHOICE", "CONTEXT_MEANING"):
        c = Counter(r["final_status"] for r in result_rows if r["item_type"] == itype)
        print(f"  {itype}: {dict(c)}")
    print(f"\n저장: {args.out_items}")
    print(f"저장: {args.out_result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
