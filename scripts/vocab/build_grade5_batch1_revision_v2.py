# -*- coding: utf-8 -*-
"""L3 중등 보강 1차 - 오답 선택지 v2 개정 파일 생성(파일 기반, DB 미반영).

v1 매니페스트(`data/vocab/nikl_grade5_l3_batch1_manifest_v1.json`, 이미
연구 DB에 적재·사용자가 전부 승인한 그 버전)에서 **prompt(문맥 예문
포함)·lemma·pos·explanation은 그대로 두고 options_json·correct_option·
public_payload_json·answer_payload_json·qa_flags_json만** 바꾼다 -
콘텐츠 뜻풀이·예문은 이번에 손대지 않는다(사용자 지시).

item_id도 그대로 유지한다(같은 문항의 "개정판"이라는 뜻 - 새 문항을
만드는 게 아니다). 그래서 이 파일을 나중에 실제 반영하면 기존
`vocabulary_publish_reviews`의 `item_hashes_at_review_json` 스냅샷과
달라져 **기존 승인이 자동으로 '신선도 만료'로 표시된다**(이미 구현된
`review_is_stale()`을 그대로 재사용 - 새 로직을 만들지 않음, 4절 검증
참고). 이번 턴에는 DB에 아무것도 쓰지 않는다 - 전부 파일로만 만든다.

HOLD 처리(`grade5_batch1_distractors_v2.HOLD`): 벨기에는 이번 개정에서
보류 - v1 문항을 그대로 복사하고 qa_flags_json에 보류 사유만 추가한다
(오답을 억지로 새로 만들지 않음)."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"
VOCAB = ROOT / "data" / "vocab"
sys.path.insert(0, str(ROOT / "scripts" / "vocab"))

from grade5_batch1_distractors_v2 import DISTRACTORS_V2, HOLD  # noqa: E402

V1_MANIFEST_PATH = VOCAB / "nikl_grade5_l3_batch1_manifest_v1.json"
V2_MANIFEST_PATH = VOCAB / "nikl_grade5_l3_batch1_manifest_v2_DRAFT.json"
GENERATOR_VERSION_V2 = "nikl_grade5_l3_batch1_distractor_revision_v2"


def has_batchim(ch: str) -> bool:
    code = ord(ch) - 0xAC00
    return 0 <= code <= 11171 and code % 28 != 0


def object_particle(word: str) -> str:
    core = word.rstrip(".") or word
    return "을" if has_batchim(core[-1]) else "를"


def load_v1() -> list[dict]:
    with open(V1_MANIFEST_PATH, encoding="utf-8") as f:
        return json.load(f)


def extract_correct_definition(item: dict) -> str:
    options = json.loads(item["options_json"])
    return options[item["correct_option"] - 1]


def build_v2_item(v1_item: dict, correct_def: str, distractors: list[str],
                   wrong_reasons: list[tuple[str, str]], key_clue: str, position: int) -> dict:
    """position: 정답이 들어갈 1-indexed 자리(1~4) - 단어마다 다르게 배정해
    정답이 항상 같은 번호에 오지 않게 한다."""
    options = distractors[:]
    options.insert(position - 1, correct_def)
    assert len(options) == 4 and len(set(options)) == 4

    item = copy.deepcopy(v1_item)
    item["options_json"] = json.dumps(options, ensure_ascii=False)
    item["correct_option"] = position
    item["public_payload_json"] = json.dumps({"options": options}, ensure_ascii=False)
    item["answer_payload_json"] = json.dumps({"correct_option": position}, ensure_ascii=False)

    old_flags = json.loads(v1_item["qa_flags_json"])[0]
    new_flags = dict(old_flags)
    new_flags["distractor_design_version"] = "v2(2026-10-07) - 의미 분야·추상도 근접 오답으로 전면 재설계, v1(배치 내 무작위 재사용) 폐기"
    new_flags["wrong_option_reasons"] = [
        {"option_text": distractors[i], "confusion_point": wrong_reasons[i][0], "reason": wrong_reasons[i][1]}
        for i in range(3)
    ]
    new_flags["key_clue"] = key_clue
    new_flags["admin_only_note"] = "wrong_option_reasons/key_clue는 검수 화면(관리자)에만 노출 - 응시 화면(제출 전)에는 노출하지 않음"
    item["qa_flags_json"] = json.dumps([new_flags], ensure_ascii=False)
    item["generator_version"] = GENERATOR_VERSION_V2
    return item


def build_held_item(v1_item: dict, hold_reason: str) -> dict:
    item = copy.deepcopy(v1_item)
    old_flags = json.loads(v1_item["qa_flags_json"])[0]
    new_flags = dict(old_flags)
    new_flags["distractor_design_version"] = "v1 그대로 유지(보류) - v2 개정에서 오답 재설계를 보류함"
    new_flags["v2_hold_reason"] = hold_reason
    item["qa_flags_json"] = json.dumps([new_flags], ensure_ascii=False)
    return item


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    v1_items = load_v1()
    by_content: dict[str, list[dict]] = {}
    for it in v1_items:
        by_content.setdefault(it["source_content_id"], []).append(it)

    assert len(by_content) == 30, len(by_content)
    revised_cids = set(DISTRACTORS_V2.keys())
    held_cids = set(HOLD.keys())
    assert revised_cids | held_cids == set(by_content.keys()), (
        revised_cids | held_cids, set(by_content.keys())
    )
    assert len(revised_cids) == 29 and len(held_cids) == 1

    # 정답 위치를 단어마다 1~4로 고르게 분산(고정 순환 - 재현 가능)
    cid_order = sorted(revised_cids)
    position_by_cid = {cid: (i % 4) + 1 for i, cid in enumerate(cid_order)}

    v2_items: list[dict] = []
    comparison_rows = []
    for cid, items in by_content.items():
        for v1_item in items:
            if cid in HOLD:
                v2_item = build_held_item(v1_item, HOLD[cid])
                status = "HELD(v1 유지)"
            else:
                d = DISTRACTORS_V2[cid]
                correct_def = extract_correct_definition(v1_item)
                v2_item = build_v2_item(
                    v1_item, correct_def, d["distractors"], d["wrong_reasons"],
                    d["key_clue"], position_by_cid[cid],
                )
                status = "REVISED"
            v2_items.append(v2_item)

            comparison_rows.append({
                "item_id": v1_item["item_id"], "item_type": v1_item["item_type"],
                "lemma": v1_item["lemma"], "status": status,
                "v1_options": v1_item["options_json"], "v1_correct_option": v1_item["correct_option"],
                "v2_options": v2_item["options_json"], "v2_correct_option": v2_item["correct_option"],
                "v1_prompt": v1_item["prompt"], "v2_prompt": v2_item["prompt"],
            })

    assert len(v2_items) == 60

    with open(V2_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(v2_items, f, ensure_ascii=False, indent=2)
    print(f"v2 개정 매니페스트(DRAFT, 60건 - 58 REVISED + 2 HELD) 저장: {V2_MANIFEST_PATH}")

    comp_path = IMP / "nikl_grade5_l3_batch1_distractor_v1_v2_comparison_20261007.csv"
    import csv
    with open(comp_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(comparison_rows[0].keys()))
        w.writeheader()
        w.writerows(comparison_rows)
    print(f"v1/v2 비교표 저장: {comp_path} ({len(comparison_rows)}행)")


if __name__ == "__main__":
    main()
