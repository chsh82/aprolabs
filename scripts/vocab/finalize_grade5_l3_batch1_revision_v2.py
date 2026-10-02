# -*- coding: utf-8 -*-
"""L3 중등 보강 1차 오답 개정 v2 - 최종 반영 대상을 고정한다(파일 작업만,
DB는 아직 안 씀 - 실제 DB 반영은 apply_grade5_l3_batch1_distractor_
revision_v2.py가 별도로 한다).

**새 버전 item_id를 쓰는 이유(사용자 지시 확인)**: `VocabularyMultiformat
Response.item_id`는 `vocabulary_multiformat_items.item_id`를 FK로 참조하고,
결과 조회(`/sessions/{id}/result`)는 그 FK로 **매번 현재 문항을 다시
읽는다**(프롬프트·보기·정답을 세션에 스냅샷으로 저장하지 않음, 직접
코드 확인). 그래서 기존 item_id를 그대로 두고 보기만 바꾸면, 이미 끝난
과거 응시의 결과 화면이 조용히 달라진다 - 그래서 **바꾸는 29단어·58문항은
새 item_id(접미사 _V2)로 등록**하고, 기존 item_id는 **삭제하지 않고
is_active=0으로만 비활성화**한다(과거 응답이 참조할 FK 대상 자체는 항상
존재해야 하므로 삭제는 원천적으로 불가능 - FK 제약이 이미 이를 강제함).
현재 이 배치로 만들어진 세션·응답은 0건(라이브 재확인, 2026-10-07) -
그래도 구조상 올바른 설계를 적용한다(지금 0건이라고 설계를 생략하지
않음).

**벨기에 2문항**: 이번 개정에서 제외·보류 - item_id·내용·is_active 전부
**그대로 둔다**(건드리지 않음). 대신 매니페스트(활성 화이트리스트)에서는
빠지므로 `_select_grade5_l3_batch1_item_ids()`의 기본 후보 풀에 들어오지
않는다(관리자 기본 응시가 58문항만 선택되는 이유) - 콘텐츠 자체와 기존
승인은 전혀 건드리지 않으므로 그 쪽 신선도는 그대로 "유효" 상태를
유지한다(검수 화면에서는 '보류'로 별도 표시)."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VOCAB = ROOT / "data" / "vocab"
sys.path.insert(0, str(ROOT / "scripts" / "vocab"))

from grade5_batch1_distractors_v2 import DISTRACTORS_V2, HOLD  # noqa: E402

V1_ORIGINAL_PATH = VOCAB / "nikl_grade5_l3_batch1_manifest_v1_ORIGINAL_60_20261007.json"  # 감사용 원본(이미 저장됨)
V2_DRAFT_PATH = VOCAB / "nikl_grade5_l3_batch1_manifest_v2_DRAFT.json"  # 이전 턴 산출물(60건, item_id 변경 없음)
ACTIVE_MANIFEST_PATH = VOCAB / "nikl_grade5_l3_batch1_manifest_v1.json"  # 실제 서버가 읽는 경로(덮어씀)

GENERATOR_VERSION_V2 = "nikl_grade5_l3_batch1_distractor_revision_v2"
NEW_ITEM_ID_SUFFIX = "_V2"


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    assert V1_ORIGINAL_PATH.is_file(), "원본 60건 매니페스트 스냅샷이 없습니다 - 먼저 보존해야 함"
    with open(V1_ORIGINAL_PATH, encoding="utf-8") as f:
        v1_original = json.load(f)
    with open(V2_DRAFT_PATH, encoding="utf-8") as f:
        v2_draft = json.load(f)
    assert len(v1_original) == 60 and len(v2_draft) == 60

    v2_by_id = {it["item_id"]: it for it in v2_draft}
    revised_cids = set(DISTRACTORS_V2.keys())
    held_cids = set(HOLD.keys())
    assert len(revised_cids) == 29 and len(held_cids) == 1

    new_active_items = []  # 58건 - 새 item_id, 새로 INSERT될 행
    old_items_to_deactivate = []  # 58건 - 기존 item_id, is_active=0으로 UPDATE될 행
    held_items_unchanged = []  # 2건 - 아무 것도 안 바뀜(기록용)

    for v1_item in v1_original:
        cid = v1_item["source_content_id"]
        if cid in held_cids:
            held_items_unchanged.append(v1_item)
            continue
        assert cid in revised_cids, cid
        v2_item = copy.deepcopy(v2_by_id[v1_item["item_id"]])
        v2_item["item_id"] = v1_item["item_id"] + NEW_ITEM_ID_SUFFIX
        v2_item["generator_version"] = GENERATOR_VERSION_V2
        flags = json.loads(v2_item["qa_flags_json"])[0]
        flags["revised_from_item_id"] = v1_item["item_id"]
        flags["revision_reason"] = "오답 선택지 v2 전면 재설계(사용자 피드백 - v1 오답이 정답과 무관해 의미를 몰라도 맞힐 수 있었음)"
        v2_item["qa_flags_json"] = json.dumps([flags], ensure_ascii=False)
        new_active_items.append(v2_item)
        old_items_to_deactivate.append(v1_item["item_id"])

    assert len(new_active_items) == 58
    assert len(old_items_to_deactivate) == 58
    assert len(held_items_unchanged) == 2

    # 실제 서버가 읽는 매니페스트(활성 화이트리스트)를 58건으로 교체한다 -
    # 벨기에 2건은 포함하지 않는다(기본 응시 풀에서 제외, 보류).
    with open(ACTIVE_MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(new_active_items, f, ensure_ascii=False, indent=2)
    print(f"활성 매니페스트 교체: {ACTIVE_MANIFEST_PATH} ({len(new_active_items)}건, 벨기에 2건 제외)")

    # apply 스크립트가 바로 쓸 수 있게 "교체 계획"도 별도 파일로 남긴다.
    plan = {
        "new_active_items": new_active_items,
        "old_item_ids_to_deactivate": old_items_to_deactivate,
        "held_item_ids_unchanged": [it["item_id"] for it in held_items_unchanged],
    }
    plan_path = ROOT / "data" / "import" / "nikl_grade5_l3_batch1_revision_v2_apply_plan_20261007.json"
    with open(plan_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    print(f"적용 계획 저장: {plan_path}")
    print(f"  신규 활성 문항(INSERT 대상): {len(new_active_items)}건")
    print(f"  비활성화 대상(UPDATE is_active=0): {len(old_items_to_deactivate)}건")
    print(f"  변경 없음(벨기에, 보류): {len(held_items_unchanged)}건")


if __name__ == "__main__":
    main()
