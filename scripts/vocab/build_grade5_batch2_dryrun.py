# -*- coding: utf-8 -*-
"""L3 중등 보강 2차(잔여 40건 중 38건) - 파일 기반 dry-run 산출물 생성.

**DB에 쓰지 않는다** - 전부 로컬 CSV/JSON 파일로만 만든다. 1차
(build_grade5_batch1_dryrun.py)와 같은 작업 분리: 콘텐츠·오답은 사람이
직접 작성해 둔 데이터 파일을 그대로 쓰고, 이 스크립트는 (1) 원본 데이터와의
교차 검증, (2) 1차 배치·기존 DB와의 중복 확인, (3) 문항 생성(1차 v2와
동일 포맷 + 처음부터 v2 오답 설계 기준 적용), (4) 산출물 파일 쓰기만 한다.

문항 생성 규칙(1차와 동일, build_grade5_batch1_dryrun.py 재사용):
  - MEANING_CHOICE: prompt="'{lemma}'의 뜻으로 가장 알맞은 것은?"
  - CONTEXT_MEANING: prompt="다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은
    것은?\n\n{예문, 낱말은 【...】로 표시}"
  - 오답 3개는 **단어마다 사람이 직접 지어낸 것**
    (grade5_batch2_distractors.py) - 1차 v1의 오프셋 재사용 방식은 쓰지
    않는다.
  - 정답 위치는 (인덱스 % 4)+1로 고정 배정.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"
VOCAB = ROOT / "data" / "vocab"
sys.path.insert(0, str(ROOT / "scripts" / "vocab"))

from grade5_batch2_content import ITEMS, HOLD  # noqa: E402
from grade5_batch2_distractors import DISTRACTORS_V1  # noqa: E402

SOURCE_VERSION = "nikl_grade5_l3_batch2_v1"
GENERATOR_VERSION = "nikl_grade5_l3_batch2_dryrun_v1"


def has_batchim(ch: str) -> bool:
    code = ord(ch) - 0xAC00
    if not (0 <= code <= 11171):
        return False
    return code % 28 != 0


def object_particle(word: str) -> str:
    core = word.rstrip(".") or word
    return "을" if has_batchim(core[-1]) else "를"


def topic_particle(word: str) -> str:
    return "은" if has_batchim(word[-1]) else "는"


def predicate_particle(word: str) -> str:
    core = word.rstrip(".") or word
    return "이라는" if has_batchim(core[-1]) else "라는"


def load_source_rows() -> dict[str, dict]:
    with open(IMP / "nikl_grade5_200_full_refresh_20261007.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return {r["candidate_id"]: r for r in rows}


def load_batch1_lemmas() -> set[str]:
    with open(IMP / "nikl_grade5_l3_batch1_final_content_20261007.csv", encoding="utf-8-sig", newline="") as f:
        return {r["lemma"] for r in csv.DictReader(f)}


def load_batch1_ids() -> set[str]:
    with open(IMP / "nikl_grade5_l3_batch1_final_content_20261007.csv", encoding="utf-8-sig", newline="") as f:
        return {r["candidate_id"] for r in csv.DictReader(f)}


def cross_check(items: list[dict], source: dict[str, dict], batch1_lemmas: set[str], batch1_ids: set[str]) -> None:
    for it in items:
        src = source.get(it["candidate_id"])
        assert src is not None, f"candidate_id 없음: {it['candidate_id']}"
        assert src["lemma"] == it["lemma"], f"lemma 불일치: {it['candidate_id']}"
        assert src["human_judgment"] == "L3", f"L3 아님: {it['candidate_id']}"
        assert it["candidate_id"] not in batch1_ids, f"1차 배치와 candidate_id 중복: {it['candidate_id']}"
        assert it["lemma"] not in batch1_lemmas, f"1차 배치와 표제어(의미 단위) 중복: {it['lemma']}"
        it["pos_src"] = src["pos"]
        it["homonym_number"] = src["homonym_number"]
        it["official_meaning_short"] = src["official_meaning_short"]
        it["specialized_domain_flag"] = src["specialized_domain_flag"]
        it["batch_no"] = src["batch_no"]
        it["reviewed_at"] = src["reviewed_at"]
    print(f"교차 검증 PASS: {len(items)}건 전부 candidate_id/lemma/L3 판정 일치, 1차 배치와 중복 0건")


def build_prompts(lemma: str, example: str, context_form: str | None) -> tuple[str, str]:
    meaning_prompt = f"'{lemma}'의 뜻으로 가장 알맞은 것은?"
    marker = context_form or lemma
    assert marker in example, f"예문에 context_form/lemma가 없음: {lemma} / {example} / marker={marker}"
    bracketed = example.replace(marker, f"【{marker}】", 1)
    context_prompt = f"다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은 것은?\n\n{bracketed}"
    return meaning_prompt, context_prompt


def build_options(correct: str, distractors: list[str], position: int) -> list[str]:
    assert len(distractors) == 3
    options = distractors[:]
    options.insert(position - 1, correct)
    assert len(options) == 4 and len(set(options)) == 4, "옵션 4개가 모두 달라야 함(정답 중복 금지)"
    return options


def build_qa_flags(it: dict, d: dict) -> list[dict]:
    return [{
        "flag": "NIKL_GRADE5_L3_BATCH2_ADMIN_PREVIEW",
        "source_version_isolation": (
            "source_version deliberately set to nikl_grade5_l3_batch2_v1 (not "
            "2.1.29, not nikl_grade5_l3_batch1_v1, not any existing pilot "
            "source_version) so this batch is structurally excluded from "
            "general/level-mode selection and every other batch/pilot - admin "
            "preview only"
        ),
        "human_level_judgment": "L3",
        "human_level_judgment_source": "vocabulary_grade5_candidate_judgments(최신 유효 판정)",
        "content_review_status": "검수 전(미승인) - vocabulary_publish_reviews류 별도 테이블에만 기록되며 이 레벨 판정과 무관",
        "expert_review_status": "검수자 확인 안 됨(자동 1차 초안 - 사람이 직접 작성했지만 공식 승인 절차 거치지 않음)",
        "auto_validation_status": it["auto_validation_status"],
        "auto_validation_notes": it["auto_validation_notes"],
        "auto_validation_report": "reports/nikl_grade5_l3_batch2_dryrun_20261007.md 참고",
        "generator_version": GENERATOR_VERSION,
        "distractor_design_version": (
            "v2 기준 처음부터 적용(2026-10-07) - 1차 v2에서 검수된 의미 분야 "
            "근접 오답 설계 기준을 신규 작성 시점부터 적용, 배치 내 무작위 "
            "재사용(v1 방식) 미사용"
        ),
        "wrong_option_reasons": [
            {"option_text": d["distractors"][i], "confusion_point": d["wrong_reasons"][i][0], "reason": d["wrong_reasons"][i][1]}
            for i in range(3)
        ],
        "key_clue": d["key_clue"],
        "admin_only_note": "wrong_option_reasons/key_clue는 검수 화면(관리자)에만 노출 - 응시 화면(제출 전)에는 노출하지 않음",
    }]


def build_held_record(cid: str, lemma: str, src: dict, reason: str) -> dict:
    return {
        "candidate_id": cid,
        "lemma": lemma,
        "pos": src["pos"],
        "homonym_number": src["homonym_number"],
        "official_meaning_short": src["official_meaning_short"],
        "hold_reason": reason,
    }


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    source = load_source_rows()
    batch1_lemmas = load_batch1_lemmas()
    batch1_ids = load_batch1_ids()

    cross_check(ITEMS, source, batch1_lemmas, batch1_ids)

    # HOLD 2건도 원천 데이터와 교차 검증(L3 판정·1차 배치 비중복)
    held_rows = []
    for cid, reason in HOLD.items():
        src = source[cid]
        assert src["human_judgment"] == "L3"
        assert cid not in batch1_ids
        assert src["lemma"] not in batch1_lemmas
        held_rows.append(build_held_record(cid, src["lemma"], src, reason))
    print(f"HOLD 교차 검증 PASS: {len(held_rows)}건")

    assert set(DISTRACTORS_V1.keys()) == {it["candidate_id"] for it in ITEMS}, "오답 데이터와 콘텐츠 candidate_id 집합 불일치"

    cid_order = sorted(it["candidate_id"] for it in ITEMS)
    position_by_cid = {cid: (i % 4) + 1 for i, cid in enumerate(cid_order)}

    manifest_items: list[dict] = []
    review_rows: list[dict] = []

    for it in ITEMS:
        cid = it["candidate_id"]
        d = DISTRACTORS_V1[cid]
        position = position_by_cid[cid]

        # --- 생성과 분리된 QA(기계적으로 점검 가능한 항목만 자동 검사) ---
        notes = []
        status = "PASS"

        all_texts = [it["student_definition"]] + d["distractors"]
        if len(set(all_texts)) != 4:
            status = "FAIL"
            notes.append("정답/오답 중 중복 텍스트 존재(정답 유일성 위반)")

        lengths = [len(t) for t in all_texts]
        if max(lengths) > min(lengths) * 3:
            notes.append(f"보기 길이 편차 큼(최소 {min(lengths)}자/최대 {max(lengths)}자) - 수동 확인")

        marker = it.get("context_form") or it["lemma"]
        if marker not in it["example"]:
            status = "FAIL"
            notes.append("예문에 표제어/활용형이 없음")

        if it["student_definition"] in d["distractors"]:
            status = "FAIL"
            notes.append("정답이 오답 목록에도 포함됨")

        it["auto_validation_status"] = status
        it["auto_validation_notes"] = "; ".join(notes) if notes else "이상 없음"

        meaning_prompt, context_prompt = build_prompts(it["lemma"], it["example"], it.get("context_form"))
        options = build_options(it["student_definition"], d["distractors"], position)
        correct_option = position

        lemma = it["lemma"]
        tp = topic_particle(lemma)
        pp = predicate_particle(it["student_definition"])
        op = object_particle(it["student_definition"])
        explanation_meaning = f"'{lemma}'{tp} '{it['student_definition']}'{pp} 뜻입니다."
        explanation_context = f"문장 속 '{lemma}'{tp} '{it['student_definition']}'{op} 뜻합니다."

        qa_flags = build_qa_flags(it, d)

        base = dict(
            source_content_id=cid,
            source_content_ids_json=None,
            sense_id=None,
            sense_ids_json=None,
            lemma=it["lemma"],
            pos=it["pos"],
            options_json=json.dumps(options, ensure_ascii=False),
            correct_option=correct_option,
            public_payload_json=json.dumps({"options": options}, ensure_ascii=False),
            answer_payload_json=json.dumps({"correct_option": correct_option}, ensure_ascii=False),
            cognitive_level=None,
            qa_flags_json=json.dumps(qa_flags, ensure_ascii=False),
            generator_version=GENERATOR_VERSION,
            source_version=SOURCE_VERSION,
        )

        meaning_item = dict(base, item_id=f"MF_G5L3B2_M_{cid}", item_type="MEANING_CHOICE",
                             prompt=meaning_prompt, explanation=explanation_meaning)
        context_item = dict(base, item_id=f"MF_G5L3B2_C_{cid}", item_type="CONTEXT_MEANING",
                             prompt=context_prompt, explanation=explanation_context)
        manifest_items.append(meaning_item)
        manifest_items.append(context_item)

        review_rows.append({
            "candidate_id": cid,
            "lemma": it["lemma"],
            "pos": it["pos"],
            "homonym_number": it["homonym_number"],
            "official_meaning_short": it["official_meaning_short"],
            "student_definition": it["student_definition"],
            "example": it["example"],
            "context_form": it.get("context_form") or "",
            "options": " / ".join(options),
            "correct_option": correct_option,
            "key_clue": d["key_clue"],
            "auto_validation_status": status,
            "auto_validation_notes": it["auto_validation_notes"],
        })

    fail_count = sum(1 for r in review_rows if r["auto_validation_status"] == "FAIL")
    print(f"자동 검증: PASS {len(review_rows) - fail_count}건 / FAIL {fail_count}건")
    assert fail_count == 0, "자동 검증 FAIL 존재 - 콘텐츠 수정 필요"

    assert len(manifest_items) == 76, len(manifest_items)

    manifest_path = VOCAB / "nikl_grade5_l3_batch2_manifest_v1_DRAFT.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_items, f, ensure_ascii=False, indent=2)
    print(f"매니페스트(DRAFT, {len(manifest_items)}건 - 38어휘×2유형) 저장: {manifest_path}")

    content_csv = IMP / "nikl_grade5_batch2_38_content_dryrun_20261007.csv"
    with open(content_csv, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["candidate_id", "lemma", "pos", "homonym_number", "batch_no",
                      "official_meaning_short", "student_definition", "example_sentence",
                      "human_level_judgment", "human_reviewed_at"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in ITEMS:
            w.writerow({
                "candidate_id": it["candidate_id"], "lemma": it["lemma"], "pos": it["pos"],
                "homonym_number": it["homonym_number"], "batch_no": it["batch_no"],
                "official_meaning_short": it["official_meaning_short"],
                "student_definition": it["student_definition"], "example_sentence": it["example"],
                "human_level_judgment": "L3", "human_reviewed_at": it["reviewed_at"],
            })
    print(f"콘텐츠 CSV 저장: {content_csv}")

    items_csv = IMP / "nikl_grade5_batch2_38_items_dryrun_20261007.csv"
    with open(items_csv, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = list(manifest_items[0].keys())
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(manifest_items)
    print(f"문항 CSV 저장: {items_csv}")

    review_csv = IMP / "nikl_grade5_batch2_38_full_review_table_20261007.csv"
    with open(review_csv, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = list(review_rows[0].keys())
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(review_rows)
    print(f"전수 검토표 저장: {review_csv}")

    held_csv = IMP / "nikl_grade5_batch2_held_2_20261007.csv"
    with open(held_csv, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = list(held_rows[0].keys())
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(held_rows)
    print(f"보류 2건 저장: {held_csv}")

    print()
    print(f"최종 집계: 작성 38어휘(76문항), 보류 2건(아멘, 파키스탄), 합계 40 = 잔여 40건과 일치")


if __name__ == "__main__":
    main()
