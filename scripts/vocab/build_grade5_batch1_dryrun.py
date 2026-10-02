# -*- coding: utf-8 -*-
"""공식 5등급(L3) 1차 보강 배치 - 파일 기반 dry-run 산출물 생성.

**DB에 쓰지 않는다** - 전부 로컬 CSV/JSON 파일로만 만든다. 콘텐츠(학생용
뜻풀이·예문)는 `grade5_batch1_content.py`에 사람이 직접 작성해 둔 것을
그대로 쓰고, 이 스크립트는 (1) 원본 데이터와의 교차 검증, (2) 문항
생성(기존 production 문항과 동일한 포맷), (3) 산출물 파일 쓰기만 한다.

문항 생성 규칙(기존 2.1.29 풀의 실제 MEANING_CHOICE/CONTEXT_MEANING 샘플과
동일한 포맷을 그대로 재현 - 새 포맷을 만들지 않음):
  - MEANING_CHOICE: prompt="'{lemma}'의 뜻으로 가장 알맞은 것은?"
  - CONTEXT_MEANING: prompt="다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은
    것은?\n\n{예문, 낱말은 【...】로 표시}"
  - 두 유형 모두 같은 4지선다 옵션 집합(정답 1개 + 다른 배치 단어 3개의
    학생용 뜻풀이)을 공유한다(기존 production 데이터의 실제 관행과 동일 -
    '의좋다'/'이를테면' 샘플에서 확인).
  - 오답(distractor) 3개는 **배치 내 다른 어휘의 학생용 뜻풀이**를 그대로
    쓴다(새로 지어내지 않음) - 인덱스 기준 +7/+13/+19(mod 30) 오프셋으로
    고정 배정해 재현 가능하게 한다.
  - 정답 위치는 (인덱스 % 4)+1로 고정 배정 - 항상 같은 자리에 정답이
    오지 않게 분산한다.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"
sys.path.insert(0, str(ROOT / "scripts" / "vocab"))

from grade5_batch1_content import ITEMS  # noqa: E402

DISTRACTOR_OFFSETS = (7, 13, 19)


def has_batchim(ch: str) -> bool:
    code = ord(ch) - 0xAC00
    if not (0 <= code <= 11171):
        return False
    return code % 28 != 0


def topic_particle(word: str) -> str:
    """은/는 - 받침 있으면 은, 없으면 는."""
    return "은" if has_batchim(word[-1]) else "는"


def object_particle(word: str) -> str:
    """을/를 - 끝의 '.'은 무시하고 마지막 글자의 받침을 본다."""
    core = word.rstrip(".") or word
    return "을" if has_batchim(core[-1]) else "를"


def predicate_particle(word: str) -> str:
    """(이)라는 - 받침 있으면 이라는, 없으면 라는."""
    core = word.rstrip(".") or word
    return "이라는" if has_batchim(core[-1]) else "라는"


def load_source_rows() -> dict[str, dict]:
    with open(IMP / "nikl_grade5_200_full_refresh_20261007.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return {r["candidate_id"]: r for r in rows}


def cross_check(items: list[dict], source: dict[str, dict]) -> None:
    for it in items:
        src = source.get(it["candidate_id"])
        assert src is not None, f"candidate_id 없음: {it['candidate_id']}"
        assert src["lemma"] == it["lemma"], f"lemma 불일치: {it['candidate_id']}"
        assert src["human_judgment"] == "L3", f"L3 아님: {it['candidate_id']}"
        it["pos_src"] = src["pos"]
        it["homonym_number"] = src["homonym_number"]
        it["official_meaning_short"] = src["official_meaning_short"]
        it["specialized_domain_flag"] = src["specialized_domain_flag"]
        it["batch_no"] = src["batch_no"]
        it["reviewed_at"] = src["reviewed_at"]
    print(f"교차 검증 PASS: {len(items)}건 전부 candidate_id/lemma/L3 판정 일치")


def build_options(items: list[dict], i: int) -> tuple[list[str], int]:
    correct = items[i]["student_definition"]
    distractors = [items[(i + off) % len(items)]["student_definition"] for off in DISTRACTOR_OFFSETS]
    correct_pos = (i % 4)  # 0-indexed slot for correct answer
    options = distractors[:]
    options.insert(correct_pos, correct)
    return options, correct_pos + 1


def build_items(items: list[dict]) -> list[dict]:
    generated = []
    for i, it in enumerate(items):
        options, correct_option = build_options(items, i)
        cid = it["candidate_id"]
        lemma = it["lemma"]

        mc_prompt = f"'{lemma}'의 뜻으로 가장 알맞은 것은?"
        definition = it["student_definition"]
        generated.append({
            "item_id": f"MF_DRY_A_{cid}", "item_type": "MEANING_CHOICE",
            "source_content_id": cid, "lemma": lemma, "pos": it["pos"],
            "prompt": mc_prompt, "options": options, "correct_option": correct_option,
            "explanation": f"'{lemma}'{topic_particle(lemma)} '{definition}'{predicate_particle(definition)} 뜻입니다.",
        })

        surface_form = it.get("context_form", lemma)
        if surface_form not in it["example"]:
            raise RuntimeError(f"예문에 표시할 표면형이 없음: {cid} {lemma!r} surface={surface_form!r} / {it['example']!r}")
        context_sentence = it["example"].replace(surface_form, f"【{surface_form}】", 1)
        cm_prompt = f"다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은 것은?\n\n{context_sentence}"
        generated.append({
            "item_id": f"MF_DRY_C_{cid}", "item_type": "CONTEXT_MEANING",
            "source_content_id": cid, "lemma": lemma, "pos": it["pos"],
            "prompt": cm_prompt, "options": options, "correct_option": correct_option,
            "explanation": f"문장 속 '{surface_form}'{topic_particle(surface_form)} '{definition}'{object_particle(definition)} 뜻합니다.",
        })
    return generated


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    source = load_source_rows()
    cross_check(ITEMS, source)

    content_path = IMP / "nikl_grade5_batch1_30_content_dryrun_20261007.csv"
    with open(content_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["candidate_id", "lemma", "pos", "homonym_number", "batch_no",
                    "official_meaning_short", "student_definition", "example_sentence",
                    "human_level_judgment", "human_reviewed_at"])
        for it in ITEMS:
            w.writerow([it["candidate_id"], it["lemma"], it["pos_src"], it["homonym_number"],
                        it["batch_no"], it["official_meaning_short"], it["student_definition"],
                        it["example"], "L3", it["reviewed_at"]])
    print(f"콘텐츠 dry-run 저장: {content_path} ({len(ITEMS)}건)")

    generated_items = build_items(ITEMS)
    items_path = IMP / "nikl_grade5_batch1_30_items_dryrun_20261007.csv"
    with open(items_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["item_id", "item_type", "source_content_id", "lemma", "pos",
                    "prompt", "options_json", "correct_option", "explanation"])
        for g in generated_items:
            w.writerow([g["item_id"], g["item_type"], g["source_content_id"], g["lemma"], g["pos"],
                        g["prompt"], json.dumps(g["options"], ensure_ascii=False), g["correct_option"],
                        g["explanation"]])
    print(f"문항 dry-run 저장: {items_path} ({len(generated_items)}건 - 30단어 x 2유형)")

    return generated_items


if __name__ == "__main__":
    main()
