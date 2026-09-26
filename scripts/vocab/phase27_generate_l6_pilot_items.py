#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase27 항목2/3: 선정된 L6 콘텐츠(최대 20건)에 뜻 고르기(MEANING_CHOICE) 1문항 +
문맥형(CONTEXT_MEANING) 1문항씩(최대 40문항) 초안을 생성한다. 읽기 전용(SELECT만) -
DB에 아무것도 쓰지 않는다.

정의 문자열을 스크립트에 하드코딩하지 않는다 - 이 스크립트를 실행할 때마다
vocabulary_contents.student_definition/canonical_definition/example_sentence를
DB에서 직접 다시 읽는다(선정 단계 JSON에는 caution/literacy_term_id 등 메타데이터만
있고 정의 문자열은 없음 - 있어도 이 스크립트는 그 값을 쓰지 않고 DB를 재조회한다).

오답(distractor) 선정: 같은 배치로 선정된 다른 19건 중에서만 고른다(외부 콘텐츠를
끌어오지 않아 파일 하나로 완결됨). 각 대상의 caution에 등장하는 다른 선정 콘텐츠의
표제어가 있으면 그 표제어는 이 대상의 오답 후보에서 제외한다(뜻이 근접한 어휘가
같은 문항의 정답 후보로 겹치는 것을 생성 단계에서부터 예방) - 이번 선정 20건은
서로 caution으로 연결된 짝이 없어(각 caution의 상대 표제어가 전부 20건 밖에 있음)
실제로 제외되는 사례는 0건이지만, 로직 자체는 항상 적용한다.

문항마다 기록하는 것: item_id, content_id, literacy_term_id, source_version
(콘텐츠 원본 + 이 문항 배치 자체의 source_version), 정답 설명(explanation),
오답별 이유(wrong_option_reasons), caution, 'L6=고2~3'이 grade_level 개별 태그가
아니라 정책 매핑(docs/literacy/07-학년경계정책-L5L6.md)에 따른 잠정 근거라는
설명(l6_grade_caveat).

이 스크립트는 초안(draft)만 만든다 - 정답 유일성/오답 의미 중복/문맥 자연스러움/
정의-예문 일치/기존 문항 중복 검사는 별도 스크립트(phase27_verify_l6_pilot_items.py)와
사람의 의미 판정(semantic_verdict)이 그 다음 단계에서 채운다.

사용:
    python3 scripts/vocab/phase27_generate_l6_pilot_items.py \\
        --db-path <research db 사본 또는 원본, 읽기 전용> \\
        --selected-json data/import/schema_reading_phase27_l6_pilot_selected_20260927.json \\
        --out data/import/schema_reading_phase27_l6_pilot_items_draft_20260927.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sqlite3
from pathlib import Path

BATCH_SOURCE_VERSION = "schema_reading_l6_pilot_dryrun_v1"
GENERATOR_VERSION = "schema_reading_phase27_l6_pilot_v1"
GRADE_CAVEAT = (
    "L6=고2~3 분류는 개별 grade_level 태그가 아니라 docs/literacy/07-학년경계정책-L5L6.md "
    "정책(L5=고1, L6=고2~3)에 따른 잠정 매핑 근거다."
)


def has_batchim(word: str) -> bool:
    """word의 마지막 글자가 받침(종성)이 있는 한글 음절인지 - 조사 이형태(은/는,
    이라는/라는) 선택에 쓴다. 한글 음절이 아니면(숫자/영문 등) 받침 있다고
    보수적으로 처리(그런 표제어/정의는 이번 배치에 없음)."""
    ch = word[-1]
    code = ord(ch) - 0xAC00
    if 0 <= code < 11172:
        return (code % 28) != 0
    return True


def eun_neun(word: str) -> str:
    return "은" if has_batchim(word) else "는"


def ira_neun(word: str) -> str:
    return "이라는" if has_batchim(word) else "라는"


def eul_reul(word: str) -> str:
    return "을" if has_batchim(word) else "를"


def i_ga(word: str) -> str:
    return "이" if has_batchim(word) else "가"


def wa_gwa(word: str) -> str:
    return "과" if has_batchim(word) else "와"


def caution_related_lemmas(caution: str, all_lemmas: set[str]) -> set[str]:
    quoted = re.findall(r"'([^']+)'", caution or "")
    return {q for q in quoted if q in all_lemmas}


def build_option_set(target: dict, distractors: list[dict], seed_key: str) -> tuple[list[str], int]:
    options = [target["student_definition"]] + [d["student_definition"] for d in distractors]
    order = list(range(4))
    rng = random.Random(int(hashlib.sha256(seed_key.encode("utf-8")).hexdigest(), 16))
    rng.shuffle(order)
    shuffled = [options[i] for i in order]
    correct_option = order.index(0) + 1
    return shuffled, correct_option


def wrong_option_reason(target: dict, distractor: dict) -> str:
    return (
        f"이 설명은 '{target['lemma']}'{i_ga(target['lemma'])} 아니라 "
        f"'{distractor['lemma']}'의 뜻이다"
        f"({distractor['student_definition']}). '{target['lemma']}'"
        f"({target['student_definition']}){wa_gwa(target['lemma'])} 의미 영역이 다르므로 오답이다."
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--selected-json", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    selected_meta = json.load(open(args.selected_json, encoding="utf-8"))
    content_ids = [r["content_id"] for r in selected_meta]
    assert len(content_ids) <= 20

    conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""
        SELECT c.content_id, c.lemma, c.pos, c.canonical_definition, c.student_definition,
               c.example_sentence, c.example_target_form, c.source_version, l.level_reason_json
        FROM vocabulary_contents c
        JOIN vocabulary_content_levels l ON c.content_id = l.content_id
        WHERE c.content_id IN ({placeholders})
        """,
        content_ids,
    ).fetchall()
    all_lemma_row = conn.execute("SELECT DISTINCT lemma FROM vocabulary_contents").fetchall()
    all_lemmas = {r[0] for r in all_lemma_row}
    conn.close()

    fresh = {}
    for content_id, lemma, pos, canonical_def, student_def, example, target_form, source_version, level_reason_raw in rows:
        lrj = json.loads(level_reason_raw)
        fresh[content_id] = {
            "content_id": content_id, "lemma": lemma, "pos": pos,
            "canonical_definition": canonical_def, "student_definition": student_def,
            "example_sentence": example, "example_target_form": target_form,
            "content_source_version": source_version,
            "literacy_term_id": lrj.get("literacy_term_id"),
            "caution": lrj.get("caution", ""),
        }
    assert len(fresh) == len(content_ids), f"DB에서 못 찾은 content_id 있음: {len(fresh)}/{len(content_ids)}"
    ordered_targets = [fresh[cid] for cid in content_ids]

    items = []
    for idx, target in enumerate(ordered_targets, start=1):
        related = caution_related_lemmas(target["caution"], all_lemmas)
        pool = [t for t in ordered_targets if t["content_id"] != target["content_id"]
                and t["lemma"] not in related]
        rng = random.Random(int(hashlib.sha256(target["content_id"].encode("utf-8")).hexdigest(), 16))
        distractors = rng.sample(pool, 3)

        seq = f"{idx:03d}"
        meaning_options, meaning_correct = build_option_set(target, distractors, target["content_id"] + "_MC")
        context_options, context_correct = build_option_set(target, distractors, target["content_id"] + "_CM")

        assert meaning_options.count(target["student_definition"]) == 1
        assert context_options.count(target["student_definition"]) == 1

        assert target["example_target_form"] in target["example_sentence"]
        context_prompt_sentence = target["example_sentence"].replace(
            target["example_target_form"], f"【{target['example_target_form']}】", 1
        )

        common_qa = {
            "content_id": target["content_id"],
            "literacy_term_id": target["literacy_term_id"],
            "content_source_version": target["content_source_version"],
            "caution": target["caution"],
            "l6_grade_caveat": GRADE_CAVEAT,
            "expert_review_status": "관리자 검토용 초안(phase27 file-based dry-run) - 전문가 검수 완료 아님, V항목",
            "distractors": [
                {"lemma": d["lemma"], "content_id": d["content_id"]} for d in distractors
            ],
            "wrong_option_reasons": [
                {"distractor_lemma": d["lemma"], "distractor_content_id": d["content_id"],
                 "reason": wrong_option_reason(target, d)}
                for d in distractors
            ],
        }

        items.append({
            "item_id": f"MF_A_SC_SRL6PILOT_20260927_L6_{seq}",
            "item_type": "MEANING_CHOICE",
            "source_content_id": target["content_id"],
            "lemma": target["lemma"],
            "pos": target["pos"],
            "prompt": f"'{target['lemma']}'의 뜻으로 가장 알맞은 것은?",
            "options_json": meaning_options,
            "correct_option": meaning_correct,
            "explanation": f"'{target['lemma']}'{eun_neun(target['lemma'])} "
                            f"'{target['student_definition']}'{ira_neun(target['student_definition'])} 뜻입니다.",
            "generator_version": GENERATOR_VERSION,
            "source_version": BATCH_SOURCE_VERSION,
            "qa_flags_json": dict(common_qa),
        })
        items.append({
            "item_id": f"MF_C_SC_SRL6PILOT_20260927_L6_{seq}",
            "item_type": "CONTEXT_MEANING",
            "source_content_id": target["content_id"],
            "lemma": target["lemma"],
            "pos": target["pos"],
            "prompt": f"다음 문장에서 표시된 낱말의 뜻으로 가장 알맞은 것은?\n\n{context_prompt_sentence}",
            "options_json": context_options,
            "correct_option": context_correct,
            "explanation": f"문장 속 '{target['lemma']}'{eun_neun(target['lemma'])} "
                            f"'{target['student_definition']}'{eul_reul(target['student_definition'])} 뜻합니다.",
            "generator_version": GENERATOR_VERSION,
            "source_version": BATCH_SOURCE_VERSION,
            "qa_flags_json": dict(common_qa),
        })

    print(f"선정 콘텐츠 {len(ordered_targets)}건 -> 초안 문항 {len(items)}건 "
          f"(MEANING_CHOICE {len(ordered_targets)} + CONTEXT_MEANING {len(ordered_targets)})")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=1)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
