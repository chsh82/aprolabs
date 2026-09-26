#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase27 항목4: 생성된 L6 파일럿 문항 초안(최대 40건)을 생성 로직과 별도로
기계적으로 검증한다. 읽기 전용(SELECT만) - DB에 아무것도 쓰지 않는다.

기계적 검사(이 스크립트가 자동으로 판정 - AUTO_PASS 컬럼):
  1. 정답 유일성: 정답 문자열(현재 DB student_definition)이 4개 보기 중 정확히
     1번만 등장하는지
  2. 보기 4개에 완전 중복 문자열이 없는지(정답 복제/오답 복제 버그 방지)
  3. 정의-예문 일치: explanation에 인용된 정의가 현재 DB student_definition과
     정확히 같은지(하드코딩 드리프트 방지 - 생성 시점 값이 아니라 검증 시점에
     DB를 다시 읽어 비교)
  4. 문맥형 문항의 대상 낱말 활용형(example_target_form)이 프롬프트 문장에
     정확히 한 번 등장하는지, 그 문장이 현재 DB example_sentence와 일치하는지
  5. 기존 vocabulary_multiformat_items와 item_id 충돌이 없는지, 같은
     (source_content_id, item_type) 조합의 기존 문항이 없는지(중복 방지)
  6. 조사 이형태(은/는, 이/가, 을/를, 이라는/라는, 와/과)가 받침 유무에 맞는지
     (한글 맞춤법 기계 검사)

이 스크립트가 하지 않는 것(사람이 별도로 판정 - semantic_verdict 컬럼):
  - 오답의 "의미 중복"이 실제로 학생에게 헷갈릴 만큼 가까운지에 대한 최종 판단
    (caution 기반 사전 배제는 생성 단계에서 이미 적용됨 - 이 스크립트는 그
    배제가 실제로 적용됐는지만 재확인)
  - 문맥형 예문이 자연스러운지, 다른 보기를 실제로 배제하는지
  - 해설의 서술이 정확한지에 대한 최종 승인

사용:
    python3 scripts/vocab/phase27_verify_l6_pilot_items.py \\
        --db-path <research db 사본 또는 원본, 읽기 전용> \\
        --items-json data/import/schema_reading_phase27_l6_pilot_items_draft_20260927.json \\
        --out data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path


def has_batchim(word: str) -> bool:
    ch = word[-1]
    code = ord(ch) - 0xAC00
    if 0 <= code < 11172:
        return (code % 28) != 0
    return True


def check_particles(item: dict) -> list[str]:
    issues = []
    lemma = item["lemma"]
    eun_neun = "은" if has_batchim(lemma) else "는"
    if f"'{lemma}'{eun_neun}" not in item["explanation"]:
        issues.append(f"은/는 조사 오류 의심: explanation={item['explanation']!r}")
    i_ga = "이" if has_batchim(lemma) else "가"
    wa_gwa = "과" if has_batchim(lemma) else "와"
    for w in item["qa_flags_json"]["wrong_option_reasons"]:
        if f"'{lemma}'{i_ga} 아니라" not in w["reason"]:
            issues.append(f"이/가 조사 오류 의심: {w['reason']!r}")
        if f"){wa_gwa} 의미" not in w["reason"]:
            issues.append(f"와/과 조사 오류 의심: {w['reason']!r}")
    return issues


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--items-json", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    items = json.load(open(args.items_json, encoding="utf-8"))
    content_ids = sorted({it["source_content_id"] for it in items})

    conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"SELECT content_id, student_definition, example_sentence, example_target_form "
        f"FROM vocabulary_contents WHERE content_id IN ({placeholders})",
        content_ids,
    ).fetchall()
    live = {r[0]: {"student_definition": r[1], "example_sentence": r[2], "example_target_form": r[3]} for r in rows}

    existing_item_ids = {r[0] for r in conn.execute("SELECT item_id FROM vocabulary_multiformat_items").fetchall()}
    existing_pairs = {
        (r[0], r[1]) for r in conn.execute(
            "SELECT source_content_id, item_type FROM vocabulary_multiformat_items"
        ).fetchall()
    }
    conn.close()

    results = []
    for item in items:
        issues = []
        cid = item["source_content_id"]
        cur = live.get(cid)
        if cur is None:
            issues.append(f"DB에서 content_id={cid} 를 찾을 수 없음")
            results.append({"item_id": item["item_id"], "auto_validation_status": "FAIL", "issues": issues})
            continue

        options = item["options_json"]
        correct_text = cur["student_definition"]
        if options.count(correct_text) != 1:
            issues.append(f"정답 유일성 위반: 정답 문자열이 보기 중 {options.count(correct_text)}회 등장")
        if len(set(options)) != len(options):
            issues.append("보기 4개 중 완전 중복 문자열 존재")
        if len(options) != 4:
            issues.append(f"보기 개수가 4가 아님: {len(options)}")
        if not (1 <= item["correct_option"] <= 4) or options[item["correct_option"] - 1] != correct_text:
            issues.append("correct_option 인덱스가 실제 정답 위치와 불일치")

        if item["item_type"] == "CONTEXT_MEANING":
            target_form = cur["example_target_form"]
            bracketed = f"【{target_form}】"
            if item["prompt"].count(bracketed) != 1:
                issues.append(f"문맥 프롬프트에 표시 형식 【{target_form}】 이 정확히 1회 등장하지 않음")
            reconstructed = cur["example_sentence"].replace(target_form, bracketed, 1)
            if reconstructed not in item["prompt"]:
                issues.append("문맥 프롬프트 문장이 현재 DB example_sentence와 다름(드리프트 의심)")

        if f"'{correct_text}'" not in item["explanation"] and correct_text not in item["explanation"]:
            issues.append("해설에 현재 DB 정의 문자열이 그대로 인용되지 않음")

        if item["item_id"] in existing_item_ids:
            issues.append("item_id가 기존 vocabulary_multiformat_items와 충돌")
        if (cid, item["item_type"]) in existing_pairs:
            issues.append("같은 content_id+item_type 조합의 기존 문항이 이미 존재(중복)")

        issues.extend(check_particles(item))

        status = "PASS" if not issues else "FAIL"
        results.append({"item_id": item["item_id"], "source_content_id": cid, "lemma": item["lemma"],
                         "item_type": item["item_type"], "auto_validation_status": status, "issues": issues})

    n_pass = sum(1 for r in results if r["auto_validation_status"] == "PASS")
    print(f"기계 검증: {n_pass}/{len(results)} PASS")
    for r in results:
        if r["auto_validation_status"] != "PASS":
            print(f"  FAIL {r['item_id']}: {r['issues']}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    print(f"저장: {args.out}")
    return 0 if n_pass == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
