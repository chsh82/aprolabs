# -*- coding: utf-8 -*-
"""L3 중등 보강 1차 v2 오답 개정 - 생성 스크립트와 분리된 QA.

기계적 검사(정답 유일성·correct_option 일치·보기 길이 편차·조사 호응)와
**사람이 직접 하는 의미 판단**(목표어를 몰라도 보기만으로 정답을 고를 수
있는가)을 분리해서 보여준다. 의미 유사도 수치(자카드 등) 하나로 "학습
가치 있음"을 선언하지 않는다 - 그 수치는 참고용으로만 같이 보여주고,
최종 판단은 아래 "편집자 판단" 칸에 사람이 직접 쓴 근거로 한다."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VOCAB = ROOT / "data" / "vocab"


def has_batchim(ch: str) -> bool:
    code = ord(ch) - 0xAC00
    return 0 <= code <= 11171 and code % 28 != 0


def check_particles(text: str) -> list[str]:
    issues = []
    for with_b, without_b in (("을", "를"), ("이", "가"), ("은", "는"), ("과", "와")):
        for m in re.finditer(rf"([가-힣])'?({with_b}|{without_b})(?=[\s.,]|$)", text):
            char, particle = m.group(1), m.group(2)
            expected = with_b if has_batchim(char) else without_b
            if particle != expected:
                issues.append(f"{char}{particle}(->{char}{expected}?) in ...{text[max(0,m.start()-6):m.end()+6]}...")
    return issues


def jaccard(a: str, b: str) -> float:
    ta, tb = set(a.replace(".", "").split()), set(b.replace(".", "").split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    with open(VOCAB / "nikl_grade5_l3_batch1_manifest_v2_DRAFT.json", encoding="utf-8") as f:
        items = json.load(f)

    by_content: dict[str, list[dict]] = {}
    for it in items:
        by_content.setdefault(it["source_content_id"], []).append(it)

    print(f"=== 1. 정답 유일성 + correct_option 일치 (기계적) === ({len(items)}문항)")
    uniq_fail, idx_fail = [], []
    for it in items:
        options = json.loads(it["options_json"])
        if len(set(options)) != 4:
            uniq_fail.append(it["item_id"])
        payload = json.loads(it["answer_payload_json"])
        if options[it["correct_option"] - 1] is None or payload["correct_option"] != it["correct_option"]:
            idx_fail.append(it["item_id"])
    print(f"정답 유일성 FAIL: {len(uniq_fail)}건 {uniq_fail}")
    print(f"correct_option 불일치: {len(idx_fail)}건 {idx_fail}")

    print("\n=== 2. 보기 길이 편차(기계적, 참고용 - '정답만 유난히 자세함' 탐지) ===")
    length_flags = []
    for it in items:
        options = json.loads(it["options_json"])
        lens = [len(o) for o in options]
        correct_len = lens[it["correct_option"] - 1]
        others_avg = sum(l for i, l in enumerate(lens) if i != it["correct_option"] - 1) / 3
        if correct_len > others_avg * 1.6 or correct_len < others_avg * 0.5:
            length_flags.append((it["item_id"], correct_len, round(others_avg, 1)))
    print(f"길이 편차 큰 문항(정답길이 vs 오답평균): {len(length_flags)}건")
    for f in length_flags:
        print(" ", f)

    print("\n=== 3. 조사 호응(기계적) ===")
    particle_issues = 0
    for it in items:
        options = json.loads(it["options_json"])
        for opt in options + [it["explanation"]]:
            issues = check_particles(opt)
            if issues:
                particle_issues += len(issues)
                print(f"  [FAIL] {it['item_id']}: {issues}")
    print(f"조사 호응 FAIL 총 {particle_issues}건")

    print("\n=== 4. 참고용 자카드 유사도(정답 vs 오답 각각) - 그 자체로 학습가치를 판정하지 않음 ===")
    for cid, (mc, cm) in {cid: (its[0], its[1]) for cid, its in by_content.items() if len(its) == 2}.items():
        options = json.loads(mc["options_json"])
        correct = options[mc["correct_option"] - 1]
        sims = [round(jaccard(correct, o), 2) for i, o in enumerate(options) if i != mc["correct_option"] - 1]
        print(f"  {mc['lemma']}: 정답-오답 자카드={sims} (참고용 - 낮다고 바로 '좋은 문항'은 아님, 5절 편집자 판단 참고)")


if __name__ == "__main__":
    main()
