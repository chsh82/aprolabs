# -*- coding: utf-8 -*-
"""RULE_A(1,368) 층화표본 확대 + RULE_B(40) 전수 검토표 생성. 읽기 전용 -
DB에는 정의 텍스트 조회(SELECT)만 하고 아무것도 쓰지 않는다.
사람_확인_판정은 항상 빈칸으로 남긴다(기계가 대신 채우지 않는다).
"""
from __future__ import annotations

import csv
import io
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
IMPORT_DIR = REPO_ROOT / "data" / "import"
SCRIPT_DIR = REPO_ROOT / "scripts" / "vocab"


def load_csv(name: str) -> list[dict]:
    with open(IMPORT_DIR / name, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    policy_rows = load_csv("nikl_base_level_policy_20260929.csv")
    active_map = json.load(open(SCRIPT_DIR / "_active_2129_map.json", encoding="utf-8"))
    defs = json.load(open(SCRIPT_DIR / "_definitions_for_sample.json", encoding="utf-8"))

    rule_a = [r for r in policy_rows if r["pattern_id"] == "RULE_A_grade4_to_L2_from_L3"]
    rule_b = [r for r in policy_rows if r["pattern_id"] == "RULE_B_grade3_to_L1_from_L2"]
    assert len(rule_a) == 1368
    assert len(rule_b) == 40

    active_a = active_map["rule_a_ids"]

    # 층화: (품사, 현재_2.1.29_출제여부). 원천 배치(level_source)는 두 규칙 모두 100%
    # NIKL_GRADE_PLUS_DETERMINISTIC_RULES 단일값이라 별도 층화축으로서 정보가 없음(예외
    # carve-out이 MANUAL_LITERACY_* 배치를 전부 걸러냈기 때문 - 재확인 완료).
    strata: dict[tuple, list[dict]] = defaultdict(list)
    for r in rule_a:
        key = (r["pos"], bool(active_a.get(r["content_id"], False)))
        strata[key].append(r)

    rng = random.Random(42)
    sample_rows: list[dict] = []
    sampling_log: list[str] = []
    for key, items in sorted(strata.items(), key=lambda kv: -len(kv[1])):
        pos, is_active = key
        n = len(items)
        if is_active:
            # 현재 실제 출제 중인 항목은 전수 포함(운영 영향 직접적이라 우선순위 최고)
            picked = items
            method = f"전수(현재 2.1.29 출제 중이라 전량 포함)"
        elif n <= 10:
            picked = items
            method = "전수(층 크기 10 이하)"
        else:
            target = {"명사": 20, "동사": 15, "형용사": 10, "부사": 8}.get(pos, 8)
            target = min(target, n)
            picked = rng.sample(items, target)
            method = f"무작위표본(seed=42, {target}/{n})"
        sampling_log.append(f"({pos}, 출제중={is_active}): 모집단 {n} -> 표본 {len(picked)}  [{method}]")
        sample_rows.extend(picked)

    # 중복 제거(혹시 있을 경우 대비) 후 정렬
    seen = set()
    dedup_sample = []
    for r in sample_rows:
        if r["content_id"] in seen:
            continue
        seen.add(r["content_id"])
        dedup_sample.append(r)
    dedup_sample.sort(key=lambda r: r["content_id"])

    with open(SCRIPT_DIR / "_rule_a_sampling_log.txt", "w", encoding="utf-8") as f:
        f.write(f"RULE_A 층화표본 - 총 {len(dedup_sample)}건 (모집단 1,368건)\n")
        f.write("층화축: (품사, 현재_2.1.29_출제여부) - 원천배치는 100% 단일값이라 축에서 제외\n\n")
        for line in sampling_log:
            f.write(line + "\n")

    def build_rows(items: list[dict], rule_label: str) -> list[dict]:
        out = []
        for r in items:
            cid = r["content_id"]
            d = defs.get(cid, {})
            out.append({
                "content_id": cid,
                "lemma": r["lemma"],
                "pos": r["pos"],
                "official_grade": r["official_grade"],
                "current_vocab_level": r["current_vocab_level"],
                "현재_2.1.29_출제중": "예" if active_map["rule_a_ids"].get(cid) or active_map["rule_b_ids"].get(cid) else "아니오",
                "canonical_definition": d.get("canonical_definition", ""),
                "student_definition": d.get("student_definition", ""),
                "자동_판정": f"{rule_label} 규칙 적용 시 proposed_base_level={r['proposed_base_level']}",
                "자동_보조_메모": "",  # 아래에서 채움
                "사람_확인_판정": "",  # 사람 검토 전까지 항상 공란
            })
        return out

    rows_a = build_rows(dedup_sample, "RULE_A(공식4등급×현재L3→L2)")
    rows_b = build_rows(rule_b, "RULE_B(공식3등급×현재L2→L1)")

    for row in rows_a + rows_b:
        cdef = row["canonical_definition"] or row["student_definition"]
        row["자동_보조_메모"] = (
            f"[기계 생성 참고용, 승인 아님] 정의: {cdef[:50]}" if cdef else "[기계 생성 참고용] 정의 미확인"
        )

    fieldnames = ["content_id", "lemma", "pos", "official_grade", "current_vocab_level",
                  "현재_2.1.29_출제중", "canonical_definition", "student_definition",
                  "자동_판정", "자동_보조_메모", "사람_확인_판정"]

    with open(IMPORT_DIR / "nikl_rule_a_stratified_sample_20260929.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_a)

    with open(IMPORT_DIR / "nikl_rule_b_full_census_20260929.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_b)

    print(f"RULE_A 표본: {len(rows_a)}건 저장 (모집단 1,368건 중 {len(rows_a)/1368*100:.1f}%)")
    print(f"RULE_B 전수: {len(rows_b)}건 저장 (모집단 40건 = 100%)")
    print("사람_확인_판정 컬럼은 전부 공란으로 저장됨(자동 승인 아님)")


if __name__ == "__main__":
    main()
