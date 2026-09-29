# -*- coding: utf-8 -*-
"""823건 예외를 중복 없는 4단계 우선순위로 분류한다. 읽기 전용(DB read만),
결과는 CSV로만 저장한다."""
from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
IMPORT_DIR = REPO_ROOT / "data" / "import"
SCRIPT_DIR = REPO_ROOT / "scripts" / "vocab"

TIER2_REASONS = {"동형이의어", "뜻이_다른_가능성(기존78)"}
TIER3_REASONS = {"교과_전문_의미", "레벨격차과대(2단계이상)"}


def main() -> None:
    with open(IMPORT_DIR / "nikl_base_level_policy_exceptions_20260929.csv",
              encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 823, len(rows)

    active_map = json.load(open(SCRIPT_DIR / "_active_2129_map.json", encoding="utf-8"))["exc_ids"]

    # exception_reason 실제 문자열 확인(정확 매칭 위해)
    reasons_seen = {r["exception_reason"] for r in rows}

    tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for r in rows:
        cid = r["content_id"]
        is_active = bool(active_map.get(cid, False))
        reason = r["exception_reason"]
        if is_active:
            tier = 1
        elif "동형이의" in reason or "뜻이_다를" in reason or "뜻풀이" in reason:
            tier = 2
        elif "전문" in reason or "격차" in reason:
            tier = 3
        else:
            tier = 4
        r["priority_tier"] = tier
        r["priority_tier_label"] = {
            1: "실제_출제_중이며_공식기준과_충돌",
            2: "동형이의_뜻_위험",
            3: "상위_교과_의미",
            4: "당장_출제_영향_없음",
        }[tier]
        tier_counts[tier] += 1

    total = sum(tier_counts.values())
    assert total == 823, total

    fieldnames = list(rows[0].keys())
    with open(IMPORT_DIR / "nikl_exceptions_priority_tiers_20260929.csv", "w",
              encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print("exception_reason 실제 문자열들:", reasons_seen)
    print("우선순위 배정 원칙(순서대로 우선 적용, 먼저 만족하는 조건이 최종 tier):")
    print(" 1) 현재 2.1.29 출제 중 -> tier1 (사유 무관, 실사용 영향 최우선)")
    print(" 2) exception_reason에 '동형이의'/'뜻이_다른'/'뜻풀이' 포함 -> tier2")
    print(" 3) exception_reason에 '전문'/'격차' 포함 -> tier3")
    print(" 4) 나머지 -> tier4")
    print()
    print("tier1(실제_출제_중이며_공식기준과_충돌):", tier_counts[1])
    print("tier2(동형이의_뜻_위험):", tier_counts[2])
    print("tier3(상위_교과_의미):", tier_counts[3])
    print("tier4(당장_출제_영향_없음):", tier_counts[4])
    print("합계:", total)


if __name__ == "__main__":
    main()
