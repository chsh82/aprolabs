#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase32 항목1: phase31 결과에서 과학 10건, 법 외 사회 10건(최대 20건)을
선정한다. 읽기 전용 - phase30 산출물 CSV만 읽는다. literacy.db/vocabulary_quiz
DB 어디에도 쓰지 않는다.

제외 대상(phase31이 이미 예외 처리한 항목 - 이번 파일럿에서 다시 다루지
않음):
  - 동형이의 위험 4건: 시차(6805)/분화(6864)/교환(5991)/공약(5956)
  - 표기 보류 2건: 르 샤를리에 원리(6960)/핌비(5900)
  - 정의 오류 의심 1건: 별의 진화(6817)
  - 소분류 재태깅 후보 5건: 부동산(6014)/등기제도(6015)/공시(6016)/
    주택임대차(6017)/상소절차(6029)

선정 기준: 교과(sense_category)·주차(note_week) 조합이 서로 다른 항목만
골라 분산시킨다(같은 소단원에 여러 건이 몰리지 않게). 구체적인 20건은 이
스크립트에 고정 목록으로 명시한다(재현 가능성을 위해 무작위 추출이 아니라
결정론적 목록 - 파일럿 규모가 작고 "대표성 있는 표본"이 아니라 "여러 소단원을
훑어보는 시험 집필"이 목적이므로 phase30처럼 통계적 층화 추출을 쓰지 않았다).

사용:
    python3 scripts/vocab/phase32_select_pilot_20.py \\
        --input data/import/schema_reading_phase30_ai_tag_577_20260927.csv \\
        --out data/import/schema_reading_phase32_selected_20_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

EXCLUDED_IDS = {
    "6805", "6864", "5991", "5956",  # 동형이의 위험 4건
    "6960", "5900",                    # 표기 보류 2건
    "6817",                            # 정의 오류 의심 1건
    "6014", "6015", "6016", "6017", "6029",  # 소분류 재태깅 후보 5건
}

SCIENCE_IDS = ["6904", "6835", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "7049"]
SOCIAL_NONLAW_IDS = ["5749", "5770", "5794", "5804", "5834", "5845", "5863", "5884", "5928", "5968"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with open(args.input, encoding="utf-8-sig") as f:
        rows = {r["term_id"]: r for r in csv.DictReader(f)}

    selected_ids = SCIENCE_IDS + SOCIAL_NONLAW_IDS
    assert len(selected_ids) == 20, f"선정 20건이 아님: {len(selected_ids)}"
    assert len(set(selected_ids)) == 20, "선정 목록에 중복 존재"
    assert not (set(selected_ids) & EXCLUDED_IDS), "제외 대상이 선정에 섞여 있음"

    out_rows = []
    seen_groups = set()
    for tid in selected_ids:
        r = rows[tid]
        pilot_group = "science" if tid in SCIENCE_IDS else "social_nonlaw"
        key = (r["sense_category"], r["note_week"])
        dup_group = key in seen_groups
        seen_groups.add(key)
        out_rows.append({
            "term_id": tid, "headword": r["headword"], "pilot_group": pilot_group,
            "subject_category": r["subject_category"], "sense_category": r["sense_category"],
            "note_subcategory": r["note_subcategory"], "note_week": r["note_week"],
            "ai_definition": r["definition"], "duplicate_subcategory_week_group": dup_group,
        })

    n_dup_groups = sum(1 for r in out_rows if r["duplicate_subcategory_week_group"])
    print(f"선정 {len(out_rows)}건 (과학 {len(SCIENCE_IDS)} + 법외 사회 {len(SOCIAL_NONLAW_IDS)})")
    print(f"교과×주차 그룹 중복(분산 실패) 건수: {n_dup_groups} (0이어야 함)")
    assert n_dup_groups == 0, "같은 교과×주차 그룹에서 2건 이상 뽑힘 - 분산 실패"

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
