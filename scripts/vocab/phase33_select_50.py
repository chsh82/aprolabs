#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase33 항목1: phase31/32가 이미 다룬 32건(phase31 예외 12 + phase32 선정 20)을
제외하고, 과학·법 외 사회(정치/경제)에서 최대 50건을 선정한다. 읽기 전용 -
phase30 산출물 CSV만 읽는다. literacy.db/vocabulary_quiz DB 어디에도 쓰지 않는다.

선정 절차(결정론적, 무작위 추출 없음 - "찾기 쉬운 표제어만 골라 수율을
높이지 마라"는 지시를 지키기 위해 헤드워드 내용·예상 검색 난이도를 전혀
보지 않고 순수 구조적 기준(교과×소분류 그룹, 주차 순서)만으로 고른다):

1. (subject_category, note_subcategory) 그룹별로 표본 크기에 따라 쿼터를
   배정한다 - 그룹 35개 전부에 기본 1건을 배정하고(전 소분류 커버),
   표본이 큰 상위 15개 그룹에는 +1을 더해 그룹당 최대 2건, 합계
   35 + 15 = 50건이 되게 한다.
2. 그룹 내부에서는 note_week -> term_id로 정렬한 뒤:
   - 쿼터 1건: 정렬된 목록의 중앙 인덱스(len//2)를 선택(그룹 안에서 특정
     주차에 치우치지 않도록 하는 구조적 규칙, 헤드워드 내용은 보지 않음)
   - 쿼터 2건: 정렬된 목록의 첫 항목과 마지막 항목을 선택(그룹 내 주차
     스팬을 최대화)
3. 이 알고리즘이 만든 50건 목록을 고정 목록으로 스크립트에 명시한다
   (재현 가능성 - phase32와 동일한 관행).

사용:
    python3 scripts/vocab/phase33_select_50.py \\
        --input data/import/schema_reading_phase30_ai_tag_577_20260927.csv \\
        --out data/import/schema_reading_phase33_selected_50_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

# phase31 예외 12건 + phase32 선정 20건 = 32건(이번 단계에서 다시 다루지 않음)
EXCLUDED_IDS = {
    # phase31 예외 12건
    "6805", "6864", "5991", "5956",  # 동형이의 위험 4건
    "6960", "5900",                    # 표기 보류 2건
    "6817",                            # 정의 오류 의심 1건
    "6014", "6015", "6016", "6017", "6029",  # 소분류 재태깅 후보 5건
    # phase32 선정 20건(과학 10 + 법외 사회 10) - 재확인은 phase33 항목5에서 별도 처리
    "6904", "6835", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "7049",
    "5749", "5770", "5794", "5804", "5834", "5845", "5863", "5884", "5928", "5968",
}

# 아래 50건은 이 파일의 select_50() 알고리즘이 산출한 고정 목록이다(재현성 확보).
# 재현: python3 scripts/vocab/phase33_select_50.py --input ... --out ... 실행 시
# 동일한 입력 CSV에 대해 항상 이 목록이 재생성됨(순수 구조적 정렬 기반, 무작위성 없음).
SELECTED_50_IDS = [
    "5881", "5915", "5954", "5980", "6881", "6901", "5924", "5945", "7025", "7045",
    "6832", "6851", "6990", "7009", "5747", "5766", "5844", "5862", "6799", "6816",
    "5800", "5817", "6974", "6989", "6863", "6880", "6784", "6798", "6924", "6938",
    "7017", "5825", "6777", "5775", "7053", "6763", "6825", "6945", "6968", "6918",
    "6858", "6908", "5839", "5786", "5876", "6956", "5920", "5796", "5868", "5950",
]


def select_50(rows: dict[str, dict]) -> list[str]:
    """rows: term_id -> row. 그룹별 쿼터 배정 + 그룹 내 구조적 선택."""
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows.values():
        if r["term_id"] in EXCLUDED_IDS:
            continue
        is_science = r["subject_category"] == "과학"
        is_social_nonlaw = r["subject_category"] == "사회" and r["sense_category"] in ("정치", "경제")
        if not (is_science or is_social_nonlaw):
            continue
        key = (r["subject_category"], r["note_subcategory"])
        groups.setdefault(key, []).append(r)

    order = sorted(groups.keys(), key=lambda k: (-len(groups[k]), k[0], k[1]))
    quota = {k: 1 for k in order}
    for k in order[:15]:
        quota[k] += 1

    selected: list[str] = []
    missing_week_strata: list[str] = []
    for k in order:
        members = sorted(groups[k], key=lambda r: (r["note_week"], r["term_id"]))
        n = quota[k]
        weeks_in_group = sorted({m["note_week"] for m in members})
        if n == 1:
            picked = [members[len(members) // 2]]
        else:
            picked = [members[0], members[-1]]
        selected.extend(p["term_id"] for p in picked)
        picked_weeks = {p["note_week"] for p in picked}
        uncovered = [w for w in weeks_in_group if w not in picked_weeks]
        if uncovered:
            missing_week_strata.append(f"{k[0]}/{k[1]}: 주차 {len(uncovered)}개 미선정({sorted(uncovered)})")

    return selected, missing_week_strata


def form_type(headword: str) -> str:
    if " " in headword:
        return "phrase"
    return "simple" if len(headword) <= 3 else "compound"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with open(args.input, encoding="utf-8-sig") as f:
        rows = {r["term_id"]: r for r in csv.DictReader(f)}

    computed_ids, missing_week_strata = select_50(rows)
    assert len(computed_ids) == 50, f"선정 50건이 아님: {len(computed_ids)}"
    assert len(set(computed_ids)) == 50, "선정 목록에 중복 존재"
    assert not (set(computed_ids) & EXCLUDED_IDS), "제외 대상이 선정에 섞여 있음"
    assert set(computed_ids) == set(SELECTED_50_IDS), (
        "고정 목록(SELECTED_50_IDS)이 알고리즘 재산출 결과와 다름 - "
        "입력 CSV가 바뀌었거나 알고리즘 수정 후 고정 목록을 갱신하지 않은 것"
    )

    out_rows = []
    for tid in computed_ids:
        r = rows[tid]
        out_rows.append({
            "term_id": tid, "headword": r["headword"],
            "subject_category": r["subject_category"], "sense_category": r["sense_category"],
            "note_subcategory": r["note_subcategory"], "note_week": r["note_week"],
            "ai_definition": r["definition"], "headword_form": form_type(r["headword"]),
        })

    from collections import Counter
    print(f"선정 {len(out_rows)}건")
    print(f"교과 분포: {dict(Counter(o['subject_category'] for o in out_rows))}")
    print(f"소분류 그룹 커버: {len({(o['subject_category'], o['note_subcategory']) for o in out_rows})}개")
    print(f"어휘 형태 분포: {dict(Counter(o['headword_form'] for o in out_rows))}")
    print(f"누락된 층(그룹 내 미선정 주차) {len(missing_week_strata)}건:")
    for line in missing_week_strata:
        print(f"  - {line}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
