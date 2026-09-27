#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase30 항목3: S 576건에서 (교과×주차) 층화 표본을 재현 가능한 seed로 뽑는다.
읽기 전용 - 입력 CSV(phase30_extract_ai_tag_577.py 산출물)만 읽고 표본 목록
CSV를 만든다. literacy.db/vocabulary_quiz DB에는 전혀 접근하지 않는다.

층화 기준: (subject_category, note_week) - "원천 유형"은 577건 전수 조사
결과 전부 xlsx_definition_present=False(원천 정의 없음)로 동일해 실제로는
분화되지 않는 축이었다(이 스크립트가 그 사실 자체를 다시 확인해 출력한다).

배분 규칙(재현 가능, seed 고정):
  1. 층 크기가 FULL_THRESHOLD(5) 이하인 작은 층은 전수 포함한다.
  2. 나머지 층은 (100 - 이미 포함된 건수)를 층 크기 비례로 배분하되 최소
     1건씩 배정하고(최대공약수식 나머지 배분, Largest Remainder Method),
     초과분은 큰 층부터 1건씩 깎아 총 100건을 넘지 않게 한다.
  3. 각 층 안에서는 `random.Random(SEED)`로 정렬 후 앞에서부터 뽑는다
     (표본 자체와 배분 규칙이 이 스크립트 재실행만으로 동일하게 재현됨).

V 1건은 이 표본과 별도로 전수 확인 대상이므로 이 스크립트가 다루지 않는다
(phase30 보고서에서 별도로 표시).

사용:
    python3 scripts/vocab/phase30_stratified_sample.py \\
        --input data/import/schema_reading_phase30_ai_tag_577_20260927.csv \\
        --out data/import/schema_reading_phase30_s_sample_20260927.csv \\
        --max-sample 100 --seed 577
"""
from __future__ import annotations

import argparse
import csv
import random
from collections import defaultdict
from pathlib import Path

FULL_THRESHOLD = 5


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--max-sample", type=int, default=100)
    ap.add_argument("--seed", type=int, default=577)
    args = ap.parse_args()

    with open(args.input, encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))
    s_rows = [r for r in all_rows if r["source"] == "schemareading-schema"]
    print(f"S(schemareading-schema) 총 {len(s_rows)}건 (기대값 576)")
    assert len(s_rows) == 576

    n_with_source = sum(1 for r in s_rows if r["xlsx_definition_present"] == "True")
    print(f"원천 XLSX에 정의가 있는 건: {n_with_source}/576건 - "
          f"{'전부 원천 정의 없음이라 원천 유형 축은 분화되지 않음' if n_with_source == 0 else '일부 있음(예상 밖 - 재확인 필요)'}")

    strata: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in s_rows:
        strata[(r["subject_category"], r["note_week"])].append(r)
    print(f"층(교과×주차) 개수: {len(strata)}")

    full_strata = {k: v for k, v in strata.items() if len(v) <= FULL_THRESHOLD}
    partial_strata = {k: v for k, v in strata.items() if len(v) > FULL_THRESHOLD}
    n_full = sum(len(v) for v in full_strata.values())
    print(f"전수 포함 대상 작은 층(크기<={FULL_THRESHOLD}): {len(full_strata)}개, {n_full}건")

    remaining_budget = args.max_sample - n_full
    total_partial = sum(len(v) for v in partial_strata.values())

    # Largest Remainder Method로 비례 배분
    raw_alloc = {}
    for k, v in partial_strata.items():
        exact = len(v) * remaining_budget / total_partial
        raw_alloc[k] = (max(1, int(exact)), exact - int(exact), len(v))
    allocated_total = sum(a[0] for a in raw_alloc.values())

    remainders = sorted(raw_alloc.items(), key=lambda kv: -kv[1][1])
    i = 0
    while allocated_total < remaining_budget and i < len(remainders):
        k, (n, rem, size) = remainders[i]
        if raw_alloc[k][0] < size:
            raw_alloc[k] = (raw_alloc[k][0] + 1, rem, size)
            allocated_total += 1
        i += 1

    while allocated_total > remaining_budget:
        k_max = max(raw_alloc, key=lambda k: raw_alloc[k][0])
        if raw_alloc[k_max][0] <= 1:
            break
        raw_alloc[k_max] = (raw_alloc[k_max][0] - 1, raw_alloc[k_max][1], raw_alloc[k_max][2])
        allocated_total -= 1

    sample_rows: list[dict] = []
    for k, v in full_strata.items():
        for r in v:
            r2 = dict(r)
            r2["stratum"] = f"{k[0]}/{k[1]}"
            r2["stratum_size"] = len(v)
            r2["sample_reason"] = "FULL_SMALL_STRATUM"
            sample_rows.append(r2)

    for k, v in partial_strata.items():
        n = raw_alloc[k][0]
        rng = random.Random(args.seed)
        shuffled = sorted(v, key=lambda r: r["term_id"])  # 결정론적 정렬 후 셔플(입력 순서 의존 제거)
        rng.shuffle(shuffled)
        chosen = shuffled[:n]
        for r in chosen:
            r2 = dict(r)
            r2["stratum"] = f"{k[0]}/{k[1]}"
            r2["stratum_size"] = len(v)
            r2["sample_reason"] = f"STRATIFIED_SAMPLE(n={n}/{len(v)})"
            sample_rows.append(r2)

    print(f"\n최종 표본: {len(sample_rows)}건 (목표 {args.max_sample} 이하)")
    fieldnames = list(sample_rows[0].keys())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(sample_rows)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
