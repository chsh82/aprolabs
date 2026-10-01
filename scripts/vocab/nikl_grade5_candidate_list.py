# -*- coding: utf-8 -*-
"""공식 5등급(중1~3 경계 신호) 17,000행을 정리하고, 연구 DB 5,950건과
대조해 기존연결/확인필요/신규후보 3분류를 만든 뒤, 신규후보에서 품사·
분야 층화표본으로 1차 검토 배치 100건을 뽑는다.

읽기 전용 - DB에는 전혀 쓰지 않는다. 입력은 전부 이미 로컬에 있는
파일이거나, 아래 export_db_contents()로 한 번 생성해 두는 JSON
스냅샷(재현 가능 - 명령어를 스크립트 안에 그대로 남겨둔다)이다.

재현 절차:
  1) (최초 1회) ssh aprolabs에서 아래 export_db_contents()의 쿼리를
     실행해 vocabulary_contents(lemma/pos/definition/vocab_level)를
     JSON으로 뽑아 scp로 로컬에 내려받는다 - 이 스크립트는 그 파일
     경로(DB_EXPORT_JSON)만 읽는다, DB에 직접 연결하지 않는다.
  2) python scripts/vocab/nikl_grade5_candidate_list.py

선행 산출물(재인용, 재검증 안 함):
  - reports/nikl_l3_backfill_investigation_20260930.md +
    data/import/nikl_l3_backfill_census_20260930.csv(49건) - 227건
    배치 중 공식5등급 매칭 49건의 완전한 전수조사. 이 49건은
    "검수 대기 후보"로 그대로 둔다 - 이번 스크립트가 재분류하지 않는다
    (49건의 content_id는 아래 매칭에서 "기존 콘텐츠와 연결 후보"로
    다시 나타나는 게 정상이며, 그게 바로 사전 조사와 일치한다는
    뜻이다 - 이번 스크립트가 독자적으로 재현해 교차검증만 한다).
  - scripts/vocab/nikl_official_match.py의 매칭 방법론(표제어+품사
    조인, 동형이의 신중 처리)을 그대로 재사용한다 - 이번엔 방향이
    반대(공식목록 → DB)라는 점만 다르다.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
from collections import defaultdict
from pathlib import Path

import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[2]
XLSX_PATH = REPO_ROOT / "raw" / "nikl_official_vocab" / "국어_기초_어휘_선정_및_어휘_등급화_목록_전체_20231231.xlsx"
# ssh aprolabs에서 1회 생성(아래 주석 쿼리 참고) - 환경변수로 경로 덮어쓰기 가능
# (git-bash /tmp는 실제로 %TEMP%에 매핑되므로 os.environ["TEMP"]를 기본값으로 쓴다)
DB_EXPORT_JSON = Path(os.environ.get(
    "NIKL_DB_EXPORT_JSON",
    str(Path(os.environ.get("TEMP", "/tmp")) / "db_contents_export.json"),
))

OUT_FULL = REPO_ROOT / "data" / "import" / "nikl_grade5_full_20261001.csv"
OUT_CROSSREF = REPO_ROOT / "data" / "import" / "nikl_grade5_crossref_20261001.csv"
OUT_BATCH100 = REPO_ROOT / "data" / "import" / "nikl_grade5_first_batch_100_20261001.csv"

# ssh aprolabs에서 1회 실행해 DB_EXPORT_JSON을 만드는 쿼리(읽기 전용):
#   SELECT c.content_id, c.lemma, c.pos, c.canonical_definition,
#          c.student_definition, l.vocab_level, l.level_status, l.boundary_flag
#   FROM vocabulary_contents c
#   LEFT JOIN vocabulary_content_levels l
#     ON l.content_id=c.content_id AND l.is_active=1
#   WHERE c.is_active=1
# (file:...?mode=ro 로 연결, 5,950행 기대)

BIGRAM_JACCARD_THRESHOLD = 0.30  # vocab_level_l0l6_reverification_20260929.md §0-4와 동일 기준 재사용


def bigram_set(s: str) -> set[str]:
    s = (s or "").replace(" ", "").replace("\n", "")
    return {s[i:i + 2] for i in range(len(s) - 1)} if len(s) > 1 else {s} if s else set()


def bigram_jaccard(a: str, b: str) -> float:
    sa, sb = bigram_set(a), bigram_set(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def norm_pos(pos: str) -> set[str]:
    if not pos:
        return set()
    return {p.strip().replace(" ", "") for p in pos.split("/") if p.strip()}


def load_grade5_rows() -> list[dict]:
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb["전체(1~5등급), 40,000개"]
    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] is None:
            continue
        grade_label, lemma, hom_no, pos, origin, orig_form, meaning, domain = row[:8]
        if grade_label != "5등급" or not lemma:
            continue
        rows.append({
            "어휘": lemma, "동형번호": hom_no, "품사": pos, "어종": origin,
            "원어": orig_form, "의미": meaning, "분야": domain,
        })
    return rows


AUTHORITATIVE_MATCH_CSV = REPO_ROOT / "data" / "import" / "nikl_official_match_full_20260929.csv"


def load_authoritative_match() -> list[dict]:
    """이미 검증된 DB->공식목록 매칭 산출물을 그대로 재사용한다(재계산
    안 함) - 49건 census의 근거이기도 한 바로 그 파일이다. 이번 스크립트가
    독자적인 뜻풀이 유사도 휴리스틱으로 재매칭을 시도했다가 49건 중 19건을
    엉뚱하게 "확인 필요"로 떨어뜨리는 회귀가 있어(진단으로 확인·기록),
    새 휴리스틱 대신 이 공인된 산출물을 단일 진실 공급원으로 쓰기로
    변경했다."""
    with open(AUTHORITATIVE_MATCH_CSV, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def build_authoritative_index(match_rows: list[dict]):
    """(lemma, pos토큰) -> 그 DB content_id의 매칭_카테고리/공식등급 목록.
    같은 lemma(품사 무관)가 DB에 하나라도 있으면 by_lemma에도 채운다."""
    by_lemma_pos = defaultdict(list)
    by_lemma = defaultdict(list)
    for r in match_rows:
        by_lemma[r["lemma"]].append(r)
        for p in norm_pos(r.get("pos") or ""):
            by_lemma_pos[(r["lemma"], p)].append(r)
    return by_lemma_pos, by_lemma


def homonym_risk_lookup_full_list() -> dict:
    """공식 40,000어 전체(등급 무관)를 훑어, 같은 (표제어,품사토큰)이
    서로 다른 동형번호로 2개 이상 존재하는지 계산한다(등급-5 부분집합만
    보면 다른 등급에 걸친 동형이의를 놓치므로 전체를 본다)."""
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb["전체(1~5등급), 40,000개"]
    seen = defaultdict(set)
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] is None:
            continue
        _, lemma, hom_no, pos, *_ = row[:8]
        if not lemma:
            continue
        for p in norm_pos(pos or ""):
            seen[(lemma, p)].add(hom_no or 0)
    return {k: (len(v) > 1) for k, v in seen.items()}


def main():
    grade5_rows = load_grade5_rows()
    n_raw = len(grade5_rows)
    assert n_raw == 17000, f"기대 17000행, 실제 {n_raw}행"

    # ---- 2절: 원천 행수 vs 고유 어휘 수 ----
    distinct_lemma = {r["어휘"] for r in grade5_rows}
    distinct_lemma_hom = {(r["어휘"], r["동형번호"] or 0) for r in grade5_rows}
    pos_counter = defaultdict(int)
    domain_has_special = 0
    for r in grade5_rows:
        for p in norm_pos(r["품사"] or ""):
            pos_counter[p] += 1
        if "전문어" in (r["분야"] or ""):
            domain_has_special += 1

    with open(OUT_FULL, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["어휘", "동형번호", "품사", "어종", "원어", "의미", "분야"])
        w.writeheader()
        w.writerows(grade5_rows)

    # ---- 3절: DB 5,950건과 대조, 3분류 ----
    # 자체 뜻풀이-유사도 재매칭은 쓰지 않는다(진단 결과, 49건 census 중
    # 19건이 '단일 후보인데도 유사도 미달'로 잘못 "확인필요"에 떨어지는
    # 회귀가 발견됐다 - 이미 검증된 nikl_official_match_full_20260929.csv를
    # 단일 진실 공급원으로 재사용해 이 회귀를 없앴다).
    match_rows = load_authoritative_match()
    assert len(match_rows) == 5950, f"기대 5950행, 실제 {len(match_rows)}행"
    by_lemma_pos, by_lemma = build_authoritative_index(match_rows)
    hom_risk = homonym_risk_lookup_full_list()  # 전체 등급 기준(등급-5만 보면 교차등급 동형이의를 놓침)

    SINGLE_MATCH_CATS = {"단일일치", "단일일치(동형이의있음·등급일치)"}
    AMBIGUOUS_CATS = {"다중후보", "표제어만일치"}

    crossref_rows = []
    bucket_counter = defaultdict(int)
    for r in grade5_rows:
        lemma, pos_field, hom_no = r["어휘"], r["품사"], r["동형번호"]
        pos_tokens = norm_pos(pos_field or "")
        pos_candidates = []
        for p in pos_tokens:
            pos_candidates.extend(by_lemma_pos.get((lemma, p), []))
        seen_ids = set()
        dedup_pos_candidates = []
        for c in pos_candidates:
            if c["content_id"] in seen_ids:
                continue
            seen_ids.add(c["content_id"])
            dedup_pos_candidates.append(c)
        lemma_only_candidates = by_lemma.get(lemma, [])
        any_hom_risk = any(hom_risk.get((lemma, p), False) for p in pos_tokens)

        # 품사까지 일치하는 DB content 중, 그 DB content가 이미 검증된
        # 매칭에서 "공식 5등급 단일일치"로 확정된 것이 있는가?
        confirmed_grade5 = [
            c for c in dedup_pos_candidates
            if c.get("공식_NIKL_2023_등급") == "5" and c.get("매칭_카테고리") in SINGLE_MATCH_CATS
        ]
        ambiguous_matches = [
            c for c in dedup_pos_candidates
            if c.get("매칭_카테고리") in AMBIGUOUS_CATS or c.get("공식_NIKL_2023_등급") != "5"
        ]

        if confirmed_grade5 and not any_hom_risk:
            bucket = "기존_콘텐츠와_연결_후보"
            matched_cid = confirmed_grade5[0]["content_id"]
        elif dedup_pos_candidates or lemma_only_candidates:
            # 품사까지 일치하지만 확정 등급5가 아니거나(다른 등급/다중후보),
            # 공식목록 안에서 동형이의 위험이 있거나, 표제어만 일치하는 경우
            bucket = "동형이의_품사_확인_필요"
            matched_cid = (confirmed_grade5[0]["content_id"] if confirmed_grade5
                           else (ambiguous_matches[0]["content_id"] if ambiguous_matches else ""))
        else:
            bucket = "신규_어휘_후보"
            matched_cid = ""

        bucket_counter[bucket] += 1
        crossref_rows.append({
            "어휘": lemma, "동형번호": hom_no, "품사": pos_field,
            "공식등급": 5, "분야": r["분야"],
            "분류": bucket,
            "매칭_content_id": matched_cid,
            "DB내_동일표제어_건수": len(lemma_only_candidates),
            "공식목록내_동형이의_위험": any_hom_risk,
        })

    with open(OUT_CROSSREF, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(crossref_rows[0].keys()))
        w.writeheader()
        w.writerows(crossref_rows)

    # ---- 49건 census와 교차검증(사전 조사 재현 확인용, 재분류 아님) ----
    census_path = REPO_ROOT / "data" / "import" / "nikl_l3_backfill_census_20260930.csv"
    with open(census_path, encoding="utf-8-sig") as f:
        census_49 = list(csv.DictReader(f))
    census_lemmas = {(row["lemma"], row["pos"]) for row in census_49}
    recovered = 0
    for cr in crossref_rows:
        key = (cr["어휘"], cr["품사"])
        if key in census_lemmas and cr["분류"] == "기존_콘텐츠와_연결_후보":
            recovered += 1

    # ---- 4절: 신규 후보에서 1차 검토 배치 100건(층화표본, seed 고정) ----
    new_rows = [r for r in crossref_rows if r["분류"] == "신규_어휘_후보"]

    def primary_pos(pos_field: str) -> str:
        toks = sorted(norm_pos(pos_field or ""))
        return toks[0] if toks else "품사없음"

    def is_specialized(domain_field: str) -> bool:
        return "전문어" in (domain_field or "")

    strata = defaultdict(list)
    for r in new_rows:
        key = (primary_pos(r["품사"]), is_specialized(r["분야"]))
        strata[key].append(r)

    total_new = len(new_rows)
    target = 100
    rng = random.Random(20261001)  # 고정 시드 - 재실행 시 동일 표본 재현

    # 비례배분 + 최소 1건 보장(표본이 있는 층에 한해), 반올림 오차는
    # 가장 큰 층에서 보정한다.
    raw_alloc = {}
    for key, items in strata.items():
        raw_alloc[key] = max(1, round(target * len(items) / total_new)) if items else 0
    alloc_sum = sum(raw_alloc.values())
    if alloc_sum != target:
        # 가장 큰 층 하나에서 차이만큼 가감
        biggest = max(raw_alloc, key=lambda k: len(strata[k]))
        raw_alloc[biggest] += (target - alloc_sum)

    batch100 = []
    for key, items in strata.items():
        n = min(raw_alloc.get(key, 0), len(items))
        sampled = rng.sample(items, n) if n > 0 else []
        pos_label, specialized = key
        for r in sampled:
            batch100.append({
                "표제어": r["어휘"], "품사": r["품사"], "공식등급": 5,
                "제안레벨": "경계(L3~L4)",
                "전문용어_위험": specialized,
                "고유명사_위험": False,  # 공식5등급 품사 체계에 '고유명사' 태그 자체가 없음(확인됨, 보고서 명시)
                "다의어_위험": bool(r["공식목록내_동형이의_위험"]),
                "선정_사유": f"층화표본({pos_label}/{'전문어' if specialized else '일반어'})",
            })

    # 100건에 못 미치면(배분 반올림 영향) 전체 신규후보 중 미선정분에서 보충
    if len(batch100) < target:
        picked_lemmas = {(b["표제어"], b["품사"]) for b in batch100}
        remaining = [r for r in new_rows if (r["어휘"], r["품사"]) not in picked_lemmas]
        extra = rng.sample(remaining, target - len(batch100))
        for r in extra:
            batch100.append({
                "표제어": r["어휘"], "품사": r["품사"], "공식등급": 5,
                "제안레벨": "경계(L3~L4)",
                "전문용어_위험": is_specialized(r["분야"]),
                "고유명사_위험": False,
                "다의어_위험": bool(r["공식목록내_동형이의_위험"]),
                "선정_사유": "배분 보충(비례배분 반올림 오차 보정)",
            })
    batch100 = batch100[:target]

    with open(OUT_BATCH100, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(batch100[0].keys()))
        w.writeheader()
        w.writerows(batch100)

    # ---- 요약 출력(영문 라벨 위주로 콘솔 인코딩 문제 회피, 상세는 JSON) ----
    summary = {
        "raw_rows": n_raw,
        "distinct_lemma": len(distinct_lemma),
        "distinct_lemma_hom": len(distinct_lemma_hom),
        "pos_counter": dict(pos_counter),
        "domain_has_special_rows": domain_has_special,
        "bucket_counter": dict(bucket_counter),
        "recovered_from_49census": recovered,
        "census_49_total": len(census_49),
        "new_candidate_total": total_new,
        "batch100_specialized": sum(1 for b in batch100 if b["전문용어_위험"]),
        "batch100_homonym_risk": sum(1 for b in batch100 if b["다의어_위험"]),
        "batch100_proper_noun": sum(1 for b in batch100 if b["고유명사_위험"]),
        "strata_count": len(strata),
    }
    with open(REPO_ROOT / "data" / "import" / "nikl_grade5_stats_20261001.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    summary_txt_path = Path(os.environ.get("TEMP", "/tmp")) / "grade5_summary.txt"
    with open(summary_txt_path, "w", encoding="utf-8") as f:
        for k, v in summary.items():
            f.write(f"{k}: {v}\n")


if __name__ == "__main__":
    main()
