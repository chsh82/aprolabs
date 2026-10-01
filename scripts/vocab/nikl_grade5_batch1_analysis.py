# -*- coding: utf-8 -*-
"""중등 어휘 레벨 검수 1차 배치(100건) 후속 분석 + 최소정보 분류기 초안.

읽기 전용: vocabulary_grade5_candidate_batch/judgments 테이블을 SELECT만
한다. vocab_level/문항/매니페스트/공개 플래그/momolib 전혀 건드리지 않음.

분류기 입력 피처는 사용자 지시대로 "표제어·품사·공식등급·짧은 원문
뜻풀이·전문어 분야"로 제한한다. 공식등급은 이 배치 전체가 '5'로
상수라 식별력이 없다(그대로 포함은 하되 무의미함을 보고서에 명시).
기존 DB vocab_level은 전혀 입력/정답으로 쓰지 않는다(애초에 이
100건은 DB에 없는 신규 어휘라 vocab_level 자체가 존재하지 않는다).

표본이 100건뿐이고 sklearn도 설치돼 있지 않아(확인함), 해석 가능한
단순 규칙(뜻풀이 길이 임계값 + 전문어 보정)을 쓴다 - 복잡한 모델을
쓸 재료도, 쓸 이유도 없다는 점을 설계 근거로 명시한다.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import sys
from collections import Counter
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
JOINED_JSON = REPO_ROOT / "scratch_grade5_joined.json"
CROSSREF_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_crossref_20261001.csv"
FULL_LIST_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_full_20261001.csv"
BATCH1_ENRICHED_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_first_batch_100_ENRICHED_20261001.csv"

OUT_ANALYSIS = REPO_ROOT / "data" / "import" / "nikl_grade5_batch1_verdicts_analysis_20261002.csv"
OUT_VALIDATION = REPO_ROOT / "data" / "import" / "nikl_grade5_classifier_validation_20261002.csv"
OUT_BATCH2 = REPO_ROOT / "data" / "import" / "nikl_grade5_batch2_candidates_20261002.csv"
OUT_REPORT = REPO_ROOT / "reports" / "nikl_grade5_batch1_analysis_and_classifier_20261002.md"

SEED = 20261002


def load_joined():
    with open(JOINED_JSON, encoding="utf-8") as f:
        d = json.load(f)
    cols = d["cols"]
    idx = {c: i for i, c in enumerate(cols)}
    return [dict(zip(cols, row)) for row in d["rows"]], idx


# ==================== Job 1+2: 정합성 + 분포 ====================

def batch_version_snapshot(row: dict) -> str:
    blob = json.dumps({
        "lemma": row["lemma"], "pos": row["pos"], "homonym_number": row["homonym_number"],
        "official_grade": row["official_grade"], "proposed_level_note": row["proposed_level_note"],
        "source_file_sha256": row["source_file_sha256"], "computed_at": row["computed_at"],
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def job1_job2(rows: list[dict]) -> dict:
    verdict_counts = Counter(r["judgment"] for r in rows)
    stale = [r for r in rows if r["source_data_version_at_review"] != batch_version_snapshot(r)]
    rationale_filled = [r for r in rows if r.get("rationale")]
    lemma_counts = Counter(r["lemma"] for r in rows)
    dup_lemmas = {k: v for k, v in lemma_counts.items() if v > 1}

    def crosstab(field):
        c: dict = {}
        for r in rows:
            key = r[field]
            c.setdefault(key, Counter())[r["judgment"]] += 1
        return {k: dict(v) for k, v in sorted(c.items(), key=lambda kv: str(kv[0]))}

    l3_lens = [len(r["official_meaning_short"] or "") for r in rows if r["judgment"] == "L3"]
    l4_lens = [len(r["official_meaning_short"] or "") for r in rows if r["judgment"] == "L4"]

    return {
        "verdict_counts": dict(verdict_counts),
        "total": len(rows),
        "stale_count": len(stale),
        "stale_ids": [r["candidate_id"] for r in stale],
        "rationale_filled_count": len(rationale_filled),
        "dup_lemma_within_batch": dup_lemmas,
        "by_pos": crosstab("pos"),
        "by_specialized": crosstab("specialized_domain_flag"),
        "by_polysemy": crosstab("polysemy_risk_flag"),
        "by_proper_noun": crosstab("proper_noun_risk_flag"),
        "avg_meaning_len_L3": sum(l3_lens) / len(l3_lens) if l3_lens else None,
        "avg_meaning_len_L4": sum(l4_lens) / len(l4_lens) if l4_lens else None,
        "n_L3": len(l3_lens),
        "n_L4": len(l4_lens),
    }


# ==================== Job 3: 분류기 ====================
# 피처: official_meaning_short 길이(문자수), specialized_domain_flag.
# 표제어 자체(lemma)는 식별자라 일반화 피처로 못 쓰고(차원이 n과 같음),
# 공식등급은 이 배치 전체가 '5' 상수라 식별력 0 - 둘 다 "입력에는
# 포함하되 분류 규칙에는 실질적으로 안 쓴다"고 명시한다.

def predict(meaning_len: int, specialized: int, threshold: float, margin: float) -> tuple[str, float]:
    """규칙: 뜻풀이 길이가 길수록 L4(중3) 쪽으로 본다(관찰된 효과 크기
    기반). 전문어 플래그는 L4 쪽으로 소폭 가중(+5자 상당)한다.
    threshold 근방(margin 이내)이면 저신뢰 -> 경계 유지로 출력한다."""
    adjusted = meaning_len + (5 if specialized else 0)
    dist = adjusted - threshold
    if abs(dist) <= margin:
        return "경계 유지", 0.5
    if dist > 0:
        conf = min(0.5 + abs(dist) / (threshold * 2), 0.95)
        return "L4", conf
    conf = min(0.5 + abs(dist) / (threshold * 2), 0.95)
    return "L3", conf


def fit_threshold(train_rows: list[dict]) -> float:
    """train 분할 안에서 0/1 오류를 최소화하는 길이 임계값을 단순 스캔으로 찾는다."""
    candidates = sorted(set(len(r["official_meaning_short"] or "") for r in train_rows))
    best_t, best_err = candidates[0], len(train_rows) + 1
    for t in candidates:
        err = 0
        for r in train_rows:
            length = len(r["official_meaning_short"] or "") + (5 if r["specialized_domain_flag"] else 0)
            pred = "L4" if length > t else "L3"
            if pred != r["judgment"]:
                err += 1
        if err < best_err:
            best_err, best_t = err, t
    return best_t


def job3_job4(rows: list[dict]) -> dict:
    lemma_counts = Counter(r["lemma"] for r in rows)
    grouping_needed = any(v > 1 for v in lemma_counts.values())

    rng = random.Random(SEED)
    shuffled = rows[:]
    rng.shuffle(shuffled)
    k = 5
    folds = [shuffled[i::k] for i in range(k)]

    all_preds = []
    for i in range(k):
        val = folds[i]
        train = [r for j, f in enumerate(folds) if j != i for r in f]
        threshold = fit_threshold(train)
        margin = threshold * 0.08  # 완만한 저신뢰 구간 - 임계값의 8%
        for r in val:
            length = len(r["official_meaning_short"] or "")
            pred, conf = predict(length, r["specialized_domain_flag"], threshold, margin)
            all_preds.append({
                "candidate_id": r["candidate_id"], "lemma": r["lemma"], "pos": r["pos"],
                "actual_judgment": r["judgment"], "predicted": pred, "confidence": round(conf, 3),
                "fold": i, "threshold_used": threshold,
            })

    # 저신뢰('경계 유지') 제외하고 L3/L4 강제예측만 정확도 계산 + 전체(저신뢰 포함) 둘 다 보고
    forced = [p for p in all_preds if p["predicted"] in ("L3", "L4")]
    correct_forced = sum(1 for p in forced if p["predicted"] == p["actual_judgment"]) if forced else 0
    acc_forced = correct_forced / len(forced) if forced else 0.0
    n_boundary_out = len(all_preds) - len(forced)

    majority_label = Counter(r["judgment"] for r in rows).most_common(1)[0][0]
    majority_acc = sum(1 for r in rows if r["judgment"] == majority_label) / len(rows)

    errors = [p for p in forced if p["predicted"] != p["actual_judgment"]]

    return {
        "lemma_dup_in_batch": grouping_needed,
        "n_folds": k,
        "all_preds": all_preds,
        "forced_accuracy": acc_forced,
        "n_forced": len(forced),
        "n_boundary_output": n_boundary_out,
        "majority_label": majority_label,
        "majority_accuracy": majority_acc,
        "errors": errors,
    }


# ==================== Job 5: 다음 배치 ====================

def job5_next_batch(batch1_candidate_ids: set, final_threshold: float, final_margin: float) -> list[dict]:
    """crossref(분류 태그)와 full-list(의미 원문)를 (어휘,동형번호,품사)
    키로 조인해 '신규_어휘_후보' 중 batch1과 겹치지 않는 풀에서 품사x전문어
    층화표본 최대 100건을 뽑고, 분류기 제안 레벨/신뢰도를 붙인다.
    사람_판정 컬럼은 의도적으로 공란."""
    if not CROSSREF_CSV.exists() or not FULL_LIST_CSV.exists():
        return []

    with open(FULL_LIST_CSV, encoding="utf-8-sig") as f:
        full_rows = list(csv.DictReader(f))
    meaning_by_key = {(r["어휘"], r["동형번호"], r["품사"]): r["의미"] for r in full_rows}

    with open(CROSSREF_CSV, encoding="utf-8-sig") as f:
        cross_rows = list(csv.DictReader(f))

    new_cands = [r for r in cross_rows if r.get("분류") == "신규_어휘_후보"]

    lemma_pos_hom_1 = set()
    with open(JOINED_JSON, encoding="utf-8") as f:
        d = json.load(f)
    cols = d["cols"]; idx = {c: i for i, c in enumerate(cols)}
    for row in d["rows"]:
        lemma_pos_hom_1.add((row[idx["lemma"]], row[idx["pos"]], str(row[idx["homonym_number"]])))

    def key3(r):
        return (r["어휘"], r["동형번호"], r["품사"])

    remaining = [r for r in new_cands if key3(r) not in lemma_pos_hom_1]

    rng = random.Random(SEED)
    rng.shuffle(remaining)
    n_target = min(100, len(remaining))
    selected = _stratified_pick(remaining, "품사", "분야", n_target, rng)

    out = []
    for r in selected:
        meaning = meaning_by_key.get(key3(r), "")
        specialized = 1 if "전문어" in str(r.get("분야") or "") else 0
        pred, conf = predict(len(meaning), specialized, final_threshold, final_margin)
        out.append({
            "lemma": r["어휘"], "pos": r["품사"], "homonym_number": r["동형번호"],
            "official_meaning_short": meaning, "specialized_domain_flag": specialized,
            "polysemy_risk_signal": r.get("공식목록내_동형이의_위험", ""),
            "자동_제안_레벨": pred, "자동_제안_신뢰도": round(conf, 3),
            "사람_판정": "",  # 의도적으로 공란
        })
    return out


def _stratified_pick(remaining, pos_col, spec_col, n_target, rng):
    strata: dict = {}
    for r in remaining:
        pos = r.get(pos_col) or "미상"
        spec = "전문어" in str(r.get(spec_col) or "")
        strata.setdefault((pos, spec), []).append(r)
    total = len(remaining)
    picked = []
    for key, items in strata.items():
        rng.shuffle(items)
        quota = max(1, round(len(items) / total * n_target))
        picked.extend(items[:quota])
    rng.shuffle(picked)
    return picked[:n_target]


def main():
    rows, idx = load_joined()
    j1j2 = job1_job2(rows)
    j3j4 = job3_job4(rows)

    # 최종 임계값: 전체 100건으로 재적합(배치2 제안용, 검증 자체는 CV로 이미 수행)
    final_threshold = fit_threshold(rows)
    final_margin = final_threshold * 0.08

    batch1_ids = set(r["candidate_id"] for r in rows)
    batch2 = job5_next_batch(batch1_ids, final_threshold, final_margin)

    # ---- 출력 파일 ----
    with open(OUT_ANALYSIS, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["candidate_id", "lemma", "pos", "homonym_number", "judgment", "rationale",
                      "reviewed_at", "stale", "specialized_domain_flag", "polysemy_risk_flag",
                      "proper_noun_risk_flag", "official_meaning_short"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        stale_ids = set(j1j2["stale_ids"])
        for r in rows:
            w.writerow({
                "candidate_id": r["candidate_id"], "lemma": r["lemma"], "pos": r["pos"],
                "homonym_number": r["homonym_number"], "judgment": r["judgment"],
                "rationale": r.get("rationale") or "", "reviewed_at": r["reviewed_at"],
                "stale": "Y" if r["candidate_id"] in stale_ids else "N",
                "specialized_domain_flag": r["specialized_domain_flag"],
                "polysemy_risk_flag": r["polysemy_risk_flag"],
                "proper_noun_risk_flag": r["proper_noun_risk_flag"],
                "official_meaning_short": r["official_meaning_short"],
            })

    with open(OUT_VALIDATION, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["candidate_id", "lemma", "pos", "fold", "actual_judgment", "predicted",
                      "confidence", "threshold_used", "correct"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for p in j3j4["all_preds"]:
            w.writerow({**p, "correct": (p["predicted"] == p["actual_judgment"])})

    with open(OUT_BATCH2, "w", encoding="utf-8-sig", newline="") as f:
        if batch2:
            w = csv.DictWriter(f, fieldnames=list(batch2[0].keys()))
            w.writeheader()
            w.writerows(batch2)
        else:
            f.write("lemma,pos,homonym_number,official_meaning_short,specialized_domain_flag,자동_제안_레벨,자동_제안_신뢰도,사람_판정\n")

    summary = {
        "job1_job2": j1j2,
        "job3_job4": {k: v for k, v in j3j4.items() if k != "all_preds"},
        "final_threshold": final_threshold,
        "batch2_count": len(batch2),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
