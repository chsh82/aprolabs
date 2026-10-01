# -*- coding: utf-8 -*-
"""2차 배치(100건) Gemini 분류기 독립 평가 - 재실행 스크립트.

사람 검수(vocabulary_grade5_candidate_judgments, batch_no=2)가 완료된 뒤,
검수 시작 전에 고정해 둔 Gemini 예측 파일
(data/import/nikl_grade5_gemini_batch2_predictions_20261003.csv)과
candidate_id 기준으로 대조한다. API 재호출도, 예측 파일 수정도 없다 -
이 스크립트는 순수 집계만 한다.

1차 배치(개발 자료)와 이 2차 배치(독립 평가) 성능은 어디서도 합쳐
집계하지 않는다.

실행:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \
        python scripts/vocab/nikl_grade5_batch2_evaluation.py
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
PREDICTIONS_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_gemini_batch2_predictions_20261003.csv"
OUTPUT_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_batch2_verdicts_vs_model_20261004.csv"


def _batch_version_snapshot(row: dict) -> str:
    """app/vocabulary_quiz/grade5_candidate_review.py의 _batch_version_snapshot()과
    동일한 해시 - 판정 만료(stale) 여부를 독립적으로 재계산하기 위해 복제했다."""
    blob = json.dumps({
        "lemma": row["lemma"], "pos": row["pos"], "homonym_number": row["homonym_number"],
        "official_grade": row["official_grade"], "proposed_level_note": row["proposed_level_note"],
        "source_file_sha256": row["source_file_sha256"], "computed_at": row["computed_at"],
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def fetch_batch2_judgments(db_path: str) -> dict:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = con.cursor()
    cur.execute("""
        SELECT b.candidate_id, b.lemma, b.pos, b.homonym_number, b.specialized_domain_flag,
               b.polysemy_risk_flag, b.proper_noun_risk_flag, b.official_grade,
               b.proposed_level_note, b.source_file_sha256, b.computed_at,
               j.id, j.judgment, j.rationale, j.reviewed_at, j.source_data_version_at_review
        FROM vocabulary_grade5_candidate_batch b
        LEFT JOIN vocabulary_grade5_candidate_judgments j ON j.candidate_id = b.candidate_id
        WHERE b.batch_no = 2
        ORDER BY b.candidate_id, j.reviewed_at
    """)
    rows = cur.fetchall()
    con.close()

    by_cand: dict[str, list] = defaultdict(list)
    for r in rows:
        by_cand[r[0]].append(r)
    return by_cand


def main() -> None:
    db_path = os.environ.get("VOCABULARY_QUIZ_DB_PATH")
    if not db_path:
        print("VOCABULARY_QUIZ_DB_PATH 미설정 - 중단")
        sys.exit(1)

    by_cand = fetch_batch2_judgments(db_path)
    print(f"고유 candidate_id 수: {len(by_cand)}")

    no_judg = [cid for cid, rs in by_cand.items() if rs[0][11] is None]
    dup = {cid: len(rs) for cid, rs in by_cand.items() if len(rs) > 1}
    print(f"판정없음: {len(no_judg)}  중복이력: {len(dup)}")

    stale = 0
    human = {}
    for cid, rs in by_cand.items():
        latest = rs[-1]
        row = {
            "candidate_id": cid, "lemma": latest[1], "pos": latest[2], "homonym_number": latest[3],
            "specialized_domain_flag": latest[4], "polysemy_risk_flag": latest[5],
            "proper_noun_risk_flag": latest[6], "official_grade": latest[7],
            "proposed_level_note": latest[8], "source_file_sha256": latest[9], "computed_at": latest[10],
        }
        judgment, rationale, reviewed_at, snap = latest[12], latest[13], latest[14], latest[15]
        if snap is not None and snap != _batch_version_snapshot(row):
            stale += 1
        human[cid] = {**row, "judgment": judgment, "rationale": rationale, "reviewed_at": reviewed_at}

    print(f"stale(원천 변경 후 만료): {stale}")
    dist = Counter(h["judgment"] for h in human.values())
    print(f"판정 분포: {dict(dist)}")

    # 예측 파일 로드 (API 재호출 없음)
    with open(PREDICTIONS_CSV, encoding="utf-8-sig") as f:
        pred_rows = list(csv.DictReader(f))
    pred = {r["candidate_id"]: r for r in pred_rows}

    assert set(human.keys()) == set(pred.keys()), "candidate_id 집합 불일치 - 예측 파일과 배치가 다름"
    assert all(r["api_error"] == "False" for r in pred_rows), "api_error가 있는 행이 섞여 있음"

    merged = []
    for cid, h in human.items():
        p = pred[cid]
        merged.append({**h, "predicted_judgment": p["predicted_judgment"],
                        "predicted_reason": p["predicted_reason"], "grounded": p["grounded"],
                        "borderline": p["borderline"], "match": h["judgment"] == p["predicted_judgment"]})

    crosstab = Counter((m["judgment"], m["predicted_judgment"]) for m in merged)
    print("\n교차표:")
    for k, v in sorted(crosstab.items()):
        print(" ", k, v)

    l3l4 = [m for m in merged if m["judgment"] in ("L3", "L4")]
    n_total = len(l3l4)
    n_match = sum(1 for m in l3l4 if m["match"])
    forced = [m for m in l3l4 if m["predicted_judgment"] in ("L3", "L4")]
    n_forced = len(forced)
    n_forced_correct = sum(1 for m in forced if m["match"])
    print(f"\n전체 정확도: {n_match}/{n_total} = {n_match/n_total:.1%}")
    print(f"Forced 정확도: {n_forced_correct}/{n_forced} = {n_forced_correct/n_forced:.1%} (커버리지 {n_forced/n_total:.1%})")

    human_l3 = [m for m in l3l4 if m["judgment"] == "L3"]
    human_l4 = [m for m in l3l4 if m["judgment"] == "L4"]
    l3_recall = sum(1 for m in human_l3 if m["predicted_judgment"] == "L3") / len(human_l3)
    l4_recall = sum(1 for m in human_l4 if m["predicted_judgment"] == "L4") / len(human_l4)
    l4_to_l3 = sum(1 for m in human_l4 if m["predicted_judgment"] == "L3")
    print(f"L3 재현율: {l3_recall:.1%} (n={len(human_l3)})")
    print(f"L4 재현율: {l4_recall:.1%} (n={len(human_l4)}) | 사람L4->모델L3: {l4_to_l3}건 ({l4_to_l3/len(human_l4):.1%})")

    maj = dist["L3"] / n_total
    print(f"\n다수결 베이스라인(항상 L3): {maj:.1%}")

    cols = ["candidate_id", "lemma", "pos", "homonym_number", "specialized_domain_flag",
            "polysemy_risk_flag", "proper_noun_risk_flag", "judgment", "rationale", "reviewed_at",
            "predicted_judgment", "predicted_reason", "grounded", "borderline", "match"]
    with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for m in merged:
            w.writerow({k: m.get(k, "") for k in cols})
    print(f"\n산출: {OUTPUT_CSV} ({len(merged)}행)")


if __name__ == "__main__":
    main()
