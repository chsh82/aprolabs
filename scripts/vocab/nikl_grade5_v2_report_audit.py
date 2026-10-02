"""중등 어휘 레벨 분류기 v2 보고서(reports/nikl_grade5_classifier_v2_
development_20261005.md) 정합성 재검증 - 읽기 전용, API 호출 없음.

기존 예측 파일(nikl_grade5_batch1_verdicts_analysis_20261002.csv,
nikl_grade5_gemini_batch1_eval_20261003.csv,
nikl_grade5_batch2_verdicts_vs_model_20261004.csv,
nikl_grade5_v2_predictions_200_20261005.csv)만 다시 읽어 confusion
matrix·사람 L4 79건 전이표를 재계산한다. 재계산 결과 보고서 5절의
"보류→보류(계속)" 셀이 15건으로 기록돼 있었는데 실제로는 16건이다
(합계가 78 → 79로 맞아야 함) - 이 스크립트가 그 재발 방지 확인 도구다.

실행: python scripts/vocab/nikl_grade5_v2_report_audit.py
"""
from __future__ import annotations

import csv
import sys
import unicodedata
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"

HOLD_LABELS = (unicodedata.normalize("NFC", "검토 필요"), unicodedata.normalize("NFC", "경계 유지"))


def _nfc(s):
    return unicodedata.normalize("NFC", s) if isinstance(s, str) else s


def _read_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in list(r.keys()):
            if r[k] is not None:
                r[k] = _nfc(r[k])
    return rows


def load_merged_200() -> list[dict]:
    b1_human = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_batch1_verdicts_analysis_20261002.csv")}
    b1_v1 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_gemini_batch1_eval_20261003.csv")}
    b2 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_batch2_verdicts_vs_model_20261004.csv")}
    v2 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_v2_predictions_200_20261005.csv")}

    rows = []
    for cid, h in b1_human.items():
        rows.append({
            "candidate_id": cid, "batch_no": 1, "lemma": h["lemma"],
            "human": h["judgment"], "v1_pred": b1_v1[cid]["predicted_judgment"],
            "v2_pred": v2[cid]["v2_predicted_judgment"],
        })
    for cid, b in b2.items():
        rows.append({
            "candidate_id": cid, "batch_no": 2, "lemma": b["lemma"],
            "human": b["judgment"], "v1_pred": b["predicted_judgment"],
            "v2_pred": v2[cid]["v2_predicted_judgment"],
        })
    assert len(rows) == 200, len(rows)
    return rows


def bucket(label: str) -> str:
    return "보류" if label in HOLD_LABELS else label


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    rows = load_merged_200()

    l4_rows = [r for r in rows if r["human"] == "L4"]
    assert len(l4_rows) == 79, len(l4_rows)

    print("=== 사람 L4 79건 - (v1_pred, v2_pred) 전체 전이표(4x4, 0건 생략) ===")
    raw = Counter((r["v1_pred"], r["v2_pred"]) for r in l4_rows)
    for k in sorted(raw):
        print(" ", k, raw[k])
    print("합계:", sum(raw.values()), "(79여야 함)")

    print("\n=== 축약 전이표(L3/L4/보류 3범주) - 보고서 5절 표와 비교 ===")
    collapsed = Counter((bucket(r["v1_pred"]), bucket(r["v2_pred"])) for r in l4_rows)
    for k in sorted(collapsed):
        print(" ", k, collapsed[k])
    print("합계:", sum(collapsed.values()), "(79여야 함, 보고서는 78로 오기재돼 있었음)")

    v1_correct = {r["candidate_id"] for r in l4_rows if r["v1_pred"] == "L4"}
    v2_correct = {r["candidate_id"] for r in l4_rows if r["v2_pred"] == "L4"}
    gained = v2_correct - v1_correct
    lost = v1_correct - v2_correct
    print(f"\nv1 L4 정답 {len(v1_correct)}건 / v2 L4 정답 {len(v2_correct)}건")
    print(f"신규 정답(gained) {len(gained)}건, 정답 상실(lost) {len(lost)}건, 순증감 {len(gained) - len(lost)}")


if __name__ == "__main__":
    main()
