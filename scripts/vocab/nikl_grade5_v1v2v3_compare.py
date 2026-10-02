"""v1/v2/v3 200건 비교 - 12건(대표 사례, v2 설계에 이미 참고됨) vs 188건(나머지)을
나눠서 본다. 200건 전체가 v1/v2/v3 중 어느 하나의 설계에든 참고된 적이 있으므로
이 비교는 "독립 평가"가 아니라 **개발자료 재평가**다(어떤 분모로도 독립 평가라고
부르지 않는다).

읽기 전용 - 기존 예측 파일만 다시 읽는다(v1/v2는 재호출 없음, v3는 캐시 파일을
읽는다 - 이 스크립트 자체는 API를 호출하지 않는다).
"""
from __future__ import annotations

import csv
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"

REP12_IDS = {
    "G5-8ce1ebfbf28902b8", "G5-75db47a8af772ebf", "G5-8eb7191f007f9d70",
    "G5-5047276d84610bfb", "G5-1db1883c75c2d50a", "G5-ea479b2f55daa588",
    "G5-24f9f70b64fbfd3c", "G5-22b1c6b714665ce3", "G5-5a623b21f6f73b62",
    "G5-5b6159140976c02f", "G5-43238e3a751baddf", "G5-7627eeba20ba7a7f",
}
HOLD = {unicodedata.normalize("NFC", "검토 필요"), unicodedata.normalize("NFC", "경계 유지")}


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


def load_200() -> list[dict]:
    b1_human = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_batch1_verdicts_analysis_20261002.csv")}
    b1_v1 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_gemini_batch1_eval_20261003.csv")}
    b2 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_batch2_verdicts_vs_model_20261004.csv")}
    v2 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_v2_predictions_200_20261005.csv")}
    v3 = {r["candidate_id"]: r for r in _read_csv(IMP / "nikl_grade5_v3_predictions_200_20261006.csv")}

    rows = []
    for cid, h in b1_human.items():
        rows.append({
            "candidate_id": cid, "batch_no": 1, "lemma": h["lemma"],
            "human": h["judgment"], "v1": b1_v1[cid]["predicted_judgment"],
            "v2": v2[cid]["v2_predicted_judgment"], "v3": v3[cid]["v3_predicted_judgment"],
            "rep12": cid in REP12_IDS,
        })
    for cid, b in b2.items():
        rows.append({
            "candidate_id": cid, "batch_no": 2, "lemma": b["lemma"],
            "human": b["judgment"], "v1": b["predicted_judgment"],
            "v2": v2[cid]["v2_predicted_judgment"], "v3": v3[cid]["v3_predicted_judgment"],
            "rep12": cid in REP12_IDS,
        })
    assert len(rows) == 200, len(rows)
    assert sum(1 for r in rows if r["rep12"]) == 12
    return rows


def metrics(rows: list[dict], pred_key: str) -> dict:
    n = len(rows)
    l3 = [r for r in rows if r["human"] == "L3"]
    l4 = [r for r in rows if r["human"] == "L4"]
    hold = sum(1 for r in rows if r[pred_key] in HOLD)
    forced_n = n - hold
    correct = sum(1 for r in rows if r[pred_key] == r["human"])
    l3_recall = sum(1 for r in l3 if r[pred_key] == "L3") / len(l3) if l3 else float("nan")
    l4_recall = sum(1 for r in l4 if r[pred_key] == "L4") / len(l4) if l4 else float("nan")
    l4_to_l3 = sum(1 for r in l4 if r[pred_key] == "L3")
    l3_to_l4 = sum(1 for r in l3 if r[pred_key] == "L4")
    return {
        "n": n, "hold": hold, "forced_n": forced_n,
        "overall_acc": correct / n if n else float("nan"),
        "forced_acc": correct / forced_n if forced_n else float("nan"),
        "coverage": forced_n / n if n else float("nan"),
        "l3_n": len(l3), "l4_n": len(l4),
        "l3_recall": l3_recall, "l4_recall": l4_recall,
        "l4_to_l3": l4_to_l3, "l4_to_l3_rate": l4_to_l3 / len(l4) if l4 else float("nan"),
        "l3_to_l4": l3_to_l4, "l3_to_l4_rate": l3_to_l4 / len(l3) if l3 else float("nan"),
    }


def fmt(m: dict) -> str:
    return (
        f"n={m['n']} 보류={m['hold']} forced_n={m['forced_n']} | "
        f"전체정확도={m['overall_acc']:.1%} forced정확도={m['forced_acc']:.1%} "
        f"커버리지={m['coverage']:.1%} | "
        f"L3재현율={m['l3_recall']:.1%}({m['l3_n']}) L4재현율={m['l4_recall']:.1%}({m['l4_n']}) | "
        f"L4->L3={m['l4_to_l3']}/{m['l4_n']}({m['l4_to_l3_rate']:.1%}) "
        f"L3->L4={m['l3_to_l4']}/{m['l3_n']}({m['l3_to_l4_rate']:.1%})"
    )


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    rows = load_200()
    rep12 = [r for r in rows if r["rep12"]]
    rest188 = [r for r in rows if not r["rep12"]]
    assert len(rep12) == 12 and len(rest188) == 188

    for label, subset in (("200건 전체", rows), ("대표 12건", rep12), ("나머지 188건", rest188)):
        print(f"\n=== {label} ===")
        for v in ("v1", "v2", "v3"):
            print(f"  {v}: {fmt(metrics(subset, v))}")

    print("\n=== 대표 12건 - v3가 사람 판정과 일치했는가(문항별) ===")
    for r in rep12:
        mark = "OK" if r["v3"] == r["human"] else ("보류" if r["v3"] in HOLD else "불일치")
        print(f"  {r['lemma']:8s} 사람={r['human']} v1={r['v1']:8s} v2={r['v2']:10s} v3={r['v3']:10s} [{mark}]")


if __name__ == "__main__":
    main()
