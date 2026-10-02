"""공식 5등급 전체 목록(17,000건)에 기본 추천 범위를 기록하고, 실제 사람이
검수한 200건(1·2차 배치)의 최신 L3/L4 판정을 별도 필드로 연결한다.

- 기본 추천 범위는 `vocabulary_official_grade_reference`에서 기존 49건의
  공식 5등급 매칭 행이 이미 쓰는 그 문자열("경계(L3~L4)")을 그대로 재사용한다
  (새 라벨을 만들지 않음 - 공식 근거 표현을 통일).
- 연결은 (어휘,품사,동형번호) == (lemma,pos,homonym_number) **완전 일치**로만
  한다 - 표제어만 일치하는 경우는 연결하지 않는다(사람 판정 자동 전파 금지).
  200건 전부 완전 일치 1:1임을 사전에 직접 검증했다(아래 ASSERT).
- 공식 근거(공식등급/기본추천범위) · 모델 제안(v1/v2/v3, 참고용·화면 비노출) ·
  사람 판정(사람판정_L3L4) 세 가지를 서로 다른 컬럼에 남기고 어느 것도
  다른 것으로 덮어쓰지 않는다.
- 판정이 연결되지 않은 16,800건(=17,000-200)은 기본추천범위를 그대로
  "경계(L3~L4)"로 유지한다 - L3나 L4 중 하나로 임의로 좁히지 않는다.

읽기 전용 - 이 스크립트는 DB에 쓰지 않는다. 입력 CSV는 전부 기존 산출물
(크로스레프는 연구 DB 조회로 이미 로컬에 있고, 200건 판정은 이번에 읽기
전용 SSH 조회로 새로 내렸다).
"""
from __future__ import annotations

import csv
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"

DEFAULT_RANGE = unicodedata.normalize("NFC", "경계(L3~L4)")


def nfc(s):
    return unicodedata.normalize("NFC", s) if isinstance(s, str) else s


def read_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in list(r.keys()):
            if r[k] is not None:
                r[k] = nfc(r[k])
    return rows


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    crossref = read_csv(IMP / "nikl_grade5_crossref_20261001.csv")
    human = read_csv(IMP / "nikl_grade5_200_latest_human_judgment_20261006.csv")
    v1 = {r["candidate_id"]: r for r in read_csv(IMP / "nikl_grade5_batch1_verdicts_analysis_20261002.csv")}
    v1b = {r["candidate_id"]: r for r in read_csv(IMP / "nikl_grade5_gemini_batch1_eval_20261003.csv")}
    b2 = {r["candidate_id"]: r for r in read_csv(IMP / "nikl_grade5_batch2_verdicts_vs_model_20261004.csv")}
    v2 = {r["candidate_id"]: r for r in read_csv(IMP / "nikl_grade5_v2_predictions_200_20261005.csv")}
    v3 = {r["candidate_id"]: r for r in read_csv(IMP / "nikl_grade5_v3_predictions_200_20261006.csv")}

    def v1_pred(cid: str, batch_no: str) -> str:
        if batch_no == "1":
            return v1b.get(cid, {}).get("predicted_judgment", "")
        return b2.get(cid, {}).get("predicted_judgment", "")

    human_by_key = {}
    for r in human:
        key = (r["lemma"], r["pos"], r["homonym_number"])
        human_by_key[key] = r

    assert len(crossref) == 17000, len(crossref)
    assert len(human) == 200, len(human)

    matched = 0
    out_rows = []
    for r in crossref:
        key = (r["어휘"], r["품사"], r["동형번호"])
        h = human_by_key.get(key)
        row = dict(r)
        row["기본추천범위"] = DEFAULT_RANGE
        if h is not None:
            matched += 1
            cid = h["candidate_id"]
            row["연결확인상태"] = "사람검수완료(완전일치)"
            row["사람판정_candidate_id"] = cid
            row["사람판정_batch_no"] = h["batch_no"]
            row["사람판정_L3L4"] = h["human_judgment"]
            row["사람판정_이유"] = h["rationale"]
            row["사람판정_시각"] = h["reviewed_at"]
            row["참고_v1예측(비노출)"] = v1_pred(cid, h["batch_no"])
            row["참고_v2예측(비노출)"] = v2.get(cid, {}).get("v2_predicted_judgment", "")
            row["참고_v3예측(비노출)"] = v3.get(cid, {}).get("v3_predicted_judgment", "")
        else:
            row["연결확인상태"] = "미검수"
            row["사람판정_candidate_id"] = ""
            row["사람판정_batch_no"] = ""
            row["사람판정_L3L4"] = ""
            row["사람판정_이유"] = ""
            row["사람판정_시각"] = ""
            row["참고_v1예측(비노출)"] = ""
            row["참고_v2예측(비노출)"] = ""
            row["참고_v3예측(비노출)"] = ""
        out_rows.append(row)

    assert matched == 200, f"expected 200 exact matches, got {matched}"
    assert len(out_rows) == 17000

    fieldnames = list(crossref[0].keys()) + [
        "기본추천범위", "연결확인상태", "사람판정_candidate_id", "사람판정_batch_no",
        "사람판정_L3L4", "사람판정_이유", "사람판정_시각",
        "참고_v1예측(비노출)", "참고_v2예측(비노출)", "참고_v3예측(비노출)",
    ]
    out_path = IMP / "nikl_grade5_official_reference_with_human_20261006.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    print(f"17,000건 중 {matched}건 사람 판정 연결, 나머지 {17000-matched}건은 "
          f"기본추천범위={DEFAULT_RANGE!r}로 경계 상태 유지 -> {out_path}")


if __name__ == "__main__":
    main()
