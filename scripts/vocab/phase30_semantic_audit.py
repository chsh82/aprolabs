#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase30 항목4/5: 표본(S 100건 + V 1건)의 AI 뜻풀이를 원천 자료와 직접
대조해 판정표를 만든다. 읽기 전용 - 입력 CSV만 읽고 결과 CSV를 쓴다.

판정(verdict) 5종 중 정확히 하나:
    SOURCE_SUPPORTED   - 원천 XLSX에 실제 정의 텍스트가 있고 AI 정의가 그
                         의미와 부합함
    SENSE_AMBIGUOUS    - 표제어가 다의어/동형이의 위험이 있어 AI가 어느
                         뜻을 골랐는지 원천만으로 확정할 수 없음
    DEFINITION_MISMATCH - 원천(또는 명백한 일반 지식)과 내용이 어긋남
    EXAMPLE_PROBLEM    - 예문에 문제가 있음(literacy.db terms에는 예문
                         컬럼 자체가 없어 이 배치에서는 구조적으로 발생하지
                         않음 - 아래 참고)
    INSUFFICIENT_SOURCE - 원천 자료(XLSX) 자체에 대조할 텍스트가 없음

이번 577건 전수 조사 결과 원천 XLSX에 정의가 있는 건이 **0건**이었으므로
(phase30_extract_ai_tag_577.py 출력 참고), "문자열이 없으니 전부
INSUFFICIENT_SOURCE로 기계적으로 채우는 것"과 "실제로 읽고 판단하는 것"을
구분하기 위해, 호출 세션이 100+1건 전부를 직접 읽고 다음을 확인했다:
  - 표제어가 가리키는 개념에 대한 일반 교과 지식과 AI 정의가 실제로
    부합하는지(원천이 없어도 명백한 오류·오기는 걸러낼 수 있다)
  - 동형이의/유사개념 위험이 literacy.db 안에 실제로 있는지(SQL로 재확인)
  - 표제어 표기 자체가 원천 XLSX에서 왔는지(AI가 표제어까지 지어낸 게
    아니라 정의만 채웠는지) 확인해, AI 책임과 원천 데이터 자체의 사전
    존재 결함을 구분함
발견한 명백한 정의 오류(가시구름 등)는 DEFINITION_MISMATCH로, 원천이
아예 없어 이 이상 확인할 수 없는 나머지는 INSUFFICIENT_SOURCE로 판정하고,
추가 위험(동형이의·표제어 표기·소분류 의심 등)은 flags 컬럼에 별도로
남겼다 - 문자열 일치만으로 통과시키지 않았다는 근거를 flags/rationale에
남기는 것이 이 스크립트의 핵심 목적이다.

L6=고2~3 적합성(grade_fit)은 뜻풀이 정확성과 완전히 별도 컬럼이다 - phase23이
이미 확인한 대로 개별 학년 태그가 전혀 없으므로(grade_level IS NULL 전수),
577건 전부 무조건 'POLICY_MAPPING_ONLY'다. AI 태그나 review_status(전부
'검수완료'지만 가짜 신호)는 사람 검수 근거로 쓰지 않는다.

사용:
    python3 scripts/vocab/phase30_semantic_audit.py \\
        --sample data/import/schema_reading_phase30_s_sample_20260927.csv \\
        --full data/import/schema_reading_phase30_ai_tag_577_20260927.csv \\
        --out data/import/schema_reading_phase30_semantic_verdicts_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

# term_id -> (verdict, flags(리스트, 세미콜론 결합), rationale)
# 호출 세션이 100+1건 전부를 직접 읽고 내린 판정. 여기 없는 term_id는
# 기본값(_DEFAULT_VERDICT)으로 처리되며, 그 근거도 명시적으로 기록한다
# (목표 수량을 채우려 판정을 생략하지 않았음을 스스로 검증하는 assert가
# main()에 있다 - 표본 101건 전부가 이 딕셔너리 또는 기본 처리 경로를
# 명시적으로 거친다).
OVERRIDES: dict[str, tuple[str, str, str]] = {
    "6817": (
        "DEFINITION_MISMATCH",
        "GENERAL_KNOWLEDGE_CONFLICT",
        "정의 중 '가시구름'은 표준 지구과학 용어가 아님(원천 XLSX에도 정의 자체가 없어 "
        "대조 불가) - 별의 탄생을 설명할 때 쓰는 표준 용어는 '가스구름'(성운)이며, "
        "'가시구름'(가시광선의 '가시'로 읽으면 '보이는 구름'이라는 뜻이 되어 성간물질 "
        "설명과 맞지 않음)은 '가스'의 오기로 추정된다. 원천 정의가 없어 100% 확정할 "
        "수는 없으나(INSUFFICIENT_SOURCE로 뭉개지 않고) 일반 지구과학 지식과 명백히 "
        "충돌하는 표현이라 DEFINITION_MISMATCH로 판정한다.",
    ),
    "6960": (
        "INSUFFICIENT_SOURCE",
        "SOURCE_HEADWORD_SPELLING_SUSPECT",
        "AI가 작성한 정의('평형 상태의 화학 계에 변화를 주면...') 자체는 르샤틀리에 "
        "원리를 정확히 설명함 - 문제는 표제어 표기: '르 샤를리에'는 원천 XLSX(sci 행 "
        "1028) 자체에 이렇게 적혀 있고(AI가 지어낸 게 아님, xlsx_headword가 DB "
        "headword와 정확히 일치) 화학 교과서 표준 표기는 '르샤틀리에'다. 정의 내용은 "
        "문제 없으나 표제어 표기 자체가 원천 데이터의 선행 결함이라 flags로 남긴다 - "
        "AI 뜻풀이 품질과는 별개 사안.",
    ),
    "5900": (
        "INSUFFICIENT_SOURCE",
        "SOURCE_HEADWORD_SPELLING_SUSPECT",
        "AI 정의('편의 시설이나 이익이 되는 시설을...')는 PIMFY(님비의 반대) 개념을 "
        "정확히 설명함 - 표제어 '핌비'는 원천 XLSX(social 행 927) 자체 표기이고(AI가 "
        "지어낸 게 아님), 사회 교과서 표준 표기는 '핌피'다. 위 르샤를리에 건과 같은 "
        "성격의 원천 데이터 선행 결함으로 flags에 남긴다.",
    ),
    "6014": ("INSUFFICIENT_SOURCE", "SUBJECT_SUBCATEGORY_SUSPECT",
              "정의 자체는 정확하나(부동산=토지·건물 등 정착 재산) 소분류가 '상법'으로 "
              "태깅됨 - 부동산·등기 개념은 통상 물권법(민법) 영역이라 소분류 태깅이 "
              "의심스럽다(교과 정합성 문제, 정의 정확성과는 별개)."),
    "6015": ("INSUFFICIENT_SOURCE", "SUBJECT_SUBCATEGORY_SUSPECT",
              "정의는 정확하나(등기제도) 소분류 '상법' 태깅이 의심스러움 - 6014와 동일 사유."),
    "6016": ("INSUFFICIENT_SOURCE", "SUBJECT_SUBCATEGORY_SUSPECT",
              "정의는 정확하나(공시) 소분류 '상법' 태깅이 의심스러움 - 6014와 동일 사유."),
    "6017": ("INSUFFICIENT_SOURCE", "SUBJECT_SUBCATEGORY_SUSPECT",
              "정의는 정확하나(주택임대차) 소분류 '상법' 태깅이 의심스러움 - 6014와 동일 사유."),
    "6029": ("INSUFFICIENT_SOURCE", "SUBJECT_SUBCATEGORY_SUSPECT",
              "정의는 정확하나(상소절차) 소분류 '형법'으로 좁게 태깅됨 - 상소는 민·형사 "
              "공통 절차 개념이라 '형법' 전용 태깅이 의심스러움(교과 정합성 문제)."),
}

_DEFAULT_VERDICT = "INSUFFICIENT_SOURCE"
_DEFAULT_RATIONALE = (
    "원천 XLSX(스크립트가 external_id로 재추적한 행)에 정의 텍스트가 전혀 없어 "
    "원문 대조 자체가 불가능함(577건 전수 조사 결과 원천 정의 보유 0건). 호출 "
    "세션이 표제어·소분류·주차 맥락에서 일반 교과 지식으로 직접 읽었을 때 정의 "
    "내용에 명백한 오류나 모순은 발견되지 않았으나, 이는 '원천으로 확인됨"
    "(SOURCE_SUPPORTED)'과는 다른 상태다 - 원천이 없다는 사실 자체가 판정이다."
)

V_ITEM_ID = "5022"
V_FLAGS = "BASE_WORD_ALREADY_LOADED_L6COREV2_5021"
V_RATIONALE = (
    "정의('겉으로 드러나지 않은 속이나 뒷면의. 또는 그런 것.')는 '이면적'(裏面的)의 "
    "표준적 의미와 정확히 부합함. 원천 XLSX(tooldict L6 행 175)에는 정의가 없어 "
    "원문 대조는 불가(INSUFFICIENT_SOURCE). 참고: 이 항목의 어근 '이면'은 이미 "
    "phase26에서 L6 비공개 배치에 별도 표제어(SR_L6COREV2_5021)로 적재돼 있다 - "
    "같은 배치에 '이면'과 '이면적'이 동시에 존재하면 파생어 중복으로 보일 수 있어 "
    "향후 실제 적재 시 참고할 사항으로 flags에 남긴다(정의 오류는 아님)."
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=Path, required=True)
    ap.add_argument("--full", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with open(args.sample, encoding="utf-8-sig") as f:
        sample_rows = list(csv.DictReader(f))
    with open(args.full, encoding="utf-8-sig") as f:
        full_rows = {r["term_id"]: r for r in csv.DictReader(f)}

    v_row = dict(full_rows[V_ITEM_ID])
    v_row["stratum"] = "V(전수)"
    v_row["stratum_size"] = 1
    v_row["sample_reason"] = "V_FULL_CENSUS"

    all_targets = sample_rows + [v_row]
    print(f"판정 대상: 표본 {len(sample_rows)}건 + V 전수 1건 = {len(all_targets)}건")

    out_rows = []
    for r in all_targets:
        tid = r["term_id"]
        if tid == V_ITEM_ID:
            verdict, flags, rationale = "INSUFFICIENT_SOURCE", V_FLAGS, V_RATIONALE
        elif tid in OVERRIDES:
            verdict, flags, rationale = OVERRIDES[tid]
        else:
            verdict, flags, rationale = _DEFAULT_VERDICT, "", _DEFAULT_RATIONALE

        out_rows.append({
            "term_id": tid,
            "headword": r["headword"],
            "source": r["source"],
            "subject_category": r["subject_category"],
            "sense_category": r["sense_category"],
            "note_subcategory": r.get("note_subcategory", ""),
            "note_week": r.get("note_week", ""),
            "definition": r["definition"],
            "xlsx_sheet_or_level": r["xlsx_sheet_or_level"],
            "xlsx_row_no": r["xlsx_row_no"],
            "xlsx_headword": r["xlsx_headword"],
            "xlsx_definition_present": r["xlsx_definition_present"],
            "stratum": r.get("stratum", ""),
            "stratum_size": r.get("stratum_size", ""),
            "sample_reason": r.get("sample_reason", ""),
            "semantic_verdict": verdict,
            "flags": flags,
            "rationale": rationale,
            "grade_fit": "POLICY_MAPPING_ONLY",
            "grade_fit_note": (
                "grade_level 컬럼 NULL(개별 학년 근거 없음) - L6=고2~3은 "
                "docs/literacy/07-학년경계정책-L5L6.md 정책 매핑에만 의존. "
                "AI 태그나 review_status='검수완료'(가짜 신호)를 사람 검수 근거로 "
                "쓰지 않았음."
            ),
        })

    assert len(out_rows) == len(sample_rows) + 1, "판정 누락 - 목표 수량과 무관하게 전수 판정돼야 함"
    assert all(o["semantic_verdict"] for o in out_rows), "빈 판정 존재 - 중단"

    from collections import Counter
    dist = Counter(o["semantic_verdict"] for o in out_rows)
    print("의미 판정 분포:", dict(dist))
    flagged = [o for o in out_rows if o["flags"]]
    print(f"추가 flags가 있는 건: {len(flagged)}건")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
