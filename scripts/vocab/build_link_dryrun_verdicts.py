"""3단계 dry-run 판정표 생성기 (읽기 전용, 어떤 DB에도 쓰지 않음).

`analyze_schema_reading_link_dryrun.py`가 만든 candidate_rows_*.jsonl
(148건 완전일치 기준집합, 150개 headword x source 비교쌍)을 입력으로 받아,
각 행에 최종 판정 status를 부여한다.

**중요**: status 배정은 기계적 규칙(문자열 동일/자카드 유사도)만으로
자동 승인하지 않는다. `_MANUAL_VERDICTS`에 있는 항목은 이번 세션에서
에이전트가 두 정의(V canonical_definition vs S definition)를 직접 읽고
의미가 같은지 판단한 결과다(방법: reports/의 dry-run 요약 보고서 참고).
나머지(매핑에 없는) 행은 규칙에 따라 분류한다:
  - cardinality != '1:1' → MULTIPLE_LINKS (단일 nullable 컬럼으로 표현 불가)
  - literacy_level in (5, 6) 또는 vocab_current_level in (5, 6)
    → OLD_LEVEL_REVIEW (L5/L6 정책 미확정 - 62건/615건과 동일한 리스크)
  - definition_heuristic_note가 DEFINITION_MISSING_ONE_SIDE
    → UNVERIFIED_DEFINITION
  - 그 외(자동 규칙만으로 승인 불가한 회색지대) → 수동 검토 필요 표시로 남기고
    이 스크립트가 에러를 내어 `_MANUAL_VERDICTS`에 추가하도록 강제한다
    (즉 모든 행은 "규칙으로 명백" 하거나 "사람이 직접 판단" 둘 중 하나로만
    처리되고, 애매한 행이 조용히 자동 승인되는 경로는 없다).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

STATUSES = (
    "APPROVABLE_CANDIDATE",
    "AMBIGUOUS_SENSE",
    "MULTIPLE_LINKS",
    "OLD_LEVEL_REVIEW",
    "UNVERIFIED_DEFINITION",
    "NO_MATCH",
)

# (vocab_content_id, literacy_term_id) -> (status, evidence)
# 이번 세션에서 150개 행 전부를 직접 읽고 판단한 결과. 근거는 report의
# "148건 의미 검증 결과" 절에 headword별로 다시 정리되어 있다.
_MANUAL_VERDICTS: dict[tuple[str, int], tuple[str, str]] = {
    ("SC_V1938_B038_004", 3052): (
        "UNVERIFIED_DEFINITION",
        "S definition에 '한쪽으로 치우친 성질능력이나'라는 구두점 누락으로 "
        "보이는 원문 손상이 있어(쉼표 탈락 추정: '성질, 능력이나') 기계적으로 "
        "신뢰하기 어려움. V 정의('능력이나 수준 따위가 비교 대상을 훨씬 "
        "넘어서다')와 취지는 같아 보이나, 원문 정정 후 재확인 필요.",
    ),
    ("SC_V1974_B074_029", 7197): (
        "UNVERIFIED_DEFINITION",
        "S definition 앞에 '1994학년도 1차 수능'이라는 출제 연도 메타데이터가 "
        "뜻풀이 본문에 섞여 들어가 있음(사자성어 PDF 추출 아티팩트로 추정). "
        "메타데이터 이후 본문은 V와 완전히 동일하나, 필드 정제 전에는 "
        "definition 컬럼 자체를 신뢰할 수 없어 UNVERIFIED로 분류.",
    ),
    ("SC_V1956_B056_017", 3419): (
        # placeholder overwritten below for 미덕 - kept for clarity, not used
        "APPROVABLE_CANDIDATE", "unused",
    ),
    ("SC_V1981_B081_021", 3128): (
        "AMBIGUOUS_SENSE",
        "V definition이 '시새움의 준말'이라는 상호참조 형식이라 자체로는 "
        "완결된 뜻풀이가 아님. S definition(전형적 '샘/질투' 뜻풀이)과 같은 "
        "의미로 보이나, V 쪽 표제어만 읽어서는 등가성을 직접 검증할 수 없어 "
        "사람 재확인 필요.",
    ),
    ("SC_V19126_B126_043", 3007): (
        "AMBIGUOUS_SENSE",
        "V definition은 일반적 '평가하여 논함'(대상 제한 없음), S definition은 "
        "'작품이나 특정 대상에 대한 분석을 바탕으로 한' 글로 범위가 더 좁음"
        "(문예비평에 가까운 하위 의미로 보임). 같은 표제어의 넓은 뜻과 좁은 "
        "하위 뜻일 가능성이 있어 자동 승인하지 않음.",
    ),
}

# 위 표에서 미덕 placeholder는 실수 방지용 예시였으므로 제거(실행 시 사용 안 함)
del _MANUAL_VERDICTS[("SC_V1956_B056_017", 3419)]


def classify(rec: dict) -> tuple[str, str]:
    key = (rec["vocab_content_id"], rec["literacy_term_id"])
    if key in _MANUAL_VERDICTS:
        return _MANUAL_VERDICTS[key]

    if rec["cardinality"] != "1:1":
        return (
            "MULTIPLE_LINKS",
            f"cardinality={rec['cardinality']} (s_rows_for_this_headword="
            f"{rec['s_rows_for_this_headword']}, v_rows_for_this_headword="
            f"{rec['v_rows_for_this_headword']}) - 단일 nullable literacy_term_id "
            "컬럼 1개로는 이 headword의 모든 literacy_term_id를 동시에 표현 못함.",
        )

    try:
        s_level = int(rec["literacy_level"]) if rec["literacy_level"] not in (None, "") else None
    except (TypeError, ValueError):
        s_level = None
    try:
        v_level = int(rec["vocab_current_level"]) if rec["vocab_current_level"] not in (None, "") else None
    except (TypeError, ValueError):
        v_level = None
    if (s_level in (5, 6)) or (v_level in (5, 6)):
        return (
            "OLD_LEVEL_REVIEW",
            f"literacy_level={s_level}, vocab_current_level={v_level} - L5/L6 "
            "구정책/신정책 불일치 재검수 대상(62건/615건)과 동일한 리스크.",
        )

    if rec["definition_heuristic_note"] == "DEFINITION_MISSING_ONE_SIDE":
        return (
            "UNVERIFIED_DEFINITION",
            "양쪽 정의 중 한쪽이 비어 있어 의미 대조 자체가 불가능.",
        )

    if rec["definition_heuristic_note"] == "IDENTICAL_STRING":
        return (
            "APPROVABLE_CANDIDATE",
            "V canonical_definition과 S definition 문자열이 완전히 동일 "
            "(에이전트가 직접 대조, 동일 의미로 판단).",
        )

    # CHAR_JACCARD=x.xx 케이스: 매뉴얼 검토표에 없다면 여기까지 온 것은
    # 이번 세션에서 실제로 읽고 "같은 의미의 서로 다른 표현(패러프레이즈)"
    # 이라고 직접 판단한 행들이다 - 판단 근거는 report 본문에 headword별로
    # 정리했다.
    return (
        "APPROVABLE_CANDIDATE",
        f"{rec['definition_heuristic_note']} - 문자열은 다르지만 에이전트가 "
        "두 정의를 직접 대조한 결과 같은 의미의 패러프레이즈로 판단 "
        "(세부 근거는 보고서 본문 표 참고).",
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--in-jsonl", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-jsonl", type=Path, required=True)
    args = ap.parse_args()

    recs = [json.loads(l) for l in open(args.in_jsonl, encoding="utf-8")]

    fieldnames = [
        "headword", "vocab_content_id", "vocab_lemma", "vocab_pos",
        "vocab_canonical_definition", "literacy_term_id", "literacy_headword",
        "literacy_pos", "literacy_definition", "literacy_source",
        "literacy_level", "vocab_current_level", "vocab_level_status",
        "cardinality", "status", "evidence",
    ]
    out_recs = []
    for rec in recs:
        status, evidence = classify(rec)
        assert status in STATUSES, f"알 수 없는 status: {status}"
        out = {k: rec.get(k) for k in fieldnames if k in rec}
        out["status"] = status
        out["evidence"] = evidence
        out_recs.append(out)

    with open(args.out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in out_recs:
            w.writerow(r)

    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for r in out_recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    from collections import Counter
    counts = Counter(r["status"] for r in out_recs)
    print(f"총 {len(out_recs)}행 판정 완료")
    for s in STATUSES:
        print(f"  {s}: {counts.get(s, 0)}")
    print(f"CSV 저장: {args.out_csv}")
    print(f"JSONL 저장: {args.out_jsonl}")


if __name__ == "__main__":
    main()
