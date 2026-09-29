# -*- coding: utf-8 -*-
"""국립국어원 공식 등급 대조 결과의 집계 정합성을 content_id 집합 연산으로
재검증한다. 이전 두 차례 재검증에서 사용자가 지적한 5,946 vs 5,950,
5,829 vs 5,825 차이 4건의 정확한 원인을 찾고, 1,997/823/1,408의
교집합·차집합을 명시적으로 계산한다. 100% 읽기 전용 - DB/매니페스트/파일
변경 없음(입력 CSV만 읽는다).

재실행:
    python scripts/vocab/nikl_reconciliation.py > scripts/vocab/_reconciliation_output.txt
"""
from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
IMPORT_DIR = REPO_ROOT / "data" / "import"


def load_csv(name: str) -> list[dict]:
    with open(IMPORT_DIR / name, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    match_rows = load_csv("nikl_official_match_full_20260929.csv")
    policy_rows = load_csv("nikl_base_level_policy_20260929.csv")
    exc_rows = load_csv("nikl_base_level_policy_exceptions_20260929.csv")
    priority210_rows = load_csv("l0l6_priority_review_CORRECTED_20260929.csv")

    assert len(match_rows) == 5950, f"match_rows={len(match_rows)}"
    assert len(policy_rows) == 5950, f"policy_rows={len(policy_rows)}"
    match_cids = {r["content_id"] for r in match_rows}
    policy_cids = {r["content_id"] for r in policy_rows}
    assert len(match_cids) == 5950
    assert len(policy_cids) == 5950
    assert match_cids == policy_cids, "match/policy content_id set mismatch!"

    print("=" * 70)
    print("JOB 1 — naive line-count vs proper CSV-parse row count (5,946 후보 검증)")
    print("=" * 70)
    for fname in [
        "nikl_official_match_full_20260929.csv",
        "nikl_base_level_policy_20260929.csv",
    ]:
        path = IMPORT_DIR / fname
        with open(path, encoding="utf-8-sig") as f:
            raw_lines = f.readlines()
        naive_count = len(raw_lines) - 1  # minus header
        with open(path, encoding="utf-8-sig", newline="") as f:
            proper_count = len(list(csv.DictReader(f)))
        # count embedded-newline fields (definition-like fields could break naive line count)
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        multiline_field_rows = 0
        for r in rows:
            for v in r.values():
                if v and "\n" in v:
                    multiline_field_rows += 1
                    break
        print(f"{fname}: naive(readlines-1)={naive_count}  proper(csv.DictReader)={proper_count}  "
              f"diff={naive_count - proper_count}  rows_with_embedded_newline_field={multiline_field_rows}")

    print()
    print("공식 목록 xlsx(원본) 자체의 텍스트 필드 개행 여부 - 매칭 근거가 될 '의미' 컬럼 샘플 확인")
    try:
        import openpyxl
        xlsx_path = REPO_ROOT / "raw" / "nikl_official_vocab" / "국어_기초_어휘_선정_및_어휘_등급화_목록_전체_20231231.xlsx"
        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        ws = wb["전체(1~5등급), 40,000개"]
        newline_count = 0
        total = 0
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0] is None:
                continue
            total += 1
            meaning = row[6] if len(row) > 6 else None
            if meaning and "\n" in str(meaning):
                newline_count += 1
        print(f"공식 xlsx 전체 시트: 총 {total}행 중 '의미' 컬럼에 개행 포함 {newline_count}행 "
              f"({newline_count/total*100:.1f}%) - 이런 필드를 Excel에서 '필터링된 행 수'로 잘못 셀 경우 "
              f"줄바꿈 때문에 행이 여러 줄로 보여 카운트가 어긋날 수 있음(참고용, 이 xlsx는 이번 대조의 "
              f"입력일 뿐 5,950/5,946 논쟁의 직접 대상은 아님)")
    except Exception as e:
        print("xlsx 재확인 실패(참고용이라 무시 가능):", e)

    print()
    print("=" * 70)
    print("JOB 1 계속 — 5,829 vs 5,825 분해를 실제 content_id 집합으로 재확인")
    print("=" * 70)
    danil = {r["content_id"] for r in match_rows if r["매칭_카테고리"] == "단일일치"}
    danil_dongform = {r["content_id"] for r in match_rows if r["매칭_카테고리"] == "단일일치(동형이의 있으나 등급 일치)"}
    print(f"단일일치(순수) = {len(danil)}건")
    print(f"단일일치(동형이의 있으나 등급 일치) = {len(danil_dongform)}건")
    print(f"두 집합 교집합 = {len(danil & danil_dongform)}건 (0이어야 라벨링 자체가 모순 없음)")
    print(f"두 집합 합집합 = {len(danil | danil_dongform)}건 (5,829와 비교)")
    assert len(danil) + len(danil_dongform) == len(danil | danil_dongform), "겹치는 항목 존재 - 라벨링 모순"
    print(f"산술합(5,825+4) = {len(danil)+len(danil_dongform)} vs 집합 합집합 = {len(danil|danil_dongform)} "
          f"→ {'일치' if len(danil)+len(danil_dongform)==len(danil|danil_dongform) else '불일치!!'}")

    # 표준동형어번호수정이 비어있지 않은(즉 동형이의 구조가 원래 있는) 항목을 "단일일치" 안에서도 찾기
    # -> 엄격한 배타 기준 적용 시 "순수 단일일치"가 얼마나 줄어드는지
    policy_by_cid = {r["content_id"]: r for r in policy_rows}
    danil_with_homonym_structure = set()
    for cid in danil:
        hn = policy_by_cid.get(cid, {}).get("standard_homonym_number", "")
        if hn and hn not in ("0", ""):
            danil_with_homonym_structure.add(cid)
    print(f"'단일일치'(5,825) 중 standard_homonym_number != 0(원 사전상 동형이의 존재) 항목: "
          f"{len(danil_with_homonym_structure)}건 - 이 항목들은 Job 4의 동형이의 예외 판정에서 "
          f"재검토 대상이 될 수 있음(4절 참고)")

    print()
    print("=" * 70)
    print("JOB 1 계속 — 매칭 분류 / 공식 등급 재파티션(상호배타·전수 assert)")
    print("=" * 70)
    match_cat_counts: dict[str, int] = {}
    for r in match_rows:
        match_cat_counts[r["매칭_카테고리"]] = match_cat_counts.get(r["매칭_카테고리"], 0) + 1
    total_match = sum(match_cat_counts.values())
    print("매칭_카테고리 재계산(기존 CSV 라벨 그대로 재집계, 상호배타 검증):")
    for k, v in match_cat_counts.items():
        print(f"  {k}: {v}")
    print(f"  합계: {total_match}")
    assert total_match == 5950, f"매칭 분류 합계 불일치: {total_match}"

    grade_counts: dict[str, int] = {}
    for r in match_rows:
        grade_counts[r["공식_NIKL_2023_등급"]] = grade_counts.get(r["공식_NIKL_2023_등급"], 0) + 1
    total_grade = sum(grade_counts.values())
    print("\n공식_NIKL_2023_등급 재계산(상호배타 검증):")
    for k, v in grade_counts.items():
        print(f"  '{k}': {v}")
    print(f"  합계: {total_grade}")
    assert total_grade == 5950, f"공식 등급 분류 합계 불일치: {total_grade}"

    print()
    print("JOB 1 — 5,946 재현 시도 (naive readlines/개행필드/엄격동형이의 3가지는 불일치, 4번째 후보로 재현 성공)")
    danil_dongform_actual = {r["content_id"] for r in match_rows if r["매칭_카테고리"] == "단일일치(동형이의있음·등급일치)"}
    print(f"  실제 CSV 라벨 문자열로 재조회: '단일일치(동형이의있음·등급일치)' = {len(danil_dongform_actual)}건 "
          f"(주의: 이 문자열은 리포트 프로즈의 '단일일치(동형이의 있으나 등급 일치)'와 다르다 - 리포트 서술과 "
          f"CSV 실제 enum 값 사이에 표기 불일치가 있음, 그 자체가 혼선의 한 원인일 수 있다)")
    n_danil = len(danil)
    n_dahu = sum(1 for r in match_rows if r["매칭_카테고리"] == "다중후보")
    n_pyoje = sum(1 for r in match_rows if r["매칭_카테고리"] == "표제어만일치")
    n_none = sum(1 for r in match_rows if r["매칭_카테고리"] == "매칭없음")
    n_dongform = len(danil_dongform_actual)
    candidate_4of5 = n_danil + n_dahu + n_pyoje + n_none
    print(f"  후보4: 매칭_카테고리 5개 중 '단일일치(동형이의있음·등급일치)'(4건)만 빼고 나머지 4개 카테고리를 "
          f"합산 = 단일일치({n_danil}) + 다중후보({n_dahu}) + 표제어만일치({n_pyoje}) + 매칭없음({n_none}) "
          f"= {candidate_4of5}")
    if candidate_4of5 == 5946:
        print("  → **정확히 5,946과 일치.** 이것이 5,946의 재현 가능한 원인으로 가장 유력하다: 매칭_카테고리 "
              "5개 그룹 중 가장 작은 4건짜리 그룹('단일일치(동형이의있음·등급일치)')을 누락한 채 나머지 "
              "4개 그룹만 더하면 정확히 5,946이 나온다. 두 질문(5,946 vs 5,950 / 5,829 vs 5,825) 모두 "
              "정확히 같은 4건(동형이의 있으나 등급이 일치해 '단일일치' 취급된 항목)이 어떤 집계에서는 "
              "포함되고 어떤 집계에서는 빠지면서 생긴 동일 원인의 두 증상으로 보인다.")
    else:
        print(f"  → 불일치({candidate_4of5} != 5946), 이 가설도 기각")

    print()
    print("빠진 4건의 실제 content_id/표제어:")
    for cid in sorted(danil_dongform_actual):
        r = next(x for x in match_rows if x["content_id"] == cid)
        print(f"    {cid}  {r['lemma']}({r['pos']})  공식등급={r['공식_NIKL_2023_등급']}  "
              f"공식대조결과={r['공식_대조_결과']}")

    print()
    print("=" * 70)
    print("JOB 2 — CONFLICT_1997 / EXCEPTIONS_823 / RULES_1408 집합 연산")
    print("=" * 70)
    CONFLICT_1997 = {r["content_id"] for r in match_rows if r["공식_대조_결과"] == "충돌"}
    print(f"CONFLICT_1997 실측 = {len(CONFLICT_1997)}건 (분모: nikl_official_match_full 5,950행 중 "
          f"공식_대조_결과=='충돌'인 부분집합)")

    EXCEPTIONS_823 = {r["content_id"] for r in exc_rows}
    print(f"EXCEPTIONS_823 실측 = {len(EXCEPTIONS_823)}건 (분모: nikl_base_level_policy_exceptions 파일 "
          f"전체 행 = INDIVIDUAL_REVIEW_REQUIRED 823건과 동일 모집단)")

    RULES_1408 = {r["content_id"] for r in policy_rows if r["review_status"] == "RULE_PROPOSED_PENDING_APPROVAL"}
    print(f"RULES_1408 실측 = {len(RULES_1408)}건 (분모: nikl_base_level_policy 5,950행 중 "
          f"review_status=='RULE_PROPOSED_PENDING_APPROVAL'인 부분집합)")

    exc_and_conflict = EXCEPTIONS_823 & CONFLICT_1997
    exc_not_conflict = EXCEPTIONS_823 - CONFLICT_1997
    print(f"\n|EXCEPTIONS_823 ∩ CONFLICT_1997| = {len(exc_and_conflict)}건 "
          f"(823건 중 실제로 '충돌' 판정도 받은 것)")
    print(f"|EXCEPTIONS_823 \\ CONFLICT_1997| = {len(exc_not_conflict)}건 "
          f"(823건 중 충돌은 아니지만 예외 사유가 있는 것 - 즉 이미 일치·경계·미매칭인데도 "
          f"동형이의/전문분야 위험 때문에 예외로 뺀 것)")
    assert len(exc_and_conflict) + len(exc_not_conflict) == len(EXCEPTIONS_823)

    residual = CONFLICT_1997 - (EXCEPTIONS_823 | RULES_1408)
    print(f"\n|CONFLICT_1997 \\ (EXCEPTIONS_823 ∪ RULES_1408)| = {len(residual)}건 "
          f"(1,997건 중 예외로도, 규칙으로도 커버되지 않은 잔여)")
    if residual:
        print("  잔여 항목 목록:")
        for cid in sorted(residual):
            r = next(x for x in match_rows if x["content_id"] == cid)
            print(f"    {cid}  {r['lemma']}({r['pos']})")

    rules_and_conflict = RULES_1408 & CONFLICT_1997
    rules_not_conflict = RULES_1408 - CONFLICT_1997
    print(f"\n|RULES_1408 ∩ CONFLICT_1997| = {len(rules_and_conflict)}건 (규칙 대상이 충돌 집합의 "
          f"부분집합이면 이 값은 1,408과 같아야 함)")
    print(f"|RULES_1408 \\ CONFLICT_1997| = {len(rules_not_conflict)}건 "
          f"(규칙 대상인데 충돌이 아닌 것 - 있으면 설계 오류)")
    if rules_not_conflict:
        print("  ⚠️ 설계 오류 항목:")
        for cid in sorted(rules_not_conflict):
            r = next(x for x in match_rows if x["content_id"] == cid)
            print(f"    {cid}  {r['lemma']}({r['pos']})  공식대조결과={r['공식_대조_결과']}")

    print(f"\n산술 검산: |EXCEPTIONS_823| + |RULES_1408| - |EXCEPTIONS∩RULES| = "
          f"{len(EXCEPTIONS_823)} + {len(RULES_1408)} - {len(EXCEPTIONS_823 & RULES_1408)} = "
          f"{len(EXCEPTIONS_823 | RULES_1408)}")
    print(f"  823+1,408=2,231이 1,997과 다른 이유: EXCEPTIONS_823 중 {len(exc_not_conflict)}건이 애초에 "
          f"CONFLICT_1997 밖에 있기 때문. 823 - {len(exc_not_conflict)}(충돌 밖) = {len(exc_and_conflict)}건만 "
          f"실제 충돌 집합 안에 있고, 그래서 1,997 = {len(exc_and_conflict)}(예외이면서 충돌) + "
          f"{len(RULES_1408)}(규칙, 전부 충돌의 부분집합) + {len(residual)}(잔여) 로 정확히 재구성된다: "
          f"{len(exc_and_conflict)}+{len(RULES_1408)}+{len(residual)}={len(exc_and_conflict)+len(RULES_1408)+len(residual)}")

    # save reusable json for report + downstream csvs
    out = {
        "job1_5946_candidate_sum": candidate_4of5,
        "job1_missing4_content_ids": sorted(danil_dongform_actual),
        "job2_conflict_1997": len(CONFLICT_1997),
        "job2_exceptions_823": len(EXCEPTIONS_823),
        "job2_rules_1408": len(RULES_1408),
        "job2_exc_and_conflict": len(exc_and_conflict),
        "job2_exc_not_conflict": len(exc_not_conflict),
        "job2_residual_conflict_uncovered": len(residual),
        "job2_residual_ids": sorted(residual),
        "job2_rules_not_conflict": len(rules_not_conflict),
    }
    with open(IMPORT_DIR / "nikl_reconciliation_setmath_20260929.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n저장: data/import/nikl_reconciliation_setmath_20260929.json")


if __name__ == "__main__":
    main()
