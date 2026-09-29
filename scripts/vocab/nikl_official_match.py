"""연구 DB 5,950건 어휘를 국립국어원(2023) 공식 4만 어휘 등급화 목록과 대조한다.

원본: raw/nikl_official_vocab/국어_기초_어휘_선정_및_어휘_등급화_목록_전체_20231231.xlsx
  (SHA-256 6eec715bca39d1006702da020f5c61a7a1f3db81fdb6101619f68d0b0ff70b53,
   국립국어원 2023년 "국어 기초 어휘 선정 및 어휘 등급화 연구", 공공누리 제1유형)

읽기 전용 - DB에는 전혀 쓰지 않는다. 이미 검증된 로컬 산출물(l0l6_level_
classification_full_CORRECTED_20260929.csv, l0l6_level_vs_db_comparison_
CORRECTED_20260929.csv, l0l6_priority_review_CORRECTED_20260929.csv)만
입력으로 쓰고 DB를 재조회하지 않는다(이미 재검증된 값이라 재조회 불필요).

주의: 이 스크립트가 다루는 "공식_NIKL_2023_등급"(1~5, 국립국어원 2023
공식 목록)과 내부 level_reason_json.nikl_vocabulary_grade(2~4, 출처
미확인·김광해 계열로 문서상 기록됨)는 이름이 비슷해도 완전히 다른
척도다 - 컬럼명을 명확히 분리해 혼동을 구조적으로 방지한다.
"""
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[2]
XLSX_PATH = REPO_ROOT / "raw" / "nikl_official_vocab" / "국어_기초_어휘_선정_및_어휘_등급화_목록_전체_20231231.xlsx"
FULL_CSV = REPO_ROOT / "data" / "import" / "l0l6_level_classification_full_CORRECTED_20260929.csv"
CMP_CSV = REPO_ROOT / "data" / "import" / "l0l6_level_vs_db_comparison_CORRECTED_20260929.csv"
PRIORITY_CSV = REPO_ROOT / "data" / "import" / "l0l6_priority_review_CORRECTED_20260929.csv"

OUT_FULL = REPO_ROOT / "data" / "import" / "nikl_official_match_full_20260929.csv"
OUT_REVIEW = REPO_ROOT / "data" / "import" / "nikl_official_review_sheet_20260929.csv"
OUT_RISK_DETAIL = REPO_ROOT / "data" / "import" / "nikl_official_review_sheet_risk_detail_20260929.csv"

GRADE_LEVEL_MAP = {
    1: "L0이전참고",
    2: "L0",
    3: "L1",
    4: "L2",
    5: "경계(L3~L4)",
}
# 일치/충돌 비교가 가능한 건 "특정 레벨 하나"를 가리키는 2~4등급뿐이다.
GRADE_SPECIFIC_DB_LEVEL = {2: 0, 3: 1, 4: 2}


def norm_pos(pos: str) -> set[str]:
    if not pos:
        return set()
    return {p.strip().replace(" ", "") for p in pos.split("/") if p.strip()}


def load_official():
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb["전체(1~5등급), 40,000개"]
    grade_of = {"1등급": 1, "2등급": 2, "3등급": 3, "4등급": 4, "5등급": 5}
    by_lemma_pos = defaultdict(list)  # (lemma, pos_token) -> [row,...]
    by_lemma = defaultdict(list)
    n = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] is None:
            continue
        grade_label, lemma, hom_no, pos, origin, orig_form, meaning, domain = row[:8]
        grade = grade_of.get(grade_label)
        if grade is None or not lemma:
            continue
        n += 1
        rec = {
            "등급": grade,
            "어휘": lemma,
            "동형번호": hom_no,
            "품사": pos,
            "어종": origin,
            "원어": orig_form,
            "의미": meaning,
            "분야": domain,
        }
        by_lemma[lemma].append(rec)
        for p in norm_pos(pos):
            by_lemma_pos[(lemma, p)].append(rec)
    print(f"[공식목록] 로드 완료: {n}행")
    return by_lemma_pos, by_lemma


def load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main():
    by_lemma_pos, by_lemma = load_official()
    full_rows = load_csv(FULL_CSV)
    cmp_rows = {r["content_id"]: r for r in load_csv(CMP_CSV)}
    priority_rows = load_csv(PRIORITY_CSV)
    priority_by_cid = {}
    for r in priority_rows:
        priority_by_cid[r["content_id"]] = r["우선순위_검토목록"]

    assert len(full_rows) == 5950, f"기대치 5950, 실제 {len(full_rows)}"
    assert len(cmp_rows) == 5950

    out_full = []
    match_counter = defaultdict(int)
    grade_bucket_counter = defaultdict(int)
    compare_counter = defaultdict(int)
    risk_rows_for_detail = []
    new_priority_cids = {}  # content_id -> tag

    for r in full_rows:
        cid = r["content_id"]
        lemma = r["lemma"]
        pos = r["pos"]
        cmp = cmp_rows[cid]

        pos_tokens = norm_pos(pos)
        exact_candidates = []
        for p in pos_tokens:
            exact_candidates.extend(by_lemma_pos.get((lemma, p), []))
        # 중복 제거(동일 레코드가 여러 pos 토큰에 걸릴 수 있음)
        seen_ids = set()
        dedup_exact = []
        for c in exact_candidates:
            key = (c["등급"], c["동형번호"], c["의미"])
            if key in seen_ids:
                continue
            seen_ids.add(key)
            dedup_exact.append(c)
        exact_candidates = dedup_exact

        lemma_only_candidates = by_lemma.get(lemma, [])

        if not lemma_only_candidates:
            category = "매칭없음"
            official_grade = None
        elif not exact_candidates:
            category = "표제어만일치"
            official_grade = None
        else:
            grades = sorted({c["등급"] for c in exact_candidates})
            if len(grades) == 1:
                category = "단일일치" if len(exact_candidates) == 1 else "단일일치(동형이의있음·등급일치)"
                official_grade = grades[0]
            else:
                category = "다중후보"
                official_grade = None  # 특정 등급 하나로 확정 불가

        match_counter[category] += 1

        if official_grade is not None:
            official_level = GRADE_LEVEL_MAP[official_grade]
            grade_bucket_counter[official_grade] += 1
        elif category == "다중후보":
            grades = sorted({c["등급"] for c in exact_candidates})
            official_level = "다중후보(" + "/".join(GRADE_LEVEL_MAP[g] for g in grades) + ")"
        else:
            official_level = "해당없음"

        # 4절: 공식 대조 결과 플래그
        db_level_raw = cmp["기존DB_vocab_level"]
        db_level = int(db_level_raw) if db_level_raw not in (None, "") else None
        if category == "다중후보":
            compare_flag = "다중의미위험"
        elif category in ("표제어만일치", "매칭없음"):
            compare_flag = "공식근거없음(" + category + ")"
        elif official_grade == 1:
            compare_flag = "참고전용(학령전)"
        elif official_grade == 5:
            compare_flag = "경계(공식5등급)"
        elif official_grade in GRADE_SPECIFIC_DB_LEVEL:
            expect = GRADE_SPECIFIC_DB_LEVEL[official_grade]
            if db_level == expect:
                compare_flag = "일치"
            else:
                compare_flag = "충돌"
        else:
            compare_flag = "미분류"

        compare_counter[compare_flag] += 1

        existing_priority_tag = priority_by_cid.get(cid, "")
        new_tag = ""
        if compare_flag in ("충돌", "다중의미위험"):
            new_tag = f"공식대조({compare_flag})"
            new_priority_cids[cid] = new_tag

        out_row = {
            "content_id": cid,
            "lemma": lemma,
            "pos": pos,
            "공식_NIKL_2023_등급": official_grade if official_grade else ("다중" if category == "다중후보" else "-"),
            "공식근거_추천레벨": official_level,
            "매칭_카테고리": category,
            "내부_김광해계열_변환등급": r["내부_변환등급"],
            "현재DB_vocab_level": db_level_raw,
            "현재DB_level_status": cmp["기존DB_level_status"],
            "현재DB_boundary_flag": cmp["기존DB_boundary_flag"],
            "공식_대조_결과": compare_flag,
            "기존_우선순위_태그": existing_priority_tag,
            "신규_공식대조_태그": new_tag,
        }
        out_full.append(out_row)

        if compare_flag in ("충돌", "다중의미위험") or existing_priority_tag:
            db_def = None  # 뜻풀이 전수 대조 범위 밖 - 원본 DB 재조회 안 함, 이미 가진 것만 기록
            risk_rows_for_detail.append({
                "content_id": cid,
                "lemma": lemma,
                "pos": pos,
                "공식_대조_결과": compare_flag,
                "공식_후보_상세": " | ".join(
                    f"{c['등급']}등급/동형{c['동형번호']}/{(c['의미'] or '')[:60]}" for c in exact_candidates
                ) if exact_candidates else "",
            })

    # ---- 출력 1: 전체 5,950건 대조표 ----
    with open(OUT_FULL, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_full[0].keys()))
        w.writeheader()
        w.writerows(out_full)

    # ---- 우선순위 통합: 기존 210 ∪ 신규(충돌/다중의미위험) ----
    combined_tags = defaultdict(list)
    for r in priority_rows:
        combined_tags[r["content_id"]].append(r["우선순위_검토목록"])
    for cid, tag in new_priority_cids.items():
        combined_tags[cid].append(tag)

    full_by_cid = {r["content_id"]: r for r in out_full}
    review_rows = []
    for cid, tags in combined_tags.items():
        fr = full_by_cid[cid]
        cmp = cmp_rows[cid]
        db_level_raw = cmp["기존DB_vocab_level"]
        # 현재DB 레벨과의 차이(짧은 플래그)
        cf = fr["공식_대조_결과"]
        if cf == "일치":
            diff = "일치"
        elif cf == "충돌":
            diff = f"공식 근거(L{GRADE_SPECIFIC_DB_LEVEL.get(fr['공식_NIKL_2023_등급'], '?')})와 DB(L{db_level_raw}) 불일치"
        elif cf in ("경계(공식5등급)", "참고전용(학령전)"):
            diff = cf
        elif cf == "다중의미위험":
            diff = "공식 등급 후보 상이(동형이의)"
        else:
            diff = cf

        review_rows.append({
            "표제어": fr["lemma"],
            "품사": fr["pos"],
            "공식_등급": fr["공식_NIKL_2023_등급"],
            "추천_레벨_또는_경계": fr["공식근거_추천레벨"],
            "근거": f"NIKL2023:{fr['매칭_카테고리']} | 내부(김광해):{fr['내부_김광해계열_변환등급']} | 태그:{';'.join(tags)}",
            "현재DB_레벨과의_차이": diff,
            "승인": "",
            "레벨수정": "",
            "보류": "",
            "_content_id": cid,  # 내부용, 아래서 risk detail 매칭에만 사용하고 최종 출력엔 유지(참조 편의)
        })

    review_rows.sort(key=lambda x: x["표제어"])

    with open(OUT_REVIEW, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["표제어", "품사", "공식_등급", "추천_레벨_또는_경계", "근거", "현재DB_레벨과의_차이", "승인", "레벨수정", "보류", "_content_id"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(review_rows)

    with open(OUT_RISK_DETAIL, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(risk_rows_for_detail[0].keys()))
        w.writeheader()
        w.writerows(risk_rows_for_detail)

    # ---- 요약 ----
    print("\n=== 매칭 카테고리 ===")
    for k, v in sorted(match_counter.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v} ({v/59.5:.1f}%)")
    print("\n=== 공식 등급 분포(매칭된 것만) ===")
    for g in sorted(grade_bucket_counter):
        print(f"  {g}등급({GRADE_LEVEL_MAP[g]}): {grade_bucket_counter[g]}")
    print("\n=== 공식 대조 결과(4절 집계) ===")
    for k, v in sorted(compare_counter.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print(f"\n기존 210건 우선순위 목록: {len(priority_rows)}행")
    print(f"신규 공식대조 위험(충돌/다중의미위험): {len(new_priority_cids)}건")
    print(f"통합 우선순위 목록(최종 고유): {len(combined_tags)}건")
    print(f"위험 상세 파일 행수: {len(risk_rows_for_detail)}")

    # 재사용을 위해 통계 JSON도 저장
    stats = {
        "match_counter": dict(match_counter),
        "grade_bucket_counter": dict(grade_bucket_counter),
        "compare_counter": dict(compare_counter),
        "prior_priority_count": len(priority_rows),
        "new_official_risk_count": len(new_priority_cids),
        "combined_priority_count": len(combined_tags),
    }
    with open(REPO_ROOT / "data" / "import" / "nikl_official_match_stats_20260929.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
