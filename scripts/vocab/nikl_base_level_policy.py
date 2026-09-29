"""국립국어원 2023 공식 등급을 '기본 레벨' 정책으로 확정 적용한다(읽기 전용).

정책(사용자 확정, 2026-09-29):
  - 어휘의 '기본 레벨'은 교재 수록 학년보다 국립국어원 공식 습득 시기 등급을
    우선한다. 교재 학년은 보조 정보로만 기록한다(DB 미변경).
  - 1등급→L0(기초 하위표시), 2등급→L0, 3등급→L1, 4등급→L2,
    5등급→경계(L3~L4)(강제 분할 안 함), L5·L6·미매칭은 별도 근거 없이 확정 안 함.

입력(전부 읽기 전용, DB에는 쓰지 않는다):
  - data/import/nikl_official_match_full_20260929.csv (직전 포크 산출물, 5,950행)
  - raw/nikl_official_vocab/*.xlsx (공식 원본, 표준동형어번호수정 재추출용)
  - data/import/l0l6_meaning_risk_78_content_ids_20260929.json
  - data/import/nikl_level_source_domain_snapshot_20260929.json (DB에서 1회 읽기전용
    조회해 저장한 level_source·specialized_domain 스냅샷 - 재현하려면 vocabulary_
    content_levels.level_source/level_reason_json을 다시 조회해 동일 형식으로 생성)
  - data/import/nikl_textbook_grade_auxiliary_snapshot_20260929.json
    (vocabulary_content_literacy_links + literacy.db terms 조인 결과, 교재 연계
    학년 보조정보용 - 142건만 존재, 나머지는 "해당없음")
  - data/vocab/pilot_l4l5_manifest_v1.json / pilot_l6_manifest_v1.json /
    existing_l0l3_manifest_v1.json (7절 dry-run 영향도 계산용)

출력:
  - data/import/nikl_base_level_policy_20260929.csv (5,950행 전체)
  - data/import/nikl_base_level_policy_exceptions_20260929.csv (예외 목록만)
  - data/import/nikl_base_level_policy_rules_20260929.json (패턴 규칙안)
  - reports/nikl_base_level_policy_20260929.md 작성용 통계 전부 stdout/파일로 남김

DB 쓰기 전혀 없음. review_status는 절대 "사람 승인 완료"를 의미하지 않는다.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nikl_official_match as nom  # noqa: E402  (load_official, norm_pos 재사용)

REPO_ROOT = Path(__file__).resolve().parents[2]
MATCH_FULL = REPO_ROOT / "data" / "import" / "nikl_official_match_full_20260929.csv"
RISK78 = REPO_ROOT / "data" / "import" / "l0l6_meaning_risk_78_content_ids_20260929.json"
LEVEL_SRC_DOMAIN = REPO_ROOT / "data" / "import" / "nikl_level_source_domain_snapshot_20260929.json"
TEXTBOOK_GRADE = REPO_ROOT / "data" / "import" / "nikl_textbook_grade_auxiliary_snapshot_20260929.json"

OUT_FULL = REPO_ROOT / "data" / "import" / "nikl_base_level_policy_20260929.csv"
OUT_EXC = REPO_ROOT / "data" / "import" / "nikl_base_level_policy_exceptions_20260929.csv"
OUT_RULES = REPO_ROOT / "data" / "import" / "nikl_base_level_policy_rules_20260929.json"
OUT_STATS = REPO_ROOT / "data" / "import" / "nikl_base_level_policy_stats_20260929.json"

# ---- 정책 매핑(확정본, 1등급도 이제 L0에 포함) ----
GRADE_TO_BASE_LEVEL = {
    1: ("L0", "기초"),
    2: ("L0", ""),
    3: ("L1", ""),
    4: ("L2", ""),
    5: ("경계(L3~L4)", "공식5등급-강제분할안함"),
}


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    by_lemma_pos, by_lemma = nom.load_official()
    with open(MATCH_FULL, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 5950, f"기대 5950, 실제 {len(rows)}"

    risk78 = set(load_json(RISK78))
    lvl_src_domain = load_json(LEVEL_SRC_DOMAIN)
    textbook = load_json(TEXTBOOK_GRADE)

    out_rows = []
    exc_rows = []
    reason_counter = Counter()
    status_counter = Counter()
    base_level_counter = Counter()

    for r in rows:
        cid = r["content_id"]
        lemma = r["lemma"]
        pos = r["pos"]

        # ---- 표준동형어번호수정: lemma+pos 정확 후보 전부 재조회(원본 재파싱) ----
        pos_tokens = nom.norm_pos(pos)
        exact = []
        for p in pos_tokens:
            exact.extend(by_lemma_pos.get((lemma, p), []))
        seen = set()
        dedup = []
        for c in exact:
            key = (c["등급"], c["동형번호"], c["의미"])
            if key in seen:
                continue
            seen.add(key)
            dedup.append(c)
        exact = dedup
        hom_numbers = sorted({c["동형번호"] for c in exact if c["동형번호"] not in (None, 0, "0")})
        # 사전 전체(품사 무관, 동일 lemma) 기준 동형이의 구조 존재 여부도 확인
        lemma_wide = by_lemma.get(lemma, [])
        lemma_wide_hom_numbers = sorted({c["동형번호"] for c in lemma_wide if c["동형번호"] not in (None, 0, "0")})

        official_grade_raw = r["공식_NIKL_2023_등급"]
        try:
            official_grade = int(official_grade_raw)
        except (TypeError, ValueError):
            official_grade = None  # 다중/매칭없음/표제어만일치/- 전부 여기로

        compare = r["공식_대조_결과"]
        current_level_raw = r["현재DB_vocab_level"]

        # ---- proposed_base_level(신정책) ----
        proposed = ""
        note = ""
        if official_grade in GRADE_TO_BASE_LEVEL:
            proposed, note = GRADE_TO_BASE_LEVEL[official_grade]
        # else: 미매칭/표제어만일치/다중후보/L5~L6 근거없음 -> 빈칸(미확정), 아래 review_status로 표시

        # ---- textbook 보조정보 ----
        tb = textbook.get(cid)
        if tb:
            textbook_aux = f"level={tb['level']}(grade_level={tb['grade_level']}, source={tb['term_source']})"
        else:
            textbook_aux = "해당없음(교재 연계 없음)"

        meta = lvl_src_domain.get(cid, {})
        level_source = meta.get("level_source", "")
        specialized_domain_db = meta.get("specialized_domain")
        specialized_domain_official = any((c.get("분야") == "전문어") for c in exact)

        # ---- 예외 사유 판정(우선순위: 뜻풀이위험 > 동형이의 > 전문분야 > 다중후보) ----
        exception_reason = ""
        secondary = []
        if cid in risk78:
            exception_reason = "뜻이_다를_가능성(뜻풀이위험78)"
        if hom_numbers or lemma_wide_hom_numbers or r["매칭_카테고리"] in (
            "다중후보", "단일일치(동형이의있음·등급일치)"
        ):
            if not exception_reason:
                exception_reason = "동형이의어"
            else:
                secondary.append("동형이의어")
        if specialized_domain_db or specialized_domain_official:
            if not exception_reason:
                exception_reason = "교과_전문_의미"
            else:
                secondary.append("교과_전문_의미")
        if r["매칭_카테고리"] == "다중후보":
            if not exception_reason:
                exception_reason = "공식목록_다중후보"
            else:
                secondary.append("공식목록_다중후보")

        # ---- Job5 표본검수 결과 반영: 현재DB레벨과 proposed_base_level이 2단계
        # 이상 벌어지는 "충돌" 항목은 표본 검토 결과(하단 주석) 1레벨 차이 패턴과
        # 성격이 달라(추상/학술 어휘가 다수) 일괄 규칙에서 제외하고 개별 검토로 돌린다.
        # 표본: grade4×L6(특별법/자아/타당성/객관적/실질/심층 등), grade2×L6(범죄/
        # 법칙/자발적 등), grade4×L5, grade3×L5/L6, grade2×L4 - 전부 "습득시기
        # 등급"과 "학술적 추상도" 축이 어긋나는 사례로 판단.
        proposed_num = {"L0": 0, "L1": 1, "L2": 2}.get(proposed)
        gap_flag = False
        if proposed_num is not None and current_level_raw not in (None, ""):
            try:
                cur_num = int(current_level_raw)
                if abs(cur_num - proposed_num) >= 2:
                    gap_flag = True
            except ValueError:
                pass
        if gap_flag:
            if not exception_reason:
                exception_reason = "레벨격차과대(2단계이상, 표본검수 결과 학술어휘 다수)"
            else:
                secondary.append("레벨격차과대(2단계이상)")

        # ---- review_status ----
        if exception_reason:
            review_status = "INDIVIDUAL_REVIEW_REQUIRED"
        elif compare == "일치":
            review_status = "NO_ACTION_MATCH"
        elif compare == "충돌":
            review_status = "RULE_PROPOSED_PENDING_APPROVAL"
        elif compare in ("경계(공식5등급)",):
            review_status = "BOUNDARY_SIGNAL_ONLY"
        elif compare in ("참고전용(학령전)",):
            review_status = "L0_BASE_PROPOSED_PENDING_APPROVAL"
        else:  # 매칭없음/표제어만일치
            review_status = "INSUFFICIENT_EVIDENCE"

        # ---- Job5 규칙 ID(표본검수로 안전 확인된 2개 패턴만 부여) ----
        pattern_id = ""
        if review_status == "RULE_PROPOSED_PENDING_APPROVAL":
            if official_grade == 4 and current_level_raw == "3":
                pattern_id = "RULE_A_grade4_to_L2_from_L3"
            elif official_grade == 3 and current_level_raw == "2":
                pattern_id = "RULE_B_grade3_to_L1_from_L2"
            else:
                pattern_id = "RESIDUAL_NO_RULE"

        base_level_counter[proposed or "미확정"] += 1
        reason_counter[exception_reason or "(예외아님)"] += 1
        status_counter[review_status] += 1

        out_row = {
            "content_id": cid,
            "lemma": lemma,
            "pos": pos,
            "official_grade": official_grade_raw,
            "proposed_base_level": proposed,
            "proposed_base_level_note": note,
            "current_vocab_level": current_level_raw,
            "exception_reason": exception_reason,
            "exception_reason_secondary": ";".join(secondary),
            "review_status": review_status,
            "textbook_grade_auxiliary": textbook_aux,
            "level_source": level_source,
            "standard_homonym_number": ";".join(str(h) for h in hom_numbers) if hom_numbers else "0",
            "pattern_id": pattern_id,
            "공식_대조_결과": compare,
            "내부_김광해계열_변환등급": r["내부_김광해계열_변환등급"],
        }
        out_rows.append(out_row)
        if exception_reason:
            exc_rows.append(out_row)

    # ---- 저장 ----
    with open(OUT_FULL, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    with open(OUT_EXC, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(exc_rows[0].keys()))
        w.writeheader()
        w.writerows(exc_rows)

    stats = {
        "base_level_counter": dict(base_level_counter),
        "reason_counter": dict(reason_counter),
        "status_counter": dict(status_counter),
        "total": len(out_rows),
        "exception_total": len(exc_rows),
    }
    with open(OUT_STATS, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    # ---- Job5: 패턴별 규칙안 JSON ----
    pattern_counter = Counter(r["pattern_id"] for r in out_rows if r["pattern_id"])
    rules = [
        {
            "rule_id": "RULE_A_grade4_to_L2_from_L3",
            "condition": "official_grade == 4 AND current_vocab_level == 3",
            "proposed_base_level": "L2",
            "count": pattern_counter.get("RULE_A_grade4_to_L2_from_L3", 0),
            "sample_evidence_summary": (
                "표본 15건(랜덤시드42) 실사 결과 해지다/낙담하다/거짓말투성이/불균형하다/"
                "백분율/물장난하다/달그락달그락/짜개다/금메달리스트/탈퇴하다/예술제/겨를/"
                "게시되다/기세등등하다 등 - 전부 초등 5~6학년(L2) 수준으로 보기에 무리가 "
                "없음. 현재DB(L3=중1~2)와 1레벨 차이로, DB_OVERCONFIDENT_BOUNDARY 가설과 "
                "부합. 일괄 적용 후보로 안전하다고 판단."
            ),
        },
        {
            "rule_id": "RULE_B_grade3_to_L1_from_L2",
            "condition": "official_grade == 3 AND current_vocab_level == 2",
            "proposed_base_level": "L1",
            "count": pattern_counter.get("RULE_B_grade3_to_L1_from_L2", 0),
            "sample_evidence_summary": (
                "표본 15건 실사 결과 아지랑이/안전띠/케첩/몰라주다/하기야/실망스럽다/"
                "제작되다/후덥지근하다/중얼중얼/요만하다/빨래집게/적응력/조심성/거칠어지다/"
                "자유스럽다 등 - 전부 초등 3~4학년(L1) 수준으로 보기에 무리가 없음. 1레벨 "
                "차이로 Rule A와 같은 성격. 일괄 적용 후보로 안전하다고 판단."
            ),
        },
    ]
    residual_count = pattern_counter.get("RESIDUAL_NO_RULE", 0)
    rules_doc = {
        "rules": rules,
        "bulk_rule_covered_count": sum(r["count"] for r in rules),
        "bulk_pool_before_residual_check": sum(pattern_counter.values()),
        "residual_no_rule_count": residual_count,
        "residual_note": (
            "현재DB레벨과 proposed_base_level 차이가 2단계 이상인 항목(예: 공식4등급이면서 "
            "현재DB가 L4~L6인 경우)은 별도 exception_reason='레벨격차과대(2단계이상)'로 "
            "재분류해 이 규칙 풀 자체에서 제외했다(아래 표본 참고) - 따라서 RESIDUAL_NO_RULE은 "
            "이론상 0에 가까워야 하며, 남은 값은 위 2개 패턴 조건에 정확히 들지 않는 경계선 "
            "케이스만 포함한다."
        ),
    }
    with open(OUT_RULES, "w", encoding="utf-8") as f:
        json.dump(rules_doc, f, ensure_ascii=False, indent=2)

    with open(REPO_ROOT / "data" / "import" / "nikl_base_level_policy_summary_20260929.txt", "w", encoding="utf-8") as f:
        f.write("proposed_base_level 분포:\n")
        for k, v in sorted(base_level_counter.items(), key=lambda x: -x[1]):
            f.write(f"  {k}: {v}\n")
        f.write(f"합계: {sum(base_level_counter.values())}\n\n")
        f.write("exception_reason 분포(1차 사유만, 중복 제외):\n")
        for k, v in sorted(reason_counter.items(), key=lambda x: -x[1]):
            f.write(f"  {k}: {v}\n")
        f.write(f"합계: {sum(reason_counter.values())}\n\n")
        f.write("review_status 분포:\n")
        for k, v in sorted(status_counter.items(), key=lambda x: -x[1]):
            f.write(f"  {k}: {v}\n")
        f.write(f"합계: {sum(status_counter.values())}\n")

    print("done", len(out_rows), len(exc_rows))


if __name__ == "__main__":
    main()
