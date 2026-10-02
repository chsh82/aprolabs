"""DB 매핑 dry-run 산출 - 기존 집계(review_status/match_type, 2026-09-29~30
산출)를 재사용하고, 이번에 라이브 DB에서 다시 읽은 입력만 확인한다. 이 스크립트는
어떤 DB도 쓰지 않는다 - `data/import/nikl_vocab_contents_5950_consolidated_
20261006.csv`(읽기 전용 조회 결과) 하나만 입력으로 받아 5개 범주로 재분류한다.

범주(상호 배타적, 아래 순서로 우선 적용 - 한 content_id는 정확히 하나의 범주에만
들어간다):
1. tier1_기본레벨조정 / tier1_현재유지 - 이미 사람이 개별 승인한 31건
   (vocabulary_official_grade_judgments 최신 판정). RULE_A/B와 전혀 겹치지
   않음(기존에 확인된 사실, 재확인만 함).
2. RULE_A / RULE_B - 정책 적용 후보 1,408건(아직 사람이 개별 승인하지 않음 -
   tier1과 범주가 다르다는 걸 분명히 하기 위해 1번과 섞지 않음).
3. 공식5등급_미검수 - 기존 DB 콘텐츠 중 공식 5등급으로 매칭됐지만 위 1·2에도
   속하지 않는 49건. 목표는 레벨 변경이 아니라 "경계(L3~L4)" 표시 유지.
4. 미매칭다중후보의미위험_별도검토 - match_type이 매칭없음/다중후보/
   단일일치_동형이의주의/표제어만일치인 125건 중 위 1~3에 이미 들어가지 않은
   나머지. 자동 정책 적용 대상이 아니다.
5. (파일에 행을 만들지 않음) 위 네 범주에 속하지 않는 나머지는 NO_ACTION_MATCH
   (정책상 변경 없음) 또는 아직 범주 미지정인 개별검토 대기 - 요약에만 건수로
   남긴다.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"

RISK_MATCH_TYPES = {"매칭없음", "다중후보", "단일일치_동형이의주의", "표제어만일치"}


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    with open(IMP / "nikl_vocab_contents_5950_consolidated_20261006.csv", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 5950, len(rows)

    out_rows = []
    counts = {
        "tier1_기본레벨조정": 0, "tier1_현재유지": 0, "RULE_A": 0, "RULE_B": 0,
        "공식5등급_미검수": 0, "미매칭다중후보의미위험_별도검토": 0,
    }
    seen_ids = set()

    for r in rows:
        cid = r["content_id"]
        cat = None
        target = ""
        note = ""

        if r["사람판정_tier1"] == "기본 레벨 조정":
            cat = "tier1_기본레벨조정"
            target = r["공식기반추천범위"]  # 'L1'/'L2' - 이미 사람이 승인한 목표
            note = f"현재 L{r['현재출제레벨']} -> {target}(사람 승인, {r['사람판정_시각']})"
        elif r["사람판정_tier1"] == "현재 유지":
            cat = "tier1_현재유지"
            target = f"L{r['현재출제레벨']}"
            note = f"변경 없음(사람 승인, {r['사람판정_시각']})"
        elif r["review_status"] == "RULE_PROPOSED_PENDING_APPROVAL":
            cat = "RULE_A" if r["공식기반추천범위"] == "L2" else "RULE_B"
            target = r["공식기반추천범위"]
            note = f"현재 L{r['현재출제레벨']} -> {target}(정책 후보, 사람 미승인 - tier1과 구분)"
        elif r["공식등급"] == "5":
            cat = "공식5등급_미검수"
            target = "경계(L3~L4)"
            risk = " [동형이의 위험 병존]" if r["연결확인상태"] in RISK_MATCH_TYPES else ""
            note = f"레벨 변경 아님 - 경계 표시만 유지, 미검수{risk}"
        elif r["연결확인상태"] in RISK_MATCH_TYPES:
            cat = "미매칭다중후보의미위험_별도검토"
            target = "해당없음(검토 필요)"
            note = f"match_type={r['연결확인상태']}, exception_reason={r['exception_reason'] or '-'}"

        if cat is None:
            continue
        assert cid not in seen_ids, f"중복 content_id: {cid}"
        seen_ids.add(cid)
        counts[cat] += 1
        out_rows.append({
            "content_id": cid, "lemma": r["lemma"], "매핑범주": cat,
            "현재출제레벨": r["현재출제레벨"], "목표": target, "공식등급": r["공식등급"],
            "연결확인상태": r["연결확인상태"], "exception_reason": r["exception_reason"],
            "비고": note,
        })

    out_path = IMP / "nikl_db_mapping_dryrun_20261006.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["content_id", "lemma", "매핑범주", "현재출제레벨", "목표",
                                            "공식등급", "연결확인상태", "exception_reason", "비고"])
        w.writeheader()
        w.writerows(out_rows)

    total_flagged = sum(counts.values())
    print("범주별 건수:", counts)
    print("합계(이 파일에 행이 있는 건수):", total_flagged)
    print("5,950 중 이 dry-run에서 다루지 않은 나머지(NO_ACTION_MATCH 등):", 5950 - total_flagged)
    print(f"-> {out_path}")


if __name__ == "__main__":
    main()
