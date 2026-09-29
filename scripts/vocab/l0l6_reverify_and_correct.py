# -*- coding: utf-8 -*-
"""읽기 전용 재검증: L0~L6 1차 분류(vocab_level_l0l6_classification_20260929.md)의
근거 대응표(nikl_vocabulary_grade 출처)와 집계 단위(207/17/78건)를 다시 검증하고
정정본을 만든다.

핵심 정정 사항:
1. `nikl_vocabulary_grade`(2/3/4등급)는 국립국어원 2023년 공식 4만 어휘 목록과
   동일하다고 확인되지 않았다 - data/import/vocabulary_leveling_v1.zip 안의
   LEVEL_POLICY.md/FINAL_REPORT.md가 원저작자 스스로 "김광해 계열의 국어교육용
   어휘 등급"이라고 명시했고, 5등급 이상 데이터가 전혀 없으며(사용자가 예시로
   든 "5→L3/L4" 매핑이 적용될 수 없음), AS_/LE_ 식별자는 로컬 KRDict 덤프의
   자체 id 체계(정수, 예: 27733)와 형식이 다르다(대조 결과 미일치) - 그래서
   "공식 등급"으로 단정하지 않고 별도 컬럼으로 확인상태를 분리해 표시한다.
2. 227건(L4~L6)은 5개 하위 배치(SR_L4CORE/SR_L5CORE/SR_L6CORE/SR_L6COREV2/
   SR_L6EVIDENCEV1)로 구성되며, 전부 level_status=REVIEW_BOUNDARY(boundary_flag=1)
   그대로다 - "이미 레벨 확정"이라는 표현은 쓰지 않는다. SR_L6EVIDENCEV1(48건)만
   phase30~35 근거기반 집필 파이프라인을 거쳤고, 그 파이프라인의 자체 추적
   컬럼(expert_review_status)조차 전량 DRAFT_NOT_REVIEWED다.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPORT_PATH = Path("C:/Users/aproa/AppData/Local/Temp/l0l6_full_export.json")

OUT_DIR = REPO_ROOT / "data" / "import"

NIKL_MAP = {
    "2등급": ("L0", 0),
    "3등급": ("경계(L1~L2)", None),
    "4등급": ("경계(L3~L4)", None),
}

BATCH_LABELS = {
    "SR_L4CORE": "L4CORE(phase13, MANUAL_STRUCTURED_AUTHORING - literacy.db 교재 어휘 대조 기반 사람 작성, 외부 사전 재검증 없음)",
    "SR_L5CORE": "L5CORE(phase14, MANUAL_STRUCTURED_AUTHORING - literacy.db 교재 어휘 대조 기반 사람 작성, 외부 사전 재검증 없음)",
    "SR_L6CORE": "L6CORE(phase24, MANUAL_STRUCTURED_AUTHORING - literacy.db 교재 어휘 대조 기반 사람 작성, 외부 사전 재검증 없음)",
    "SR_L6COREV2": "L6COREV2(phase26, MANUAL_STRUCTURED_AUTHORING - literacy.db 교재 어휘 대조 기반 사람 작성, 외부 사전 재검증 없음)",
    "SR_L6EVIDENCEV1": "L6EVIDENCEV1(phase30~35 근거기반 집필 - 사전/공공기관 출처 재검증까지 거쳤으나 expert_review_status=DRAFT_NOT_REVIEWED 100%, 전문가 최종검수 미완료)",
}


def batch_of(content_id: str) -> str:
    for prefix in ("SR_L6EVIDENCEV1", "SR_L6COREV2", "SR_L6CORE", "SR_L5CORE", "SR_L4CORE"):
        if content_id.startswith(prefix):
            return prefix
    return "UNKNOWN"


def nikl_grade(level_reason_json: str) -> str | None:
    try:
        d = json.loads(level_reason_json)
    except (TypeError, ValueError):
        return None
    if isinstance(d, str):
        try:
            d = json.loads(d)
        except (TypeError, ValueError):
            return None
    if not isinstance(d, list):
        return None
    for feat in d:
        if isinstance(feat, dict) and feat.get("feature") == "nikl_vocabulary_grade":
            return feat.get("value")
    return None


def main() -> None:
    with open(EXPORT_PATH, encoding="utf-8") as f:
        rows = json.load(f)
    assert len(rows) == 5950, f"기대치와 다름: {len(rows)}"

    with open(OUT_DIR / "l0l6_boundary_207_content_ids_20260929.json", encoding="utf-8") as f:
        set207 = set(json.load(f))
    with open(OUT_DIR / "l0l6_l2_provisional_17_content_ids_20260929.json", encoding="utf-8") as f:
        set17 = set(json.load(f))
    with open(OUT_DIR / "l0l6_meaning_risk_78_content_ids_20260929.json", encoding="utf-8") as f:
        set78 = set(json.load(f))

    union_207_17 = set207 | set17
    final_priority = union_207_17 | set78

    full_out = []
    cmp_out = []
    conflict_counter = {"MATCH": 0, "CONFLICT": 0, "DB_OVERCONFIDENT_BOUNDARY": 0,
                         "BOUNDARY_DB_ALSO_BOUNDARY": 0, "OUT_OF_SCOPE": 0}

    for r in rows:
        cid = r["content_id"]
        g = nikl_grade(r["level_reason_json"])
        if g is None:
            # 227건(L4~L6 수동 배치)
            batch = batch_of(cid)
            label = BATCH_LABELS.get(batch, f"UNKNOWN_BATCH({cid})")
            official_status = "범위 밖(자동분류 미대상)"
            internal_level = "제외(자동분류 범위 밖 - 수동 배치)"
            confidence = "N/A"
            risk = "N/A - 4절 227건 별도 감사 대상"
            cmp_out.append({
                "content_id": cid, "lemma": r["lemma"],
                "추천_내부등급": internal_level,
                "기존DB_vocab_level": r["vocab_level"],
                "기존DB_level_status": r["level_status"],
                "기존DB_boundary_flag": r["boundary_flag"],
                "비교_플래그": "OUT_OF_SCOPE",
                "227건_하위배치": label,
            })
            conflict_counter["OUT_OF_SCOPE"] += 1
        else:
            rec_label, rec_level = NIKL_MAP[g]
            official_status = "미확인(김광해 계열로 문서상 기록됨 - 국립국어원 2023 공식 4만 어휘 목록과의 동일성 미확인, 1절 참고)"
            internal_level = rec_label
            confidence = "상" if rec_level is not None else "중"
            risk = "없음" if rec_level is not None else f"원천 {g}는 정책상 {rec_label} 두 밴드에 걸침 - 세부레벨 강제분할 안 함"

            if rec_level is not None:
                if r["vocab_level"] == rec_level and r["level_status"] != "REVIEW_BOUNDARY":
                    flag = "MATCH"
                elif r["vocab_level"] == rec_level and r["level_status"] == "REVIEW_BOUNDARY":
                    flag = "BOUNDARY_DB_ALSO_BOUNDARY"
                else:
                    flag = "CONFLICT"
            else:
                flag = "BOUNDARY_DB_ALSO_BOUNDARY" if r["level_status"] == "REVIEW_BOUNDARY" else "DB_OVERCONFIDENT_BOUNDARY"
            conflict_counter[flag] += 1
            cmp_out.append({
                "content_id": cid, "lemma": r["lemma"],
                "추천_내부등급": internal_level,
                "기존DB_vocab_level": r["vocab_level"],
                "기존DB_level_status": r["level_status"],
                "기존DB_boundary_flag": r["boundary_flag"],
                "비교_플래그": flag,
                "227건_하위배치": "",
            })

        in_207 = cid in set207
        in_17 = cid in set17
        in_78 = cid in set78
        src_tags = []
        if in_207:
            src_tags.append("207(레벨보류)")
        if in_17:
            src_tags.append("17(L2_PROVISIONAL_AUTO)")
        if in_78:
            src_tags.append("78(뜻풀이위험)")

        full_out.append({
            "content_id": cid, "lemma": r["lemma"], "pos": r["pos"],
            "공식등급_확인상태": official_status,
            "내부_변환등급": internal_level,
            "신뢰도": confidence,
            "경계_동형이의_위험": risk,
            "우선순위_검토목록": ";".join(src_tags) if src_tags else "",
            "사람_판정": "",
        })

    # ---- 산출물 저장 ----
    with open(OUT_DIR / "l0l6_level_classification_full_CORRECTED_20260929.csv", "w",
              encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(full_out[0].keys()))
        w.writeheader()
        w.writerows(full_out)

    with open(OUT_DIR / "l0l6_level_vs_db_comparison_CORRECTED_20260929.csv", "w",
              encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(cmp_out[0].keys()))
        w.writeheader()
        w.writerows(cmp_out)

    priority_out = [r for r in full_out if r["content_id"] in final_priority]
    with open(OUT_DIR / "l0l6_priority_review_CORRECTED_20260929.csv", "w",
              encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(priority_out[0].keys()))
        w.writeheader()
        w.writerows(priority_out)

    print(f"전체 {len(full_out)}행, 비교표 {len(cmp_out)}행, 우선순위 {len(priority_out)}행")
    print("충돌 플래그 집계(재검증):", conflict_counter)
    print("207 크기:", len(set207), "17 크기:", len(set17), "207∩17:", len(set207 & set17),
          "207∪17:", len(union_207_17), "(207∪17)∩78:", len(union_207_17 & set78),
          "최종 합집합(207∪17∪78):", len(final_priority))


if __name__ == "__main__":
    main()
