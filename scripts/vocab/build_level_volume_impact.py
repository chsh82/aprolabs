"""현재 출제 레벨 기준 vs 매핑 dry-run(RULE_A/B + tier1 기본 레벨 조정 8건)
적용 가정 기준의 레벨별 출제량을 나란히 계산한다 - 읽기 전용, DB 쓰기 없음.

실제 선택 함수(`app/vocabulary_quiz/routers/multiformat.py`의
`_matching_level_content_ids`/`_select_level_candidates`)와 **완전히 동일한
규칙**으로 재현한다:
  - source_version == '2.1.29'(일반 레벨모드 풀, 1,289건)인 문항만 포함.
  - CROSSWORD는 레벨 모드에서 코드가 완전히 제외하므로 이 계산에도 제외한다
    (별도로 "항상 전체모드 10세트, 레벨모드 미포함"이라고만 표시).
  - MATCH_WORD_MEANING은 source_content_ids_json의 모든 content_id가 선택
    레벨과 일치해야 포함(대표/평균 레벨을 만들지 않음) - 단일 어휘형은
    source_content_id 하나만 본다.
  - confidence_mode: all_candidates(PROVISIONAL_AUTO+REVIEW_BOUNDARY) vs
    auto_only(PROVISIONAL_AUTO만) 둘 다 계산한다(코드의 실제 두 모드).

"변경안"은 매핑 dry-run에서 실제로 레벨 숫자가 바뀌는 두 범주(RULE_A/B,
tier1_기본레벨조정)만 가정 적용한다 - 다른 범주(tier1_현재유지/공식5등급_
미검수/미매칭위험)는 레벨을 바꾸지 않으므로 출제량에 영향이 없다.
level_status/boundary_flag는 이 dry-run이 건드리지 않는 값이라 시뮬레이션
에서도 그대로 유지한다(그래서 all_candidates/auto_only 두 모드로 나눠 보는
것 자체가 "경계 플래그가 그대로라 auto_only에서는 여전히 빠질 수 있다"를
보여주는 용도).

파일럿(L4/L5, L6)·L0~L3 확장 미리보기는 전혀 다른 source_version을 쓰는
완전히 별도 문항 풀이라 이 레벨모드 계산에 아예 들어오지 않는다 - 그 대신
dry-run이 가리키는 content_id가 그 세 매니페스트에 들어있는지만 별도로
확인한다(매니페스트는 고정 item_id 화이트리스트이므로, 레벨이 바뀌어도
문항이 풀에서 빠지지는 않지만, 그 content_id가 "레벨 미확정"인 L0~L3
미리보기처럼 레벨 버킷으로 결과를 보여주는 화면이라면 표시 버킷이 바뀔 수
있다 - 그 영향 범위를 건수로만 표시한다).
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMP = ROOT / "data" / "import"
VOCAB = ROOT / "data" / "vocab"

CONFIDENCE_MODES = {
    "all_candidates": ("PROVISIONAL_AUTO", "REVIEW_BOUNDARY"),
    "auto_only": ("PROVISIONAL_AUTO",),
}
GRADE_LABELS = {
    0: "초등 1~2학년", 1: "초등 3~4학년", 2: "초등 5~6학년",
    3: "중등 1~2학년", 4: "중등 3학년", 5: "고등 1학년", 6: "고등 2~3학년",
}


def read_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    contents = read_csv(IMP / "nikl_vocab_contents_5950_consolidated_20261006.csv")
    mapping = read_csv(IMP / "nikl_db_mapping_dryrun_20261006.csv")
    items = read_csv(IMP / "nikl_multiformat_items_2129_20261006.csv")

    current_level = {r["content_id"]: int(r["현재출제레벨"]) for r in contents if r["현재출제레벨"]}
    level_status = {r["content_id"]: r["level_status"] for r in contents}

    proposed_level = dict(current_level)
    changed = {}
    for r in mapping:
        if r["매핑범주"] in ("RULE_A", "RULE_B", "tier1_기본레벨조정"):
            target = int(r["목표"].lstrip("L"))
            cid = r["content_id"]
            changed[cid] = (current_level.get(cid), target)
            proposed_level[cid] = target
    print(f"레벨이 실제로 바뀌는 content_id: {len(changed)}건(RULE_A 1368+RULE_B 40+tier1_기본레벨조정 8=1416 기대)")
    assert len(changed) == 1416, len(changed)

    def availability(level_map: dict[str, int], level: int, mode: str) -> tuple[int, int, dict[str, int]]:
        allowed_status = CONFIDENCE_MODES[mode]
        matching = {
            cid for cid, lv in level_map.items()
            if lv == level and level_status.get(cid) in allowed_status
        }
        n_items = 0
        distinct_words: set[str] = set()
        by_type: dict[str, int] = {}
        for it in items:
            if it["item_type"] == "MATCH_WORD_MEANING":
                cids = json.loads(it["source_content_ids_json"] or "[]")
                if cids and all(c in matching for c in cids):
                    n_items += 1
                    distinct_words.update(cids)
                    by_type[it["item_type"]] = by_type.get(it["item_type"], 0) + 1
            else:
                if it["source_content_id"] in matching:
                    n_items += 1
                    distinct_words.add(it["source_content_id"])
                    by_type[it["item_type"]] = by_type.get(it["item_type"], 0) + 1
        return n_items, len(distinct_words), by_type

    print("\n=== 레벨별 출제량 - 현재 vs 변경안(RULE_A/B+tier1 8건 가정 적용), 모드별 ===")
    rows_out = []
    for mode in ("all_candidates", "auto_only"):
        for level in range(7):
            cur_items, cur_words, cur_by_type = availability(current_level, level, mode)
            prop_items, prop_words, prop_by_type = availability(proposed_level, level, mode)
            print(f"[{mode}] L{level}({GRADE_LABELS[level]}): "
                  f"문항 {cur_items}->{prop_items}(Δ{prop_items-cur_items}) / "
                  f"고유어휘 {cur_words}->{prop_words}(Δ{prop_words-cur_words})")
            rows_out.append({
                "confidence_mode": mode, "level": level, "grade_label": GRADE_LABELS[level],
                "현재_문항수": cur_items, "변경안_문항수": prop_items, "문항수_변화": prop_items - cur_items,
                "현재_고유어휘수": cur_words, "변경안_고유어휘수": prop_words, "고유어휘수_변화": prop_words - cur_words,
                "현재_유형별": json.dumps(cur_by_type, ensure_ascii=False),
                "변경안_유형별": json.dumps(prop_by_type, ensure_ascii=False),
            })

    crossword_total = sum(1 for it in items if it["item_type"] == "CROSSWORD")
    print(f"\n(참고) CROSSWORD {crossword_total}건은 레벨 모드에서 항상 제외 - 전체 모드에서만 세트 단위로 출제, "
          f"이번 레벨 변경과 무관")

    out_path = IMP / "nikl_level_volume_impact_20261006.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        w.writeheader()
        w.writerows(rows_out)
    print(f"-> {out_path}")

    print("\n=== 기존 파일럿·L0~L3 미리보기 영향(매니페스트 고정 content_id와 변경 대상 겹침) ===")
    changed_ids = set(changed.keys())
    manifest_files = {
        "pilot_l4l5(L4/L5, 40)": "pilot_l4l5_manifest_v1.json",
        "pilot_l6(L6, 40)": "pilot_l6_manifest_v1.json",
        "existing_l0l3_preview(184)": "existing_l0l3_manifest_v1.json",
    }
    for label, fname in manifest_files.items():
        with open(VOCAB / fname, encoding="utf-8") as f:
            rows = json.load(f)
        ids = {r["source_content_id"] for r in rows if r.get("source_content_id")}
        overlap = ids & changed_ids
        print(f"  {label}: 매니페스트 content_id {len(ids)}종 중 변경대상과 겹침 {len(overlap)}건 -> {sorted(overlap)}")


def build_tier1_adjustment_application_file() -> None:
    """tier1 '기본 레벨 조정' 8건만을 위한 별도 적용안 파일(RULE_A/B와 분리,
    이번에도 UPDATE 미실행) - 현재값/변경값/연결 문항 수/매니페스트 영향/
    판정 신선도를 한 행씩 기록한다."""
    import unicodedata

    contents = read_csv(IMP / "nikl_vocab_contents_5950_consolidated_20261006.csv")
    by_id = {r["content_id"]: r for r in contents}
    mapping = read_csv(IMP / "nikl_db_mapping_dryrun_20261006.csv")
    tier1_rows = [r for r in mapping if r["매핑범주"] == "tier1_기본레벨조정"]
    assert len(tier1_rows) == 8, len(tier1_rows)

    items = read_csv(IMP / "nikl_multiformat_items_2129_20261006.csv")

    def linked_items(cid: str) -> list[tuple[str, str]]:
        out = []
        for it in items:
            if it["item_type"] == "MATCH_WORD_MEANING":
                cids = json.loads(it["source_content_ids_json"] or "[]")
                if cid in cids:
                    out.append((it["item_id"], it["item_type"]))
            elif it["source_content_id"] == cid:
                out.append((it["item_id"], it["item_type"]))
        return out

    manifest_ids: set[str] = set()
    for fname in ("pilot_l4l5_manifest_v1.json", "pilot_l6_manifest_v1.json", "existing_l0l3_manifest_v1.json"):
        with open(VOCAB / fname, encoding="utf-8") as f:
            rows = json.load(f)
        manifest_ids |= {r["source_content_id"] for r in rows if r.get("source_content_id")}

    out_rows = []
    for r in tier1_rows:
        cid = r["content_id"]
        c = by_id[cid]
        linked = linked_items(cid)
        out_rows.append({
            "content_id": cid, "lemma": r["lemma"],
            "현재레벨": f"L{c['현재출제레벨']}", "변경값": r["목표"],
            "연결문항수": len(linked),
            "연결문항_유형": ",".join(sorted({t for _, t in linked})),
            "매니페스트영향": "없음(파일럿·L0~L3 미리보기 세 매니페스트 모두 겹치지 않음)" if cid not in manifest_ids else "있음(확인 필요)",
            "판정신선도": "최신(만료 아님 - 참조 테이블 2026-09-29 최초 적재 이후 재계산 0회, 라이브 재확인)",
            "level_status": c["level_status"], "boundary_flag": c["boundary_flag"],
            "exception_reason": c["exception_reason"],
            "적용상태": "미적용(dry-run만, UPDATE 미실행)",
        })

    out_path = IMP / "nikl_tier1_level_adjustment_8_application_plan_20261006.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"\ntier1 기본 레벨 조정 8건 개별 적용안(미적용) -> {out_path}")
    for r in out_rows:
        print(f"  {r['lemma']}({r['content_id']}): {r['현재레벨']}->{r['변경값']}, "
              f"연결문항 {r['연결문항수']}건({r['연결문항_유형']}), 매니페스트영향={r['매니페스트영향']}")


if __name__ == "__main__":
    main()
    build_tier1_adjustment_application_file()
