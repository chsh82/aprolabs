#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase30 항목1/2: L6 AI 태그 577건(V 1 + S 576)을 literacy.db에서 다시
추출해 term_id 기준으로 고정하고, raw/schema-reading/의 원천 XLSX 행과
연결한다. 읽기 전용(SELECT만) - literacy.db·vocabulary_quiz DB 어디에도
쓰지 않는다.

대상 정의(phase9/phase23이 이미 확립한 필터, 재확인만 함):
    literacy.db terms 중 level=6, source IN ('schemareading-tooldict',
    'schemareading-schema'), note LIKE '%[AI 자동 생성 뜻풀이]%'
    (review_status='검수완료'는 이 배치가 자동으로 남긴 가짜 신호이므로
    필터 조건으로 쓰지 않는다 - phase9의 핵심 경고)

**혼동 금지**: 이것은 phase5/6/9가 감사한 "AI 레벨 판정 3,103건"
(krdict/sajaseongeo-pdf 출처, GROUNDED/AI_ONLY/POLICY_CONFLICT 4분류)과
완전히 다른 대상이다 - 3,103건은 V/S(schemareading) 표제어와 교집합이
0건인 별도 트랙이며, 이 스크립트는 그쪽을 전혀 건드리지 않는다.

원천 XLSX 연결: external_id가 스키마는 "{sheet_key}-{row_no}"
(scripts/literacy/import_schemareading_vocab.py:207), 학습도구어는
"L{level}-{row_no}"(같은 파일:173) 형식으로 그 항목을 만든 원본 엑셀
행 번호를 그대로 보존하고 있음을 이용해, external_id를 파싱해 원본 행을
다시 찾는다(스크립트 로직 자체는 scripts/literacy/schemareading_parser.py의
iter_schema_entries/iter_tooldict_entries를 그대로 재사용 - 새로 파싱
로직을 만들지 않음).

사용:
    python3 scripts/vocab/phase30_extract_ai_tag_577.py \\
        --literacy-db data/literacy.db \\
        --out data/import/schema_reading_phase30_ai_tag_577_20260927.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))
from schemareading_parser import iter_schema_entries, iter_tooldict_entries  # noqa: E402

AI_TAG = "[AI 자동 생성 뜻풀이]"


def load_target_577(literacy_db: Path) -> list[dict]:
    conn = sqlite3.connect(f"file:{literacy_db}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    rows = conn.execute(
        """
        SELECT id, headword, origin, definition, pos, sense_category, subject_category,
               grade_level, grade_source, source, external_id, review_status, note,
               level, reviewed_at
        FROM terms
        WHERE level = 6
          AND source IN ('schemareading-tooldict', 'schemareading-schema')
          AND note LIKE ?
        ORDER BY source, external_id
        """,
        (f"%{AI_TAG}%",),
    ).fetchall()
    conn.close()
    cols = ["term_id", "headword", "origin", "definition", "pos", "sense_category", "subject_category",
            "grade_level", "grade_source", "source", "external_id", "review_status", "note",
            "level", "reviewed_at"]
    return [dict(zip(cols, r)) for r in rows]


def _parse_note_subcategory_week(note: str) -> tuple[str | None, str | None]:
    sub = None
    week = None
    m = re.search(r"소분류:\s*([^/]+)", note or "")
    if m:
        sub = m.group(1).strip()
    m = re.search(r"주차:\s*([^/]+?)(?:\s*/|$)", note or "")
    if m:
        week = m.group(1).strip()
    return sub, week


def build_xlsx_indexes():
    schema_index: dict[tuple[str, int], object] = {}
    for e in iter_schema_entries():
        schema_index[(e.sheet_key, e.row_no)] = e
    tooldict_index: dict[tuple[int, int], object] = {}
    for e in iter_tooldict_entries():
        tooldict_index[(e.level, e.row_no)] = e
    return schema_index, tooldict_index


_SCHEMA_EID_RE = re.compile(r"^(social|sci|phil)-(\d+)$")
_TOOLDICT_EID_RE = re.compile(r"^L(\d+)-(\d+)$")


def link_to_source(row: dict, schema_index: dict, tooldict_index: dict) -> dict:
    eid = row["external_id"] or ""
    linked = False
    src_headword = src_definition = src_week = src_sense_category = src_subject_category = None
    src_sheet_or_level = src_row_no = None
    headword_match = False
    source_definition_present = False

    m = _SCHEMA_EID_RE.match(eid)
    if m:
        sheet_key, row_no = m.group(1), int(m.group(2))
        src_sheet_or_level, src_row_no = sheet_key, row_no
        e = schema_index.get((sheet_key, row_no))
        if e is not None:
            linked = True
            src_headword = e.headword
            src_definition = e.definition
            src_week = e.week
            src_sense_category = e.sense_category
            src_subject_category = e.subject_category
            headword_match = (e.headword == row["headword"])
            source_definition_present = bool(e.definition)
    else:
        m2 = _TOOLDICT_EID_RE.match(eid)
        if m2:
            level, row_no = int(m2.group(1)), int(m2.group(2))
            src_sheet_or_level, src_row_no = level, row_no
            e = tooldict_index.get((level, row_no))
            if e is not None:
                linked = True
                src_headword = e.headword
                src_definition = "|".join(e.definitions) if e.definitions else None
                src_week = e.week
                headword_match = (e.headword == row["headword"])
                source_definition_present = bool(e.definitions)

    sub_category, week_from_note = _parse_note_subcategory_week(row["note"])

    out = dict(row)
    out["note_subcategory"] = sub_category
    out["note_week"] = week_from_note
    out["db_definition_present"] = bool((row["definition"] or "").strip())
    out["xlsx_sheet_or_level"] = src_sheet_or_level
    out["xlsx_row_no"] = src_row_no
    out["xlsx_linked"] = linked
    out["xlsx_headword"] = src_headword
    out["xlsx_headword_match"] = headword_match
    out["xlsx_definition"] = src_definition
    out["xlsx_definition_present"] = source_definition_present
    out["xlsx_week"] = src_week
    out["xlsx_sense_category"] = src_sense_category
    out["xlsx_subject_category"] = src_subject_category
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--literacy-db", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    target = load_target_577(args.literacy_db)
    print(f"대상 577건 재추출: {len(target)}건 (기대값 577)")
    v_count = sum(1 for r in target if r["source"] == "schemareading-tooldict")
    s_count = sum(1 for r in target if r["source"] == "schemareading-schema")
    print(f"  V(schemareading-tooldict)={v_count}, S(schemareading-schema)={s_count}")
    assert len(target) == 577, f"기대와 다른 건수: {len(target)}"
    assert v_count == 1 and s_count == 576, f"기대와 다른 V/S 분포: V={v_count}, S={s_count}"

    schema_index, tooldict_index = build_xlsx_indexes()
    print(f"원천 XLSX 로드: schema 행 {len(schema_index)}건, tooldict 행 {len(tooldict_index)}건")

    linked_rows = [link_to_source(r, schema_index, tooldict_index) for r in target]

    n_linked = sum(1 for r in linked_rows if r["xlsx_linked"])
    n_unlinked = len(linked_rows) - n_linked
    n_mismatch = sum(1 for r in linked_rows if r["xlsx_linked"] and not r["xlsx_headword_match"])
    n_src_def = sum(1 for r in linked_rows if r["xlsx_definition_present"])
    print(f"\nXLSX 연결: {n_linked}/{len(linked_rows)}건 연결됨, 미연결 {n_unlinked}건, "
          f"연결됐지만 표제어 불일치 {n_mismatch}건")
    print(f"원천 XLSX에 정의가 실제로 있는 건: {n_src_def}/{len(linked_rows)}건")

    fieldnames = [
        "term_id", "headword", "pos", "source", "subject_category", "sense_category",
        "note_subcategory", "note_week", "definition", "db_definition_present",
        "review_status", "reviewed_at", "external_id", "grade_level", "grade_source",
        "xlsx_sheet_or_level", "xlsx_row_no", "xlsx_linked", "xlsx_headword",
        "xlsx_headword_match", "xlsx_definition", "xlsx_definition_present",
        "xlsx_week", "xlsx_sense_category", "xlsx_subject_category", "note",
    ]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(linked_rows)
    print(f"\n저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
