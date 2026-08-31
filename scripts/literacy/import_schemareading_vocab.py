"""스키마리딩 어휘 자료(학습 도구어 사전 + 스키마 어휘 목록) 정제 및 적재.

docs/literacy/04-스키마리딩어휘적재.md 참고. 어휘퀴즈DB.xlsx(4지선다 338문항)는
여기서 다루지 않는다 - quiz_items 대상이라 별도 지시서로 처리한다.

중복 처리 원칙(4-3절):
- **같은 source 내부**: 나선형 반복(같은 표제어가 여러 레벨에 재등장) - 최저
  레벨을 대표로 채택하고 note에 전체 등장 레벨을 남긴다.
- **source 간에는 절대 병합하지 않는다** - "교류"(도구어=일상 의미, 스키마=전기
  교류) 같은 동형이의어가 실제로 있어서, 병합하면 물리 용어가 초등 어휘가
  되는 오류가 생긴다.
- **스키마 자료는 시트(사회/과학/인문철학) 간에도 병합하지 않는다** - 실제
  파일을 확인해보니 같은 source 안에서도 시트가 다르면 동형이의어일 위험이
  있다(예: "기압"이 사회 시트와 과학 시트 양쪽에 있음, 8건 확인). 나선형
  반복 병합은 "같은 시트(sheet_key) 안에서 레벨만 다른 경우"로 한정한다.
- **tooldict는 같은 레벨 안에서도 표제어가 중복되는 경우가 있고(42건), 그중
  16건은 실제 동형이의어다** - Phase 3의 "동레벨 중복은 조용히 병합" 규칙은
  완전히 같은 내용이 재등장하는 걸 가정한 것이라 여기 안 맞는다. 버리지
  않고 `merge_same_level_duplicates()`로 대표 항목의 definitions에 이어붙여
  기존 `|`-다의어 처리 경로(examples)로 보존한다. 딱 1건("삶", L4)은 다른
  단어의 뜻풀이가 잘못 들어간 원본 데이터 오류로 판단해 제외한다.

실행:
    python scripts/literacy/import_schemareading_vocab.py --dry-run   # 저장 없이 결과만 출력
    python scripts/literacy/import_schemareading_vocab.py             # 실제 적재
"""
from __future__ import annotations

import argparse
import io
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemareading_parser import (  # noqa: E402
    SchemaEntry,
    ToolDictEntry,
    iter_schema_entries,
    iter_tooldict_entries,
    merge_same_level_duplicates,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

TOOLDICT_SOURCE = "schemareading-tooldict"
SCHEMA_SOURCE = "schemareading-schema"
LICENSE = "자체제작"


def group_tooldict(entries: list[ToolDictEntry]) -> dict[str, list[ToolDictEntry]]:
    groups: dict[str, list[ToolDictEntry]] = defaultdict(list)
    for e in entries:
        groups[e.headword].append(e)
    return groups


def pick_tooldict_representative(group: list[ToolDictEntry]) -> tuple[ToolDictEntry, list[int]]:
    ranked = sorted(group, key=lambda e: (e.level, e.row_no))
    rep = ranked[0]
    other_levels = sorted({e.level for e in group if e.level != rep.level})
    return rep, other_levels


def group_schema(entries: list[SchemaEntry]) -> dict[tuple[str, str], list[SchemaEntry]]:
    """(sheet_key, headword)로 그룹핑 - 시트(과목) 간에는 절대 안 합친다."""
    groups: dict[tuple[str, str], list[SchemaEntry]] = defaultdict(list)
    for e in entries:
        groups[(e.sheet_key, e.headword)].append(e)
    return groups


def pick_schema_representative(group: list[SchemaEntry]) -> tuple[SchemaEntry, list[int]]:
    ranked = sorted(group, key=lambda e: (e.level if e.level is not None else 99, e.row_no))
    rep = ranked[0]
    other_levels = sorted({e.level for e in group if e.level != rep.level and e.level is not None})
    return rep, other_levels


def build_tooldict_note(rep: ToolDictEntry, other_levels: list[int]) -> str | None:
    parts = []
    if rep.usage_note:
        parts.append(f"용법: {rep.usage_note}")
    if rep.data_error_note:
        parts.append(rep.data_error_note)
    if other_levels:
        parts.append("나선형 반복: " + ",".join(f"L{lv}" for lv in other_levels))
    return " / ".join(parts) if parts else None


def build_schema_note(rep: SchemaEntry, other_levels: list[int]) -> str | None:
    parts = []
    if rep.sub_category:
        parts.append(f"소분류: {rep.sub_category}")
    if rep.week:
        parts.append(f"주차: {rep.week}")
    if other_levels:
        parts.append("나선형 반복: " + ",".join(f"L{lv}" for lv in other_levels))
    return " / ".join(parts) if parts else None


def _insert_example_if_new(conn: sqlite3.Connection, term_id: int, source: str, sentence: str) -> None:
    exists = conn.execute(
        "SELECT 1 FROM examples WHERE term_id = ? AND source = ? AND sentence = ?",
        (term_id, source, sentence),
    ).fetchone()
    if not exists:
        conn.execute(
            "INSERT INTO examples (term_id, sentence, source) VALUES (?, ?, ?)",
            (term_id, sentence, source),
        )


def run(dry_run: bool) -> dict:
    tooldict_entries = list(iter_tooldict_entries())
    tooldict_entries, same_level_merges = merge_same_level_duplicates(tooldict_entries)
    schema_entries = list(iter_schema_entries())

    tooldict_groups = group_tooldict(tooldict_entries)
    tooldict_reps = [pick_tooldict_representative(g) for g in tooldict_groups.values()]

    schema_groups = group_schema(schema_entries)
    schema_reps = [pick_schema_representative(g) for g in schema_groups.values()]

    tooldict_words = {e.headword for e in tooldict_entries}
    schema_words = {e.headword for e in schema_entries}
    cross_source_words = sorted(tooldict_words & schema_words)

    schema_word_sheets: dict[str, set[str]] = defaultdict(set)
    for e in schema_entries:
        schema_word_sheets[e.headword].add(e.sheet_key)
    cross_sheet_words = sorted(w for w, sheets in schema_word_sheets.items() if len(sheets) > 1)

    report = {
        "tooldict_entries": tooldict_entries,
        "schema_entries": schema_entries,
        "tooldict_reps": tooldict_reps,
        "schema_reps": schema_reps,
        "cross_source_words": cross_source_words,
        "cross_sheet_words": cross_sheet_words,
        "same_level_merges": same_level_merges,
    }

    if dry_run:
        return report

    # --- 실제 적재 ---
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA foreign_keys=ON")

    run_ids = {}
    for source in (TOOLDICT_SOURCE, SCHEMA_SOURCE):
        cur = conn.execute(
            "INSERT INTO collection_runs (source, started_at, status) VALUES (?, ?, 'running')",
            (source, now),
        )
        run_ids[source] = cur.lastrowid
    conn.commit()

    inserted = {TOOLDICT_SOURCE: 0, SCHEMA_SOURCE: 0}
    try:
        for rep, other_levels in tooldict_reps:
            external_id = f"L{rep.level}-{rep.row_no}"
            note = build_tooldict_note(rep, other_levels)
            definition = rep.definitions[0] if rep.definitions else None

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO terms
                    (category, headword, origin, definition, pos, level, grade_source,
                     source, license, external_id, collected_at, review_status, note)
                VALUES ('어휘', ?, ?, ?, ?, ?, 'manual', ?, ?, ?, ?, '검수전', ?)
                """,
                (rep.headword, rep.origin, definition, rep.pos, rep.level,
                 TOOLDICT_SOURCE, LICENSE, external_id, now, note),
            )
            if cur.rowcount:
                inserted[TOOLDICT_SOURCE] += 1

            term_id = conn.execute(
                "SELECT id FROM terms WHERE source = ? AND external_id = ?",
                (TOOLDICT_SOURCE, external_id),
            ).fetchone()[0]

            for i, d in enumerate(rep.definitions[1:], start=2):
                _insert_example_if_new(conn, term_id, f"tooldict-sense-{i}", d)
            for s in rep.synonyms:
                _insert_example_if_new(conn, term_id, "tooldict-synonym", s)
            for a in rep.antonyms:
                _insert_example_if_new(conn, term_id, "tooldict-antonym", a)
            for rel in rep.related:
                _insert_example_if_new(conn, term_id, "tooldict-related", rel)

        conn.commit()

        for rep, other_levels in schema_reps:
            external_id = f"{rep.sheet_key}-{rep.row_no}"
            note = build_schema_note(rep, other_levels)
            review_status = "검수전" if rep.definition else "보류"

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO terms
                    (category, headword, definition, sense_category, subject_category,
                     level, grade_source, source, license, external_id, collected_at,
                     review_status, note)
                VALUES ('어휘', ?, ?, ?, ?, ?, 'manual', ?, ?, ?, ?, ?, ?)
                """,
                (rep.headword, rep.definition, rep.sense_category, rep.subject_category,
                 rep.level, SCHEMA_SOURCE, LICENSE, external_id, now, review_status, note),
            )
            if cur.rowcount:
                inserted[SCHEMA_SOURCE] += 1

        conn.commit()

        totals = {TOOLDICT_SOURCE: len(tooldict_reps), SCHEMA_SOURCE: len(schema_reps)}
        for source in (TOOLDICT_SOURCE, SCHEMA_SOURCE):
            conn.execute(
                """
                UPDATE collection_runs
                SET finished_at = ?, fetched_count = ?, inserted_count = ?, status = 'success'
                WHERE id = ?
                """,
                (datetime.now().isoformat(sep=" ", timespec="seconds"),
                 totals[source], inserted[source], run_ids[source]),
            )
        conn.commit()
    except Exception as e:
        for source in (TOOLDICT_SOURCE, SCHEMA_SOURCE):
            conn.execute(
                "UPDATE collection_runs SET finished_at = ?, status = 'failed', error = ? WHERE id = ?",
                (datetime.now().isoformat(sep=" ", timespec="seconds"), str(e), run_ids[source]),
            )
        conn.commit()
        raise
    finally:
        conn.close()

    report["inserted"] = inserted
    return report


def print_dry_run_report(report: dict) -> None:
    tooldict_reps = report["tooldict_reps"]
    schema_reps = report["schema_reps"]

    # 1. 자료별·레벨별 건수 표
    print("=== 1. 자료별·레벨별 건수 ===")
    tooldict_by_level = defaultdict(int)
    for rep, _ in tooldict_reps:
        tooldict_by_level[rep.level] += 1
    print(f"  tooldict 총 대표 표제어: {len(tooldict_reps)}건")
    for lv in sorted(tooldict_by_level):
        print(f"    L{lv}: {tooldict_by_level[lv]}건")

    schema_by_level = defaultdict(int)
    for rep, _ in schema_reps:
        schema_by_level[rep.level] += 1
    print(f"  schema 총 대표 표제어: {len(schema_reps)}건")
    for lv in sorted(schema_by_level, key=lambda x: (x is None, x)):
        print(f"    L{lv}: {schema_by_level[lv]}건")

    # 2. 정제 전후 비교 20건
    print("\n=== 2. 정제 전후 비교 (raw != cleaned, 최대 20건) ===")
    shown = 0
    for rep, _ in tooldict_reps + schema_reps:
        if rep.raw_headword != rep.headword:
            print(f"  {rep.raw_headword!r} -> {rep.headword!r}")
            shown += 1
            if shown >= 20:
                break
    if shown == 0:
        print("  없음 (전부 원본 그대로)")

    # 3. 같은 source 내 나선형 반복
    tooldict_spiral = [(rep, others) for rep, others in tooldict_reps if others]
    schema_spiral = [(rep, others) for rep, others in schema_reps if others]
    print(f"\n=== 3. 나선형 반복 (tooldict {len(tooldict_spiral)}건, schema {len(schema_spiral)}건) - 예시 20건 ===")
    shown = 0
    for rep, others in tooldict_spiral + schema_spiral:
        print(f"  {rep.headword} - 채택: L{rep.level}, 다른 레벨: {','.join(f'L{o}' for o in others)}")
        shown += 1
        if shown >= 20:
            break

    # 4. source 간(및 schema 시트 간) 동일 표제어
    print(f"\n=== 4. source 간 동일 표제어 (tooldict<->schema, 전체 {len(report['cross_source_words'])}건) ===")
    print(f"  {report['cross_source_words']}")
    print(f"\n    schema 내부 시트 간 동일 표제어(사회/과학/인문철학, 병합 안 함, {len(report['cross_sheet_words'])}건):")
    print(f"    {report['cross_sheet_words']}")

    # 5. definition 빈 건수
    tooldict_empty_def = sum(1 for rep, _ in tooldict_reps if not rep.definitions)
    schema_empty_def = sum(1 for rep, _ in schema_reps if not rep.definition)
    print(f"\n=== 5. definition 빈 건수 ===")
    print(f"  tooldict: {tooldict_empty_def}/{len(tooldict_reps)}")
    print(f"  schema: {schema_empty_def}/{len(schema_reps)}")

    # 6. sense_category 채움 비율
    schema_sense_filled = sum(1 for rep, _ in schema_reps if rep.sense_category)
    print(f"\n=== 6. sense_category 채움 비율 ===")
    print(f"  schema: {schema_sense_filled}/{len(schema_reps)} ({schema_sense_filled/len(schema_reps)*100:.1f}%)")
    print(f"  tooldict: 해당 없음(이 자료에는 sense_category 개념 없음, 전부 NULL)")

    # 7. 표제어에 이상 문자 포함 항목
    print("\n=== 7. 표제어에 공백·괄호·개행 등 이상 문자 포함 항목 전체 ===")
    weird_re = re.compile(r"[\s()\[\]]")
    weird = [rep for rep, _ in tooldict_reps + schema_reps if weird_re.search(rep.headword)]
    for rep in weird:
        source = "tooldict" if isinstance(rep, ToolDictEntry) else "schema"
        print(f"  [{source}] {rep.headword!r}")
    if not weird:
        print("  없음")

    # 8. Level4 7컬럼 분기 처리 결과 샘플 5건
    print("\n=== 8. Level4(7컬럼 분기) 처리 결과 샘플 5건 ===")
    level4_shown = 0
    for rep, _ in tooldict_reps:
        if rep.level == 4:
            print(f"  {rep.headword}: synonyms={rep.synonyms}, antonyms={rep.antonyms}, related={rep.related}")
            level4_shown += 1
            if level4_shown >= 5:
                break


def print_paren_fix_report(report: dict) -> None:
    """표제어 뒤 괄호(용법 예시/한자) 처리 결과를 전후 비교로 출력한다."""
    print("=== tooldict 표제어 괄호 처리 전후 비교 ===")
    shown = 0
    for rep, _ in report["tooldict_reps"]:
        before = rep.raw_headword.strip()
        if rep.origin is None and rep.usage_note is None:
            continue
        if rep.origin:
            print(f"  {before!r} -> headword={rep.headword!r}, origin={rep.origin!r}")
        else:
            print(f"  {before!r} -> headword={rep.headword!r}, note='용법: {rep.usage_note}'")
        shown += 1
    print(f"  (총 {shown}건 처리)")


def print_same_level_merge_report(report: dict) -> None:
    """같은 레벨 내 동형이의어 병합 결과 - 대표로 남은 뜻풀이와 examples로
    밀려난 뜻풀이를 나란히 출력한다(어느 쪽이 대표가 맞는지는 사람이 확인)."""
    merges = report["same_level_merges"]
    print(f"=== 같은 레벨 내 동형이의어 병합 결과 ({len(merges)}건) ===")
    for m in merges:
        print(f"  L{m.level} {m.headword!r} (대표 row_no={m.primary_row_no})")
        print(f"    대표로 채택된 뜻풀이(terms.definition은 이 중 첫 번째): {m.primary_definitions}")
        if m.pushed_definitions:
            print(f"    examples로 보존된 나머지 뜻풀이: {m.pushed_definitions}")
        if m.excluded_error_definitions:
            print(f"    원본 데이터 오류로 제외(보존 안 함): {m.excluded_error_definitions}")


def main() -> int:
    parser = argparse.ArgumentParser(description="스키마리딩 어휘 자료 정제 및 적재")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 결과만 출력")
    args = parser.parse_args()

    report = run(dry_run=args.dry_run)

    print_paren_fix_report(report)
    print()
    print_same_level_merge_report(report)
    print()

    if args.dry_run:
        print_dry_run_report(report)
        print("\n--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
        return 0

    print(f"terms 신규 - tooldict {report['inserted'][TOOLDICT_SOURCE]}건, "
          f"schema {report['inserted'][SCHEMA_SOURCE]}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())
