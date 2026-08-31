"""교재 어휘 정제 및 적재 - momo_book_db/momo_book.db의 vocabulary를 literacy.db terms로.

docs/literacy/03-교재어휘적재.md 참고. 교재DB는 SQLite mode=ro로만 연다 -
어떤 경우에도 쓰지 않는다. literacy.db 쪽은 app.literacy.db.get_db_path()로
경로만 가져와 raw sqlite3로 직접 연다(수집은 오래 걸리는 배치라 웹앱 세션에
묶이면 안 됨 - 이 프로젝트 전반의 관례).

이 어휘들은 실제 학생이 해당 레벨에서 읽은 단어라 등급의 "정답지"로 쓰인다 -
양보다 정확도가 중요하다. 그래서:
- 컬럼(word/definition) 뒤바뀜 의심 행은 적재하지 않고 사람 검수로 넘긴다
- 교재DB 데이터 자체는 절대 고치지 않는다(오류를 발견해도 기록만 함)
- 정제 전 원문은 항상 terms.note에 보존한다

실행:
    python scripts/literacy/import_textbook_vocab.py --dry-run   # 저장 없이 정제/매칭 결과만 출력
    python scripts/literacy/import_textbook_vocab.py             # 실제 적재(승인 후에만)
"""
from __future__ import annotations

import argparse
import csv
import io
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from krdict_dump import Entry, iter_entries  # noqa: E402
from normalize import CleanResult, clean_headword, has_space, is_suspected_swap  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

TEXTBOOK_DB_PATH = REPO_ROOT / "momo_book_db" / "momo_book.db"
KRDICT_XML_DIR = REPO_ROOT / "raw" / "krdict" / "krdict_dump"
UNMATCHED_CSV_PATH = REPO_ROOT / "data" / "literacy" / "unmatched.csv"

LEVEL_TO_GRADE = {f"L{i}": i for i in range(1, 10)}
SOURCE = "momo-textbook"
LICENSE = "자체제작"


def get_textbook_conn() -> sqlite3.Connection:
    """교재DB - mode=ro로만 연다. 쓰기 시도는 SQLite가 직접 막는다."""
    uri = f"file:{TEXTBOOK_DB_PATH.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_vocabulary_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT v.id, v.word, v.definition, v.example_sentence, d.level
        FROM vocabulary v
        JOIN documents d ON v.doc_id = d.doc_id
        """
    ).fetchall()


class Candidate:
    """정제 + 레벨까지 붙은 vocabulary 1행."""

    def __init__(self, row: sqlite3.Row, clean: CleanResult):
        self.vocab_id = row["id"]
        self.definition = row["definition"]
        self.level = row["level"]
        self.grade_level = LEVEL_TO_GRADE.get(row["level"])
        self.clean = clean
        # 공백 포함은 더 이상 여기서 미리 걷어내지 않는다(사용자 결정 -
        # "경을 치다"/"얼이 빠지다" 같은 정상 관용구가 공백 규칙 하나로
        # 잘못 걸러진 걸 실측으로 확인함). 사전 매칭 결과를 본 뒤에
        # 분류한다 - run()의 매칭 단계 참고.
        self.exclude_reason: str | None = "컬럼의심" if is_suspected_swap(clean.cleaned) else None
        self.has_space = has_space(clean.cleaned)

    @property
    def suspected(self) -> bool:
        return self.exclude_reason == "컬럼의심"


def build_krdict_index() -> dict[str, list[Entry]]:
    xml_paths = sorted(KRDICT_XML_DIR.glob("*.xml"))
    index: dict[str, list[Entry]] = defaultdict(list)
    for entry in iter_entries(xml_paths):
        index[entry.headword].append(entry)
    return index


def pick_krdict_match(index: dict[str, list[Entry]], headword: str) -> tuple[Entry | None, int]:
    """(선택된 항목, 전체 동음이의 후보 수)를 반환한다. 없으면 (None, 0)."""
    candidates = index.get(headword)
    if not candidates:
        return None, 0
    word_candidates = [e for e in candidates if e.lexical_unit == "단어"]
    chosen_pool = word_candidates or candidates
    return chosen_pool[0], len(candidates)


def group_by_headword(candidates: list[Candidate]) -> dict[str, list[Candidate]]:
    groups: dict[str, list[Candidate]] = defaultdict(list)
    for c in candidates:
        groups[c.clean.cleaned].append(c)
    return groups


def pick_representative(group: list[Candidate]) -> tuple[Candidate, list[str]]:
    """그룹 안에서 최저 레벨(동률이면 vocab_id 최소)을 대표로, 그 외 레벨 목록을 반환."""
    ranked = sorted(group, key=lambda c: (c.grade_level, c.vocab_id))
    representative = ranked[0]
    other_levels = sorted({c.level for c in group if c.level != representative.level})
    return representative, other_levels


def build_note(representative: Candidate, other_levels: list[str], krdict_note: str | None) -> str:
    parts = [f"원문: {representative.clean.original}"]
    if other_levels:
        parts.append(f"{','.join(other_levels)} 중복 등장")
    if krdict_note:
        parts.append(krdict_note)
    return " / ".join(parts)


def run(dry_run: bool) -> dict:
    textbook_conn = get_textbook_conn()
    rows = load_vocabulary_rows(textbook_conn)

    candidates = [Candidate(row, clean_headword(row["word"])) for row in rows]
    textbook_conn.close()  # 교재DB는 다 읽었으니 즉시 닫는다(오래 열어둘 이유 없음)

    suspected = [c for c in candidates if c.suspected]
    clean_ok = [c for c in candidates if c.exclude_reason is None]
    changed_count = sum(1 for c in candidates if c.clean.cleaned != c.clean.original)
    # 어차피 제외되는 행(컬럼의심)은 여기서 또 볼 필요 없음 - 실제로
    # 적재될 행 중 남은 괄호만 보고한다.
    other_paren_entries = [c for c in clean_ok if c.clean.other_parens]

    groups = group_by_headword(clean_ok)
    representatives: list[tuple[Candidate, list[str]]] = [
        (*pick_representative(group),) for group in groups.values()
    ]
    dup_level_reps = [(rep, others) for rep, others in representatives if others]

    krdict_index = build_krdict_index()

    matched = 0
    unmatched_rows: list[Candidate] = []
    sense_cat_filled = 0
    subject_cat_filled = 0
    match_results: dict[int, tuple[Entry | None, int]] = {}

    space_matched: list[Candidate] = []
    space_unmatched: list[Candidate] = []

    for rep, _ in representatives:
        entry, homonym_count = pick_krdict_match(krdict_index, rep.clean.cleaned)
        match_results[rep.vocab_id] = (entry, homonym_count)
        if entry is None:
            unmatched_rows.append(rep)
            if rep.has_space:
                space_unmatched.append(rep)
            continue
        matched += 1
        if rep.has_space:
            space_matched.append(rep)
        if entry.sense_category:
            sense_cat_filled += 1
        if entry.subject_category:
            subject_cat_filled += 1

    total_reps = len(representatives)

    report = {
        "total_vocab_rows": len(candidates),
        "distinct_raw_word": len({c.clean.original for c in candidates}),
        "changed_count": changed_count,
        "suspected": suspected,
        "other_paren_entries": other_paren_entries,
        "dup_level_reps": dup_level_reps,
        "total_representatives": total_reps,
        "matched": matched,
        "unmatched_rows": unmatched_rows,
        "sense_cat_filled": sense_cat_filled,
        "subject_cat_filled": subject_cat_filled,
        "representatives": representatives,
        "match_results": match_results,
        "space_matched": space_matched,
        "space_unmatched": space_unmatched,
    }

    if dry_run:
        return report

    # --- 실제 적재 ---
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    literacy_conn = sqlite3.connect(get_db_path())
    literacy_conn.execute("PRAGMA foreign_keys=ON")

    started_at = now
    cur = literacy_conn.execute(
        "INSERT INTO collection_runs (source, started_at, status) VALUES (?, ?, 'running')",
        (SOURCE, started_at),
    )
    run_id = cur.lastrowid
    literacy_conn.commit()

    unmatched_csv_rows: list[tuple[str, str, str, str]] = []
    for c in suspected:
        unmatched_csv_rows.append((c.clean.cleaned, c.level, c.clean.original, "컬럼의심"))

    inserted = 0
    try:
        for rep, other_levels in representatives:
            entry, homonym_count = match_results[rep.vocab_id]
            krdict_note = f"krdict 동음이의 {homonym_count}건 중 1번 채택" if homonym_count > 1 else None
            note = build_note(rep, other_levels, krdict_note)

            definition = rep.definition or (entry.definitions[0] if entry and entry.definitions else None)
            pos = entry.pos if entry else None
            origin = rep.clean.origin or None
            sense_category = entry.sense_category if entry else None
            subject_category = entry.subject_category if entry else None
            review_status = "검수전" if entry else "보류"
            # 매칭된 krdict 항목이 관용구면 category도 관용구로 저장한다(사용자
            # 결정 - "경을 치다"/"얼이 빠지다" 같은 건 원래부터 관용구이므로
            # '어휘'로 넣으면 안 됨). 매칭 안 되거나 단어/구/문법·표현이면 '어휘'.
            category = "관용구" if entry and entry.lexical_unit == "관용구" else "어휘"

            cur = literacy_conn.execute(
                """
                INSERT OR IGNORE INTO terms
                    (category, headword, origin, definition, pos, sense_category,
                     subject_category, grade_level, grade_source, source, license,
                     external_id, collected_at, review_status, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual', ?, ?, ?, ?, ?, ?)
                """,
                (category, rep.clean.cleaned, origin, definition, pos, sense_category, subject_category,
                 rep.grade_level, SOURCE, LICENSE, str(rep.vocab_id), now, review_status, note),
            )
            if cur.rowcount:
                inserted += 1

            if entry and entry.definitions:
                term_id = literacy_conn.execute(
                    "SELECT id FROM terms WHERE source = ? AND external_id = ?",
                    (SOURCE, str(rep.vocab_id)),
                ).fetchone()[0]
                exists = literacy_conn.execute(
                    "SELECT 1 FROM examples WHERE term_id = ? AND source = 'krdict-definition'",
                    (term_id,),
                ).fetchone()
                if not exists:
                    literacy_conn.execute(
                        "INSERT INTO examples (term_id, sentence, source) VALUES (?, ?, 'krdict-definition')",
                        (term_id, entry.definitions[0]),
                    )

            if entry is None:
                # 공백 포함인데 매칭도 안 된 건 "어휘아님의심"(정상 관용구인데 krdict에
                # 없을 수도 있어 사람이 판단) - 공백 없이 단순 매칭 실패는 기존대로.
                reason = "어휘아님의심" if rep.has_space else "krdict매칭실패"
                unmatched_csv_rows.append((rep.clean.cleaned, rep.level, rep.clean.original, reason))

        literacy_conn.commit()

        UNMATCHED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(UNMATCHED_CSV_PATH, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["단어", "레벨", "원문", "실패사유"])
            writer.writerows(unmatched_csv_rows)

        literacy_conn.execute(
            """
            UPDATE collection_runs
            SET finished_at = ?, fetched_count = ?, inserted_count = ?, status = 'success'
            WHERE id = ?
            """,
            (datetime.now().isoformat(sep=" ", timespec="seconds"), total_reps, inserted, run_id),
        )
        literacy_conn.commit()
    except Exception as e:
        literacy_conn.execute(
            "UPDATE collection_runs SET finished_at = ?, status = 'failed', error = ? WHERE id = ?",
            (datetime.now().isoformat(sep=" ", timespec="seconds"), str(e), run_id),
        )
        literacy_conn.commit()
        raise
    finally:
        literacy_conn.close()

    report["inserted"] = inserted
    report["unmatched_csv_rows"] = len(unmatched_csv_rows)
    return report


def print_dry_run_report(report: dict) -> None:
    print(f"vocabulary 총 행수: {report['total_vocab_rows']}, 고유 word(원문): {report['distinct_raw_word']}")
    print(f"정제로 값이 바뀐 행수: {report['changed_count']}")

    print("\n=== 정제 전후 비교 20건 ===")
    shown = 0
    for rep, _ in report["representatives"]:
        if rep.clean.cleaned == rep.clean.original:
            continue
        print(f"  {rep.clean.original!r} -> {rep.clean.cleaned!r}")
        shown += 1
        if shown >= 20:
            break

    print(f"\n=== 4절 규칙: 여러 레벨에 등장한 단어 ({len(report['dup_level_reps'])}건) ===")
    for rep, others in report["dup_level_reps"]:
        print(f"  {rep.clean.cleaned} - 채택: {rep.level}, 다른 레벨: {','.join(others)}")

    print(f"\n=== 컬럼 뒤바뀜 의심 행 전체 ({len(report['suspected'])}건) ===")
    for c in report["suspected"]:
        print(f"  id={c.vocab_id} [{c.level}] {c.clean.cleaned!r}")

    space_matched = report["space_matched"]
    space_unmatched = report["space_unmatched"]
    print(f"\n=== 공백 포함 표제어 처리 결과 (전체 {len(space_matched) + len(space_unmatched)}건) ===")
    print(f"  매칭 성공({len(space_matched)}건):")
    for c in space_matched:
        entry, _ = report["match_results"][c.vocab_id]
        print(f"    id={c.vocab_id} {c.clean.cleaned!r} -> category={'관용구' if entry.lexical_unit == '관용구' else '어휘'}"
              f" (krdict lexicalUnit={entry.lexical_unit})")
    print(f"  매칭 실패 - review_status='보류', unmatched.csv 사유='어휘아님의심' ({len(space_unmatched)}건):")
    for c in space_unmatched:
        print(f"    id={c.vocab_id} [{c.level}] {c.clean.cleaned!r}")

    matched = report["matched"]
    total_reps = report["total_representatives"]
    rate = matched / total_reps * 100 if total_reps else 0
    print(f"\n=== 사전 매칭 ===")
    print(f"  대표 표제어 {total_reps}건 중 매칭 {matched}건, 실패 {len(report['unmatched_rows'])}건 ({rate:.1f}%)")
    print(f"  sense_category 채움: {report['sense_cat_filled']}/{matched} "
          f"({report['sense_cat_filled']/matched*100:.1f}%)" if matched else "  sense_category 채움: 0/0")
    print(f"  subject_category 채움: {report['subject_cat_filled']}/{matched} "
          f"({report['subject_cat_filled']/matched*100:.1f}%)" if matched else "  subject_category 채움: 0/0")

    print(f"\n=== 정제 후에도 괄호가 남은 표제어 전체 ({len(report['other_paren_entries'])}건) ===")
    for c in report["other_paren_entries"]:
        print(f"  id={c.vocab_id} {c.clean.cleaned!r} (원문: {c.clean.original!r})")


def main() -> int:
    parser = argparse.ArgumentParser(description="교재 어휘 정제 및 적재")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 정제/매칭 결과만 출력")
    args = parser.parse_args()

    report = run(dry_run=args.dry_run)

    if args.dry_run:
        print_dry_run_report(report)
        print("\n--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
        return 0

    print(f"terms 신규 {report['inserted']}건, unmatched.csv {report['unmatched_csv_rows']}행")
    return 0


if __name__ == "__main__":
    sys.exit(main())
