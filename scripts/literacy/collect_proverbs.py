"""속담·관용구 수집 - krdict 전체 덤프에서 lexicalUnit='속담'|'관용구' 항목을 추출해 저장.

CLAUDE.md 6절 조사 결과 - 검색 API(`part=ip`)는 한 글자 검색어에 대해 0건을
반환해서(예: '가'/'나'/'다' 전부 0건) 순회 시드 방식으로는 커버리지를 증명할
수 없었다. 그래서 API 순회를 버리고, krdict의 "사전 전체 내려받기"(XML,
인증 불필요) 파일을 직접 파싱하는 방식으로 전환했다. 이 방식은 "파일을
열어보면 커버리지가 그대로 확인된다"는 장점이 있다 - 실제로 덤프에서 센
속담/관용구 개수가 krdict 공식 통계(속담 657 / 관용구 2,227)와 정확히
일치하는 것으로 완전 수집을 증명했다.

저장 규칙:
- terms: 첫 번째 Sense의 definition만. category는 '속담' 또는 '관용구'.
  source='krdict', license='공공누리 제1유형(출처표시)', grade_level=NULL,
  grade_source='auto', review_status='검수전'.
- examples: 두 번째 Sense부터, sentence=해당 Sense의 definition,
  source='krdict-sense-{순번}' (순번은 2부터). 뜻풀이를 버리지 않기 위함
  (docs/literacy/02-속담수집.md 후속 논의 - 스키마는 바꾸지 않음).
- syntacticPattern(관용구 구문 패턴)은 이번 단계에서 저장하지 않는다.
  덤프 원본이 raw/krdict/에 남아있으니 나중에 필요하면 재처리하면 된다.

**중요 - external_id는 LexicalEntry@id 그대로 쓰지 않는다**: 실제 덤프를
받아 확인한 결과, `LexicalEntry@id`는 속담/관용구 자신의 고유 ID가 아니라
그것이 딸려 있는 원 표제어(부모 단어)의 ID를 그대로 물려받은 값이었다(예:
"코" 관련 관용구·속담 16개가 전부 id=46814를 공유). 그래서 `(id, headword)`
조합을 external_id로 쓴다 - 2,884건 전체에서 이 조합이 완전히 유일함을
직접 확인했다(id 단독은 1160종류뿐, headword 단독도 1건 중복 있음).

중복 방지: terms는 UNIQUE(source, external_id) + INSERT OR IGNORE.
examples는 UNIQUE 제약이 없어서, INSERT 전에 (term_id, source) 존재 여부를
직접 확인해서 중복을 막는다(재실행해도 행 수가 늘지 않아야 하므로).

DB는 ORM을 쓰지 않고 raw sqlite3로 직접 연다(app.literacy.db.get_db_path()로
경로만 가져옴) - 수집은 오래 걸리고 실패가 잦은 배치라 웹앱 세션에 묶이면
안 된다.

실행:
    python scripts/literacy/collect_proverbs.py               # 전체 수집
    python scripts/literacy/collect_proverbs.py --dry-run      # DB 저장 없이 개수만 출력
    python scripts/literacy/collect_proverbs.py --redownload   # 캐시된 덤프 무시하고 새로 받음
"""
from __future__ import annotations

import argparse
import io
import random
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from krdict_dump import Entry, download_dump, extract_dump, iter_entries  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

CATEGORIES = ("속담", "관용구")
SOURCE = "krdict"
LICENSE = "공공누리 제1유형(출처표시)"


def load_target_entries(force_redownload: bool = False) -> list[Entry]:
    zip_path = download_dump(force=force_redownload)
    xml_paths = extract_dump(zip_path)
    return [e for e in iter_entries(xml_paths) if e.lexical_unit in CATEGORIES]


def save_entries(conn: sqlite3.Connection, entries: list[Entry]) -> tuple[int, int]:
    """(신규 terms 건수, 신규 examples 건수)를 반환한다."""
    inserted_terms = 0
    inserted_examples = 0
    now = datetime.now().isoformat(sep=" ", timespec="seconds")

    for entry in entries:
        first_definition = entry.definitions[0] if entry.definitions else ""
        # LexicalEntry@id는 부모 표제어와 공유되는 값이라 그대로 못 씀 - (id, headword)로 유일성 확보
        external_id = f"{entry.external_id}:{entry.headword}"

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO terms
                (category, headword, definition, source, license, external_id,
                 collected_at, review_status, grade_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, '검수전', 'auto')
            """,
            (entry.lexical_unit, entry.headword, first_definition, SOURCE, LICENSE,
             external_id, now),
        )
        if cur.rowcount:
            inserted_terms += 1

        term_row = conn.execute(
            "SELECT id FROM terms WHERE source = ? AND external_id = ?",
            (SOURCE, external_id),
        ).fetchone()
        term_id = term_row[0]

        for order, definition in enumerate(entry.definitions[1:], start=2):
            example_source = f"krdict-sense-{order}"
            exists = conn.execute(
                "SELECT 1 FROM examples WHERE term_id = ? AND source = ?",
                (term_id, example_source),
            ).fetchone()
            if exists:
                continue
            conn.execute(
                "INSERT INTO examples (term_id, sentence, source) VALUES (?, ?, ?)",
                (term_id, definition, example_source),
            )
            inserted_examples += 1

    conn.commit()
    return inserted_terms, inserted_examples


def main() -> int:
    parser = argparse.ArgumentParser(description="krdict 덤프에서 속담·관용구 수집")
    parser.add_argument("--dry-run", action="store_true",
                       help="DB에 저장하지 않고 파싱 결과 개수만 출력")
    parser.add_argument("--redownload", action="store_true",
                       help="캐시된 덤프를 무시하고 새로 받음")
    args = parser.parse_args()

    entries = load_target_entries(force_redownload=args.redownload)
    counts = {cat: sum(1 for e in entries if e.lexical_unit == cat) for cat in CATEGORIES}
    print(f"덤프에서 추출: {counts}, 합계 {len(entries)}건")

    if args.dry_run:
        print("--dry-run - DB에 저장하지 않음")
        sample = random.sample(entries, min(5, len(entries)))
        for e in sample:
            print(f"  [{e.lexical_unit}] {e.headword}: {e.definitions[0] if e.definitions else '(정의 없음)'}")
        return 0

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")

    started_at = datetime.now().isoformat(sep=" ", timespec="seconds")
    cur = conn.execute(
        "INSERT INTO collection_runs (source, started_at, status) VALUES (?, ?, 'running')",
        ("krdict-dump", started_at),
    )
    run_id = cur.lastrowid
    conn.commit()

    try:
        inserted_terms, inserted_examples = save_entries(conn, entries)
        conn.execute(
            """
            UPDATE collection_runs
            SET finished_at = ?, fetched_count = ?, inserted_count = ?, status = 'success'
            WHERE id = ?
            """,
            (datetime.now().isoformat(sep=" ", timespec="seconds"), len(entries),
             inserted_terms, run_id),
        )
        conn.commit()
    except Exception as e:
        conn.execute(
            "UPDATE collection_runs SET finished_at = ?, status = 'failed', error = ? WHERE id = ?",
            (datetime.now().isoformat(sep=" ", timespec="seconds"), str(e), run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()

    print(f"terms 신규 {inserted_terms}건, examples 신규 {inserted_examples}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())
