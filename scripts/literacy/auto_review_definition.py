"""뜻풀이 공백 어휘(724건) 자동 생성 - 완전자동(사람 확인 없음).

사용자 결정(2026-09-02, auto_review_level.py와 동일 배경). Phase 6 지시서가
"이번 단계에서 AI 자동 생성 붙이지 마라, 자동 생성은 별도 단계에서 결정한다"고
미뤄둔 그 결정을 지금 내린 것 - 검수 UI(수동, /literacy/review/definition)는
그대로 남아있고, 이 스크립트는 별도의 완전자동 경로다.

대상: terms WHERE category='어휘' AND (definition IS NULL OR definition='')

같은 sense_category(없으면 level)로 묶어서 배치 호출하고, 이미 뜻풀이가
있는 같은 그룹 어휘 몇 개를 문체 참고용으로 프롬프트에 넣는다(검수 UI의
"같은 소분류의 다른 어휘 뜻풀이 참고"와 같은 아이디어).

note 필드는 기존 내용(소분류/주차 등)이 있으므로 덮어쓰지 않고 뒤에 붙인다.

실행:
    python auto_review_definition.py --dry-run
    python auto_review_definition.py --limit 50
    python auto_review_definition.py
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
from gemini_client import call_gemini_json, get_client  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from app.literacy.db import get_db_path  # noqa: E402

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

BATCH_SIZE = 15


def select_targets(conn: sqlite3.Connection, limit: int | None) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, headword, level, sense_category, subject_category, note
        FROM terms
        WHERE category='어휘' AND (definition IS NULL OR definition='')
        ORDER BY id
        """
    ).fetchall()
    targets = [
        {"id": r[0], "headword": r[1], "level": r[2], "sense_category": r[3],
         "subject_category": r[4], "note": r[5]}
        for r in rows
    ]
    return targets[:limit] if limit else targets


def _sub_category(note: str | None) -> str | None:
    if not note:
        return None
    m = re.search(r"소분류:\s*([^/]+)", note)
    return m.group(1).strip() if m else None


def make_batches(targets: list[dict]) -> list[tuple[str, list[dict]]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in targets:
        key = t["sense_category"] or f"level:{t['level']}"
        groups[key].append(t)
    batches = []
    for key, items in groups.items():
        for i in range(0, len(items), BATCH_SIZE):
            batches.append((key, items[i:i + BATCH_SIZE]))
    return batches


def fetch_references(conn: sqlite3.Connection, sense_category: str | None, level: int | None, exclude_ids: set[int]) -> list[tuple[str, str]]:
    if sense_category:
        rows = conn.execute(
            """
            SELECT headword, definition FROM terms
            WHERE category='어휘' AND sense_category=? AND definition IS NOT NULL AND definition != ''
            LIMIT 20
            """,
            (sense_category,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT headword, definition FROM terms
            WHERE category='어휘' AND level=? AND definition IS NOT NULL AND definition != ''
            LIMIT 20
            """,
            (level,),
        ).fetchall()
    return rows[:5]


def build_prompt(batch: list[dict], references: list[tuple[str, str]], label: str) -> str:
    parts = [
        "너는 한국 초중고 학생용 국어사전에 실릴 어휘 뜻풀이를 작성한다.",
        f"이번 배치의 공통 주제/레벨: {label}",
        "",
        "규칙:",
        "- 국어사전 표제어 뜻풀이 문체를 따른다(예: '~하는 것.', '~함.', '~하다.')",
        "- 간결하게 한 문장으로 쓴다(길어도 60자 이내)",
        "- 없는 사실을 지어내지 말고, 표제어의 일반적으로 알려진 뜻만 쓴다",
        "",
    ]
    if references:
        parts.append("참고용 - 같은 그룹의 다른 어휘 뜻풀이(문체만 참고, 베끼지 마라):")
        for hw, defi in references:
            parts.append(f"- {hw}: {defi}")
        parts.append("")

    parts.append("아래 표제어들의 뜻풀이를 작성하라:")
    for item in batch:
        parts.append(f"- id: {item['id']}, 표제어: {item['headword']}")

    parts.append(
        "\n출력은 아래 JSON 형식만 반환하라:\n"
        '{"items": [{"id": 123, "definition": "..."}, ...]}\n'
        f"배열에는 위 {len(batch)}개 id가 전부, 그리고 그것만 있어야 한다."
    )
    return "\n".join(parts)


def save_result(conn: sqlite3.Connection, term_id: int, definition: str, now: str) -> None:
    conn.execute(
        "UPDATE terms SET definition=?, review_status='검수완료', reviewed_at=?, "
        "note = COALESCE(note || ' / ', '') || '[AI 자동 생성 뜻풀이]' WHERE id=?",
        (definition, now, term_id),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="뜻풀이 공백 어휘 완전자동 생성")
    parser.add_argument("--dry-run", action="store_true", help="10건만 생성해서 출력, DB 저장 안 함")
    parser.add_argument("--limit", type=int, default=None, help="처리할 최대 건수")
    args = parser.parse_args()

    conn = sqlite3.connect(get_db_path())
    conn.execute("PRAGMA foreign_keys=ON")

    limit = 10 if args.dry_run else args.limit
    targets = select_targets(conn, limit)
    if not targets:
        print("대상 없음")
        conn.close()
        return 0

    print(f"대상 {len(targets)}건")
    client = get_client()
    batches = make_batches(targets)

    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    done = failed = 0
    for bi, (label, batch) in enumerate(batches, 1):
        print(f"[배치 {bi}/{len(batches)}] {label} - {len(batch)}건")
        exclude_ids = {b["id"] for b in batch}
        references = fetch_references(conn, batch[0]["sense_category"], batch[0]["level"], exclude_ids)
        prompt = build_prompt(batch, references, label)
        try:
            parsed = call_gemini_json(client, prompt)
        except Exception as e:  # noqa: BLE001
            print(f"  실패: {e}", file=sys.stderr)
            failed += len(batch)
            continue

        by_id = {item["id"]: item for item in batch}
        items = {int(it["id"]): it for it in parsed.get("items", [])}
        for term_id, item in by_id.items():
            result = items.get(term_id)
            if not result or not result.get("definition"):
                print(f"  누락: id={term_id}", file=sys.stderr)
                failed += 1
                continue
            definition = result["definition"].strip()
            if args.dry_run:
                print(f"  {item['headword']}: {definition}")
            else:
                save_result(conn, term_id, definition, now)
                done += 1
        if not args.dry_run:
            conn.commit()

    conn.close()
    print(f"\n완료 {done}건, 실패 {failed}건" + (" (dry-run - 저장 안 함)" if args.dry_run else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
