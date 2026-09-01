"""속담·관용구(level IS NULL) 레벨 자동 부여 - 완전자동(사람 확인 없음).

사용자 결정(2026-09-02): 검수 UI(수동)와 별개로, Gemini가 판정하면 사람
확인 없이 바로 확정한다. 다만 "누가/무엇이 판정했는지"는 note에 남겨서
나중에 구분할 수 있게 한다 - review_status='검수완료'가 사람 검수인지
AI 자동 판정인지 note로만 구분 가능(스키마는 안 바꿈).

대상: terms WHERE category IN ('속담','관용구') AND level IS NULL
      AND review_status != '제외'

레벨 0~6 중 하나 또는 "해당없음"(교육과정에 낼 만하지 않음 - review_UI의
"해당없음" 버튼과 같은 의미) 판정. "해당없음"이면 review_status='제외'.

실행:
    python auto_review_level.py --dry-run          # 10건만 판정해서 출력, 저장 안 함
    python auto_review_level.py --limit 50          # 50건만 실제 저장
    python auto_review_level.py                      # 전체 실행
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
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

BATCH_SIZE = 20

LEVEL_TABLE = """| level | 학년 |
|---|---|
| 0 | 초1~2 |
| 1 | 초3~4 |
| 2 | 초5~6 |
| 3 | 중1~2 |
| 4 | 중3 |
| 5 | 고1~2 |
| 6 | 고3 |"""


def select_targets(conn: sqlite3.Connection, limit: int | None) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, headword, definition, category
        FROM terms
        WHERE category IN ('속담','관용구') AND level IS NULL AND review_status != '제외'
        ORDER BY id
        """
    ).fetchall()
    targets = [{"id": r[0], "headword": r[1], "definition": r[2], "category": r[3]} for r in rows]
    return targets[:limit] if limit else targets


def build_prompt(batch: list[dict]) -> str:
    parts = [
        "너는 한국 초중고 국어 교육과정 관점에서 속담·관용구에 학습 레벨을 부여한다.",
        "",
        "레벨 대응표:",
        LEVEL_TABLE,
        "",
        "판단 기준:",
        "- 얼마나 널리 쓰이고 익숙한 표현인지, 학교 교과서/국어 학습에 등장할 만한지로 판단한다",
        "- 뜻이 어렵거나 한자어가 많이 섞여 있으면 레벨이 높다",
        "- 초중고 교육과정에 낼 만한 표현이 아니면(너무 저속하거나, 사장된 표현이거나, "
        "교육적으로 부적절하면) \"해당없음\"으로 판정한다",
        "",
        "아래 각 항목에 대해 레벨(0~6) 또는 \"해당없음\"을 판정하고, 한 줄 근거를 달아라.",
        "",
    ]
    for item in batch:
        parts.append(f"- id: {item['id']}")
        parts.append(f"  {item['category']}: {item['headword']}")
        parts.append(f"  뜻: {item['definition']}")
        parts.append("")

    parts.append(
        "출력은 아래 JSON 형식만 반환하라:\n"
        '{"items": [{"id": 123, "level": 3, "reason": "..."}, '
        '{"id": 456, "level": null, "reason": "..."}, ...]}\n'
        "level은 0~6 정수 또는 해당없음이면 null이다. "
        f"배열에는 위 {len(batch)}개 id가 전부, 그리고 그것만 있어야 한다."
    )
    return "\n".join(parts)


def make_batches(targets: list[dict]) -> list[list[dict]]:
    return [targets[i:i + BATCH_SIZE] for i in range(0, len(targets), BATCH_SIZE)]


def save_result(conn: sqlite3.Connection, term_id: int, level: int | None, reason: str, now: str) -> None:
    if level is None:
        conn.execute(
            "UPDATE terms SET review_status='제외', reviewed_at=?, "
            "note = COALESCE(note || ' / ', '') || ? WHERE id=?",
            (now, f"[AI 자동 판정: 해당없음] {reason}", term_id),
        )
    else:
        conn.execute(
            "UPDATE terms SET level=?, grade_source='auto', review_status='검수완료', reviewed_at=?, "
            "note = COALESCE(note || ' / ', '') || ? WHERE id=?",
            (level, now, f"[AI 자동 레벨 부여: {level}] {reason}", term_id),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="속담·관용구 레벨 완전자동 판정")
    parser.add_argument("--dry-run", action="store_true", help="10건만 판정해서 출력, DB 저장 안 함")
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
    for bi, batch in enumerate(batches, 1):
        print(f"[배치 {bi}/{len(batches)}] {len(batch)}건")
        prompt = build_prompt(batch)
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
            if not result:
                print(f"  누락: id={term_id}", file=sys.stderr)
                failed += 1
                continue
            level = result.get("level")
            reason = result.get("reason", "")
            if args.dry_run:
                print(f"  {item['headword']}: level={level} - {reason}")
            else:
                save_result(conn, term_id, level, reason, now)
                done += 1
        if not args.dry_run:
            conn.commit()

    conn.close()
    print(f"\n완료 {done}건, 실패 {failed}건" + (" (dry-run - 저장 안 함)" if args.dry_run else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
