"""생성 문항(248건, review_status='검수전') O/X/보류 자동 판정 - 완전자동.

사용자 결정(2026-09-02, auto_review_level.py와 동일 배경).

핵심: 정답(correct_answer)이 terms.definition을 그대로 복사한 것이므로,
"오답과 정답이 논리적으로 앞뒤가 맞는지"만 보면 이미 알려진 문제(예: "통계"
문항 - terms.definition 자체가 원본 데이터 오류였던 사례)를 못 잡는다.
그래서 프롬프트에서 "정답이 실제로 그 표제어의 뜻으로 맞는지, 네 자신의
지식으로 독립적으로 판단하라"고 명시한다 - 이게 검수 UI 5절의 6개
불합격 사유 중 "정답이 틀림"을 자동으로 잡아내려는 지점이다.

대상: quiz_items WHERE quiz_type='뜻풀이선택' AND review_status='검수전'

실행:
    python auto_review_quiz.py --dry-run
    python auto_review_quiz.py --limit 30
    python auto_review_quiz.py
"""
from __future__ import annotations

import argparse
import io
import json
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

BATCH_SIZE = 10
REJECT_REASONS = [
    "정답이 틀림",
    "오답이 너무 쉬움",
    "오답이 정답에 가까움",
    "표기 문제",
    "레벨 부적절",
    "기타",
]


def select_targets(conn: sqlite3.Connection, limit: int | None) -> list[dict]:
    rows = conn.execute(
        """
        SELECT q.id, q.term_id, t.headword, t.category, t.level, t.source,
               q.question, q.correct_answer, q.distractors
        FROM quiz_items q JOIN terms t ON t.id = q.term_id
        WHERE q.quiz_type='뜻풀이선택' AND q.review_status='검수전'
        ORDER BY q.id
        """
    ).fetchall()
    targets = [
        {
            "quiz_id": r[0], "term_id": r[1], "headword": r[2], "category": r[3],
            "level": r[4], "source": r[5], "question": r[6], "correct_answer": r[7],
            "distractors": json.loads(r[8]) if r[8] else [],
        }
        for r in rows
    ]
    return targets[:limit] if limit else targets


def make_batches(targets: list[dict]) -> list[list[dict]]:
    return [targets[i:i + BATCH_SIZE] for i in range(0, len(targets), BATCH_SIZE)]


def build_prompt(batch: list[dict]) -> str:
    reasons_list = "/".join(REJECT_REASONS)
    parts = [
        "너는 국어 4지선다 문항의 품질을 검수하는 검수자다.",
        "",
        "각 문항마다 단어(또는 속담)와 정답, 오답 3개가 주어진다. 판정 기준:",
        "",
        "1. 가장 먼저 확인할 것 - 정답 자체가 실제로 그 단어의 뜻이 맞는지, "
        "네가 알고 있는 한국어 어휘 지식으로 독립적으로 판단하라. "
        "정답 텍스트를 무조건 믿지 말고, 표제어의 실제 뜻과 다르면 반드시 X로 판정하라.",
        "2. 오답 3개가 정답과 명확히 구분되는지(너무 쉬워서 소거법으로 풀리는지, "
        "반대로 정답과 너무 비슷해서 정답이 두 개처럼 보이는지) 확인하라.",
        "3. 숫자 접미사(예: '개설02')처럼 표제어 표기 자체가 이상하면 표기 문제로 X.",
        "4. 레벨(0~6, 초1~2부터 고3)에 비해 단어나 뜻풀이가 부적절하게 어렵거나 쉬우면 X.",
        "",
        "판정: O(합격) / X(불합격) / 보류(판단이 애매함) 중 하나.",
        f"X인 경우 사유를 반드시 다음 중 하나로 골라라: {reasons_list}",
        "",
        "대상 문항:",
    ]
    for item in batch:
        parts.append(f"- quiz_id: {item['quiz_id']}")
        parts.append(f"  단어: {item['headword']} (레벨 {item['level']}, {item['category']})")
        parts.append(f"  정답: {item['correct_answer']}")
        for i, d in enumerate(item["distractors"], 1):
            parts.append(f"  오답{i}: {d}")
        parts.append("")

    parts.append(
        "출력은 아래 JSON 형식만 반환하라:\n"
        '{"items": [{"quiz_id": 1, "verdict": "O", "reject_reason": null, "rationale": "..."}, '
        '{"quiz_id": 2, "verdict": "X", "reject_reason": "정답이 틀림", "rationale": "..."}, ...]}\n'
        f"배열에는 위 {len(batch)}개 quiz_id가 전부, 그리고 그것만 있어야 한다. "
        "verdict가 X가 아니면 reject_reason은 null이다."
    )
    return "\n".join(parts)


def save_result(conn: sqlite3.Connection, quiz_id: int, verdict: str, reject_reason: str | None,
                 rationale: str, now: str) -> None:
    status_map = {"O": "검수완료", "X": "제외", "보류": "보류"}
    status = status_map.get(verdict)
    if status is None:
        raise ValueError(f"알 수 없는 verdict: {verdict}")
    conn.execute(
        "UPDATE quiz_items SET review_status=?, reject_reason=?, "
        "note=?, reviewed_at=? WHERE id=?",
        (status, reject_reason if verdict == "X" else None,
         f"[AI 자동 판정] {rationale}", now, quiz_id),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="생성 문항 완전자동 O/X/보류 판정")
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
    verdict_counts = {"O": 0, "X": 0, "보류": 0}
    failed = 0
    for bi, batch in enumerate(batches, 1):
        print(f"[배치 {bi}/{len(batches)}] {len(batch)}건")
        prompt = build_prompt(batch)
        try:
            parsed = call_gemini_json(client, prompt)
        except Exception as e:  # noqa: BLE001
            print(f"  실패: {e}", file=sys.stderr)
            failed += len(batch)
            continue

        by_id = {item["quiz_id"]: item for item in batch}
        items = {int(it["quiz_id"]): it for it in parsed.get("items", [])}
        for quiz_id, item in by_id.items():
            result = items.get(quiz_id)
            if not result:
                print(f"  누락: quiz_id={quiz_id}", file=sys.stderr)
                failed += 1
                continue
            verdict = result.get("verdict")
            reject_reason = result.get("reject_reason")
            rationale = result.get("rationale", "")
            if verdict not in verdict_counts:
                print(f"  알 수 없는 verdict: {verdict} (quiz_id={quiz_id})", file=sys.stderr)
                failed += 1
                continue
            if args.dry_run:
                tag = f"[{verdict}]" + (f"({reject_reason})" if reject_reason else "")
                print(f"  {item['headword']} {tag}: {rationale}")
            else:
                save_result(conn, quiz_id, verdict, reject_reason, rationale, now)
            verdict_counts[verdict] += 1
        if not args.dry_run:
            conn.commit()

    conn.close()
    print(f"\nO={verdict_counts['O']}, X={verdict_counts['X']}, 보류={verdict_counts['보류']}, "
          f"실패={failed}" + (" (dry-run - 저장 안 함)" if args.dry_run else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
