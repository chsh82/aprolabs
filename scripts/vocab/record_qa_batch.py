"""vocabulary_qa_batches에 사람 표본검수 결과 1행을 기록한다.

사용자가 직접 검수한 결과를 그대로 받아 적는다 - 개수를 재계산하거나
vocabulary_review_samples의 review_status를 건드리지 않는다(요청 범위 밖).

실행:
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/record_qa_batch.py \\
        --version 2.1.29 --reviewed 100 --pass-count 100 --revise 0 --exclude 0 \\
        --critical 0 --major 0 --minor 0 --approved-for-rnd-quiz \\
        --notes "사람 표본검수 100개에서 오류 없음"
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocabulary_quiz.db import get_db_path  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--version", required=True)
    p.add_argument("--reviewed", type=int, required=True)
    p.add_argument("--pass-count", type=int, required=True)
    p.add_argument("--revise", type=int, required=True)
    p.add_argument("--exclude", type=int, required=True)
    p.add_argument("--critical", type=int, required=True)
    p.add_argument("--major", type=int, required=True)
    p.add_argument("--minor", type=int, required=True)
    p.add_argument("--approved-for-rnd-quiz", action="store_true")
    p.add_argument("--approved-for-public", action="store_true")
    p.add_argument("--notes", default=None)
    args = p.parse_args()

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO vocabulary_qa_batches
           (version, reviewed_sample_count, pass_count, revise_count, exclude_count,
            critical_error_count, major_error_count, minor_error_count,
            approved_for_rnd_quiz, approved_for_public, reviewed_at, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (args.version, args.reviewed, args.pass_count, args.revise, args.exclude,
         args.critical, args.major, args.minor,
         1 if args.approved_for_rnd_quiz else 0, 1 if args.approved_for_public else 0,
         datetime.now(timezone.utc).isoformat(), args.notes),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM vocabulary_qa_batches ORDER BY id DESC LIMIT 1").fetchone()
    print("기록 완료:", row)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
