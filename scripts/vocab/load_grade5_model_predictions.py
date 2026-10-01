# -*- coding: utf-8 -*-
"""scripts/vocab/nikl_grade5_gemini_classifier.py가 만든 예측 CSV를
vocabulary_grade5_candidate_model_predictions에 적재한다. 사람 판정
테이블(vocabulary_grade5_candidate_judgments)은 이 스크립트가 절대
건드리지 않는다 - 완전히 분리된 별도 테이블.

같은 (candidate_id, model_version, prompt_template_hash) 조합이 이미
있으면 SKIP(재적재해도 중복 삽입 안 됨) - apply_grade5_candidate_batch.py
류의 compare-and-swap까지는 아니지만, 이 테이블은 사람이 수정하지 않는
순수 기계 산출물이라 "이미 있으면 건너뛴다"만으로 충분하다(값이 다르면
그건 모델/프롬프트가 바뀐 것이므로 model_version/prompt_template_hash
자체가 달라져 자연히 새 행이 된다 - 충돌 개념이 없음).

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/load_grade5_model_predictions.py --csv <예측.csv>

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/load_grade5_model_predictions.py --csv <예측.csv> --apply
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.vocab.db_path_guard import DbPathGuardError, connect_rw, guard_db_path  # noqa: E402


def _to_int_or_none(v: str) -> int | None:
    if v in (None, "", "None"):
        return None
    return 1 if str(v).strip().lower() in ("true", "1") else 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--database", default=None)
    args = parser.parse_args()

    try:
        db_path = guard_db_path(args.database)
    except DbPathGuardError as e:
        print(f"GATE 1 FAIL: {e}")
        sys.exit(1)
    print("GATE 1 PASS: 가드 통과")

    if Path(db_path).name != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: research DB 파일명이 아님 - 중단:", db_path)
        sys.exit(1)

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print("GATE 3 FAIL: 입력 파일 없음:", csv_path)
        sys.exit(1)

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    print(f"GATE 3 PASS: 입력 {len(rows)}건")

    computed_at = datetime.now(timezone.utc).isoformat()
    con = connect_rw(db_path)
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='vocabulary_grade5_candidate_model_predictions'"
    )
    if cur.fetchone() is None:
        print("GATE 4 FAIL: vocabulary_grade5_candidate_model_predictions 테이블이 없습니다 - "
              "먼저 migrate_add_grade5_model_predictions.py를 실행하세요")
        con.close()
        sys.exit(1)

    existing_keys = set()
    cur.execute("SELECT candidate_id, model_version, prompt_template_hash FROM vocabulary_grade5_candidate_model_predictions")
    for cid, mv, th in cur.fetchall():
        existing_keys.add((cid, mv, th))

    to_insert = []
    skipped = 0
    for r in rows:
        key = (r["candidate_id"], r["model"], r["prompt_template_hash"])
        if key in existing_keys:
            skipped += 1
            continue
        to_insert.append((
            r["candidate_id"], r["model"], r["model"], r["prompt_template_hash"],
            r["predicted_judgment"] or None, r["predicted_reason"] or None,
            _to_int_or_none(r["grounded"]), _to_int_or_none(r["borderline"]),
            1 if str(r["api_error"]).strip().lower() == "true" else 0,
            r["error_detail"] or None, computed_at,
        ))

    print(f"GATE 5: 신규 삽입 대상 {len(to_insert)}건, 이미 존재(스킵) {skipped}건")

    if not args.apply:
        print("[DRY-RUN] 실제 삽입 안 함.")
        con.close()
        return

    try:
        cur.execute("BEGIN")
        cur.executemany(
            "INSERT INTO vocabulary_grade5_candidate_model_predictions "
            "(candidate_id, model_name, model_version, prompt_template_hash, predicted_judgment, "
            "predicted_reason, grounded, borderline, api_error, error_detail, computed_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            to_insert,
        )
        con.commit()
        print(f"GATE 6 PASS: 단일 트랜잭션 커밋 완료({len(to_insert)}건 삽입)")
    except Exception as e:  # noqa: BLE001
        con.rollback()
        print(f"GATE 6 FAIL: 예외 발생, 전체 롤백: {e}")
        con.close()
        raise

    cur.execute("SELECT COUNT(*) FROM vocabulary_grade5_candidate_judgments")
    judgments_count = cur.fetchone()[0]
    print(f"GATE 7: 사람 판정 테이블(vocabulary_grade5_candidate_judgments) 행 수 = {judgments_count} "
          f"(이 스크립트는 이 테이블을 절대 건드리지 않음 - 변경 없어야 정상)")

    con.close()


if __name__ == "__main__":
    main()
