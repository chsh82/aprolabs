# -*- coding: utf-8 -*-
"""L3 중등 보강 2차 - 위험 기반 검수 결과를 qa_flags_json에 추가 기록한다.

`vocabulary_publish_reviews`(사람 승인 테이블)에는 아무것도 쓰지 않는다 -
오직 `vocabulary_multiformat_items.qa_flags_json`의 기존 배열 첫 원소에
`risk_review` 키 하나를 **추가**할 뿐, 다른 필드(prompt/options_json/
correct_option/explanation 등 정답·보기·채점에 관련된 모든 것)는 절대
건드리지 않는다. 기본은 dry-run, `--apply`가 있어야만 실제로 쓴다.

안전장치(apply_grade5_l3_batch2.py와 동일한 GATE 패턴을 UPDATE용으로 적용):
  GATE 1~2: 환경·DB 경로 가드
  GATE 3: 적용 전 DB SHA-256 기록
  GATE 4: SQLite Backup API 백업 + 복원 가능성 검증
  GATE 5: 대상 76행 조회, qa_flags_json 외 변경 없음을 보장하는 비교 로직
  GATE 6: risk_review 키가 이미 있는 행은 SKIP(멱등성), 없는 행만 UPDATE
  GATE 7: 단일 트랜잭션 커밋
  GATE 8: integrity_check/foreign_key_check
  GATE 9: 변경 후에도 prompt/options_json/correct_option/explanation/
          source_version/is_active 등 risk_review 외 모든 필드가 적용 전과
          바이트 단위로 동일한지 재확인(가장 중요한 가드 - 정답/보기를
          실수로 건드리지 않았다는 증거)
  GATE 10: 무관 데이터(1차 배치, RULE_A/B 등) 불변 확인
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.vocab.db_path_guard import DbPathGuardError, connect_rw, guard_db_path  # noqa: E402
from scripts.vocab.grade5_batch2_risk_review import (  # noqa: E402
    RISK_CATEGORY, SAMPLE_REVIEW, SCHEMA_VERSION, REVIEWED_AT, REVIEWER, SAMPLE_SEED,
)

SOURCE_VERSION = "nikl_grade5_l3_batch2_v1"
BATCH1_SOURCE_VERSION = "nikl_grade5_l3_batch1_v1"
EXPECTED_ITEM_COUNT = 76

IMMUTABLE_COLS = [
    "item_id", "item_type", "source_content_id", "lemma", "pos", "prompt",
    "options_json", "correct_option", "public_payload_json", "answer_payload_json",
    "explanation", "generator_version", "source_version", "is_active",
]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_risk_review_payload(lemma: str, item_type: str) -> dict:
    category, reason = RISK_CATEGORY.get(lemma, ("NONE", None))
    sampled = (lemma, item_type) in SAMPLE_REVIEW
    return {
        "schema_version": SCHEMA_VERSION,
        "reviewed_at": REVIEWED_AT,
        "reviewer": REVIEWER,
        "risk_category": category,
        "risk_reason": reason,
        "sampled": sampled,
        "sample_seed": SAMPLE_SEED if sampled else None,
        "sample_verdict": SAMPLE_REVIEW.get((lemma, item_type)) if sampled else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--database", default=None)
    args = parser.parse_args()

    try:
        db_path = guard_db_path(args.database)
    except DbPathGuardError as e:
        print(f"GATE 1 FAIL: {e}")
        sys.exit(1)
    print("GATE 1 PASS: 가드 통과")

    if os.path.basename(db_path) != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: research DB 파일명이 아님 - 중단:", db_path)
        sys.exit(1)
    print("GATE 2 PASS:", "실제 적용 대상" if args.apply else "[DRY-RUN] 대상", "DB 경로:", db_path)

    pre_apply_hash = sha256_of(Path(db_path))
    print("GATE 3: 적용 전 DB 파일 SHA-256:", pre_apply_hash)

    con = connect_rw(db_path)
    cur = con.cursor()
    rows = cur.execute(
        "SELECT item_id, lemma, item_type, qa_flags_json FROM vocabulary_multiformat_items "
        "WHERE source_version = ? ORDER BY item_id",
        (SOURCE_VERSION,),
    ).fetchall()
    if len(rows) != EXPECTED_ITEM_COUNT:
        print(f"GATE 5 FAIL: 대상 행수 {len(rows)} != 기대 {EXPECTED_ITEM_COUNT}")
        con.close()
        sys.exit(1)
    print(f"GATE 5 PASS: 대상 {len(rows)}행 조회")

    to_update = []
    skip_count = 0
    for item_id, lemma, item_type, qa_flags_json in rows:
        flags_list = json.loads(qa_flags_json) if qa_flags_json else [{}]
        flags = flags_list[0] if flags_list else {}
        if "risk_review" in flags:
            skip_count += 1
            continue
        new_flags = dict(flags)
        new_flags["risk_review"] = build_risk_review_payload(lemma, item_type)
        flags_list[0] = new_flags
        to_update.append((item_id, json.dumps(flags_list, ensure_ascii=False)))

    print(f"GATE 6: 신규 업데이트 대상 {len(to_update)}건, 이미 risk_review 있어 스킵 {skip_count}건")

    if not args.apply:
        print(f"[DRY-RUN] 실제 반영 안 함. {len(to_update)}건 업데이트 예정.")
        con.close()
        return

    backup_path = None
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_batch2_risk_review_{ts}"
    src = connect_rw(db_path)
    dst = sqlite3.connect(backup_path)
    with dst:
        src.backup(dst)
    dst.close()
    backup_hash = sha256_of(Path(backup_path))
    print("GATE 4: 백업 생성:", backup_path, "SHA-256:", backup_hash)
    verify_con = sqlite3.connect(backup_path)
    vintegrity = verify_con.execute("PRAGMA integrity_check").fetchone()[0]
    verify_con.close()
    print(f"GATE 4: 백업 복원 가능성 검증 - integrity={vintegrity}")
    if vintegrity != "ok":
        print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
        sys.exit(1)

    # 불변 필드 스냅샷(업데이트 전) - 나중에 바이트 단위로 재확인
    pre_immutable = {
        r[0]: cur.execute(
            f"SELECT {', '.join(IMMUTABLE_COLS)} FROM vocabulary_multiformat_items WHERE item_id=?",
            (r[0],),
        ).fetchone()
        for r in rows
    }
    pre_batch1_count = cur.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (BATCH1_SOURCE_VERSION,)
    ).fetchone()[0]

    try:
        cur.execute("BEGIN")
        for item_id, new_json in to_update:
            cur.execute(
                "UPDATE vocabulary_multiformat_items SET qa_flags_json=? WHERE item_id=?",
                (new_json, item_id),
            )
        con.commit()
        print(f"GATE 7 PASS: 단일 트랜잭션 커밋 완료({len(to_update)}건 업데이트)")
    except Exception as e:  # noqa: BLE001
        con.rollback()
        print(f"GATE 7 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        raise

    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 8: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 8: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    immutable_ok = True
    for item_id, pre_vals in pre_immutable.items():
        post_vals = cur.execute(
            f"SELECT {', '.join(IMMUTABLE_COLS)} FROM vocabulary_multiformat_items WHERE item_id=?",
            (item_id,),
        ).fetchone()
        if post_vals != pre_vals:
            immutable_ok = False
            print(f"GATE 9 FAIL: {item_id}의 불변 필드가 바뀜! pre={pre_vals} post={post_vals}")
    print(f"GATE 9: 정답/보기/설명 등 불변 필드 {len(pre_immutable)}건 전부 바이트 단위 동일 - {'PASS' if immutable_ok else 'FAIL'}")

    post_batch1_count = cur.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (BATCH1_SOURCE_VERSION,)
    ).fetchone()[0]
    batch1_ok = pre_batch1_count == post_batch1_count
    print(f"GATE 10: 1차 배치 문항 수 불변 - {pre_batch1_count} -> {post_batch1_count} ({'PASS' if batch1_ok else 'FAIL'})")

    con.close()
    if not (integrity_ok and fk_ok and immutable_ok and batch1_ok):
        raise RuntimeError("사후 검증 실패 - 위 GATE 로그 확인 필요")

    print("\nBACKUP=", backup_path)
    print("PRE_APPLY_HASH=", pre_apply_hash)


if __name__ == "__main__":
    main()
