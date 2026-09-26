#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase26 항목3/4: phase23의 잔여 V NEW_CANDIDATE 35건 중 NEW_PRIVATE_LOADABLE
32건(HOLD 3건 제외)을 vocabulary_quiz_research.db에 두 번째 L6 배치로 단일
트랜잭션 적재한다. phase24_apply_l6_core.py를 batch2용으로 최소 수정한 스크립트
(같은 게이트 구조: 존재 확인 -> 단일 트랜잭션 -> 커밋 후 무결성 검사).

절대 하지 않는 것: vocabulary_items/vocabulary_multiformat_items(퀴즈 문항) 생성,
student_exposure/public_ready를 0이 아닌 값으로 설정, HOLD 3건(매커니즘/이성/신장)
적재, literacy.db 쓰기, git 작업.

게이트(하나라도 실패하면 아무것도 쓰지 않고 중단):
  1. APP_ENV=research 재확인 (.env + 실행 중 프로세스 /proc/<pid>/environ)
  2. db-path basename 하드가드
  3. 입력 CSV의 content_id가 현재 DB에 전혀 존재하지 않음(dangling 방지) 확인
  4. 단일 트랜잭션 삽입 (executemany 아님, 전부 성공 or 전부 롤백)
  5. 트랜잭션 커밋 후 무결성 검사 + 기존 행(신규 32건 제외 전체) 체크섬 불변 확인

멱등성: 이미 존재하는 content_id는 스킵(재실행해도 중복 삽입 없음).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

DB_BASENAME = "vocabulary_quiz_research.db"
BATCH_ID = "SCHEMA_READING_PHASE26_L6_V35_BATCH2"
MERGE_SOURCE = "SCHEMA_READING_PHASE26_L6_V35_BATCH2"
SOURCE_VERSION = "schema_reading_literacy_l6_manual_v2"  # phase24(v1)와 구분되는 두 번째 L6
    # 배치 고유 마커 - quiz.py/multiformat.py의 문항 매칭 조건과 겹치지 않음
GENERATION_METHOD = "MANUAL_STRUCTURED_AUTHORING"
QA_METHOD = "DETERMINISTIC_PLUS_HEURISTIC"
GENERATION_STATUS = None
LEVEL_VERSION = "level_policy_v0.1"
LEVEL_STATUS = "REVIEW_BOUNDARY"
VOCAB_LEVEL = 6
TARGET_GRADE_BAND = "고등 2~3학년"
LEVEL_SOURCE = "MANUAL_LITERACY_L6_MATCH"
CONTENT_ID_PREFIX = "SR_L6COREV2_"


def read_env_value(env_file: Path, key: str) -> str | None:
    if not env_file.exists():
        return None
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()
    return None


def read_process_environ(pid: int, key: str) -> str | None:
    data = Path(f"/proc/{pid}/environ").read_bytes()
    for entry in data.split(b"\0"):
        if entry.startswith(f"{key}=".encode("utf-8")):
            return entry.split(b"=", 1)[1].decode("utf-8")
    return None


def find_uvicorn_app_main_pid() -> int | None:
    out = subprocess.run(["pgrep", "-f", "uvicorn app.main:app"], capture_output=True, text=True)
    pids = [int(p) for p in out.stdout.split() if p.strip()]
    return pids[0] if pids else None


def table_snapshot(con: sqlite3.Connection, table: str, exclude_prefix: str | None = None) -> tuple[int, str]:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    where = ""
    params: tuple = ()
    if exclude_prefix and "content_id" in cols:
        where = "WHERE content_id NOT LIKE ?"
        params = (exclude_prefix + "%",)
    rows = con.execute(f"SELECT {', '.join(cols)} FROM {table} {where} ORDER BY id", params).fetchall()
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join("" if v is None else str(v) for v in row).encode("utf-8"))
        h.update(b"\x1e")
    return len(rows), h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--rows-csv", type=Path, required=True)
    args = ap.parse_args()

    env_app_env = read_env_value(args.env_file, "APP_ENV")
    print(f"GATE 1a: .env APP_ENV={env_app_env!r}")
    if env_app_env != "research":
        print("GATE 1a FAIL - 중단, 아무것도 쓰지 않음")
        return 1

    pid = find_uvicorn_app_main_pid()
    if pid is None:
        print("GATE 1b FAIL: 'uvicorn app.main:app' 실행 중인 프로세스를 찾지 못함 - 중단")
        return 1
    proc_app_env = read_process_environ(pid, "APP_ENV")
    proc_db_path = read_process_environ(pid, "VOCABULARY_QUIZ_DB_PATH")
    print(f"GATE 1b: PID={pid} process APP_ENV={proc_app_env!r} VOCABULARY_QUIZ_DB_PATH={proc_db_path!r}")
    if proc_app_env != "research" or proc_db_path != str(args.db_path):
        print("GATE 1b FAIL - .env와 실행 중 프로세스 environ 불일치 또는 research 아님 - 중단")
        return 1
    print("GATE 1 PASS (.env와 실행 중 프로세스 environ byte-for-byte 일치)")

    print(f"GATE 2: db-path basename={args.db_path.name!r}")
    if args.db_path.name != DB_BASENAME:
        print("GATE 2 FAIL - research DB가 아님 - 중단")
        return 1
    print("GATE 2 PASS")

    with open(args.rows_csv, encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))
    loadable = [r for r in all_rows if r["final_classification"] == "NEW_PRIVATE_LOADABLE"]
    held = [r for r in all_rows if r["final_classification"] == "HOLD"]
    if len(loadable) + len(held) != len(all_rows):
        print(f"GATE 0 FAIL: final_classification 값이 예상 밖(NEW_PRIVATE_LOADABLE/HOLD 외) - 중단")
        return 1
    print(f"\n입력: 전체 {len(all_rows)}건, LOAD 대상 {len(loadable)}건, HOLD {len(held)}건")
    for r in held:
        print(f"  HOLD: {r['lemma']} - {r['hold_reason']}")

    content_rows = []
    for r in loadable:
        expected_cid = f"{CONTENT_ID_PREFIX}{r['literacy_term_id']}"
        if r["content_id"] != expected_cid:
            print(f"GATE 0 FAIL: {r['lemma']} content_id={r['content_id']!r} != 기대값 {expected_cid!r} - 중단")
            return 1
        content_rows.append((expected_cid, r))

    con = sqlite3.connect(str(args.db_path))
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    pre_vc_count, pre_vc_checksum = table_snapshot(con, "vocabulary_contents", CONTENT_ID_PREFIX)
    pre_vcl_count, pre_vcl_checksum = table_snapshot(con, "vocabulary_content_levels", CONTENT_ID_PREFIX)
    pre_mfi_count, pre_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    print(f"\n적재 전 스냅샷: vocabulary_contents(신규 접두어 제외)={pre_vc_count}행 sha256={pre_vc_checksum}")
    print(f"적재 전 스냅샷: vocabulary_content_levels(신규 접두어 제외)={pre_vcl_count}행 sha256={pre_vcl_checksum}")
    print(f"적재 전 스냅샷: vocabulary_multiformat_items(전체)={pre_mfi_count}행 sha256={pre_mfi_checksum}")

    # GATE 3: 멱등성 사전 점검
    existing = set()
    cur.execute(f"SELECT content_id FROM vocabulary_contents WHERE content_id LIKE '{CONTENT_ID_PREFIX}%'")
    for (cid,) in cur.fetchall():
        existing.add(cid)
    to_insert = [(cid, r) for cid, r in content_rows if cid not in existing]
    to_skip = [(cid, r) for cid, r in content_rows if cid in existing]
    print(f"\nGATE 3: 이미 존재(스킵 대상) {len(to_skip)}건, 신규 삽입 대상 {len(to_insert)}건")

    if not to_insert:
        print("신규 삽입 대상 0건 - 멱등 재실행으로 판단, 트랜잭션 없이 종료")
        con.close()
        print("INSERTED=0")
        print(f"SKIPPED={len(to_skip)}")
        print(f"HELD={len(held)}")
        return 0

    try:
        cur.execute("BEGIN")
        for cid, r in to_insert:
            cur.execute(
                """INSERT INTO vocabulary_contents
                   (content_id, sense_id, lexical_entry_id, batch_id, lemma, pos,
                    canonical_definition, student_definition, example_sentence,
                    example_target_form, generation_method, qa_method, generation_status,
                    student_exposure, public_ready, quality_batch_id, hold_reason,
                    merge_source, source_version, is_active)
                   VALUES (?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, NULL, ?, ?, 1)""",
                (
                    cid, BATCH_ID, r["lemma"], r["pos"], r["canonical_definition"],
                    r["student_definition"], r["example_sentence"], r["example_target_form"],
                    GENERATION_METHOD, QA_METHOD, GENERATION_STATUS, MERGE_SOURCE, SOURCE_VERSION,
                ),
            )
            level_reason = {
                "literacy_term_id": r["literacy_term_id"],
                "literacy_source": r["source"],
                "phase23_group": "L6_CANDIDATE_BATCH2",
                "is_S_subject_concept": False,
                "subject_category": None,
                "note_week": None,
                "caution": r["caution"],
                "loaded_by": "schema_reading_phase26_l6_v35_batch2",
                "expert_review_required": False,
                "expert_review_status": "NOT_APPLICABLE",
                "note": "REVIEW_BOUNDARY: 사람이 직접 literacy.db 정의 대조 후 작성, 자동 채점 미실시. "
                        "phase23의 잔여 V NEW_CANDIDATE 35건 중 매커니즘(표기 정책 미정)/이성/신장"
                        "(동형이의 목표 뜻 미확정)을 제외한 32건. grade_level 컬럼이 NULL이라 '고2~3' "
                        "근거는 docs/literacy/07-학년경계정책-L5L6.md 정책(L6=고2~3)에 의존하며 "
                        "개별 학년 태그는 없다.",
            }
            cur.execute(
                """INSERT INTO vocabulary_content_levels
                   (content_id, vocab_level, target_grade_band, level_score, level_confidence,
                    level_status, boundary_flag, level_source, level_version, level_reason_json,
                    is_active)
                   VALUES (?, ?, ?, NULL, NULL, ?, 1, ?, ?, ?, 1)""",
                (
                    cid, VOCAB_LEVEL, TARGET_GRADE_BAND, LEVEL_STATUS, LEVEL_SOURCE, LEVEL_VERSION,
                    json.dumps(level_reason, ensure_ascii=False),
                ),
            )
        con.commit()
        print(f"\nGATE 4 PASS: 단일 트랜잭션 커밋 완료, 삽입 {len(to_insert)}건 (vocabulary_contents + vocabulary_content_levels 각각)")
    except Exception as e:
        con.rollback()
        print(f"\nGATE 4 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        return 2

    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 5: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 5: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    new_ids = [cid for cid, _ in to_insert]
    placeholders = ",".join("?" for _ in new_ids)
    cur.execute(
        f"SELECT content_id, student_exposure, public_ready FROM vocabulary_contents WHERE content_id IN ({placeholders})",
        new_ids,
    )
    exposure_rows = cur.fetchall()
    all_private = all(se == 0 and pr == 0 for _, se, pr in exposure_rows)
    print(f"GATE 5: 신규 {len(exposure_rows)}건 student_exposure/public_ready 전부 0 = {all_private}")

    cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
    mfi_count = cur.fetchone()[0]
    print(f"GATE 5: vocabulary_multiformat_items 행수(변화 없어야 함) = {mfi_count}")

    post_vc_count, post_vc_checksum = table_snapshot(con, "vocabulary_contents", CONTENT_ID_PREFIX)
    vc_unchanged = (post_vc_count, post_vc_checksum) == (pre_vc_count, pre_vc_checksum)
    print(f"GATE 5: 기존 vocabulary_contents(신규 32건 제외) 체크섬 불변 = {vc_unchanged}")

    post_vcl_count, post_vcl_checksum = table_snapshot(con, "vocabulary_content_levels", CONTENT_ID_PREFIX)
    vcl_unchanged = (post_vcl_count, post_vcl_checksum) == (pre_vcl_count, pre_vcl_checksum)
    print(f"GATE 5: 기존 vocabulary_content_levels(신규 32건 제외) 체크섬 불변 = {vcl_unchanged}")

    post_mfi_count, post_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    mfi_unchanged = (post_mfi_count, post_mfi_checksum) == (pre_mfi_count, pre_mfi_checksum)
    print(f"GATE 5: vocabulary_multiformat_items 불변 = {mfi_unchanged} ({pre_mfi_count} -> {post_mfi_count}행)")

    con.close()
    ok = integrity_ok and fk_ok and all_private and vc_unchanged and vcl_unchanged and mfi_unchanged
    print(f"\nINSERTED={len(to_insert)}")
    print(f"SKIPPED={len(to_skip)}")
    print(f"HELD={len(held)}")
    print(f"INTEGRITY_OK={integrity_ok}")
    print(f"FK_OK={fk_ok}")
    print(f"EXPOSURE_ALL_PRIVATE={all_private}")
    print(f"EXISTING_ROWS_UNCHANGED={vc_unchanged and vcl_unchanged and mfi_unchanged}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
