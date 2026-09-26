#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase22: SR_L4CORE_4786("정기") vocabulary_contents.student_definition
단일 행을 게이트 기반으로 수정한다(phase13/14/vq_research_backup.py 패턴 재사용).

배경: phase21이 원인 조사(phase16 하드코딩 스크립트가 vocabulary_contents를
갱신하는 코드 경로 자체를 만든 적이 없음)와 수정안(dry-run)을 냈다
(reports/schema_reading_phase21_literacy_10fix_and_jeonggi_root_cause_20260926.md,
data/import/schema_reading_phase21_jeonggi_student_definition_fix_dryrun_20260926.json).
phase22는 그 제안을 실제로 적용하는 유일한 단일 트랜잭션이다.

절대 하지 않는 것: student_exposure/public_ready 변경, 다른 어떤 content_id/컬럼
변경, vocabulary_multiformat_items/vocabulary_content_levels 쓰기, literacy.db 쓰기,
git 작업.

게이트(하나라도 실패하면 아무것도 쓰지 않고 중단):
  1. APP_ENV=research 재확인 (.env + 실행 중 uvicorn 프로세스 /proc/<pid>/environ)
  2. db-path basename 하드가드
  3. 하드 가드: content_id='SR_L4CORE_4786'의 현재 student_definition이 정확히
     기대 구버전 문자열과 일치해야 함 - 다르면(이미 누가 바꿨거나 등) 즉시 중단
  4. 단일 트랜잭션 UPDATE (student_definition 컬럼만)
  5. 커밋 후 검증: 1행 새 값 확인, 나머지 전 행 체크섬 불변,
     vocabulary_multiformat_items/vocabulary_content_levels 행수·내용 불변,
     integrity_check/foreign_key_check, exposure/public_ready 합계 여전히 0

멱등성: 재실행 시 GATE 3에서 이미 새 값이면 "대상 아님"으로 스킵하고 종료(0건).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

DB_BASENAME = "vocabulary_quiz_research.db"
TARGET_CONTENT_ID = "SR_L4CORE_4786"
OLD_VALUE = "일정한 기간마다 되풀이하도록 정한 것"
NEW_VALUE = "기한이나 기간이 일정하게 정해져 있는 것"


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


def row_checksum(con: sqlite3.Connection, exclude_content_id: str) -> tuple[int, str]:
    """content_id != exclude 인 모든 vocabulary_contents 행의 체크섬(행수, sha256)."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(vocabulary_contents)").fetchall()]
    rows = con.execute(
        f"SELECT {', '.join(cols)} FROM vocabulary_contents "
        f"WHERE content_id != ? ORDER BY content_id",
        (exclude_content_id,),
    ).fetchall()
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join("" if v is None else str(v) for v in row).encode("utf-8"))
        h.update(b"\x1e")
    return len(rows), h.hexdigest()


def table_snapshot(con: sqlite3.Connection, table: str) -> tuple[int, str]:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    rows = con.execute(f"SELECT {', '.join(cols)} FROM {table} ORDER BY id").fetchall()
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join("" if v is None else str(v) for v in row).encode("utf-8"))
        h.update(b"\x1e")
    return len(rows), h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    args = ap.parse_args()

    # GATE 1: APP_ENV=research 재확인 (.env + 실행 중 프로세스 environ)
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

    # GATE 2: db-path 하드가드
    print(f"GATE 2: db-path basename={args.db_path.name!r}")
    if args.db_path.name != DB_BASENAME:
        print("GATE 2 FAIL - research DB가 아님 - 중단")
        return 1
    if not args.db_path.exists():
        print(f"GATE 2 FAIL: db-path가 존재하지 않음: {args.db_path}")
        return 1
    print("GATE 2 PASS")

    con = sqlite3.connect(str(args.db_path))
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    # UPDATE 전 스냅샷(나머지 행 체크섬 + 관련 테이블 스냅샷)
    pre_other_count, pre_other_checksum = row_checksum(con, TARGET_CONTENT_ID)
    pre_levels_count, pre_levels_checksum = table_snapshot(con, "vocabulary_content_levels")
    pre_mfi_count, pre_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    print(f"\nUPDATE 전 스냅샷: vocabulary_contents(제외 1건)={pre_other_count}행 sha256={pre_other_checksum}")
    print(f"UPDATE 전 스냅샷: vocabulary_content_levels={pre_levels_count}행 sha256={pre_levels_checksum}")
    print(f"UPDATE 전 스냅샷: vocabulary_multiformat_items={pre_mfi_count}행 sha256={pre_mfi_checksum}")

    # GATE 3: 하드 가드 - 현재 값이 정확히 기대 구버전 문자열인지 확인
    row = cur.execute(
        "SELECT student_definition FROM vocabulary_contents WHERE content_id=?",
        (TARGET_CONTENT_ID,),
    ).fetchone()
    if row is None:
        print(f"GATE 3 FAIL: content_id={TARGET_CONTENT_ID!r} 행을 찾을 수 없음 - 중단")
        con.close()
        return 2
    current_value = row[0]
    print(f"\nGATE 3: 현재 student_definition={current_value!r}")
    if current_value == NEW_VALUE:
        print("GATE 3: 이미 새 값 - 멱등 재실행으로 판단, 대상 0건, 트랜잭션 없이 종료")
        con.close()
        print("UPDATED=0")
        print("IDEMPOTENT_SKIP=True")
        return 0
    if current_value != OLD_VALUE:
        print(f"GATE 3 FAIL: 현재 값이 기대 구버전 문자열과도, 새 값과도 다름"
              f"(예상 구버전={OLD_VALUE!r}) - 예상 밖 상태이므로 즉시 중단, 아무것도 쓰지 않음")
        con.close()
        return 3
    print("GATE 3 PASS: 현재 값이 정확히 기대 구버전 문자열과 일치")

    # GATE 4: 단일 트랜잭션 UPDATE (student_definition 컬럼만)
    try:
        cur.execute("BEGIN")
        cur.execute(
            "UPDATE vocabulary_contents SET student_definition=? WHERE content_id=? AND student_definition=?",
            (NEW_VALUE, TARGET_CONTENT_ID, OLD_VALUE),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"예상치 못한 UPDATE 영향 행수: {cur.rowcount} (기대값 1)")
        con.commit()
        print(f"\nGATE 4 PASS: 단일 트랜잭션 커밋 완료, UPDATE 1건({TARGET_CONTENT_ID})")
    except Exception as e:
        con.rollback()
        print(f"\nGATE 4 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        return 4

    # GATE 5: 커밋 후 검증
    new_row = cur.execute(
        "SELECT content_id, lemma, pos, canonical_definition, student_definition, "
        "example_sentence, example_target_form, student_exposure, public_ready, is_active "
        "FROM vocabulary_contents WHERE content_id=?",
        (TARGET_CONTENT_ID,),
    ).fetchone()
    print(f"\nGATE 5: 적용 후 행 = {new_row}")
    value_ok = new_row is not None and new_row[4] == NEW_VALUE

    post_other_count, post_other_checksum = row_checksum(con, TARGET_CONTENT_ID)
    other_rows_unchanged = (post_other_count, post_other_checksum) == (pre_other_count, pre_other_checksum)
    print(f"GATE 5: 나머지 {post_other_count}행 체크섬 불변 = {other_rows_unchanged} "
          f"(pre={pre_other_checksum} post={post_other_checksum})")

    post_levels_count, post_levels_checksum = table_snapshot(con, "vocabulary_content_levels")
    levels_unchanged = (post_levels_count, post_levels_checksum) == (pre_levels_count, pre_levels_checksum)
    print(f"GATE 5: vocabulary_content_levels 불변 = {levels_unchanged} "
          f"({pre_levels_count} -> {post_levels_count}행)")

    post_mfi_count, post_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    mfi_unchanged = (post_mfi_count, post_mfi_checksum) == (pre_mfi_count, pre_mfi_checksum)
    print(f"GATE 5: vocabulary_multiformat_items 불변 = {mfi_unchanged} "
          f"({pre_mfi_count} -> {post_mfi_count}행)")

    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 5: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 5: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    exposure_sum, public_ready_sum = cur.execute(
        "SELECT COALESCE(SUM(student_exposure),0), COALESCE(SUM(public_ready),0) FROM vocabulary_contents"
    ).fetchone()
    exposure_still_zero = exposure_sum == 0 and public_ready_sum == 0
    print(f"GATE 5: 전체 student_exposure 합계={exposure_sum}, public_ready 합계={public_ready_sum} "
          f"(둘 다 0이어야 함) = {exposure_still_zero}")

    # 멱등성 재확인: 동일 하드가드 쿼리를 다시 실행하면 대상 0건이어야 함
    recheck = cur.execute(
        "SELECT COUNT(*) FROM vocabulary_contents WHERE content_id=? AND student_definition=?",
        (TARGET_CONTENT_ID, OLD_VALUE),
    ).fetchone()[0]
    idempotent_ready = recheck == 0
    print(f"GATE 5: 재실행 멱등성 사전 확인(구버전 값 남은 행수)={recheck} (0이어야 함) = {idempotent_ready}")

    con.close()

    all_pass = (
        value_ok and other_rows_unchanged and levels_unchanged and mfi_unchanged
        and integrity_ok and fk_ok and exposure_still_zero and idempotent_ready
    )
    print(f"\n최종: {'PASS' if all_pass else 'FAIL'}")
    print(f"UPDATED=1")
    print(f"OLD_VALUE={OLD_VALUE}")
    print(f"NEW_VALUE={NEW_VALUE}")
    print(f"VALUE_OK={value_ok}")
    print(f"OTHER_ROWS_UNCHANGED={other_rows_unchanged}")
    print(f"LEVELS_UNCHANGED={levels_unchanged}")
    print(f"MFI_UNCHANGED={mfi_unchanged}")
    print(f"INTEGRITY_OK={integrity_ok}")
    print(f"FK_OK={fk_ok}")
    print(f"EXPOSURE_STILL_ZERO={exposure_still_zero}")
    print(f"IDEMPOTENT_READY={idempotent_ready}")
    return 0 if all_pass else 5


if __name__ == "__main__":
    sys.exit(main())
