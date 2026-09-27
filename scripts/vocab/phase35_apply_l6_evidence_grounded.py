#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase35 항목3: phase32~35의 근거 기반 집필 최종 DRAFT_READY 48건을
vocabulary_quiz_research.db에 신규 source_version으로 비공개(student_exposure=0,
public_ready=0) 적재한다. phase24/26 apply 스크립트와 같은 게이트 구조를
따르되, SQLite Backup API 백업과 dry-run(기본)/--apply 이중 안전장치를
추가했다.

절대 하지 않는 것: vocabulary_items/vocabulary_multiformat_items(퀴즈 문항)
생성, student_exposure/public_ready를 0이 아닌 값으로 설정, literacy.db
쓰기, git 작업, HOLD 항목 적재.

게이트(하나라도 실패하면 아무것도 쓰지 않고 중단):
  1. APP_ENV=research 재확인(.env + 실행 중 uvicorn 프로세스 /proc/<pid>/environ
     양쪽이 서로 일치 + research)
  2. db-path basename 하드가드(vocabulary_quiz_research.db만 허용)
  3. SQLite Backup API로 사전 백업 생성 + 백업 파일 자체의 integrity_check
  4. 입력 CSV의 content_id가 현재 DB에 전혀 존재하지 않음(dangling 방지,
     이미 있으면 멱등 스킵) 확인
  5. 단일 트랜잭션 삽입(전부 성공 or 전부 롤백) - 기본은 SAVEPOINT 후 항상
     ROLLBACK(dry-run), --apply를 줘야 실제 COMMIT
  6. 커밋 후 integrity_check + FK + 기존 5,902/1,369건 체크섬 불변 + 신규
     행 전부 REVIEW_BOUNDARY·student_exposure/public_ready=0 확인

사용(항상 먼저 dry-run으로 검증):
    APP_ENV=research python3 phase35_apply_l6_evidence_grounded.py \\
        --env-file /home/chsh82/aprolabs/.env \\
        --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \\
        --rows-csv schema_reading_phase35_apply_rows_20260929.csv \\
        --backup-dir /home/chsh82/aprolabs_data/vocabulary_quiz/backups

    (통과 확인 후) 위와 동일 명령에 --apply 추가
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DB_BASENAME = "vocabulary_quiz_research.db"
BATCH_ID = "SCHEMA_READING_PHASE35_L6_EVIDENCE_GROUNDED"
MERGE_SOURCE = "SCHEMA_READING_PHASE35_L6_EVIDENCE_GROUNDED"
SOURCE_VERSION = "schema_reading_l6_evidence_grounded_v1"
GENERATION_METHOD = "MANUAL_STRUCTURED_AUTHORING"
QA_METHOD = "INDEPENDENT_SOURCE_VERIFIED"  # phase32~35: 독립 자료 대조를 거친 초안임을 구분
GENERATION_STATUS = None
LEVEL_VERSION = "level_policy_v0.1"
LEVEL_STATUS = "REVIEW_BOUNDARY"
VOCAB_LEVEL = 6
TARGET_GRADE_BAND = "고등 2~3학년"
LEVEL_SOURCE = "MANUAL_LITERACY_L6_MATCH"
CONTENT_ID_PREFIX = "SR_L6EVIDENCEV1_"


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


def sqlite_backup(src_path: Path, backup_dir: Path, tag: str) -> Path:
    """SQLite Backup API(Connection.backup)로 일관된 백업 생성 - 단순 파일
    복사가 아니라 WAL/저널 상태까지 올바르게 반영하는 공식 API를 쓴다."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst_path = backup_dir / f"{src_path.name}.bak-{tag}-pre-migration-{ts}.db"
    src = sqlite3.connect(str(src_path))
    dst = sqlite3.connect(str(dst_path))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    verify = sqlite3.connect(str(dst_path))
    try:
        result = verify.execute("PRAGMA integrity_check").fetchall()
    finally:
        verify.close()
    ok = len(result) == 1 and result[0][0] == "ok"
    print(f"백업 생성: {dst_path} (integrity_check={'ok' if ok else result})")
    if not ok:
        raise RuntimeError(f"백업 파일 integrity_check 실패: {result}")
    return dst_path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--rows-csv", type=Path, required=True)
    ap.add_argument("--backup-dir", type=Path, required=True)
    ap.add_argument("--apply", action="store_true", help="기본은 dry-run(트랜잭션 후 항상 ROLLBACK). 이 플래그가 있어야 실제 COMMIT.")
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

    backup_path = sqlite_backup(args.db_path, args.backup_dir, "phase35-l6-evidence-grounded")
    print(f"GATE 3 PASS: 백업 생성 및 integrity_check 통과 -> {backup_path}")

    with open(args.rows_csv, encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))
    loadable = [r for r in all_rows if r["final_classification"] == "NEW_PRIVATE_LOADABLE"]
    if len(loadable) != len(all_rows):
        print("GATE 0 FAIL: final_classification 값이 예상 밖(NEW_PRIVATE_LOADABLE 외) - 중단")
        return 1
    print(f"\n입력: {len(loadable)}건 전부 NEW_PRIVATE_LOADABLE")

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

    existing = {cid for (cid,) in cur.execute(
        f"SELECT content_id FROM vocabulary_contents WHERE content_id LIKE '{CONTENT_ID_PREFIX}%'"
    ).fetchall()}
    to_insert = [(cid, r) for cid, r in content_rows if cid not in existing]
    to_skip = [(cid, r) for cid, r in content_rows if cid in existing]
    print(f"\nGATE 4: 이미 존재(멱등 스킵 대상) {len(to_skip)}건, 신규 삽입 대상 {len(to_insert)}건")

    if not to_insert:
        print("신규 삽입 대상 0건 - 멱등 재실행으로 판단, 트랜잭션 없이 종료")
        con.close()
        print("INSERTED=0")
        print(f"SKIPPED={len(to_skip)}")
        return 0

    mode = "APPLY(실제 COMMIT)" if args.apply else "DRY-RUN(항상 ROLLBACK)"
    print(f"\n실행 모드: {mode}")

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
                "subject_category": r["subject_category"],
                "sense_category": r["sense_category"],
                "note_subcategory": r["note_subcategory"],
                "note_week": r["note_week"],
                "is_S_subject_concept": False,
                "loaded_by": "schema_reading_phase35_l6_evidence_grounded",
                "expert_review_required": True,
                "expert_review_status": "DRAFT_NOT_REVIEWED",
                "note": (
                    "REVIEW_BOUNDARY: phase32~35에서 WebSearch/WebFetch로 독립 자료(표준국어대사전/"
                    "공공기관/학술 우선, 위키백과 단독 근거는 자동 배제)를 직접 대조해 뜻 근거를 확인한 "
                    "뒤 작성한 학생용 뜻풀이·예문. 자동 채점 미실시, 전문가(교과 교사) 검수 대기 중. "
                    "grade_level 컬럼이 NULL이라 '고2~3' 근거는 docs/literacy/07-학년경계정책-L5L6.md "
                    "정책(L6=고2~3)에 의존하며 개별 학년 태그는 없음(뜻 근거와 완전히 별도 판단)."
                ),
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

        integrity = cur.execute("PRAGMA integrity_check").fetchall()
        fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
        integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
        fk_ok = len(fk_check) == 0

        new_ids = [cid for cid, _ in to_insert]
        placeholders = ",".join("?" for _ in new_ids)
        cur.execute(
            f"SELECT content_id, student_exposure, public_ready FROM vocabulary_contents WHERE content_id IN ({placeholders})",
            new_ids,
        )
        exposure_rows = cur.fetchall()
        all_private = all(se == 0 and pr == 0 for _, se, pr in exposure_rows) and len(exposure_rows) == len(new_ids)

        cur.execute(
            f"SELECT content_id FROM vocabulary_content_levels WHERE content_id IN ({placeholders}) AND level_status='REVIEW_BOUNDARY'",
            new_ids,
        )
        review_boundary_ok = len(cur.fetchall()) == len(new_ids)

        cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
        total_vc_in_txn = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
        mfi_count = cur.fetchone()[0]

        if args.apply:
            con.commit()
            print(f"\nGATE 5 PASS: 단일 트랜잭션 COMMIT 완료, 삽입 {len(to_insert)}건 (vocabulary_contents + vocabulary_content_levels 각각)")
        else:
            con.rollback()
            print(f"\nGATE 5: DRY-RUN이므로 삽입 {len(to_insert)}건 트랜잭션을 검증 후 전부 ROLLBACK함(실제 반영 없음)")
    except Exception as e:
        con.rollback()
        print(f"\nGATE 5 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        return 2

    print(f"GATE 6: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 6: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")
    print(f"GATE 6: 신규 {len(exposure_rows)}건 student_exposure/public_ready 전부 0 = {all_private}")
    print(f"GATE 6: 신규 {len(new_ids)}건 전부 level_status=REVIEW_BOUNDARY = {review_boundary_ok}")
    print(f"GATE 6: 트랜잭션 내 vocabulary_contents 총 행수(커밋 전 기준) = {total_vc_in_txn} (기존 5902 + 신규 {len(to_insert)} = {5902 + len(to_insert)}여야 함)")
    print(f"GATE 6: vocabulary_multiformat_items 행수(변화 없어야 함) = {mfi_count} (기존 1369여야 함)")

    post_vc_count, post_vc_checksum = table_snapshot(con, "vocabulary_contents", CONTENT_ID_PREFIX)
    vc_unchanged = (post_vc_count, post_vc_checksum) == (pre_vc_count, pre_vc_checksum)
    print(f"GATE 6: 기존 vocabulary_contents(신규 접두어 제외) 체크섬 불변 = {vc_unchanged}")

    post_vcl_count, post_vcl_checksum = table_snapshot(con, "vocabulary_content_levels", CONTENT_ID_PREFIX)
    vcl_unchanged = (post_vcl_count, post_vcl_checksum) == (pre_vcl_count, pre_vcl_checksum)
    print(f"GATE 6: 기존 vocabulary_content_levels(신규 접두어 제외) 체크섬 불변 = {vcl_unchanged}")

    post_mfi_count, post_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    mfi_unchanged = (post_mfi_count, post_mfi_checksum) == (pre_mfi_count, pre_mfi_checksum)
    print(f"GATE 6: vocabulary_multiformat_items 불변 = {mfi_unchanged} ({pre_mfi_count} -> {post_mfi_count}행)")

    con.close()
    ok = integrity_ok and fk_ok and all_private and review_boundary_ok and vc_unchanged and vcl_unchanged and mfi_unchanged \
        and total_vc_in_txn == 5902 + len(to_insert) and mfi_count == 1369
    print(f"\nMODE={'APPLY' if args.apply else 'DRY_RUN'}")
    print(f"INSERTED={len(to_insert) if args.apply else 0}")
    print(f"VALIDATED_WOULD_INSERT={len(to_insert)}")
    print(f"SKIPPED={len(to_skip)}")
    print(f"INTEGRITY_OK={integrity_ok}")
    print(f"FK_OK={fk_ok}")
    print(f"EXPOSURE_ALL_PRIVATE={all_private}")
    print(f"REVIEW_BOUNDARY_OK={review_boundary_ok}")
    print(f"EXISTING_ROWS_UNCHANGED={vc_unchanged and vcl_unchanged and mfi_unchanged}")
    print(f"BACKUP_PATH={backup_path}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
