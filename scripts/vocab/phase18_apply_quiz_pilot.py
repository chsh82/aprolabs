#!/usr/bin/env python3
"""Phase18: 스키마리딩x어휘 L4/L5 파일럿 문항 40건(MEANING_CHOICE 20 + CONTEXT_MEANING 20)을
vocabulary_quiz_research.db의 vocabulary_multiformat_items에 단일 트랜잭션으로 적재한다.

phase13_apply_l4_core.py/phase14_apply_l5_core.py와 정확히 같은 게이트 패턴을 재사용한다
(APP_ENV 재확인, db-path 하드가드, dangling 방지, 단일 트랜잭션, 커밋 후 무결성 검사, 멱등성).

절대 하지 않는 것:
  - vocabulary_contents/vocabulary_content_levels 어떤 행도 수정하지 않음(20개
    content_id의 student_exposure/public_ready/level_status 등 전부 그대로).
  - literacy.db 쓰기, git 작업.
  - source_version을 2.1.29(기존 1,289건과 동일 값)로 설정 - 의도적으로
    'schema_reading_l4l5_pilot_dryrun_v1'을 그대로 사용해 app/vocabulary_quiz/routers/
    multiformat.py의 SOURCE_VERSION="2.1.29" 하드코딩 필터(_select_question_items/
    _select_level_candidates 양쪽 다)에 걸리지 않게 한다 - 이 40건은 관리자의 일반
    출제 흐름(혼합 모드/레벨 모드 전부)에서 구조적으로 절대 선택되지 않는다("관리자
    검토용" 상태를 phase13/14의 REVIEW_BOUNDARY와 같은 취지로, 이 테이블에서는
    source_version 불일치로 구현).

게이트(하나라도 실패하면 아무것도 쓰지 않고 중단):
  1. APP_ENV=research 재확인 (.env + 실행 중 프로세스 /proc/<pid>/environ)
  2. db-path basename 하드가드
  3. 입력 40건의 source_content_id 20개가 전부 vocabulary_contents에 존재하고
     is_active=1인지 확인(dangling 방지) + 그 20개의 student_exposure/public_ready가
     여전히 0인지 확인(우리가 건드리지 않았음을 재확인)
  4. item_id 40개 중 이미 존재하는 것은 스킵 대상으로 분류(멱등성 사전 점검)
  5. 단일 트랜잭션 삽입 (전부 성공 or 전부 롤백)
  6. 트랜잭션 커밋 후 무결성 검사 + 유형별 20/20 + 콘텐츠 연결 40/40 + 중복 0 + 고아 0
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

DB_BASENAME = "vocabulary_quiz_research.db"
EXPECTED_SOURCE_VERSION = "schema_reading_l4l5_pilot_dryrun_v1"


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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--rows-json", type=Path, required=True)
    args = ap.parse_args()

    # GATE 1: APP_ENV=research 재확인
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
    print("GATE 2 PASS")

    rows = json.load(open(args.rows_json, encoding="utf-8"))
    print(f"\n입력: {len(rows)}건")
    if len(rows) != 40:
        print(f"GATE 3 FAIL: 입력 건수가 40이 아님({len(rows)}) - 중단")
        return 1
    if any(r["source_version"] != EXPECTED_SOURCE_VERSION for r in rows):
        print(f"GATE 3 FAIL: source_version이 기대값({EXPECTED_SOURCE_VERSION!r})과 다른 행이 있음 - 중단")
        return 1
    item_ids = [r["item_id"] for r in rows]
    if len(set(item_ids)) != len(item_ids):
        print("GATE 3 FAIL: 입력 자체에 item_id 중복이 있음 - 중단")
        return 1

    con = sqlite3.connect(str(args.db_path))
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    # GATE 3: dangling 방지 - source_content_id 20개 전부 존재 + is_active=1 + 노출/공개 0
    content_ids = sorted({r["source_content_id"] for r in rows})
    placeholders = ",".join("?" for _ in content_ids)
    cur.execute(
        f"SELECT content_id, is_active, student_exposure, public_ready FROM vocabulary_contents "
        f"WHERE content_id IN ({placeholders})",
        content_ids,
    )
    found = {row[0]: row[1:] for row in cur.fetchall()}
    missing = [c for c in content_ids if c not in found]
    if missing:
        print(f"GATE 3 FAIL: vocabulary_contents에 없는 content_id {len(missing)}건: {missing}")
        con.close()
        return 1
    ineligible = [c for c, (active, exp, pub) in found.items() if active != 1 or exp != 0 or pub != 0]
    if ineligible:
        print(f"GATE 3 FAIL: is_active!=1 이거나 student_exposure/public_ready!=0 인 content_id {len(ineligible)}건: {ineligible}")
        con.close()
        return 1
    print(f"GATE 3 PASS: content_id {len(content_ids)}개 전부 존재, is_active=1, student_exposure=0, public_ready=0")

    # GATE 4: 멱등성 사전 점검
    item_ph = ",".join("?" for _ in item_ids)
    cur.execute(f"SELECT item_id FROM vocabulary_multiformat_items WHERE item_id IN ({item_ph})", item_ids)
    existing = {r[0] for r in cur.fetchall()}
    to_insert = [r for r in rows if r["item_id"] not in existing]
    to_skip = [r for r in rows if r["item_id"] in existing]
    print(f"\nGATE 4: 이미 존재(스킵 대상) {len(to_skip)}건, 신규 삽입 대상 {len(to_insert)}건")

    if not to_insert:
        print("신규 삽입 대상 0건 - 멱등 재실행으로 판단, 트랜잭션 없이 종료")
        con.close()
        print("INSERTED=0")
        print(f"SKIPPED={len(to_skip)}")
        return 0

    # GATE 5: 단일 트랜잭션
    try:
        cur.execute("BEGIN")
        for r in to_insert:
            cur.execute(
                """INSERT INTO vocabulary_multiformat_items
                   (item_id, item_type, source_content_id, source_content_ids_json, sense_id, sense_ids_json,
                    lemma, pos, prompt, options_json, correct_option, public_payload_json, answer_payload_json,
                    explanation, cognitive_level, qa_flags_json, generator_version, source_version, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    r["item_id"], r["item_type"], r["source_content_id"], r["source_content_ids_json"],
                    r["sense_id"], r["sense_ids_json"], r["lemma"], r["pos"], r["prompt"], r["options_json"],
                    r["correct_option"], r["public_payload_json"], r["answer_payload_json"], r["explanation"],
                    r["cognitive_level"], r["qa_flags_json"], r["generator_version"], r["source_version"],
                ),
            )
        con.commit()
        print(f"\nGATE 5 PASS: 단일 트랜잭션 커밋 완료, 삽입 {len(to_insert)}건")
    except Exception as e:
        con.rollback()
        print(f"\nGATE 5 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        return 2

    # GATE 6: 커밋 후 검증
    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 6: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 6: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    new_ids = [r["item_id"] for r in to_insert]
    ph = ",".join("?" for _ in new_ids)
    cur.execute(
        f"SELECT item_type, COUNT(*) FROM vocabulary_multiformat_items WHERE item_id IN ({ph}) GROUP BY item_type",
        new_ids,
    )
    type_counts = dict(cur.fetchall())
    print(f"GATE 6: 신규 유형별 건수 = {type_counts}")

    cur.execute(
        f"SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE item_id IN ({ph}) "
        f"AND source_content_id IS NOT NULL "
        f"AND source_content_id IN (SELECT content_id FROM vocabulary_contents)",
        new_ids,
    )
    linked = cur.fetchone()[0]
    print(f"GATE 6: 신규 40건 중 유효 content_id에 연결된 건수 = {linked}")

    cur.execute(
        f"SELECT item_id, COUNT(*) c FROM vocabulary_multiformat_items WHERE item_id IN ({ph}) "
        f"GROUP BY item_id HAVING c > 1",
        new_ids,
    )
    dup_after = cur.fetchall()
    print(f"GATE 6: 신규 item_id 중복(UNIQUE 위반이면 애초에 여기 도달 못함) = {len(dup_after)}건")

    con.close()
    print(f"\nINSERTED={len(to_insert)}")
    print(f"SKIPPED={len(to_skip)}")
    print(f"INTEGRITY_OK={integrity_ok}")
    print(f"FK_OK={fk_ok}")
    print(f"TYPE_COUNTS={json.dumps(type_counts, ensure_ascii=False)}")
    print(f"LINKED_40={linked == len(to_insert)}")
    ok = integrity_ok and fk_ok and linked == len(to_insert) and len(dup_after) == 0
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
