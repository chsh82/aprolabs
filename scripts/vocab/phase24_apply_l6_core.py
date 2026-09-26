#!/usr/bin/env python3
"""Phase24: 50건(V37+S13) NEW_PRIVATE_LOADABLE 항목(L6)을 vocabulary_quiz_research.db에
단일 트랜잭션으로 적재한다(vocabulary_contents 신규 행 + vocabulary_content_levels
신규 행, 같은 트랜잭션). phase14_apply_l5_core.py(원형은 phase13_apply_l4_core.py)를
L6용으로 최소 수정한 스크립트.

절대 하지 않는 것: vocabulary_items/vocabulary_multiformat_items(퀴즈 문항) 생성,
student_exposure/public_ready를 0이 아닌 값으로 설정, literacy.db 쓰기, git 작업.

게이트(하나라도 실패하면 아무것도 쓰지 않고 중단):
  1. APP_ENV=research 재확인 (.env + 실행 중 프로세스 /proc/<pid>/environ)
  2. db-path basename 하드가드
  3. 입력 파일의 content_id가 현재 DB에 전혀 존재하지 않음(dangling 방지) 확인
  4. 단일 트랜잭션 삽입 (executemany 아님, 전부 성공 or 전부 롤백)
  5. 트랜잭션 커밋 후 무결성 검사

멱등성: 이미 존재하는 content_id는 스킵(재실행해도 중복 삽입 없음).

S(교과개념어) 13건은 자동검사 통과 여부와 무관하게 level_reason_json에
expert_review_required=true를 기록해 "전문가 검수 필요" 상태를 구조적으로 표시한다.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

DB_BASENAME = "vocabulary_quiz_research.db"
BATCH_ID = "SCHEMA_READING_PHASE24_L6_CORE_50"
MERGE_SOURCE = "SCHEMA_READING_PHASE24_L6_CORE_50"
SOURCE_VERSION = "schema_reading_literacy_l6_manual_v1"  # 의도적으로 2.1.29(SOURCE_VERSION)와도,
    # phase13(l4_manual_v1)/phase14(l5_manual_v1)와도 다른 새 고유 값 - quiz.py/multiformat.py의
    # 문항 매칭 조건과 절대 겹치지 않게 하는 추가 방어선. L4/L5 배치와도 구분되는 고유 마커.
GENERATION_METHOD = "MANUAL_STRUCTURED_AUTHORING"  # 기존 사용 값 재사용(phase13/14와 동일) - 사람이
    # 직접 literacy.db 정의를 읽고 학생용 정의·예문을 작성했음을 정확히 반영
QA_METHOD = "DETERMINISTIC_PLUS_HEURISTIC"  # 기존 사용 값 재사용 - 결정론적 대조(정의 일치, 레벨
    # 재조회) + 휴리스틱 검토(동형이의어·반의어쌍 스캔, 예문 자연스러움 직접 읽고 판단)를 반영
GENERATION_STATUS = None  # 의도적으로 NULL - PRIVATE_SERVER_READY(_CANDIDATE) 화이트리스트에
    # 들지 않게 하는 추가 방어선(quiz.py ELIGIBLE_STATUSES). vocabulary_items 행이 아예 없어
    # 이 값과 무관하게 이미 출제 불가능하지만, 안전판을 하나 더 둔다.
LEVEL_VERSION = "level_policy_v0.1"  # 기존 유일 버전 재사용(레벨 정책 자체는 동일)
LEVEL_STATUS = "REVIEW_BOUNDARY"  # PROVISIONAL_AUTO/REVIEW_BOUNDARY 중 기존 값만 사용(새 값
    # 발명 금지 지시 준수). REVIEW_BOUNDARY를 택한 근거: 이 50건은 자동 채점 알고리즘을 거치지
    # 않았고, 사람이 이번 세션에서 처음 작성·대조했을 뿐 교과 전문가 검수(S 13건 전부)나 별도
    # QA 배치를 거치지 않았다 - phase13/14와 동일 판단.
VOCAB_LEVEL = 6
TARGET_GRADE_BAND = "고등 2~3학년"  # docs/literacy/07-학년경계정책-L5L6.md(2026-09-24,
    # 사용자 확정): "L5=고1, L6=고2~3". terms.level=6은 이 정책에 따라 고2~3으로 매핑된다
    # (grade_level 컬럼은 이 소스 행들에서 전부 NULL이라 개별 학년 태그는 없음 - 리스크로 기록).
LEVEL_SOURCE = "MANUAL_LITERACY_L6_MATCH"


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
    print("GATE 2 PASS")

    rows = json.load(open(args.rows_json, encoding="utf-8"))
    loadable = [r for r in rows if not r["hold"]]
    held = [r for r in rows if r["hold"]]
    print(f"\n입력: 전체 {len(rows)}건, LOAD 대상 {len(loadable)}건, HOLD {len(held)}건")
    for r in held:
        print(f"  HOLD: {r['lemma']} - {r['hold_reason']}")

    content_rows = []
    for r in loadable:
        content_id = f"SR_L6CORE_{r['literacy_term_id']}"
        content_rows.append((content_id, r))

    con = sqlite3.connect(str(args.db_path))
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    # GATE 3: 멱등성 사전 점검 - 이미 존재하는 content_id는 스킵 대상으로 분류
    existing = set()
    cur.execute("SELECT content_id FROM vocabulary_contents WHERE content_id LIKE 'SR_L6CORE_%'")
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
        return 0

    # GATE 4: 단일 트랜잭션
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
                "literacy_source": r["literacy_source"],
                "phase23_group": r["group"],
                "is_S_subject_concept": r["is_S"],
                "subject_category": r["subject_category"],
                "note_week": r["note_week"],
                "caution": r["caution"],
                "loaded_by": "schema_reading_phase24_l6_core50",
                "expert_review_required": bool(r["is_S"]),
                "expert_review_status": "PENDING" if r["is_S"] else "NOT_APPLICABLE",
                "note": "REVIEW_BOUNDARY: 사람이 직접 literacy.db 정의 대조 후 작성, 자동 채점 미실시. "
                        "S(교과개념어) 13건은 자동검사 통과 여부와 무관하게 전문가 검수 대기 상태다. "
                        "grade_level 컬럼이 NULL이라 '고2~3' 근거는 docs/literacy/07-학년경계정책-L5L6.md "
                        "정책(L6=고2~3)에 의존하며 개별 학년 태그는 없다.",
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

    # GATE 5: 커밋 후 무결성/제약 확인
    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 5: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 5: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    # 신규 행 노출 플래그 확인
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

    con.close()
    print(f"\nINSERTED={len(to_insert)}")
    print(f"SKIPPED={len(to_skip)}")
    print(f"HELD={len(held)}")
    print(f"INTEGRITY_OK={integrity_ok}")
    print(f"FK_OK={fk_ok}")
    print(f"EXPOSURE_ALL_PRIVATE={all_private}")
    return 0 if (integrity_ok and fk_ok and all_private) else 3


if __name__ == "__main__":
    sys.exit(main())
