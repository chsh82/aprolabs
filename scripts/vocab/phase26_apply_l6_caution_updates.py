#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase26 항목1: phase25가 발견한 근접 유의어 3쌍(개략/개괄·개관, 총합/총량,
합리적/논리적)의 caution을 이미 적재된 L6 50건의 level_reason_json에 기록한다
(phase22_apply_jeonggi_student_definition_fix.py 패턴 재사용 - 단, 대상 컬럼이
vocabulary_contents.student_definition이 아니라 vocabulary_content_levels.
level_reason_json 안의 "caution" 키라는 점만 다름).

절대 하지 않는 것: student_definition/canonical_definition/lemma 등 다른 컬럼
변경, student_exposure/public_ready 변경, level_reason_json의 caution 외 다른
키 변경, vocabulary_multiformat_items 쓰기, literacy.db 쓰기, git 작업.

게이트(하나라도 실패하면 아무것도 쓰지 않고 중단):
  1. APP_ENV=research 재확인 (.env + 실행 중 uvicorn 프로세스 /proc/<pid>/environ)
  2. db-path basename 하드가드
  3. 하드 가드: 3건 전부 현재 level_reason_json.caution이 정확히 ""(빈 문자열)
     이어야 함 - 하나라도 다르면(이미 누가 바꿨거나 등) 전부 중단, 부분 적용 없음
  4. 단일 트랜잭션 UPDATE (level_reason_json 컬럼만, caution 키만 교체하고
     나머지 키는 원래 순서·값 그대로 유지)
  5. 커밋 후 검증: 3건 새 caution 확인, 나머지 전 행(다른 47건 포함) 체크섬 불변,
     vocabulary_contents/vocabulary_multiformat_items 행수·내용 불변,
     integrity_check/foreign_key_check, exposure/public_ready 합계 여전히 0

멱등성: 재실행 시 GATE 3에서 3건 전부 이미 새 caution이면 "대상 아님"으로
스킵하고 종료(0건).
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

NEW_CAUTIONS = {
    "SR_L6CORE_4973": (
        "기존 콘텐츠 '개괄'(SR_L5CORE_4839, 중요한 내용이나 줄거리를 대강 추려 냄)·"
        "'개관'(SR_L5CORE_4838, 전체를 대강 살펴봄)과 뜻이 매우 근접한 유의어 3종 "
        "클러스터(모두 '대강 요약/개관'류) - 동시 출제 시 구분 문구 권장. "
        "(phase25 재검토로 발견, phase26에서 기록)"
    ),
    "SR_L6CORE_4966": (
        "기존 콘텐츠 '총량'(SC_V19118_B118_002, 전체의 양 또는 무게)과 '전체를 "
        "합한 값' 계열 근접 유의어 - 총합=점수 등의 합계, 총량=물리적 양의 총계로 "
        "용법은 다르나 뜻풀이가 근접해 동시 출제 시 구분 문구 권장. "
        "(phase25 재검토로 발견, phase26에서 기록)"
    ),
    "SR_L6CORE_4972": (
        "기존 콘텐츠 '논리적'(SR_L5CORE_4858, 논리에 맞는 것)과 실제 국어 교육에서도 "
        "자주 혼동/구분 지도되는 근접 유의어 - 동시 출제 시 구분 문구 권장. "
        "(phase25 재검토로 발견, phase26에서 기록)"
    ),
}
LEVEL_VERSION = "level_policy_v0.1"


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


def table_snapshot(con: sqlite3.Connection, table: str, exclude_content_ids: set[str] | None = None) -> tuple[int, str]:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    where = ""
    params: tuple = ()
    if exclude_content_ids and "content_id" in cols:
        qmarks = ",".join("?" * len(exclude_content_ids))
        where = f"WHERE content_id NOT IN ({qmarks})"
        params = tuple(exclude_content_ids)
    rows = con.execute(
        f"SELECT {', '.join(cols)} FROM {table} {where} ORDER BY id", params
    ).fetchall()
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
    print("GATE 1 PASS")

    print(f"GATE 2: db-path basename={args.db_path.name!r}")
    if args.db_path.name != DB_BASENAME or not args.db_path.exists():
        print("GATE 2 FAIL - 중단")
        return 1
    print("GATE 2 PASS")

    con = sqlite3.connect(str(args.db_path))
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    target_ids = set(NEW_CAUTIONS.keys())
    pre_other_vcl_count, pre_other_vcl_checksum = table_snapshot(con, "vocabulary_content_levels", target_ids)
    pre_vc_count, pre_vc_checksum = table_snapshot(con, "vocabulary_contents")
    pre_mfi_count, pre_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    print(f"\nUPDATE 전 스냅샷: vocabulary_content_levels(대상 3건 제외)={pre_other_vcl_count}행 sha256={pre_other_vcl_checksum}")
    print(f"UPDATE 전 스냅샷: vocabulary_contents(전체)={pre_vc_count}행 sha256={pre_vc_checksum}")
    print(f"UPDATE 전 스냅샷: vocabulary_multiformat_items(전체)={pre_mfi_count}행 sha256={pre_mfi_checksum}")

    # GATE 3: 하드 가드 - 3건 전부 현재 caution이 정확히 ""인지 확인
    current_raw = {}
    current_json = {}
    for cid in NEW_CAUTIONS:
        row = cur.execute(
            "SELECT level_reason_json FROM vocabulary_content_levels WHERE content_id=? AND level_version=?",
            (cid, LEVEL_VERSION),
        ).fetchone()
        if row is None:
            print(f"GATE 3 FAIL: content_id={cid!r} 행을 찾을 수 없음 - 중단, 아무것도 쓰지 않음")
            con.close()
            return 2
        current_raw[cid] = row[0]
        current_json[cid] = json.loads(row[0])

    already_updated = [cid for cid, j in current_json.items() if j.get("caution") == NEW_CAUTIONS[cid]]
    still_empty = [cid for cid, j in current_json.items() if j.get("caution") == ""]
    unexpected = [cid for cid in NEW_CAUTIONS if cid not in already_updated and cid not in still_empty]

    print(f"\nGATE 3: 이미 새 값={len(already_updated)}건, 빈 값(대상)={len(still_empty)}건, 예상 밖 값={len(unexpected)}건")
    if unexpected:
        for cid in unexpected:
            print(f"  예상 밖: {cid} caution={current_json[cid].get('caution')!r}")
        print("GATE 3 FAIL: 예상 밖 상태 존재 - 부분 적용 없이 즉시 중단")
        con.close()
        return 3
    if len(already_updated) == len(NEW_CAUTIONS):
        print("GATE 3: 3건 전부 이미 새 값 - 멱등 재실행으로 판단, 대상 0건, 트랜잭션 없이 종료")
        con.close()
        print("UPDATED=0")
        print("IDEMPOTENT_SKIP=True")
        return 0
    if already_updated:
        print("GATE 3 FAIL: 일부만 이미 새 값이고 일부는 빈 값 - 예상 밖 혼재 상태, 부분 적용 위험이 있어 중단")
        con.close()
        return 3
    print("GATE 3 PASS: 3건 전부 현재 caution이 정확히 빈 문자열")

    # GATE 4: 단일 트랜잭션 UPDATE (level_reason_json 컬럼만, caution 키만 교체)
    try:
        cur.execute("BEGIN")
        updated = 0
        for cid, new_caution in NEW_CAUTIONS.items():
            new_dict = dict(current_json[cid])
            new_dict["caution"] = new_caution
            new_json_str = json.dumps(new_dict, ensure_ascii=False)
            cur.execute(
                "UPDATE vocabulary_content_levels SET level_reason_json=? "
                "WHERE content_id=? AND level_version=? AND level_reason_json=?",
                (new_json_str, cid, LEVEL_VERSION, current_raw[cid]),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"예상치 못한 UPDATE 영향 행수: {cid} rowcount={cur.rowcount} (기대값 1)")
            updated += 1
        con.commit()
        print(f"\nGATE 4 PASS: 단일 트랜잭션 커밋 완료, UPDATE {updated}건")
    except Exception as e:
        con.rollback()
        print(f"\nGATE 4 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        return 4

    # GATE 5: 커밋 후 검증
    all_ok = True
    for cid, expected_caution in NEW_CAUTIONS.items():
        row = cur.execute(
            "SELECT level_reason_json FROM vocabulary_content_levels WHERE content_id=? AND level_version=?",
            (cid, LEVEL_VERSION),
        ).fetchone()
        actual = json.loads(row[0])
        ok = actual.get("caution") == expected_caution
        other_keys_ok = all(actual.get(k) == current_json[cid].get(k) for k in current_json[cid] if k != "caution")
        print(f"GATE 5: {cid} caution 갱신 확인={ok}, 다른 키 불변={other_keys_ok}")
        all_ok = all_ok and ok and other_keys_ok

    post_other_vcl_count, post_other_vcl_checksum = table_snapshot(con, "vocabulary_content_levels", target_ids)
    vcl_unchanged = (post_other_vcl_count, post_other_vcl_checksum) == (pre_other_vcl_count, pre_other_vcl_checksum)
    print(f"GATE 5: vocabulary_content_levels(대상 3건 제외) 체크섬 불변 = {vcl_unchanged}")

    post_vc_count, post_vc_checksum = table_snapshot(con, "vocabulary_contents")
    vc_unchanged = (post_vc_count, post_vc_checksum) == (pre_vc_count, pre_vc_checksum)
    print(f"GATE 5: vocabulary_contents 전체 불변 = {vc_unchanged} ({pre_vc_count} -> {post_vc_count}행)")

    post_mfi_count, post_mfi_checksum = table_snapshot(con, "vocabulary_multiformat_items")
    mfi_unchanged = (post_mfi_count, post_mfi_checksum) == (pre_mfi_count, pre_mfi_checksum)
    print(f"GATE 5: vocabulary_multiformat_items 불변 = {mfi_unchanged} ({pre_mfi_count} -> {post_mfi_count}행)")

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
    print(f"GATE 5: 전체 student_exposure/public_ready 합계=({exposure_sum},{public_ready_sum}) = {exposure_still_zero}")

    con.close()
    final_ok = all_ok and vcl_unchanged and vc_unchanged and mfi_unchanged and integrity_ok and fk_ok and exposure_still_zero
    print(f"\n{'[PASS]' if final_ok else '[FAIL]'} 전체 게이트 종합")
    print(f"UPDATED={3 if final_ok else '?'}")
    return 0 if final_ok else 5


if __name__ == "__main__":
    sys.exit(main())
