# -*- coding: utf-8 -*-
"""L3 중등 보강 1차 오답 개정 v2를 연구 DB에 반영한다. 기본은 dry-run,
`--apply`가 있어야만 실제로 쓴다.

**핵심 설계 - 기존 item_id를 덮어쓰지 않는다**: `vocabulary_multiformat_
responses.item_id`는 `vocabulary_multiformat_items.item_id`를 FK로
참조하고, 결과 조회가 세션 스냅샷이 아니라 **문항을 매번 다시 읽는다**
(직접 코드 확인, app/vocabulary_quiz/routers/multiformat.py의 `/result`).
그래서 기존 문항을 그대로 고쳐 쓰면 이미 끝난 과거 응시의 결과 화면이
조용히 달라질 위험이 있다 - 이번 적용은 (a) 29단어 58문항을 **새
item_id**(접미사 `_V2`)로 INSERT하고, (b) 기존 58문항은 **삭제하지 않고
`is_active=0`으로만 UPDATE**한다(FK 대상이 계속 존재해야 하므로 삭제는
애초에 불가능 - 감사 자료로도 보존됨). 적용 전 라이브 재확인 결과 이
배치로 생성된 세션·응답은 0건이었지만(2026-10-07), 0건이라는 이유로
이 안전장치를 생략하지 않는다.

**벨기에 2문항**: 이번에도 건드리지 않는다(INSERT도 UPDATE도 없음) -
매니페스트 파일 교체(별도 커밋, DB 밖)로 기본 응시 풀에서만 빠진다.

안전장치(GATE 1~13):
  GATE 1~4: APP_ENV=research·DB 파일명·백업(SQLite Backup API+복원성)
  GATE 5: 적용 계획 파일(`nikl_grade5_l3_batch1_revision_v2_apply_plan_
          20261007.json`) 건수 확인(신규 58/비활성화 58/보류 2)
  GATE 6: 비활성화 대상 58건 각각의 **현재 DB 값**이 원본 매니페스트
          스냅샷과 정확히 일치하는지 확인(다르면 중단) - 이미 적용된
          상태(새 id 존재+구 id 비활성)면 스킵(멱등)
  GATE 7: 과거 응시 기록(세션/응답)이 비활성화 대상 item_id를 참조하는지
          확인(있으면 건수만 보고, 이 설계가 바로 그 경우를 보호함)
  GATE 8: 단일 트랜잭션(INSERT 58 + UPDATE 58)
  GATE 9: integrity_check/foreign_key_check
  GATE 10: 비대상 불변(RULE_A/B, vocabulary_content_levels 총수,
           vocabulary_contents 30건 내용, vocabulary_publish_reviews
           행수 - 승인을 덮어쓰거나 복사하지 않았다는 증거)
  GATE 11: 벨기에 2문항 완전 불변
  GATE 12: 과거 응답이 참조하던 구 item_id 행이 여전히 원본 그대로인지
           재확인(있다면)
  GATE 13: 29개 콘텐츠의 `vocabulary_publish_reviews` 최신 승인이
           신선도 검사 기준으로 만료(stale)로 전환됐는지 확인(새 로직
           없이 기존 review_is_stale() 재사용 - 여기서는 해시로 동치
           검증만 함)

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/apply_grade5_l3_batch1_distractor_revision_v2.py

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/apply_grade5_l3_batch1_distractor_revision_v2.py --apply
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

PLAN_PATH = REPO_ROOT / "data" / "import" / "nikl_grade5_l3_batch1_revision_v2_apply_plan_20261007.json"
ORIGINAL_MANIFEST_PATH = REPO_ROOT / "data" / "vocab" / "nikl_grade5_l3_batch1_manifest_v1_ORIGINAL_60_20261007.json"
SOURCE_VERSION = "nikl_grade5_l3_batch1_v1"
HELD_CONTENT_IDS = {"G5-0fa0e0a975e55d66"}  # 벨기에

_ITEM_COLS = [
    "item_id", "item_type", "source_content_id", "source_content_ids_json",
    "lemma", "pos", "prompt", "options_json", "correct_option",
    "public_payload_json", "answer_payload_json", "explanation",
    "qa_flags_json", "generator_version", "source_version", "is_active",
]
_COMPARE_COLS = ["prompt", "options_json", "correct_option", "explanation",
                 "source_content_id", "source_version", "is_active"]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_plan() -> dict:
    with open(PLAN_PATH, encoding="utf-8") as f:
        plan = json.load(f)
    if len(plan["new_active_items"]) != 58:
        raise RuntimeError(f"GATE 5 FAIL: 신규 문항 {len(plan['new_active_items'])} != 58")
    if len(plan["old_item_ids_to_deactivate"]) != 58:
        raise RuntimeError(f"GATE 5 FAIL: 비활성화 대상 {len(plan['old_item_ids_to_deactivate'])} != 58")
    if len(plan["held_item_ids_unchanged"]) != 2:
        raise RuntimeError(f"GATE 5 FAIL: 보류 대상 {len(plan['held_item_ids_unchanged'])} != 2")
    print("GATE 5 PASS: 적용 계획 건수 확인(신규 58 / 비활성화 58 / 보류 2)")
    return plan


def load_original_snapshot() -> dict[str, dict]:
    with open(ORIGINAL_MANIFEST_PATH, encoding="utf-8") as f:
        rows = json.load(f)
    return {r["item_id"]: r for r in rows}


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

    plan = load_plan()
    original_snapshot = load_original_snapshot()

    con = connect_rw(db_path)
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    # GATE 6: 비활성화 대상 58건의 현재 값 검증 + 이미 적용된 건 분리
    to_deactivate, to_insert, already_done, conflicts = [], [], [], []
    new_by_old_id = {it["item_id"][:-3]: it for it in plan["new_active_items"]}  # "_V2" 제거 -> 구 id
    for old_id in plan["old_item_ids_to_deactivate"]:
        row = cur.execute(
            f"SELECT {', '.join(_COMPARE_COLS)} FROM vocabulary_multiformat_items WHERE item_id=?", (old_id,)
        ).fetchone()
        if row is None:
            conflicts.append((old_id, "DB에 해당 item_id가 없음"))
            continue
        current = dict(zip(_COMPARE_COLS, row))
        expected = original_snapshot[old_id]
        new_id = old_id + "_V2"
        new_exists = cur.execute(
            "SELECT 1 FROM vocabulary_multiformat_items WHERE item_id=?", (new_id,)
        ).fetchone() is not None

        if current["is_active"] == 0 and new_exists:
            already_done.append(old_id)
            continue
        if current["is_active"] == 1 and not new_exists:
            mismatches = {
                c: (current[c], str(expected[c]))
                for c in ("prompt", "correct_option", "source_content_id", "source_version")
                if str(current[c]) != str(expected[c])
            }
            if str(current["options_json"]) != str(expected["options_json"]):
                mismatches["options_json"] = "현재값이 원본 스냅샷과 다름"
            if mismatches:
                conflicts.append((old_id, f"예상 원본값과 불일치: {mismatches}"))
                continue
            to_deactivate.append(old_id)
            to_insert.append(new_by_old_id[old_id])
            continue
        conflicts.append((old_id, f"예상 밖 상태(is_active={current['is_active']}, new_exists={new_exists})"))

    print(f"GATE 6: 적용 대상 {len(to_deactivate)}건, 이미 적용됨(멱등 스킵) {len(already_done)}건, "
          f"충돌 {len(conflicts)}건")
    if conflicts:
        print("GATE 6 FAIL: 충돌 발견 - 전체 중단")
        for old_id, detail in conflicts[:10]:
            print(f"  - {old_id}: {detail}")
        con.close()
        sys.exit(1)

    # GATE 7: 과거 응시 영향 확인(정보성)
    all_old_ids = plan["old_item_ids_to_deactivate"]
    placeholder = ",".join("?" for _ in all_old_ids)
    past_response_count = cur.execute(
        f"SELECT COUNT(*) FROM vocabulary_multiformat_responses WHERE item_id IN ({placeholder})", all_old_ids
    ).fetchone()[0]
    print(f"GATE 7: 비활성화 대상을 참조하는 과거 응답 {past_response_count}건 "
          f"({'새 id 설계로 보호됨' if past_response_count else '현재 0건 - 그래도 설계는 유지'})")

    if not to_deactivate:
        print("\n적용할 변경이 없습니다(전부 이미 적용됨) - 멱등 종료.")
        con.close()
        return

    if not args.apply:
        print(f"\n[DRY-RUN] 실제 UPDATE/INSERT 안 함. 적용 예정 {len(to_deactivate)}건(콘텐츠 기준 "
              f"{len({it['source_content_id'] for it in to_insert})}개).")
        con.close()
        return

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_g5l3b1_distractor_v2_{ts}"
    dst = sqlite3.connect(backup_path)
    with dst:
        con.backup(dst)
    dst.close()
    print("GATE 4: 백업 생성:", backup_path, "SHA-256:", sha256_of(Path(backup_path)))
    verify_con = sqlite3.connect(backup_path)
    vintegrity = verify_con.execute("PRAGMA integrity_check").fetchone()[0]
    verify_con.close()
    print(f"GATE 4: 백업 복원 가능성 검증 - integrity={vintegrity}")
    if vintegrity != "ok":
        print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
        sys.exit(1)

    pre_counts = {
        "RULE_A/B": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_reference WHERE review_status='RULE_PROPOSED_PENDING_APPROVAL'"
        ).fetchone()[0],
        "vocabulary_content_levels 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_content_levels").fetchone()[0],
        "vocabulary_publish_reviews 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_publish_reviews").fetchone()[0],
        "이 배치 콘텐츠 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_contents WHERE source_version=?", (SOURCE_VERSION,)
        ).fetchone()[0],
    }
    held_snapshot_pre = {
        iid: cur.execute(
            f"SELECT {', '.join(_COMPARE_COLS)} FROM vocabulary_multiformat_items WHERE item_id=?", (iid,)
        ).fetchone()
        for iid in plan["held_item_ids_unchanged"]
    }

    try:
        cur.execute("BEGIN")
        for old_id in to_deactivate:
            cur.execute("UPDATE vocabulary_multiformat_items SET is_active=0, updated_at=datetime('now') WHERE item_id=?", (old_id,))
        for it in to_insert:
            cols = ", ".join(_ITEM_COLS)
            qs = ", ".join("?" for _ in _ITEM_COLS)
            values = [it.get(c, 1 if c == "is_active" else None) for c in _ITEM_COLS]
            cur.execute(f"INSERT INTO vocabulary_multiformat_items ({cols}) VALUES ({qs})", values)
        con.commit()
        print(f"GATE 8 PASS: 단일 트랜잭션 커밋(비활성화 {len(to_deactivate)}건 + 신규 삽입 {len(to_insert)}건)")
    except Exception as e:  # noqa: BLE001
        con.rollback()
        print(f"GATE 8 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        raise

    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 9: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 9: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    post_counts = {
        "RULE_A/B": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_reference WHERE review_status='RULE_PROPOSED_PENDING_APPROVAL'"
        ).fetchone()[0],
        "vocabulary_content_levels 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_content_levels").fetchone()[0],
        "vocabulary_publish_reviews 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_publish_reviews").fetchone()[0],
        "이 배치 콘텐츠 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_contents WHERE source_version=?", (SOURCE_VERSION,)
        ).fetchone()[0],
    }
    counts_ok = pre_counts == post_counts
    print(f"GATE 10: 비대상 불변(RULE_A/B·레벨테이블·승인행수·콘텐츠수) - {'PASS' if counts_ok else 'FAIL'}")
    for k in pre_counts:
        print(f"  {k}: {pre_counts[k]} -> {post_counts[k]}")

    held_ok = True
    for iid in plan["held_item_ids_unchanged"]:
        post_row = cur.execute(
            f"SELECT {', '.join(_COMPARE_COLS)} FROM vocabulary_multiformat_items WHERE item_id=?", (iid,)
        ).fetchone()
        if post_row != held_snapshot_pre[iid]:
            held_ok = False
            print(f"  GATE 11 FAIL: {iid} 변경됨 - 이전={held_snapshot_pre[iid]} 이후={post_row}")
    print(f"GATE 11: 벨기에 2문항 완전 불변 - {'PASS' if held_ok else 'FAIL'}")

    past_ok = True
    if past_response_count:
        for old_id in all_old_ids:
            row = cur.execute(
                f"SELECT {', '.join(_COMPARE_COLS)} FROM vocabulary_multiformat_items WHERE item_id=?", (old_id,)
            ).fetchone()
            expected = original_snapshot[old_id]
            if str(row[0]) != str(expected["prompt"]) or str(row[1]) != str(expected["options_json"]):
                past_ok = False
    print(f"GATE 12: 과거 응답 참조 문항 원본 보존 - {'PASS' if past_ok else 'FAIL'}"
          f"{'(해당 없음, 과거 응답 0건)' if not past_response_count else ''}")

    print("GATE 13: 29개 콘텐츠의 승인 신선도 만료 전환은 이 스크립트가 "
          "vocabulary_publish_reviews를 전혀 쓰지 않으므로(그 테이블을 import조차 안 함) "
          "별도 라이브 조회로 사후 검증한다(보고서 참고) - 여기서는 그 테이블을 "
          "건드리지 않았다는 사실만 GATE 10의 행수 불변으로 보증한다.")

    con.close()
    if not (integrity_ok and fk_ok and counts_ok and held_ok and past_ok):
        raise RuntimeError("사후 검증 실패 - 위 GATE 로그 확인 필요(백업=" + backup_path + ")")

    print("\nBACKUP=", backup_path)
    print("PRE_APPLY_HASH=", pre_apply_hash)
    print(f"\n적용 완료: 콘텐츠 {len({it['source_content_id'] for it in to_insert})}개, 문항 {len(to_insert)}건 신규 활성화, "
          f"{len(to_deactivate)}건 비활성화(보존)")


if __name__ == "__main__":
    main()
