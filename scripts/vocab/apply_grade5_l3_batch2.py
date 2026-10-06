# -*- coding: utf-8 -*-
"""L3 중등 보강 2차 배치(38콘텐츠·76문항)를 vocabulary_contents·
vocabulary_multiformat_items에 **비공개**로 적재한다. 기본은 dry-run,
`--apply`가 있어야만 실제로 쓴다. 1차(apply_grade5_l3_batch1.py)와 완전히
동일한 GATE 패턴을 그대로 따르되(복제가 아니라 같은 검증 절차를 배치2
입력 파일에 다시 적용), 입력 파일·SOURCE_VERSION·기대 건수만 다르다.

입력: `data/vocab/nikl_grade5_l3_batch2_manifest_v1.json`(76문항, 고정
매니페스트) + `data/import/nikl_grade5_l3_batch2_final_content_20261007.csv`
(38콘텐츠). 둘 다 이번 턴에 직접 고정한 산출물이다.

**비공개 보장**: 삽입하는 모든 콘텐츠 행은 `student_exposure=0,
public_ready=0`로 고정한다(입력 CSV에 이 값이 있어도 무시하고 하드코딩).
`vocabulary_content_levels`에는 아무 행도 쓰지 않는다(1차와 동일 이유 -
레벨 정책 파이프라인을 거친 적 없는 신규 어휘).

안전장치(apply_grade5_l3_batch1.py와 동일한 GATE 패턴):
  GATE 1: APP_ENV=research만 허용
  GATE 2: DB 파일명 확인
  GATE 3: 적용 전 DB SHA-256 기록
  GATE 4: SQLite Backup API 백업 + 복원 가능성 검증
  GATE 5: 입력 파일(매니페스트·콘텐츠 CSV) SHA-256 기록 + 건수 확인(38/76)
  GATE 6: 콘텐츠 38건·문항 76건 조립, content_id 중복 없음 확인
  GATE 7: 기존 행과 충돌(다른 값의 동일 ID) 시 전체 중단 - 동일 값이면 SKIP
  GATE 8: 단일 트랜잭션 커밋
  GATE 9: integrity_check/foreign_key_check
  GATE 10: 무관 테이블(RULE_A/B 건수, 다른 source_version 콘텐츠/문항 수,
           vocabulary_content_levels 총수, **1차 배치(batch1) 콘텐츠·문항
           수**) 불변 확인
  GATE 11: 적재된 38건 전부 student_exposure=0/public_ready=0 확인

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/apply_grade5_l3_batch2.py

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/apply_grade5_l3_batch2.py --apply
"""
from __future__ import annotations

import argparse
import csv
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

CONTENT_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_l3_batch2_final_content_20261007.csv"
MANIFEST_JSON = REPO_ROOT / "data" / "vocab" / "nikl_grade5_l3_batch2_manifest_v1.json"
SOURCE_VERSION = "nikl_grade5_l3_batch2_v1"
BATCH1_SOURCE_VERSION = "nikl_grade5_l3_batch1_v1"
EXPECTED_CONTENT_COUNT = 38
EXPECTED_ITEM_COUNT = 76

_CONTENT_COLS = [
    "content_id", "lemma", "pos", "canonical_definition", "student_definition",
    "example_sentence", "generation_method", "qa_method", "generation_status",
    "student_exposure", "public_ready", "source_version", "is_active",
]
_ITEM_COLS = [
    "item_id", "item_type", "source_content_id", "source_content_ids_json",
    "lemma", "pos", "prompt", "options_json", "correct_option",
    "public_payload_json", "answer_payload_json", "explanation",
    "qa_flags_json", "generator_version", "source_version", "is_active",
]
_CONTENT_COMPARE_COLS = list(_CONTENT_COLS)
_ITEM_COMPARE_COLS = list(_ITEM_COLS)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_rows() -> tuple[list[dict], list[dict]]:
    with open(CONTENT_CSV, encoding="utf-8-sig", newline="") as f:
        content_src = list(csv.DictReader(f))
    with open(MANIFEST_JSON, encoding="utf-8") as f:
        item_src = json.load(f)

    if len(content_src) != EXPECTED_CONTENT_COUNT:
        raise RuntimeError(f"GATE 6 FAIL: 콘텐츠 행수 {len(content_src)} != 기대 {EXPECTED_CONTENT_COUNT}")
    if len(item_src) != EXPECTED_ITEM_COUNT:
        raise RuntimeError(f"GATE 6 FAIL: 문항 행수 {len(item_src)} != 기대 {EXPECTED_ITEM_COUNT}")

    content_rows = []
    for r in content_src:
        content_rows.append({
            "content_id": r["content_id"], "lemma": r["lemma"], "pos": r["pos"],
            "canonical_definition": r["official_meaning_short"],
            "student_definition": r["student_definition"],
            "example_sentence": r["example_sentence"],
            "generation_method": "human_authored_from_official_definition",
            "qa_method": "mechanical_check+author_review(see report)",
            "generation_status": "draft_admin_preview",
            "student_exposure": 0, "public_ready": 0,  # 하드코딩 - 입력값 무시
            "source_version": SOURCE_VERSION, "is_active": 1,
        })
    content_ids = {r["content_id"] for r in content_rows}
    if len(content_ids) != EXPECTED_CONTENT_COUNT:
        raise RuntimeError("GATE 6 FAIL: content_id 중복 발견")

    item_rows = []
    for it in item_src:
        if it["source_content_id"] not in content_ids:
            raise RuntimeError(f"GATE 6 FAIL: 문항이 참조하는 content_id가 콘텐츠 38건에 없음: {it['source_content_id']}")
        item_rows.append({
            "item_id": it["item_id"], "item_type": it["item_type"],
            "source_content_id": it["source_content_id"],
            "source_content_ids_json": it.get("source_content_ids_json"),
            "lemma": it["lemma"], "pos": it["pos"], "prompt": it["prompt"],
            "options_json": it["options_json"], "correct_option": it["correct_option"],
            "public_payload_json": it["public_payload_json"],
            "answer_payload_json": it["answer_payload_json"], "explanation": it["explanation"],
            "qa_flags_json": it["qa_flags_json"], "generator_version": it["generator_version"],
            "source_version": SOURCE_VERSION, "is_active": 1,
        })
    item_ids = {r["item_id"] for r in item_rows}
    if len(item_ids) != EXPECTED_ITEM_COUNT:
        raise RuntimeError("GATE 6 FAIL: item_id 중복 발견")

    print(f"GATE 6 PASS: 콘텐츠 {len(content_rows)}건, 문항 {len(item_rows)}건 조립 완료, "
          f"전부 student_exposure=0/public_ready=0")
    return content_rows, item_rows


def _compare_and_split(cur: sqlite3.Cursor, table: str, key_col: str, rows: list[dict], compare_cols: list[str]):
    cur.execute(f"SELECT {key_col}, {', '.join(compare_cols)} FROM {table}")
    existing = {row[0]: dict(zip(compare_cols, row[1:])) for row in cur.fetchall()}
    to_insert, to_skip, conflicts = [], [], []
    for r in rows:
        key = r[key_col]
        if key in existing:
            cur_row = existing[key]
            diffs = {c: (cur_row[c], r[c]) for c in compare_cols
                     if str(cur_row[c]) != str(r[c]) and not (cur_row[c] is None and r[c] is None)}
            if diffs:
                conflicts.append((key, diffs))
            else:
                to_skip.append(key)
        else:
            to_insert.append(r)
    return to_insert, to_skip, conflicts


def apply_to_db(db_path: str, content_rows: list[dict], item_rows: list[dict], *, dry_run: bool) -> None:
    con = connect_rw(db_path)
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    c_insert, c_skip, c_conflict = _compare_and_split(cur, "vocabulary_contents", "content_id",
                                                        content_rows, _CONTENT_COMPARE_COLS)
    i_insert, i_skip, i_conflict = _compare_and_split(cur, "vocabulary_multiformat_items", "item_id",
                                                        item_rows, _ITEM_COMPARE_COLS)

    print(f"GATE 7: 콘텐츠 - 신규 {len(c_insert)}, 동일값 스킵 {len(c_skip)}, 충돌 {len(c_conflict)}")
    print(f"GATE 7: 문항   - 신규 {len(i_insert)}, 동일값 스킵 {len(i_skip)}, 충돌 {len(i_conflict)}")

    if c_conflict or i_conflict:
        print("GATE 7 FAIL: 다른 값의 동일 ID 발견 - 전체 중단(덮어쓰지 않음)")
        for key, diffs in (c_conflict + i_conflict)[:10]:
            print(f"  - {key}: {diffs}")
        con.close()
        raise RuntimeError(f"GATE 7 FAIL: 충돌 {len(c_conflict) + len(i_conflict)}건")

    if dry_run:
        print(f"[DRY-RUN] 실제 적재 안 함. 콘텐츠 삽입 예정 {len(c_insert)}건, 문항 삽입 예정 {len(i_insert)}건.")
        con.close()
        return

    pre_counts = {
        "RULE_A/B": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_reference WHERE review_status='RULE_PROPOSED_PENDING_APPROVAL'"
        ).fetchone()[0],
        "vocabulary_content_levels 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_content_levels").fetchone()[0],
        "다른 source_version 콘텐츠 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_contents WHERE source_version != ?", (SOURCE_VERSION,)
        ).fetchone()[0],
        "다른 source_version 문항 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version != ?", (SOURCE_VERSION,)
        ).fetchone()[0],
        "1차 배치(batch1) 콘텐츠 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_contents WHERE source_version = ?", (BATCH1_SOURCE_VERSION,)
        ).fetchone()[0],
        "1차 배치(batch1) 문항 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version = ?", (BATCH1_SOURCE_VERSION,)
        ).fetchone()[0],
    }

    try:
        cur.execute("BEGIN")
        for r in c_insert:
            cols = ", ".join(_CONTENT_COLS)
            qs = ", ".join("?" for _ in _CONTENT_COLS)
            cur.execute(f"INSERT INTO vocabulary_contents ({cols}) VALUES ({qs})", [r[c] for c in _CONTENT_COLS])
        for r in i_insert:
            cols = ", ".join(_ITEM_COLS)
            qs = ", ".join("?" for _ in _ITEM_COLS)
            cur.execute(f"INSERT INTO vocabulary_multiformat_items ({cols}) VALUES ({qs})", [r[c] for c in _ITEM_COLS])
        con.commit()
        print(f"GATE 8 PASS: 단일 트랜잭션 커밋 완료(콘텐츠 {len(c_insert)}건, 문항 {len(i_insert)}건)")
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
        "다른 source_version 콘텐츠 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_contents WHERE source_version != ?", (SOURCE_VERSION,)
        ).fetchone()[0],
        "다른 source_version 문항 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version != ?", (SOURCE_VERSION,)
        ).fetchone()[0],
        "1차 배치(batch1) 콘텐츠 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_contents WHERE source_version = ?", (BATCH1_SOURCE_VERSION,)
        ).fetchone()[0],
        "1차 배치(batch1) 문항 수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version = ?", (BATCH1_SOURCE_VERSION,)
        ).fetchone()[0],
    }
    counts_ok = pre_counts == post_counts
    print(f"GATE 10: 무관 테이블·1차 배치 불변 - {'PASS' if counts_ok else 'FAIL'}")
    for k in pre_counts:
        print(f"  {k}: {pre_counts[k]} -> {post_counts[k]}")

    exposure_rows = cur.execute(
        "SELECT content_id, student_exposure, public_ready FROM vocabulary_contents WHERE source_version = ?",
        (SOURCE_VERSION,),
    ).fetchall()
    exposure_ok = len(exposure_rows) == EXPECTED_CONTENT_COUNT and all(se == 0 and pr == 0 for _, se, pr in exposure_rows)
    print(f"GATE 11: 적재 {len(exposure_rows)}건 전부 student_exposure=0/public_ready=0 - {'PASS' if exposure_ok else 'FAIL'}")

    con.close()
    if not (integrity_ok and fk_ok and counts_ok and exposure_ok):
        raise RuntimeError("사후 검증 실패 - 위 GATE 로그 확인 필요")


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

    content_rows, item_rows = build_rows()
    print("GATE 5: 입력 파일 SHA-256:")
    print(f"  {CONTENT_CSV}: {sha256_of(CONTENT_CSV)}")
    print(f"  {MANIFEST_JSON}: {sha256_of(MANIFEST_JSON)}")

    backup_path = None
    if args.apply:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = f"{db_path}.bak_grade5_l3_batch2_{ts}"
        src = connect_rw(db_path)
        dst = sqlite3.connect(backup_path)
        with dst:
            src.backup(dst)
        src.close()
        dst.close()
        backup_hash = sha256_of(Path(backup_path))
        print("GATE 4: 백업 생성:", backup_path, "SHA-256:", backup_hash)

        verify_con = sqlite3.connect(backup_path)
        vcur = verify_con.cursor()
        vintegrity = vcur.execute("PRAGMA integrity_check").fetchone()[0]
        vcontents = vcur.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        verify_con.close()
        print(f"GATE 4: 백업 복원 가능성 검증 - integrity={vintegrity}, contents={vcontents}")
        if vintegrity != "ok":
            print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
            sys.exit(1)

    apply_to_db(db_path, content_rows, item_rows, dry_run=not args.apply)

    if args.apply:
        print("\nBACKUP=", backup_path)
        print("PRE_APPLY_HASH=", pre_apply_hash)


if __name__ == "__main__":
    main()
