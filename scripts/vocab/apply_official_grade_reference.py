# -*- coding: utf-8 -*-
"""국립국어원 공식 어휘 등급(vocabulary_official_grade_reference)을
연구 DB에 적재한다. 기본은 항상 dry-run(쓰기 없음, 계획만 출력) - 실제
적용은 --apply 플래그가 있어야만 한다.

3개 입력 CSV(data/import/nikl_base_level_policy_20260929.csv,
nikl_official_match_full_20260929.csv, nikl_exceptions_priority_tiers_
20260929.csv)를 content_id로 조인해 5,950행을 만든다. 다중후보/매칭없음/
표제어만일치(121건)에는 official_grade/proposed_base_level을 절대
추정해 채우지 않는다(NULL 유지 - 하드 assert로 강제).

안전장치(apply_existing_l0l3_update38_insert42.py와 같은 패턴):
  GATE 1: APP_ENV=research만 허용
  GATE 2: DB 파일명 확인(연구 DB인지)
  GATE 3: 적용 전 DB SHA-256 기록
  GATE 4: SQLite Backup API 백업 + 복원 가능성(integrity_check) 검증
  GATE 5: 입력 CSV 3종 SHA-256 확인(dry-run 때와 동일한 파일인지)
  GATE 6: 행 수·조인 정합성(5,950) 확인, 121건 NULL 하드 assert
  GATE 7: 기존 행과 충돌(다른 값의 동일 content_id) 시 전체 중단 -
          덮어쓰지 않음. 동일 값이면 스킵(멱등 재실행의 근거).
  GATE 8: 단일 트랜잭션 커밋, 실패 시 전체 롤백
  GATE 9: integrity_check/foreign_key_check
  GATE 10: 무관 테이블(콘텐츠/문항 총수) 불변 확인

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/apply_official_grade_reference.py

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구 DB경로> \\
        python scripts/vocab/apply_official_grade_reference.py --apply
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import sqlite3
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

from app.vocabulary_quiz.db import get_db_path  # noqa: E402

POLICY_CSV = REPO_ROOT / "data" / "import" / "nikl_base_level_policy_20260929.csv"
MATCH_CSV = REPO_ROOT / "data" / "import" / "nikl_official_match_full_20260929.csv"
TIERS_CSV = REPO_ROOT / "data" / "import" / "nikl_exceptions_priority_tiers_20260929.csv"

EXPECTED_TOTAL = 5950
EXPECTED_MATCH_TYPE_COUNTS = {
    "단일일치": 5825,
    "단일일치_동형이의주의": 4,
    "다중후보": 7,
    "매칭없음": 101,
    "표제어만일치": 13,
}
NULL_GRADE_MATCH_TYPES = {"다중후보", "매칭없음", "표제어만일치"}

SOURCE_REPORT_SEQ = 1160
SOURCE_FILE_SHA256 = "6eec715bca39d1006702da020f5c61a7a1f3db81fdb6101619f68d0b0ff70b53"
SOURCE_VERSION_LABEL = "2023년 국어 기초 어휘 선정 및 어휘 등급화 연구"
COMPUTED_BY_SCRIPT = "apply_official_grade_reference.py"

_MATCH_TYPE_MAP = {
    "단일일치": "단일일치",
    "단일일치(동형이의있음·등급일치)": "단일일치_동형이의주의",
    "다중후보": "다중후보",
    "매칭없음": "매칭없음",
    "표제어만일치": "표제어만일치",
}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def build_rows() -> tuple[list[dict], dict[str, str]]:
    policy_rows = load_csv(POLICY_CSV)
    match_rows = load_csv(MATCH_CSV)
    tier_rows = load_csv(TIERS_CSV)

    input_hashes = {
        str(POLICY_CSV): sha256_of(POLICY_CSV),
        str(MATCH_CSV): sha256_of(MATCH_CSV),
        str(TIERS_CSV): sha256_of(TIERS_CSV),
    }

    if len(policy_rows) != EXPECTED_TOTAL or len(match_rows) != EXPECTED_TOTAL:
        raise RuntimeError(
            f"GATE 6 FAIL: 입력 행수 불일치 policy={len(policy_rows)} match={len(match_rows)} 기대={EXPECTED_TOTAL}"
        )

    match_by_cid = {r["content_id"]: r for r in match_rows}
    tier_by_cid = {r["content_id"]: r for r in tier_rows}

    policy_cids = {r["content_id"] for r in policy_rows}
    match_cids = set(match_by_cid.keys())
    if policy_cids != match_cids:
        missing_in_match = policy_cids - match_cids
        missing_in_policy = match_cids - policy_cids
        raise RuntimeError(
            f"GATE 6 FAIL: content_id 집합 불일치 - policy에만 {len(missing_in_match)}건, "
            f"match에만 {len(missing_in_policy)}건"
        )
    if not (set(tier_by_cid.keys()) <= policy_cids):
        raise RuntimeError("GATE 6 FAIL: tiers CSV에 policy에 없는 content_id 존재")

    computed_at = datetime.now(timezone.utc).isoformat()

    out_rows: list[dict] = []
    match_type_counter: dict[str, int] = {}
    null_violation = []

    for p in policy_rows:
        cid = p["content_id"]
        m = match_by_cid[cid]
        t = tier_by_cid.get(cid)

        raw_category = m["매칭_카테고리"]
        match_type = _MATCH_TYPE_MAP.get(raw_category)
        if match_type is None:
            raise RuntimeError(f"GATE 6 FAIL: 알 수 없는 매칭_카테고리 값 {raw_category!r} (content_id={cid})")
        match_type_counter[match_type] = match_type_counter.get(match_type, 0) + 1

        raw_official_grade = p["official_grade"] or None
        raw_proposed_base_level = p["proposed_base_level"] or None

        # official_grade는 '1'..'5'인 실제 등급값일 때만 진짜 값으로 취급한다.
        # 다중후보/매칭없음 행은 원본 CSV에 '다중'/'-' 같은 플레이스홀더 문자열이
        # 들어 있을 수 있는데, 이는 "등급을 추정해 채운 것"이 아니라 "값 없음"의
        # 다른 표기이므로 NULL로 정규화한다(진짜 위반은 '1'~'5' 중 하나가 들어간
        # 경우뿐).
        official_grade = raw_official_grade if raw_official_grade in {"1", "2", "3", "4", "5"} else None
        proposed_base_level = raw_proposed_base_level if match_type not in NULL_GRADE_MATCH_TYPES else None

        if match_type in NULL_GRADE_MATCH_TYPES:
            if official_grade is not None:
                null_violation.append((cid, "official_grade", official_grade))
            official_grade = None
            proposed_base_level = None

        homonym_raw = p.get("standard_homonym_number") or ""
        try:
            homonym_num = int(homonym_raw) if homonym_raw != "" else None
        except ValueError:
            homonym_num = None

        current_level_raw = m.get("현재DB_vocab_level") or ""
        try:
            current_level_snap = int(current_level_raw) if current_level_raw != "" else None
        except ValueError:
            current_level_snap = None

        out_rows.append({
            "content_id": cid,
            "official_grade": official_grade,
            "proposed_base_level": proposed_base_level,
            "proposed_base_level_note": p.get("proposed_base_level_note") or None,
            "match_type": match_type,
            "standard_homonym_number": homonym_num,
            "exception_reason": p.get("exception_reason") or None,
            "exception_reason_secondary": p.get("exception_reason_secondary") or None,
            "priority_tier": int(t["priority_tier"]) if (t and t.get("priority_tier")) else None,
            "review_status": p["review_status"],
            "human_approval_status": None,
            "current_vocab_level_snapshot": current_level_snap,
            "current_level_status_snapshot": m.get("현재DB_level_status") or None,
            "level_source": p.get("level_source") or None,
            "source_report_seq": SOURCE_REPORT_SEQ,
            "source_file_sha256": SOURCE_FILE_SHA256,
            "source_version_label": SOURCE_VERSION_LABEL,
            "computed_at": computed_at,
            "computed_by_script": COMPUTED_BY_SCRIPT,
        })

    if null_violation:
        raise RuntimeError(
            f"GATE 6 FAIL: 다중후보/매칭없음/표제어만일치인데 official_grade에 실제 등급값('1'~'5')이 "
            f"채워져 있는 행 발견(추정 금지 위반): {null_violation[:10]}"
        )

    grade_confirmed_missing = [
        r["content_id"] for r in out_rows
        if r["match_type"] not in NULL_GRADE_MATCH_TYPES and r["official_grade"] is None
    ]
    if grade_confirmed_missing:
        raise RuntimeError(
            f"GATE 6 FAIL: 단일일치류인데 official_grade가 비어 있는 행 발견: {grade_confirmed_missing[:10]}"
        )

    for k, expected in EXPECTED_MATCH_TYPE_COUNTS.items():
        actual = match_type_counter.get(k, 0)
        if actual != expected:
            raise RuntimeError(f"GATE 6 FAIL: match_type={k!r} 건수 {actual} != 기대 {expected}")

    if len(out_rows) != EXPECTED_TOTAL:
        raise RuntimeError(f"GATE 6 FAIL: 최종 행수 {len(out_rows)} != 기대 {EXPECTED_TOTAL}")

    print(f"GATE 6 PASS: {len(out_rows)}행 조립 완료, match_type 분포={match_type_counter}, "
          f"121건(다중후보/매칭없음/표제어만일치) official_grade/proposed_base_level 전부 NULL 확인")
    return out_rows, input_hashes


_INSERT_COLS = [
    "content_id", "official_grade", "proposed_base_level", "proposed_base_level_note",
    "match_type", "standard_homonym_number", "exception_reason", "exception_reason_secondary",
    "priority_tier", "review_status", "human_approval_status", "current_vocab_level_snapshot",
    "current_level_status_snapshot", "level_source", "source_report_seq", "source_file_sha256",
    "source_version_label", "computed_at", "computed_by_script",
]

_COMPARE_COLS = [c for c in _INSERT_COLS if c not in ("computed_at",)]  # computed_at은 재실행마다 달라도 무방


def apply_to_db(db_path: str, rows: list[dict], *, dry_run: bool) -> None:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vocabulary_official_grade_reference'")
    if cur.fetchone() is None:
        raise RuntimeError("vocabulary_official_grade_reference 테이블이 없습니다 - "
                            "먼저 migrate_add_official_grade_reference.py를 실행하세요")

    cur.execute(f"SELECT content_id, {', '.join(_COMPARE_COLS)} FROM vocabulary_official_grade_reference")
    existing = {row[0]: dict(zip(_COMPARE_COLS, row[1:])) for row in cur.fetchall()}

    to_insert = []
    to_skip_identical = []
    conflicts = []
    for r in rows:
        cid = r["content_id"]
        if cid in existing:
            cur_row = existing[cid]
            diffs = {c: (cur_row[c], r[c]) for c in _COMPARE_COLS if str(cur_row[c]) != str(r[c] if r[c] is not None else None) and not (cur_row[c] is None and r[c] is None)}
            if diffs:
                conflicts.append((cid, diffs))
            else:
                to_skip_identical.append(cid)
        else:
            to_insert.append(r)

    print(f"GATE 7: 기존 행 {len(existing)}건, 신규 삽입 대상 {len(to_insert)}건, "
          f"동일값 스킵 {len(to_skip_identical)}건, 충돌 {len(conflicts)}건")

    if conflicts:
        print("GATE 7 FAIL: 다른 값의 동일 content_id 발견 - 전체 중단(덮어쓰지 않음)")
        for cid, diffs in conflicts[:10]:
            print(f"  - {cid}: {diffs}")
        con.close()
        raise RuntimeError(f"GATE 7 FAIL: 충돌 {len(conflicts)}건")

    if dry_run:
        print(f"[DRY-RUN] 실제 삽입 안 함. 삽입 예정 {len(to_insert)}건, 스킵(멱등) {len(to_skip_identical)}건.")
        con.close()
        return

    cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
    pre_content_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
    pre_item_total = cur.fetchone()[0]

    placeholders = ", ".join("?" for _ in _INSERT_COLS)
    col_list = ", ".join(_INSERT_COLS)
    try:
        cur.execute("BEGIN")
        for r in to_insert:
            cur.execute(
                f"INSERT INTO vocabulary_official_grade_reference ({col_list}) VALUES ({placeholders})",
                [r[c] for c in _INSERT_COLS],
            )
        con.commit()
        print(f"GATE 8 PASS: 단일 트랜잭션 커밋 완료(신규 삽입 {len(to_insert)}건)")
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

    cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
    post_content_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
    post_item_total = cur.fetchone()[0]
    print(f"GATE 10: 콘텐츠 총수 {pre_content_total} -> {post_content_total} "
          f"({'PASS(불변)' if pre_content_total == post_content_total else 'FAIL'})")
    print(f"GATE 10: 문항 총수 {pre_item_total} -> {post_item_total} "
          f"({'PASS(불변)' if pre_item_total == post_item_total else 'FAIL'})")

    cur.execute("SELECT COUNT(*) FROM vocabulary_official_grade_reference")
    final_total = cur.fetchone()[0]
    print(f"GATE 11: vocabulary_official_grade_reference 총수 = {final_total} (기대 {EXPECTED_TOTAL})")

    con.close()
    if not (integrity_ok and fk_ok and final_total == EXPECTED_TOTAL and pre_content_total == post_content_total
            and pre_item_total == post_item_total):
        raise RuntimeError("사후 검증 실패 - 위 GATE 로그 확인 필요")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 적용(기본은 dry-run)")
    args = parser.parse_args()

    if args.apply:
        app_env = os.environ.get("APP_ENV", "")
        if app_env != "research":
            print(f"GATE 1 FAIL: APP_ENV={app_env!r} - 실제 적용은 research에서만 허용, 중단")
            sys.exit(1)
        print("GATE 1 PASS: APP_ENV=research")

    db_path = str(get_db_path())
    if os.path.basename(db_path) != "vocabulary_quiz_research.db" and args.apply:
        print("GATE 2 FAIL: research DB 아님 - 중단:", db_path)
        sys.exit(1)
    print("GATE 2:", "실제 적용 대상" if args.apply else "[DRY-RUN] 대상", "DB 경로:", db_path)

    if not os.path.exists(db_path):
        print("GATE 2 FAIL: DB 파일 없음:", db_path)
        sys.exit(1)

    pre_apply_hash = sha256_of(Path(db_path))
    print("GATE 3: 적용 전 DB 파일 SHA-256:", pre_apply_hash)

    rows, input_hashes = build_rows()
    print("GATE 5: 입력 CSV SHA-256:")
    for path, h in input_hashes.items():
        print(f"  {path}: {h}")

    backup_path = None
    if args.apply:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = f"{db_path}.bak_official_grade_ref_{ts}"
        src = sqlite3.connect(db_path)
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
        vcur.execute("SELECT COUNT(*) FROM vocabulary_contents")
        vcontents = vcur.fetchone()[0]
        vcur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
        vitems = vcur.fetchone()[0]
        verify_con.close()
        print(f"GATE 4: 백업 복원 가능성 검증 - integrity={vintegrity}, contents={vcontents}, items={vitems}")
        if vintegrity != "ok":
            print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
            sys.exit(1)

    apply_to_db(db_path, rows, dry_run=not args.apply)

    if args.apply:
        print("\nBACKUP=", backup_path)
        print("PRE_APPLY_HASH=", pre_apply_hash)


if __name__ == "__main__":
    main()
