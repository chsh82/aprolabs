# -*- coding: utf-8 -*-
"""공식 5등급 1차 검토 배치 100건(data/import/nikl_grade5_first_batch_100_
ENRICHED_20261001.csv)을 vocabulary_grade5_candidate_batch에 적재한다.
기본은 항상 dry-run(쓰기 없음) - 실제 적용은 --apply가 있어야만 한다.

이 스크립트는 100건을 다시 선정하거나 재조사하지 않는다 - 이미 확정된
CSV를 그대로 읽어 적재만 한다(선정 로직은 scripts/vocab/
nikl_grade5_candidate_list.py, 이 스크립트는 그 산출물의 소비자일 뿐).

candidate_id 파생(사용자 지시: "공식 원천 식별자와 자료 버전을 기반으로
안정적으로"): sha256(lemma|pos|homonym_number|source_file_sha256)의 앞
16자리를 "G5-" 접두사와 결합한다 - 순수 (표제어,품사,동형번호,자료버전)의
함수라 행 순서에 의존하지 않고, 같은 CSV를 재적재하면 항상 같은 ID가
나온다. 공식 자료 버전이 실제로 바뀌면(source_file_sha256이 달라지면)
같은 단어라도 다른 ID가 생겨, 버전 드리프트가 조용히 충돌 대신 "새 ID"로
드러난다(의도한 설계).

재적재 멱등성(사용자 지시대로 apply_official_grade_reference.py와 동일한
compare-and-swap): 같은 candidate_id가 이미 있고 값이 완전히 동일하면
SKIP, 하나라도 다르면 그 즉시 전체 트랜잭션을 중단한다(덮어쓰지 않음).

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/apply_grade5_candidate_batch.py

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/apply_grade5_candidate_batch.py --apply
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
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

from scripts.vocab.db_path_guard import DbPathGuardError, connect_rw, guard_db_path  # noqa: E402

BATCH_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_first_batch_100_ENRICHED_20261001.csv"
XLSX_PATH = REPO_ROOT / "raw" / "nikl_official_vocab" / "국어_기초_어휘_선정_및_어휘_등급화_목록_전체_20231231.xlsx"
SOURCE_REPORT_SEQ = 1160
SOURCE_FILE_SHA256 = "6eec715bca39d1006702da020f5c61a7a1f3db81fdb6101619f68d0b0ff70b53"
BATCH_NO = 1
EXPECTED_TOTAL = 100


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def candidate_id_for(lemma: str, pos: str, homonym_number: str | int | None) -> str:
    hom = str(homonym_number) if homonym_number not in (None, "") else "0"
    blob = f"{lemma}|{pos}|{hom}|{SOURCE_FILE_SHA256}"
    return "G5-" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _to_bool_int(v: str) -> int:
    return 1 if str(v).strip().lower() in ("true", "1", "yes") else 0


_INSERT_COLS = [
    "candidate_id", "batch_no", "lemma", "pos", "homonym_number", "official_meaning_short",
    "specialized_domain_flag", "polysemy_risk_flag", "proper_noun_risk_flag", "selection_reason",
    "official_grade", "proposed_level_note", "source_report_seq", "source_file_sha256",
    "computed_at", "computed_by_script",
]
_COMPARE_COLS = [c for c in _INSERT_COLS if c not in ("computed_at",)]


def build_rows() -> tuple[list[dict], str]:
    if not BATCH_CSV.is_file():
        raise RuntimeError(f"입력 파일 없음: {BATCH_CSV}")
    batch_hash = sha256_of(BATCH_CSV)

    with open(BATCH_CSV, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if len(rows) != EXPECTED_TOTAL:
        raise RuntimeError(f"GATE FAIL: 입력 행수 {len(rows)} != 기대 {EXPECTED_TOTAL}")

    computed_at = datetime.now(timezone.utc).isoformat()
    out = []
    seen_ids = set()
    for r in rows:
        lemma = r["표제어"]
        pos = r["품사"]
        hom = r.get("동형번호") or None
        cid = candidate_id_for(lemma, pos, hom)
        if cid in seen_ids:
            raise RuntimeError(f"GATE FAIL: candidate_id 충돌(배치 내부 중복) - {cid} ({lemma}/{pos}/{hom})")
        seen_ids.add(cid)
        out.append({
            "candidate_id": cid,
            "batch_no": BATCH_NO,
            "lemma": lemma,
            "pos": pos,
            "homonym_number": int(hom) if hom not in (None, "", "0") else (0 if hom == "0" else None),
            "official_meaning_short": (r.get("의미") or None),
            "specialized_domain_flag": _to_bool_int(r.get("전문용어_위험", "False")),
            "polysemy_risk_flag": _to_bool_int(r.get("다의어_위험", "False")),
            "proper_noun_risk_flag": _to_bool_int(r.get("고유명사_위험", "False")),
            "selection_reason": r.get("선정_사유") or None,
            "official_grade": r.get("공식등급") or "5",
            "proposed_level_note": r.get("제안레벨") or "경계(L3~L4)",
            "source_report_seq": SOURCE_REPORT_SEQ,
            "source_file_sha256": SOURCE_FILE_SHA256,
            "computed_at": computed_at,
            "computed_by_script": "apply_grade5_candidate_batch.py",
        })

    if len(out) != EXPECTED_TOTAL:
        raise RuntimeError(f"GATE FAIL: 최종 조립 행수 {len(out)} != 기대 {EXPECTED_TOTAL}")
    print(f"GATE 6 PASS: {len(out)}건 조립 완료, candidate_id 전부 고유({len(seen_ids)}개)")
    return out, batch_hash


def apply_to_db(db_path: str, rows: list[dict], *, dry_run: bool,
                 expected_table_total: int | None = None) -> None:
    """expected_table_total: 적용 후 vocabulary_grade5_candidate_batch 전체
    행 수의 기댓값(누적, 여러 배치 합산). 생략하면 이 모듈의 EXPECTED_TOTAL
    (단일 배치 전용 값)을 그대로 쓴다 - 1차 배치(batch1)는 테이블 전체가
    그 배치뿐이라 기존 동작과 동일하다. 2차 이후 배치(apply_grade5_
    candidate_batch2.py 등)는 누적 총수(예: 200)를 명시로 넘긴다."""
    if expected_table_total is None:
        expected_table_total = EXPECTED_TOTAL
    con = connect_rw(db_path)
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='vocabulary_grade5_candidate_batch'"
    )
    if cur.fetchone() is None:
        raise RuntimeError("vocabulary_grade5_candidate_batch 테이블이 없습니다 - "
                            "먼저 migrate_add_grade5_candidate_batch.py를 실행하세요")

    cur.execute(f"SELECT candidate_id, {', '.join(_COMPARE_COLS)} FROM vocabulary_grade5_candidate_batch")
    existing = {row[0]: dict(zip(_COMPARE_COLS, row[1:])) for row in cur.fetchall()}

    to_insert, to_skip_identical, conflicts = [], [], []
    for r in rows:
        cid = r["candidate_id"]
        if cid in existing:
            cur_row = existing[cid]
            diffs = {
                c: (cur_row[c], r[c]) for c in _COMPARE_COLS
                if str(cur_row[c]) != str(r[c] if r[c] is not None else None)
                and not (cur_row[c] is None and r[c] is None)
            }
            (conflicts if diffs else to_skip_identical).append((cid, diffs) if diffs else cid)
        else:
            to_insert.append(r)

    print(f"GATE 7: 기존 행 {len(existing)}건, 신규 삽입 대상 {len(to_insert)}건, "
          f"동일값 스킵 {len(to_skip_identical)}건, 충돌 {len(conflicts)}건")

    if conflicts:
        print("GATE 7 FAIL: 같은 candidate_id인데 값이 다른 행 발견 - 전체 중단(덮어쓰지 않음)")
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
    cur.execute("SELECT COUNT(*) FROM vocabulary_official_grade_reference")
    pre_ref_total = cur.fetchone()[0]

    placeholders = ", ".join("?" for _ in _INSERT_COLS)
    col_list = ", ".join(_INSERT_COLS)
    try:
        cur.execute("BEGIN")
        for r in to_insert:
            cur.execute(
                f"INSERT INTO vocabulary_grade5_candidate_batch ({col_list}) VALUES ({placeholders})",
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
    cur.execute("SELECT COUNT(*) FROM vocabulary_official_grade_reference")
    post_ref_total = cur.fetchone()[0]
    print(f"GATE 10: 콘텐츠 {pre_content_total}->{post_content_total} "
          f"({'PASS(불변)' if pre_content_total == post_content_total else 'FAIL'})")
    print(f"GATE 10: 문항 {pre_item_total}->{post_item_total} "
          f"({'PASS(불변)' if pre_item_total == post_item_total else 'FAIL'})")
    print(f"GATE 10: 공식등급참조테이블 {pre_ref_total}->{post_ref_total} "
          f"({'PASS(불변)' if pre_ref_total == post_ref_total else 'FAIL'})")

    cur.execute("SELECT COUNT(*) FROM vocabulary_grade5_candidate_batch")
    final_total = cur.fetchone()[0]
    print(f"GATE 11: vocabulary_grade5_candidate_batch 총수 = {final_total} (기대 {expected_table_total})")

    con.close()
    if not (integrity_ok and fk_ok and final_total == expected_table_total
            and pre_content_total == post_content_total and pre_item_total == post_item_total
            and pre_ref_total == post_ref_total):
        raise RuntimeError("사후 검증 실패 - 위 GATE 로그 확인 필요")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 적용(기본은 dry-run)")
    parser.add_argument("--database", default=None, help="VOCABULARY_QUIZ_DB_PATH와 반드시 일치해야 함(교차 확인용)")
    args = parser.parse_args()

    try:
        db_path = guard_db_path(args.database)
    except DbPathGuardError as e:
        print(f"GATE 1 FAIL: {e}")
        sys.exit(1)
    print("GATE 1 PASS: 가드 통과(APP_ENV=research, VOCABULARY_QUIZ_DB_PATH 실존 확인)")

    if os.path.basename(db_path) != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: research DB 파일명이 아님 - 중단:", db_path)
        sys.exit(1)
    print("GATE 2 PASS:", "실제 적용 대상" if args.apply else "[DRY-RUN] 대상", "DB 경로:", db_path)

    pre_apply_hash = sha256_of(Path(db_path))
    print("GATE 3: 적용 전 DB 파일 SHA-256:", pre_apply_hash)

    if not XLSX_PATH.is_file():
        print("GATE 5 FAIL: 공식 xlsx 원본을 찾을 수 없음:", XLSX_PATH)
        sys.exit(1)
    xlsx_hash = sha256_of(XLSX_PATH)
    if xlsx_hash != SOURCE_FILE_SHA256:
        print(f"GATE 5 FAIL: 공식 xlsx 해시 불일치(기대 {SOURCE_FILE_SHA256}, 실제 {xlsx_hash}) - 자료 버전이 바뀌었습니다")
        sys.exit(1)
    print("GATE 5 PASS: 공식 xlsx SHA-256 확인:", xlsx_hash)

    rows, batch_hash = build_rows()
    print("GATE 5: 입력 CSV SHA-256:", batch_hash)

    backup_path = None
    if args.apply:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = f"{db_path}.bak_grade5_candidate_batch_{ts}"
        src = connect_rw(db_path)
        import sqlite3 as _sqlite3
        dst = _sqlite3.connect(backup_path)
        with dst:
            src.backup(dst)
        src.close()
        dst.close()
        backup_hash = sha256_of(Path(backup_path))
        print("GATE 4: SQLite Backup API 백업 생성:", backup_path, "SHA-256:", backup_hash)

        verify = _sqlite3.connect(backup_path)
        integrity = verify.execute("PRAGMA integrity_check").fetchone()[0]
        c_count = verify.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
        i_count = verify.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        verify.close()
        print(f"GATE 4: 백업 복원 가능성 확인 - integrity_check={integrity}, contents={c_count}, items={i_count}")
        if integrity != "ok":
            print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
            sys.exit(1)

    apply_to_db(db_path, rows, dry_run=not args.apply)
    print("완료(dry-run)" if not args.apply else "완료(실제 적용)")


if __name__ == "__main__":
    main()
