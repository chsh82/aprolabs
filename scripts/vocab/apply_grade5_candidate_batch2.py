# -*- coding: utf-8 -*-
"""공식 5등급 2차 검토 배치 100건(data/import/nikl_grade5_batch2_candidates_
20261002.csv)을 vocabulary_grade5_candidate_batch에 batch_no=2로 적재한다.
apply_grade5_candidate_batch.py(1차 배치)와 완전히 같은 안전장치 - 기본은
항상 dry-run, candidate_id는 (lemma|pos|homonym_number|source_file_sha256)
결정적 해시, 재적재는 동일값 SKIP·충돌 시 전체 중단(apply_grade5_candidate_
batch.py와 동일 공식/함수 - 복붙이 아니라 같은 로직을 그대로 재사용).

이 스크립트는 2차 배치를 재선정하지 않는다 - scripts/vocab/
nikl_grade5_batch1_analysis.py(batch2_candidates 생성)의 산출물을 그대로
읽어 적재만 한다. Gemini 모델 제안 컬럼(자동_제안_레벨 등)은 이 테이블에
싣지 않는다(별도 vocabulary_grade5_candidate_model_predictions로 분리 -
scripts/vocab/load_grade5_model_predictions.py 참고).

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/apply_grade5_candidate_batch2.py

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구DB경로> \\
        python scripts/vocab/apply_grade5_candidate_batch2.py --apply
"""
from __future__ import annotations

import argparse
import csv
import io
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
from scripts.vocab.apply_grade5_candidate_batch import (  # noqa: E402
    sha256_of, candidate_id_for, _to_bool_int, _INSERT_COLS, _COMPARE_COLS, apply_to_db,
)

BATCH_CSV = REPO_ROOT / "data" / "import" / "nikl_grade5_batch2_candidates_20261002.csv"
XLSX_PATH = REPO_ROOT / "raw" / "nikl_official_vocab" / "국어_기초_어휘_선정_및_어휘_등급화_목록_전체_20231231.xlsx"
SOURCE_REPORT_SEQ = 1160
SOURCE_FILE_SHA256 = "6eec715bca39d1006702da020f5c61a7a1f3db81fdb6101619f68d0b0ff70b53"
BATCH_NO = 2
EXPECTED_TOTAL = 100


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
        lemma = r["lemma"]
        pos = r["pos"]
        hom = r.get("homonym_number") or None
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
            "official_meaning_short": (r.get("official_meaning_short") or None),
            "specialized_domain_flag": _to_bool_int(r.get("specialized_domain_flag", "0")),
            "polysemy_risk_flag": _to_bool_int(r.get("polysemy_risk_signal", "False")),
            "proper_noun_risk_flag": 0,  # 공식 자료 자체에 고유명사 태그가 없음(1차 배치 조사에서 확인됨)
            "selection_reason": "층화표본(2차 배치, seed=20261002, 품사×전문어분야)",
            "official_grade": "5",
            "proposed_level_note": "경계(L3~L4)",
            "source_report_seq": SOURCE_REPORT_SEQ,
            "source_file_sha256": SOURCE_FILE_SHA256,
            "computed_at": computed_at,
            "computed_by_script": "apply_grade5_candidate_batch2.py",
        })

    if len(out) != EXPECTED_TOTAL:
        raise RuntimeError(f"GATE FAIL: 최종 조립 행수 {len(out)} != 기대 {EXPECTED_TOTAL}")
    print(f"GATE 6 PASS: {len(out)}건 조립 완료, candidate_id 전부 고유({len(seen_ids)}개)")
    return out, batch_hash


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
    print("GATE 1 PASS: 가드 통과(APP_ENV=research, VOCABULARY_QUIZ_DB_PATH 실존 확인)")

    if Path(db_path).name != "vocabulary_quiz_research.db":
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
        print(f"GATE 5 FAIL: 공식 xlsx 해시 불일치 - 자료 버전이 바뀌었습니다")
        sys.exit(1)
    print("GATE 5 PASS: 공식 xlsx SHA-256 확인:", xlsx_hash)

    rows, batch_hash = build_rows()
    print("GATE 5: 입력 CSV SHA-256:", batch_hash)

    if args.apply:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = f"{db_path}.bak_grade5_candidate_batch2_{ts}"
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

    # 누적 기대 총수 = 이전 배치(1차, 100건) + 이번 배치(100건) = 200.
    # (추후 3차 배치가 생기면 그 스크립트가 다시 누적값을 계산해 넘긴다.)
    apply_to_db(db_path, rows, dry_run=not args.apply, expected_table_total=BATCH_NO * EXPECTED_TOTAL)
    print("완료(dry-run)" if not args.apply else "완료(실제 적용)")


if __name__ == "__main__":
    main()
