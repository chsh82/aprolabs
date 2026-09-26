"""사자성어 파서 날짜 정규식 수정(10건) - 서버 literacy.db 대상 재적용.

`apply_sajaseongeo_datefix_10.py`(로컬 phase7이 이미 적용 완료한 스크립트)를
서버 대상으로 거의 그대로 재사용한다. 단 하나 다른 점: 서버에는 `fitz`
(PyMuPDF)와 `raw/sajaseongeo/` 원본 PDF가 없어(확인됨) `merge_sources()`를
서버에서 직접 실행할 수 없다. 따라서 새 definition 값은 로컬에서 이미
`merge_sources()`로 계산 및 로컬 DB 적용 결과와 대조까지 마친 값을
`sajaseongeo_datefix_10_new_defs.json`에 담아 이 스크립트와 함께 전달하고,
이 스크립트는 그 값을 신뢰하는 대신 **서버 DB에 실제로 저장된 값에서 날짜
접두어만 제거하면 정확히 그 값과 일치하는지**를 GATE 0b로 교차 검증한다
(맹목적으로 로컬 값을 주입하지 않음).

날짜 정규식(`_DATE_TOKEN`/`_DATE_LEAD_RE`)은 `sajaseongeo_parser.py`와 동일한
값을 이 파일에 그대로 복제했다(그 파일을 import하면 fitz가 없어 ImportError가
나므로 부득이하게 복제 - 값 자체는 원본과 100% 동일해야 하며, GATE 0b가 실제로
이 복제본으로 서버 원본 텍스트를 정확히 재현하는지 검증하므로 값이 어긋나면
여기서 바로 실패한다).

절차(phase7의 로컬 적용과 동일):
1. 하드 가드: source='sajaseongeo-pdf' AND definition LIKE '%학년도%' 결과가
   정확히 EXPECTED_IDS 10개와 일치해야 함.
2. GATE 0b: 10건 각각 서버 저장 definition에서 날짜 리드를 제거한 결과가
   new_defs.json의 new_definition과 정확히 일치해야 함(로컬 계산값 신뢰
   검증). 하나라도 불일치하면 중단.
3. 새 definition에 '학년도'가 남아있지 않아야 함.
4. SQLite Backup API로 백업 + 무결성(row count/PRAGMA integrity_check/
   foreign_key_check) 검증. 실패 시 중단(DB 미변경).
5. UPDATE 전 sajaseongeo-pdf 전체 체크섬(id|headword|level) + 10건 제외
   나머지 definition explicit 스냅샷.
6. 단일 트랜잭션으로 정확히 10건만 UPDATE(definition 컬럼만).
7. UPDATE 후: 10건 값 재확인, 나머지 explicit definition 불변, 체크섬 불변,
   integrity_check, foreign_key_check.
8. 멱등성: 동일 하드가드 재실행 시 0건.

--apply 없이 실행하면 dry-run(DB 변경 없음).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
NEW_DEFS_PATH = SCRIPT_DIR / "sajaseongeo_datefix_10_new_defs.json"

# sajaseongeo_parser.py의 _DATE_TOKEN/_DATE_LEAD_RE를 그대로 복제(주석 포함) -
# 서버에 fitz가 없어 그 모듈을 import할 수 없기 때문. 값은 원본과 동일해야
# 하고, GATE 0b가 이 복제본으로 실제 서버 텍스트를 정확히 재현하는지 검증한다.
_DATE_TOKEN = (
    r"\d{4}학년도\s*예비\s*시행(?:\s*[A-Z]형(?:\s*,\s*[A-Z]형)*)?"
    r"|\d{4}학년도\s*\d차\s*수능"
    r"|\d{4}\.(?:수능|\d{1,2})(?:\s*[A-Z]형(?:\s*,\s*[A-Z]형)*)?"
)
_DATE_LEAD_RE = re.compile(r"^\s*(" + _DATE_TOKEN + r")\s*(?:/\s*)?")


def strip_date_lead(text: str) -> str:
    """PDF2 파서의 날짜 스트리핑 루프와 동일한 방식으로 선행 날짜 토큰을 전부 제거한다."""
    while True:
        m = _DATE_LEAD_RE.match(text)
        if not m:
            break
        text = text[m.end():]
    return text.strip()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True,
                     help="서버 literacy.db 경로 (예: /home/chsh82/aprolabs/data/literacy.db)")
    ap.add_argument("--backup-dir", type=Path, required=True)
    ap.add_argument("--apply", action="store_true", help="지정하지 않으면 dry-run")
    ap.add_argument("--result-out", type=Path, default=None)
    args = ap.parse_args()

    if not NEW_DEFS_PATH.exists():
        print(f"GATE FAIL: new_defs 파일이 없음: {NEW_DEFS_PATH}")
        return 1
    with open(NEW_DEFS_PATH, encoding="utf-8") as f:
        new_defs_raw = json.load(f)
    new_defs = {int(k): v for k, v in new_defs_raw.items()}
    EXPECTED_IDS = sorted(new_defs.keys())
    if len(EXPECTED_IDS) != 10 or len(set(EXPECTED_IDS)) != 10:
        print(f"GATE FAIL: new_defs.json이 정확히 10건이 아님: {len(EXPECTED_IDS)}건")
        return 1

    db_path = args.db_path
    if not db_path.exists():
        print(f"GATE FAIL: db-path가 존재하지 않음: {db_path}")
        return 1
    if db_path.name != "literacy.db":
        print(f"GATE FAIL: db-path basename이 literacy.db가 아님: {db_path.name}")
        return 1

    print("=== 0. 대상 하드 가드 ===")
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    rows = conn.execute(
        "SELECT id FROM terms WHERE source='sajaseongeo-pdf' AND definition LIKE '%학년도%' ORDER BY id"
    ).fetchall()
    actual_ids = [r[0] for r in rows]
    print("실제 대상 id:", actual_ids)
    print("기대 대상 id:", EXPECTED_IDS)
    if actual_ids != EXPECTED_IDS:
        print("GATE FAIL: 대상 id 목록이 기대값과 다름 - 중단(DB 미변경)")
        conn.close()
        return 1
    print("GATE PASS: 정확히 10건, id 목록 일치")

    print()
    print("=== 0b. 서버 원문 -> 날짜 제거 결과가 로컬 계산값과 정확히 일치하는지 교차 검증 ===")
    old_defs = {}
    all_match = True
    for tid in EXPECTED_IDS:
        row = conn.execute("SELECT headword, definition FROM terms WHERE id=?", (tid,)).fetchone()
        hw, cur_def = row
        old_defs[tid] = cur_def
        expected = new_defs[tid]
        if hw != expected["headword"]:
            print(f"GATE FAIL: id={tid} headword 불일치 server={hw!r} expected={expected['headword']!r}")
            all_match = False
            continue
        stripped = strip_date_lead(cur_def)
        matches = stripped == expected["new_definition"]
        print(f"id={tid} {hw}")
        print(f"  서버 원문(이전): {cur_def!r}")
        print(f"  날짜제거 결과 : {stripped!r}")
        print(f"  로컬 계산값   : {expected['new_definition']!r}")
        print(f"  일치 여부     : {matches}")
        if not matches:
            all_match = False
    conn.close()
    if not all_match:
        print("GATE FAIL: 하나 이상 불일치 - 중단(DB 미변경)")
        return 1
    print("GATE PASS: 10건 전부 서버 원문 날짜제거 결과가 로컬 계산값과 정확히 일치")

    print()
    print("=== 1. 새 definition '학년도' 잔존 여부 확인 ===")
    for tid in EXPECTED_IDS:
        if "학년도" in new_defs[tid]["new_definition"]:
            print(f"GATE FAIL: id={tid} 새 definition에 '학년도'가 남아있음 - 중단")
            return 1
    print("GATE PASS: 10건 모두 새 definition에 '학년도' 없음")

    def checksum_sajaseongeo(c: sqlite3.Connection) -> tuple[str, int]:
        rs = c.execute(
            "SELECT id, headword, level FROM terms WHERE source='sajaseongeo-pdf' ORDER BY id"
        ).fetchall()
        h = hashlib.sha256()
        for r in rs:
            h.update(f"{r[0]}|{r[1]}|{r[2]}".encode("utf-8"))
        return h.hexdigest(), len(rs)

    print()
    print("=== 2. UPDATE 전 체크섬(sajaseongeo-pdf 전체) ===")
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    checksum_before, count_before = checksum_sajaseongeo(conn)
    others_before = conn.execute(
        "SELECT id, definition FROM terms WHERE source='sajaseongeo-pdf' AND id NOT IN (%s)"
        % ",".join(map(str, EXPECTED_IDS))
    ).fetchall()
    conn.close()
    print(f"행수={count_before}, 체크섬={checksum_before}, 10건 제외 나머지={len(others_before)}건")

    if not args.apply:
        print()
        print("=== DRY-RUN 종료 (--apply 없음, DB 변경 없음) ===")
        return 0

    def sha256_file(p: Path) -> str:
        h = hashlib.sha256()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    print()
    print("=== 3. 백업(SQLite Backup API) ===")
    args.backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = args.backup_dir / f"literacy.db.bak-sajaseongeo-datefix-server-pre-{ts}"
    src = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    src.execute("PRAGMA query_only=ON")
    dst = sqlite3.connect(str(backup_path))
    src.backup(dst)
    src.close()
    dst.close()

    orig = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    bak = sqlite3.connect(f"file:{backup_path.as_posix()}?mode=ro", uri=True)
    orig_count = orig.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
    bak_count = bak.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
    integrity = bak.execute("PRAGMA integrity_check").fetchone()[0]
    fk_violations = bak.execute("PRAGMA foreign_key_check").fetchall()
    orig.close()
    bak.close()
    backup_sha256 = sha256_file(backup_path)
    verify = {
        "orig_terms_count": orig_count,
        "backup_terms_count": bak_count,
        "row_count_match": orig_count == bak_count,
        "integrity_check": integrity,
        "fk_violations": len(fk_violations),
        "sha256": backup_sha256,
        "size_bytes": backup_path.stat().st_size,
    }
    print(json.dumps(verify, ensure_ascii=False, indent=2))
    if not verify["row_count_match"] or verify["integrity_check"] != "ok" or verify["fk_violations"] != 0:
        print("GATE FAIL: 백업 검증 실패 - 중단(DB 미변경)")
        return 1
    print("GATE PASS: 백업 무결성 확인")
    print(f"백업 경로: {backup_path}")

    print()
    print("=== 4. UPDATE 적용 (정확히 10건, definition 컬럼만, 단일 트랜잭션) ===")
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    updated = 0
    try:
        for tid in EXPECTED_IDS:
            cur = conn.execute(
                "UPDATE terms SET definition=? WHERE id=? AND source='sajaseongeo-pdf'",
                (new_defs[tid]["new_definition"], tid),
            )
            updated += cur.rowcount
        if updated != 10:
            raise RuntimeError(f"UPDATE된 행수가 10이 아님: {updated} - 커밋하지 않고 롤백")
        conn.commit()
    except Exception as ex:
        conn.rollback()
        conn.close()
        print(f"GATE FAIL: UPDATE 중 예외 발생, 롤백함: {ex}")
        return 1
    conn.close()
    print(f"UPDATE 완료: {updated}건")

    print()
    print("=== 5. UPDATE 후 검증 ===")
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")

    ok_count = 0
    for tid in EXPECTED_IDS:
        row = conn.execute(
            "SELECT definition, level, review_status, reviewed_at, note FROM terms WHERE id=?", (tid,)
        ).fetchone()
        matches = row[0] == new_defs[tid]["new_definition"]
        ok_count += matches
        print(f"id={tid} definition 일치={matches} level={row[1]} review_status={row[2]}")
    print(f"10건 중 값 일치: {ok_count}/10")

    checksum_after, count_after = checksum_sajaseongeo(conn)
    print(f"UPDATE 후 sajaseongeo-pdf 전체 행수: {count_after} (이전 {count_before})")
    print(f"체크섬(전체, id/headword/level): 이전={checksum_before} 이후={checksum_after} 일치={checksum_before==checksum_after}")

    others_after = conn.execute(
        "SELECT id, definition FROM terms WHERE source='sajaseongeo-pdf' AND id NOT IN (%s)"
        % ",".join(map(str, EXPECTED_IDS))
    ).fetchall()
    others_unchanged = others_before == others_after
    print(f"10건 제외 나머지 {len(others_after)}건 definition explicit 비교(백업 전후): 완전 동일={others_unchanged}")

    integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
    fk_after = conn.execute("PRAGMA foreign_key_check").fetchall()
    print(f"PRAGMA integrity_check: {integrity_after}")
    print(f"PRAGMA foreign_key_check 위반: {len(fk_after)}")
    conn.close()

    print()
    print("=== 6. 멱등성(동일 하드가드 재실행) ===")
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    rows2 = conn.execute(
        "SELECT id FROM terms WHERE source='sajaseongeo-pdf' AND definition LIKE '%학년도%'"
    ).fetchall()
    conn.close()
    print(f"재실행 시 대상 건수(기대값 0): {len(rows2)}")

    result = {
        "backup_path": str(backup_path),
        "backup_sha256": backup_sha256,
        "updated": updated,
        "value_match": ok_count,
        "checksum_before": checksum_before,
        "checksum_after": checksum_after,
        "checksum_match": checksum_before == checksum_after,
        "others_unchanged": others_unchanged,
        "others_count": len(others_after),
        "integrity_check": integrity_after,
        "fk_violations": len(fk_after),
        "idempotency_remaining": len(rows2),
        "old_defs": old_defs,
    }
    outpath = args.result_out or (SCRIPT_DIR.parent.parent / "data" / "import" / "sajaseongeo_datefix_10_server_apply_result.json")
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"결과 저장: {outpath}")

    all_pass = (
        ok_count == 10 and checksum_before == checksum_after and others_unchanged
        and integrity_after == "ok" and len(fk_after) == 0 and len(rows2) == 0
    )
    print(f"\n최종 게이트: {'ALL PASS' if all_pass else 'FAIL - 확인 필요'}")
    return 0 if all_pass else 2


if __name__ == "__main__":
    sys.exit(main())
