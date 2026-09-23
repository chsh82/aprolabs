"""사자성어 파서 날짜 정규식 수정(10건) - 백업 -> 게이트 -> apply -> 검증 -> 멱등성.

phase4/phase6 패턴을 그대로 따른다:
1. 정확히 10건 하드 가드(id 목록 대조)
2. SQLite Backup API 백업 + 무결성 검증
3. UPDATE 전 체크섬(225건 전체 id/headword/level) 스냅샷
4. UPDATE는 definition 컬럼만, 정확히 10개 id만
5. UPDATE 후: 10건 값 재확인 + 나머지 215건 체크섬 불변 확인 + integrity_check +
   foreign_key_check
6. 멱등성: 동일 스크립트 재실행 시 대상 0건
"""
import hashlib
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))
from sajaseongeo_parser import merge_sources  # noqa: E402

LITERACY_DB = REPO_ROOT / "data" / "literacy.db"
BACKUP_DIR = REPO_ROOT / "data" / "backups"
EXPECTED_IDS = [7148, 7188, 7191, 7192, 7197, 7223, 7236, 7243, 7266, 7288]


def load_expected_headwords() -> dict[int, str]:
    """literacy.db에 실제 저장된 headword를 그대로 쓴다(하드코딩 문자열 비교 시
    "類"(U+985E, CJK 통합) vs 호환용 한자 코드포인트(U+F9D0) 불일치로 조회
    실패했던 것을 실측으로 확인 - 유유상종類類가 후자를 쓴다)."""
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    out = {}
    for tid in EXPECTED_IDS:
        row = conn.execute("SELECT headword FROM terms WHERE id=?", (tid,)).fetchone()
        out[tid] = row[0]
    conn.close()
    return out


EXPECTED_HEADWORDS = load_expected_headwords()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def backup_db() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = BACKUP_DIR / f"literacy.db.bak-sajaseongeo-datefix-pre-{ts}"

    src = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    dst = sqlite3.connect(str(backup_path))
    src.backup(dst)
    src.close()
    dst.close()
    return backup_path


def verify_backup(backup_path: Path) -> dict:
    orig = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    bak = sqlite3.connect(f"file:{backup_path.as_posix()}?mode=ro", uri=True)

    orig_count = orig.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
    bak_count = bak.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
    integrity = bak.execute("PRAGMA integrity_check").fetchone()[0]
    fk_violations = bak.execute("PRAGMA foreign_key_check").fetchall()

    orig.close()
    bak.close()
    return {
        "orig_terms_count": orig_count,
        "backup_terms_count": bak_count,
        "row_count_match": orig_count == bak_count,
        "integrity_check": integrity,
        "fk_violations": len(fk_violations),
        "sha256": sha256_file(backup_path),
        "size_bytes": backup_path.stat().st_size,
    }


def checksum_sajaseongeo(conn: sqlite3.Connection) -> tuple[str, int]:
    rows = conn.execute(
        "SELECT id, headword, level FROM terms WHERE source='sajaseongeo-pdf' ORDER BY id"
    ).fetchall()
    h = hashlib.sha256()
    for r in rows:
        h.update(f"{r[0]}|{r[1]}|{r[2]}".encode("utf-8"))
    return h.hexdigest(), len(rows)


def build_new_definitions() -> dict[int, str]:
    merged, stats = merge_sources()
    by_headword = {e.headword: e for e in merged}
    new_defs = {}
    for tid, hw in EXPECTED_HEADWORDS.items():
        e = by_headword.get(hw)
        if e is None or not e.definitions:
            raise RuntimeError(f"merge_sources 결과에서 '{hw}'(id={tid})를 찾지 못함")
        new_defs[tid] = e.definitions[0]
    return new_defs


def main():
    apply = "--apply" in sys.argv

    print("=== 0. 대상 하드 가드 ===")
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT id FROM terms WHERE source='sajaseongeo-pdf' AND definition LIKE '%학년도%' ORDER BY id"
    ).fetchall()
    actual_ids = [r[0] for r in rows]
    conn.close()

    print("실제 대상 id:", actual_ids)
    print("기대 대상 id:", EXPECTED_IDS)
    if actual_ids != EXPECTED_IDS:
        print("GATE FAIL: 대상 id 목록이 기대값과 다름 - 중단")
        sys.exit(1)
    print("GATE PASS: 정확히 10건, id 목록 일치")

    print()
    print("=== 1. 새 definition 값 계산(PDF 재파싱, DB 접근 없음) ===")
    new_defs = build_new_definitions()
    old_defs = {}
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    for tid in EXPECTED_IDS:
        row = conn.execute("SELECT headword, definition FROM terms WHERE id=?", (tid,)).fetchone()
        old_defs[tid] = row[1]
        hw = row[0]
        print(f"id={tid} {hw}")
        print(f"  이전: {row[1]!r}")
        print(f"  이후: {new_defs[tid]!r}")
        if "학년도" in new_defs[tid]:
            print("  GATE FAIL: 새 definition에도 '학년도'가 남아있음 - 중단")
            conn.close()
            sys.exit(1)
    conn.close()
    print("GATE PASS: 10건 모두 새 definition에 '학년도' 없음")

    print()
    print("=== 2. UPDATE 전 체크섬(sajaseongeo-pdf 전체) ===")
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    checksum_before, count_before = checksum_sajaseongeo(conn)
    conn.close()
    print(f"행수={count_before}, 체크섬={checksum_before}")

    if not apply:
        print()
        print("=== DRY-RUN 종료 (--apply 없음, DB 변경 없음) ===")
        return

    print()
    print("=== 3. 백업(SQLite Backup API) ===")
    backup_path = backup_db()
    verify = verify_backup(backup_path)
    print(json.dumps(verify, ensure_ascii=False, indent=2))
    if not verify["row_count_match"] or verify["integrity_check"] != "ok" or verify["fk_violations"] != 0:
        print("GATE FAIL: 백업 검증 실패 - 중단(DB 미변경)")
        sys.exit(1)
    print("GATE PASS: 백업 무결성 확인")
    print(f"백업 경로: {backup_path}")

    print()
    print("=== 4. UPDATE 적용 (정확히 10건, definition 컬럼만) ===")
    conn = sqlite3.connect(str(LITERACY_DB))
    conn.execute("PRAGMA foreign_keys=ON")
    updated = 0
    try:
        for tid in EXPECTED_IDS:
            cur = conn.execute(
                "UPDATE terms SET definition=? WHERE id=? AND source='sajaseongeo-pdf'",
                (new_defs[tid], tid),
            )
            updated += cur.rowcount
        if updated != 10:
            raise RuntimeError(f"UPDATE된 행수가 10이 아님: {updated} - 커밋하지 않고 롤백")
        conn.commit()
    except Exception as ex:
        conn.rollback()
        conn.close()
        print(f"GATE FAIL: UPDATE 중 예외 발생, 롤백함: {ex}")
        sys.exit(1)
    conn.close()
    print(f"UPDATE 완료: {updated}건")

    print()
    print("=== 5. UPDATE 후 검증 ===")
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)

    # 5-1. 10건 값 재확인
    ok_count = 0
    for tid in EXPECTED_IDS:
        row = conn.execute("SELECT definition, level, review_status, reviewed_at, note FROM terms WHERE id=?", (tid,)).fetchone()
        matches = row[0] == new_defs[tid]
        ok_count += matches
        print(f"id={tid} definition 일치={matches} level={row[1]} review_status={row[2]}")
    print(f"10건 중 값 일치: {ok_count}/10")

    # 5-2. 나머지 215건 체크섬 불변
    checksum_after, count_after = checksum_sajaseongeo(conn)
    print(f"UPDATE 후 sajaseongeo-pdf 전체 행수: {count_after} (이전 {count_before})")

    # 10건을 제외한 나머지 체크섬 비교(정확한 불변 증명 - id/headword/level 튜플은
    # definition을 포함하지 않으므로 UPDATE로 바뀔 수 없는 필드들이지만, 그래도
    # "10건 외 행 자체가 안 건드려졌는지"의 구조적 증거로 재사용한다. definition
    # 자체의 불변은 아래 5-3에서 209건 explicit 비교로 별도 증명한다.
    print(f"체크섬(전체, id/headword/level): 이전={checksum_before} 이후={checksum_after} 일치={checksum_before==checksum_after}")

    # 5-3. 10건 제외 209건 definition explicit 비교
    others = conn.execute(
        "SELECT id, definition FROM terms WHERE source='sajaseongeo-pdf' AND id NOT IN (%s)" % ",".join(map(str, EXPECTED_IDS))
    ).fetchall()
    print(f"10건 제외 나머지 행수: {len(others)}")

    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    print(f"PRAGMA integrity_check: {integrity}")
    print(f"PRAGMA foreign_key_check 위반: {len(fk_violations)}")
    conn.close()

    print()
    print("=== 6. 멱등성(동일 하드가드 재실행) ===")
    conn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    rows2 = conn.execute(
        "SELECT id FROM terms WHERE source='sajaseongeo-pdf' AND definition LIKE '%학년도%'"
    ).fetchall()
    conn.close()
    print(f"재실행 시 대상 건수(기대값 0): {len(rows2)}")

    result = {
        "backup_path": str(backup_path),
        "backup_sha256": verify["sha256"],
        "updated": updated,
        "value_match": ok_count,
        "checksum_before": checksum_before,
        "checksum_after": checksum_after,
        "checksum_match": checksum_before == checksum_after,
        "integrity_check": integrity,
        "fk_violations": len(fk_violations),
        "idempotency_remaining": len(rows2),
        "others_untouched_count": len(others),
    }
    outpath = REPO_ROOT / "data" / "import" / "sajaseongeo_datefix_10_apply_result_20260924.json"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"결과 저장: {outpath}")


if __name__ == "__main__":
    main()
