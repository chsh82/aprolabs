"""어휘 레벨(자동 후보) CSV 적재 CLI - local_rnd/research 전용, production 거부.

data/import/vocabulary_levels_v0.1.csv(전체 5,723개 어휘의 레벨 후보)를
VOCABULARY_QUIZ_DB_PATH가 가리키는 DB의 vocabulary_content_levels 테이블에
(content_id, level_version) 기준 upsert한다. 이 테이블은 순수 추가 - 기존
vocabulary_contents/items/multiformat_items/review_samples는 전혀 건드리지
않는다. 아직 학생 출제 필터·PUBLIC_READY·student_exposure에는 연결하지 않는다
(레벨 자동 후보 단계일 뿐, 운영 승격 여부는 이 스크립트의 책임 밖).

기존 scripts/vocab/import_multiformat_quiz.py와 같은 안전장치를 그대로 따른다:
기본은 항상 dry-run(트랜잭션 ROLLBACK), --apply는 APP_ENV가 local_rnd/research일
때만 허용, --database가 idiom.db거나 VOCABULARY_QUIZ_DB_PATH와 다르면 거부,
HARD 검증 실패 시 전체 롤백, 재실행 시 unchanged 처리(멱등).

실행 예 (로컬, dry-run):
    APP_ENV=local_rnd VOCABULARY_QUIZ_DB_PATH=data/vocab/vocabulary_quiz_rnd.db \\
    python scripts/vocab/import_vocabulary_levels.py \\
        --csv data/import/vocabulary_levels_v0.1.csv \\
        --database data/vocab/vocabulary_quiz_rnd.db

    (위 + --apply 로 실제 적재)
"""
from __future__ import annotations

import argparse
import csv
import io
import json
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

from app.vocabulary_quiz.db import ensure_schema, get_db_path  # noqa: E402
from app.vocab.db import get_db_path as get_idiom_db_path  # noqa: E402

DEFAULT_CSV = REPO_ROOT / "data" / "import" / "vocabulary_levels_v0.1.csv"
DEFAULT_DATABASE = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd.db"
EXPECTED_VERSION = "level_policy_v0.1"
EXPECTED_ROW_COUNT = 5723
ALLOWED_STATUSES = ("PROVISIONAL_AUTO", "REVIEW_BOUNDARY")

REQUIRED_FIELDS = ["content_id", "vocab_level", "level_status", "level_version"]
_ALLOWED_APPLY_ENVS = ("local_rnd", "research")


# ==================== 환경/경로 가드 ====================

def assert_apply_allowed() -> None:
    import os
    app_env = os.environ.get("APP_ENV", "")
    if app_env not in _ALLOWED_APPLY_ENVS:
        print(f"거부: APP_ENV={app_env!r} 에서는 --apply를 허용하지 않습니다 "
              f"({', '.join(_ALLOWED_APPLY_ENVS)}만 허용). production이거나 APP_ENV 미설정이면 여기서 막힙니다.",
              file=sys.stderr)
        sys.exit(2)
    print(f"환경 확인: APP_ENV={app_env!r} (실제 적재 허용)")


def assert_local_database_path(db_path: Path) -> None:
    resolved = db_path.resolve()
    idiom_path = get_idiom_db_path().resolve()
    if resolved == idiom_path or resolved.name == "idiom.db":
        print(f"거부: --database가 기존 사자성어 DB(idiom.db)를 가리킵니다: {resolved}", file=sys.stderr)
        sys.exit(2)
    expected = get_db_path().resolve()
    if resolved != expected:
        print(f"거부: --database({resolved})가 VOCABULARY_QUIZ_DB_PATH로 지정된 공식 경로"
              f"({expected})와 다릅니다.", file=sys.stderr)
        sys.exit(2)
    print(f"DB 경로 확인: {resolved} (idiom.db 아님, VOCABULARY_QUIZ_DB_PATH와 일치)")


def sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ==================== CSV 로드 ====================

def load_csv(path: Path) -> list[dict]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError as e:
        raise SystemExit(f"치명적 오류: {path.name} 이 UTF-8이 아닙니다: {e}")
    return list(csv.DictReader(io.StringIO(text)))


# ==================== 검증 ====================

def validate(rows: list[dict], db_content_ids: set[str]) -> tuple[list[str], list[str]]:
    hard: list[str] = []
    soft: list[str] = []

    if len(rows) != EXPECTED_ROW_COUNT:
        hard.append(f"CSV 행 수가 {EXPECTED_ROW_COUNT}개가 아님(실제 {len(rows)}개)")

    missing_fields = sum(1 for r in rows if any(not (r.get(f) or "").strip() for f in REQUIRED_FIELDS))
    if missing_fields:
        hard.append(f"필수 필드 누락 {missing_fields}건 ({REQUIRED_FIELDS})")

    csv_content_ids = [r["content_id"] for r in rows if r.get("content_id")]
    dup = len(csv_content_ids) - len(set(csv_content_ids))
    if dup:
        hard.append(f"content_id 중복 {dup}건")

    csv_id_set = set(csv_content_ids)
    missing_in_db = csv_id_set - db_content_ids
    if missing_in_db:
        hard.append(f"vocabulary_contents에 없는 content_id {len(missing_in_db)}건: {sorted(missing_in_db)[:5]}...")
    missing_in_csv = db_content_ids - csv_id_set
    if missing_in_csv:
        hard.append(f"vocabulary_contents에는 있지만 CSV에 없는 content_id {len(missing_in_csv)}건: "
                     f"{sorted(missing_in_csv)[:5]}...")

    bad_level = 0
    bad_status = 0
    bad_version = 0
    bad_confidence = 0
    bad_boundary = 0
    bad_json = 0
    for r in rows:
        try:
            level = int(r["vocab_level"])
            if not (0 <= level <= 6):
                bad_level += 1
        except (ValueError, TypeError):
            bad_level += 1

        if r.get("level_status") not in ALLOWED_STATUSES:
            bad_status += 1

        if r.get("level_version") != EXPECTED_VERSION:
            bad_version += 1

        conf = r.get("level_confidence")
        if conf not in (None, ""):
            try:
                conf_f = float(conf)
                if not (0.0 <= conf_f <= 1.0):
                    bad_confidence += 1
            except ValueError:
                bad_confidence += 1

        flag = r.get("boundary_flag")
        if flag not in (None, "", "0", "1"):
            bad_boundary += 1

        reason = r.get("level_reason_json")
        if reason:
            try:
                json.loads(reason)
            except json.JSONDecodeError:
                bad_json += 1

    if bad_level:
        hard.append(f"vocab_level이 0~6 범위 밖이거나 정수가 아닌 행 {bad_level}건")
    if bad_status:
        hard.append(f"level_status가 허용값({ALLOWED_STATUSES}) 밖인 행 {bad_status}건")
    if bad_version:
        hard.append(f"level_version이 기대값({EXPECTED_VERSION!r})과 다른 행 {bad_version}건")
    if bad_confidence:
        hard.append(f"level_confidence가 0~1 범위 밖인 행 {bad_confidence}건")
    if bad_boundary:
        hard.append(f"boundary_flag가 0/1이 아닌 행 {bad_boundary}건")
    if bad_json:
        hard.append(f"level_reason_json 파싱 실패 {bad_json}건")

    # L5/L6가 0건인 것은 정상 - 실패로 취급하지 않는다(정보용 SOFT만 남김)
    level_counts: dict[int, int] = {}
    for r in rows:
        try:
            level_counts[int(r["vocab_level"])] = level_counts.get(int(r["vocab_level"]), 0) + 1
        except (ValueError, TypeError):
            continue
    for lv in (5, 6):
        if level_counts.get(lv, 0) == 0:
            soft.append(f"[SOFT] L{lv} 0건 (정상 - 고학년 어휘 원천 미확보 단계, 실패 아님)")

    return hard, soft


# ==================== DB 헬퍼 ====================

def load_content_ids(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT content_id FROM vocabulary_contents").fetchall()}


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


_UPDATE_FIELDS = [
    "vocab_level", "target_grade_band", "level_score", "level_confidence", "level_status",
    "boundary_flag", "level_source", "level_reason_json",
]


def _row_values(row: dict) -> dict:
    conf = row.get("level_confidence")
    score = row.get("level_score")
    flag = row.get("boundary_flag")
    return {
        "vocab_level": int(row["vocab_level"]),
        "target_grade_band": row.get("target_grade_band") or None,
        "level_score": float(score) if score not in (None, "") else None,
        "level_confidence": float(conf) if conf not in (None, "") else None,
        "level_status": row["level_status"],
        "boundary_flag": int(flag) if flag not in (None, "") else 0,
        "level_source": row.get("level_source") or None,
        "level_reason_json": row.get("level_reason_json") or None,
    }


def upsert(conn: sqlite3.Connection, rows: list[dict], version: str) -> dict:
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}
    existing = {r[0]: r for r in conn.execute(
        f"SELECT content_id, {', '.join(_UPDATE_FIELDS)} FROM vocabulary_content_levels WHERE level_version = ?",
        (version,),
    )}

    for row in rows:
        cid = row["content_id"]
        values = _row_values(row)
        new_tuple = tuple(values[f] for f in _UPDATE_FIELDS)
        if cid not in existing:
            columns = ["content_id", "level_version", *_UPDATE_FIELDS]
            placeholders = ", ".join("?" for _ in columns)
            conn.execute(
                f"INSERT INTO vocabulary_content_levels ({', '.join(columns)}) VALUES ({placeholders})",
                (cid, version, *new_tuple),
            )
            counts["inserted"] += 1
        else:
            old_tuple = existing[cid][1:]
            if old_tuple != new_tuple:
                set_clause = ", ".join(f"{f} = ?" for f in _UPDATE_FIELDS)
                conn.execute(
                    f"UPDATE vocabulary_content_levels SET {set_clause}, updated_at = datetime('now') "
                    f"WHERE content_id = ? AND level_version = ?",
                    (*new_tuple, cid, version),
                )
                counts["updated"] += 1
            else:
                counts["unchanged"] += 1
    return counts


# ==================== 메인 ====================

def run(csv_path: Path, database: Path, apply: bool) -> int:
    print(f"=== 어휘 레벨(자동 후보) 적재 ({'실제 적재' if apply else 'DRY-RUN'}) ===")

    if not csv_path.exists():
        print(f"파일이 없습니다: {csv_path}", file=sys.stderr)
        return 1
    print(f"원본 CSV: {csv_path} (SHA-256 {sha256_file(csv_path)})")

    assert_local_database_path(database)
    if apply:
        assert_apply_allowed()

    idiom_db_path = get_idiom_db_path()
    idiom_checksum_before = sha256_file(idiom_db_path) if idiom_db_path.exists() else None

    rows = load_csv(csv_path)
    print(f"CSV 행: {len(rows)}건")

    ensure_schema()
    conn = sqlite3.connect(database)
    conn.execute("PRAGMA foreign_keys=ON")

    db_content_ids = load_content_ids(conn)
    hard_failures, soft_warnings = validate(rows, db_content_ids)

    print(f"\n--- HARD 검사: {'실패 ' + str(len(hard_failures)) + '건' if hard_failures else '전부 통과'} ---")
    for f in hard_failures:
        print(f"  X {f}")
    print(f"\n--- SOFT 검사(참고용): {'경고 ' + str(len(soft_warnings)) + '건' if soft_warnings else '전부 통과'} ---")
    for w in soft_warnings:
        print(f"  ! {w}")

    if hard_failures:
        conn.close()
        report_path = csv_path.parent / "import_vocabulary_levels_failures.json"
        report_path.write_text(json.dumps({"hard_failures": hard_failures, "soft_warnings": soft_warnings},
                                           ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n검사 실패 - 실제 적재를 진행하지 않습니다. 상세: {report_path}")
        return 1

    # 기존 콘텐츠·문항·검수 표본·멀티포맷 테이블 불변 확인(적재 전후 비교용 스냅샷)
    guard_tables = ["vocabulary_contents", "vocabulary_items", "vocabulary_review_samples",
                     "vocabulary_multiformat_items"]
    counts_before_guard = {t: _table_count(conn, t) for t in guard_tables}
    exposure_before = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE student_exposure=1").fetchone()[0]
    public_before = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE public_ready=1").fetchone()[0]

    backup_path = None
    if apply and database.exists():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = database.with_name(f"{database.name}.bak-{timestamp}")
        src = sqlite3.connect(database)
        dst = sqlite3.connect(backup_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        print(f"\n백업 완료(sqlite3.Connection.backup API): {backup_path}")

    before = _table_count(conn, "vocabulary_content_levels")
    counts = upsert(conn, rows, EXPECTED_VERSION)
    after = _table_count(conn, "vocabulary_content_levels")

    level_dist: dict[int, int] = {}
    status_dist: dict[str, int] = {}
    for r in rows:
        lv = int(r["vocab_level"])
        level_dist[lv] = level_dist.get(lv, 0) + 1
        status_dist[r["level_status"]] = status_dist.get(r["level_status"], 0) + 1

    print(f"\n--- 결과 ({'적용' if apply else 'dry-run, 아래는 실제 적용 시 예상값'}) ---")
    print(f"레벨 행: 신규 {counts['inserted']} / 갱신 {counts['updated']} / 변경없음 {counts['unchanged']} "
          f"(전 {before} -> 후 {after})")
    print(f"레벨 분포: {dict(sorted(level_dist.items()))}")
    print(f"상태 분포: {status_dist}")

    for t in guard_tables:
        after_guard = _table_count(conn, t)
        assert counts_before_guard[t] == after_guard, f"{t} 행 수가 적재 중 변경됨(전 {counts_before_guard[t]}, 후 {after_guard})"
    exposure_after = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE student_exposure=1").fetchone()[0]
    public_after = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE public_ready=1").fetchone()[0]
    assert exposure_before == exposure_after and public_before == public_after, \
        "student_exposure/public_ready가 적재 중 변경됨"
    print(f"기존 테이블 불변 확인: {guard_tables} 행 수 그대로, student_exposure={exposure_after}건, "
          f"public_ready={public_after}건 그대로")

    if apply:
        conn.commit()
        print("\n실제 적재 완료 (COMMIT)")
    else:
        conn.rollback()
        print("\nDRY-RUN이므로 ROLLBACK - DB는 변경되지 않았습니다. 적재하려면 --apply를 붙여 재실행하세요.")
    conn.close()

    idiom_checksum_after = sha256_file(idiom_db_path) if idiom_db_path.exists() else None
    if idiom_checksum_before != idiom_checksum_after:
        print(f"\n치명적 경고: idiom.db 체크섬이 변경되었습니다! before={idiom_checksum_before} after={idiom_checksum_after}",
              file=sys.stderr)
        return 1
    print(f"\nidiom.db 체크섬 불변 확인: {idiom_checksum_before}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="어휘 레벨(자동 후보) CSV 적재 (기본 dry-run, 로컬 R&D 전용)")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="적재할 CSV 경로")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE, help="적재 대상 SQLite 경로")
    parser.add_argument("--apply", action="store_true", help="실제 적재 (기본은 dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="명시적 dry-run (기본값과 동일, 문서화용)")
    args = parser.parse_args()
    return run(args.csv, args.database, args.apply)


if __name__ == "__main__":
    sys.exit(main())
