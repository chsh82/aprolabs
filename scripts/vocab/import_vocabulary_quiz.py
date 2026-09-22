"""일반 어휘 퀴즈 DB 적재 CLI - local_rnd/research 전용, production 거부.

이 프로젝트는 FastAPI이고 Flask가 아니라서(app/main.py의 include_router
패턴 확인됨) `flask vocabulary import-db` 형태의 Flask CLI 명령은 만들
수 없다 - 기존 scripts/vocab/*.py(rebuild_db.py, seed_relations.py 등)와
같은 독립 argparse 스크립트로 만들고, 플래그 이름만 요청받은 대로 맞췄다.

<archive>의 server_staging_candidate_content/items CSV를 VOCABULARY_QUIZ_DB_PATH가
가리키는 DB(app/vocab/idiom.db와 완전히 별개 파일, 별개 모듈)에 upsert한다.

기본은 항상 dry-run(트랜잭션을 ROLLBACK). 실제 적재는 --apply를 줘야만
COMMIT한다. --apply는 환경변수 APP_ENV가 'local_rnd' 또는 'research'일
때만 허용한다(운영 온라인 서버는 리눅스이므로 OS 종류만으로는 로컬/서버
연구환경과 운영을 구분할 수 없다 - 반드시 APP_ENV를 명시해야 한다).
APP_ENV=production이거나 미설정이면 거부한다. --database 경로가 idiom.db를
가리키거나 VOCABULARY_QUIZ_DB_PATH로 지정된 공식 경로와 다르면 즉시 거부한다.

실행 예 (로컬):
    APP_ENV=local_rnd VOCABULARY_QUIZ_DB_PATH=data/vocab/vocabulary_quiz_rnd.db \\
    python scripts/vocab/import_vocabulary_quiz.py \\
        --archive data/import/vocabulary_master_v2.1.29_rewrite_batch_019.zip \\
        --database data/vocab/vocabulary_quiz_rnd.db \\
        --version 2.1.29
        # (기본 dry-run)

    APP_ENV=local_rnd VOCABULARY_QUIZ_DB_PATH=data/vocab/vocabulary_quiz_rnd.db \\
    python scripts/vocab/import_vocabulary_quiz.py \\
        --archive data/import/vocabulary_master_v2.1.29_rewrite_batch_019.zip \\
        --database data/vocab/vocabulary_quiz_rnd.db \\
        --version 2.1.29 --apply

실행 예 (연구용 서버, APP_ENV=research):
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<persistent-db-path> \\
    python scripts/vocab/import_vocabulary_quiz.py \\
        --archive <persistent-import-zip-path> \\
        --database <persistent-db-path> \\
        --version 2.1.29 --apply
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
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

DEFAULT_ARCHIVE = REPO_ROOT / "data" / "import" / "vocabulary_master_v2.1.29_rewrite_batch_019.zip"
DEFAULT_DATABASE = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd.db"
DEFAULT_VERSION = "2.1.29"
EXTRACT_ROOT = REPO_ROOT / "data" / "import"

CONTENT_CSV = "server_staging_candidate_content_v2.1.29.csv"
ITEMS_CSV = "server_staging_candidate_items_v2.1.29.csv"
CUMULATIVE_VERIFICATION_JSON = "cumulative_verification_v2.1.29.json"

CONTENT_REQUIRED = ["content_id", "sense_id", "lemma", "generation_status", "canonical_definition"]
ITEM_REQUIRED = ["item_id", "content_id", "stem", "option_1", "option_2", "option_3", "option_4", "correct_option"]

CONTENT_UPDATE_FIELDS = [
    "sense_id", "lexical_entry_id", "batch_id", "lemma", "pos",
    "canonical_definition", "student_definition", "example_sentence",
    "example_target_form", "generation_method", "qa_method",
    "generation_status", "quality_batch_id", "hold_reason", "merge_source",
]
ITEM_UPDATE_FIELDS = [
    "sense_id", "batch_id", "lemma", "item_type", "stem",
    "option_1", "option_2", "option_3", "option_4", "correct_option",
    "explanation", "generation_status", "quality_batch_id", "merge_source",
]

_INT_FIELDS = {"correct_option"}
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


# ==================== 환경/경로 가드 ====================
#
# OS 종류(Windows/Linux)만으로는 온라인 연구 서버(Linux)를 R&D로 인식할 수
# 없으므로, 명시적 환경변수 APP_ENV로 판단한다.
#   APP_ENV=local_rnd  - 로컬 개발 머신
#   APP_ENV=research   - 연구용 GCP 서버(소유자 전용, 고객 미접근)
#   APP_ENV=production - 향후 실제 운영 환경 (여기서는 --apply 거부)
_ALLOWED_APPLY_ENVS = ("local_rnd", "research")


def assert_apply_allowed() -> None:
    app_env = os.environ.get("APP_ENV", "")
    if app_env not in _ALLOWED_APPLY_ENVS:
        print(f"거부: APP_ENV={app_env!r} 에서는 --apply를 허용하지 않습니다 "
              f"({', '.join(_ALLOWED_APPLY_ENVS)}만 허용). production이거나 APP_ENV 미설정이면 여기서 막힙니다.",
              file=sys.stderr)
        sys.exit(2)
    print(f"환경 확인: APP_ENV={app_env!r} (실제 적재 허용)")


def assert_local_database_path(db_path: Path) -> None:
    """운영/사자성어 DB를 실수로 가리키는 것과, VOCABULARY_QUIZ_DB_PATH로
    지정된 공식 경로가 아닌 임의 경로에 적재하는 것을 막는다. 서버의
    영구 데이터 경로는 저장소 바깥(배포 디렉터리 밖)이므로 "저장소
    data/vocab/ 안"이라는 조건은 쓰지 않는다 - 대신 "VOCABULARY_QUIZ_DB_PATH가
    가리키는 값과 정확히 같아야 한다"로 검사한다."""
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


# ==================== 원본 파일 처리 ====================

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_archive(zip_path: Path, version: str) -> Path:
    """원본 ZIP은 건드리지 않고, data/import/vocabulary_master_<version>/에 압축 해제한다."""
    try:
        with zipfile.ZipFile(zip_path) as z:
            bad = z.testzip()
            if bad is not None:
                raise SystemExit(f"치명적 오류: ZIP이 손상되었습니다 (손상된 파일: {bad})")
            extract_dir = EXTRACT_ROOT / f"vocabulary_master_{version}"
            extract_dir.mkdir(parents=True, exist_ok=True)
            z.extractall(extract_dir)
    except zipfile.BadZipFile as e:
        raise SystemExit(f"치명적 오류: ZIP이 손상되었습니다: {e}")
    return extract_dir


def load_csv(path: Path) -> list[dict]:
    raw = path.read_bytes()
    try:
        raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as e:
        raise SystemExit(f"치명적 오류: {path.name} 이 UTF-8이 아닙니다: {e}")
    text = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8")
    return list(csv.DictReader(text))


def check_cumulative_verification(extract_dir: Path) -> tuple[bool, str]:
    path = extract_dir / CUMULATIVE_VERIFICATION_JSON
    if not path.exists():
        return False, f"{CUMULATIVE_VERIFICATION_JSON} 파일이 없습니다"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return False, f"{CUMULATIVE_VERIFICATION_JSON} 파싱 실패: {e}"

    # verification/status/overall_status 등 흔히 쓰이는 키를 순서대로 탐색
    def _find_status(obj, depth=0):
        if depth > 4 or not isinstance(obj, dict):
            return None
        for key in ("verification", "status", "overall_status", "result"):
            if key in obj and isinstance(obj[key], str):
                return obj[key]
        for v in obj.values():
            found = _find_status(v, depth + 1)
            if found:
                return found
        return None

    status = _find_status(data)
    if status is None:
        return False, f"{CUMULATIVE_VERIFICATION_JSON}에서 verification/status 값을 찾지 못함 (원본: {json.dumps(data, ensure_ascii=False)[:200]})"
    ok = status.strip().upper() == "PASS"
    return ok, f"{CUMULATIVE_VERIFICATION_JSON} verification={status!r}"


# ==================== 검증 ====================

def _has_formula_injection(value: str | None) -> bool:
    if not value:
        return False
    return value.startswith(_FORMULA_PREFIXES)


def _coerce(field: str, value: str | None):
    """SQLite INTEGER 컬럼은 조회 시 int로 돌아오는데 CSV 원본은 문자열이다.
    캐스팅 없이 비교하면 1 != '1'로 판정돼 재실행마다 '변경됨' 오탐이 난다
    (실제로 이전 라운드에서 이 버그를 재현해서 고쳤다 - 여기도 동일 적용)."""
    if value in (None, ""):
        return None
    if field in _INT_FIELDS:
        return int(value)
    return value


def validate(content_rows: list[dict], item_rows: list[dict],
             cum_verification_ok: bool, cum_verification_note: str) -> tuple[list[str], list[str]]:
    hard: list[str] = []
    soft: list[str] = []

    content_ids = [r["content_id"] for r in content_rows]
    item_ids = [r["item_id"] for r in item_rows]

    dup_content = len(content_ids) - len(set(content_ids))
    if dup_content:
        hard.append(f"content_id 중복 {dup_content}건")
    dup_items = len(item_ids) - len(set(item_ids))
    if dup_items:
        hard.append(f"item_id 중복 {dup_items}건")

    content_id_set = set(content_ids)
    item_content_ids = set(r["content_id"] for r in item_rows)
    orphan_items = item_content_ids - content_id_set
    if orphan_items:
        hard.append(f"콘텐츠 없는 문항 {len(orphan_items)}건: {sorted(orphan_items)[:5]}...")
    content_without_item = content_id_set - item_content_ids
    if content_without_item:
        hard.append(f"문항 없는 콘텐츠 {len(content_without_item)}건: {sorted(content_without_item)[:5]}...")

    missing_field_rows = sum(1 for r in content_rows if any(not (r.get(f) or "").strip() for f in CONTENT_REQUIRED))
    if missing_field_rows:
        hard.append(f"콘텐츠 필수값 누락(canonical_definition 포함) {missing_field_rows}건 ({CONTENT_REQUIRED})")

    missing_item_field_rows = 0
    bad_correct_option = 0
    dup_options = 0
    for r in item_rows:
        if any(not (r.get(f) or "").strip() for f in ITEM_REQUIRED):
            missing_item_field_rows += 1
            continue
        if r["correct_option"] not in ("1", "2", "3", "4"):
            bad_correct_option += 1
        opts = [r["option_1"], r["option_2"], r["option_3"], r["option_4"]]
        if len(set(opts)) != 4:
            dup_options += 1
    if missing_item_field_rows:
        hard.append(f"문항 필수값 누락 {missing_item_field_rows}건 ({ITEM_REQUIRED})")
    if bad_correct_option:
        hard.append(f"정답 번호 범위 오류(1~4 아님) {bad_correct_option}건")
    if dup_options:
        hard.append(f"선택지 중복 오류 {dup_options}건")

    auto_hold_content = sum(1 for r in content_rows if r.get("generation_status") == "AUTO_HOLD")
    auto_hold_items = sum(1 for r in item_rows if r.get("generation_status") == "AUTO_HOLD")
    if auto_hold_content or auto_hold_items:
        hard.append(f"AUTO_HOLD 유입: 콘텐츠 {auto_hold_content}건, 문항 {auto_hold_items}건")

    exposure_on = sum(1 for r in content_rows if r.get("student_exposure") != "0") + \
        sum(1 for r in item_rows if r.get("student_exposure") != "0")
    if exposure_on:
        hard.append(f"student_exposure 활성 데이터 {exposure_on}건")

    public_on = sum(1 for r in content_rows if r.get("public_ready") != "0") + \
        sum(1 for r in item_rows if r.get("public_ready") != "0")
    if public_on:
        hard.append(f"public_ready 활성 데이터 {public_on}건")

    injection_hits = sum(
        1 for r in content_rows + item_rows
        if any(_has_formula_injection(v) for v in r.values())
    )
    if injection_hits:
        hard.append(f"CSV 수식 삽입 위험 문자열이 있는 행 {injection_hits}건")

    if not cum_verification_ok:
        hard.append(f"누적 검증 JSON이 PASS가 아님: {cum_verification_note}")

    # SOFT
    content_by_id = {r["content_id"]: r for r in content_rows}
    mismatch_answer = 0
    for r in item_rows:
        c = content_by_id.get(r["content_id"])
        if not c:
            continue
        try:
            idx = int(r["correct_option"])
        except (TypeError, ValueError):
            continue
        opt_key = f"option_{idx}"
        if c.get("student_definition") and r.get(opt_key) and c["student_definition"] != r[opt_key]:
            mismatch_answer += 1
    if mismatch_answer:
        soft.append(f"[SOFT] correct_option 위치 선택지가 student_definition과 다른 문항 {mismatch_answer}건 "
                     f"(표현 차이일 수 있음 - 사람 검수 권장)")

    missing_target_form = sum(
        1 for r in content_rows
        if r.get("example_sentence") and r.get("example_target_form")
        and r["example_target_form"] not in r["example_sentence"]
    )
    if missing_target_form:
        soft.append(f"[SOFT] example_sentence에 example_target_form이 그대로 없는 콘텐츠 {missing_target_form}건 "
                     f"(활용형 차이일 수 있음 - 사람 검수 권장)")

    return hard, soft


# ==================== DB 헬퍼 ====================

def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def upsert(conn: sqlite3.Connection, content_rows: list[dict], item_rows: list[dict], version: str) -> dict:
    """content_id/item_id 기준 upsert. student_exposure/public_ready는
    이미 있는 행이면 절대 갱신하지 않는다 - 나중에 운영 승격으로 값이
    바뀌어 있을 수 있는데, 재적재가 그걸 되돌리면 안 되기 때문이다."""
    counts = {"content_inserted": 0, "content_updated": 0, "content_unchanged": 0,
              "items_inserted": 0, "items_updated": 0, "items_unchanged": 0}

    existing_content = {row[0]: row for row in conn.execute(
        f"SELECT content_id, {', '.join(CONTENT_UPDATE_FIELDS)} FROM vocabulary_contents"
    )}
    for r in content_rows:
        cid = r["content_id"]
        new_values = tuple(_coerce(f, r.get(f)) for f in CONTENT_UPDATE_FIELDS)
        if cid not in existing_content:
            conn.execute(
                f"""INSERT INTO vocabulary_contents
                    (content_id, {', '.join(CONTENT_UPDATE_FIELDS)},
                     student_exposure, public_ready, source_version)
                    VALUES (?, {', '.join('?' for _ in CONTENT_UPDATE_FIELDS)}, ?, ?, ?)""",
                (cid, *new_values, int(r.get("student_exposure") or 0),
                 int(r.get("public_ready") or 0), version),
            )
            counts["content_inserted"] += 1
        else:
            old_values = existing_content[cid][1:]
            if old_values != new_values:
                set_clause = ", ".join(f"{f} = ?" for f in CONTENT_UPDATE_FIELDS)
                conn.execute(
                    f"UPDATE vocabulary_contents SET {set_clause}, source_version = ?, "
                    f"updated_at = datetime('now') WHERE content_id = ?",
                    (*new_values, version, cid),
                )
                counts["content_updated"] += 1
            else:
                conn.execute(
                    "UPDATE vocabulary_contents SET source_version = ? WHERE content_id = ?",
                    (version, cid),
                )
                counts["content_unchanged"] += 1

    existing_items = {row[0]: row for row in conn.execute(
        f"SELECT item_id, {', '.join(ITEM_UPDATE_FIELDS)} FROM vocabulary_items"
    )}
    for r in item_rows:
        iid = r["item_id"]
        new_values = tuple(_coerce(f, r.get(f)) for f in ITEM_UPDATE_FIELDS)
        if iid not in existing_items:
            conn.execute(
                f"""INSERT INTO vocabulary_items
                    (item_id, content_id, {', '.join(ITEM_UPDATE_FIELDS)},
                     student_exposure, public_ready, source_version)
                    VALUES (?, ?, {', '.join('?' for _ in ITEM_UPDATE_FIELDS)}, ?, ?, ?)""",
                (iid, r["content_id"], *new_values, int(r.get("student_exposure") or 0),
                 int(r.get("public_ready") or 0), version),
            )
            counts["items_inserted"] += 1
        else:
            old_values = existing_items[iid][1:]
            if old_values != new_values:
                set_clause = ", ".join(f"{f} = ?" for f in ITEM_UPDATE_FIELDS)
                conn.execute(
                    f"UPDATE vocabulary_items SET {set_clause}, source_version = ?, "
                    f"updated_at = datetime('now') WHERE item_id = ?",
                    (*new_values, version, iid),
                )
                counts["items_updated"] += 1
            else:
                conn.execute(
                    "UPDATE vocabulary_items SET source_version = ? WHERE item_id = ?",
                    (version, iid),
                )
                counts["items_unchanged"] += 1

    return counts


# ==================== 메인 ====================

def run(archive: Path, database: Path, version: str, apply: bool) -> int:
    print(f"=== 일반 어휘 퀴즈 v{version} 적재 ({'실제 적재' if apply else 'DRY-RUN'}) ===")

    if not archive.exists():
        print(f"파일이 없습니다: {archive}", file=sys.stderr)
        print(f"예상 경로: {archive}", file=sys.stderr)
        return 1
    print(f"원본 ZIP: {archive}")

    assert_local_database_path(database)
    if apply:
        assert_apply_allowed()

    idiom_db_path = get_idiom_db_path()
    idiom_checksum_before = sha256_file(idiom_db_path) if idiom_db_path.exists() else None

    source_sha256 = sha256_file(archive)
    print(f"원본 ZIP SHA-256: {source_sha256}")

    extract_dir = extract_archive(archive, version)
    print(f"압축 해제: {extract_dir} (원본 ZIP은 변경하지 않음)")

    content_rows = load_csv(extract_dir / CONTENT_CSV)
    item_rows = load_csv(extract_dir / ITEMS_CSV)
    print(f"콘텐츠 행: {len(content_rows)}건 / 문항 행: {len(item_rows)}건")

    cum_ok, cum_note = check_cumulative_verification(extract_dir)
    print(f"누적 검증: {cum_note}")

    hard_failures, soft_warnings = validate(content_rows, item_rows, cum_ok, cum_note)

    print(f"\n--- HARD 검사: {'실패 ' + str(len(hard_failures)) + '건' if hard_failures else '전부 통과'} ---")
    for f in hard_failures:
        print(f"  X {f}")
    print(f"\n--- SOFT 검사(참고용): {'경고 ' + str(len(soft_warnings)) + '건' if soft_warnings else '전부 통과'} ---")
    for w in soft_warnings:
        print(f"  ! {w}")

    # assert_local_database_path()가 이미 database == get_db_path()임을 보장했으므로
    # (VOCABULARY_QUIZ_DB_PATH가 이 경로를 가리키는 상태) ensure_schema() 하나로 충분하다.
    ensure_schema()

    started_at = datetime.now(timezone.utc).isoformat()

    if hard_failures:
        conn = sqlite3.connect(database)
        conn.execute(
            """INSERT INTO vocabulary_import_batches
               (version, source_filename, source_sha256, started_at, completed_at, status,
                content_count, item_count, validation_result, notes)
               VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?)""",
            (version, archive.name, source_sha256, started_at,
             "DRY_RUN_FAILED" if not apply else "FAILED",
             len(content_rows), len(item_rows),
             json.dumps({"hard": hard_failures, "soft": soft_warnings}, ensure_ascii=False),
             "검사 실패로 적재 중단"),
        )
        conn.commit()
        conn.close()
        report_path = EXTRACT_ROOT / f"import_failures_v{version}.json"
        report_path.write_text(json.dumps({"hard_failures": hard_failures, "soft_warnings": soft_warnings},
                                           ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n검사 실패 - 실제 적재를 진행하지 않습니다. 상세: {report_path}")
        return 1

    backup_path = None
    if apply and database.exists():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = database.with_name(f"{database.name}.bak-{timestamp}")
        shutil.copy2(database, backup_path)
        print(f"\n백업 완료: {backup_path}")

    conn = sqlite3.connect(database)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        content_before = _table_count(conn, "vocabulary_contents")
        items_before = _table_count(conn, "vocabulary_items")

        counts = upsert(conn, content_rows, item_rows, version)

        content_after = _table_count(conn, "vocabulary_contents")
        items_after = _table_count(conn, "vocabulary_items")

        print(f"\n--- 결과 ({'적용' if apply else 'dry-run, 아래는 실제 적용 시 예상값'}) ---")
        print(f"콘텐츠: 신규 {counts['content_inserted']} / 갱신 {counts['content_updated']} / "
              f"변경없음 {counts['content_unchanged']}  (전 {content_before} -> 후 {content_after})")
        print(f"문항:   신규 {counts['items_inserted']} / 갱신 {counts['items_updated']} / "
              f"변경없음 {counts['items_unchanged']}  (전 {items_before} -> 후 {items_after})")

        conn.execute(
            """INSERT INTO vocabulary_import_batches
               (version, source_filename, source_sha256, started_at, completed_at, status,
                content_count, item_count, inserted_count, updated_count, unchanged_count,
                failed_count, validation_result)
               VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)""",
            (version, archive.name, source_sha256, started_at,
             "COMPLETED" if apply else "DRY_RUN_OK",
             len(content_rows), len(item_rows),
             counts["content_inserted"] + counts["items_inserted"],
             counts["content_updated"] + counts["items_updated"],
             counts["content_unchanged"] + counts["items_unchanged"],
             0,
             json.dumps({"hard": hard_failures, "soft": soft_warnings}, ensure_ascii=False)),
        )

        if apply:
            conn.commit()
            print("\n실제 적재 완료 (COMMIT)")
        else:
            conn.rollback()
            print("\nDRY-RUN이므로 ROLLBACK - DB는 변경되지 않았습니다. 적재하려면 --apply를 붙여 재실행하세요.")
    finally:
        conn.close()

    idiom_checksum_after = sha256_file(idiom_db_path) if idiom_db_path.exists() else None
    if idiom_checksum_before != idiom_checksum_after:
        print(f"\n치명적 경고: idiom.db 체크섬이 변경되었습니다! before={idiom_checksum_before} after={idiom_checksum_after}",
              file=sys.stderr)
        return 1
    print(f"\nidiom.db 체크섬 불변 확인: {idiom_checksum_before}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="일반 어휘 퀴즈 DB 적재 (기본 dry-run, 로컬 R&D 전용)")
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE, help="적재할 zip 경로")
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE, help="적재 대상 SQLite 경로")
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument("--apply", action="store_true", help="실제 적재 (기본은 dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="명시적 dry-run (기본값과 동일, 문서화용)")
    args = parser.parse_args()
    return run(args.archive, args.database, args.version, args.apply)


if __name__ == "__main__":
    sys.exit(main())
