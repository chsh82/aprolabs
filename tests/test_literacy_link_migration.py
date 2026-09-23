"""schema_reading x vocabulary_quiz 4단계(literacy 링크 적용) 회귀 테스트.

`scripts/vocab/migrate_vocabulary_quiz_add_literacy_links.py`,
`scripts/vocab/import_literacy_links.py`,
`scripts/vocab/rollback_literacy_link_apply.py`를 실제 서브프로세스로 실행해
검증한다(fixture는 임시 디렉토리에 만드는 가짜 DB - 실제 research DB는
건드리지 않음). pytest 없음, tests/test_admin_level_quiz.py와 같은 방식.

`import_literacy_links.py`는 "정확히 142건"이 아니면 방어적으로 중단하도록
하드코딩돼 있다(4단계 apply 전용 1회성 가드) - 그래서 성공 경로 테스트는
fixture도 정확히 142건으로 맞춘다. 실패 경로 테스트에서만 다른 건수를 쓴다.

검증 대상:
- APP_ENV 하드 가드(research가 아니면 무조건 중단, 아무것도 안 씀)
- 마이그레이션 DDL 멱등성(재실행해도 안전)
- 임포터 dry-run이 실제로 아무것도 쓰지 않는지
- 임포터 apply의 멱등성(재실행 시 추가 삽입 0)
- 정확히 142건이 아니면 임포터가 중단하는 방어 로직
- 롤백 스크립트의 dry-run(무변경) / --confirm(실제 복원) 동작

실행:
    python tests/test_literacy_link_migration.py
"""
from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = REPO_ROOT / "scripts" / "vocab"
MIGRATE_PY = SCRIPTS / "migrate_vocabulary_quiz_add_literacy_links.py"
IMPORT_PY = SCRIPTS / "import_literacy_links.py"
ROLLBACK_PY = SCRIPTS / "rollback_literacy_link_apply.py"

EXPECTED_TARGET_COUNT = 142  # import_literacy_links.py에 하드코딩된 기대 건수

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = ""):
    _results.append((name, condition, detail))
    print(f"{_PASS if condition else _FAIL} {name}" + (f" - {detail}" if detail and not condition else ""))


def run(script: Path, args: list[str]) -> subprocess.CompletedProcess:
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, str(script)] + args,
        capture_output=True, text=True, encoding="utf-8", errors="replace", env=env,
    )


def make_env_file(tmp: Path, app_env: str) -> Path:
    p = tmp / f".env.{app_env}"
    p.write_text(f"APP_ENV={app_env}\n", encoding="utf-8")
    return p


def make_fixture_db(path: Path, n_contents: int) -> None:
    """실제 vocabulary_quiz_research.db와 스키마 이름만 같은 최소 fixture."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE vocabulary_contents (content_id TEXT PRIMARY KEY, is_active INTEGER NOT NULL DEFAULT 1)"
    )
    for i in range(n_contents):
        conn.execute("INSERT INTO vocabulary_contents (content_id, is_active) VALUES (?, 1)", (f"FIX_{i}",))
    conn.commit()
    conn.close()


def make_targets_json(tmp: Path, n: int) -> Path:
    targets = [
        {
            "vocab_content_id": f"FIX_{i}",
            "literacy_term_id": 1000 + i,
            "headword": f"fixture{i}",
            "literacy_headword": f"fixture{i}",
            "literacy_source": "fixture-source",
            "link_method": "test",
            "evidence": "fixture row for regression test",
        }
        for i in range(n)
    ]
    p = tmp / "targets.json"
    p.write_text(json.dumps({"targets": targets}, ensure_ascii=False), encoding="utf-8")
    return p


def table_exists(db_path: Path, name: str) -> bool:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchall()
    conn.close()
    return len(r) == 1


def row_count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.close()
    return n


def test_app_env_gate_blocks_non_research():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_path = tmp / "vocabulary_quiz_research.db"
        make_fixture_db(db_path, EXPECTED_TARGET_COUNT)
        env_file = make_env_file(tmp, "production")

        r = run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        check("migrate: APP_ENV=production -> 비정상 종료", r.returncode != 0, r.stdout)
        check("migrate: APP_ENV=production -> 테이블 생성 안 됨",
              not table_exists(db_path, "vocabulary_content_literacy_links"))


def test_migrate_ddl_idempotent():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_path = tmp / "vocabulary_quiz_research.db"
        make_fixture_db(db_path, EXPECTED_TARGET_COUNT)
        env_file = make_env_file(tmp, "research")

        r1 = run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        check("migrate: 1회차 apply 성공", r1.returncode == 0, r1.stdout + r1.stderr)
        check("migrate: 1회차 후 테이블 존재", table_exists(db_path, "vocabulary_content_literacy_links"))

        r2 = run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        check("migrate: 2회차 apply(재실행)도 성공(멱등)", r2.returncode == 0, r2.stdout + r2.stderr)


def test_import_dry_run_writes_nothing():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_path = tmp / "vocabulary_quiz_research.db"
        make_fixture_db(db_path, EXPECTED_TARGET_COUNT)
        env_file = make_env_file(tmp, "research")
        run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        targets = make_targets_json(tmp, EXPECTED_TARGET_COUNT)

        before = row_count(db_path, "vocabulary_content_literacy_links")
        r = run(IMPORT_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--targets-json", str(targets)])
        after = row_count(db_path, "vocabulary_content_literacy_links")
        check("import: dry-run 종료코드 0", r.returncode == 0, r.stdout + r.stderr)
        check("import: dry-run은 실제로 아무것도 안 씀", before == after == 0)
        check(f"import: dry-run 출력에 DRY_RUN_INSERT_COUNT={EXPECTED_TARGET_COUNT}",
              f"DRY_RUN_INSERT_COUNT={EXPECTED_TARGET_COUNT}" in r.stdout, r.stdout)


def test_import_apply_then_rerun_is_idempotent():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_path = tmp / "vocabulary_quiz_research.db"
        make_fixture_db(db_path, EXPECTED_TARGET_COUNT)
        env_file = make_env_file(tmp, "research")
        run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        targets = make_targets_json(tmp, EXPECTED_TARGET_COUNT)

        r1 = run(IMPORT_PY, ["--env-file", str(env_file), "--db-path", str(db_path),
                              "--targets-json", str(targets), "--apply"])
        check("import: 1회차 apply 성공", r1.returncode == 0, r1.stdout + r1.stderr)
        check(f"import: 1회차 후 {EXPECTED_TARGET_COUNT}건 삽입",
              row_count(db_path, "vocabulary_content_literacy_links") == EXPECTED_TARGET_COUNT)
        check(f"import: 1회차 APPLY_INSERTED={EXPECTED_TARGET_COUNT}",
              f"APPLY_INSERTED={EXPECTED_TARGET_COUNT}" in r1.stdout, r1.stdout)

        r2 = run(IMPORT_PY, ["--env-file", str(env_file), "--db-path", str(db_path),
                              "--targets-json", str(targets), "--apply"])
        check("import: 2회차(재실행) 성공", r2.returncode == 0, r2.stdout + r2.stderr)
        check(f"import: 2회차 후에도 여전히 {EXPECTED_TARGET_COUNT}건(중복 삽입 없음)",
              row_count(db_path, "vocabulary_content_literacy_links") == EXPECTED_TARGET_COUNT)
        check("import: 2회차 APPLY_INSERTED=0(멱등)", "APPLY_INSERTED=0" in r2.stdout, r2.stdout)


def test_import_rejects_wrong_target_count():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_path = tmp / "vocabulary_quiz_research.db"
        make_fixture_db(db_path, 3)
        env_file = make_env_file(tmp, "research")
        run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        targets = make_targets_json(tmp, 3)  # 정확히 142건이 아니면 거부돼야 함

        r = run(IMPORT_PY, ["--env-file", str(env_file), "--db-path", str(db_path),
                             "--targets-json", str(targets), "--apply"])
        check("import: 기대 건수(142)와 다르면 중단", r.returncode != 0, r.stdout)
        check("import: 기대 건수 불일치 시 아무것도 안 씀",
              row_count(db_path, "vocabulary_content_literacy_links") == 0)


def test_rollback_dry_run_then_confirm():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        db_path = tmp / "vocabulary_quiz_research.db"
        make_fixture_db(db_path, EXPECTED_TARGET_COUNT)
        env_file = make_env_file(tmp, "research")

        # "적용 전" 상태를 진짜로 백업해둔다(마이그레이션 실행 전에 복사).
        backups_dir = tmp / "backups"
        backups_dir.mkdir()
        backup_path = backups_dir / "vocabulary_quiz_research.db.bak-test-pre-migration"
        shutil.copy2(db_path, backup_path)

        run(MIGRATE_PY, ["--env-file", str(env_file), "--db-path", str(db_path), "--apply"])
        targets = make_targets_json(tmp, EXPECTED_TARGET_COUNT)
        run(IMPORT_PY, ["--env-file", str(env_file), "--db-path", str(db_path),
                        "--targets-json", str(targets), "--apply"])
        check(f"rollback fixture 준비: 링크 {EXPECTED_TARGET_COUNT}건 존재",
              row_count(db_path, "vocabulary_content_literacy_links") == EXPECTED_TARGET_COUNT)
        check("rollback fixture 준비: 백업에는 링크 테이블이 없음(적용 전 상태)",
              not table_exists(backup_path, "vocabulary_content_literacy_links"))

        r_dry = run(ROLLBACK_PY, ["--env-file", str(env_file), "--db-path", str(db_path),
                                   "--backup-path", str(backup_path)])
        check("rollback: dry-run 종료코드 0", r_dry.returncode == 0, r_dry.stdout + r_dry.stderr)
        check("rollback: dry-run은 DB를 안 바꿈(여전히 링크 존재)",
              row_count(db_path, "vocabulary_content_literacy_links") == EXPECTED_TARGET_COUNT)

        r_confirm = run(ROLLBACK_PY, ["--env-file", str(env_file), "--db-path", str(db_path),
                                       "--backup-path", str(backup_path), "--confirm"])
        check("rollback: --confirm 종료코드 0", r_confirm.returncode == 0, r_confirm.stdout + r_confirm.stderr)
        check("rollback: 복원 후 링크 테이블이 적용 전 상태(테이블 없음)로 돌아감",
              not table_exists(db_path, "vocabulary_content_literacy_links"))
        check("rollback: 안전 백업(SAFETY_COPY) 파일 생성됨", "SAFETY_COPY=" in r_confirm.stdout, r_confirm.stdout)


def main() -> int:
    test_app_env_gate_blocks_non_research()
    test_migrate_ddl_idempotent()
    test_import_dry_run_writes_nothing()
    test_import_apply_then_rerun_is_idempotent()
    test_import_rejects_wrong_target_count()
    test_rollback_dry_run_then_confirm()

    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_fail = len(_results) - n_pass
    print(f"\n{n_pass}/{len(_results)} passed" + (f", {n_fail} FAILED" if n_fail else ""))
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
