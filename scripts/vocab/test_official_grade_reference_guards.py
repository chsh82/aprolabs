# -*- coding: utf-8 -*-
"""db_path_guard.guard_db_path()의 4개 시나리오를 스크래치 경로로만
격리 검증한다(실 연구 DB는 전혀 건드리지 않음). pytest 없이 순수
assert로 작성 - python scripts/vocab/test_official_grade_reference_guards.py
로 직접 실행한다.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.vocab.db_path_guard import DbPathGuardError, guard_db_path  # noqa: E402


def _make_scratch_db(dirpath: Path) -> Path:
    p = dirpath / "scratch_research.db"
    con = sqlite3.connect(str(p))
    con.execute("CREATE TABLE t(x)")
    con.commit()
    con.close()
    return p


def main() -> None:
    results = []
    saved_env = dict(os.environ)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        scratch_db = _make_scratch_db(tmp_path)
        nonexistent = tmp_path / "does_not_exist.db"
        other_db = tmp_path / "other.db"
        sqlite3.connect(str(other_db)).close()

        # (a) VOCABULARY_QUIZ_DB_PATH 미설정
        os.environ.clear()
        os.environ.update(saved_env)
        os.environ["APP_ENV"] = "research"
        os.environ.pop("VOCABULARY_QUIZ_DB_PATH", None)
        try:
            guard_db_path(None)
            results.append(("(a) 미설정 env var", "FAIL - 예외가 발생해야 하는데 통과함"))
        except DbPathGuardError as e:
            ok = "GUARD FAIL(2/4)" in str(e)
            results.append(("(a) 미설정 env var", f"{'PASS' if ok else 'FAIL(잘못된 사유)'} - {e}"))

        # (b) --database가 VOCABULARY_QUIZ_DB_PATH와 다름
        os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(scratch_db)
        try:
            guard_db_path(str(other_db))
            results.append(("(b) --database 불일치", "FAIL - 예외가 발생해야 하는데 통과함"))
        except DbPathGuardError as e:
            ok = "GUARD FAIL(3/4)" in str(e)
            results.append(("(b) --database 불일치", f"{'PASS' if ok else 'FAIL(잘못된 사유)'} - {e}"))

        # (c) VOCABULARY_QUIZ_DB_PATH가 존재하지 않는 파일
        os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(nonexistent)
        try:
            guard_db_path(None)
            results.append(("(c) 존재하지 않는 파일", "FAIL - 예외가 발생해야 하는데 통과함"))
        except DbPathGuardError as e:
            ok = "GUARD FAIL(4/4)" in str(e)
            results.append(("(c) 존재하지 않는 파일", f"{'PASS' if ok else 'FAIL(잘못된 사유)'} - {e}"))

        # (d) 정상 경로(전부 일치, 파일 존재, APP_ENV=research)
        os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(scratch_db)
        try:
            resolved = guard_db_path(str(scratch_db))
            ok = Path(resolved) == scratch_db.resolve()
            results.append(("(d) 정상 경로", f"{'PASS' if ok else 'FAIL'} - resolved={resolved}"))
        except DbPathGuardError as e:
            results.append(("(d) 정상 경로", f"FAIL - 예외 발생하면 안 되는데 발생: {e}"))

        # (e) 보너스: APP_ENV가 research가 아님
        os.environ["APP_ENV"] = "local_rnd"
        try:
            guard_db_path(str(scratch_db))
            results.append(("(e) APP_ENV != research", "FAIL - 예외가 발생해야 하는데 통과함"))
        except DbPathGuardError as e:
            ok = "GUARD FAIL(1/4)" in str(e)
            results.append(("(e) APP_ENV != research", f"{'PASS' if ok else 'FAIL(잘못된 사유)'} - {e}"))

    os.environ.clear()
    os.environ.update(saved_env)

    print("=== db_path_guard 격리 테스트 결과 ===")
    all_pass = True
    for name, outcome in results:
        print(f"{name}: {outcome}")
        if not outcome.startswith("PASS"):
            all_pass = False

    print()
    if all_pass:
        print("전부 PASS")
        sys.exit(0)
    else:
        print("일부 FAIL - 위 로그 확인")
        sys.exit(1)


if __name__ == "__main__":
    main()
