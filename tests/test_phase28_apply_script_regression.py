# -*- coding: utf-8 -*-
"""Phase29 항목4 - phase28에서 발견한 'DB 커밋 후 검증 쿼리 예외'가 재발해도
적용 결과를 오인하지 않도록 하는 회귀 테스트.

phase28_apply_l6_pilot_quiz_items.py는 최초 실행 시 GATE 5(단일 트랜잭션
커밋)까지는 정상 완료됐지만, 그 직후(GATE 6) 실행되는 읽기 전용 검증 쿼리
하나(`GROUP BY item_id HAVING c > 1`)에 파라미터 바인딩이 빠져 있어 스크립트
자체가 예외로 죽었다(sqlite3.ProgrammingError). 이때 데이터는 이미 정상
커밋된 상태였는데, 스크립트의 비정상 종료만 보고 "적용 실패"로 오인하면
안 된다는 교훈을 재현 가능한 테스트로 남긴다.

이 버그는 "신규 삽입이 실제로 있을 때"(GATE 4의 to_insert가 비어 있지 않을
때)만 도달하는 코드 경로에 있다 - 이미 40건이 전부 적재된 지금의 운영 DB로는
멱등 재실행(GATE 4에서 조기 return)만 가능해 그 경로에 다시 도달할 수 없다.
그래서 이 테스트는 완전히 격리된 새 SQLite 파일에 스키마와 phase28이 요구하는
최소 콘텐츠(20개 L6 content_id, is_active/exposure/public/level_status 조건
충족)만 채워 넣고, "최초 삽입" 상황을 실제로 재현해 스크립트 전체(GATE 1~6)를
그대로 실행한다. GATE 1(APP_ENV/프로세스 environ 하드가드)만, 격리 DB를
가리키는 프로세스가 실제로는 없으므로 두 헬퍼 함수를 모듈 로드 후 패치해
우회한다(이 패치는 "GATE 1 검증 자체를 테스트한다"는 목적이 아니라 - 그건
스크립트 소스를 읽고 이미 확인됨 - GATE 3 이후의 커밋·검증 로직을 재현하기
위한 테스트 하니스일 뿐이다). 나머지 게이트는 스크립트 코드를 한 글자도
바꾸지 않고 그대로 실행된다.

pytest 없이 이 저장소 관례([PASS]/[FAIL] 출력)를 따른다.

실행:
    python tests/test_phase28_apply_script_regression.py
(별도 DB 사본이나 서버 접속이 필요 없다 - 매번 새 임시 SQLite 파일을 만들고 끝나면 지운다)
"""
from __future__ import annotations

import importlib.util
import io
import json
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
APPLY_SCRIPT = REPO_ROOT / "scripts" / "vocab" / "phase28_apply_l6_pilot_quiz_items.py"
ROWS_JSON = REPO_ROOT / "data" / "import" / "schema_reading_phase28_l6_pilot_quiz_rows_20260927.json"
SCHEMA_SQL = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_schema.sql"

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    check(APPLY_SCRIPT.exists(), f"phase28 적용 스크립트 존재: {APPLY_SCRIPT}")
    check(ROWS_JSON.exists(), f"phase28 rows-json 존재: {ROWS_JSON}")
    check(SCHEMA_SQL.exists(), f"vocabulary_quiz 스키마 존재: {SCHEMA_SQL}")
    if not (APPLY_SCRIPT.exists() and ROWS_JSON.exists() and SCHEMA_SQL.exists()):
        return False

    rows = json.load(open(ROWS_JSON, encoding="utf-8"))
    check(len(rows) == 40, f"입력 rows 40건(실제 {len(rows)})")
    content_ids = sorted({r["source_content_id"] for r in rows})
    check(len(content_ids) == 20, f"입력이 참조하는 content_id 20개(실제 {len(content_ids)})")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "vocabulary_quiz_research.db"  # GATE 2 basename 하드가드 통과용
        env_file = Path(tmpdir) / ".env"
        env_file.write_text("APP_ENV=research\n", encoding="utf-8")

        conn = sqlite3.connect(str(db_path))
        conn.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))
        for cid in content_ids:
            lemma = next(r["lemma"] for r in rows if r["source_content_id"] == cid)
            conn.execute(
                "INSERT INTO vocabulary_contents (content_id, lemma, source_version, student_exposure, "
                "public_ready, is_active) VALUES (?, ?, 'schema_reading_literacy_l6_manual_v1', 0, 0, 1)",
                (cid, lemma),
            )
            conn.execute(
                "INSERT INTO vocabulary_content_levels (content_id, vocab_level, level_status, boundary_flag, "
                "level_version, is_active) VALUES (?, 6, 'REVIEW_BOUNDARY', 1, 'level_policy_v0.1', 1)",
                (cid,),
            )
        conn.commit()
        conn.close()
        check(True, f"격리 DB에 phase28이 요구하는 20개 content_id를 최소 조건(active/exposure=0/"
                     f"public=0/REVIEW_BOUNDARY)으로 채움: {db_path}")

        spec = importlib.util.spec_from_file_location("phase28_apply_regression_target", APPLY_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)  # 스크립트 소스를 한 글자도 바꾸지 않고 그대로 로드

        # GATE 1은 "실제 uvicorn 프로세스"를 요구하는데, 이 격리 DB를 가리키는 프로세스는
        # 당연히 없다 - GATE 3 이후(신규 삽입·커밋 후 검증)를 재현하는 것이 이 테스트의
        # 목적이므로, GATE 1의 프로세스 조회 두 헬퍼만 이 테스트 하니스 안에서 패치한다.
        mod.find_uvicorn_app_main_pid = lambda: 999999
        mod.read_process_environ = lambda pid, key: {
            "APP_ENV": "research", "VOCABULARY_QUIZ_DB_PATH": str(db_path),
        }.get(key)
        # GATE 3의 "적용 전 행수" 하드가드는 phase28 실서버 마이그레이션 시점의 정확한
        # 행수(5,902/1,329)를 상수로 박아 둔 것 - 이 격리 DB는 20개 content_id만 채웠으므로
        # 이 테스트가 재현하려는 GATE 3~6 로직(신규 삽입·커밋 후 검증)과 무관한 이 두
        # 상수만 격리 DB 규모에 맞게 패치한다(그 외 로직·게이트 순서는 전혀 바꾸지 않음).
        mod.EXPECTED_CONTENTS_COUNT = len(content_ids)
        mod.EXPECTED_TOTAL_BEFORE = 0
        mod.EXPECTED_TOTAL_AFTER = 40

        argv_backup = sys.argv
        sys.argv = ["phase28_apply_l6_pilot_quiz_items.py",
                    "--env-file", str(env_file), "--db-path", str(db_path), "--rows-json", str(ROWS_JSON)]
        exc_info = None
        exit_code = None
        try:
            exit_code = mod.main()
        except Exception:  # noqa: BLE001 - 정확히 phase28에서 재발했던 예외 유형을 잡아 보고
            exc_info = traceback.format_exc()
        finally:
            sys.argv = argv_backup

        check(exc_info is None,
              "최초 삽입(GATE 3~6 전체 경로)이 파이썬 예외 없이 끝남(phase28에서 재발했던 "
              "'Incorrect number of bindings supplied' 재발 없음)",
              exc_info or "")
        check(exit_code == 0, f"스크립트 종료 코드 0(실제 {exit_code})")

        # phase28에서 얻은 교훈의 핵심: 스크립트 출력/종료 코드만 믿지 않고 DB를 직접
        # 재조회해서 실제 상태를 재확인한다 - 설령 위에서 예외가 났더라도 이 블록은 그대로
        # 실행해 "그래도 커밋은 됐는지"를 스스로 판단한다.
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        post_count = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
        dup = conn.execute(
            "SELECT item_id, COUNT(*) c FROM vocabulary_multiformat_items GROUP BY item_id HAVING c > 1"
        ).fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchall()
        conn.close()
        check(post_count == 40, f"스크립트 출력과 무관하게 DB 직접 재조회로 40건 삽입 확인(실제 {post_count})")
        check(not dup, f"DB 직접 재조회로 item_id 중복 0건 확인(실제 {dup})")
        check(integrity == [("ok",)], f"DB 직접 재조회로 integrity_check=ok 확인(실제 {integrity})")

        # 멱등 재실행 - 이번엔 GATE 4에서 조기 return하는 경로(원래 버그가 있던 GATE 6에는
        # 도달하지 않음)도 함께 확인해 둔다.
        sys.argv = ["phase28_apply_l6_pilot_quiz_items.py",
                    "--env-file", str(env_file), "--db-path", str(db_path), "--rows-json", str(ROWS_JSON)]
        try:
            exit_code2 = mod.main()
            exc2 = None
        except Exception:  # noqa: BLE001
            exit_code2 = None
            exc2 = traceback.format_exc()
        finally:
            sys.argv = argv_backup
        check(exc2 is None and exit_code2 == 0, f"멱등 재실행도 예외 없이 종료 코드 0(실제 exc={exc2 is not None}, code={exit_code2})")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
