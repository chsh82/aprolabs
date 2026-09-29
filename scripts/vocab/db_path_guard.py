# -*- coding: utf-8 -*-
"""국립국어원 등급 참조/판정 스크립트 전용 DB 경로 가드.

2026-09-29 실수 재발 방지: VOCABULARY_QUIZ_DB_PATH를 빠뜨렸을 때
app.vocabulary_quiz.db.get_db_path()가 조용히 로컬 R&D DB
(data/vocab/vocabulary_quiz_rnd.db)로 폴백해, 의도와 다른 DB에
테이블이 생기는 사고가 실제로 있었다(즉시 발견·복구했지만 구조적으로
막아야 한다는 결론).

이 모듈의 guard_db_path()는 어떤 sqlite3.connect()도 호출되기 전에
아래 4가지 중 하나라도 걸리면 즉시 실패(RuntimeError)한다:
  1. APP_ENV가 정확히 'research'가 아님
  2. VOCABULARY_QUIZ_DB_PATH 환경변수가 비어 있음(미설정)
  3. --database 인자가 주어졌는데, 정규화(resolve)한 절대경로가
     VOCABULARY_QUIZ_DB_PATH를 정규화한 경로와 다름
  4. 대상 DB 파일이 실제로 존재하지 않음(Path.is_file()가 False)

통과하면, 실제 쓰기 연결은 반드시 connect_rw()로 열어야 한다 -
mode=rw로 열기 때문에 SQLite가 없는 파일을 암묵적으로 새로 만들 수
없다(mode=rwc가 아님 - 가드를 어떻게든 우회해도 이 두 번째 방어선이
남는다).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path


class DbPathGuardError(RuntimeError):
    pass


def guard_db_path(database_arg: str | None = None, *, require_research_env: bool = True) -> str:
    """가드 전부 통과 시 검증된 DB 경로 문자열을 반환한다. 실패 시 예외."""
    if require_research_env:
        app_env = os.environ.get("APP_ENV", "")
        if app_env != "research":
            raise DbPathGuardError(
                f"GUARD FAIL(1/4): APP_ENV={app_env!r} - 이 스크립트는 APP_ENV=research에서만 실행 가능합니다"
            )

    env_val = os.environ.get("VOCABULARY_QUIZ_DB_PATH", "")
    if not env_val:
        raise DbPathGuardError(
            "GUARD FAIL(2/4): VOCABULARY_QUIZ_DB_PATH 환경변수가 설정돼 있지 않습니다 - "
            "폴백 DB(vocabulary_quiz_rnd.db)로 조용히 넘어가는 것을 막기 위해 명시적 설정을 요구합니다"
        )

    env_resolved = Path(env_val).resolve()

    if database_arg:
        arg_resolved = Path(database_arg).resolve()
        if arg_resolved != env_resolved:
            raise DbPathGuardError(
                f"GUARD FAIL(3/4): --database({arg_resolved})가 VOCABULARY_QUIZ_DB_PATH({env_resolved})와 "
                "다른 경로를 가리킵니다 - 둘을 일치시키거나 하나만 쓰세요"
            )

    if not env_resolved.is_file():
        raise DbPathGuardError(
            f"GUARD FAIL(4/4): 대상 DB 파일이 존재하지 않습니다: {env_resolved} - "
            "SQLite가 이 경로에 새 파일을 암묵적으로 만들지 못하도록 사전에 차단합니다"
        )

    return str(env_resolved)


def connect_rw(db_path: str) -> sqlite3.Connection:
    """mode=rw로만 연다 - 파일이 없으면 sqlite3.OperationalError로 즉시 실패한다
    (mode=rwc가 아니므로 암묵적 신규 생성이 원천적으로 불가능하다 - 2차 방어선)."""
    uri = f"file:{Path(db_path).as_posix()}?mode=rw"
    return sqlite3.connect(uri, uri=True)


def connect_ro(db_path: str) -> sqlite3.Connection:
    uri = f"file:{Path(db_path).as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)
