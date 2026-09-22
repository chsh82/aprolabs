"""일반 어휘 퀴즈 DB(`data/vocab/vocabulary_quiz_rnd.db`) 전용 연결 설정.

`app/vocab/db.py`(idiom.db)와 같은 격리 원칙 - 완전히 별도 engine/Base/
SessionLocal을 쓴다. 사자성어(idiom.db)와 콘텐츠 도메인이 다르고, 이번
작업은 로컬 R&D 전용이라 파일명에도 `_rnd`를 명시한다(운영 승격 시
별도 파일/경로로 다시 설계한다 - 이 파일을 그대로 운영에 올리지 않는다).

VOCABULARY_QUIZ_DB_PATH 환경변수로 경로를 지정한다 - 로컬은 저장소 안
기본 경로로 폴백하지만(개발 편의), 연구용 서버·운영에서는 배포 디렉터리
바깥의 영구 데이터 경로를 반드시 이 환경변수로 명시해야 한다. 코드 안에
서버 절대경로를 하드코딩하지 않는다.

get_db_path()는 매번 환경변수를 새로 읽는다(모듈 로드 시점에 고정하지
않음) - import_vocabulary_quiz.py 같은 스크립트/테스트가 같은 프로세스
안에서 환경변수를 바꿔가며 여러 DB를 다뤄야 할 수 있기 때문이다. 반면
engine/SessionLocal(FastAPI 앱이 쓰는 쪽)은 프로세스 시작 시점의 경로에
고정된다 - 서버 프로세스는 어차피 환경변수가 바뀌면 재시작되므로 이걸로
충분하고, 매 요청마다 engine을 다시 만들 필요가 없다.
"""
import os
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# app/vocabulary_quiz/db.py -> app/vocabulary_quiz -> app -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DB_PATH = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_rnd.db"
SCHEMA_PATH = REPO_ROOT / "data" / "vocab" / "vocabulary_quiz_schema.sql"


def _resolve_db_path() -> Path:
    env_val = os.environ.get("VOCABULARY_QUIZ_DB_PATH")
    return Path(env_val) if env_val else _DEFAULT_DB_PATH


DB_PATH = _resolve_db_path()  # FastAPI 앱(engine/SessionLocal)이 쓰는, 프로세스 시작 시점에 고정된 경로
DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})


@event.listens_for(engine, "connect")
def _enable_foreign_keys(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_vocabulary_quiz_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_path() -> Path:
    """배치 스크립트가 ORM 없이 raw sqlite3로 직접 열 때 쓸 경로 - 호출 시점의
    VOCABULARY_QUIZ_DB_PATH를 그대로 반영한다(모듈 로드 시점에 고정된 DB_PATH와
    다름)."""
    return _resolve_db_path()


def ensure_schema() -> None:
    """DB 파일이 없으면 vocabulary_quiz_schema.sql로 새로 만든다. 이미 있으면
    아무것도 안 한다(idempotent) - 기존 데이터를 절대 건드리지 않는다."""
    import sqlite3

    db_path = get_db_path()
    if db_path.exists():
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()
