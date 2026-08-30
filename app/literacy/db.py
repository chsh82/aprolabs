"""문해력 DB(`data/literacy.db`) 전용 연결 설정.

기존 app/database.py(aprolabs.db)와 완전히 분리된 별도 engine/SessionLocal을
쓴다 - 기존 get_db를 건드리지 않는다.
"""
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# app/literacy/db.py -> app/literacy -> app -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = REPO_ROOT / "data" / "literacy.db"

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


def get_literacy_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_path() -> Path:
    """배치 스크립트가 ORM 없이 raw sqlite3로 직접 열 때 쓸 경로."""
    return DB_PATH
