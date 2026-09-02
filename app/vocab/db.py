"""어휘 게임 DB(`data/vocab/idiom.db`) 전용 연결 설정.

app/literacy/db.py와 같은 패턴 - 완전히 분리된 자기 engine/Base/SessionLocal을
쓴다. app/vocab/ 아래 어떤 파일도 이 파일 밖의 다른 app 모듈을 import하지
않는다(momoai.kr 이관 시 app/vocab/ 디렉터리만 그대로 들어낼 수 있어야 하므로).
"""
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# app/vocab/db.py -> app/vocab -> app -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = REPO_ROOT / "data" / "vocab" / "idiom.db"

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


def get_vocab_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_path() -> Path:
    """배치 스크립트가 ORM 없이 raw sqlite3로 직접 열 때 쓸 경로."""
    return DB_PATH
