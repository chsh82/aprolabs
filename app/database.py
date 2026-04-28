from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = "sqlite:///./aprolabs.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from app.models import question  # noqa: F401
    Base.metadata.create_all(bind=engine)
    # 기존 DB에 신규 컬럼 안전하게 추가 (SQLite ALTER TABLE은 IF NOT EXISTS 미지원)
    _safe_add_columns()


def _safe_add_columns():
    migrations = [
        ("pipeline_jobs", "sub_type", "VARCHAR(20)"),
    ]
    with engine.connect() as conn:
        for table, col, col_type in migrations:
            try:
                conn.execute(__import__("sqlalchemy").text(
                    f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"
                ))
                conn.commit()
            except Exception:
                pass  # 이미 존재하면 무시
