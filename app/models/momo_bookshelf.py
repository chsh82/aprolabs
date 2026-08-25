"""모모의 책장 - 분기별/학년별 주차 커리큘럼 (엑셀 업로드로 채워지는 도서 목록)"""
from datetime import datetime
import uuid
from sqlalchemy import Column, String, Integer, Boolean, DateTime, UniqueConstraint
from app.database import Base

GRADES = [
    "초등 1학년", "초등 2학년", "초등 3학년", "초등 4학년", "초등 5학년", "초등 6학년",
    "중학교 1학년", "중학교 2학년", "중학교 3학년",
]


class MomoBookshelfWeek(Base):
    """연도 + 분기 + 학년 + 주차 단위의 커리큘럼 한 줄"""
    __tablename__ = "momo_bookshelf_weeks"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    year = Column(Integer, nullable=False, index=True)          # 2026
    quarter = Column(String(30), nullable=False, index=True)    # "1분기(고전)"
    grade = Column(String(20), nullable=False, index=True)      # "초등 3학년"
    week_number = Column(Integer, nullable=False)                # 1~13
    date_range = Column(String(50), nullable=True)               # "11/24 ~ 11/30" (원문 그대로)

    title = Column(String(300), nullable=False)
    author = Column(String(200), nullable=True)
    publisher = Column(String(200), nullable=True)
    is_holiday = Column(Boolean, default=False)                  # 저자가 "-"인 휴강/특강 표시 행

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("year", "quarter", "grade", "week_number", name="uq_momo_bookshelf_week"),
    )


class MomoRequiredBook(Base):
    """모모의 책장 필독서 - 커리큘럼(momo_bookshelf_weeks)에서 주차를 뺀 학년+분기별 도서 목록.
    "(연장)" 표시가 붙은 재당 주차는 원본 책과 합쳐져 하나로 존재함.
    ISBN은 자동 채움이 아니라 /isbn 검색 화면에서 사람이 확인하고 직접 연결함."""
    __tablename__ = "momo_required_books"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    year = Column(Integer, nullable=False, index=True)
    quarter = Column(String(30), nullable=False, index=True)
    grade = Column(String(20), nullable=False, index=True)

    title = Column(String(300), nullable=False)
    author = Column(String(200), nullable=True)
    publisher = Column(String(200), nullable=True)

    isbn13 = Column(String(20), nullable=True, index=True)
    isbn10 = Column(String(20), nullable=True)
    cover_url = Column(String(500), nullable=True)
    aladin_link = Column(String(500), nullable=True)
    is_auto_linked = Column(Boolean, default=False)  # 일괄 자동 연결로 채워졌는지 (사람이 확인 안 함, 스팟체크 유도용)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint("year", "quarter", "grade", "title", name="uq_momo_required_book"),
    )
