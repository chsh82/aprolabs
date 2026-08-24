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
