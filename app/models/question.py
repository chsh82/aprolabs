from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, Float, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base
from app.models.passage import ExamPaper  # noqa: ensure table is registered
import uuid

CATEGORIES = ["독서논술", "국어", "사회", "과학", "기타"]

# 카테고리별 소메뉴 (subcategory)
SUBCATEGORIES: dict = {
    "국어": ["수능국어DB"],
}

THINKING_TYPES = [
    "이항대립", "전제추론", "유추", "인과관계", "비교대조",
    "사례적용", "관점분석", "논거평가", "구조파악"
]


class Question(Base):
    __tablename__ = "questions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # 대분류 / 소분류
    category = Column(String(50), nullable=True, default="기타")
    subcategory = Column(String(100), nullable=True)

    # 문제지 연결
    paper_code = Column(String(100), ForeignKey("exam_papers.paper_code"), nullable=True)
    paper = relationship("ExamPaper", back_populates="questions",
                         foreign_keys=[paper_code])

    # 수능국어DB 전용: 지문 연결
    passage_id = Column(String(36), ForeignKey("passages.id"), nullable=True)
    passage = relationship("Passage", back_populates="questions", viewonly=True)

    # 수능국어DB 전용: 문항 구조
    question_number = Column(Integer, nullable=True)   # 시험지 문항 번호
    stem = Column(Text, nullable=True)                 # 발문
    choices = Column(JSON, nullable=True)              # {1:"①...", 2:"②...", ...}
    thinking_types = Column(JSON, nullable=True)       # ["이항대립", ...]
    status = Column(String(20), default="approved")    # pending/reviewed/approved

    # 문항 내용 (기존)
    content = Column(Text, nullable=False)
    answer = Column(Text, nullable=True)
    explanation = Column(Text, nullable=True)

    # 분류
    subject = Column(String(50), nullable=True)
    unit = Column(String(100), nullable=True)
    topic = Column(String(100), nullable=True)
    difficulty = Column(String(20), nullable=True)
    question_type = Column(String(50), nullable=True)

    # 출처
    source = Column(String(200), nullable=True)
    source_year = Column(Integer, nullable=True)
    source_number = Column(Integer, nullable=True)

    # 파일
    image_path = Column(String(500), nullable=True)

    # 태그
    tags = Column(JSON, nullable=True, default=list)

    # 메타
    memo = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
