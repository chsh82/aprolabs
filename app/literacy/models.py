"""문해력 DB(`data/literacy.db`) SQLAlchemy 모델.

docs/literacy/01-스키마.md 5절 그대로. 기존 app/models/*.py의 Column(...)
선언 방식(Mapped[...] 신문법 아님)을 그대로 따른다.
"""
from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
)

from app.literacy.db import Base


class Term(Base):
    """표제어 통합 테이블 - 어휘/사자성어/속담/한자어/상식용어를 한 테이블에 모은다."""

    __tablename__ = "terms"

    id = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(Text, nullable=False, index=True)  # 어휘/사자성어/속담/한자어/상식용어
    headword = Column(Text, nullable=False, index=True)
    origin = Column(Text, nullable=True)
    definition = Column(Text, nullable=True)
    pos = Column(Text, nullable=True)
    sense_category = Column(Text, nullable=True, index=True)
    subject_category = Column(Text, nullable=True)
    grade_level = Column(Integer, nullable=True, index=True)  # 1~12 (초1=1 ... 고3=12) - 삭제하지 않음
    level = Column(Integer, nullable=True, index=True)  # 0~6 (초1~2=0 ... 고3=6) - docs/literacy/04-스키마리딩어휘.md
    grade_source = Column(Text, nullable=False, default="auto")  # auto/manual
    source = Column(Text, nullable=False)
    license = Column(Text, nullable=True)
    external_id = Column(Text, nullable=True)  # 원본 시스템 고유 ID (krdict target_code 등)
    collected_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=True)
    review_status = Column(Text, nullable=False, default="검수전", index=True)  # 검수전/검수완료/보류/제외
    note = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_terms_source_external_id"),
    )


class Example(Base):
    """용례."""

    __tablename__ = "examples"

    id = Column(Integer, primary_key=True, autoincrement=True)
    term_id = Column(Integer, ForeignKey("terms.id", ondelete="CASCADE"), nullable=False, index=True)
    sentence = Column(Text, nullable=False)
    source = Column(Text, nullable=True)


class Hanja(Base):
    """한자 낱글자."""

    __tablename__ = "hanja"

    id = Column(Integer, primary_key=True, autoincrement=True)
    character = Column(Text, nullable=False, unique=True)  # 한자 1자
    meaning_reading = Column(Text, nullable=True)  # 훈음 (예: 물 수)
    radical = Column(Text, nullable=True)
    stroke_count = Column(Integer, nullable=True)
    edu_level = Column(Text, nullable=True, index=True)  # 중/고 (한문 교육용 기초 한자 1800자 구분)
    hanja_grade = Column(Text, nullable=True, index=True)  # 한국어문회 급수 (8급~1급)


class TermHanja(Base):
    """어휘-한자 연결."""

    __tablename__ = "term_hanja"

    term_id = Column(Integer, ForeignKey("terms.id", ondelete="CASCADE"), nullable=False)
    hanja_id = Column(Integer, ForeignKey("hanja.id", ondelete="CASCADE"), nullable=False)
    position = Column(Integer, nullable=False)  # 표제어 내 한자 위치 (0부터)

    __table_args__ = (
        PrimaryKeyConstraint("term_id", "hanja_id", "position"),
    )


class QuizItem(Base):
    """생성된 문항."""

    __tablename__ = "quiz_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    term_id = Column(Integer, ForeignKey("terms.id"), nullable=False, index=True)
    quiz_type = Column(Text, nullable=False, index=True)  # 뜻풀이선택/빈칸채우기/유의어 등
    question = Column(Text, nullable=False)
    correct_answer = Column(Text, nullable=False)
    distractors = Column(Text, nullable=True)  # 오답 3개, JSON 배열 문자열
    difficulty = Column(Integer, nullable=True)
    generated_by = Column(Text, nullable=True)  # api/manual
    model = Column(Text, nullable=True)  # 생성에 사용한 모델명
    review_status = Column(Text, nullable=False, default="검수전", index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)


class CollectionRun(Base):
    """수집 이력."""

    __tablename__ = "collection_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    fetched_count = Column(Integer, nullable=True)
    inserted_count = Column(Integer, nullable=True)
    status = Column(Text, nullable=True)  # running/success/failed
    error = Column(Text, nullable=True)
