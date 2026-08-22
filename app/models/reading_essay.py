"""독서논술 교재 PDF 추출 결과 모델.

교재(ReadingMaterial) 1건 = 학생용+교사용 PDF 한 쌍(또는 교사용 단독, B/C
계열처럼 학생용 diff가 필요 없는 경우).
템플릿 계열(A1/A2/B/C)에 따라 어휘/OX/글쓰기 유무나 발문-답변 구조가
다르므로, 하위 테이블 컬럼은 전부 nullable + 일부는 JSON으로 유연하게 둔다.
"""
from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base
import uuid

TEMPLATE_FAMILIES = ["A1", "A2", "B", "C"]

# 파이프라인 상태: pending(스캔 대상 등록) -> extracted(파싱 완료) ->
# reviewed(검수 완료) / error(파싱 실패)
STATUSES = ["pending", "extracted", "reviewed", "error"]


class ReadingMaterial(Base):
    """독서논술 교재 1건 (학년 + 분기 + 주차 + 제목 단위)"""
    __tablename__ = "reading_materials"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    quarter = Column(String(20), nullable=True, index=True)   # "26년3분기"
    grade = Column(String(10), nullable=True, index=True)     # "초3"
    week = Column(String(20), nullable=True)                  # "3분기 10주차"
    title = Column(String(300), nullable=True)
    level = Column(String(10), nullable=True)                 # "LV3"
    author = Column(String(100), nullable=True)
    cover_quotes = Column(JSON, nullable=True)

    template_family = Column(String(10), nullable=True, index=True)  # A1/A2/B/C

    # 알라딘 ISBN 검색으로 연결한 실제 출판 도서 서지정보 (선택, /isbn 에서 연결)
    book_isbn13 = Column(String(20), nullable=True, index=True)
    book_isbn10 = Column(String(20), nullable=True)
    book_publisher = Column(String(200), nullable=True)
    book_pub_date = Column(String(20), nullable=True)
    book_cover_url = Column(String(500), nullable=True)
    book_aladin_link = Column(String(500), nullable=True)

    student_pdf_path = Column(String(500), nullable=True)
    teacher_pdf_path = Column(String(500), nullable=True)

    status = Column(String(20), default="pending", index=True)
    error_message = Column(Text, nullable=True)

    # extract() 원본 결과 전체 보존 (계열별로 모양이 다름 - 디버깅/재처리용)
    raw_extraction = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    vocabulary_items = relationship(
        "VocabularyItem", back_populates="material",
        cascade="all, delete-orphan", order_by="VocabularyItem.order_index")
    ox_items = relationship(
        "OXQuizItem", back_populates="material",
        cascade="all, delete-orphan", order_by="OXQuizItem.order_index")
    discussion_items = relationship(
        "DiscussionQuestion", back_populates="material",
        cascade="all, delete-orphan", order_by="DiscussionQuestion.order_index")
    writing_prompt = relationship(
        "WritingPrompt", back_populates="material",
        uselist=False, cascade="all, delete-orphan")


class VocabularyItem(Base):
    """어휘 항목 (표 형식 word/definition 또는 A2 매칭게임)"""
    __tablename__ = "reading_vocabulary"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    material_id = Column(String(36), ForeignKey("reading_materials.id"), nullable=False, index=True)

    word = Column(String(100), nullable=True)
    page = Column(Integer, nullable=True)
    definition = Column(Text, nullable=True)
    matched_definition_no = Column(Integer, nullable=True)  # A2 선잇기 매칭게임 전용
    order_index = Column(Integer, default=0)

    material = relationship("ReadingMaterial", back_populates="vocabulary_items")


class OXQuizItem(Base):
    """O.X 퀴즈 항목"""
    __tablename__ = "reading_ox_items"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    material_id = Column(String(36), ForeignKey("reading_materials.id"), nullable=False, index=True)

    statement = Column(Text, nullable=True)
    page = Column(Integer, nullable=True)
    answer = Column(String(5), nullable=True)          # "○" / "X"
    wrong_explanation = Column(Text, nullable=True)
    order_index = Column(Integer, default=0)

    material = relationship("ReadingMaterial", back_populates="ox_items")


class DiscussionQuestion(Base):
    """질문과 토론 / 심화 문제 문항 (계열별 공통 정규화 형태)"""
    __tablename__ = "reading_discussion_questions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    material_id = Column(String(36), ForeignKey("reading_materials.id"), nullable=False, index=True)

    work_title = Column(String(300), nullable=True)     # 다중 작품 구성 교재의 작품명 (B/C)
    question_no = Column(String(20), nullable=True)
    reading_type = Column(String(50), nullable=True)    # 독해유형 (A1/C만 존재)
    quotes = Column(JSON, nullable=True)                # 인용문 리스트 [{text, page_ref}] 또는 문자열 리스트
    question_text = Column(Text, nullable=True)

    label = Column(Text, nullable=True)
    model_answer = Column(Text, nullable=True)
    sub_answers = Column(JSON, nullable=True)           # [{label, answer}] - 비교표형
    raw_tail = Column(Text, nullable=True)              # 구조화 실패 시 원문 그대로 (A2/B 한계 대응)

    order_index = Column(Integer, default=0)

    material = relationship("ReadingMaterial", back_populates="discussion_items")


class WritingPrompt(Base):
    """글쓰기 프롬프트 (교재당 1건)"""
    __tablename__ = "reading_writing_prompts"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    material_id = Column(String(36), ForeignKey("reading_materials.id"), nullable=False, unique=True)

    theme = Column(Text, nullable=True)
    steps = Column(JSON, nullable=True)   # [{step, text}]

    material = relationship("ReadingMaterial", back_populates="writing_prompt")
