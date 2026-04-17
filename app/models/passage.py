from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, Float, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base
import uuid


class ExamPaper(Base):
    """문제지 식별 단위 (year + exam_type + subject → paper_code)"""
    __tablename__ = "exam_papers"

    paper_code = Column(String(100), primary_key=True)   # e.g. "2025-수능-국어"
    source_year = Column(Integer, nullable=True)
    exam_type = Column(String(100), nullable=True)
    subject = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.now)

    passages = relationship("Passage", back_populates="paper",
                            foreign_keys="Passage.paper_code")
    questions = relationship("Question", back_populates="paper",
                             foreign_keys="Question.paper_code")


class Passage(Base):
    """지문 (1:N → questions)"""
    __tablename__ = "passages"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # 문제지 연결
    paper_code = Column(String(100), ForeignKey("exam_papers.paper_code"), nullable=True)
    paper_seq = Column(Integer, nullable=True)          # 문제지 내 순번 (1, 2, ...)
    paper = relationship("ExamPaper", back_populates="passages",
                         foreign_keys=[paper_code])

    category = Column(String(50), nullable=True, default="국어")
    source = Column(String(200), nullable=True)       # 예: "2024 수능"
    source_year = Column(Integer, nullable=True)
    exam_type = Column(String(100), nullable=True)    # 수능 / 6월 모평 / 9월 모평 / 학력평가 등
    subject = Column(String(50), nullable=True)       # 국어 / 영어 / 수학 등
    question_range = Column(String(50), nullable=True)  # "1~3", "4~6"

    content = Column(Text, nullable=True)             # 지문 본문
    image_paths = Column(JSON, nullable=True)         # 삽화 이미지 경로 목록

    complexity_score = Column(Float, nullable=True)   # 텍스트 복잡도 0.0~1.0
    concepts = Column(JSON, nullable=True)            # ["경제", "법률", ...]

    created_at = Column(DateTime, default=datetime.now)

    questions = relationship("Question", back_populates="passage",
                             foreign_keys="Question.passage_id",
                             viewonly=True)


class PipelineJob(Base):
    """PDF 파이프라인 작업 추적"""
    __tablename__ = "pipeline_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_number = Column(Integer, nullable=True)  # 순차 고유 번호

    filename = Column(String(500), nullable=False)
    file_path = Column(String(500), nullable=True)    # 업로드된 PDF 경로
    page_image_paths = Column(JSON, nullable=True)    # 페이지별 이미지 경로

    # 파이프라인 상태
    # uploading → parsing → segmenting → tagging → reviewing → done / error
    status = Column(String(50), default="uploading")
    error_message = Column(Text, nullable=True)

    # 메타
    source = Column(String(200), nullable=True)
    source_year = Column(Integer, nullable=True)
    exam_type = Column(String(100), nullable=True)
    subject = Column(String(50), nullable=True, default="국어")
    grade = Column(String(10), nullable=True)   # 고1 / 고2 / 고3

    # 정답/해설 파일
    answer_file_path = Column(String(500), nullable=True)

    # AI 파싱 결과 (원본 JSON)
    raw_result = Column(JSON, nullable=True)

    # 세그멘테이션 결과 (편집 가능한 구조)
    segments = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
