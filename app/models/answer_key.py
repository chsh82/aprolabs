from datetime import datetime
from sqlalchemy import Column, String, Text, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base
import uuid


class AnswerKey(Base):
    """정답/해설 파일 단위"""
    __tablename__ = "answer_keys"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    filename = Column(String(500), nullable=False)
    file_path = Column(String(500), nullable=True)

    # 시험 메타 (업로드 시 입력)
    source_year = Column(Integer, nullable=True)
    exam_type = Column(String(100), nullable=True)
    subject = Column(String(50), nullable=True)

    # 매칭 상태
    paper_code = Column(String(100), nullable=True)   # 매칭된 문제지 코드
    status = Column(String(30), default="unmatched")  # unmatched / matched

    created_at = Column(DateTime, default=datetime.now)

    items = relationship("AnswerKeyItem", back_populates="key",
                         cascade="all, delete-orphan", order_by="AnswerKeyItem.question_number")


class AnswerKeyItem(Base):
    """문항별 정답·해설"""
    __tablename__ = "answer_key_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(String(36), ForeignKey("answer_keys.id", ondelete="CASCADE"))
    question_number = Column(Integer, nullable=False)
    answer = Column(String(20), nullable=True)
    passage_explanation = Column(Text, nullable=True)   # 지문해설 (같은 지문 문항 공유)
    explanation = Column(Text, nullable=True)           # 문항해설 (개별)

    key = relationship("AnswerKey", back_populates="items")
