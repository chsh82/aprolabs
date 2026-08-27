"""작업일지 게시판 - 작업 내용/주요 사항을 기록해두는 개인용 노트"""
from datetime import datetime
import uuid
from sqlalchemy import Column, String, Text, DateTime
from app.database import Base

ENTRY_TYPES = ["작업일지", "주요사항"]


class JournalEntry(Base):
    __tablename__ = "journal_entries"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    entry_type = Column(String(20), nullable=False, default="작업일지")
    title = Column(String(300), nullable=False)
    content = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
