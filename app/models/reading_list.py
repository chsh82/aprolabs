"""ISBN 검색 결과를 모아두는 독서 리스트 (특정 교재와 무관한 범용 도서 컬렉션)"""
from datetime import datetime
import uuid
from sqlalchemy import Column, String, Text, DateTime
from app.database import Base


class ReadingListBook(Base):
    __tablename__ = "reading_list_books"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    isbn13 = Column(String(20), nullable=True, index=True)
    isbn10 = Column(String(20), nullable=True)
    title = Column(String(300), nullable=False)
    author = Column(String(300), nullable=True)
    publisher = Column(String(200), nullable=True)
    pub_date = Column(String(20), nullable=True)
    cover_url = Column(String(500), nullable=True)
    aladin_link = Column(String(500), nullable=True)

    created_at = Column(DateTime, default=datetime.now)
