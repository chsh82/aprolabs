"""알라딘 ISBN 검색 결과 캐시 (API 호출량 절약용)"""
from datetime import datetime
from sqlalchemy import Column, String, Text, DateTime
from app.database import Base

CACHE_TTL_DAYS = 7


class IsbnSearchCache(Base):
    __tablename__ = "isbn_search_cache"

    cache_key = Column(String(300), primary_key=True)  # f"{title}|{author}|{limit}"
    response_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
