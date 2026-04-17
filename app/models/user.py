from datetime import datetime
from sqlalchemy import Column, String, Boolean, DateTime
from app.database import Base
import uuid


class User(Base):
    __tablename__ = "users"

    id         = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email      = Column(String, unique=True, nullable=False, index=True)
    hashed_pw  = Column(String, nullable=False)
    is_admin   = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)
