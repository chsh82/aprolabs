from datetime import datetime
from sqlalchemy import Column, String, Integer, Float, DateTime, Text
from app.database import Base
import uuid

# 모델별 토큰 단가 (USD / 1M tokens)
PRICING = {
    "gemini-2.0-flash":   {"input": 0.075,  "output": 0.30},
    "claude-sonnet-4-6":  {"input": 3.0,    "output": 15.0},
}


class ApiUsage(Base):
    __tablename__ = "api_usage"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    service = Column(String(20))          # gemini / claude
    model = Column(String(50))
    purpose = Column(String(100))         # ocr / segment / tag / classify
    job_id = Column(String(36), nullable=True)

    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    cost_usd = Column(Float, default=0.0)

    created_at = Column(DateTime, default=datetime.now)


def calc_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    price = PRICING.get(model, {"input": 0, "output": 0})
    return (input_tokens * price["input"] + output_tokens * price["output"]) / 1_000_000
