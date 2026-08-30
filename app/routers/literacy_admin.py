"""문해력 DB 관리자용 라우터 (동작 확인용 최소 엔드포인트만 - 검수 UI는 Phase 4).

GET /literacy/health -> terms 테이블 행 수
"""
from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.literacy.db import get_literacy_db
from app.literacy.models import Term

router = APIRouter(prefix="/literacy")


@router.get("/health")
def health(db: Session = Depends(get_literacy_db)):
    terms_count = db.query(func.count(Term.id)).scalar()
    return {"status": "ok", "terms_count": terms_count}
