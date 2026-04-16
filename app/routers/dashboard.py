from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta

from app.database import get_db
from app.models.question import Question, CATEGORIES, SUBCATEGORIES
from app.models.api_usage import ApiUsage

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _base_ctx(db: Session, **kwargs):
    counts = {}
    for cat in CATEGORIES:
        counts[cat] = db.query(Question).filter(Question.category == cat).count()
    counts["전체"] = db.query(Question).count()
    sub_counts = {}
    for cat, subs in SUBCATEGORIES.items():
        for sub in subs:
            sub_counts[f"{cat}:{sub}"] = (
                db.query(Question)
                .filter(Question.category == cat, Question.subcategory == sub)
                .count()
            )
    return {"categories": CATEGORIES, "subcategories": SUBCATEGORIES,
            "category_counts": counts, "sub_counts": sub_counts, **kwargs}


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    # 전체 합계
    total_cost = db.query(func.sum(ApiUsage.cost_usd)).scalar() or 0.0
    total_calls = db.query(func.count(ApiUsage.id)).scalar() or 0
    total_input = db.query(func.sum(ApiUsage.input_tokens)).scalar() or 0
    total_output = db.query(func.sum(ApiUsage.output_tokens)).scalar() or 0

    # 서비스별
    by_service = db.query(
        ApiUsage.service,
        func.count(ApiUsage.id).label("calls"),
        func.sum(ApiUsage.input_tokens).label("input_tokens"),
        func.sum(ApiUsage.output_tokens).label("output_tokens"),
        func.sum(ApiUsage.cost_usd).label("cost"),
    ).group_by(ApiUsage.service).all()

    # 용도별
    by_purpose = db.query(
        ApiUsage.purpose,
        ApiUsage.service,
        func.count(ApiUsage.id).label("calls"),
        func.sum(ApiUsage.cost_usd).label("cost"),
    ).group_by(ApiUsage.purpose, ApiUsage.service).all()

    # 일별 (최근 30일)
    since = datetime.now() - timedelta(days=30)
    daily_rows = db.query(
        func.strftime("%Y-%m-%d", ApiUsage.created_at).label("day"),
        func.sum(ApiUsage.cost_usd).label("cost"),
        func.count(ApiUsage.id).label("calls"),
    ).filter(ApiUsage.created_at >= since).group_by("day").order_by("day").all()

    # 최근 20건
    recent = db.query(ApiUsage).order_by(ApiUsage.created_at.desc()).limit(20).all()

    ctx = _base_ctx(db,
        request=request,
        total_cost=total_cost,
        total_calls=total_calls,
        total_input=total_input,
        total_output=total_output,
        by_service=by_service,
        by_purpose=by_purpose,
        daily_rows=daily_rows,
        recent=recent,
    )
    return templates.TemplateResponse("dashboard.html", ctx)
