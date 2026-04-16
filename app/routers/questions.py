from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.database import get_db
from app.models.question import Question, CATEGORIES, SUBCATEGORIES

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _base_ctx(db: Session, **kwargs):
    """모든 템플릿에 공통으로 넘길 컨텍스트"""
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

    return {
        "categories": CATEGORIES,
        "subcategories": SUBCATEGORIES,
        "category_counts": counts,
        "sub_counts": sub_counts,
        **kwargs,
    }


@router.get("/", response_class=HTMLResponse)
def index(request: Request, db: Session = Depends(get_db),
          category: str = "", subcategory: str = "",
          difficulty: str = "", q: str = ""):
    query = db.query(Question)
    if category:
        query = query.filter(Question.category == category)
    if subcategory:
        query = query.filter(Question.subcategory == subcategory)
    if difficulty:
        query = query.filter(Question.difficulty == difficulty)
    if q:
        query = query.filter(or_(
            Question.content.contains(q),
            Question.topic.contains(q),
            Question.unit.contains(q),
            Question.source.contains(q),
        ))
    questions = query.order_by(Question.created_at.desc()).all()

    if subcategory:
        page_title = subcategory
    elif category:
        page_title = f"{category} 문항"
    else:
        page_title = "전체 문항"

    ctx = _base_ctx(db,
        request=request,
        questions=questions,
        filter_category=category,
        filter_subcategory=subcategory,
        filter_difficulty=difficulty,
        filter_q=q,
        total=len(questions),
        page_title=page_title,
    )
    return templates.TemplateResponse("index.html", ctx)


@router.get("/questions/{question_id}", response_class=HTMLResponse)
def detail(request: Request, question_id: str, db: Session = Depends(get_db)):
    q = db.query(Question).filter(Question.id == question_id).first()
    if not q:
        raise HTTPException(status_code=404)
    ctx = _base_ctx(db, request=request, q=q)
    return templates.TemplateResponse("detail.html", ctx)


@router.post("/questions/{question_id}/edit")
def edit(question_id: str, db: Session = Depends(get_db),
         category: str = Form("기타"),
         subcategory: str = Form(""),
         content: str = Form(""), answer: str = Form(""),
         explanation: str = Form(""), subject: str = Form(""),
         unit: str = Form(""), topic: str = Form(""),
         difficulty: str = Form(""), question_type: str = Form(""),
         source: str = Form(""), source_year: str = Form(""),
         source_number: str = Form(""), memo: str = Form("")):
    q = db.query(Question).filter(Question.id == question_id).first()
    if not q:
        raise HTTPException(status_code=404)
    q.category = category or "기타"
    q.subcategory = subcategory or None
    q.content = content
    q.answer = answer or None
    q.explanation = explanation or None
    q.subject = subject or None
    q.unit = unit or None
    q.topic = topic or None
    q.difficulty = difficulty or None
    q.question_type = question_type or None
    q.source = source or None
    q.source_year = int(source_year) if source_year.isdigit() else None
    q.source_number = int(source_number) if source_number.isdigit() else None
    q.memo = memo or None
    db.commit()
    return RedirectResponse(url=f"/questions/{question_id}", status_code=303)


@router.post("/questions/{question_id}/delete")
def delete(question_id: str, db: Session = Depends(get_db),
           redirect_to: str = Form("/")):
    q = db.query(Question).filter(Question.id == question_id).first()
    if q:
        db.delete(q)
        db.commit()
    return RedirectResponse(url=redirect_to, status_code=303)


@router.post("/questions/bulk-delete")
async def bulk_delete(request: Request, db: Session = Depends(get_db)):
    data = await request.json()
    ids = data.get("ids", [])
    if ids:
        db.query(Question).filter(Question.id.in_(ids)).delete(synchronize_session=False)
        db.commit()
    return JSONResponse({"ok": True, "deleted": len(ids)})
