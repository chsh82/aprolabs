import os
import uuid
import aiofiles
from fastapi import APIRouter, Depends, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.question import Question, CATEGORIES, SUBCATEGORIES
from app.services.ocr import extract_text_from_image
from app.services.classifier import classify_question

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
UPLOAD_DIR = "uploads"


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

    return {
        "categories": CATEGORIES,
        "subcategories": SUBCATEGORIES,
        "category_counts": counts,
        "sub_counts": sub_counts,
        **kwargs,
    }


@router.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, db: Session = Depends(get_db),
                preset_category: str = "", preset_subcategory: str = ""):
    ctx = _base_ctx(db, request=request,
                    preset_category=preset_category,
                    preset_subcategory=preset_subcategory)
    return templates.TemplateResponse("upload.html", ctx)


@router.post("/upload")
async def upload(
    request: Request,
    db: Session = Depends(get_db),
    file: UploadFile = File(None),
    manual_text: str = Form(""),
    category: str = Form("기타"),
    subcategory: str = Form(""),
    source: str = Form(""),
    source_year: str = Form(""),
):
    content = ""
    image_path = None

    if file and file.filename:
        ext = os.path.splitext(file.filename)[1].lower()
        filename = f"{uuid.uuid4()}{ext}"
        image_path = os.path.join(UPLOAD_DIR, filename)
        async with aiofiles.open(image_path, "wb") as f:
            await f.write(await file.read())
        content = await extract_text_from_image(image_path)
    elif manual_text.strip():
        content = manual_text.strip()
    else:
        ctx = _base_ctx(db, request=request,
                        error="이미지 또는 텍스트를 입력해주세요.",
                        preset_category=category,
                        preset_subcategory=subcategory)
        return templates.TemplateResponse("upload.html", ctx)

    classification = await classify_question(content, category)

    q = Question(
        category=category,
        subcategory=subcategory or None,
        content=content,
        image_path=image_path,
        subject=classification.get("subject"),
        unit=classification.get("unit"),
        topic=classification.get("topic"),
        difficulty=classification.get("difficulty"),
        question_type=classification.get("question_type"),
        tags=classification.get("tags", []),
        answer=classification.get("answer_hint"),
        source=source or None,
        source_year=int(source_year) if source_year.isdigit() else None,
    )
    db.add(q)
    db.commit()
    db.refresh(q)

    return RedirectResponse(url=f"/questions/{q.id}", status_code=303)
