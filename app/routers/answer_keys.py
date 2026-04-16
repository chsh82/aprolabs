"""
정답 DB 라우터
GET  /suneung/answer-keys              → 목록
POST /suneung/answer-keys/upload       → PDF 업로드 + 파싱 후 저장
POST /suneung/answer-keys/{key_id}/delete → 삭제
GET  /suneung/answer-keys/{key_id}     → 상세
POST /suneung/answer-keys/{key_id}/save  → 문항별 정답 저장 (JSON)
POST /suneung/answer-keys/{key_id}/match → 문제지 매칭
"""
import os
import re
import uuid

from fastapi import APIRouter, Depends, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
import aiofiles

from app.database import get_db
from app.models.answer_key import AnswerKey, AnswerKeyItem
from app.models.question import Question, CATEGORIES, SUBCATEGORIES
from app.services.layout_analyzer import extract_pdf_text

router = APIRouter(prefix="/suneung")
templates = Jinja2Templates(directory="app/templates")
UPLOAD_DIR = "uploads/suneung"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ─────────────────────────────────────────
# 공통 컨텍스트
# ─────────────────────────────────────────

def _base_ctx(db: Session, **kwargs):
    counts = {cat: db.query(Question).filter(Question.category == cat).count()
              for cat in CATEGORIES}
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


# ─────────────────────────────────────────
# 정답 PDF 파싱
# ─────────────────────────────────────────

def _parse_answer_key_pdf(pdf_path: str) -> list[dict]:
    """
    정답 PDF → [{question_number, answer, explanation}, ...]
    """
    try:
        raw_text, _, _ = extract_pdf_text(pdf_path)
    except Exception:
        return []

    ANS_RE = re.compile(
        r'^[\s]*(\d{1,2})\s*[번\.\)\-\s]\s*([①②③④⑤])',
        re.MULTILINE,
    )
    TABLE_RE = re.compile(r'([①②③④⑤])')

    matches = list(ANS_RE.finditer(raw_text))
    items = []

    if matches:
        for i, m in enumerate(matches):
            num = int(m.group(1))
            ans = m.group(2)
            if not (1 <= num <= 45):
                continue

            exp_start = m.end()
            exp_end = matches[i + 1].start() if i + 1 < len(matches) else len(raw_text)
            exp_raw = raw_text[exp_start:exp_end].strip()
            exp_clean = re.sub(r'\s+', ' ', exp_raw).strip()

            items.append({
                "question_number": num,
                "answer": ans,
                "explanation": exp_clean if len(exp_clean) > 20 else None,
            })
    else:
        # 폴백: 원문자 순서대로 1번부터
        circles = TABLE_RE.findall(raw_text[:500])
        for i, c in enumerate(circles, 1):
            if i <= 45:
                items.append({
                    "question_number": i,
                    "answer": c,
                    "explanation": None,
                })

    return items


# ─────────────────────────────────────────
# 목록
# ─────────────────────────────────────────

@router.get("/answer-keys", response_class=HTMLResponse)
def answer_keys_list(request: Request, db: Session = Depends(get_db)):
    keys = db.query(AnswerKey).order_by(AnswerKey.created_at.desc()).all()
    ctx = _base_ctx(db, request=request, answer_keys=keys)
    return templates.TemplateResponse("suneung/answer_keys.html", ctx)


# ─────────────────────────────────────────
# 업로드
# ─────────────────────────────────────────

@router.post("/answer-keys/upload")
async def answer_keys_upload(
    request: Request,
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    source_year: str = Form(""),
    exam_type: str = Form("수능"),
    subject: str = Form("국어"),
):
    if not file or not file.filename:
        return RedirectResponse(url="/suneung/answer-keys", status_code=303)

    key_id = str(uuid.uuid4())
    ext = os.path.splitext(file.filename)[1].lower()
    file_path = os.path.join(UPLOAD_DIR, f"{key_id}_answer{ext}")

    async with aiofiles.open(file_path, "wb") as f:
        await f.write(await file.read())

    key = AnswerKey(
        id=key_id,
        filename=file.filename,
        file_path=file_path,
        source_year=int(source_year) if source_year.isdigit() else None,
        exam_type=exam_type or None,
        subject=subject or None,
        status="unmatched",
    )
    db.add(key)
    db.flush()

    parsed = _parse_answer_key_pdf(file_path)
    for item_data in parsed:
        db.add(AnswerKeyItem(
            key_id=key_id,
            question_number=item_data["question_number"],
            answer=item_data["answer"],
            explanation=item_data["explanation"],
        ))

    db.commit()
    return RedirectResponse(url="/suneung/answer-keys", status_code=303)


# ─────────────────────────────────────────
# 삭제
# ─────────────────────────────────────────

@router.post("/answer-keys/{key_id}/delete")
def answer_key_delete(key_id: str, db: Session = Depends(get_db)):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")
    if key.file_path and os.path.exists(key.file_path):
        try:
            os.remove(key.file_path)
        except Exception:
            pass
    db.delete(key)
    db.commit()
    return RedirectResponse(url="/suneung/answer-keys", status_code=303)


# ─────────────────────────────────────────
# 상세
# ─────────────────────────────────────────

@router.get("/answer-keys/{key_id}", response_class=HTMLResponse)
def answer_key_detail(key_id: str, request: Request, db: Session = Depends(get_db)):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")
    ctx = _base_ctx(db, request=request, key=key)
    return templates.TemplateResponse("suneung/answer_key_detail.html", ctx)


# ─────────────────────────────────────────
# 문항 저장 (JSON)
# ─────────────────────────────────────────

@router.post("/answer-keys/{key_id}/save")
async def answer_key_save(key_id: str, request: Request, db: Session = Depends(get_db)):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")

    body = await request.json()
    items_data = body.get("items", [])

    # 기존 항목 삭제 후 재삽입
    for item in key.items:
        db.delete(item)
    db.flush()

    for item_data in items_data:
        num = item_data.get("question_number")
        if num is None:
            continue
        db.add(AnswerKeyItem(
            key_id=key_id,
            question_number=int(num),
            answer=item_data.get("answer") or None,
            explanation=item_data.get("explanation") or None,
        ))

    db.commit()
    return JSONResponse({"ok": True})


# ─────────────────────────────────────────
# 문제지 매칭
# ─────────────────────────────────────────

@router.post("/answer-keys/{key_id}/match")
def answer_key_match(
    key_id: str,
    db: Session = Depends(get_db),
    paper_code: str = Form(...),
):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")

    key.paper_code = paper_code
    key.status = "matched"

    # 해당 paper_code의 Question들에 answer/explanation 업데이트
    for item in key.items:
        q = (
            db.query(Question)
            .filter(
                Question.paper_code == paper_code,
                Question.question_number == item.question_number,
            )
            .first()
        )
        if q:
            if item.answer:
                q.answer = item.answer
            if item.explanation:
                q.explanation = item.explanation

    db.commit()
    return RedirectResponse(url=f"/suneung/answer-keys/{key_id}", status_code=303)
