"""작업일지 게시판.

GET  /journal              -> 목록(구분별 필터)
POST /journal               -> 새 글 작성
GET  /journal/{id}          -> 상세(수정 폼)
POST /journal/{id}          -> 수정
POST /journal/{id}/delete   -> 삭제
"""
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.journal import JournalEntry, ENTRY_TYPES

router = APIRouter(prefix="/journal")
templates = Jinja2Templates(directory="app/templates")


@router.get("", response_class=HTMLResponse)
def journal_index(request: Request, entry_type: str = "", db: Session = Depends(get_db)):
    query = db.query(JournalEntry)
    if entry_type:
        query = query.filter(JournalEntry.entry_type == entry_type)
    entries = query.order_by(JournalEntry.created_at.desc()).all()

    counts = {t: db.query(JournalEntry).filter(JournalEntry.entry_type == t).count() for t in ENTRY_TYPES}
    counts["전체"] = db.query(JournalEntry).count()

    return templates.TemplateResponse("journal/index.html", {
        "request": request,
        "entries": entries,
        "entry_types": ENTRY_TYPES,
        "filter_type": entry_type,
        "counts": counts,
    })


@router.post("")
def journal_create(
    request: Request,
    title: str = Form(...),
    content: str = Form(""),
    entry_type: str = Form("작업일지"),
    db: Session = Depends(get_db),
):
    if entry_type not in ENTRY_TYPES:
        raise HTTPException(status_code=400, detail="알 수 없는 구분입니다.")
    entry = JournalEntry(title=title.strip() or "(제목 없음)", content=content, entry_type=entry_type)
    db.add(entry)
    db.commit()
    return RedirectResponse(url="/journal", status_code=303)


@router.get("/{entry_id}", response_class=HTMLResponse)
def journal_detail(request: Request, entry_id: str, db: Session = Depends(get_db)):
    entry = db.query(JournalEntry).filter(JournalEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="글을 찾을 수 없습니다.")
    return templates.TemplateResponse("journal/detail.html", {
        "request": request,
        "entry": entry,
        "entry_types": ENTRY_TYPES,
    })


@router.post("/{entry_id}")
def journal_update(
    entry_id: str,
    title: str = Form(...),
    content: str = Form(""),
    entry_type: str = Form("작업일지"),
    db: Session = Depends(get_db),
):
    entry = db.query(JournalEntry).filter(JournalEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="글을 찾을 수 없습니다.")
    if entry_type not in ENTRY_TYPES:
        raise HTTPException(status_code=400, detail="알 수 없는 구분입니다.")
    entry.title = title.strip() or "(제목 없음)"
    entry.content = content
    entry.entry_type = entry_type
    db.commit()
    return RedirectResponse(url=f"/journal/{entry_id}", status_code=303)


@router.post("/{entry_id}/delete")
def journal_delete(entry_id: str, db: Session = Depends(get_db)):
    entry = db.query(JournalEntry).filter(JournalEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="글을 찾을 수 없습니다.")
    db.delete(entry)
    db.commit()
    return RedirectResponse(url="/journal", status_code=303)
