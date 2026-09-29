"""국립국어원 공식 등급 tier1(31건) 빠른 검수 화면 - /vocab-publish-review
(momolib 1순위 공개검토)와는 완전히 별도 경로(/vocab-official-grade-review),
그 라우터를 한 글자도 건드리지 않는다. require_admin을 그대로 재사용해
비로그인 401/비관리자 403은 기존 검수 화면과 동일하게 보장된다.

절대 하지 않는 것: vocab_level/level_status/boundary_flag/student_exposure/
public_ready를 바꾸는 라우트, 문항이나 매니페스트를 바꾸는 라우트, 판정을
자동으로 채우거나 일괄 승인하는 API. 판정은 오직 사람이 폼을 제출해야만
vocabulary_official_grade_judgments에 한 행씩 append된다."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db as get_main_db
from app.models.user import User
from app.vocabulary_quiz import official_grade_review as ogr
from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models import VocabularyContent
from app.vocabulary_quiz.models_official_grade_review import JUDGMENT_CHOICES, VocabularyOfficialGradeReference

router = APIRouter(prefix="/vocab-official-grade-review")
templates = Jinja2Templates(directory="app/templates")


def _reviewer_email(user_id: str) -> str | None:
    db = next(get_main_db())
    try:
        u = db.query(User).filter(User.id == user_id).first()
        return u.email if u else None
    finally:
        db.close()


@router.get("/")
def index(
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    content_ids = ogr.ordered_tier1_content_ids(db)
    rows = []
    for cid in content_ids:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == cid).first()
        ref = (
            db.query(VocabularyOfficialGradeReference)
            .filter(VocabularyOfficialGradeReference.content_id == cid)
            .first()
        )
        if content is None or ref is None:
            continue
        judgment = ogr.latest_judgment(db, cid)
        stale = ogr.judgment_is_stale(judgment, ref) if judgment else None
        rows.append({
            "content": content, "ref": ref, "judgment": judgment, "stale": stale,
            "diff_reason": ogr.diff_reason(ref),
        })

    return templates.TemplateResponse("vocabulary_quiz/official_grade_review_index.html", {
        "request": request, "rows": rows,
    }, headers=NOINDEX_HEADERS)


@router.get("/{content_id}")
def detail(
    content_id: str,
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == content_id).first()
    ref = (
        db.query(VocabularyOfficialGradeReference)
        .filter(VocabularyOfficialGradeReference.content_id == content_id)
        .first()
    )
    if content is None or ref is None:
        raise HTTPException(status_code=404, detail="tier1 참조 데이터를 찾을 수 없습니다")
    if ref.priority_tier != 1:
        raise HTTPException(status_code=404, detail="이 콘텐츠는 tier1 대상이 아닙니다")

    level = ogr.content_level(db, content_id)
    history = ogr.judgment_history(db, content_id)
    latest = history[0] if history else None
    stale = ogr.judgment_is_stale(latest, ref) if latest else None

    return templates.TemplateResponse("vocabulary_quiz/official_grade_review_detail.html", {
        "request": request, "content": content, "ref": ref, "level": level,
        "short_example": ogr.short_example(content), "diff_reason": ogr.diff_reason(ref),
        "history": history, "latest": latest, "stale": stale,
        "judgment_choices": JUDGMENT_CHOICES,
    }, headers=NOINDEX_HEADERS)


@router.post("/{content_id}/judgment")
def save_judgment(
    content_id: str,
    request: Request,
    judgment: str = Form(...),
    rationale: str = Form(""),
    db: Session = Depends(get_vocabulary_quiz_db),
    admin_user_id: str = Depends(require_admin),
):
    ref = (
        db.query(VocabularyOfficialGradeReference)
        .filter(VocabularyOfficialGradeReference.content_id == content_id)
        .first()
    )
    if ref is None or ref.priority_tier != 1:
        raise HTTPException(status_code=404, detail="tier1 참조 데이터를 찾을 수 없습니다")
    if judgment not in JUDGMENT_CHOICES:
        raise HTTPException(status_code=400, detail="알 수 없는 판정 값입니다")

    ogr.save_judgment(db, content_id, judgment, rationale.strip(), admin_user_id, _reviewer_email(admin_user_id))

    next_id = ogr.next_content_id(db, content_id)
    if next_id:
        return RedirectResponse(url=f"/vocab-official-grade-review/{next_id}", status_code=303)
    return RedirectResponse(url="/vocab-official-grade-review/", status_code=303)
