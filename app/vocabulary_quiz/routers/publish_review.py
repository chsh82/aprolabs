"""momolib으로 이식된 1순위(파일럿 연결) 40콘텐츠 공개검토 화면 - 기존
app/vocabulary_quiz/routers/review.py(QA 표본 검수)와는 완전히 별도
경로(/vocab-publish-review)를 쓴다(그 라우터를 한 글자도 건드리지
않음). require_admin을 그대로 재사용해 비로그인 401/비관리자 403은
기존 검수 화면과 동일하게 보장된다.

절대 하지 않는 것: student_exposure/public_ready/level_status/
boundary_flag를 바꾸는 라우트, 실제 승격(공개 전환) 버튼이나 API."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db as get_main_db
from app.models.user import User
from app.vocabulary_quiz import publish_review as pr
from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models import VocabularyContent, VocabularyContentLevel
from app.vocabulary_quiz.models_publish_review import VERDICT_CHOICES, VERDICT_LABELS

router = APIRouter(prefix="/vocab-publish-review")
templates = Jinja2Templates(directory="app/templates")


def _reviewer_email(user_id: str) -> str | None:
    db = next(get_main_db())
    try:
        u = db.query(User).filter(User.id == user_id).first()
        return u.email if u else None
    finally:
        db.close()


@router.get("/")
def publish_review_index(
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    content_ids = pr.tier1_content_ids(db)
    rows = []
    for cid in content_ids:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == cid).first()
        if content is None:
            continue
        level = db.query(VocabularyContentLevel).filter(VocabularyContentLevel.content_id == cid).first()
        review = pr.latest_review(db, cid)
        stale = pr.review_is_stale(db, review, content) if review else None
        rows.append({
            "content": content,
            "vocab_level": level.vocab_level if level else None,
            "review": review,
            "stale": stale,
        })
    rows.sort(key=lambda r: ((r["vocab_level"] or 99), r["content"].lemma))

    return templates.TemplateResponse(request, "vocabulary_quiz/publish_review_index.html", {
        "request": request, "rows": rows, "verdict_labels": VERDICT_LABELS,
    }, headers=NOINDEX_HEADERS)


@router.get("/{content_id}")
def publish_review_detail(
    content_id: str,
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == content_id).first()
    if content is None:
        raise HTTPException(status_code=404, detail="콘텐츠를 찾을 수 없습니다")

    level = db.query(VocabularyContentLevel).filter(VocabularyContentLevel.content_id == content_id).first()
    items = pr.linked_items(db, content_id)
    items_with_options = [
        {"item": it, "options": json.loads(it.options_json) if it.options_json else []}
        for it in items
    ]
    cautions = pr.content_cautions(content, level)

    history = pr.review_history(db, content_id)
    latest = history[0] if history else None
    stale = pr.review_is_stale(db, latest, content) if latest else None

    return templates.TemplateResponse(request, "vocabulary_quiz/publish_review_detail.html", {
        "request": request, "content": content, "level": level,
        "items_with_options": items_with_options, "cautions": cautions,
        "history": history, "latest": latest, "stale": stale,
        "verdict_choices": VERDICT_CHOICES, "verdict_labels": VERDICT_LABELS,
    }, headers=NOINDEX_HEADERS)


@router.post("/{content_id}/verdict")
def publish_review_save_verdict(
    content_id: str,
    request: Request,
    verdict: str = Form(...),
    rationale: str = Form(""),
    db: Session = Depends(get_vocabulary_quiz_db),
    admin_user_id: str = Depends(require_admin),
):
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == content_id).first()
    if content is None:
        raise HTTPException(status_code=404, detail="콘텐츠를 찾을 수 없습니다")
    if verdict not in VERDICT_CHOICES:
        raise HTTPException(status_code=400, detail="알 수 없는 판정 값입니다")

    pr.save_review(db, content_id, verdict, rationale.strip(), admin_user_id, _reviewer_email(admin_user_id))

    # 저장하면 같은 화면에 머무르지 않고 목록 순서상 다음 항목으로 자동 이동한다
    # (40건을 순서대로 검토하는 작업 흐름 지원). 마지막 항목이면 목록으로.
    next_id = pr.next_content_id(db, content_id)
    if next_id:
        return RedirectResponse(url=f"/vocab-publish-review/{next_id}", status_code=303)
    return RedirectResponse(url="/vocab-publish-review/", status_code=303)
