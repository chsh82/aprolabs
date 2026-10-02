"""L3 중등 보강 1차(2026-10-07) 콘텐츠·문항 검수 화면 - 기존
app/vocabulary_quiz/routers/publish_review.py(momolib 1순위)와는 완전히
별도 경로(/vocab-grade5-l3-batch1-review)를 쓴다(그 라우터·그 대상
content_id 집합을 한 글자도 건드리지 않음 - momolib 무관).
require_admin을 그대로 재사용해 비로그인 401/비관리자 403은 기존 검수
화면과 동일하게 보장된다.

절대 하지 않는 것: student_exposure/public_ready/level_status/
boundary_flag를 바꾸는 라우트, 실제 승격(공개 전환) 버튼이나 API, 사람의
L3 레벨 판정을 대신 쓰거나 바꾸는 코드(그 판정은 grade5_candidate_review
화면에서만 한다 - 이 화면은 읽기만 한다). 검수 판정을 미리 채워 넣는
코드도 없다 - 폼 제출이 있어야만 행이 생긴다."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db as get_main_db
from app.models.user import User
from app.vocabulary_quiz import grade5_l3_batch1_review as br
from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models import VocabularyContent
from app.vocabulary_quiz.models_publish_review import VERDICT_CHOICES, VERDICT_LABELS

router = APIRouter(prefix="/vocab-grade5-l3-batch1-review")
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
    content_ids = br.ordered_batch1_content_ids(db)
    rows = []
    for cid in content_ids:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == cid).first()
        if content is None:
            continue
        review = br.latest_review(db, cid)
        stale = br.review_is_stale(db, review, content) if review else None
        rows.append({"content": content, "review": review, "stale": stale})

    judged = sum(1 for r in rows if r["review"] is not None)
    return templates.TemplateResponse("vocabulary_quiz/grade5_l3_batch1_review_index.html", {
        "request": request, "rows": rows, "verdict_labels": VERDICT_LABELS,
        "total": len(rows), "judged": judged,
    }, headers=NOINDEX_HEADERS)


@router.get("/{content_id}")
def detail(
    content_id: str,
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    content = db.query(VocabularyContent).filter(
        VocabularyContent.content_id == content_id,
        VocabularyContent.source_version == br.SOURCE_VERSION,
    ).first()
    if content is None:
        raise HTTPException(status_code=404, detail="콘텐츠를 찾을 수 없습니다")

    items = br.linked_items(db, content_id)
    items_with_options = [
        {"item": it, "options": json.loads(it.options_json) if it.options_json else []}
        for it in items
    ]

    history = br.review_history(db, content_id)
    latest = history[0] if history else None
    stale = br.review_is_stale(db, latest, content) if latest else None

    return templates.TemplateResponse("vocabulary_quiz/grade5_l3_batch1_review_detail.html", {
        "request": request, "content": content,
        "items_with_options": items_with_options,
        "human_level_note": br.human_level_note(content_id),
        "history": history, "latest": latest, "stale": stale,
        "verdict_choices": VERDICT_CHOICES, "verdict_labels": VERDICT_LABELS,
    }, headers=NOINDEX_HEADERS)


@router.post("/{content_id}/verdict")
def save_verdict(
    content_id: str,
    request: Request,
    verdict: str = Form(...),
    rationale: str = Form(""),
    db: Session = Depends(get_vocabulary_quiz_db),
    admin_user_id: str = Depends(require_admin),
):
    content = db.query(VocabularyContent).filter(
        VocabularyContent.content_id == content_id,
        VocabularyContent.source_version == br.SOURCE_VERSION,
    ).first()
    if content is None:
        raise HTTPException(status_code=404, detail="콘텐츠를 찾을 수 없습니다")
    if verdict not in VERDICT_CHOICES:
        raise HTTPException(status_code=400, detail="알 수 없는 판정 값입니다")

    br.save_review(db, content_id, verdict, rationale.strip(), admin_user_id, _reviewer_email(admin_user_id))

    next_id = br.next_content_id(db, content_id)
    if next_id:
        return RedirectResponse(url=f"/vocab-grade5-l3-batch1-review/{next_id}", status_code=303)
    return RedirectResponse(url="/vocab-grade5-l3-batch1-review/", status_code=303)
