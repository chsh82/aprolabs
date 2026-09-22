"""일반 어휘 퀴즈 QA 표본 검수 UI - app/routers/literacy_admin.py의
review_hub/review_quiz 패턴(허브 + 1건씩 넘기며 검수)을 그대로 따른다.

검수 결과는 vocabulary_review_samples.review_status에만 기록한다 -
vocabulary_contents/items의 student_exposure/public_ready는 여기서
절대 건드리지 않는다(운영 승격은 별도 절차, 이 화면의 책임 밖).

기존 auth_middleware(app/main.py)가 이미 앱 전체에 로그인을 강제하지만,
관리자 아닌 로그인 사용자를 구분하는 라우트는 이 앱에 아직 하나도 없었다
(지금까지 계정이 admin@aprolabs.co.kr 1명뿐이라 구분할 필요가 없었음).
검수 데이터는 아직 학생 비공개 상태라도 민감하므로, 여기서는 명시적으로
is_admin을 한 번 더 확인한다 - 로그인 미들웨어가 뚫리거나 나중에 비관리자
계정이 생겨도 이 화면만은 관리자로 한정되게 하는 방어선."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.auth import get_current_user_id
from app.database import get_db
from app.models.user import User
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models import VocabularyContent, VocabularyItem, VocabularyReviewSample

router = APIRouter(prefix="/vocabulary-quiz")
templates = Jinja2Templates(directory="app/templates")

_VALID_STATUSES = ("UNREVIEWED", "PASS", "REVISE", "EXCLUDE")
_NOINDEX_HEADERS = {"X-Robots-Tag": "noindex, nofollow", "Cache-Control": "private, no-store"}


def require_admin(request: Request) -> str:
    user_id = get_current_user_id(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다")
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == user_id).first()
    finally:
        db.close()
    if not user or not user.is_admin:
        raise HTTPException(status_code=403, detail="관리자만 접근할 수 있습니다")
    return user_id


@router.get("/review")
def review_hub(request: Request, version: str = "2.1.29", db: Session = Depends(get_vocabulary_quiz_db),
               _admin: str = Depends(require_admin)):
    counts = dict(
        db.query(VocabularyReviewSample.review_status, func.count(VocabularyReviewSample.id))
        .filter(VocabularyReviewSample.sample_version == version)
        .group_by(VocabularyReviewSample.review_status)
        .all()
    )
    total = sum(counts.values())
    versions = [row[0] for row in db.query(VocabularyReviewSample.sample_version).distinct().all()]

    return templates.TemplateResponse("vocabulary_quiz/review_hub.html", {
        "request": request, "version": version, "versions": versions,
        "counts": counts, "total": total,
    }, headers=_NOINDEX_HEADERS)


def _card_query(db: Session, version: str, status: str | None):
    q = db.query(VocabularyReviewSample).filter(VocabularyReviewSample.sample_version == version)
    if status:
        q = q.filter(VocabularyReviewSample.review_status == status)
    return q


@router.get("/review/card")
def review_card(
    request: Request,
    version: str = "2.1.29",
    status: str | None = None,
    id: int | None = None,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    base = _card_query(db, version, status)

    if id is not None:
        sample = base.filter(VocabularyReviewSample.id == id).first()
    else:
        sample = base.filter(VocabularyReviewSample.review_status == "UNREVIEWED").order_by(
            VocabularyReviewSample.id.asc()
        ).first()
        if sample is None:
            sample = base.order_by(VocabularyReviewSample.id.asc()).first()

    done = _card_query(db, version, status).filter(VocabularyReviewSample.review_status != "UNREVIEWED").count()
    total = _card_query(db, version, status).count()

    if sample is None:
        return templates.TemplateResponse("vocabulary_quiz/review_card.html", {
            "request": request, "sample": None, "version": version, "status": status,
            "done": done, "total": total,
        }, headers=_NOINDEX_HEADERS)

    prev_id = (
        base.filter(VocabularyReviewSample.id < sample.id).order_by(VocabularyReviewSample.id.desc()).first()
    )
    next_id = (
        base.filter(VocabularyReviewSample.id > sample.id).order_by(VocabularyReviewSample.id.asc()).first()
    )

    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == sample.content_id).first()
    item = db.query(VocabularyItem).filter(VocabularyItem.item_id == sample.item_id).first()

    return templates.TemplateResponse("vocabulary_quiz/review_card.html", {
        "request": request, "sample": sample, "content": content, "item": item,
        "version": version, "status": status,
        "prev_id": prev_id.id if prev_id else None, "next_id": next_id.id if next_id else None,
        "done": done, "total": total,
    }, headers=_NOINDEX_HEADERS)


@router.post("/review/save")
async def review_save(request: Request, db: Session = Depends(get_vocabulary_quiz_db),
                       _admin: str = Depends(require_admin)):
    body = await request.json()
    sample_id = body.get("id")
    review_status = body.get("review_status")
    note = body.get("note")

    if review_status not in _VALID_STATUSES:
        return {"ok": False, "error": f"알 수 없는 검수 상태: {review_status}"}

    sample = db.query(VocabularyReviewSample).filter(VocabularyReviewSample.id == sample_id).first()
    if sample is None:
        return {"ok": False, "error": "표본을 찾을 수 없습니다"}

    sample.review_status = review_status
    sample.review_note = note
    sample.reviewed_at = datetime.now(timezone.utc).isoformat()
    sample.updated_at = datetime.now(timezone.utc).isoformat()
    db.commit()

    return {"ok": True}
