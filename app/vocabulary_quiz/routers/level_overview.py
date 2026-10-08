"""어휘 레벨 현황 - 읽기 전용 통합 조회 화면(`/vocab-level-overview/`).

GET 라우트 하나뿐이다 - POST/판정 저장 라우트가 이 파일에도, 템플릿에도
없다. require_admin을 그대로 재사용해 비로그인 401/비관리자 403은 기존
검수 화면과 동일하게 보장된다. vocab_level/문항/매니페스트/공개 플래그를
바꾸는 코드는 이 라우터 어디에도 없다."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.vocabulary_quiz import level_overview as lo
from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models_official_grade_review import JUDGMENT_CHOICES

router = APIRouter(prefix="/vocab-level-overview")
templates = Jinja2Templates(directory="app/templates")


@router.get("/")
def index(
    request: Request,
    level: str = "",
    judgment: str = "",
    change_candidate: str = "",
    needs_review: str = "",
    q: str = "",
    page: int = 1,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    all_rows = lo.load_all_rows(db)
    level_int = int(level) if level.isdigit() else None
    result = lo.filter_and_paginate(
        all_rows, level=level_int, judgment=judgment,
        change_candidate_only=(change_candidate == "1"),
        needs_review_only=(needs_review == "1"),
        q=q.strip(), page=page,
    )

    return templates.TemplateResponse(request, "vocabulary_quiz/level_overview_index.html", {
        "request": request, "result": result,
        "category_labels": lo.CATEGORY_LABELS, "grade_labels": lo.GRADE_LABELS,
        "judgment_choices": JUDGMENT_CHOICES,
        "filters": {
            "level": level, "judgment": judgment, "change_candidate": change_candidate,
            "needs_review": needs_review, "q": q,
        },
    }, headers=NOINDEX_HEADERS)
