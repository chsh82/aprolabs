"""공식 5등급 신규 어휘 1차 배치(100건) 검수 화면 - tier1
(/vocab-official-grade-review)과 완전히 별도 경로(/vocab-grade5-candidate-
review), 그 라우터를 한 글자도 건드리지 않는다. require_admin을 그대로
재사용해 비로그인 401/비관리자 403은 기존 검수 화면과 동일하게 보장된다.

절대 하지 않는 것: vocab_level/level_status/boundary_flag/student_exposure/
public_ready를 바꾸는 라우트, 문항이나 매니페스트를 바꾸는 라우트, 판정을
자동으로 채우거나 일괄 승인하는 API, 뜻풀이·예문 입력 필드(이번 단계
범위 밖). '제외' 판정도 다른 판정과 똑같이 append-only 행 추가일 뿐 -
vocabulary_grade5_candidate_batch 행을 삭제·수정하는 코드는 없다."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db as get_main_db
from app.models.user import User
from app.vocabulary_quiz import grade5_candidate_review as g5r
from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models_grade5_candidate_review import JUDGMENT_CHOICES, JUDGMENT_LABELS

router = APIRouter(prefix="/vocab-grade5-candidate-review")
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
    filter: str = "",
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    batch_no = g5r.DEFAULT_BATCH_NO
    rows_all = g5r.ordered_batch_rows(db, batch_no)
    latest_map = g5r.latest_judgments_map(db, batch_no)
    stats = g5r.progress_stats(db, batch_no)

    rows = []
    for r in rows_all:
        j = latest_map.get(r.candidate_id)
        if filter == "unjudged" and j is not None:
            continue
        stale = g5r.judgment_is_stale(j, r) if j is not None else None
        rows.append({"row": r, "judgment": j, "stale": stale})

    return templates.TemplateResponse("vocabulary_quiz/grade5_candidate_review_index.html", {
        "request": request, "rows": rows, "stats": stats, "filter": filter,
        "judgment_labels": JUDGMENT_LABELS,
    }, headers=NOINDEX_HEADERS)


@router.get("/{candidate_id}")
def detail(
    candidate_id: str,
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    row = g5r.get_candidate(db, candidate_id)
    if row is None:
        raise HTTPException(status_code=404, detail="후보 데이터를 찾을 수 없습니다")

    ids = g5r.ordered_candidate_ids(db, row.batch_no)
    idx = ids.index(candidate_id)
    prev_id = ids[idx - 1] if idx > 0 else None
    next_id = ids[idx + 1] if idx + 1 < len(ids) else None

    history = g5r.judgment_history(db, candidate_id)
    latest = history[0] if history else None
    stale = g5r.judgment_is_stale(latest, row) if latest else None
    token = g5r.new_submission_token()

    return templates.TemplateResponse("vocabulary_quiz/grade5_candidate_review_detail.html", {
        "request": request, "row": row, "history": history, "latest": latest, "stale": stale,
        "judgment_choices": JUDGMENT_CHOICES, "judgment_labels": JUDGMENT_LABELS,
        "prev_id": prev_id, "next_id": next_id, "submission_token": token,
        "position": idx + 1, "total": len(ids),
    }, headers=NOINDEX_HEADERS)


@router.post("/{candidate_id}/judgment")
def save_judgment(
    candidate_id: str,
    request: Request,
    judgment: str = Form(...),
    rationale: str = Form(""),
    submission_token: str = Form(...),
    db: Session = Depends(get_vocabulary_quiz_db),
    admin_user_id: str = Depends(require_admin),
):
    row = g5r.get_candidate(db, candidate_id)
    if row is None:
        raise HTTPException(status_code=404, detail="후보 데이터를 찾을 수 없습니다")
    if judgment not in JUDGMENT_CHOICES:
        raise HTTPException(status_code=400, detail="알 수 없는 판정 값입니다")

    g5r.save_judgment(
        db, candidate_id, judgment, rationale.strip(),
        admin_user_id, _reviewer_email(admin_user_id), submission_token,
    )

    next_id = g5r.next_unjudged_candidate_id(db, candidate_id, row.batch_no)
    if next_id:
        return RedirectResponse(url=f"/vocab-grade5-candidate-review/{next_id}", status_code=303)
    return RedirectResponse(url="/vocab-grade5-candidate-review/", status_code=303)
