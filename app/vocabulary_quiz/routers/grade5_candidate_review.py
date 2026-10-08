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
    batch: int = g5r.DEFAULT_BATCH_NO,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    # v2 분류기 보고서 대표 12건은 배치 구분 없이 한 화면에서 다시 보는
    # 전용 뷰다 - 기존 배치별 진행률·목록 로직과 완전히 분리해서 처리한다
    # (아래 batch_no 분기와 섞으면 진행률 집계가 12건 기준으로 잘못 계산됨).
    if filter == "v2_rep12":
        rows_all = g5r.rep12_rows(db)
        latest_map = g5r.latest_judgments_map_for_ids(db, [r.candidate_id for r in rows_all])
        rows = []
        for r in rows_all:
            j = latest_map.get(r.candidate_id)
            stale = g5r.judgment_is_stale(j, r) if j is not None else None
            rows.append({"row": r, "judgment": j, "stale": stale})
        return templates.TemplateResponse(request, "vocabulary_quiz/grade5_candidate_review_index.html", {
            "request": request, "rows": rows, "stats": None, "filter": filter,
            "judgment_labels": JUDGMENT_LABELS, "batch_no": batch,
            "available_batches": g5r.available_batch_numbers(db),
        }, headers=NOINDEX_HEADERS)

    # 배치별 진행률·판정 집계가 섞이지 않도록 ?batch=N으로 분리한다(기존
    # service 계층 함수들은 처음부터 batch_no 파라미터를 받고 있었다 -
    # 이 라우트만 1번으로 고정돼 있던 것을 외부로 노출한다).
    batch_no = batch
    available_batches = g5r.available_batch_numbers(db)
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

    return templates.TemplateResponse(request, "vocabulary_quiz/grade5_candidate_review_index.html", {
        "request": request, "rows": rows, "stats": stats, "filter": filter,
        "judgment_labels": JUDGMENT_LABELS, "batch_no": batch_no,
        "available_batches": available_batches,
    }, headers=NOINDEX_HEADERS)


@router.get("/{candidate_id}")
def detail(
    candidate_id: str,
    request: Request,
    from_filter: str = "",
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    row = g5r.get_candidate(db, candidate_id)
    if row is None:
        raise HTTPException(status_code=404, detail="후보 데이터를 찾을 수 없습니다")

    # v2 대표 12건 목록에서 들어온 경우, 이전/다음도 그 12건 안에서만
    # 움직인다 - 그 후보가 속한 전체 배치(최대 100건) 순서로 튀면 "대표
    # 사례만 훑어본다"는 목적에 맞지 않는다.
    if from_filter == "v2_rep12" and candidate_id in g5r.V2_REP12_CANDIDATE_IDS:
        ids = g5r.V2_REP12_CANDIDATE_IDS
    else:
        from_filter = ""
        ids = g5r.ordered_candidate_ids(db, row.batch_no)
    idx = ids.index(candidate_id)
    prev_id = ids[idx - 1] if idx > 0 else None
    next_id = ids[idx + 1] if idx + 1 < len(ids) else None

    history = g5r.judgment_history(db, candidate_id)
    latest = history[0] if history else None
    stale = g5r.judgment_is_stale(latest, row) if latest else None
    token = g5r.new_submission_token()

    return templates.TemplateResponse(request, "vocabulary_quiz/grade5_candidate_review_detail.html", {
        "request": request, "row": row, "history": history, "latest": latest, "stale": stale,
        "judgment_choices": JUDGMENT_CHOICES, "judgment_labels": JUDGMENT_LABELS,
        "prev_id": prev_id, "next_id": next_id, "submission_token": token,
        "position": idx + 1, "total": len(ids), "from_filter": from_filter,
    }, headers=NOINDEX_HEADERS)


@router.post("/{candidate_id}/judgment")
def save_judgment(
    candidate_id: str,
    request: Request,
    judgment: str = Form(...),
    rationale: str = Form(""),
    submission_token: str = Form(...),
    return_filter: str = Form(""),
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

    # 대표 12건 화면에서 들어온 저장은 "다음 미검수"를 찾아 전체 배치를
    # 헤매지 않고 그 12건 목록으로 돌아간다 - 12건은 이미 전부 판정이
    # 있어 next_unjudged_candidate_id가 배치 전체에서 엉뚱한 항목을
    # 찾아버리기 때문.
    if return_filter == "v2_rep12":
        return RedirectResponse(url="/vocab-grade5-candidate-review/?filter=v2_rep12", status_code=303)

    next_id = g5r.next_unjudged_candidate_id(db, candidate_id, row.batch_no)
    if next_id:
        return RedirectResponse(url=f"/vocab-grade5-candidate-review/{next_id}", status_code=303)
    return RedirectResponse(url="/vocab-grade5-candidate-review/", status_code=303)
