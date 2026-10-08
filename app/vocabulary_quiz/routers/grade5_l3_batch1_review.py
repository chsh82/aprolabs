"""L3 중등 보강 콘텐츠·문항 검수 화면 - 기존
app/vocabulary_quiz/routers/publish_review.py(momolib 1순위)와는 완전히
별도 경로(/vocab-grade5-l3-batch1-review)를 쓴다(그 라우터·그 대상
content_id 집합을 한 글자도 건드리지 않음 - momolib 무관).
require_admin을 그대로 재사용해 비로그인 401/비관리자 403은 기존 검수
화면과 동일하게 보장된다.

**배치 단위 재사용(2026-10-07 2차 배치 추가)**: URL 경로(`/vocab-grade5-
l3-batch1-review/...`)는 1차가 이미 쓰던 그대로 유지해 기존 북마크를
깨지 않는다 - 목록은 `?batch=batch2`로 다른 배치를 보고, 상세/판정
저장은 content_id가 전역에서 유일하므로 경로에 배치를 넣지 않고 콘텐츠의
source_version에서 배치를 내부적으로 알아낸다. 라우터·템플릿·
`vocabulary_publish_reviews` 테이블은 배치마다 복제하지 않고 전부
공유한다 - 배치별 차이는 `grade5_l3_batch1_review.BATCHES`에만 있다.

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


def _resolve_content_any_batch(db: Session, content_id: str) -> tuple[VocabularyContent, br.L3BatchConfig]:
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == content_id).first()
    if content is None:
        raise HTTPException(status_code=404, detail="콘텐츠를 찾을 수 없습니다")
    cfg = br.batch_config_for_source_version(content.source_version)
    if cfg is None:
        raise HTTPException(status_code=404, detail="이 콘텐츠는 L3 보강 검수 대상 배치가 아닙니다")
    return content, cfg


@router.get("/")
def index(
    request: Request,
    batch: str = "batch1",
    filter: str = "all",  # noqa: A002 - 쿼리 파라미터명을 ?filter=로 유지하기 위해 그대로 둠
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    filter_mode = filter
    try:
        cfg = br.get_batch_config(batch)
    except ValueError:
        raise HTTPException(status_code=404, detail="알 수 없는 배치입니다")
    if filter_mode not in ("all", "risk", "sample"):
        raise HTTPException(status_code=400, detail="알 수 없는 filter 값입니다(all/risk/sample만 허용)")

    content_ids = br.ordered_batch_content_ids(db, cfg)
    rows = []
    for cid in content_ids:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == cid).first()
        if content is None:
            continue
        review = br.latest_review(db, cid)
        stale = br.review_is_stale(db, cfg, review, content) if review else None
        held = br.is_held(cfg, cid)
        if held:
            status = "보류"
        elif review is None:
            status = "검수 전"
        elif stale:
            status = "재검수 필요"
        else:
            status = "승인 유지"
        # 위험 기반 검수(참고 정보일 뿐, 사람 판정을 대신하지 않음) - risk_review
        # 비노출 배치(1차 등)는 risk_info가 항상 None이라 필터가 자연히 안 보임.
        risk_info = br.risk_review_info(db, cfg, cid)
        rows.append({
            "content": content, "review": review, "stale": stale,
            "held": held, "status": status, "unloaded": False,
            "risk_info": risk_info,
        })

    # 콘텐츠 자체를 DB에 적재하지 않은 보류 항목(예: 2차의 아멘·파키스탄) -
    # vocabulary_contents에 행이 없어도 보류 사유를 목록에서 보이게 한다.
    for cid, lemma in cfg.unloaded_held_lemmas.items():
        rows.append({
            "content": None, "unloaded_content_id": cid, "unloaded_lemma": lemma,
            "unloaded_reason": br.hold_reason(cfg, cid),
            "review": None, "stale": None, "held": True, "status": "보류",
            "unloaded": True, "risk_info": None,
        })

    judged = sum(1 for r in rows if r["review"] is not None)
    held_count = sum(1 for r in rows if r["held"])
    stale_count = sum(1 for r in rows if r["status"] == "재검수 필요")
    approved_count = sum(1 for r in rows if r["status"] == "승인 유지")
    risk_count = sum(1 for r in rows if r["risk_info"] and r["risk_info"].get("risk_category") not in (None, "NONE"))
    sample_count = sum(1 for r in rows if r["risk_info"] and r["risk_info"].get("sampled"))

    if filter_mode == "risk":
        visible_rows = [r for r in rows if r["risk_info"] and r["risk_info"].get("risk_category") not in (None, "NONE")]
    elif filter_mode == "sample":
        visible_rows = [r for r in rows if r["risk_info"] and r["risk_info"].get("sampled")]
    else:
        visible_rows = rows

    return templates.TemplateResponse(request, "vocabulary_quiz/grade5_l3_batch1_review_index.html", {
        "request": request, "rows": visible_rows, "verdict_labels": VERDICT_LABELS,
        "total": len(rows), "judged": judged,
        "held_count": held_count, "stale_count": stale_count, "approved_count": approved_count,
        "risk_count": risk_count, "sample_count": sample_count, "filter_mode": filter_mode,
        "batches": br.BATCHES, "current_batch": cfg,
    }, headers=NOINDEX_HEADERS)


@router.get("/{content_id}")
def detail(
    content_id: str,
    request: Request,
    db: Session = Depends(get_vocabulary_quiz_db),
    _admin: str = Depends(require_admin),
):
    content, cfg = _resolve_content_any_batch(db, content_id)

    items = br.linked_items(db, cfg, content_id)

    def _with_admin_notes(it):
        options = json.loads(it.options_json) if it.options_json else []
        flags_list = json.loads(it.qa_flags_json) if it.qa_flags_json else [{}]
        flags = flags_list[0] if flags_list else {}
        return {
            "item": it, "options": options,
            "wrong_option_reasons": flags.get("wrong_option_reasons"),
            "key_clue": flags.get("key_clue"),
            "revised_from_item_id": flags.get("revised_from_item_id"),
            "v2_hold_reason": flags.get("v2_hold_reason"),
            "risk_review": flags.get("risk_review"),
        }

    items_with_options = [_with_admin_notes(it) for it in items]
    old_items_with_options = [_with_admin_notes(it) for it in br.inactive_items(db, cfg, content_id)]

    history = br.review_history(db, content_id)
    latest = history[0] if history else None
    stale = br.review_is_stale(db, cfg, latest, content) if latest else None

    return templates.TemplateResponse(request, "vocabulary_quiz/grade5_l3_batch1_review_detail.html", {
        "request": request, "content": content, "current_batch": cfg,
        "items_with_options": items_with_options,
        "old_items_with_options": old_items_with_options,
        "held": br.is_held(cfg, content_id), "hold_reason": br.hold_reason(cfg, content_id),
        "human_level_note": br.human_level_note(content_id),
        "risk_info": br.risk_review_info(db, cfg, content_id),
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
    content, cfg = _resolve_content_any_batch(db, content_id)
    if verdict not in VERDICT_CHOICES:
        raise HTTPException(status_code=400, detail="알 수 없는 판정 값입니다")

    br.save_review(db, cfg, content_id, verdict, rationale.strip(), admin_user_id, _reviewer_email(admin_user_id))

    next_id = br.next_content_id(db, cfg, content_id)
    if next_id:
        return RedirectResponse(url=f"/vocab-grade5-l3-batch1-review/{next_id}", status_code=303)
    return RedirectResponse(url=f"/vocab-grade5-l3-batch1-review/?batch={cfg.batch_id}", status_code=303)
