"""어휘 퀴즈 MVP - 관리자 전용, 학생 비공개, R&D 전용.

문항 출제 조건: source_version=2.1.29, generation_status IN
(PRIVATE_SERVER_READY, PRIVATE_SERVER_READY_CANDIDATE), is_active=1 -
화이트리스트 방식이라 AUTO_HOLD는 애초에 나올 수 없다.

채점은 항상 서버가 DB의 vocabulary_items.correct_option을 기준으로 한다
- 클라이언트가 POST /answer에 보낸 값은 "이 학생이 고른 번호"로만 쓰고,
정답 여부 판정에는 절대 쓰지 않는다.

세션 시작 시 문항 20개(중복 없이)를 뽑아 vocabulary_quiz_attempts에
전부 미응답 상태(selected_option=NULL)로 먼저 만들어 둔다 - "다음 문제"는
그중 order_index가 가장 작은 미응답 행을 찾는 것으로 자연스럽게 처리되고,
새로고침/재접속해도 같은 로직이 같은 지점을 가리킨다(별도 세션 상태
캐시가 필요 없음)."""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models import (
    VocabularyContent,
    VocabularyItem,
    VocabularyQuizAttempt,
    VocabularyQuizSession,
)

router = APIRouter(prefix="/vocabulary-quiz")
templates = Jinja2Templates(directory="app/templates")

QUESTION_COUNT = 20
SOURCE_VERSION = "2.1.29"
ELIGIBLE_STATUSES = ("PRIVATE_SERVER_READY", "PRIVATE_SERVER_READY_CANDIDATE")


def _select_question_items(db: Session, n: int) -> list[str]:
    rows = (
        db.query(VocabularyItem.item_id)
        .join(VocabularyContent, VocabularyContent.content_id == VocabularyItem.content_id)
        .filter(
            VocabularyItem.source_version == SOURCE_VERSION,
            VocabularyContent.source_version == SOURCE_VERSION,
            VocabularyContent.generation_status.in_(ELIGIBLE_STATUSES),
            VocabularyItem.is_active == 1,
            VocabularyContent.is_active == 1,
        )
        .all()
    )
    ids = [r[0] for r in rows]
    if len(ids) < n:
        raise HTTPException(status_code=500, detail=f"출제 가능한 문항이 부족합니다 ({len(ids)}/{n})")
    return random.sample(ids, n)


@router.get("/play")
def play_start(request: Request, db: Session = Depends(get_vocabulary_quiz_db),
               admin: str = Depends(require_admin)):
    in_progress = (
        db.query(VocabularyQuizSession)
        .filter(VocabularyQuizSession.user_id == admin, VocabularyQuizSession.status == "in_progress")
        .order_by(VocabularyQuizSession.started_at.desc())
        .first()
    )
    return templates.TemplateResponse("vocabulary_quiz/play.html", {
        "request": request, "question_count": QUESTION_COUNT, "source_version": SOURCE_VERSION,
        "in_progress": in_progress,
    }, headers=NOINDEX_HEADERS)


@router.post("/play")
def play_create(request: Request, db: Session = Depends(get_vocabulary_quiz_db),
                 admin: str = Depends(require_admin)):
    item_ids = _select_question_items(db, QUESTION_COUNT)
    now = datetime.now(timezone.utc).isoformat()
    session_id = str(uuid.uuid4())

    session = VocabularyQuizSession(
        id=session_id, user_id=admin, source_version=SOURCE_VERSION,
        question_count=QUESTION_COUNT, correct_count=0, status="in_progress", started_at=now,
    )
    db.add(session)
    db.flush()  # session INSERT를 먼저 내보내지 않으면 SQLite executemany 배치 경로에서
    # attempts의 session_id FK가 아직 없는 행을 가리켜 FOREIGN KEY constraint failed가 남 -
    # 실제로 재현해서 확인한 문제라 explicit flush로 순서를 강제한다.

    items = {row.item_id: row for row in db.query(VocabularyItem).filter(VocabularyItem.item_id.in_(item_ids)).all()}
    for idx, item_id in enumerate(item_ids, start=1):
        db.add(VocabularyQuizAttempt(
            session_id=session_id, item_id=item_id, order_index=idx,
            correct_option=items[item_id].correct_option,
        ))
    db.commit()

    return RedirectResponse(f"/vocabulary-quiz/session/{session_id}", status_code=303)


def _get_owned_session(db: Session, session_id: str, admin: str) -> VocabularyQuizSession:
    session = db.query(VocabularyQuizSession).filter(VocabularyQuizSession.id == session_id).first()
    if session is None:
        raise HTTPException(status_code=404, detail="세션을 찾을 수 없습니다")
    if session.user_id != admin:
        raise HTTPException(status_code=403, detail="본인 세션만 볼 수 있습니다")
    return session


@router.get("/session/{session_id}")
def session_question(request: Request, session_id: str, db: Session = Depends(get_vocabulary_quiz_db),
                      admin: str = Depends(require_admin)):
    session = _get_owned_session(db, session_id, admin)
    if session.status == "completed":
        return RedirectResponse(f"/vocabulary-quiz/session/{session_id}/result", status_code=303)

    attempt = (
        db.query(VocabularyQuizAttempt)
        .filter(VocabularyQuizAttempt.session_id == session_id, VocabularyQuizAttempt.selected_option.is_(None))
        .order_by(VocabularyQuizAttempt.order_index.asc())
        .first()
    )
    if attempt is None:
        # 전부 응답됐는데 세션이 아직 in_progress인 경우(이론상 없어야 하지만 방어) - 완료 처리
        session.status = "completed"
        session.completed_at = datetime.now(timezone.utc).isoformat()
        db.commit()
        return RedirectResponse(f"/vocabulary-quiz/session/{session_id}/result", status_code=303)

    item = db.query(VocabularyItem).filter(VocabularyItem.item_id == attempt.item_id).first()
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == item.content_id).first()

    return templates.TemplateResponse("vocabulary_quiz/session_question.html", {
        "request": request, "session": session, "attempt": attempt, "item": item, "content": content,
    }, headers=NOINDEX_HEADERS)


@router.post("/session/{session_id}/answer")
def session_answer(
    request: Request,
    session_id: str,
    item_id: str = Form(...),
    selected_option: int = Form(...),
    db: Session = Depends(get_vocabulary_quiz_db),
    admin: str = Depends(require_admin),
):
    session = _get_owned_session(db, session_id, admin)
    if session.status == "completed":
        raise HTTPException(status_code=409, detail="이미 완료된 세션입니다")
    if selected_option not in (1, 2, 3, 4):
        raise HTTPException(status_code=400, detail="선택지는 1~4여야 합니다")

    attempt = (
        db.query(VocabularyQuizAttempt)
        .filter(VocabularyQuizAttempt.session_id == session_id, VocabularyQuizAttempt.item_id == item_id)
        .first()
    )
    if attempt is None:
        raise HTTPException(status_code=404, detail="문항을 찾을 수 없습니다")
    if attempt.selected_option is not None:
        raise HTTPException(status_code=409, detail="이미 답변한 문항입니다 - 변경할 수 없습니다")

    # 채점은 항상 서버가 DB의 correct_option 기준으로 한다(클라이언트 값 신뢰 안 함)
    is_correct = 1 if selected_option == attempt.correct_option else 0
    attempt.selected_option = selected_option
    attempt.is_correct = is_correct
    attempt.answered_at = datetime.now(timezone.utc).isoformat()
    if is_correct:
        session.correct_count += 1

    # SessionLocal이 autoflush=False라(app/vocabulary_quiz/db.py) 위에서 바꾼
    # attempt.selected_option이 flush 전이면 아래 count 쿼리가 이 행을 여전히
    # "미응답"으로 센다 - 마지막 문제에서 remaining이 1 남은 것처럼 잘못 계산돼
    # is_last가 False가 되는 버그를 실제로 재현해서 여기서 고쳤다.
    db.flush()

    remaining = (
        db.query(VocabularyQuizAttempt)
        .filter(VocabularyQuizAttempt.session_id == session_id, VocabularyQuizAttempt.selected_option.is_(None))
        .count()
    )
    is_last = remaining == 0
    if is_last:
        session.status = "completed"
        session.completed_at = datetime.now(timezone.utc).isoformat()

    db.commit()

    item = db.query(VocabularyItem).filter(VocabularyItem.item_id == item_id).first()
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == item.content_id).first()

    return templates.TemplateResponse("vocabulary_quiz/session_feedback.html", {
        "request": request, "session": session, "attempt": attempt, "item": item, "content": content,
        "is_last": is_last,
    }, headers=NOINDEX_HEADERS)


@router.get("/session/{session_id}/result")
def session_result(request: Request, session_id: str, db: Session = Depends(get_vocabulary_quiz_db),
                    admin: str = Depends(require_admin)):
    session = _get_owned_session(db, session_id, admin)
    if session.status != "completed":
        return RedirectResponse(f"/vocabulary-quiz/session/{session_id}", status_code=303)

    attempts = (
        db.query(VocabularyQuizAttempt)
        .filter(VocabularyQuizAttempt.session_id == session_id)
        .order_by(VocabularyQuizAttempt.order_index.asc())
        .all()
    )
    wrong = [a for a in attempts if a.is_correct == 0]

    wrong_details = []
    for a in wrong:
        item = db.query(VocabularyItem).filter(VocabularyItem.item_id == a.item_id).first()
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == item.content_id).first()
        wrong_details.append({"attempt": a, "item": item, "content": content})

    total = session.question_count
    correct = session.correct_count
    accuracy = round(correct / total * 100, 1) if total else 0.0

    return templates.TemplateResponse("vocabulary_quiz/session_result.html", {
        "request": request, "session": session, "total": total, "correct": correct,
        "wrong_count": len(wrong), "accuracy": accuracy, "wrong_details": wrong_details,
    }, headers=NOINDEX_HEADERS)
