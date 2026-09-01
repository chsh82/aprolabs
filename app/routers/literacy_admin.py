"""문해력 DB 관리자용 라우터.

GET /literacy/health -> terms 테이블 행 수
Phase 6(검수 UI, docs/literacy/06-검수UI.md)부터 /literacy/review/* 추가.
전부 관리자용 - 전역 인증 미들웨어가 그대로 적용된다(화이트리스트 미수정).
"""
from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.literacy.db import get_literacy_db
from app.literacy.models import Term, QuizItem

router = APIRouter(prefix="/literacy")
templates = Jinja2Templates(directory="app/templates")


@router.get("/health")
def health(db: Session = Depends(get_literacy_db)):
    terms_count = db.query(func.count(Term.id)).scalar()
    return {"status": "ok", "terms_count": terms_count}


# ── 공통 상수 ──

QUIZ_TYPE = "뜻풀이선택"
REJECT_REASONS = [
    ("정답이 틀림", "정답이 틀림"),
    ("오답이 너무 쉬움", "오답이 너무 쉬움 (소거법으로 풀림)"),
    ("오답이 정답에 가까움", "오답이 정답에 가까움 (정답이 둘)"),
    ("표기 문제", "표기 문제 (숫자 접미사 등)"),
    ("레벨 부적절", "레벨 부적절"),
    ("기타", "기타 (메모 필수)"),
]


def _base_ctx(request: Request, **kwargs):
    return {"request": request, **kwargs}


# ── 허브 ──

@router.get("/review")
def review_hub(request: Request, db: Session = Depends(get_literacy_db)):
    quiz_total = db.query(func.count(QuizItem.id)).filter(
        QuizItem.quiz_type == QUIZ_TYPE,
        (QuizItem.review_status == "검수전") | (QuizItem.reviewed_at.isnot(None)),
    ).scalar()
    quiz_done = db.query(func.count(QuizItem.id)).filter(
        QuizItem.quiz_type == QUIZ_TYPE, QuizItem.reviewed_at.isnot(None)
    ).scalar()

    def_total = db.query(func.count(Term.id)).filter(
        Term.category == "어휘",
        ((Term.definition.is_(None)) | (Term.definition == "")) | (Term.reviewed_at.isnot(None)),
    ).scalar()
    def_done = db.query(func.count(Term.id)).filter(
        Term.category == "어휘", Term.reviewed_at.isnot(None)
    ).scalar()

    level_total = db.query(func.count(Term.id)).filter(
        Term.category.in_(["속담", "관용구"]),
        (Term.level.is_(None)) | (Term.reviewed_at.isnot(None)),
    ).scalar()
    level_done = db.query(func.count(Term.id)).filter(
        Term.category.in_(["속담", "관용구"]), Term.reviewed_at.isnot(None)
    ).scalar()

    category_counts = dict(
        db.query(Term.category, func.count(Term.id)).group_by(Term.category).all()
    )
    level_counts = dict(
        db.query(Term.level, func.count(Term.id)).group_by(Term.level).all()
    )
    review_status_counts = dict(
        db.query(Term.review_status, func.count(Term.id)).group_by(Term.review_status).all()
    )

    recent_terms = (
        db.query(Term)
        .filter(Term.reviewed_at.isnot(None))
        .order_by(Term.reviewed_at.desc())
        .limit(20)
        .all()
    )
    recent_quiz = (
        db.query(QuizItem)
        .filter(QuizItem.reviewed_at.isnot(None))
        .order_by(QuizItem.reviewed_at.desc())
        .limit(20)
        .all()
    )
    recent = sorted(
        [{"kind": "term", "headword": t.headword, "at": t.reviewed_at} for t in recent_terms]
        + [{"kind": "quiz", "headword": None, "id": q.id, "at": q.reviewed_at} for q in recent_quiz],
        key=lambda r: r["at"],
        reverse=True,
    )[:20]

    ctx = _base_ctx(
        request,
        quiz_total=quiz_total, quiz_done=quiz_done,
        def_total=def_total, def_done=def_done,
        level_total=level_total, level_done=level_done,
        category_counts=category_counts, level_counts=level_counts,
        review_status_counts=review_status_counts,
        recent=recent,
    )
    return templates.TemplateResponse("literacy/review_hub.html", ctx)


# ── 화면① 문항 검수 ──

def _quiz_base_query(db: Session, level: str | None, source: str | None, category: str | None):
    q = db.query(QuizItem, Term).join(Term, Term.id == QuizItem.term_id).filter(
        QuizItem.quiz_type == QUIZ_TYPE
    )
    if level:
        q = q.filter(Term.level == int(level))
    if source:
        q = q.filter(Term.source == source)
    if category:
        q = q.filter(Term.category == category)
    return q


@router.get("/review/quiz")
def review_quiz(
    request: Request,
    id: int | None = None,
    level: str | None = None,
    source: str | None = None,
    category: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    base = _quiz_base_query(db, level, source, category)

    if id is not None:
        row = base.filter(QuizItem.id == id).first()
    else:
        row = base.filter(QuizItem.review_status == "검수전").order_by(QuizItem.id.asc()).first()

    prev_row = None
    next_row = None
    if row:
        quiz, term = row
        prev_row = base.filter(QuizItem.id < quiz.id).order_by(QuizItem.id.desc()).first()
        # 다음: 아직 검수 안 된 것 우선, 없으면 그냥 다음 id
        next_row = (
            base.filter(QuizItem.id > quiz.id, QuizItem.review_status == "검수전")
            .order_by(QuizItem.id.asc()).first()
            or base.filter(QuizItem.id > quiz.id).order_by(QuizItem.id.asc()).first()
        )

    total = base.filter(
        (QuizItem.review_status == "검수전") | (QuizItem.reviewed_at.isnot(None))
    ).count()
    done = base.filter(QuizItem.reviewed_at.isnot(None)).count()

    distractors = json.loads(row[0].distractors) if row and row[0].distractors else []

    ctx = _base_ctx(
        request,
        quiz=row[0] if row else None,
        term=row[1] if row else None,
        distractors=distractors,
        prev_id=prev_row[0].id if prev_row else None,
        next_id=next_row[0].id if next_row else None,
        total=total, done=done,
        level=level, source=source, category=category,
        reject_reasons=REJECT_REASONS,
    )
    return templates.TemplateResponse("literacy/review_quiz.html", ctx)


@router.post("/review/quiz/save")
async def review_quiz_save(request: Request, db: Session = Depends(get_literacy_db)):
    body = await request.json()
    quiz_id = body.get("id")
    verdict = body.get("verdict")  # O / X / 보류
    reject_reason = body.get("reject_reason")
    note = body.get("note")

    quiz = db.query(QuizItem).filter(QuizItem.id == quiz_id).first()
    if not quiz:
        return {"ok": False, "error": "문항을 찾을 수 없음"}

    status_map = {"O": "검수완료", "X": "제외", "보류": "보류"}
    if verdict not in status_map:
        return {"ok": False, "error": f"알 수 없는 판정: {verdict}"}
    if verdict == "X" and not reject_reason:
        return {"ok": False, "error": "X 판정은 사유가 필요합니다"}

    quiz.review_status = status_map[verdict]
    quiz.reject_reason = reject_reason if verdict == "X" else None
    quiz.note = note
    quiz.reviewed_at = datetime.now()
    db.commit()
    return {"ok": True}


@router.get("/review/quiz/stats")
def review_quiz_stats(request: Request, db: Session = Depends(get_literacy_db)):
    status_counts = dict(
        db.query(QuizItem.review_status, func.count(QuizItem.id))
        .filter(QuizItem.quiz_type == QUIZ_TYPE)
        .group_by(QuizItem.review_status)
        .all()
    )
    reason_counts = dict(
        db.query(QuizItem.reject_reason, func.count(QuizItem.id))
        .filter(QuizItem.quiz_type == QUIZ_TYPE, QuizItem.reject_reason.isnot(None))
        .group_by(QuizItem.reject_reason)
        .all()
    )
    ctx = _base_ctx(request, status_counts=status_counts, reason_counts=reason_counts)
    return templates.TemplateResponse("literacy/review_quiz_stats.html", ctx)


# ── 화면② 뜻풀이 작성 ──

def _definition_base_query(db: Session, level: str | None, source: str | None):
    q = db.query(Term).filter(Term.category == "어휘")
    if level:
        q = q.filter(Term.level == int(level))
    if source:
        q = q.filter(Term.source == source)
    return q


def _parse_sub_category(note: str | None) -> str | None:
    if not note:
        return None
    import re
    m = re.search(r"소분류:\s*([^/]+)", note)
    return m.group(1).strip() if m else None


@router.get("/review/definition")
def review_definition(
    request: Request,
    id: int | None = None,
    level: str | None = None,
    source: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    base = _definition_base_query(db, level, source)
    empty = lambda q: q.filter((Term.definition.is_(None)) | (Term.definition == ""))

    if id is not None:
        term = base.filter(Term.id == id).first()
    else:
        term = empty(base).order_by(Term.id.asc()).first()

    prev_term = None
    next_term = None
    references = []
    if term:
        prev_term = base.filter(Term.id < term.id).order_by(Term.id.desc()).first()
        next_term = (
            empty(base).filter(Term.id > term.id).order_by(Term.id.asc()).first()
            or base.filter(Term.id > term.id).order_by(Term.id.asc()).first()
        )
        sub_cat = _parse_sub_category(term.note)
        if sub_cat:
            candidates = (
                db.query(Term)
                .filter(Term.id != term.id, Term.definition.isnot(None), Term.definition != "")
                .filter(Term.note.like(f"%소분류: {sub_cat}%"))
                .limit(20)
                .all()
            )
            references = candidates[:5]

    total = base.filter(
        ((Term.definition.is_(None)) | (Term.definition == "")) | (Term.reviewed_at.isnot(None))
    ).count()
    done = base.filter(Term.reviewed_at.isnot(None)).count()

    ctx = _base_ctx(
        request, term=term, references=references,
        prev_id=prev_term.id if prev_term else None,
        next_id=next_term.id if next_term else None,
        total=total, done=done, level=level, source=source,
    )
    return templates.TemplateResponse("literacy/review_definition.html", ctx)


@router.post("/review/definition/save")
async def review_definition_save(request: Request, db: Session = Depends(get_literacy_db)):
    body = await request.json()
    term_id = body.get("id")
    definition = (body.get("definition") or "").strip()

    if not definition:
        return {"ok": False, "error": "뜻풀이를 입력하세요"}

    term = db.query(Term).filter(Term.id == term_id).first()
    if not term:
        return {"ok": False, "error": "표제어를 찾을 수 없음"}

    term.definition = definition
    term.review_status = "검수완료"
    term.reviewed_at = datetime.now()
    db.commit()
    return {"ok": True}


# ── 화면③ 레벨 부여 ──

def _level_base_query(db: Session, category: str | None):
    q = db.query(Term).filter(Term.category.in_(["속담", "관용구"]))
    if category:
        q = q.filter(Term.category == category)
    return q


@router.get("/review/level")
def review_level(
    request: Request,
    id: int | None = None,
    category: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    base = _level_base_query(db, category)
    pending = lambda q: q.filter(Term.level.is_(None), Term.review_status != "제외")

    if id is not None:
        term = base.filter(Term.id == id).first()
    else:
        term = pending(base).order_by(func.length(Term.definition).asc(), Term.id.asc()).first()

    prev_term = None
    next_term = None
    similar = []
    if term:
        cur_len = len(term.definition or "")
        prev_term = (
            base.filter(
                (func.length(Term.definition) < cur_len)
                | ((func.length(Term.definition) == cur_len) & (Term.id < term.id))
            )
            .order_by(func.length(Term.definition).desc(), Term.id.desc())
            .first()
        )
        next_term = (
            pending(base)
            .filter(
                (func.length(Term.definition) > cur_len)
                | ((func.length(Term.definition) == cur_len) & (Term.id > term.id))
            )
            .order_by(func.length(Term.definition).asc(), Term.id.asc())
            .first()
        )
        if term.level is not None:
            similar = (
                db.query(Term)
                .filter(Term.category == term.category, Term.level == term.level, Term.id != term.id)
                .limit(5)
                .all()
            )

    total = base.filter(
        (Term.level.is_(None)) | (Term.reviewed_at.isnot(None))
    ).count()
    done = base.filter(Term.reviewed_at.isnot(None)).count()

    ctx = _base_ctx(
        request, term=term, similar=similar,
        prev_id=prev_term.id if prev_term else None,
        next_id=next_term.id if next_term else None,
        total=total, done=done, category=category,
    )
    return templates.TemplateResponse("literacy/review_level.html", ctx)


@router.post("/review/level/save")
async def review_level_save(request: Request, db: Session = Depends(get_literacy_db)):
    body = await request.json()
    term_id = body.get("id")
    level = body.get("level")  # 0~6 정수, 또는 None("해당 없음")

    term = db.query(Term).filter(Term.id == term_id).first()
    if not term:
        return {"ok": False, "error": "표제어를 찾을 수 없음"}

    if level is None:
        term.review_status = "제외"
    else:
        term.level = int(level)
        term.grade_source = "manual"
        term.review_status = "검수완료"
    term.reviewed_at = datetime.now()
    db.commit()
    return {"ok": True}


# ── 결과 보기(목록) - 검수 끝난 내용을 훑어보는 화면. 한 건씩 판정하는
# review_quiz/definition/level과는 목적이 다르다(판정 아니라 결과 확인이라
# 지시서의 "목록형 화면 금지"는 여기 해당 안 됨) ──

PAGE_SIZE = 100


@router.get("/review/level/results")
def review_level_results(
    request: Request,
    page: int = 1,
    category: str | None = None,
    level: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    q = db.query(Term).filter(Term.category.in_(["속담", "관용구"]), Term.reviewed_at.isnot(None))
    if category:
        q = q.filter(Term.category == category)
    if level == "제외":
        q = q.filter(Term.review_status == "제외")
    elif level:
        q = q.filter(Term.level == int(level))

    total = q.count()
    rows = (
        q.order_by(Term.level.is_(None), Term.level, Term.id)
        .offset((page - 1) * PAGE_SIZE)
        .limit(PAGE_SIZE)
        .all()
    )
    ctx = _base_ctx(
        request, rows=rows, total=total, page=page,
        total_pages=max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        category=category, level=level,
    )
    return templates.TemplateResponse("literacy/results_level.html", ctx)


@router.get("/review/definition/results")
def review_definition_results(
    request: Request,
    page: int = 1,
    level: str | None = None,
    source: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    q = db.query(Term).filter(
        Term.category == "어휘", Term.reviewed_at.isnot(None),
        Term.note.like("%AI 자동 생성 뜻풀이%"),
    )
    if level:
        q = q.filter(Term.level == int(level))
    if source:
        q = q.filter(Term.source == source)

    total = q.count()
    rows = q.order_by(Term.id).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()
    ctx = _base_ctx(
        request, rows=rows, total=total, page=page,
        total_pages=max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        level=level, source=source,
    )
    return templates.TemplateResponse("literacy/results_definition.html", ctx)


@router.get("/review/quiz/results")
def review_quiz_results(
    request: Request,
    page: int = 1,
    review_status: str | None = None,
    reject_reason: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    q = (
        db.query(QuizItem, Term)
        .join(Term, Term.id == QuizItem.term_id)
        .filter(QuizItem.quiz_type == QUIZ_TYPE, QuizItem.reviewed_at.isnot(None))
    )
    if review_status:
        q = q.filter(QuizItem.review_status == review_status)
    if reject_reason:
        q = q.filter(QuizItem.reject_reason == reject_reason)

    total = q.count()
    rows = q.order_by(QuizItem.id).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()
    ctx = _base_ctx(
        request, rows=rows, total=total, page=page,
        total_pages=max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        review_status=review_status, reject_reason=reject_reason,
        reject_reasons=REJECT_REASONS,
    )
    return templates.TemplateResponse("literacy/results_quiz.html", ctx)


# ── 전체 조회 - terms 7,087건 전체를 카테고리(어휘/속담/관용구) 무관하게
# 훑어보는 범용 화면. 위 results_*는 "AI가 손댄 것"만 좁게 보여주는 반면,
# 이건 AI가 손댔는지와 무관하게 DB에 있는 걸 전부 보여준다. ──

@router.get("/terms")
def terms_browse(
    request: Request,
    page: int = 1,
    category: str | None = None,
    level: str | None = None,
    source: str | None = None,
    q: str | None = None,
    db: Session = Depends(get_literacy_db),
):
    query = db.query(Term)
    if category:
        query = query.filter(Term.category == category)
    if level:
        query = query.filter(Term.level == int(level))
    if source:
        query = query.filter(Term.source == source)
    if q:
        query = query.filter(Term.headword.contains(q))

    total = query.count()
    rows = query.order_by(Term.id).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()

    category_counts = dict(db.query(Term.category, func.count(Term.id)).group_by(Term.category).all())
    source_counts = dict(db.query(Term.source, func.count(Term.id)).group_by(Term.source).all())

    ctx = _base_ctx(
        request, rows=rows, total=total, page=page,
        total_pages=max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        category=category, level=level, source=source, q=q,
        category_counts=category_counts, source_counts=source_counts,
    )
    return templates.TemplateResponse("literacy/terms_browse.html", ctx)
