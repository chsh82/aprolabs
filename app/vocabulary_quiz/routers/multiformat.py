"""초등 다유형 어휘 퀴즈(파일럿) - 관리자 전용, 학생 비공개, R&D 전용.

기존 app/vocabulary_quiz/routers/quiz.py(단일 4지선다 MVP, /vocabulary-quiz/play)와는
완전히 별도 라우터·테이블을 쓴다 - 섞이지 않는다. 이 모듈의 URL도 그래서
/vocabulary-quiz/play가 아니라 /vocabulary-quiz/multiformat/play를 쓴다
(기존 경로와 충돌 방지 - CLAUDE_CODE_MIGRATION.md 권장 URL과 다른 부분이지만
기존 코드를 덮어쓰지 않기 위한 불가피한 조정).

JSON API는 요청받은 계약대로 /api/vocabulary-quiz/... 경로를 그대로 쓴다.

채점은 항상 서버가 vocabulary_multiformat_items.answer_payload_json 기준으로
한다 - 클라이언트가 보낸 값은 "학생이 제출한 응답"으로만 저장하고 정답 판정에는
쓰지 않는다. 문제 조회 응답(GET .../next)에는 정답 관련 필드(correct_option,
answer_text, accepted_answers, 연결형 answers)를 절대 포함하지 않는다 - 제출
후(POST .../answer) 응답에만 정답·해설을 담는다.

직접입력형(CONTEXT_CLOZE) 채점: 앞뒤 공백 제거 + Unicode NFC 정규화 후
accepted_answers와 정확히 비교한다. 유사어/LLM 판정은 쓰지 않는다.

문맥빈칸(CONTEXT_CLOZE)은 최대 2회 시도를 허용한다 - 1차 시도가 틀리면
정답 처리를 확정하지 않고(응답 행이 계속 "미응답" 상태로 남아 GET .../next가
같은 문항을 다시 내려준다) 초성 힌트를 자동으로 공개한다. 학생이 스스로
POST .../hint를 호출해 1차 시도 전에 미리 힌트를 볼 수도 있다 - 힌트 열람은
시도 횟수를 소모하지 않는다. 2차 시도(정답이든 오답이든)에서 비로소 확정
(answered_at 기록)한다. 초성은 정답을 완전히 노출하지 않는 부분 힌트이지만
그래도 서버가 요청 시점에만 계산해서 내려주고 문제 조회 응답에 기본으로
포함하지 않는다(힌트를 이미 본 문항만 GET .../next에도 다시 포함).

연결형(MATCH_WORD_MEANING) 채점: 4개 낱말-뜻 매핑을 전부 비교해 correct_count
(0~4)를 반환한다 - is_correct는 4/4일 때만 1.

십자말(CROSSWORD)은 100세트 중 한 세트를 출제하는 전용 모드다 - 문항 수
선택과 무관하게 항상 1세트, 다른 유형과 섞이지 않는다(item_types에
CROSSWORD와 다른 유형을 함께 넣으면 400). 가능하면 같은 관리자가 직전에
푼 세트를 바로 다시 출제하지 않는다. 채점은 좌표(row,col) 단위로 정규화한
글자를 비교하고, 활성 칸이 전부 맞아야 세트 전체 정답 - 칸별 정오와 부분
점수(correct_count/total_count)를 함께 반환한다. MATCH_WORD_MEANING처럼
단일 시도로 즉시 확정한다(CONTEXT_CLOZE 같은 재시도는 없음).

vocabulary_multiformat_items.public_payload_json에는 유형별로 클라이언트에
그대로 내려줘도 되는 표시 정보가 import 시점에 미리 계산돼 있다(선택형
options / 빈칸 input_hint / 연결형 words·definitions / 십자말 rows·cols·
활성칸·entries(정답 제외)) - API는 매 요청마다 answer_payload_json에서
다시 골라내는 대신 이 컬럼을 그대로 쓰고, 세션 상태에 따라 달라지는 부분
(문맥빈칸 힌트, 연결형 뜻 순서 섞기)만 요청 시점에 덧붙인다.
"""
from __future__ import annotations

import json
import random
import unicodedata
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.vocabulary_quiz.auth import NOINDEX_HEADERS, require_admin
from app.vocabulary_quiz.db import get_vocabulary_quiz_db
from app.vocabulary_quiz.models import (
    VocabularyMultiformatItem,
    VocabularyMultiformatResponse,
    VocabularyMultiformatSession,
)

api_router = APIRouter(prefix="/api/vocabulary-quiz")
page_router = APIRouter(prefix="/vocabulary-quiz/multiformat")
templates = Jinja2Templates(directory="app/templates")

SOURCE_VERSION = "2.1.29"
DEFAULT_QUESTION_COUNT = 10
ITEM_TYPES = (
    "MEANING_CHOICE", "WORD_FROM_DEFINITION", "CONTEXT_MEANING",
    "CONTEXT_CLOZE", "MATCH_WORD_MEANING", "CROSSWORD",
)
CHOICE_TYPES = ("MEANING_CHOICE", "WORD_FROM_DEFINITION", "CONTEXT_MEANING")
MAX_CLOZE_ATTEMPTS = 2

_CHOSEONG_LIST = list("ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ")


def _to_choseong(word: str) -> str:
    """완성형 한글 음절만 초성으로 바꾸고, 그 외 문자(공백 등)는 그대로 둔다."""
    result = []
    for ch in word:
        code = ord(ch) - 0xAC00
        if 0 <= code <= 11171:
            result.append(_CHOSEONG_LIST[code // (21 * 28)])
        else:
            result.append(ch)
    return "".join(result)


# ==================== 요청 바디 스키마 ====================

class CreateSessionBody(BaseModel):
    item_types: list[str] | None = None
    question_count: int = DEFAULT_QUESTION_COUNT


class AnswerBody(BaseModel):
    item_id: str
    selected_option: int | None = None
    answer_text: str | None = None
    answers: dict[str, str] | None = None
    cells: dict[str, str] | None = None


class HintBody(BaseModel):
    item_id: str


# ==================== 문항 선택 ====================

def _select_question_items(db: Session, n: int, item_types: list[str] | None) -> list[str]:
    q = db.query(VocabularyMultiformatItem.item_id).filter(
        VocabularyMultiformatItem.source_version == SOURCE_VERSION,
        VocabularyMultiformatItem.is_active == 1,
    )
    if item_types:
        q = q.filter(VocabularyMultiformatItem.item_type.in_(item_types))
    else:
        # 기본 혼합 모드는 CROSSWORD를 포함하지 않는다 - 세트 하나가 10개 칸짜리라
        # "문항 N개" 혼합 세션에 자연스럽게 섞이지 않는다. CROSSWORD는 단독 모드로만 출제한다.
        q = q.filter(VocabularyMultiformatItem.item_type != "CROSSWORD")
    ids = [r[0] for r in q.all()]
    if len(ids) < n:
        raise HTTPException(status_code=400, detail=f"출제 가능한 문항이 부족합니다 ({len(ids)}/{n})")
    return random.sample(ids, n)


def _last_crossword_item_id(db: Session, admin: str) -> str | None:
    row = (
        db.query(VocabularyMultiformatResponse.item_id)
        .join(VocabularyMultiformatSession, VocabularyMultiformatSession.id == VocabularyMultiformatResponse.session_id)
        .filter(VocabularyMultiformatSession.user_id == admin, VocabularyMultiformatResponse.item_type == "CROSSWORD")
        .order_by(VocabularyMultiformatSession.started_at.desc())
        .first()
    )
    return row[0] if row else None


def _select_crossword_item(db: Session, admin: str) -> str:
    ids = [r[0] for r in db.query(VocabularyMultiformatItem.item_id).filter(
        VocabularyMultiformatItem.source_version == SOURCE_VERSION,
        VocabularyMultiformatItem.is_active == 1,
        VocabularyMultiformatItem.item_type == "CROSSWORD",
    ).all()]
    if not ids:
        raise HTTPException(status_code=400, detail="출제 가능한 십자말 세트가 없습니다")
    last_id = _last_crossword_item_id(db, admin)
    pool = [i for i in ids if i != last_id] if last_id and len(ids) > 1 else ids
    return random.choice(pool)


def _get_owned_session(db: Session, session_id: str, admin: str) -> VocabularyMultiformatSession:
    session = db.query(VocabularyMultiformatSession).filter(
        VocabularyMultiformatSession.id == session_id
    ).first()
    if session is None:
        raise HTTPException(status_code=404, detail="세션을 찾을 수 없습니다")
    if session.user_id != admin:
        raise HTTPException(status_code=403, detail="본인 세션만 볼 수 있습니다")
    return session


# ==================== 문제 조회용 payload (정답 필드 제외) ====================

def _public_item_payload(item: VocabularyMultiformatItem,
                          response: VocabularyMultiformatResponse | None = None) -> dict:
    public = json.loads(item.public_payload_json) if item.public_payload_json else {}
    base = {
        "item_id": item.item_id,
        "item_type": item.item_type,
        "prompt": item.prompt,
        "lemma": item.lemma,
        "pos": item.pos,
        "cognitive_level": item.cognitive_level,
    }
    if item.item_type in CHOICE_TYPES:
        base["options"] = public.get("options")
    elif item.item_type == "CONTEXT_CLOZE":
        base["input_hint"] = public.get("input_hint")
        base["attempt_count"] = response.attempt_count if response else 0
        base["attempts_left"] = MAX_CLOZE_ATTEMPTS - (response.attempt_count if response else 0)
        # 이미 힌트를 본 문항이면(1차 시도를 틀렸거나 스스로 요청) 재접속/재조회 시에도
        # 다시 보여준다 - 한 번 열람 권한을 얻은 힌트를 새로고침으로 잃게 하지 않는다.
        if response and response.hint_used:
            answer_payload = json.loads(item.answer_payload_json)
            base["choseong_hint"] = _to_choseong(answer_payload["answer_text"])
        else:
            base["choseong_hint"] = None
    elif item.item_type == "MATCH_WORD_MEANING":
        words = list(public.get("words") or [])
        definitions = list(public.get("definitions") or [])
        random.shuffle(definitions)  # 원래 순서로 노출하면 위치만으로 정답을 유추할 수 있어 매 조회마다 섞는다
        base["words"] = words
        base["definitions"] = definitions
    elif item.item_type == "CROSSWORD":
        base["rows"] = public.get("rows")
        base["cols"] = public.get("cols")
        base["cells"] = public.get("cells")
        base["entries"] = public.get("entries")
    return base


# ==================== 채점 ====================

def _grade(item: VocabularyMultiformatItem, body: AnswerBody) -> dict:
    """반환: {is_correct, correct_count, total_count, submitted}"""
    payload = json.loads(item.answer_payload_json)
    t = item.item_type

    if t in CHOICE_TYPES:
        if body.selected_option not in (1, 2, 3, 4):
            raise HTTPException(status_code=400, detail="selected_option은 1~4여야 합니다")
        is_correct = 1 if body.selected_option == payload["correct_option"] else 0
        return {"is_correct": is_correct, "correct_count": None, "total_count": None,
                "submitted": {"selected_option": body.selected_option}}

    if t == "CONTEXT_CLOZE":
        submitted_text = (body.answer_text or "").strip()
        normalized = unicodedata.normalize("NFC", submitted_text)
        accepted = [unicodedata.normalize("NFC", a.strip()) for a in (payload.get("accepted_answers") or [])]
        is_correct = 1 if normalized and normalized in accepted else 0
        return {"is_correct": is_correct, "correct_count": None, "total_count": None,
                "submitted": {"answer_text": submitted_text}}

    if t == "MATCH_WORD_MEANING":
        submitted_answers = body.answers or {}
        correct_map: dict = payload.get("answers") or {}
        correct_count = sum(1 for w, d in correct_map.items() if submitted_answers.get(w) == d)
        total_count = len(correct_map)
        is_correct = 1 if correct_count == total_count else 0
        return {"is_correct": is_correct, "correct_count": correct_count, "total_count": total_count,
                "submitted": {"answers": submitted_answers}}

    if t == "CROSSWORD":
        submitted_cells = body.cells or {}
        correct_cells: dict = payload.get("cells") or {}
        cell_results = {}
        correct_count = 0
        for key, correct_char in correct_cells.items():
            submitted_char = unicodedata.normalize("NFC", (submitted_cells.get(key) or "").strip())
            ok = submitted_char == unicodedata.normalize("NFC", correct_char)
            cell_results[key] = ok
            if ok:
                correct_count += 1
        total_count = len(correct_cells)
        is_correct = 1 if correct_count == total_count else 0
        return {"is_correct": is_correct, "correct_count": correct_count, "total_count": total_count,
                "submitted": {"cells": submitted_cells}, "cell_results": cell_results}

    raise HTTPException(status_code=500, detail=f"알 수 없는 item_type: {t}")


def _correct_answer_payload(item: VocabularyMultiformatItem) -> dict:
    """정답 제출 후에만 내려주는 정답 정보 - 문제 조회(next) 응답에는 절대 포함하지 않는다."""
    payload = json.loads(item.answer_payload_json)
    t = item.item_type
    if t in CHOICE_TYPES:
        options = json.loads(item.options_json)
        correct_option = payload["correct_option"]
        return {"correct_option": correct_option, "correct_text": options[correct_option - 1]}
    if t == "CONTEXT_CLOZE":
        return {"answer_text": payload.get("answer_text"), "accepted_answers": payload.get("accepted_answers")}
    if t == "MATCH_WORD_MEANING":
        return {"answers": payload.get("answers")}
    if t == "CROSSWORD":
        return {"cells": payload.get("cells"), "entries": payload.get("entries")}
    return {}


# ==================== API ====================

def _apply_noindex(response: Response) -> None:
    for k, v in NOINDEX_HEADERS.items():
        response.headers[k] = v


@api_router.post("/sessions")
def create_session(body: CreateSessionBody, response: Response, db: Session = Depends(get_vocabulary_quiz_db),
                    admin: str = Depends(require_admin)):
    _apply_noindex(response)
    item_types = None
    if body.item_types:
        unknown = set(body.item_types) - set(ITEM_TYPES)
        if unknown:
            raise HTTPException(status_code=400, detail=f"알 수 없는 item_type: {sorted(unknown)}")
        item_types = body.item_types

    if item_types and "CROSSWORD" in item_types:
        if item_types != ["CROSSWORD"]:
            raise HTTPException(status_code=400,
                                 detail="CROSSWORD는 단독 모드로만 선택할 수 있습니다(다른 유형과 섞을 수 없습니다)")
        item_ids = [_select_crossword_item(db, admin)]
    else:
        if body.question_count < 1 or body.question_count > 50:
            raise HTTPException(status_code=400, detail="question_count는 1~50 사이여야 합니다")
        item_ids = _select_question_items(db, body.question_count, item_types)
    items_by_id = {
        row.item_id: row for row in
        db.query(VocabularyMultiformatItem).filter(VocabularyMultiformatItem.item_id.in_(item_ids)).all()
    }

    now = datetime.now(timezone.utc).isoformat()
    session_id = str(uuid.uuid4())
    session = VocabularyMultiformatSession(
        id=session_id, user_id=admin, source_version=SOURCE_VERSION,
        item_types_json=json.dumps(item_types, ensure_ascii=False) if item_types else None,
        question_count=len(item_ids), correct_count=0, status="in_progress", started_at=now,
    )
    db.add(session)
    db.flush()  # session INSERT를 먼저 내보내지 않으면 responses의 session_id FK가 아직 없는
    # 행을 가리켜 FOREIGN KEY constraint failed가 남 (기존 quiz.py에서 실제로 재현된 패턴과 동일)

    for idx, item_id in enumerate(item_ids, start=1):
        item = items_by_id[item_id]
        db.add(VocabularyMultiformatResponse(
            session_id=session_id, item_id=item_id, order_index=idx, item_type=item.item_type,
        ))
    db.commit()

    return {
        "session_id": session_id,
        "question_count": len(item_ids),
        "item_types": item_types,
        "source_version": SOURCE_VERSION,
    }


@api_router.get("/sessions/{session_id}/next")
def next_question(session_id: str, response: Response, db: Session = Depends(get_vocabulary_quiz_db),
                   admin: str = Depends(require_admin)):
    _apply_noindex(response)
    session = _get_owned_session(db, session_id, admin)

    answered = db.query(VocabularyMultiformatResponse).filter(
        VocabularyMultiformatResponse.session_id == session_id,
        VocabularyMultiformatResponse.answered_at.isnot(None),
    ).count()

    if session.status == "completed":
        return {"done": True, "status": "completed",
                "progress": {"answered": answered, "total": session.question_count}}

    response = (
        db.query(VocabularyMultiformatResponse)
        .filter(VocabularyMultiformatResponse.session_id == session_id,
                VocabularyMultiformatResponse.answered_at.is_(None))
        .order_by(VocabularyMultiformatResponse.order_index.asc())
        .first()
    )
    if response is None:
        # 전부 응답됐는데 세션이 아직 in_progress인 경우(이론상 없어야 하지만 방어) - 완료 처리
        session.status = "completed"
        session.completed_at = datetime.now(timezone.utc).isoformat()
        db.commit()
        return {"done": True, "status": "completed",
                "progress": {"answered": answered, "total": session.question_count}}

    item = db.query(VocabularyMultiformatItem).filter(
        VocabularyMultiformatItem.item_id == response.item_id
    ).first()

    return {
        "done": False,
        "item": _public_item_payload(item, response),
        "order_index": response.order_index,
        "progress": {"answered": answered, "total": session.question_count},
    }


def _find_response(db: Session, session_id: str, item_id: str) -> VocabularyMultiformatResponse:
    response = db.query(VocabularyMultiformatResponse).filter(
        VocabularyMultiformatResponse.session_id == session_id,
        VocabularyMultiformatResponse.item_id == item_id,
    ).first()
    if response is None:
        raise HTTPException(status_code=404, detail="문항을 찾을 수 없습니다")
    return response


@api_router.post("/sessions/{session_id}/hint")
def request_hint(session_id: str, body: HintBody, response: Response,
                  db: Session = Depends(get_vocabulary_quiz_db), admin: str = Depends(require_admin)):
    """문맥빈칸(CONTEXT_CLOZE) 전용 - 학생이 스스로 요청하는 초성 힌트.
    시도 횟수를 소모하지 않는다(attempt_count는 그대로 두고 hint_used만 1로 표시)."""
    _apply_noindex(response)
    session = _get_owned_session(db, session_id, admin)
    if session.status == "completed":
        raise HTTPException(status_code=409, detail="이미 완료된 세션입니다")

    resp_row = _find_response(db, session_id, body.item_id)
    if resp_row.item_type != "CONTEXT_CLOZE":
        raise HTTPException(status_code=400, detail="초성 힌트는 문맥빈칸(CONTEXT_CLOZE) 문항에서만 사용할 수 있습니다")
    if resp_row.answered_at is not None:
        raise HTTPException(status_code=409, detail="이미 완료된 문항입니다")

    item = db.query(VocabularyMultiformatItem).filter(
        VocabularyMultiformatItem.item_id == body.item_id
    ).first()
    answer_payload = json.loads(item.answer_payload_json)
    choseong_hint = _to_choseong(answer_payload["answer_text"])

    resp_row.hint_used = 1
    db.commit()

    return {"choseong_hint": choseong_hint, "attempt_count": resp_row.attempt_count,
            "attempts_left": MAX_CLOZE_ATTEMPTS - resp_row.attempt_count}


@api_router.post("/sessions/{session_id}/answer")
def submit_answer(session_id: str, body: AnswerBody, response: Response,
                   db: Session = Depends(get_vocabulary_quiz_db), admin: str = Depends(require_admin)):
    _apply_noindex(response)
    session = _get_owned_session(db, session_id, admin)
    if session.status == "completed":
        raise HTTPException(status_code=409, detail="이미 완료된 세션입니다")

    resp_row = _find_response(db, session_id, body.item_id)
    if resp_row.answered_at is not None:
        raise HTTPException(status_code=409, detail="이미 답변한 문항입니다 - 변경할 수 없습니다")

    item = db.query(VocabularyMultiformatItem).filter(
        VocabularyMultiformatItem.item_id == body.item_id
    ).first()

    result = _grade(item, body)
    resp_row.submitted_payload_json = json.dumps(result["submitted"], ensure_ascii=False)
    resp_row.attempt_count += 1

    # 문맥빈칸은 최대 2회 시도 - 1차 시도가 틀리면 확정하지 않고(answered_at 유지 NULL)
    # 초성 힌트를 자동 공개한 뒤 재시도를 허용한다. 그 외 유형/2차 시도는 즉시 확정한다.
    is_retry_pending = (
        item.item_type == "CONTEXT_CLOZE"
        and not result["is_correct"]
        and resp_row.attempt_count < MAX_CLOZE_ATTEMPTS
    )

    if is_retry_pending:
        resp_row.hint_used = 1
        db.commit()
        answer_payload = json.loads(item.answer_payload_json)
        return {
            "is_correct": False,
            "retry_available": True,
            "attempts_left": MAX_CLOZE_ATTEMPTS - resp_row.attempt_count,
            "choseong_hint": _to_choseong(answer_payload["answer_text"]),
            "is_last": False,
        }

    resp_row.is_correct = result["is_correct"]
    resp_row.correct_count = result["correct_count"]
    resp_row.total_count = result["total_count"]
    resp_row.answered_at = datetime.now(timezone.utc).isoformat()
    if result["is_correct"]:
        session.correct_count += 1

    # SessionLocal이 autoflush=False라(app/vocabulary_quiz/db.py) 아래 count 쿼리 전에
    # 명시적으로 flush해야 방금 바꾼 answered_at이 반영된다 - 기존 quiz.py의
    # session_answer()에서 실제로 재현·수정된 버그와 같은 패턴이라 여기서도 동일 적용.
    db.flush()

    remaining = db.query(VocabularyMultiformatResponse).filter(
        VocabularyMultiformatResponse.session_id == session_id,
        VocabularyMultiformatResponse.answered_at.is_(None),
    ).count()
    is_last = remaining == 0
    if is_last:
        session.status = "completed"
        session.completed_at = datetime.now(timezone.utc).isoformat()

    db.commit()

    return {
        "is_correct": bool(result["is_correct"]),
        "retry_available": False,
        "correct_count": result["correct_count"],
        "total_count": result["total_count"],
        "cell_results": result.get("cell_results"),  # CROSSWORD 전용: 칸별 정오. 그 외 유형은 None
        "correct_answer": _correct_answer_payload(item),
        "explanation": item.explanation,
        "is_last": is_last,
    }


@api_router.get("/sessions/{session_id}/result")
def session_result(session_id: str, response: Response, db: Session = Depends(get_vocabulary_quiz_db),
                    admin: str = Depends(require_admin)):
    _apply_noindex(response)
    session = _get_owned_session(db, session_id, admin)

    responses = (
        db.query(VocabularyMultiformatResponse)
        .filter(VocabularyMultiformatResponse.session_id == session_id)
        .order_by(VocabularyMultiformatResponse.order_index.asc())
        .all()
    )

    by_type: dict[str, dict] = {}
    wrong_items = []
    for r in responses:
        stats = by_type.setdefault(r.item_type, {"correct": 0, "total": 0})
        stats["total"] += 1
        if r.is_correct:
            stats["correct"] += 1
        if r.answered_at is not None and not r.is_correct:
            item = db.query(VocabularyMultiformatItem).filter(
                VocabularyMultiformatItem.item_id == r.item_id
            ).first()
            wrong_items.append({
                "item_id": r.item_id,
                "item_type": r.item_type,
                "prompt": item.prompt if item else None,
                "explanation": item.explanation if item else None,
                "your_answer": json.loads(r.submitted_payload_json) if r.submitted_payload_json else None,
                "correct_answer": _correct_answer_payload(item) if item else None,
                "correct_count": r.correct_count,
                "total_count": r.total_count,
                "attempt_count": r.attempt_count,
                "hint_used": bool(r.hint_used),
            })

    for stats in by_type.values():
        stats["accuracy"] = round(stats["correct"] / stats["total"] * 100, 1) if stats["total"] else 0.0

    total = session.question_count
    correct = session.correct_count
    accuracy = round(correct / total * 100, 1) if total else 0.0

    return {
        "session_id": session_id,
        "status": session.status,
        "total": total,
        "correct": correct,
        "accuracy": accuracy,
        "by_type": by_type,
        "wrong_items": wrong_items,
    }


# ==================== 관리자 화면 ====================

@page_router.get("/play")
def play_page(request: Request, admin: str = Depends(require_admin)):
    return templates.TemplateResponse("vocabulary_quiz/multiformat_play.html", {
        "request": request, "item_types": ITEM_TYPES, "default_question_count": DEFAULT_QUESTION_COUNT,
    }, headers=NOINDEX_HEADERS)
