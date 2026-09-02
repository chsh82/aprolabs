"""학습 기록 적재 API - HANDOFF.md 5절.

POST /vocab/attempt?student_id=<학생ID>

게임이 문항 하나에 답할 때마다 기록 하나를 만들지만, 매번 요청을 보내면
너무 잦으므로 배열로 모아 한 번에 보내는 것도 지원한다(게임이 끝날 때
한 번에 전송하는 쪽을 권장 - app/vocab/static/games/*.html 참고).

student_id는 개별 기록이 아니라 요청 전체에 하나만 붙는다 - 한 세션
안에서 학생이 바뀔 일이 없으므로 쿼리스트링으로 받는다(games의 ?level=
과 같은 자리). 지금은 aprolabs 로그인 세션과 연결돼 있지 않아
'test_01' 같은 값이 들어간다(docs/vocab/EXTERNAL_WIRING.md 3절 -
학생 인증은 momoai.kr 이관 후 처리).

ms(응답까지 걸린 시간)는 필수다. 문항이 화면에 뜬 시점부터 답할 때까지의
시간이고, 나중에 재출제 간격과 난이도 조정의 근거가 된다(app/vocab/models.py
Attempt 모델 docstring, HANDOFF.md 5절).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.vocab.db import get_vocab_db
from app.vocab.models import Attempt

router = APIRouter(prefix="/vocab")


class AttemptIn(BaseModel):
    idiom_id: int | None = None
    game: str
    format: str
    is_correct: bool
    ms: int


def _save(db: Session, student_id: str, items: list[AttemptIn]) -> int:
    for item in items:
        db.add(Attempt(
            student_id=student_id,
            idiom_id=item.idiom_id,
            game=item.game,
            format=item.format,
            is_correct=1 if item.is_correct else 0,
            ms=item.ms,
        ))
    db.commit()
    return len(items)


@router.post("/attempt")
def post_attempt(
    payload: AttemptIn | list[AttemptIn],
    student_id: str = Query("test_01"),
    db: Session = Depends(get_vocab_db),
):
    items = payload if isinstance(payload, list) else [payload]
    inserted = _save(db, student_id, items)
    return {"inserted": inserted}
