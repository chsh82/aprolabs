"""문항 공급 API - docs/vocab/quiz-api.md 계약대로.

GET /vocab/quiz?format=&count=&level=&strict=&topic=&exclude=

지금 실제로 문항을 내려주는 형식은 gate3/assemble/hanja 셋뿐이다
(app/vocab/services/quiz_builder.py 참고). mc4/situation4/usage는
필요한 원천 데이터(example/오용문 등)가 아직 없다(docs/vocab/MISSING.md) -
이 셋은 501 같은 에러가 아니라 `items: []` + `note`로 정상 응답한다.
게임 쪽 코드가 빈 배열만 보고 "지금은 문항이 없다"로 처리할 수 있게
하기 위함이다(에러 처리 분기를 게임마다 새로 안 만들어도 됨).

format이 위 6개 중 아무것도 아니면(오타 등) 그건 클라이언트 입력 오류라
400으로 처리한다 - "아직 구현 안 됨"과 "잘못된 값"은 다른 문제다.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.vocab.db import get_vocab_db
from app.vocab.services import quiz_builder

router = APIRouter(prefix="/vocab")

_NOT_READY_NOTES = {
    "mc4": "mc4는 문맥 빈칸 문장이 필요한데 example 테이블이 비어 있어 아직 생성할 수 없습니다.",
    "situation4": "situation4는 상황문이 필요한데 아직 확보된 데이터가 없습니다.",
    "usage": "usage는 검수된 오용/정상 문장이 필요한데 아직 확보된 데이터가 없습니다.",
}

_ALL_FORMATS = quiz_builder.SUPPORTED_FORMATS | set(_NOT_READY_NOTES)


def _parse_exclude(exclude: str | None) -> set[int]:
    if not exclude:
        return set()
    ids = set()
    for token in exclude.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ids.add(int(token))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"exclude에 정수가 아닌 값이 있습니다: {token}")
    return ids


@router.get("/quiz")
def get_quiz(
    format: str = Query(...),
    count: int = Query(10, ge=1, le=50),
    level: int | None = Query(None, ge=1, le=12),
    strict: bool = Query(False),
    topic: str | None = Query(None),
    exclude: str | None = Query(None),
    db: Session = Depends(get_vocab_db),
):
    if format not in _ALL_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"알 수 없는 format: {format!r}. 가능한 값: {sorted(_ALL_FORMATS)}",
        )

    if format in _NOT_READY_NOTES:
        return {"format": format, "items": [], "note": _NOT_READY_NOTES[format]}

    exclude_ids = _parse_exclude(exclude)
    return quiz_builder.build(db, format, count, level, strict, topic, exclude_ids)
