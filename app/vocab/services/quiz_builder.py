"""GET /vocab/quiz 응답 조립 - 형식별 필드는 docs/vocab/quiz-api.md 계약을 그대로 따른다.

지금 실제로 만들 수 있는 형식은 gate3/assemble/hanja 셋뿐이다(다른 셋은
필요한 데이터(example/relation 등)가 없다 - docs/vocab/MISSING.md 참고).
나머지 형식(mc4/situation4/usage)은 이 파일이 아니라 라우터에서 빈 items +
note로 처리한다 - 이 파일은 데이터가 있는 형식만 안다.
"""
from __future__ import annotations

import random

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.vocab.models import Hanja, Idiom, IdiomHanja, TopicLink
from app.vocab.services import distractor

SUPPORTED_FORMATS = {"gate3", "assemble", "hanja"}

DEFAULT_ASSEMBLE_DISTRACT_SIZE = 8


def _why(idiom: Idiom) -> str | None:
    """오답 해설 자리 - origin_story 우선, 없으면 meaning, 둘 다 없으면 None.

    사용자 결정(2026-09-03): origin_story가 전부 NULL인 지금은 결과적으로
    거의 항상 meaning과 같은 텍스트가 들어간다. published 게이트가
    meaning IS NOT NULL을 요구하므로 여기까지 온 idiom은 meaning이 항상
    있어 None이 나올 일은 사실상 없다.
    """
    return idiom.origin_story or idiom.meaning


def _closeness_weight(level_min: int, level: int) -> float:
    return 1.0 / (1 + abs(level - level_min))


def _weighted_sample_without_replacement(rows: list[Idiom], level: int, count: int) -> list[Idiom]:
    pool = list(rows)
    weights = [_closeness_weight(r.level_min, level) for r in pool]
    chosen: list[Idiom] = []
    while pool and len(chosen) < count:
        total = sum(weights)
        r = random.uniform(0, total)
        upto = 0.0
        for i, w in enumerate(weights):
            upto += w
            if upto >= r:
                chosen.append(pool.pop(i))
                weights.pop(i)
                break
    return chosen


def select_pool(
    db: Session,
    *,
    level: int | None,
    strict: bool,
    topic: str | None,
    exclude_ids: set[int],
) -> list[Idiom]:
    """published 중 level/strict/topic/exclude 조건을 만족하는 전체 후보.

    level 매칭 규칙(docs/vocab/quiz-api.md "level 매칭 방식"):
    strict=False(기본)면 level_min <= level 인 것 전부, strict=True면
    level_min == level 인 것만.
    """
    q = select(Idiom).where(Idiom.status == "published")
    if exclude_ids:
        q = q.where(Idiom.idiom_id.notin_(exclude_ids))
    if topic:
        topic_ids = select(TopicLink.idiom_id).where(TopicLink.topic == topic)
        q = q.where(Idiom.idiom_id.in_(topic_ids))
    if level is not None:
        if strict:
            q = q.where(Idiom.level_min == level)
        else:
            q = q.where(Idiom.level_min <= level)
    return db.execute(q).scalars().all()


def _pick_sample(pool: list[Idiom], count: int, level: int | None, strict: bool) -> list[Idiom]:
    if level is not None and not strict:
        return _weighted_sample_without_replacement(pool, level, count)
    return random.sample(pool, min(count, len(pool)))


def _hanja_ready_ids(db: Session, candidate_ids: list[int]) -> set[int]:
    """idiom_hanja 4자가 다 있고, 4자 모두 hun/eum이 있는 idiom_id만 남긴다."""
    if not candidate_ids:
        return set()
    rows = db.execute(
        select(IdiomHanja.idiom_id, IdiomHanja.char).where(IdiomHanja.idiom_id.in_(candidate_ids))
    ).all()
    chars_by_idiom: dict[int, list[str]] = {}
    for idiom_id, char in rows:
        chars_by_idiom.setdefault(idiom_id, []).append(char)

    all_chars = {c for chars in chars_by_idiom.values() for c in chars}
    hun_rows = db.execute(
        select(Hanja.char, Hanja.hun, Hanja.eum).where(Hanja.char.in_(all_chars))
    ).all()
    complete_chars = {char for char, hun, eum in hun_rows if hun and eum}

    return {
        idiom_id for idiom_id, chars in chars_by_idiom.items()
        if len(chars) == 4 and all(c in complete_chars for c in chars)
    }


def build_gate3(db: Session, idioms: list[Idiom], exclude_ids: set[int]) -> list[dict]:
    used = set(exclude_ids) | {i.idiom_id for i in idioms}
    items = []
    for idiom in idioms:
        wrong = distractor.pick_distractors(db, idiom, 2, used)
        if len(wrong) < 2:
            continue  # 오답 2개를 못 채우면 계약(w는 정확히 2개)을 어기니 건너뛴다
        items.append({
            "id": idiom.idiom_id,
            "mean": idiom.meaning,
            "a": idiom.headword,
            "w": [w.headword for w in wrong],
            "why": _why(idiom),
        })
        used |= {w.idiom_id for w in wrong}
    return items


def build_assemble(db: Session, idioms: list[Idiom]) -> dict:
    items = [
        {
            "id": idiom.idiom_id,
            "mean": idiom.meaning,
            "word": idiom.headword,
            "why": _why(idiom),
        }
        for idiom in idioms
    ]

    own_chars = {c for idiom in idioms for c in idiom.headword}
    other_rows = db.execute(
        select(Idiom.headword).where(
            Idiom.status == "published",
            Idiom.idiom_id.notin_([i.idiom_id for i in idioms]),
        )
    ).all()
    pool_chars = list({c for (hw,) in other_rows for c in hw} - own_chars)
    random.shuffle(pool_chars)
    distract = pool_chars[:DEFAULT_ASSEMBLE_DISTRACT_SIZE]

    return {"format": "assemble", "items": items, "distract": distract}


def build_hanja(db: Session, idioms: list[Idiom]) -> dict:
    items = []
    hun: dict[str, str] = {}
    for idiom in idioms:
        chars = db.execute(
            select(IdiomHanja.char).where(IdiomHanja.idiom_id == idiom.idiom_id)
            .order_by(IdiomHanja.position)
        ).scalars().all()
        items.append({
            "id": idiom.idiom_id,
            "kor": idiom.headword,
            "han": idiom.hanja,
            "mean": idiom.meaning,
        })
        for c in chars:
            if c in hun:
                continue
            row = db.execute(select(Hanja.hun, Hanja.eum).where(Hanja.char == c)).first()
            if row and row[0] and row[1]:
                hun[c] = f"{row[0]} {row[1]}"

    return {"format": "hanja", "items": items, "hun": hun}


def build(
    db: Session,
    format: str,
    count: int,
    level: int | None,
    strict: bool,
    topic: str | None,
    exclude_ids: set[int],
) -> dict:
    pool = select_pool(db, level=level, strict=strict, topic=topic, exclude_ids=exclude_ids)

    if format == "hanja":
        ready_ids = _hanja_ready_ids(db, [i.idiom_id for i in pool])
        pool = [i for i in pool if i.idiom_id in ready_ids]

    sample = _pick_sample(pool, count, level, strict)

    if format == "gate3":
        return {"format": "gate3", "items": build_gate3(db, sample, exclude_ids)}
    if format == "assemble":
        return build_assemble(db, sample)
    if format == "hanja":
        return build_hanja(db, sample)

    raise ValueError(f"quiz_builder는 {format}을 모른다")  # 라우터가 SUPPORTED_FORMATS로 미리 막아야 함
