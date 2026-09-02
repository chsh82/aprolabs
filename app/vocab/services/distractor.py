"""오답(distractor) 후보 idiom 선택 - HANDOFF.md 2절의 우선순위를 그대로 따른다.

    1순위 - relation 테이블의 confusable(혼동쌍)
    2순위 - 같은 topic_link 주제
    3순위 - 같은 level_min 대

지금은 relation/topic_link가 비어 있어(2026-09-03 기준) 3순위만 실제로
동작하지만, 나중에 혼동쌍/주제가 채워지면 이 함수를 고치지 않고도 1·2순위가
먼저 걸린다 - 우선순위 로직은 데이터 유무와 무관하게 항상 위에서부터 채운다.
"""
from __future__ import annotations

import random

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.vocab.models import Idiom, Relation, TopicLink


def _confusable_ids(db: Session, idiom_id: int) -> set[int]:
    rows = db.execute(
        select(Relation.idiom_a, Relation.idiom_b).where(
            Relation.rel_type == "confusable",
            (Relation.idiom_a == idiom_id) | (Relation.idiom_b == idiom_id),
        )
    ).all()
    return {(b if a == idiom_id else a) for a, b in rows}


def _topic_sibling_ids(db: Session, idiom_id: int) -> set[int]:
    topics = [t for (t,) in db.execute(
        select(TopicLink.topic).where(TopicLink.idiom_id == idiom_id)
    ).all()]
    if not topics:
        return set()
    rows = db.execute(
        select(TopicLink.idiom_id).where(TopicLink.topic.in_(topics))
    ).all()
    return {r[0] for r in rows}


def _same_band_ids(db: Session, level_min: int | None) -> set[int]:
    if level_min is None:
        return set()
    rows = db.execute(
        select(Idiom.idiom_id).where(Idiom.level_min == level_min, Idiom.status == "published")
    ).all()
    return {r[0] for r in rows}


def _sample_idioms(db: Session, ids: set[int], n: int) -> list[Idiom]:
    if not ids or n <= 0:
        return []
    picked_ids = random.sample(sorted(ids), min(n, len(ids)))
    rows = db.execute(
        select(Idiom).where(Idiom.idiom_id.in_(picked_ids), Idiom.status == "published")
    ).scalars().all()
    random.shuffle(rows)
    return rows


def pick_distractors(db: Session, idiom: Idiom, n: int, exclude_ids: set[int]) -> list[Idiom]:
    """idiom의 오답 보기로 쓸 다른 idiom n개를 우선순위대로 채워서 돌려준다.

    exclude_ids: 이번 응답 배치에 이미 쓰인(정답 또는 다른 문항의 오답) idiom_id -
    같은 배치 안에서 오답이 중복되거나 다른 문항의 정답과 겹치지 않게 한다.
    """
    exclude = {idiom.idiom_id} | exclude_ids
    picked: list[Idiom] = []

    for candidate_ids in (
        _confusable_ids(db, idiom.idiom_id),
        _topic_sibling_ids(db, idiom.idiom_id),
        _same_band_ids(db, idiom.level_min),
    ):
        if len(picked) >= n:
            break
        remaining = candidate_ids - exclude - {p.idiom_id for p in picked}
        more = _sample_idioms(db, remaining, n - len(picked))
        picked.extend(more)

    return picked[:n]
