# -*- coding: utf-8 -*-
"""독서논술 DB 통합 검색 - 어휘/OX/토론문항/글쓰기를 가로질러 키워드로 검색."""
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models.reading_essay import (
    ReadingMaterial, VocabularyItem, OXQuizItem, DiscussionQuestion, WritingPrompt,
)


def _material_filter(query, model_cls, quarter, grade, family):
    query = query.join(ReadingMaterial, model_cls.material_id == ReadingMaterial.id)
    if quarter:
        query = query.filter(ReadingMaterial.quarter == quarter)
    if grade:
        query = query.filter(ReadingMaterial.grade == grade)
    if family:
        query = query.filter(ReadingMaterial.template_family == family)
    return query


def search(db: Session, q="", quarter=None, grade=None, family=None,
           content_types=None, limit=200):
    """q(검색어)와 필터로 어휘/OX/토론/글쓰기를 가로질러 검색.

    content_types: {'vocab','ox','discussion','writing'} 부분집합. None이면 전체.
    반환: [{'type':..., 'material':ReadingMaterial, 'item':<row>}] (관련도 없이 최신순)
    """
    content_types = content_types or {'vocab', 'ox', 'discussion', 'writing'}
    results = []

    if 'vocab' in content_types:
        query = db.query(VocabularyItem)
        query = _material_filter(query, VocabularyItem, quarter, grade, family)
        if q:
            query = query.filter(or_(
                VocabularyItem.word.contains(q), VocabularyItem.definition.contains(q),
            ))
        for item in query.order_by(VocabularyItem.material_id).limit(limit).all():
            results.append({'type': 'vocab', 'material': item.material, 'item': item})

    if 'ox' in content_types:
        query = db.query(OXQuizItem)
        query = _material_filter(query, OXQuizItem, quarter, grade, family)
        if q:
            query = query.filter(OXQuizItem.statement.contains(q))
        for item in query.order_by(OXQuizItem.material_id).limit(limit).all():
            results.append({'type': 'ox', 'material': item.material, 'item': item})

    if 'discussion' in content_types:
        query = db.query(DiscussionQuestion)
        query = _material_filter(query, DiscussionQuestion, quarter, grade, family)
        if q:
            query = query.filter(or_(
                DiscussionQuestion.question_text.contains(q),
                DiscussionQuestion.model_answer.contains(q),
                DiscussionQuestion.label.contains(q),
                DiscussionQuestion.raw_tail.contains(q),
            ))
        for item in query.order_by(DiscussionQuestion.material_id).limit(limit).all():
            results.append({'type': 'discussion', 'material': item.material, 'item': item})

    if 'writing' in content_types:
        query = db.query(WritingPrompt)
        query = _material_filter(query, WritingPrompt, quarter, grade, family)
        if q:
            query = query.filter(WritingPrompt.theme.contains(q))
        for item in query.order_by(WritingPrompt.material_id).limit(limit).all():
            results.append({'type': 'writing', 'material': item.material, 'item': item})

    return results[:limit]
