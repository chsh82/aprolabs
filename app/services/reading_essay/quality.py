# -*- coding: utf-8 -*-
"""추출 결과 품질 집계 - 계열/분기별 결측치 통계 + 문제 있는 교재 목록."""
from collections import defaultdict

from sqlalchemy.orm import Session

from app.models.reading_essay import ReadingMaterial, TEMPLATE_FAMILIES


def family_stats(db: Session):
    """계열별 결측치 집계."""
    materials = db.query(ReadingMaterial).filter(ReadingMaterial.status == 'extracted').all()
    by_family = defaultdict(list)
    for m in materials:
        by_family[m.template_family].append(m)

    stats = []
    for fam in TEMPLATE_FAMILIES:
        items = by_family.get(fam, [])
        n = len(items)
        if n == 0:
            stats.append({'family': fam, 'count': 0})
            continue

        total_vocab = sum(len(m.vocabulary_items) for m in items)
        total_ox = sum(len(m.ox_items) for m in items)
        total_disc = sum(len(m.discussion_items) for m in items)

        vocab_no_def = sum(1 for m in items for v in m.vocabulary_items if not v.definition)
        ox_no_answer = sum(1 for m in items for o in m.ox_items if not o.answer)
        disc_no_answer = sum(
            1 for m in items for d in m.discussion_items
            if not d.model_answer and not d.sub_answers and not d.raw_tail
        )
        disc_has_raw_tail = sum(1 for m in items for d in m.discussion_items if d.raw_tail)

        stats.append({
            'family': fam, 'count': n,
            'no_title': sum(1 for m in items if not m.title),
            'no_vocab': sum(1 for m in items if len(m.vocabulary_items) == 0),
            'no_ox': sum(1 for m in items if len(m.ox_items) == 0),
            'no_discussion': sum(1 for m in items if len(m.discussion_items) == 0),
            'no_writing': sum(1 for m in items if not m.writing_prompt),
            'total_vocab': total_vocab, 'vocab_no_def': vocab_no_def,
            'total_ox': total_ox, 'ox_no_answer': ox_no_answer,
            'total_disc': total_disc, 'disc_no_answer': disc_no_answer,
            'disc_has_raw_tail': disc_has_raw_tail,
        })
    return stats


def flagged_materials(db: Session, family=None, issue=None, limit=100):
    """문제가 있어 보이는 교재 목록 (검수 우선순위용).

    issue: 'no_discussion' | 'no_answer_majority' | 'no_writing' | None(전체 flagged)
    """
    query = db.query(ReadingMaterial).filter(ReadingMaterial.status == 'extracted')
    if family:
        query = query.filter(ReadingMaterial.template_family == family)
    materials = query.all()

    flagged = []
    for m in materials:
        n_disc = len(m.discussion_items)
        n_no_answer = sum(
            1 for d in m.discussion_items
            if not d.model_answer and not d.sub_answers and not d.raw_tail
        )
        reasons = []
        if n_disc == 0:
            reasons.append('토론 문항 0건')
        elif n_no_answer / n_disc > 0.5:
            reasons.append(f'토론 문항 답변 누락 {n_no_answer}/{n_disc}')
        if not m.writing_prompt:
            reasons.append('글쓰기 없음')
        if not m.title:
            reasons.append('제목 없음')

        if not reasons:
            continue
        if issue == 'no_discussion' and n_disc != 0:
            continue
        if issue == 'no_answer_majority' and not (n_disc and n_no_answer / n_disc > 0.5):
            continue
        if issue == 'no_writing' and m.writing_prompt:
            continue

        flagged.append({'material': m, 'reasons': reasons})
        if len(flagged) >= limit:
            break
    return flagged
