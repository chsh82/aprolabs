# -*- coding: utf-8 -*-
"""각 계열(A1/A2/B/C)의 extract() 결과를 공통 DB 모델(ReadingMaterial 등)로
정규화해서 저장하는 어댑터.

계열마다 결과 딕셔너리 모양이 달라서(A1: cover/vocabulary/ox_quiz/
discussion_questions/writing_prompt, B/C: doc_title 또는 cover/works/
writing_prompt, ...) 여기서 전부 같은 행 모양으로 펼친다.
"""
from app.models.reading_essay import (
    ReadingMaterial, VocabularyItem, OXQuizItem, DiscussionQuestion, WritingPrompt,
)


def _cover_of(family, extraction):
    if family in ('A1', 'C'):
        c = extraction.get('cover') or {}
        return {
            'level': c.get('level'), 'quarter': c.get('quarter'),
            'title': c.get('title'), 'author': None,
            'cover_quotes': c.get('cover_quotes'),
        }
    if family == 'A2':
        c = extraction.get('cover') or {}
        return {
            'level': c.get('level'), 'quarter': None,
            'title': c.get('title'), 'author': c.get('author'),
            'cover_quotes': c.get('cover_quotes'),
        }
    if family == 'B':
        return {
            'level': None, 'quarter': None,
            'title': extraction.get('doc_title'), 'author': None,
            'cover_quotes': None,
        }
    return {'level': None, 'quarter': None, 'title': None, 'author': None, 'cover_quotes': None}


def _vocab_rows(family, extraction):
    if family == 'A1':
        return [
            {'word': v.get('word'), 'page': v.get('page'),
             'definition': v.get('definition'), 'matched_definition_no': None}
            for v in extraction.get('vocabulary', []) or []
        ]
    if family == 'A2':
        rows = [
            {'word': v.get('word'), 'page': None, 'definition': v.get('definition'),
             'matched_definition_no': v.get('matched_definition_no')}
            for v in extraction.get('vocabulary_matching', []) or []
        ]
        for fb in (extraction.get('vocabulary_fillblank') or {}).get('items', []) or []:
            rows.append({
                'word': fb.get('answer_word'), 'page': None,
                'definition': fb.get('word_explanation'), 'matched_definition_no': None,
            })
        return rows
    return []  # B, C: 어휘 섹션 없음


def _ox_rows(family, extraction):
    if family in ('A1', 'A2'):
        return [
            {'statement': o.get('statement'), 'page': o.get('page'),
             'answer': o.get('answer'), 'wrong_explanation': o.get('wrong_explanation')}
            for o in extraction.get('ox_quiz', []) or []
        ]
    return []  # B, C: OX 섹션 없음


def _discussion_rows(family, extraction):
    rows = []
    if family == 'A1':
        for block in extraction.get('discussion_questions', []) or []:
            reading_type = block.get('reading_type')
            excerpts = block.get('excerpts')
            for item in block.get('items', []) or []:
                rows.append({
                    'work_title': None, 'question_no': item.get('question_no'),
                    'reading_type': reading_type, 'quotes': excerpts,
                    'question_text': item.get('question_text'),
                    'label': item.get('label'), 'model_answer': item.get('model_answer'),
                    'sub_answers': item.get('sub_answers'), 'raw_tail': None,
                })
    elif family == 'A2':
        for item in extraction.get('discussion_questions', []) or []:
            bullets = item.get('bullet_answers') or []
            rows.append({
                'work_title': None, 'question_no': item.get('question_no'),
                'reading_type': None, 'quotes': None,
                'question_text': item.get('question_text'),
                'label': None,
                'model_answer': ' / '.join(bullets) if bullets else None,
                'sub_answers': None, 'raw_tail': item.get('raw_tail'),
            })
    elif family == 'B':
        for work in extraction.get('works', []) or []:
            for item in work.get('items', []) or []:
                excerpt = item.get('excerpt')
                rows.append({
                    'work_title': work.get('title'), 'question_no': item.get('question_no'),
                    'reading_type': None, 'quotes': [excerpt] if excerpt else None,
                    'question_text': item.get('question_or_notes'),
                    'label': None, 'model_answer': item.get('answer'),
                    'sub_answers': None, 'raw_tail': None,
                })
    elif family == 'C':
        for work in extraction.get('works', []) or []:
            for item in work.get('items', []) or []:
                rows.append({
                    'work_title': work.get('title'), 'question_no': item.get('question_no'),
                    'reading_type': item.get('reading_type'), 'quotes': item.get('quotes'),
                    'question_text': item.get('question_text'),
                    'label': item.get('label'), 'model_answer': item.get('model_answer'),
                    'sub_answers': item.get('sub_answers'), 'raw_tail': None,
                })
    return rows


def _writing_of(family, extraction):
    wp = extraction.get('writing_prompt') or {}
    if family in ('A1', 'A2'):
        step1 = wp.get('step1_questions') or []
        steps = [{'step': str(i + 1), 'text': q} for i, q in enumerate(step1)]
        if wp.get('step2_instruction'):
            steps.append({'step': str(len(steps) + 1), 'text': wp['step2_instruction']})
        return {'theme': wp.get('theme'), 'steps': steps}
    if family == 'B':
        return {'theme': wp.get('theme_title'), 'steps': wp.get('steps') or []}
    if family == 'C':
        return {'theme': wp.get('theme'), 'steps': wp.get('steps') or []}
    return {'theme': None, 'steps': []}


def save_material(db, material, family, extraction):
    """이미 생성된(pending 상태) material에 추출 결과를 채워서 저장.
    하위 항목(어휘/OX/토론/글쓰기)은 기존 것을 지우고 새로 씀 (재추출 대응)."""
    cover = _cover_of(family, extraction)
    material.title = cover['title'] or material.title
    material.level = cover['level']
    material.author = cover['author']
    material.cover_quotes = cover['cover_quotes']
    if cover['quarter']:
        material.quarter = material.quarter or cover['quarter']
    material.template_family = family
    material.raw_extraction = extraction
    material.status = 'extracted'
    material.error_message = None

    material.vocabulary_items.clear()
    material.ox_items.clear()
    material.discussion_items.clear()

    for idx, v in enumerate(_vocab_rows(family, extraction)):
        material.vocabulary_items.append(VocabularyItem(order_index=idx, **v))
    for idx, o in enumerate(_ox_rows(family, extraction)):
        material.ox_items.append(OXQuizItem(order_index=idx, **o))
    for idx, d in enumerate(_discussion_rows(family, extraction)):
        material.discussion_items.append(DiscussionQuestion(order_index=idx, **d))

    writing = _writing_of(family, extraction)
    if material.writing_prompt:
        material.writing_prompt.theme = writing['theme']
        material.writing_prompt.steps = writing['steps']
    elif writing['theme'] or writing['steps']:
        material.writing_prompt = WritingPrompt(theme=writing['theme'], steps=writing['steps'])

    db.add(material)
    db.commit()
    db.refresh(material)
    return material
