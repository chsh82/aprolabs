# -*- coding: utf-8 -*-
"""
독서논술 교재 PDF(A2 계열: 초1~초2 그림책형) -> 문항 정보 JSON 추출 프로토타입.

A1(test_reading_essay_extract.py)과 달리:
  - 어휘가 "낱말-뜻 선잇기" 매칭 게임 + <보기> 빈칸채우기 (표 아님)
  - "질문과 토론"에 독해유형 라벨/색칠된 표 셀이 없음
  - 답변이 "- " 불릿 문단으로 오거나, 문장 안 빈칸에 바로 채워지는 형태
    (인라인 빈칸/글자별로 줄바꿈된 좁은 텍스트박스 등은 이번 프로토타입에서
    신뢰도 낮음 - 알려진 한계로 남김)

사용법:
    python test_reading_essay_extract_a2.py <학생용.pdf> <교사용.pdf> [output.json]
"""
import re
import sys
import json
from pathlib import Path

import fitz

from test_reading_essay_extract import (
    norm, PAGE_MARKER_RE, get_lines_with_bbox, find_ox_answers, parse_writing,
)

QNUM_RE = re.compile(r'^(\d+(?:-\d+)?)\.\s*(.*)')
BULLET_RE = re.compile(r'^[-+]\s*(.*)')
VOCAB_MARK_RE = re.compile(r'^∙\s*(\d*)$')


# ────────────────────────────────────────────────
# 섹션 페이지 범위
# ────────────────────────────────────────────────

def find_section_bounds(pages_text):
    idx_vocab = next((i for i, t in enumerate(pages_text) if re.search(r'1-1\s*\n?\s*어휘|어휘력', t)), None)
    idx_disc = next((i for i, t in enumerate(pages_text) if re.search(r'질문과\s*토론', t)), None)
    idx_write = next(
        (i for i, t in enumerate(pages_text)
         if re.search(r'내\s*글로\s*엮기|글쓰기', t) and (idx_disc is None or i > idx_disc)),
        None
    )
    n = len(pages_text)
    return {
        'cover': (0, idx_vocab if idx_vocab is not None else n),
        'vocab': (idx_vocab, idx_disc) if idx_vocab is not None else None,
        'discussion': (idx_disc, idx_write) if idx_disc is not None else None,
        'writing': (idx_write, n) if idx_write is not None else None,
    }


def parse_cover(text):
    level = None
    m = re.search(r'(LV\s*\d+)', text)
    if m:
        level = m.group(1).replace(' ', '')

    lines = [norm(l) for l in text.split('\n') if norm(l)]
    quotes = [l for l in lines if l.startswith('"') or l.startswith('“')]

    title = None
    author = None
    for l in lines:
        if l in quotes or PAGE_MARKER_RE.match(l):
            continue
        if re.match(r'^LV\s*\d+$', l):
            continue
        if title is None:
            title = l
        elif author is None:
            author = l
            break
    return {'level': level, 'title': title, 'author': author, 'cover_quotes': quotes}


# ────────────────────────────────────────────────
# 어휘: 낱말-뜻 선잇기 + <보기> 빈칸채우기
# ────────────────────────────────────────────────

def parse_vocab_matching(teacher_text):
    """
    형식(교사용 기준):
      단어
      ∙<보통 정답 번호>      <- 단어쪽 점 (번호 없음, 그냥 ∙)
      ∙<정답 번호>           <- 뜻쪽 점 (교사용에만 번호 있음)
      뜻 줄1
      뜻 줄2 (선택)
    5세트가 반복. 번호가 있는 쪽(∙N)이 "이 단어가 N번째로 나열된 뜻과 연결된다"는 의미.
    """
    lines = [norm(l) for l in teacher_text.split('\n')]
    items = []
    i = 0
    n = len(lines)
    while i < n:
        if not lines[i] or not lines[i + 1:i + 2] or not VOCAB_MARK_RE.match(lines[i + 1] if i + 1 < n else ''):
            i += 1
            continue
        word = lines[i]
        m1 = VOCAB_MARK_RE.match(lines[i + 1]) if i + 1 < n else None
        m2 = VOCAB_MARK_RE.match(lines[i + 2]) if i + 2 < n else None
        if not (m1 and m2):
            i += 1
            continue
        match_no = m1.group(1) or m2.group(1) or None
        def_lines = []
        j = i + 3
        while j < n and lines[j] and not VOCAB_MARK_RE.match(lines[j] if j + 1 <= n else '') \
                and not (j + 1 < n and VOCAB_MARK_RE.match(lines[j + 1])):
            def_lines.append(lines[j])
            j += 1
            if len(def_lines) >= 2:
                break
        items.append({
            'word': word,
            'matched_definition_no': int(match_no) if match_no else None,
            'definition': norm(' '.join(def_lines)),
        })
        i = j
        if len(items) >= 5:  # 관찰된 템플릿은 항상 5세트
            break
    return items


def parse_vocab_fillblank(teacher_text):
    """<보기> 문장 빈칸 채우기: (N) 문장 ( 정답 ) 마무리 + * 정답 : 뜻"""
    m_bogi = re.search(r'<보기>\s*([^\n]+)', teacher_text)
    word_bank = m_bogi.group(1).split() if m_bogi else []

    pattern = re.compile(r'\((\d+)\)\s*(.*?)\(\s*([^)]+?)\s*\)(.*?)\n\*\s*([^:：]+)[:：]\s*(.+)')
    items = []
    for m in pattern.finditer(teacher_text):
        items.append({
            'no': int(m.group(1)),
            'sentence_template': norm(m.group(2) + ' ( ___ ) ' + m.group(4)),
            'answer_word': norm(m.group(3)),
            'word_explanation': norm(m.group(6)),
        })
    return {'word_bank': word_bank, 'items': items}


# ────────────────────────────────────────────────
# O.X 퀴즈 (페이지 표기 없음)
# ────────────────────────────────────────────────

def parse_ox_statements_a2(teacher_text):
    marker = re.search(r'O\.?X\s*퀴즈\s*\n', teacher_text)
    text = teacher_text[marker.end():] if marker else teacher_text
    # "○\nX" 또는 "○X" 두 형식 모두 지원
    pattern = re.compile(r'([^\n]+)\n[○O]\s*\n?X', re.MULTILINE)
    items = []
    for m in pattern.finditer(text):
        items.append({'statement': norm(m.group(1))})
    return items


# ────────────────────────────────────────────────
# 질문과 토론 (독해유형/표 셀 없음, 불릿형 답변)
# ────────────────────────────────────────────────

def parse_discussion_a2(doc, page_indices):
    """
    문항번호(N. / N-M.) 뒤에 오는 '- '/'+ ' 불릿 라인들을 모범답안으로 수집.
    알려진 한계: 문장 안에 인라인으로 빈칸을 채우는 형태(예: "둘 다 (코감기)에
    걸렸어요")나, 좁은 텍스트박스에서 한 글자씩 줄바꿈되는 형태는 이 방식으로
    깨끗하게 못 잡아서 raw_tail 필드에 원문 그대로 남겨둔다.
    """
    items = []
    current = None

    for pidx in page_indices:
        page = doc[pidx]
        lines = get_lines_with_bbox(page)
        for line in lines:
            s = norm(line['text'])
            if not s:
                continue
            m_q = QNUM_RE.match(s)
            if m_q:
                if current:
                    items.append(current)
                current = {
                    'question_no': m_q.group(1),
                    'question_text': m_q.group(2),
                    'bullet_answers': [],
                    'raw_tail': [],
                }
                continue
            if current is None:
                continue
            if PAGE_MARKER_RE.match(s):
                continue
            m_b = BULLET_RE.match(s)
            if m_b:
                current['bullet_answers'].append(m_b.group(1))
            else:
                current['raw_tail'].append(s)

    if current:
        items.append(current)

    for it in items:
        it['raw_tail'] = norm(' '.join(it['raw_tail']))
        if not it['raw_tail']:
            it['raw_tail'] = None
    return items


# ────────────────────────────────────────────────
# 메인
# ────────────────────────────────────────────────

def extract(student_path, teacher_path):
    doc_s = fitz.open(student_path)
    doc_t = fitz.open(teacher_path)

    pages_text_t = [doc_t[i].get_text() for i in range(len(doc_t))]
    bounds = find_section_bounds(pages_text_t)

    cover = parse_cover('\n'.join(pages_text_t[slice(*bounds['cover'])]))

    vocab_matching, vocab_fillblank = [], {}
    if bounds['vocab']:
        a, b = bounds['vocab']
        vocab_text = '\n'.join(pages_text_t[a:b])
        vocab_matching = parse_vocab_matching(vocab_text)
        vocab_fillblank = parse_vocab_fillblank(vocab_text)

    ox_items = []
    discussion = []
    if bounds['discussion']:
        a, b = bounds['discussion']
        ox_stmts = parse_ox_statements_a2('\n'.join(pages_text_t[a:b]))
        ox_answers = []
        for pidx in range(a, b):
            ox_answers.extend(find_ox_answers(doc_t[pidx]))
        ox_answers.sort(key=lambda x: x['y'])
        for stmt, ans in zip(ox_stmts, ox_answers):
            ox_items.append({**stmt, 'answer': ans['answer']})
        if len(ox_stmts) != len(ox_answers):
            print(f'[WARN] OX 문장 {len(ox_stmts)}건 vs 정답표시 {len(ox_answers)}건 불일치', file=sys.stderr)

        discussion = parse_discussion_a2(doc_t, range(a, b))

    writing = {}
    if bounds['writing']:
        a, b = bounds['writing']
        writing = parse_writing('\n'.join(pages_text_t[a:b]))

    return {
        'cover': cover,
        'vocabulary_matching': vocab_matching,
        'vocabulary_fillblank': vocab_fillblank,
        'ox_quiz': ox_items,
        'discussion_questions': discussion,
        'writing_prompt': writing,
        'source': {'student_pdf': str(student_path), 'teacher_pdf': str(teacher_path)},
    }


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('사용법: python test_reading_essay_extract_a2.py <학생용.pdf> <교사용.pdf> [output.json]')
        sys.exit(1)

    student_path = Path(sys.argv[1])
    teacher_path = Path(sys.argv[2])
    out_path = Path(sys.argv[3]) if len(sys.argv) > 3 else Path('extract_output_a2.json')

    result = extract(student_path, teacher_path)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f'완료 -> {out_path}')
    print(f"  표지: {result['cover']}")
    print(f"  어휘(매칭): {len(result['vocabulary_matching'])}건")
    print(f"  어휘(빈칸): {len(result['vocabulary_fillblank'].get('items', []))}건")
    print(f"  OX: {len(result['ox_quiz'])}건")
    print(f"  토론 문항: {len(result['discussion_questions'])}건")
    print(f"  글쓰기 Step1 질문: {len(result['writing_prompt'].get('step1_questions', []))}건")
