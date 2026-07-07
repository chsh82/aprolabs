# -*- coding: utf-8 -*-
"""
독서논술 교재 PDF(C 계열: 중3 소설이론표형) -> 문항 정보 추출.

A1과 매우 비슷한 색칠된 표 셀(라벨+모범답안) 기법을 그대로 쓰지만:
  - 헤더 문구가 다름: "1 단계. 소설 구성의 3요소" / "3단계: 주제 글쓰기"
    (A1의 "1단계:어휘력.../2 질문과토론.../3 글쓰기"가 아님)
  - 어휘/OX 섹션이 없고, 대신 "소설 구성의 3요소" 비교표(작품별 인물/
    사건/배경/구성방식-시점)가 맨 앞에 옴
  - 독해유형이 "[분석적 / 추론적 독해]"처럼 대괄호로 감싸여 있음(A1은
    괄호 없이 단독 줄)
  - 인용문 페이지 표기가 "-p.NN"이 아니라 문장 끝 "(NN)"이고, 인용문이
    여러 개면 "~" 줄로 구분됨
  - 여러 작품을 한 교재에서 다루므로 "n번째 이야기. 제목 (연도)" 같은
    작품 구분 줄이 있음 (없으면 단일 작품/비문학 교재로 처리 - fallback)
"""
import re

import fitz

from .extract_a1 import norm, PAGE_MARKER_RE, get_lines_with_bbox, find_cell_pairs

STEP_RE = re.compile(r'^Step\s*(\d+)\.\s*(.*)', re.IGNORECASE)

WORK_HEADER_RE = re.compile(r'^(첫|두|세|네|다섯|여섯|일곱|여덟|아홉|열)\s*번째\s*이야기\.\s*(.+)$')
# 문항 번호와 독해유형이 한 줄에 같이 나옴: "1. [분석적 / 추론적 독해]"
READING_TYPE_RE = re.compile(r'^(\d+)\.\s*\[([가-힣]+(?:\s*/\s*[가-힣]+)*\s*독해)\]$')
EXCERPT_END_RE = re.compile(r'\(\d+(?:~\d+)?\)\s*$')
QUOTE_SEP_RE = re.compile(r'^~$')
STAGE_HEADER_RE = re.compile(r'^\d\s*단계')

_DISC_FALLBACK_RE = re.compile(r'^\d+\.\s*\[[가-힣]+(?:\s*/\s*[가-힣]+)*\s*독해\]$', re.MULTILINE)


def find_section_bounds(pages_text):
    idx_theory = next((i for i, t in enumerate(pages_text) if '소설 구성의' in t or '소설의 3요소' in t), None)
    idx_disc = next((i for i, t in enumerate(pages_text) if '번째 이야기.' in t), None)
    if idx_disc is None:
        # "n번째 이야기" 작품 구분이 없는 단일 작품/비문학 교재
        # (예: 철학·사회 이슈 도서) - 문항+독해유형 결합줄로 대신 탐지
        idx_disc = next((i for i, t in enumerate(pages_text) if _DISC_FALLBACK_RE.search(t)), None)
    idx_write = next(
        (i for i, t in enumerate(pages_text)
         if re.search(r'주제\s*글쓰기|글쓰기\s*주제|내\s*글로\s*엮기', t) and (idx_disc is None or i > idx_disc)),
        None
    )
    n = len(pages_text)
    cover_end = idx_theory if idx_theory is not None else (idx_disc if idx_disc is not None else n)
    return {
        'cover': (0, cover_end),
        'theory_table': (idx_theory, idx_disc) if idx_theory is not None else None,
        'discussion': (idx_disc, idx_write) if idx_disc is not None else None,
        'writing': (idx_write, n) if idx_write is not None else None,
    }


def parse_cover(text):
    level = None
    m = re.search(r'(LV\s*\d+)', text)
    if m:
        level = m.group(1).replace(' ', '')
    m2 = re.search(r'(\d+분기\s*\S+)', text)
    quarter = norm(m2.group(1)) if m2 else None

    lines = [norm(l) for l in text.split('\n') if norm(l)]
    quotes = [l for l in lines if l.startswith('"') or l.startswith('“')]

    title = None
    for l in lines:
        if l == quarter or l in quotes:
            continue
        if re.match(r'^LV\s*\d+$', l) or re.match(r'^\d+주차$', l) or PAGE_MARKER_RE.match(l):
            continue
        title = l
        break
    return {'level': level, 'quarter': quarter, 'title': title, 'cover_quotes': quotes}


def parse_theory_table(text):
    """'소설 구성의 3요소' 비교표(작품별 인물/사건/배경/구성방식 비교).

    셀마다 줄바꿈 수가 달라서(예: "전쟁 중 포로로\n잡힘" 2줄, 어떤 셀은
    1줄) plain text 줄 수만으로는 열을 구분할 수 없다. 이 표는 컬럼별
    x좌표로 다시 접근해야 정확히 파싱 가능 - 이번 프로토타입에서는 구조화를
    포기하고 원문만 보존한다 (알려진 한계, 문항 자체보다 부가 정보라 우선순위 낮음).
    """
    lines = [norm(l) for l in text.split('\n') if norm(l)]
    return {'raw_text': ' '.join(lines), 'note': '컬럼 구조 미파싱 (x좌표 기반 재작업 필요)'}


def parse_discussion_c(doc, page_indices):
    works = []
    current_work = {'title': None, 'items': []}
    current_item = None

    def close_item():
        nonlocal current_item
        if current_item is None:
            return
        current_work['items'].append(current_item)
        current_item = None

    for pidx in page_indices:
        page = doc[pidx]
        lines = get_lines_with_bbox(page)
        cells = find_cell_pairs(page)
        cell_ranges = [(c['label_rect'].y0, c['label_rect'].y1, c) for c in cells]

        def in_any_cell(y):
            return any(y0 - 2 <= y <= y1 + 2 for y0, y1, _ in cell_ranges)

        page_items_start = len(current_work['items'])
        excerpt_buf = []
        quotes = []
        mode = None
        i = 0
        n = len(lines)
        while i < n:
            line = lines[i]
            y = line['y0']
            if in_any_cell(y):
                i += 1
                continue
            s = norm(line['text'])
            if not s:
                i += 1
                continue

            m_work = WORK_HEADER_RE.match(s)
            if m_work:
                close_item()
                if current_work['items']:
                    works.append(current_work)
                current_work = {'title': m_work.group(2), 'items': []}
                page_items_start = 0
                i += 1
                continue

            m_type = READING_TYPE_RE.match(s)
            if m_type:
                close_item()
                mode = 'excerpt'
                quotes = []
                excerpt_buf = []
                current_item = {'question_no': m_type.group(1), 'reading_type': m_type.group(2),
                                 'quotes': [], 'question_text': None, 'cells': [], 'y_start': y}
                i += 1
                continue

            if current_item is None:
                i += 1
                continue

            if mode == 'excerpt':
                if QUOTE_SEP_RE.match(s):
                    i += 1
                    continue
                excerpt_buf.append(s)
                if EXCERPT_END_RE.search(s):
                    quotes.append(norm(' '.join(excerpt_buf)))
                    excerpt_buf = []
                    current_item['quotes'] = quotes
                    next_is_sep = i + 1 < n and QUOTE_SEP_RE.match(norm(lines[i + 1]['text']))
                    if not next_is_sep:
                        mode = 'question'
                i += 1
                continue

            if mode == 'question' and not PAGE_MARKER_RE.match(s):
                current_item['question_text'] = (
                    (current_item['question_text'] + ' ' + s) if current_item['question_text'] else s
                )
            i += 1

        # 이 페이지에서 닫힌 아이템 + 현재 아이템 중, y가 가장 가까운 이전
        # 아이템에 셀을 배분한다 (A1과 동일한 방식 - 한 페이지에 문항이
        # 여러 개일 수 있으므로 "현재 진행 중인 것"에만 배분하면 안 됨).
        page_items = current_work['items'][page_items_start:]
        if current_item:
            page_items = page_items + [current_item]
        for y0, y1, cell in cell_ranges:
            owner = None
            for it in page_items:
                if it['y_start'] <= y0:
                    owner = it
            if owner is not None:
                owner['cells'].append(cell)

    close_item()
    if current_work['items']:
        works.append(current_work)

    for w in works:
        for it in w['items']:
            cells = it.pop('cells', [])
            it.pop('y_start', None)
            if len(cells) == 1:
                it['label'] = cells[0]['label_text']
                it['model_answer'] = cells[0]['answer_text']
            elif len(cells) > 1:
                it['label'] = None
                it['model_answer'] = None
                it['sub_answers'] = [{'label': c['label_text'], 'answer': c['answer_text']} for c in cells]
            else:
                it['label'] = None
                it['model_answer'] = None
    return works


def parse_writing_c(text):
    """'3단계: 주제 글쓰기' - "Step N. <문단>"이 한 줄에 같이 오는 형식.
    Step 줄 이전의 텍스트 중 마지막 문장을 주제(theme)로 잡는다."""
    lines = [norm(l) for l in text.split('\n') if norm(l)]
    theme = None
    steps = []
    for l in lines:
        m_step = STEP_RE.match(l)
        if m_step:
            steps.append({'step': m_step.group(1), 'text': m_step.group(2)})
            continue
        if steps:
            steps[-1]['text'] = norm(steps[-1]['text'] + ' ' + l)
            continue
        if l.startswith('"') or l.startswith('“') or l.startswith('<') \
                or l.startswith('[') or PAGE_MARKER_RE.match(l) or STAGE_HEADER_RE.match(l):
            continue
        theme = l
    return {'theme': theme, 'steps': steps}


def extract(teacher_path):
    doc = fitz.open(teacher_path)
    pages_text = [doc[i].get_text() for i in range(len(doc))]
    bounds = find_section_bounds(pages_text)

    cover = parse_cover('\n'.join(pages_text[slice(*bounds['cover'])]))

    theory_table = {}
    if bounds['theory_table']:
        a, b = bounds['theory_table']
        theory_table = parse_theory_table('\n'.join(pages_text[a:b]))

    discussion = []
    if bounds['discussion']:
        a, b = bounds['discussion']
        discussion = parse_discussion_c(doc, range(a, b))

    writing = {}
    if bounds['writing']:
        a, b = bounds['writing']
        writing = parse_writing_c('\n'.join(pages_text[a:b]))

    return {
        'cover': cover,
        'theory_table': theory_table,
        'works': discussion,
        'writing_prompt': writing,
        'source': {'teacher_pdf': str(teacher_path)},
    }
