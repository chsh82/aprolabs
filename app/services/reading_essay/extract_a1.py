# -*- coding: utf-8 -*-
"""
독서논술 교재 PDF(A1 계열: 초3~초6 챕터북형, 학생용+교사용) -> 문항 정보 추출.

구조 가정 (Apro Harkness 독서논술 교재 템플릿):
  p1        표지 (LV, 분기/과목, 제목, 표지 문구)
  p2..      1단계: 어휘력(단어/페이지/뜻) + 내용확인 O.X 퀴즈
  p..       2 질문과 토론 함께 들여다보기 (독해유형별 인용문+발문+답변란)
  마지막    3 글쓰기 내 글로 엮기 (주제문구 + Step1 질문들 + Step2 지시문)

핵심 기법:
  - 어휘/OX/발문 텍스트는 정규식으로 파싱 (템플릿이 매우 규칙적)
  - OX 정답(O/X)은 텍스트만으론 알 수 없음 -> get_drawings()의 셀 강조색 fill이
    '○'/'X' 글자 bbox를 포함하는지로 판별 (교사용 PDF 필요)
  - 발문(question) vs 라벨(label) vs 모범답안(answer)의 경계도 텍스트만으론
    모호함 -> get_drawings()의 색칠된 표 셀 rect + 테두리 stroke로 라벨/답변
    셀의 실제 좌표를 구하고, get_text(clip=rect)로 셀 내부 텍스트만 추출
"""
import fitz
import re
import sys

READING_TYPE_RE = re.compile(r'^[가-힣]+(?:\s*/\s*[가-힣]+)*\s*독해$')
PAGE_REF_RE = re.compile(r'^-p\.(\S+)')
QNUM_RE = re.compile(r'^(\d+(?:-\d+)?)\.\s*(.*)')
PAGE_MARKER_RE = re.compile(r'^-\s*\d+\s*-$')
# PDF 줄바꿈으로 "페이지"가 "페\n이지"처럼 쪼개지는 경우가 있어 공백 허용
PAGE_WORD = r'페\s*이\s*지'


def norm(s):
    return re.sub(r'\s+', ' ', s or '').strip()


# ────────────────────────────────────────────────
# 섹션 페이지 범위 탐색
# ────────────────────────────────────────────────

def find_section_bounds(pages_text):
    idx_vocab = next((i for i, t in enumerate(pages_text) if '1단계' in t), None)
    idx_disc = next((i for i, t in enumerate(pages_text) if re.search(r'질문과\s*토론', t)), None)
    idx_write = next(
        (i for i, t in enumerate(pages_text)
         if re.search(r'내\s*글로\s*엮기', t) and (idx_disc is None or i > idx_disc)),
        None
    )
    n = len(pages_text)
    # 다음 구간의 시작 마커를 못 찾으면 구간 끝을 None이 아니라 문서 끝(n)으로
    # 두어야 한다 - range(a, None)이 TypeError를 내기 때문 (예: 동시집이
    # "글쓰기 내 글로 엮기" 대신 "동시 쓰기"를 씀 -> idx_write=None)
    vocab_end = idx_disc if idx_disc is not None else n
    disc_end = idx_write if idx_write is not None else n
    return {
        'cover': (0, idx_vocab if idx_vocab is not None else n),
        'vocab_ox': (idx_vocab, vocab_end) if idx_vocab is not None else None,
        'discussion': (idx_disc, disc_end) if idx_disc is not None else None,
        'writing': (idx_write, n) if idx_write is not None else None,
    }


# ────────────────────────────────────────────────
# 표지
# ────────────────────────────────────────────────

def parse_cover(text):
    level = None
    m = re.search(r'(LV\s*\d+)', text)
    if m:
        level = m.group(1).replace(' ', '')

    m2 = re.search(r'(\d+분기\s*\S+)', text)
    quarter = norm(m2.group(1)) if m2 else None

    lines = [norm(l) for l in text.split('\n') if norm(l)]
    quotes = [l for l in lines if l.startswith('"') or l.startswith('“')]

    # 제목이 여러 줄에 걸쳐 나올 수 있음: "빈센트 반 고흐,\n세상을 노랗게 물들이다"
    title = None
    title_lines = []
    for l in lines:
        is_boundary = (l == quarter or l in quotes or
                       re.match(r'^LV\s*\d+$', l) or PAGE_MARKER_RE.match(l))
        if is_boundary:
            if title_lines:
                break
            continue
        title_lines.append(l)

    if title_lines:
        tokens = ' '.join(title_lines).split(' ')
        if len(tokens) > 1 and all(len(t) == 1 for t in tokens):
            # 한 글자씩 띄어 렌더링된 제목: "박 문 수 전" -> "박문수전"
            title = ''.join(tokens)
        else:
            title = norm(' '.join(title_lines))
    return {'level': level, 'quarter': quarter, 'title': title, 'cover_quotes': quotes}


# ────────────────────────────────────────────────
# 1단계: 어휘 + O.X 퀴즈
# ────────────────────────────────────────────────

def parse_vocab(text):
    pattern = re.compile(r'([^\n(]+)\n\((\d+)' + PAGE_WORD + r'\)\n뜻\n(.+?)\n문장', re.DOTALL)
    items = []
    for m in pattern.finditer(text):
        items.append({
            'word': norm(m.group(1)),
            'page': int(m.group(2)),
            'definition': norm(m.group(3)),
        })
    return items


def parse_ox_statements(teacher_text):
    """교사용 텍스트에서 O.X 문장 + 페이지 + (오답 시) 설명을 파싱."""
    # 어휘 섹션과 뒤섞이지 않도록 "O.X 퀴즈를 풀어 보세요" 안내문 이후부터만 탐색
    marker = re.search(r'O\.?X\s*퀴즈를\s*풀어\s*보세요[.\s]*\n', teacher_text)
    search_text = teacher_text[marker.end():] if marker else teacher_text

    pattern = re.compile(
        r'([^()\n][\s\S]*?)\((\d+)' + PAGE_WORD + r'(?:[:：]\s*([^)]+))?\)\s*\n\S+\s*\nX',
        re.MULTILINE,
    )
    items = []
    for m in pattern.finditer(search_text):
        items.append({
            'statement': norm(m.group(1)),
            'page': int(m.group(2)),
            'wrong_explanation': norm(m.group(3)) if m.group(3) else None,
        })
    return items


def _rect_contains(outer, inner, tol=3):
    return (outer.x0 - tol <= inner.x0 and outer.y0 - tol <= inner.y0 and
            outer.x1 + tol >= inner.x1 and outer.y1 + tol >= inner.y1)


def find_ox_answers(page):
    """'○'/'X' 글자 위에 강조색 fill이 덮여있는 쪽을 정답으로 판별."""
    words = page.get_text('words')
    ox_words = [w for w in words if w[4] in ('○', 'X', 'O')]
    if not ox_words:
        return []

    drawings = page.get_drawings()
    page_area = page.rect.get_area()
    candidate_fills = [
        d for d in drawings
        if d['type'] == 'f' and d.get('fill') and d['fill'] != (1.0, 1.0, 1.0)
        and d['rect'].get_area() < 0.5 * page_area
    ]

    rows = []
    for w in sorted(ox_words, key=lambda w: w[1]):
        wrect = fitz.Rect(w[0], w[1], w[2], w[3])
        placed = False
        for row in rows:
            if abs(row['y'] - w[1]) < 5:
                row['words'].append((w[4], wrect))
                placed = True
                break
        if not placed:
            rows.append({'y': w[1], 'words': [(w[4], wrect)]})

    answers = []
    for row in sorted(rows, key=lambda r: r['y']):
        answer = None
        for label, wrect in row['words']:
            for d in candidate_fills:
                if _rect_contains(d['rect'], wrect):
                    answer = label
                    break
            if answer:
                break
        answers.append({'y': row['y'], 'answer': answer})
    return answers


# ────────────────────────────────────────────────
# 2. 질문과 토론 (발문/라벨/모범답안)
# ────────────────────────────────────────────────

def get_lines_with_bbox(page):
    d = page.get_text('dict')
    out = []
    for block in d['blocks']:
        for line in block.get('lines', []):
            text = ''.join(s['text'] for s in line['spans'])
            if text.strip():
                bbox = line['bbox']
                out.append({'y0': bbox[1], 'y1': bbox[3], 'x0': bbox[0], 'text': text})
    out.sort(key=lambda l: (round(l['y0']), l['x0']))
    return out


def find_cell_pairs(page):
    """색칠된 라벨 셀 + 테두리로 구한 답변 셀 rect 쌍 목록 (y0 순)."""
    drawings = page.get_drawings()
    fills = [d for d in drawings if d['type'] == 'f' and d.get('fill')]
    strokes = [d for d in drawings if d['type'] == 's']
    page_area = page.rect.get_area()

    candidates = [
        d for d in fills
        if d['rect'].get_area() < 0.5 * page_area and (d['rect'].y1 - d['rect'].y0) > 40
    ]

    cells = []
    for d in candidates:
        r = d['rect']
        right_edge = r.x1
        for s in strokes:
            sr = s['rect']
            is_horiz = abs(sr.y0 - sr.y1) < 1
            near_top = abs(sr.y0 - r.y0) < 2
            near_bottom = abs(sr.y0 - r.y1) < 2
            if is_horiz and (near_top or near_bottom) and sr.x1 > right_edge:
                right_edge = max(right_edge, sr.x1)
        if right_edge > r.x1 + 5:
            answer_rect = fitz.Rect(r.x1, r.y0, right_edge, r.y1)
            cells.append({
                'label_rect': r,
                'answer_rect': answer_rect,
                'label_text': norm(page.get_text('text', clip=r)),
                'answer_text': norm(page.get_text('text', clip=answer_rect)),
            })
    cells.sort(key=lambda c: c['label_rect'].y0)
    return cells


def parse_discussion(doc, page_indices):
    """discussion 섹션 페이지들에서 독해유형 블록 -> 인용문/문항 목록 추출.

    핵심: 색칠된 표 셀(라벨+모범답안)의 y범위에 속하는 텍스트 라인은
    발문/인용문 파싱에서 전부 건너뛴다. 이렇게 하면
      - 모범답안 안의 "1. / 2." 같은 번호매김이 새 문항으로 오인되는 문제
      - 발문 뒤에 라벨/모범답안 텍스트가 그대로 붙어버리는 문제
    를 동시에 해결할 수 있다. 셀은 문항 하나당 여러 개(예: 비교표 2행)
    있을 수 있으므로 items[i]['cells']는 리스트로 유지한다.
    """
    blocks = []
    current = None

    for pidx in page_indices:
        page = doc[pidx]
        lines = get_lines_with_bbox(page)
        cells = find_cell_pairs(page)
        cell_ranges = [(c['label_rect'].y0, c['label_rect'].y1, c) for c in cells]
        page_blocks_start_idx = len(blocks)  # 이 페이지에서 새로 닫힌 블록들의 시작 인덱스

        def in_any_cell(y):
            return any(y0 - 2 <= y <= y1 + 2 for y0, y1, _ in cell_ranges)

        mode = None
        excerpt_buf = []
        i = 0
        n = len(lines)
        while i < n:
            line = lines[i]
            y = line['y0']

            if in_any_cell(y):
                i += 1
                continue

            s = norm(line['text'])

            if READING_TYPE_RE.match(s):
                if current:
                    blocks.append(current)
                current = {'reading_type': s, 'excerpts': [], 'items': []}
                mode = 'excerpt'
                excerpt_buf = []
                i += 1
                continue

            if current is None:
                i += 1
                continue

            m_ref = PAGE_REF_RE.match(s)
            if m_ref and mode == 'excerpt':
                current['excerpts'].append({
                    'text': norm(' '.join(excerpt_buf)),
                    'page_ref': m_ref.group(1),
                })
                excerpt_buf = []
                i += 1
                continue

            m_q = QNUM_RE.match(s)
            if m_q:
                mode = 'question'
                qtext_lines = [m_q.group(2)]
                item_y_start = y
                j = i + 1
                while j < n:
                    nline = lines[j]
                    ny = nline['y0']
                    if in_any_cell(ny):
                        break
                    ns = norm(nline['text'])
                    if QNUM_RE.match(ns) or READING_TYPE_RE.match(ns):
                        break
                    qtext_lines.append(ns)
                    j += 1
                current['items'].append({
                    'question_no': m_q.group(1),
                    'question_text': norm(' '.join(qtext_lines)),
                    'y_start': item_y_start,
                    'cells': [],
                })
                i = j
                continue

            if mode == 'excerpt':
                excerpt_buf.append(s)
            i += 1

        # 이 페이지의 셀들을 y순으로 문항들에 배분 (문항당 여러 셀 허용).
        # 한 페이지에 독해유형 블록이 여러 개 있을 수 있으므로(예: Q1, Q2가
        # 같은 페이지), 이 페이지에서 닫힌 블록들 + 현재 진행 중인 블록의
        # 문항을 모두 모아서 배분 대상으로 삼는다.
        page_items = []
        for b in blocks[page_blocks_start_idx:]:
            page_items.extend(b['items'])
        if current:
            page_items.extend(current['items'])

        for y0, y1, cell in cell_ranges:
            owner = None
            for it in page_items:
                if it['y_start'] <= y0:
                    owner = it
            if owner is not None:
                owner['cells'].append(cell)

    if current:
        blocks.append(current)

    # 정리: cells -> label/model_answer(단일) 또는 sub_answers(복수)로 펼치기
    for b in blocks:
        for it in b['items']:
            cells = it.pop('cells', [])
            it.pop('y_start', None)
            if len(cells) == 1:
                it['label'] = cells[0]['label_text']
                it['model_answer'] = cells[0]['answer_text']
            elif len(cells) > 1:
                it['label'] = None
                it['model_answer'] = None
                it['sub_answers'] = [
                    {'label': c['label_text'], 'answer': c['answer_text']} for c in cells
                ]
            else:
                it['label'] = None
                it['model_answer'] = None
    return blocks


# ────────────────────────────────────────────────
# 3. 글쓰기
# ────────────────────────────────────────────────

def parse_writing(text):
    lines = [norm(l) for l in text.split('\n') if norm(l)]
    theme = None
    step1 = []
    step2 = None
    mode = None
    for l in lines:
        if l.startswith('Step 1'):
            mode = 'step1'
            continue
        if l.startswith('Step 2'):
            mode = 'step2'
            continue
        if mode == 'step1':
            m = re.match(r'^\d+\.\s*(.*)', l) or re.match(r'^[-+]\s*(.*)', l)
            if m:
                step1.append(m.group(1))
        elif mode == 'step2':
            step2 = (step2 + ' ' + l) if step2 else l
        elif mode is None and not l.startswith('"') and not l.startswith('“') \
                and l not in ('3', '글쓰기', '내 글로 엮기') and not PAGE_MARKER_RE.match(l):
            theme = l
    return {'theme': theme, 'step1_questions': step1, 'step2_instruction': step2}


# ────────────────────────────────────────────────
# 메인
# ────────────────────────────────────────────────

def extract(student_path, teacher_path):
    doc_s = fitz.open(student_path)
    doc_t = fitz.open(teacher_path)

    pages_text_s = [doc_s[i].get_text() for i in range(len(doc_s))]
    pages_text_t = [doc_t[i].get_text() for i in range(len(doc_t))]

    bounds = find_section_bounds(pages_text_t)

    cover_text = '\n'.join(pages_text_t[slice(*bounds['cover'])])
    cover = parse_cover(cover_text)

    vocab, ox_items = [], []
    if bounds['vocab_ox']:
        a, b = bounds['vocab_ox']
        student_vocab_text = '\n'.join(pages_text_s[a:b])
        teacher_vocab_text = '\n'.join(pages_text_t[a:b])
        vocab = parse_vocab(student_vocab_text)
        ox_stmts = parse_ox_statements(teacher_vocab_text)

        ox_answers = []
        for pidx in range(a, b):
            ox_answers.extend(find_ox_answers(doc_t[pidx]))
        ox_answers.sort(key=lambda x: x['y'])

        for stmt, ans in zip(ox_stmts, ox_answers):
            ox_items.append({**stmt, 'answer': ans['answer']})
        if len(ox_stmts) != len(ox_answers):
            print(f'[WARN] OX 문장 {len(ox_stmts)}건 vs 정답표시 {len(ox_answers)}건 불일치', file=sys.stderr)

    discussion = []
    if bounds['discussion']:
        a, b = bounds['discussion']
        discussion = parse_discussion(doc_t, range(a, b))

    writing = {}
    if bounds['writing']:
        a, b = bounds['writing']
        writing = parse_writing('\n'.join(pages_text_t[a:b]))

    return {
        'cover': cover,
        'vocabulary': vocab,
        'ox_quiz': ox_items,
        'discussion_questions': discussion,
        'writing_prompt': writing,
        'source': {
            'student_pdf': str(student_path),
            'teacher_pdf': str(teacher_path),
        },
    }
