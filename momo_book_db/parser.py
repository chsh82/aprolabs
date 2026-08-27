# -*- coding: utf-8 -*-
"""
모모의책장 교재DB — Phase 1 파서 (A1 계열: 1단계 어휘/OX, 2단계 질문과 토론, 3단계 글쓰기)
학생용 PDF -> 골든 샘플과 같은 모양의 JSON.

사용법: python parser.py <학생용 PDF 경로> [출력 json 경로]
"""
import os
import re
import sys
import json

import fitz
import pdfplumber

UI_TYPE_CATALOG = """\
- text_short: 짧은 서술형 빈칸 (한 줄 정도의 시작 문구/라벨 하나)
- text_long: 장문 서술형 빈칸 (긴 문단을 쓰는 칸, 또는 답변란 형태가 불분명함)
- choice_ab: "동의할 수 있어요!"/"동의할 수 없어요!" 같은 양자택일
- choice_multi: 3개 이상의 선택지 중 고르는 형태
- text_short_multi: 짧은 빈칸/라벨이 2개 붙어있는 형태
- table_compare: 여러 열(컬럼)로 된 비교표
- unknown: 위 어느 것에도 맞지 않거나 판단하기 어려움
"""

LLM_PROMPT_TEMPLATE = """다음은 초등학생 독서논술 학습지의 한 문항을 PDF에서 그대로 뽑은 원문입니다.
줄바꿈은 PDF가 임의의 폭에서 끊은 것이라 신뢰할 수 없습니다(단어 중간에서 끊길 수 있음).

원문:
\"\"\"
{body_text}
\"\"\"

이 원문에서:
1. "question_text": 답변란(빈칸 시작 문구, 선택지 라벨 등)을 제외한 나머지 전체 문장을 담아주세요.
   질문 앞에 나오는 설명/맥락 문장도 학생이 실제로 읽는 문항의 일부이니 포함해야 합니다
   (마지막 물음표로 끝나는 문장 하나만 뽑는 게 아닙니다).
   원문 표현을 절대 바꾸거나 다른 말로 다시 쓰지 마세요(paraphrase 금지) - 줄바꿈으로 끊긴
   단어만 자연스럽게 붙이고, 그 외에는 원문 그대로 옮겨주세요.
2. "ui_type": 답변란의 형태를 아래 목록 중 하나로 분류하세요.
{catalog}
3. "ui_config": ui_type에 맞는 설정(JSON 객체). 예:
   - choice_ab/choice_multi -> {{"options": [...]}}
   - text_short_multi -> {{"blanks": [...]}}
   - text_short -> {{"starter": "..."}}
   - text_long/table_compare/unknown -> {{}}

아래 JSON 형식으로만 답하세요. 다른 설명은 붙이지 마세요.
{{"question_text": "...", "ui_type": "...", "ui_config": {{...}}}}
"""


def llm_classify_discussion_item(body_text: str, model: str = None) -> dict | None:
    """규칙 기반으로 애매한 문항의 question_text/ui_type/ui_config를 LLM에 물어봄.
    실패하면 None (호출한 쪽에서 규칙 기반 결과로 fallback)."""
    try:
        from anthropic import Anthropic
    except ImportError:
        return None

    model = model or os.environ.get('MOMOAI_MODEL', 'claude-haiku-4-5-20251001')
    prompt = LLM_PROMPT_TEMPLATE.format(body_text=body_text, catalog=UI_TYPE_CATALOG)
    try:
        client = Anthropic()
        resp = client.messages.create(
            model=model,
            max_tokens=800,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = resp.content[0].text
        m = re.search(r'\{.*\}', text, re.S)
        if not m:
            return None
        data = json.loads(m.group(0))
        if 'question_text' not in data or 'ui_type' not in data:
            return None
        if not isinstance(data.get('ui_config'), dict):
            data['ui_config'] = {}
        return data
    except Exception as e:
        print(f'[LLM 분류 실패, 규칙 기반으로 대체] {e}', file=sys.stderr)
        return None

READING_TYPE_ORDER = ["사실적", "추론적", "비판적", "분석적", "적용적"]
# 태그 표기가 "비판적 / 추론적 독해"처럼 슬래시로 나뉘기도 하고, "비판적추론적/ 독해"처럼
# 단어 사이 구분자 없이 붙어서 나오기도 함(교재별 PDF 조판 차이). 둘 다 매칭.
# 태그 표기 변형: "사실적 독해"(기본), "[1] 사실적 독해"/"[1~3] 추론적/비판적 독해"
# (문항번호(범위)가 앞에 붙음), "[분석적 독해]"(태그 전체가 대괄호로 감싸짐)
TAG_LINE_RE = re.compile(r'^(?:\[[\d~\-]+\]\s*)?\[?\s*((?:[가-힣]+적\s*/?\s*)+)독해\s*\]?\s*$')
# 저학년 교재는 태그와 문항 번호가 한 줄에 붙어 나옴: "[비교하며 읽기] 5. 페르시아..."
INLINE_TAG_QNUM_RE = re.compile(r'^\[([^\]]+)\]\s*(\d+(?:-\d+)?\.\s*.*)$')
# 저학년 교재는 "독해"로 끝나지 않는 태그를 씀: "[비교하며 읽기]", "[생각하며 읽기]",
# "[사실적 이해]" 등. 줄 전체가 대괄호 하나로만 된 경우만 태그로 인정해서, 문장 중간에
# 나오는 선택지형 대괄호("바로 [보이는 것/보이지 않는 것] 예요.")와 헷갈리지 않게 함.
LOW_GRADE_TAG_RE = re.compile(r'^\[([가-힣\s/]{2,20})\]$')
PAGE_REF_RE = re.compile(r'^-p\.?\s*([\d~\-]+)', re.IGNORECASE)
PAGE_FOOTER_RE = re.compile(r'^-\s*\d+\s*-$')  # 페이지 하단 쪽번호("- 7 -") - 본문 아님
QNUM_RE = re.compile(r'^(\d+(?:-\d+)?)\.\s*(.*)')
# 교재별로 "1단계"(숫자-단계 붙여서)로 나오는 경우도 있고, "단계"라는 글자와 숫자가
# 서로 다른 텍스트박스에 있어서 추출 순서가 "단계 \n 1"처럼 뒤집혀 나오는 경우도 있음
# (예: "신화의 숲" 교재). 둘 다 매칭하도록 함.
# 숫자(1/2/3단계)로 단계를 찾으면, "2단계 들어가기"처럼 중간에 다른 소단계가 끼어 있어서
# 순서가 1/2/3/4단계로 밀리는 교재를 놓침(예: 초정리 편지 - 3단계가 토론, 4단계가 글쓰기).
# 그래서 숫자 대신 각 단계 표제에 항상 나오는 내용 키워드로 찾고, 페이지 첫 줄(=표제)에서만
# 검사해서 본문 중간에 우연히 같은 단어가 나와도 오탐하지 않게 함.
# 중등 교재는 1단계가 어휘/OX가 아니라 배경지식 설명글이라 어휘 관련 키워드가 전혀 없음.
# 그래도 1단계 번호 자체는 항상 "1단계"로 나오므로(뒤 단계만 중간에 밀리는 경우가 있음)
# 어휘 키워드가 없으면 "1단계" 표기만으로도 인식되게 함(첫 줄만 보므로 오탐 위험 낮음).
STAGE1_RE = re.compile(r'어휘|단어.*(?:퀴즈|문장|O\.?X)|^[\[【]?\s*1\s*단계')
# 폰트 손상 교재는 "심화# 문제"처럼 단어 사이에 이상한 문자(#)가 끼어 나옴 - 공백 대신
# 그런 문자도 허용해서 인식되게 함(요청자 확인 - 폰트 손상은 검수 때 수정)
STAGE2_RE = re.compile(r'질문과\s*토론|심화\s*이해\s*질문|심화[\s#]*문제')
STAGE3_RE = re.compile(r'글쓰기')


def normalize_reading_type(raw: str) -> str:
    """'적용적 / 비판적 독해', '비판적추론적/ 독해'(구분자 없이 붙은 표기) 등을
    사실적<추론적<비판적<분석적<적용적 순으로 정규화.
    저학년 교재는 "비교하며 읽기", "생각하며 읽기"처럼 전혀 다른 태그 어휘를 쓰기도 하는데,
    이 경우 "적"으로 끝나는 조각이 없으니 그냥 원문 그대로 남겨둠(버리지 않음)."""
    parts = re.findall(r'[가-힣]+?적', raw)
    if not parts:
        return raw.strip()
    parts = sorted(set(parts), key=lambda p: READING_TYPE_ORDER.index(p) if p in READING_TYPE_ORDER else 99)
    return '/'.join(parts)


def load_pages(pdf_path: str):
    """각 페이지의 (PyMuPDF 텍스트, pdfplumber page 객체) 리스트.

    fitz의 기본 get_text()는 PDF 내부에 텍스트가 그려진 순서(그림 그리듯 저장된 순서)를
    그대로 따르는데, 교재에 따라 이 순서가 실제 읽는 순서와 달라서(인용부호/말줄임표 등이
    별도 텍스트 조각으로 다른 시점에 그려진 경우) 문장이 뒤섞여 나오는 경우가 있었음
    (예: "신화의 숲" 교재). sort=True로 좌표 기준(위→아래, 좌→우) 정렬하면 실제 읽는
    순서에 훨씬 가까워짐 - 골든샘플 교재로도 회귀 확인함.
    """
    fitz_doc = fitz.open(pdf_path)
    plumber_doc = pdfplumber.open(pdf_path)
    pages = []
    for i in range(len(fitz_doc)):
        pages.append({
            'no': i + 1,
            'text': fitz_doc[i].get_text(sort=True),
            'plumber': plumber_doc.pages[i],
        })
    return pages


def find_stage_boundaries(pages):
    """어휘/토론/글쓰기 단계가 시작하는 페이지 번호(1-based) 찾기.
    각 단계 표제는 보통 그 페이지의 첫 줄에 나오지만, "이름/날짜" 기입란처럼 표제 앞에
    한 줄이 더 끼어 나오는 교재가 있어서(예: "누가 내 시간을 훔쳐갔지?") 첫 두 줄까지 검사함."""
    stage1 = stage2 = stage3 = None
    for p in pages:
        head_lines = [l.strip() for l in p['text'].splitlines() if l.strip()]
        head = ' '.join(head_lines[:2])
        if stage1 is None and STAGE1_RE.search(head):
            stage1 = p['no']
        if stage2 is None and STAGE2_RE.search(head):
            stage2 = p['no']
        if stage3 is None and STAGE3_RE.search(head):
            stage3 = p['no']
    return stage1, stage2, stage3


def _ox_row_text(row):
    """OX 표 한 행에서 문장 텍스트를 뽑음. 보통 문장이 첫 칸에 다 있지만, 줄바꿈 때문에
    문장이 둘로 나뉘고 마지막 조각에 "○ X" 표시가 붙어 나오는 경우도 있어서(예: 초1
    "세계의 문화"), 그런 셀들도 모두 이어붙이고 끝의 ○/X 표시만 떼어냄."""
    cells = [(c or '').replace('\n', ' ').strip() for c in row if (c or '').strip()]
    if cells:
        cells[-1] = re.sub(r'[○Xx\s]+$', '', cells[-1]).strip()
    return ' '.join(c for c in cells if c and c not in ('○', 'X', 'x'))


def _is_ox_table(rows):
    """행 중에 ○/X 정답 칸이 있으면 OX 표로 판단.
    줄바꿈 때문에 "...기 힘든 곳이에요. ○ X"처럼 문장 끝과 ○/X가 한 셀에 합쳐지는
    경우도 있어서, 셀 전체 일치 외에 끝부분에 ○ X가 붙어있는지도 확인함."""
    for row in rows:
        for c in row:
            cell = (c or '').strip()
            if cell in ('○', 'X', 'x'):
                return True
            if re.search(r'○\s*[Xx]\s*$', cell):
                return True
    return False


def _parse_vocab_table_standard(rows, page_no):
    """"단어 / 뜻 / 문장" 형식 - 단어별로 뜻, 문장 만들기 칸이 이어짐"""
    vocab = []
    order = 0
    current = None
    for row in rows:
        col0 = (row[0] or '').strip()
        col1 = (row[1] or '').strip()
        if col0 and col0 not in ('단어',):
            m = re.match(r'(.+?)\s*\((\d+)\s*페이지\)', col0.replace('\n', ' '))
            order += 1
            current = {
                'order_no': order,
                'word': m.group(1).strip() if m else col0,
                'book_page': int(m.group(2)) if m else None,
                'definition': None,
                'example_sentence': None,
                'source_page': page_no,
                'raw_text': ' | '.join(c for c in row if c) or None,
            }
            vocab.append(current)
        if current and col1 == '뜻':
            current['definition'] = (row[2] or '').strip() or None
            current['raw_text'] = (current['raw_text'] or '') + ' / ' + ' | '.join(c for c in row if c)
        if current and col1 == '문장':
            current['example_sentence'] = (row[2] or '').strip() or None
            current['raw_text'] = (current['raw_text'] or '') + ' / ' + ' | '.join(c for c in row if c)
    return vocab


def _parse_vocab_table_hint_quiz(rows, page_no):
    """"의미 / 용어" 형식 - 실제 단어 철자 없이 뜻과 자음 힌트(예: ㄴㄹㅅㅅㅈ)만 주어짐.
    word 칸에는 자음 힌트를, definition 칸에는 뜻을 넣는다(2026-08-26 요청자 확인)."""
    vocab = []
    order = 0
    for row in rows:
        col0 = (row[0] or '').replace('\n', ' ').strip()
        col1 = (row[1] or '').replace('\n', ' ').strip()
        if col0 in ('의미',) or not col0:
            continue
        order += 1
        vocab.append({
            'order_no': order,
            'word': col1 or None,
            'book_page': None,
            'definition': col0 or None,
            'example_sentence': None,
            'source_page': page_no,
            'raw_text': ' | '.join(c for c in row if c) or None,
        })
    return vocab


def _parse_vocab_table_words_only(rows, page_no):
    """선 잇기(매칭) 형식 - 단어 목록과 뜻 목록이 각각 한 칸짜리 표로 따로 뽑히는데,
    인쇄 순서가 정답 짝이 아니라서(일부러 순서를 꼬아서 줄로 잇게 만듦) 뜻과 자동으로
    맞출 수 없음. 단어만 채우고 definition은 비워둠(요청자 확인, 2026-08-26)."""
    vocab = []
    order = 0
    for row in rows:
        word = (row[0] or '').replace('\n', ' ').strip()
        if not word:
            continue
        order += 1
        vocab.append({
            'order_no': order, 'word': word, 'book_page': None, 'definition': None,
            'example_sentence': None, 'source_page': page_no, 'raw_text': word,
        })
    return vocab


def parse_vocabulary_and_ox(page, log):
    """1단계 페이지의 표(어휘, OX)를 파싱.

    표 개수/순서가 책마다 다를 수 있어서(예: 제목줄이 별도 1행짜리 표로 잡히는 경우,
    어휘 문제가 "단어/뜻/문장"이 아니라 "의미/용어(자음 힌트)" 퀴즈 형식인 경우),
    위치(tables[0], tables[1])로 단정하지 않고 내용으로 어떤 표인지 판별한다.
    """
    tables = page['plumber'].extract_tables()
    # 1행짜리 표는 제목줄이 표로 잘못 잡힌 것(예: "1단계 | 어휘력 향상 & ...") - 걸러냄
    real_tables = [t for t in tables if len(t) > 1]

    ox_table = next((t for t in real_tables if _is_ox_table(t)), None)
    vocab_table = None
    vocab_format = None
    for t in real_tables:
        if t is ox_table:
            continue
        header0 = (t[0][0] or '').strip()
        if header0 == '의미':
            vocab_table, vocab_format = t, 'hint_quiz'
            break
        if header0 == '단어':
            vocab_table, vocab_format = t, 'standard'
            break

    matching_words = None
    if vocab_table is None:
        # 선 잇기(매칭) 형식: 단어 목록, 뜻 목록이 각각 한 칸짜리 표로 따로 뽑힘(초1/초2)
        single_col_tables = [
            t for t in real_tables
            if t is not ox_table and all(len([c for c in row if (c or '').strip()]) <= 1 for row in t)
        ]
        if len(single_col_tables) >= 2:
            def _avg_len(t):
                cells = [(row[0] or '').strip() for row in t if (row[0] or '').strip()]
                return sum(len(c) for c in cells) / max(len(cells), 1)
            # 단어가 뜻보다 훨씬 짧으므로, 평균 글자수가 가장 짧은 표를 단어 목록으로 판단
            word_table = min(single_col_tables, key=_avg_len)
            vocab_table, vocab_format = word_table, 'matching_words_only'
        else:
            # 표로도 못 뽑히는 경우가 있음 - "단어  ∙       ∙  뜻..." 처럼 단어 옆에 항상
            # 연결점(∙)이 붙어 나오므로, 표 대신 원문 줄에서 "∙" 앞 단어를 직접 찾음
            found = [m.group(1) for m in (re.match(r'^\s*([^\s∙]+)\s*∙', l) for l in page['text'].splitlines()) if m]
            if found:
                matching_words = found

    if vocab_table is None and matching_words is None and ox_table is None:
        # 표가 없거나, 있어도 어휘/OX로 알려진 형식이 아님 - 배경지식 설명글로 보임(중등 교재에서 흔함)
        return [], [], True

    vocab, ox = [], []

    if vocab_table is not None:
        if vocab_format == 'hint_quiz':
            vocab = _parse_vocab_table_hint_quiz(vocab_table, page['no'])
            log.append({'level': 'info', 'stage': 'vocabulary',
                        'message': '이 페이지는 "의미/용어(자음 힌트)" 퀴즈 형식 - word 칸에 자음 힌트, definition 칸에 뜻을 넣음'})
        elif vocab_format == 'matching_words_only':
            vocab = _parse_vocab_table_words_only(vocab_table, page['no'])
            log.append({'level': 'info', 'stage': 'vocabulary',
                        'message': '이 페이지는 선 잇기(매칭) 형식 - 인쇄 순서가 정답 짝이 아니라서 단어만 자동 추출함(뜻은 직접 확인 필요)'})
        else:
            vocab = _parse_vocab_table_standard(vocab_table, page['no'])
    elif matching_words is not None:
        vocab = [
            {'order_no': i, 'word': w, 'book_page': None, 'definition': None,
             'example_sentence': None, 'source_page': page['no'], 'raw_text': w}
            for i, w in enumerate(matching_words, start=1)
        ]
        log.append({'level': 'info', 'stage': 'vocabulary',
                    'message': '이 페이지는 선 잇기(매칭) 형식 - 인쇄 순서가 정답 짝이 아니라서 단어만 자동 추출함(뜻은 직접 확인 필요)'})
    else:
        log.append({'level': 'error', 'stage': 'vocabulary', 'message': f'{page["no"]}페이지에서 표를 찾지 못함'})

    if ox_table is not None:
        for i, row in enumerate(ox_table, start=1):
            statement_raw = _ox_row_text(row)
            m = re.search(r'\((\d+)\s*페이지\)\s*$', statement_raw)
            page_no = int(m.group(1)) if m else None
            statement = re.sub(r'\s*\(\d+\s*페이지\)\s*$', '', statement_raw).strip()
            ox.append({
                'order_no': i,
                'question': statement,
                'answer': None,
                'evidence_page': page_no,
                'explanation': None,
                'source_page': page['no'],
                'raw_text': ' | '.join(c for c in row if c) or None,
            })
    else:
        log.append({'level': 'error', 'stage': 'ox_quiz', 'message': f'{page["no"]}페이지에서 OX 표를 찾지 못함'})

    if vocab:
        log.append({'level': 'warning', 'stage': 'vocabulary', 'message': '어휘 항목에 명시적 번호가 없어 등장 순서로 order_no 부여함'})
    if ox:
        log.append({'level': 'warning', 'stage': 'ox_quiz', 'message': 'OX 문항에 명시적 번호가 없어 등장 순서로 order_no 부여함'})

    return vocab, ox, False


# 답변란(ui_type) 패턴 - 원문 텍스트 뒷부분에서 정규식으로 판별
CHOICE_AB_RE = re.compile(r'동의할 수 있어요!\s*동의할 수 없어요!', re.S)


def _group_into_labels(tail_text: str):
    """줄바꿈으로 쪼개진 라벨을 "!"나 "?"로 끝날 때까지 모아 하나의 라벨로 합침
    (예: "법이 기준이 되어야 \n해요!" -> "법이 기준이 되어야 해요!")"""
    labels = []
    buf = ''
    for raw_line in tail_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        buf = (buf + ' ' + line).strip() if buf else line
        if line[-1] in '!?':
            labels.append(buf)
            buf = ''
    if buf:
        labels.append(buf)
    return labels


def guess_ui_type(tail_text: str):
    """문항 텍스트 뒤에 붙은 답변란 힌트로 ui_type/ui_config 추정"""
    tail_text = tail_text.strip()
    if CHOICE_AB_RE.search(tail_text):
        return 'choice_ab', {'options': ['동의할 수 있어요!', '동의할 수 없어요!']}

    labels = _group_into_labels(tail_text)
    short_labels = [l for l in labels if len(l) < 20]
    if len(short_labels) >= 3 and len(short_labels) == len(labels):
        return 'choice_multi', {'options': short_labels}
    if len(short_labels) == 2 and len(short_labels) == len(labels):
        return 'text_short_multi', {'blanks': short_labels}

    joined = ' '.join(tail_text.split())
    if joined and len(joined) < 30:
        return 'text_short', {'starter': joined}
    return 'text_long', {}


def parse_discussion(pages, stage2_start, stage3_start, log, use_llm=False):
    items = []
    current_tag = None
    order_no = 0

    full_text = ''
    page_map = []  # (char_start, page_no)
    for p in pages:
        if p['no'] < stage2_start or p['no'] >= stage3_start:
            continue
        page_map.append((len(full_text), p['no']))
        full_text += p['text'] + '\n'

    def page_of(pos):
        pn = page_map[0][1]
        for start, no in page_map:
            if start <= pos:
                pn = no
        return pn

    lines = full_text.splitlines()
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].strip()
        # 저학년 교재는 태그와 문항 번호가 한 줄에 붙어 나옴: "[비교하며 읽기] 5. 페르시아..."
        inline_match = INLINE_TAG_QNUM_RE.match(line)
        if inline_match:
            current_tag = normalize_reading_type(inline_match.group(1))
            line = inline_match.group(2)

        tag_match = TAG_LINE_RE.match(line) or LOW_GRADE_TAG_RE.match(line)
        if tag_match:
            current_tag = normalize_reading_type(tag_match.group(1))
            i += 1
            continue

        qnum_match = QNUM_RE.match(line)
        # 문항 번호가 없는 경우도 있음(예: 발췌문+"-p.NN" 뒤에 번호 없이 바로 질문이 이어짐).
        # "-p.NN" 줄 바로 다음(빈 줄 제외)이 번호로 시작하지 않으면 번호 없는 문항으로 보고,
        # 이전 문항 번호 + 1로 순서를 추정해서 입력함(요청자 확인, 2026-08-26).
        pageref_here = None if qnum_match else PAGE_REF_RE.match(line)
        is_unnumbered_start = False
        if pageref_here:
            j2 = i + 1
            while j2 < n and not lines[j2].strip():
                j2 += 1
            nxt_line = lines[j2].strip() if j2 < n else ''
            if not QNUM_RE.match(nxt_line):
                is_unnumbered_start = True

        if qnum_match or is_unnumbered_start:
            if qnum_match:
                order_label = qnum_match.group(1)
                order_no_val = int(order_label.split('-')[0])
                body_seed = [qnum_match.group(2)]
            else:
                order_no_val = order_no + 1
                order_label = str(order_no_val)
                body_seed = []
                log.append({'level': 'info', 'stage': 'discussion_qa',
                            'message': f'{order_no_val}번 문항에 번호 표기가 없어 순서로 추정함'})

            # 발췌문: 이 문항 앞, page-ref 줄("-p.NN")까지 역방향 수집.
            # sort=True로 뽑으면 줄마다 빈 줄이 섞여 나와서 빈 줄은 그냥 건너뛰고,
            # 태그줄/이전 문항줄을 만나면 멈춤. "-p.NN"을 못 만나고 멈추면(=4-2처럼
            # 발췌문 없이 이어지는 소문항) 발췌문 없음(None)으로 처리함.
            excerpt_lines = []
            j = i - 1
            excerpt_page = None
            found_page_ref = False
            while j >= 0:
                prev = lines[j].strip()
                if TAG_LINE_RE.match(prev) or LOW_GRADE_TAG_RE.match(prev) or QNUM_RE.match(prev) or INLINE_TAG_QNUM_RE.match(prev):
                    break
                pref = PAGE_REF_RE.match(prev)
                if pref:
                    found_page_ref = True
                    excerpt_page = int(re.findall(r'\d+', pref.group(1))[0])
                    j -= 1
                    continue
                if prev:
                    excerpt_lines.insert(0, prev)
                j -= 1
            if pageref_here:
                # 번호 없는 문항은 자기 자신이 "-p.NN" 줄이므로 그 값을 그대로 씀
                found_page_ref = True
                excerpt_page = int(re.findall(r'\d+', pageref_here.group(1))[0])
            excerpt_text = ' '.join(excerpt_lines).strip() if found_page_ref and excerpt_lines else None

            # 질문 본문 + 답변란: 다음 태그줄/문항줄/페이지 끝까지 모음
            body_lines = list(body_seed)
            k = i + 1
            while k < n:
                nxt = lines[k].strip()
                if TAG_LINE_RE.match(nxt) or LOW_GRADE_TAG_RE.match(nxt) or QNUM_RE.match(nxt) or PAGE_REF_RE.match(nxt) or INLINE_TAG_QNUM_RE.match(nxt):
                    break
                if nxt and not PAGE_FOOTER_RE.match(nxt):
                    body_lines.append(nxt)
                k += 1
            # 질문 텍스트 vs 답변란 분리.
            # 규칙 기반 시도(문장 종결부호 단위로 뭉친 뒤 끝에서 짧은 뭉치를 답변란으로 뗌).
            units = []
            buf = ''
            for l in body_lines:
                buf = (buf + ' ' + l).strip() if buf else l
                if buf and buf[-1] in '.?!~”’」』':
                    units.append(buf)
                    buf = ''
            if buf:
                units.append(buf)

            tail_units = []
            while len(units) > 1 and len(units[-1]) < 20:
                tail_units.insert(0, units.pop())
            question_text = ' '.join(units).strip()
            tail = '\n'.join(tail_units)
            ui_type, ui_config = guess_ui_type(tail)
            llm_used = False

            # 규칙 기반은 항상 무언가를 반환하므로, use_llm이면 LLM 결과로 덮어씀
            # (규칙 기반 결과는 LLM이 실패할 때의 fallback으로 남겨둠)
            if use_llm:
                llm_result = llm_classify_discussion_item('\n'.join(body_lines))
                if llm_result:
                    question_text = llm_result['question_text']
                    ui_type = llm_result['ui_type']
                    ui_config = llm_result['ui_config']
                    llm_used = True
                else:
                    log.append({'level': 'warning', 'stage': 'discussion_qa',
                                'message': f'{order_label}번 문항 LLM 분류 실패 - 규칙 기반 결과 사용'})

            items.append({
                'order_no': order_no_val,
                'order_label': order_label,
                'reading_type': current_tag,
                'excerpt_text': excerpt_text,
                'excerpt_page': excerpt_page,
                'question_text': re.sub(r'\s+', ' ', question_text).strip(),
                'ui_type': ui_type,
                'ui_config': json.dumps(ui_config, ensure_ascii=False),
                'model_answer': None,
                'source_page': page_of(sum(len(l) + 1 for l in lines[:i])),
                'extraction_confidence': 0.9 if llm_used else 0.6,
                'raw_text': '\n'.join(body_lines),
            })
            order_no = order_no_val
            i = k
            continue
        i += 1

    return items


STEP_RE = re.compile(r'^Step\s*(\d+)[.:]\s*(.*)')
SUBNUM_RE = re.compile(r'^(\d+)\.\s*(.*)')


def parse_essay(pages, stage3_start, log):
    text = ''
    page_no = stage3_start
    for p in pages:
        if p['no'] >= stage3_start:
            text = p['text']
            page_no = p['no']
            break

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    step_markers = []  # (line_idx, step_num, 같은 줄에 이어붙은 텍스트)
    for idx, l in enumerate(lines):
        m = STEP_RE.match(l)
        if m:
            step_markers.append((idx, int(m.group(1)), m.group(2)))

    # main_topic 추정. 제목 블록 다음에 책 속 인용문이 통째로 붙어 나오는 교재도 있어서
    # (예: 초6 일부), 인용문처럼 보이는 블록("..." 로 시작하거나 "(81)"같은 쪽번호 인용으로
    # 끝나는 블록)은 제외하고 남은 마지막 블록을 씀. 빈 줄이 문단 경계 신호라서(sort=True)
    # 여기서는 빈 줄을 지우지 않은 원본 줄로 블록을 나눔.
    raw_lines = text.splitlines()
    header_re = re.compile(r'^\d+\s*(?:단계\s*)?글쓰기')

    def _clean_blocks(block_lines):
        blocks = _split_into_blocks(block_lines)
        blocks = [[l for l in b if not header_re.match(l)] for b in blocks]
        blocks = [b for b in blocks if b]
        split_blocks = []
        for b in blocks:
            split_blocks.extend(_split_quote_prefix_block(b))
        return [b for b in split_blocks if b]

    if step_markers:
        first_raw_idx = next((i for i, l in enumerate(raw_lines) if STEP_RE.match(l.strip())), len(raw_lines))
        scan_blocks = _clean_blocks(raw_lines[:first_raw_idx])
    else:
        # Step 구조가 전혀 없는 교재도 있음(예: 중2 일부 - 인용문 비교 + 질문 하나만 있는 자유
        # 서술형). 이 경우 전체 내용에서 main_topic을 찾음.
        scan_blocks = _clean_blocks(raw_lines)

    topic_blocks = [b for b in scan_blocks if not _looks_like_quote_block(b)]
    main_block = topic_blocks[-1] if topic_blocks else None
    main_topic = ' '.join(main_block).strip() if main_block else None

    # "글쓰기 안내": 주제(main_topic)와 Step1 사이(Step 구조가 없으면 본문 전체)에 있는
    # 나머지 내용(안내문/인용문/배경 설명 등)을 모음. 주제 블록을 뺀 나머지 전부를 씀
    # (요청자 확인, 2026-08-26 - 주제와 Step 사이에 별도 칸으로 넣기로 함)
    guide_blocks = [b for b in scan_blocks if b is not main_block]
    writing_guide = '\n'.join(' '.join(b) for b in guide_blocks).strip() or None

    outline = []
    closing_instruction = None

    if step_markers:
        block0_end = step_markers[1][0] if len(step_markers) > 1 else len(lines)
        block0_lines = lines[step_markers[0][0] + 1:block0_end]
        has_subnum = any(SUBNUM_RE.match(l) for l in block0_lines)

        if has_subnum:
            # "A형": Step 1. 안에 번호 붙은 소질문(1./2./3.)이 있고, Step 2.는 마무리 안내문
            order = 0
            current = None
            for l in block0_lines:
                m = SUBNUM_RE.match(l)
                if m:
                    order += 1
                    role = ['intro', 'body', 'conclusion'][min(order - 1, 2)]
                    current = {'order_no': order, 'question_text': m.group(2), 'role': role}
                    outline.append(current)
                elif current:
                    current['question_text'] = (current['question_text'] + ' ' + l).strip()

            if len(step_markers) > 1:
                idx1, _, trailing1 = step_markers[1]
                closing_lines = ([trailing1] if trailing1 else []) + \
                    [l for l in lines[idx1 + 1:] if not PAGE_FOOTER_RE.match(l)]
                closing_instruction = ' '.join(l for l in closing_lines if l).strip() or None
        else:
            # "B형": Step1./Step2./Step3. 각각이 그 자체로 하나의 소질문(예: 초6 일부 교재)
            for i, (idx, num, trailing) in enumerate(step_markers):
                block_end = step_markers[i + 1][0] if i + 1 < len(step_markers) else len(lines)
                block_lines = ([trailing] if trailing else []) + \
                    [l for l in lines[idx + 1:block_end] if not PAGE_FOOTER_RE.match(l)]
                role = ['intro', 'body', 'conclusion'][min(i, 2)]
                outline.append({'order_no': num, 'question_text': ' '.join(l for l in block_lines if l).strip(), 'role': role})

    if not main_topic:
        log.append({'level': 'warning', 'stage': 'essay_prompt', 'message': 'main_topic 추정 실패'})

    return {
        'main_topic': main_topic,
        'writing_guide': writing_guide,
        'writing_format': 'text_long',
        'min_length': None,
        'closing_instruction': closing_instruction,
        'source_page': page_no,
        'outline_questions': outline,
        'raw_text': text or None,
    }


def _split_into_blocks(lines):
    """sort=True로 뽑은 텍스트는 문장 줄바꿈마다 빈 줄이 하나씩 끼어 있어서,
    빈 줄 1개는 같은 문단 안 줄바꿈, 빈 줄 2개 이상은 문단(블록) 경계로 보고 나눔."""
    blocks = []
    current_block = []
    blank_run = 0
    for raw in lines:
        line = raw.strip()
        if PAGE_FOOTER_RE.match(line):
            continue
        if not line:
            blank_run += 1
            continue
        if blank_run >= 2 and current_block:
            blocks.append(current_block)
            current_block = []
        blank_run = 0
        current_block.append(line)
    if current_block:
        blocks.append(current_block)
    return blocks


def _looks_like_quote_block(block):
    """블록이 책 속 인용문처럼 보이는지(여는 인용부호로 시작하거나 "(81)"같은 쪽번호
    인용으로 끝나는지) 판단 - main_topic 후보에서 제외하기 위함"""
    joined = ' '.join(block)
    return joined.startswith('“') or joined.startswith('"') or bool(re.search(r'\(\w*\d+\w*\)\s*$', joined))


def _split_quote_prefix_block(block):
    """인용문과 그 다음 내용(제목 등) 사이에 빈 줄이 1개뿐이라 한 블록으로 뭉쳐지는 경우가
    있어서(예: 초정리 편지), 블록 안에서 인용부호가 닫히는 줄을 찾으면 그 뒤를 별도 블록으로 뗀다."""
    if not (block[0].startswith('“') or block[0].startswith('"')):
        return [block]
    for i, l in enumerate(block):
        if re.search(r'[”\"]\s*(\(\w*\d+\w*\))?\s*$', l):
            return [block[:i + 1], block[i + 1:]] if block[i + 1:] else [block[:i + 1]]
    return [block]


def extract_cover_message(page1_text: str):
    """표지(1페이지) 하단의 대표 문구를 뽑음.
    표지는 보통 [레벨/주차/제목 블록] - [대표 문구 블록] - [쪽번호] 구조라, 마지막 블록을 씀."""
    blocks = _split_into_blocks(page1_text.splitlines())
    if len(blocks) < 2:
        return None
    return ' '.join(blocks[-1]).strip() or None


def _is_decorative_image(xref, xref_pages):
    """여러 페이지에 반복해서 나오는 이미지(로고 배너 등 장식용)는 내용과 무관하다고 보고 제외"""
    return len(xref_pages.get(xref, ())) > 1


def extract_images(pdf_path: str, stage1_start, stage2_start, stage3_start):
    """표지 이미지(1페이지), 1단계(배경지식) 이미지, 2단계(토론) 페이지별 삽화를 뽑음.
    여러 페이지에 반복 등장하는 이미지(로고 배너 등)는 장식용으로 보고 제외함."""
    fitz_doc = fitz.open(pdf_path)
    xref_pages = {}
    for i in range(len(fitz_doc)):
        for img in fitz_doc[i].get_images():
            xref_pages.setdefault(img[0], set()).add(i)

    images = []

    def _biggest(xrefs):
        best = None
        for xref in xrefs:
            info = fitz_doc.extract_image(xref)
            if best is None or info['width'] * info['height'] > best[1]['width'] * best[1]['height']:
                best = (xref, info)
        return best

    cover_xrefs = [img[0] for img in fitz_doc[0].get_images() if not _is_decorative_image(img[0], xref_pages)]
    best = _biggest(cover_xrefs)
    if best:
        _, info = best
        images.append({'image_type': 'cover', 'source_page': 1, 'ext': info['ext'], 'image_bytes': info['image']})

    if stage1_start:
        page = fitz_doc[stage1_start - 1]
        xrefs = [img[0] for img in page.get_images() if not _is_decorative_image(img[0], xref_pages)]
        best = _biggest(xrefs)
        if best:
            _, info = best
            images.append({'image_type': 'background', 'source_page': stage1_start, 'ext': info['ext'], 'image_bytes': info['image']})

    if stage2_start and stage3_start:
        for page_no in range(stage2_start, stage3_start):
            page = fitz_doc[page_no - 1]
            xrefs = [img[0] for img in page.get_images() if not _is_decorative_image(img[0], xref_pages)]
            best = _biggest(xrefs)
            if best:
                _, info = best
                images.append({'image_type': 'illustration', 'source_page': page_no, 'ext': info['ext'], 'image_bytes': info['image']})

    return images


def parse_pdf(pdf_path: str, use_llm: bool = False) -> dict:
    log = []
    pages = load_pages(pdf_path)
    stage1, stage2, stage3 = find_stage_boundaries(pages)
    if not (stage1 and stage2 and stage3):
        log.append({'level': 'error', 'stage': 'stage_split', 'message': f'단계 구분 실패: 1단계={stage1}, 2단계={stage2}, 3단계={stage3}'})

    vocab, ox = ([], [])
    background_text = None
    for p in pages:
        if p['no'] == stage1:
            vocab, ox, is_background = parse_vocabulary_and_ox(p, log)
            # 저학년 교재는 1단계가 "1-1 어휘"+"1-2 사고" 두 페이지에 걸쳐 있음.
            # 1-1에서 표(어휘/OX/선잇기 단어)를 찾았어도 "1-2 사고" 같은 다음 페이지 내용은
            # 어디에도 안 담기니, stage1 다음 페이지부터는 항상 배경지식으로 같이 담음
            # (1-1 자체가 표를 못 찾은 경우는 stage1페이지부터 포함).
            bg_start = p['no'] if is_background else p['no'] + 1
            bg_end = stage2 if stage2 else p['no'] + 1
            background_text = '\n\n'.join(
                bp['text'].strip() for bp in pages if bg_start <= bp['no'] < bg_end and bp['text'].strip()
            ) or None
            if is_background:
                log.append({'level': 'info', 'stage': 'background_text',
                            'message': f'{p["no"]}페이지에 어휘/OX 표가 없어 배경지식 설명글로 분류함'})
            break

    if not ox:
        # 저학년 교재는 OX퀴즈가 1단계가 아니라 2단계 첫 페이지 맨 앞에 있는 경우가 있음
        for p in pages:
            if stage2 and p['no'] == stage2:
                tables = [t for t in p['plumber'].extract_tables() if len(t) > 1]
                ox_table = next((t for t in tables if _is_ox_table(t)), None)
                if ox_table:
                    ox = []
                    for i, row in enumerate(ox_table, start=1):
                        statement_raw = _ox_row_text(row)
                        m = re.search(r'\((\d+)\s*페이지\)\s*$', statement_raw)
                        page_no = int(m.group(1)) if m else None
                        statement = re.sub(r'\s*\(\d+\s*페이지\)\s*$', '', statement_raw).strip()
                        ox.append({
                            'order_no': i, 'question': statement, 'answer': None,
                            'evidence_page': page_no, 'explanation': None, 'source_page': p['no'],
                            'raw_text': ' | '.join(c for c in row if c) or None,
                        })
                    log.append({'level': 'info', 'stage': 'ox_quiz',
                                'message': f'{p["no"]}페이지(2단계)에서 OX 표를 찾음 - 1단계가 아니라 2단계에 있는 교재'})
                break

    discussion = parse_discussion(pages, stage2, stage3, log, use_llm=use_llm) if stage2 and stage3 else []
    essay = parse_essay(pages, stage3, log) if stage3 else {}

    cover_message = extract_cover_message(pages[0]['text']) if pages else None
    if not cover_message:
        log.append({'level': 'warning', 'stage': 'documents', 'message': '표지 대표 문구 추정 실패'})
    images = extract_images(pdf_path, stage1, stage2, stage3)

    return {
        'cover_message': cover_message,
        'background_text': background_text,
        'vocabulary': vocab,
        'ox_quiz': ox,
        'discussion_qa': discussion,
        'essay_prompt': essay,
        'images': images,
        'extraction_log': log,
    }


if __name__ == '__main__':
    if '--llm' in sys.argv:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
    args = [a for a in sys.argv[1:] if a != '--llm']
    use_llm = '--llm' in sys.argv
    pdf_path = args[0]
    out_path = args[1] if len(args) > 1 else None
    result = parse_pdf(pdf_path, use_llm=use_llm)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if out_path:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f'저장 완료: {out_path}')
    else:
        import sys as _sys
        _sys.stdout.reconfigure(encoding='utf-8')
        print(text)
