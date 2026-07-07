# -*- coding: utf-8 -*-
"""
독서논술 교재 PDF(B 계열: 중1~중3/초6 소설·수필형) -> 문항 정보 JSON 추출 프로토타입.

A1/A2와 달리:
  - 표지에 LV/분기 없이 "[제목_교사용]" 같은 대괄호 헤더로 시작
  - 작품이 여러 개면 "<작품명>"으로 구간이 나뉨
  - "* ..." 로 시작하는 배경지식/수업준비 note가 문항 앞에 옴
  - 문항 번호 앞에 "★", "[모모 제외]" 같은 장식 태그가 붙기도 함
  - 답변이 표가 아니라 "답: ..." 텍스트, 또는 "- 예상 답변 -" 자유서술,
    또는 아예 답 없이 열린 질문으로 끝남
  - 글쓰기가 "Step1./Step2./Step3." (개수 가변) 각각 긴 문단

한계: 답변 라벨(예: "이덕무가 유득공을 찾아간 이유")과 그 내용이 색칠된
표 셀 없이 줄글로만 이어지는 경우, 라벨과 본문을 구분하지 못하고 answer
필드에 통째로 들어간다 (A2와 동일한 근본적 한계).

사용법:
    python test_reading_essay_extract_b.py <교사용.pdf> [output.json]

주의: B 계열은 학생용에 힌트/예시가 상당수 그대로 들어있는 경우가 많아
(A2에서 확인된 패턴과 유사) 교사용 단독 파싱으로 설계함. 필요시 학생용을
추가로 넘겨 답변 유무 diff를 낼 수 있으나 이번 프로토타입에는 미포함.
"""
import re
import sys
import json
from pathlib import Path

import fitz

from test_reading_essay_extract import norm, PAGE_MARKER_RE

BRACKET_HEADER_RE = re.compile(r'^\[(.+)\]$')
WORK_TITLE_RE = re.compile(r'^<(.+)>$')
NOTE_RE = re.compile(r'^\*\s*(.+)')
QNUM_RE = re.compile(r'^(?:\[[^\]]*\]\s*)?(?:★\s*)?(\d+(?:-\d+)?)\.\s*(.*)')
ANSWER_RE = re.compile(r'^답\s*[:：]\s*(.*)')
STEP_RE = re.compile(r'^Step\s*(\d+)\.\s*(.*)', re.IGNORECASE)
STAGE2_RE = re.compile(r'^\d\s*단계$')
WRITING_TOPIC_RE = re.compile(r'^<?글쓰기\s*주제>?$')
EXCERPT_END_RE = re.compile(r'\(\d+\)\s*$')


def get_lines(text):
    return [norm(l) for l in text.split('\n') if norm(l)]


def split_excerpt(tail_lines):
    """tail_lines(문항 첫 줄부터) 중 '(NN)'으로 끝나는 줄까지를 인용문으로 분리."""
    for idx, l in enumerate(tail_lines):
        if EXCERPT_END_RE.search(l):
            return norm(' '.join(tail_lines[:idx + 1])), tail_lines[idx + 1:]
    return None, tail_lines


def split_answer(rest_lines):
    """'답: ...' 마커 기준으로 질문/답변 분리. 마커 없으면 answer=None, 전부 question_or_notes."""
    joined = rest_lines
    for idx, l in enumerate(joined):
        m = ANSWER_RE.match(l)
        if m:
            question = norm(' '.join(joined[:idx]))
            answer_lines = [m.group(1)] + joined[idx + 1:]
            return question, norm(' '.join(answer_lines))
    return norm(' '.join(joined)), None


def parse_body(lines):
    """대괄호 헤더 이후 전체를 작품(work) 단위로 나누고, 각 work 안의 번호 문항을 추출."""
    works = []
    current_work = {'title': None, 'prep_notes': [], 'items': []}
    current_item = None

    def close_item():
        nonlocal current_item
        if current_item is None:
            return
        excerpt, rest = split_excerpt(current_item['raw'])
        question, answer = split_answer(rest)
        current_item['excerpt'] = excerpt
        current_item['question_or_notes'] = question
        current_item['answer'] = answer
        del current_item['raw']
        current_work['items'].append(current_item)
        current_item = None

    for l in lines:
        if STAGE2_RE.match(l) or WRITING_TOPIC_RE.match(l):
            close_item()
            break
        if WORK_TITLE_RE.match(l):
            close_item()
            if current_work['items'] or current_work['prep_notes']:
                works.append(current_work)
            current_work = {'title': WORK_TITLE_RE.match(l).group(1), 'prep_notes': [], 'items': []}
            continue

        m_q = QNUM_RE.match(l)
        if m_q:
            close_item()
            current_item = {'question_no': m_q.group(1), 'raw': [m_q.group(2)] if m_q.group(2) else []}
            continue

        if current_item is not None:
            current_item['raw'].append(l)
        else:
            m_note = NOTE_RE.match(l)
            if m_note:
                current_work['prep_notes'].append(m_note.group(1))

    close_item()
    if current_work['items'] or current_work['prep_notes']:
        works.append(current_work)
    return works


def parse_writing_b(lines):
    theme_title = None
    steps = {}
    mode = None
    cur_step = None
    for l in lines:
        if STAGE2_RE.match(l):
            continue
        if WRITING_TOPIC_RE.match(l):
            mode = 'await_title'
            continue
        m_step = STEP_RE.match(l)
        if m_step:
            cur_step = m_step.group(1)
            steps[cur_step] = [m_step.group(2)] if m_step.group(2) else []
            mode = 'steps'
            continue
        if mode == 'await_title':
            theme_title = l
            mode = None
            continue
        if mode == 'steps' and cur_step is not None:
            steps[cur_step].append(l)
    return {
        'theme_title': theme_title,
        'steps': [{'step': k, 'text': norm(' '.join(v))} for k, v in sorted(steps.items(), key=lambda kv: int(kv[0]))],
    }


def extract(teacher_path):
    doc = fitz.open(teacher_path)
    full_text = '\n'.join(page.get_text() for page in doc)
    lines = get_lines(full_text)

    doc_title = None
    if lines and BRACKET_HEADER_RE.match(lines[0]):
        doc_title = BRACKET_HEADER_RE.match(lines[0]).group(1)
        lines = lines[1:]

    idx_stage2 = next((i for i, l in enumerate(lines) if STAGE2_RE.match(l)), len(lines))
    body_lines = lines[:idx_stage2]
    writing_lines = lines[idx_stage2:]

    works = parse_body(body_lines)
    writing = parse_writing_b(writing_lines)

    return {
        'doc_title': doc_title,
        'works': works,
        'writing_prompt': writing,
        'source': {'teacher_pdf': str(teacher_path)},
    }


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('사용법: python test_reading_essay_extract_b.py <교사용.pdf> [output.json]')
        sys.exit(1)

    teacher_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path('extract_output_b.json')

    result = extract(teacher_path)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

    n_items = sum(len(w['items']) for w in result['works'])
    n_answered = sum(1 for w in result['works'] for it in w['items'] if it['answer'])
    print(f'완료 -> {out_path}')
    print(f"  문서 제목: {result['doc_title']}")
    print(f"  작품 구간: {len(result['works'])}개")
    print(f"  문항: {n_items}건 (답: 마커 있는 문항 {n_answered}건)")
    print(f"  글쓰기 Step: {len(result['writing_prompt']['steps'])}개")
