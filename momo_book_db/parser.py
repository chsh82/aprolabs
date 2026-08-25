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
TAG_LINE_RE = re.compile(r'^([가-힣]+적(?:\s*/\s*[가-힣]+적)*)\s*독해\s*$')
PAGE_REF_RE = re.compile(r'^-p\.?\s*([\d~\-]+)')
PAGE_FOOTER_RE = re.compile(r'^-\s*\d+\s*-$')  # 페이지 하단 쪽번호("- 7 -") - 본문 아님
QNUM_RE = re.compile(r'^(\d+(?:-\d+)?)\.\s*(.*)')
STAGE1_RE = re.compile(r'1\s*단계')
STAGE2_RE = re.compile(r'2\s*단계')
STAGE3_RE = re.compile(r'3\s*단계')


def normalize_reading_type(raw: str) -> str:
    """'적용적 / 비판적 독해' 등을 사실적<추론적<비판적<분석적<적용적 순으로 정규화"""
    parts = [p.strip() for p in raw.split('/') if p.strip()]
    parts = sorted(set(parts), key=lambda p: READING_TYPE_ORDER.index(p) if p in READING_TYPE_ORDER else 99)
    return '/'.join(parts)


def load_pages(pdf_path: str):
    """각 페이지의 (PyMuPDF 텍스트, pdfplumber page 객체) 리스트"""
    fitz_doc = fitz.open(pdf_path)
    plumber_doc = pdfplumber.open(pdf_path)
    pages = []
    for i in range(len(fitz_doc)):
        pages.append({
            'no': i + 1,
            'text': fitz_doc[i].get_text(),
            'plumber': plumber_doc.pages[i],
        })
    return pages


def find_stage_boundaries(pages):
    """1단계/2단계/3단계가 시작하는 페이지 번호(1-based) 찾기"""
    stage1 = stage2 = stage3 = None
    for p in pages:
        if stage1 is None and STAGE1_RE.search(p['text']):
            stage1 = p['no']
        if stage2 is None and STAGE2_RE.search(p['text']):
            stage2 = p['no']
        if stage3 is None and STAGE3_RE.search(p['text']):
            stage3 = p['no']
    return stage1, stage2, stage3


def parse_vocabulary_and_ox(page, log):
    """1단계 페이지의 표 2개(어휘, OX)를 파싱"""
    tables = page['plumber'].extract_tables()
    vocab, ox = [], []

    if len(tables) >= 1:
        rows = tables[0]
        order = 0
        current = None
        for row in rows:
            col0 = (row[0] or '').strip()
            col1 = (row[1] or '').strip()
            if col0 and col0 not in ('단어',):
                # 새 단어 시작: "단어\n(NN페이지)"
                m = re.match(r'(.+?)\s*\((\d+)\s*페이지\)', col0.replace('\n', ' '))
                order += 1
                current = {
                    'order_no': order,
                    'word': m.group(1).strip() if m else col0,
                    'book_page': int(m.group(2)) if m else None,
                    'definition': None,
                    'example_sentence': None,
                    'source_page': page['no'],
                    'raw_text': ' | '.join(c for c in row if c) or None,
                }
                vocab.append(current)
            if current and col1 == '뜻':
                current['definition'] = (row[2] or '').strip() or None
                current['raw_text'] = (current['raw_text'] or '') + ' / ' + ' | '.join(c for c in row if c)
            if current and col1 == '문장':
                current['example_sentence'] = (row[2] or '').strip() or None
                current['raw_text'] = (current['raw_text'] or '') + ' / ' + ' | '.join(c for c in row if c)
    else:
        log.append({'level': 'error', 'stage': 'vocabulary', 'message': f'{page["no"]}페이지에서 표를 찾지 못함'})

    if len(tables) >= 2:
        rows = tables[1]
        for i, row in enumerate(rows, start=1):
            statement_raw = (row[0] or '').strip().replace('\n', ' ')
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

    return vocab, ox


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
        tag_match = TAG_LINE_RE.match(line)
        if tag_match:
            current_tag = normalize_reading_type(tag_match.group(1))
            i += 1
            continue

        qnum_match = QNUM_RE.match(line)
        if qnum_match:
            order_label = qnum_match.group(1)
            order_no_val = int(order_label.split('-')[0])
            # 발췌문: 이 문항 앞, page-ref 줄까지 역방향 수집 (직전 태그줄 다음부터)
            # 발췌문/발췌 페이지는 문항 앞의 "-p.NN" 줄을 찾아 역으로 모음
            excerpt_lines = []
            j = i - 1
            excerpt_page = None
            while j >= 0:
                prev = lines[j].strip()
                pref = PAGE_REF_RE.match(prev)
                if pref:
                    excerpt_page = int(re.findall(r'\d+', pref.group(1))[0])
                    j -= 1
                    continue
                if TAG_LINE_RE.match(prev) or QNUM_RE.match(prev) or not prev:
                    break
                excerpt_lines.insert(0, prev)
                j -= 1
            excerpt_text = ' '.join(excerpt_lines).strip() or None

            # 질문 본문 + 답변란: 다음 태그줄/문항줄/페이지 끝까지 모음
            body_lines = [qnum_match.group(2)]
            k = i + 1
            while k < n:
                nxt = lines[k].strip()
                if TAG_LINE_RE.match(nxt) or QNUM_RE.match(nxt) or PAGE_REF_RE.match(nxt):
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
            i = k
            continue
        i += 1

    return items


def parse_essay(pages, stage3_start, log):
    text = ''
    page_no = stage3_start
    for p in pages:
        if p['no'] >= stage3_start:
            text = p['text']
            page_no = p['no']
            break

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    # "Step 1." 이전 줄들 중 마지막 한두 줄을 main_topic으로 추정
    step1_idx = next((idx for idx, l in enumerate(lines) if l.startswith('Step 1')), None)
    main_topic = None
    if step1_idx:
        candidates = [l for l in lines[:step1_idx] if not re.match(r'^\d+\s*단계$|^글쓰기$|^\s*내\s*글로\s*엮기\s*$', l)]
        main_topic = candidates[-1] if candidates else None

    outline = []
    if step1_idx is not None:
        step2_idx = next((idx for idx, l in enumerate(lines) if l.startswith('Step 2')), len(lines))
        order = 0
        current = None
        for l in lines[step1_idx + 1:step2_idx]:
            m = re.match(r'^(\d+)\.\s*(.*)', l)
            if m:
                order += 1
                role = ['intro', 'body', 'conclusion'][min(order - 1, 2)]
                current = {'order_no': order, 'question_text': m.group(2), 'role': role}
                outline.append(current)
            elif current:
                current['question_text'] = (current['question_text'] + ' ' + l).strip()

    if not main_topic:
        log.append({'level': 'warning', 'stage': 'essay_prompt', 'message': 'main_topic 추정 실패'})

    return {
        'main_topic': main_topic,
        'writing_format': 'text_long',
        'min_length': None,
        'source_page': page_no,
        'outline_questions': outline,
        'raw_text': text or None,
    }


def parse_pdf(pdf_path: str, use_llm: bool = False) -> dict:
    log = []
    pages = load_pages(pdf_path)
    stage1, stage2, stage3 = find_stage_boundaries(pages)
    if not (stage1 and stage2 and stage3):
        log.append({'level': 'error', 'stage': 'stage_split', 'message': f'단계 구분 실패: 1단계={stage1}, 2단계={stage2}, 3단계={stage3}'})

    vocab, ox = ([], [])
    for p in pages:
        if p['no'] == stage1:
            vocab, ox = parse_vocabulary_and_ox(p, log)
            break

    discussion = parse_discussion(pages, stage2, stage3, log, use_llm=use_llm) if stage2 and stage3 else []
    essay = parse_essay(pages, stage3, log) if stage3 else {}

    return {
        'vocabulary': vocab,
        'ox_quiz': ox,
        'discussion_qa': discussion,
        'essay_prompt': essay,
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
