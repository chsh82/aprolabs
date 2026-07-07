# -*- coding: utf-8 -*-
"""교사용 PDF 1개를 보고 템플릿 계열(A1/A2/B/C)을 판별.

판별 순서 (경험적으로 검증된 순서 - 뒤에 오는 조건이 앞선 조건보다
느슨해서 먼저 걸러내지 않으면 오분류됨):
  1. 표지 첫 줄이 "[...]" 대괄호 헤더 -> B
     (A1/A2/C는 전부 "- 1 -\nLV N..."으로 시작, B만 대괄호로 시작)
  2. "번째 이야기." 또는 "소설 구성의/소설의 3요소" 또는
     "N. [독해유형]" 결합줄 -> C
     (C는 철학·사회 비문학 단일도서 변형도 있어 이야기헤더가 없을 수 있음)
  3. "1단계"라는 문자열이 있으면 -> A1
     (A2는 "1단계" 대신 "1-1"/"1-2" 하위 스테이지 번호를 씀)
  4. 그 외 -> A2
  5. 위 어느 것도 애매하면 None (수동 확인 필요)
"""
import re

import fitz

_BRACKET_HEADER_RE = re.compile(r'^\[.+\]')
_WORK_HEADER_RE = re.compile(r'번째\s*이야기\.')
_THEORY_TABLE_RE = re.compile(r'소설\s*구성의|소설의\s*3요소')
_BRACKET_TYPE_RE = re.compile(r'^\d+\.\s*\[[가-힣]+(?:\s*/\s*[가-힣]+)*\s*독해\]$', re.MULTILINE)
_STAGE1_RE = re.compile(r'1\s*단계')


def classify_text(text):
    """텍스트만으로 계열 판별. 반환: 'A1'/'A2'/'B'/'C'/None"""
    first_line = next((l.strip() for l in text.split('\n') if l.strip()), '')
    if _BRACKET_HEADER_RE.match(first_line):
        return 'B'
    if _WORK_HEADER_RE.search(text) or _THEORY_TABLE_RE.search(text) or _BRACKET_TYPE_RE.search(text):
        return 'C'
    if _STAGE1_RE.search(text):
        return 'A1'
    if '질문과' in text and '토론' in text:
        return 'A2'
    return None


def classify_family(pdf_path):
    """교사용 PDF 경로를 받아 계열을 판별. 텍스트가 거의 없으면(스캔 이미지 등) None."""
    doc = fitz.open(pdf_path)
    # A2는 "질문과 토론" 헤더가 4페이지 근처에 나오는 경우가 있어 전체를 훑는다
    # (교재가 보통 6~11페이지라 비용은 낮음).
    text = '\n'.join(page.get_text() for page in doc)
    doc.close()
    if len(text.strip()) < 20:
        return None
    return classify_text(text)
