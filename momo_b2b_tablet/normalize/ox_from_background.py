"""SPEC §4 "O·X 누락, 저학년 OX가 ox_quiz에 없고 배경 텍스트에" 처리.

ox_quiz 테이블이 비어 있을 때만 documents.background_text에서 "O.X 퀴즈"
구간을 파싱해 대체한다. 실제 데이터(L2-Q2-W08)로 확인: PDF에서 뽑은 텍스트가
2단(문장/체크박스) 레이아웃을 한 줄씩 풀어써서, 문장 중간에 "○   X" 체크박스
줄이 끼어 있고 문장 자체도 줄바꿈으로 쪼개져 있다:

    은비네 가족들은 고양이를 무척 좋아해서 은비는 쉽게 고양이를 키울 수
                                       ○   X
    있었어요. (13페이지)

체크박스 줄을 제거하고 남은 줄을 공백으로 이어붙인 뒤, 끝의 "(N페이지)"를
페이지 번호로 뽑아낸다. 이렇게 복원한 문장은 골든 샘플(L2-Q2-W08.layout.json)
oxp 페이지의 5개 문장과 정확히 일치함을 확인했다."""
from __future__ import annotations

import re

_SECTION_RE = re.compile(r"O\s*[.·]?\s*X\s*퀴즈")
# 체크박스만 있는 줄만 지운다(빈 줄까지 지우면 문단 구분자가 없어져 전부 한
# 덩어리로 붙어버린다 - 실제로 이 버그를 만들고 고쳤다). 그래서 ○/X 문자가
# 최소 하나는 있어야 매칭되게 한다.
_CHECKBOX_LINE_RE = re.compile(r"^[\sOoXx0-9]*[○][\sOoXx0-9]*$")
_TRAILING_PAGE_RE = re.compile(r"\(\s*(\d+)\s*페이지\s*\)")
_PAGE_FOOTER_RE = re.compile(r"-\s*\d+\s*-\s*$")


def extract_ox_from_background(background_text: str | None) -> list[tuple[str, int | None]]:
    """[(문장, 쪽번호|None), ...] 반환. 구간을 못 찾으면 빈 리스트."""
    if not background_text:
        return []
    m = _SECTION_RE.search(background_text)
    if not m:
        return []
    tail = background_text[m.end():]

    lines = tail.split("\n")
    kept_lines = [ln for ln in lines if not _CHECKBOX_LINE_RE.match(ln)]
    text = "\n".join(kept_lines)

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", text)]
    out: list[tuple[str, int | None]] = []
    for p in paragraphs:
        if not p:
            continue
        joined = " ".join(line.strip() for line in p.split("\n") if line.strip())
        joined = _PAGE_FOOTER_RE.sub("", joined).strip()  # 쪽 하단 "- 3 -" 같은 인쇄 쪽번호 제거
        joined = re.sub(r"\s+", " ", joined).strip()
        pm = _TRAILING_PAGE_RE.search(joined)
        page = int(pm.group(1)) if pm else None
        # 인용 페이지 표시(그리고 그 뒤에 붙어 넘어온 다음 문단 쓰레기)는 전부 버린다 -
        # 문장은 그 인용 앞부분만 쓴다.
        sentence = joined[: pm.start()].strip() if pm else joined
        if sentence:
            out.append((sentence, page))
    return out
