"""중등(mid) STEP1 배경지식 추출 - documents.background_text에서.

사용자 지시(2026-09-23, "먼저: Stage 3에 배경지식 추출 반영"): momo_book.db를
직접 훑어 중등 75건 중 상당수가 background_text에 실제 배경지식 원문을 갖고
있음을 확인했다(review_status는 전부 'pending'만 들어있어 사용자가 본 건
컬럼이 밀린 다른 샘플 DB였음 - 원본 momo_book.db는 이 필드 밀림이 없다는
①단계 조사 결과와 일치한다).

규칙으로 확실히 구분되는 것만 분류한다(화살표 연표, O·X 패턴) - LLM 분류는
넣지 않는다(사용자 지시 3번). 그 외(프로즈, 그리고 실제로 시도해봤지만
신뢰할 수 없었던 "표" 패턴 - 아래 표 참고)는 전부 bgtext(본문+이미지)로 두고
구조 플래그(category=structure)만 남긴다.

"표 기호" 패턴에 대한 기록: 정규식으로(2칸 이상 공백으로 갈라진 열이 2줄 이상)
탐지를 시도했으나 실제 데이터에서 오탐(순수 프로즈 문단에 "* 항목:" 나열이
있으면 표로 잘못 잡힘)과 누락(진짜 표는 PDF 추출 과정에서 셀 내용이 여러 줄로
쪼개져 흩어져 있어 규칙이 못 잡음)이 둘 다 나서 제외했다 - 결과적으로 표 형태도
prose로 분류돼 bgtext + structure 플래그로 넘어간다."""
from __future__ import annotations

import re

_ARROW_RE = re.compile(r"->")
_OX_MARK_RE = re.compile(r"(?:^|\n)\s*\d+\)\s")
_ANSWER_RE = re.compile(r"(?:^|\n)\s*답\s*[:：]")
_HEADER_RE = re.compile(r"^\s*[\[【]?\s*1\s*단계\s*[\]】]?\s*[.:：]?\s*")
_PAGE_FOOTER_RE = re.compile(r"^\s*-\s*\d+\s*-\s*$", re.M)
_TERM_RE = re.compile(r"※\s*([^:：\n]+)[:：]\s*(.+)", re.S)
_TRAILING_PAREN_RE = re.compile(r"\((.*)\)\s*$")  # 끝에 걸리는 괄호는 중첩(예: "(효종(이름)의 ...)")도 있어 non-greedy 문자클래스로는 못 잡는다
_YEAR_PAREN_RE = re.compile(r"\s*\((\d{4})\)\s*")

BGTEXT_MAX_CHARS_PER_PAGE = 600
TIMELINE_ROW_SIZE = 6


def classify_background(text: str | None) -> str:
    """timeline(화살표 연표) | ox(번호+답: 패턴) | prose(그 외, 표 포함) | empty."""
    if not text or not text.strip():
        return "empty"
    if _ARROW_RE.search(text):
        return "timeline"
    if len(_OX_MARK_RE.findall(text)) >= 2 and _ANSWER_RE.search(text):
        return "ox"
    return "prose"


def extract_title(text: str) -> str:
    first_line = next((ln.strip() for ln in text.split("\n") if ln.strip()), "")
    title = _HEADER_RE.sub("", first_line).strip()
    return title or "배경지식"


def _rejoin(chunk_lines: list[str]) -> str:
    return " ".join(ln.strip() for ln in chunk_lines if ln.strip())


def extract_paragraphs(text: str) -> list[str]:
    """첫 줄(제목)과 쪽 번호 각주("- 2 -")를 걷어내고, PDF 줄바꿈으로 쪼개진
    문장을 다시 이어 붙여 단락 목록을 낸다(빈 줄 2개 이상을 단락 경계로 본다)."""
    body = "\n".join(text.split("\n")[1:])
    body = _PAGE_FOOTER_RE.sub("", body)
    chunks = re.split(r"\n\s*\n+", body)
    paragraphs = []
    for chunk in chunks:
        text_line = _rejoin(chunk.split("\n"))
        if text_line and not text_line.startswith("※"):
            paragraphs.append(text_line)
    return paragraphs


def paginate_paragraphs(paragraphs: list[str], max_chars: int = BGTEXT_MAX_CHARS_PER_PAGE) -> list[list[str]]:
    """본문이 길면 페이지를 나눈다(STEP2 제시문 배치 규칙과 같은 방식 - 한 페이지
    분량을 넘으면 다음 페이지로)."""
    pages: list[list[str]] = []
    current: list[str] = []
    current_len = 0
    for p in paragraphs:
        if current and current_len + len(p) > max_chars:
            pages.append(current)
            current, current_len = [], 0
        current.append(p)
        current_len += len(p)
    if current:
        pages.append(current)
    return pages or [[]]


def extract_term_note(text: str) -> dict | None:
    """"※용어: 설명" 형태의 각주 문단(L9-Q3-W07의 "※삼전도의 굴욕: ..."처럼)."""
    m = _TERM_RE.search(text)
    if not m:
        return None
    title = m.group(1).strip()
    body_raw = _PAGE_FOOTER_RE.sub("", m.group(2))  # 쪽 번호 각주("- 2 -")는 줄 단위일 때 걷어내야 한다
    body = _rejoin(body_raw.split("\n"))
    return {"title": f"※ {title}", "text": body} if body else None


def parse_timeline(text: str) -> list[list[dict]]:
    """화살표(->)로 이어진 연표를 표시용 줄(rows)로 분해한다.

    원본 PDF의 줄바꿈은 페이지 폭에 맞춘 자리일 뿐 의미가 없다(실제로 확인함 -
    L9-Q3-W07 원문은 7줄로 꺾여 있지만 그 경계가 의미 단위와 무관하다). 그래서
    화살표 체인 전체(마지막 "->" 뒤, 화살표가 없는 마지막 노드 한 줄까지 포함)를
    한 번에 이어 붙인 뒤 TIMELINE_ROW_SIZE개씩 다시 나눈다. 연도(4자리 숫자)는
    괄호가 어디 있든(끝이든 중간이든) 찾아서 y로 빼내고, 남는 끝자리 괄호는 nt로
    분리한다. 강조 노드(hl/end)는 여기서 정하지 않는다 - 검수 단계 몫(사용자 지시)."""
    start = text.find("->")
    if start == -1:
        return []
    line_start = text.rfind("\n", 0, start) + 1
    term_start = text.find("※")
    end = term_start if term_start != -1 else len(text)
    chain_block = _PAGE_FOOTER_RE.sub("", text[line_start:end])
    flat = _rejoin(chain_block.split("\n"))
    nodes_raw = [n.strip() for n in flat.split("->") if n.strip()]

    nodes = []
    for n in nodes_raw:
        node: dict = {}
        year_m = _YEAR_PAREN_RE.search(n)
        if year_m:
            node["y"] = year_m.group(1)
            n = re.sub(r"\s{2,}", " ", (n[:year_m.start()] + " " + n[year_m.end():])).strip()
        trailing_m = _TRAILING_PAREN_RE.search(n)
        if trailing_m:
            node["nt"] = trailing_m.group(1).strip()
            n = n[:trailing_m.start()].strip()
        node["e"] = n
        nodes.append(node)

    return [nodes[i:i + TIMELINE_ROW_SIZE] for i in range(0, len(nodes), TIMELINE_ROW_SIZE)]
