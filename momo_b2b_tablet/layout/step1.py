"""STEP1 페이지 - SPEC §3.4 "학년대별 STEP1 구성".

저학년: 낱말 익히기 -> 생각상자(그림 칸) -> O·X 퀴즈
고학년(elem-upper): 낱말 익히기(1쪽) -> O·X 퀴즈(1쪽, 남는 자리 이미지)
중학생(mid): 배경지식 - documents.background_text에서 뽑는다(사용자 지시
2026-09-23 "먼저: Stage 3에 배경지식 추출 반영" - momo_book.db를 직접 훑어
중등 75건 중 상당수가 이 필드에 실제 원문을 갖고 있음을 확인했다. 처음엔 이걸
못 보고 통째로 placeholder 처리했었다). 화살표(->) 연표는 bgline.rows로 분해,
그 외(프로즈/O·X/표 등 - 표는 실제로 규칙 탐지를 시도했지만 신뢰할 수 없어 뺐다.
layout/background.py 참고)는 bgtext(본문+이미지) 페이지로 만들고 구조 플래그를
남긴다. LLM 분류는 넣지 않는다.
"""
from __future__ import annotations

from normalize.models import Flag, NormalizedDoc

from .background import (
    TIMELINE_ROW_SIZE,
    classify_background,
    extract_paragraphs,
    extract_term_note,
    extract_title,
    paginate_paragraphs,
    parse_timeline,
)
from .rules import LOWER_VOCAB_CHARACTER, VOCAB_CHARACTER, WRITING_CHARACTER

_CHAR_NAME = {
    "holmes": "셜록 홈즈", "aronnax": "아로낙스 박사", "jekyll": "지킬 박사",
    "fogg": "필리어스 포그", "anne": "앤", "dorothy": "도로시",
}


def _vocab_page(doc: NormalizedDoc) -> tuple[dict, list[Flag]]:
    character = LOWER_VOCAB_CHARACTER if doc.band == "lower" else VOCAB_CHARACTER
    items = []
    for v in doc.vocab:
        item = {"w": v.word, "d": v.definition or "", "p": v.book_page}
        if any(f.kind == "sup" for f in v.flags):
            item["sup"] = True
        items.append(item)
    page = {
        "type": "vocab", "step": "STEP 1", "title": "낱말 익히기",
        "guide": {"img": character, "rt": "어휘", "nm": f"{_CHAR_NAME[character]}와 단서 모으기"},
        "vocab": items,
    }
    return page, []


def _oxp_page(doc: NormalizedDoc, with_slot: bool) -> tuple[dict, list[Flag]]:
    flags = []
    ox_items = [{"s": o.question, "p": o.evidence_page} for o in doc.ox]
    page = {
        "type": "oxp", "step": "STEP 1", "title": "내용 확인하기",
        "guide": {"img": VOCAB_CHARACTER, "rt": "사실적 독해", "nm": f"{_CHAR_NAME[VOCAB_CHARACTER]}와 근거 찾기"},
        "ox": ox_items,
    }
    if with_slot:
        page["slot"] = {"scene": "(LLM 생성 필요)", "avoid": "O·X 문항의 정답을 암시하는 장면을 넣지 않는다."}
        flags.append(Flag(kind="derived", category="placeholder",
                           message="STEP1 O·X 페이지의 이미지 생성 지시문은 LLM이 채워야 함(자리표시자)"))
    return page, flags


def _draw_page(doc: NormalizedDoc) -> tuple[dict, list[Flag]]:
    page = {
        "type": "draw", "step": "STEP 1", "title": "생각 상자",
        "guide": {"img": WRITING_CHARACTER, "rt": "생각 표현하기", "nm": f"{_CHAR_NAME[WRITING_CHARACTER]}와 생각 나누기"},
        "id": "S1-draw",
        "inst": f"『{doc.book_title}』을 읽으며 떠올랐던 생각이나 기억에 남는 장면을 그려 보세요.",
    }
    return page, []


def _background_image(doc: NormalizedDoc) -> str | None:
    bg_img = next((img.file_path for img in doc.images if img.image_type == "background"), None)
    if not bg_img:
        bg_img = next((img.file_path for img in doc.images if img.image_type == "cover"), None)
    return bg_img


def _bgline_from_timeline(doc: NormalizedDoc) -> tuple[dict, list[Flag]]:
    text = doc.background_text
    rows = parse_timeline(text)
    term = extract_term_note(text) or {"title": "", "text": ""}
    bg_img = _background_image(doc)
    caption = (term["title"].lstrip("※ ").strip() if term.get("title") else extract_title(text))
    page = {
        "type": "bgline", "step": "STEP 1", "title": extract_title(text),
        "guide": {"rt": "배경지식"},
        "inst": f"『{doc.book_title}』을 읽기 전, 흐름을 따라가며 배경지식을 익혀 봅시다.",
        "rows": rows,
        "image": {"key": bg_img or "", "caption": caption},
        "term": term,
    }
    flags = [Flag(kind="split", category="structure",
                   message=f"배경지식 연표: 노드 {sum(len(r) for r in rows)}개를 {len(rows)}줄로 "
                           f"임시 배분함(줄당 {TIMELINE_ROW_SIZE}개씩, 원본 PDF 줄바꿈은 의미가 없어 "
                           f"무시함) - 강조 노드(hl/end)는 그 차시 주제 노드와 마지막(책의 출발점) "
                           f"노드를 검수에서 지정해야 함(SPEC §3.4)")]
    if not bg_img:
        flags.append(Flag(kind="derived", category="placeholder",
                           message="배경지식 연표에 붙일 원본 이미지가 없음"))
    return page, flags


def _bgtext_pages(doc: NormalizedDoc) -> tuple[list[dict], list[Flag]]:
    text = doc.background_text
    kind = classify_background(text)
    title = extract_title(text)
    chunks = paginate_paragraphs(extract_paragraphs(text))
    bg_img = _background_image(doc)
    slot = {"img": bg_img, "src": "원본 배경지식 이미지"} if bg_img else \
        {"scene": "(LLM 생성 필요)", "avoid": "본문 내용과 어긋나는 장면을 넣지 않는다."}

    pages = []
    for i, chunk in enumerate(chunks):
        pages.append({
            "type": "bgtext", "step": "STEP 1", "title": title if i == 0 else f"{title} (계속)",
            "guide": {"rt": "배경지식"},
            "paragraphs": chunk,
            "slot": dict(slot),
        })

    note = "O·X 형식으로 보임 - 검수에서 상호작용형(oxp 등) 위젯으로 바꿀지 판단. " if kind == "ox" else ""
    flags = [Flag(kind="derived", category="structure",
                   message=f"{note}배경지식을 bgtext(본문+이미지)로 임시 배치함({len(pages)}쪽) - "
                           f"이 페이지 유형·분량이 적절한지, 더 나은 위젯(표 등)이 있는지 검수 필요")]
    if not bg_img:
        flags.append(Flag(kind="derived", category="placeholder",
                           message="배경지식에 붙일 원본 이미지가 없어 생성 슬롯이 필요함"))
    return pages, flags


def _bgline_stub(doc: NormalizedDoc) -> tuple[dict, list[Flag]]:
    """background_text 자체가 비어 있는 극소수 중등 문서용 - 정말 원문이 없을 때만."""
    page = {
        "type": "bgline", "step": "STEP 1", "title": "배경지식",
        "guide": {"rt": "배경지식"}, "inst": "", "rows": [],
        "image": {"key": "", "caption": ""}, "term": {"title": "", "text": ""},
    }
    flags = [Flag(kind="missing", category="placeholder",
                   message="documents.background_text가 비어 있어 배경지식 원문이 없음 - "
                            "LLM/편집자가 이 책에 맞춰 새로 작성해야 함(SPEC §3.4)")]
    return page, flags


def build_step1_pages(doc: NormalizedDoc) -> tuple[list[dict], list[Flag]]:
    if doc.band == "mid":
        kind = classify_background(doc.background_text)
        if kind == "empty":
            page, flags = _bgline_stub(doc)
            return [page], flags
        if kind == "timeline":
            page, flags = _bgline_from_timeline(doc)
            return [page], flags
        pages, flags = _bgtext_pages(doc)
        return pages, flags

    pages: list[dict] = []
    flags: list[Flag] = []
    vp, vf = _vocab_page(doc)
    pages.append(vp); flags.extend(vf)

    if doc.band == "lower":
        dp, df = _draw_page(doc)
        pages.append(dp); flags.extend(df)
        op, of = _oxp_page(doc, with_slot=False)
    else:  # elem-upper
        op, of = _oxp_page(doc, with_slot=True)
    pages.append(op); flags.extend(of)

    return pages, flags
