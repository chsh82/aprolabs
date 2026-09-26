"""STEP2 페이지 - 문항(NormalizedQA) 하나당 페이지 하나(SPEC §3.4 "한 페이지에
소문항 하나"). QA 순서는 정규화 결과(doc.qa) 그대로 쓴다 - "이 문항은 학생용
시안에서 뺀다" 같은 편집 판단은 여기서 하지 않는다(그건 ③ 검수 단계의 일이지,
초안 생성기가 추측할 근거가 없다). 그래서 골든 샘플(사람이 고른 소수 문항만
실은 시안)보다 이 함수가 만드는 페이지 수가 더 많을 수 있다 - 이건 버그가
아니라 "자동화는 초안까지"(SPEC §1) 원칙에 따른 의도된 차이다.
"""
from __future__ import annotations

from normalize.models import Flag, NormalizedDoc, NormalizedQA

from .rules import (
    LONG_QUESTION_MIN,
    UNSUPPORTED_UI_TYPES,
    character_for_reading_type,
    excerpt_page_type,
    form_for_ui_type,
    reading_type_label,
)

_STEP2_NM = {
    "holmes": "셜록 홈즈와 근거 찾기",
    "aronnax": "아로낙스 박사와 나눠 보기",
    "jekyll": "지킬 박사와 숨은 뜻 찾기",
    "fogg": "필리어스 포그와 적용해 보기",
}
_WIDE_FORMS = {"table", "compare", "choice", "choiceList"}
_LEAKY_FORMS = {"choice", "choiceList"}  # 선택지 자체가 답이라 이미지 지시문에서 답 유출을 따로 조심해야 함
# 2026-09-26: document_image.image_type이 "illustration"만 있는 게 아니다(실측:
# reference 5건, excerpt 2건 - 전부 source_page가 있는 진짜 참고 이미지/원문
# 이미지였다. 두근두근 한국사 조선총독부·삼전도비 사진이 image_type='reference'로
# 들어 있어 기존엔 "illustration"만 찾아 통째로 누락됐었다). 표지/배경은 여전히
# 제외한다(이미 cover/background 페이지가 따로 쓴다).
_ORIGINAL_IMAGE_TYPES = {"illustration", "reference", "excerpt"}

# 제시문 분리(2026-09-26 사용자 지시 [3순위]) - 실제 페이지 조판이 아니라
# 글자수 기준 경험적 규칙이다(렌더러가 고정 크기 슬라이드라 텍스트가 흐르지
# 않으므로, 실제 폰트/여백으로 넘치는지는 인쇄 미리보기에서 검수자가 최종
# 확인해야 한다 - 젊은 예술가의 초상 4쪽처럼 1,000자가 넘는 전기적 배경
# 설명이 한 페이지에 안 들어가는 사례가 실측 근거). 문단(줄바꿈) 경계로만
# 나누고 문장 중간을 자르지 않는다.
_EXCERPT_SPLIT_THRESHOLD = 500
_EXCERPT_PAGE_BUDGET = 500


def _split_excerpt_into_pages(excerpt_text: str, budget: int) -> list[str]:
    """제시문을 문단 단위로 budget자 이내씩 묶어 여러 페이지 분량으로 나눈다.
    문단 하나가 budget보다 길면 그 문단만으로 페이지 하나를 채운다(문장을
    더 쪼개는 것보다 문단을 지키는 쪽이 자연스럽다)."""
    paragraphs = [p for p in excerpt_text.split("\n") if p.strip()]
    if not paragraphs:
        return [excerpt_text]
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for p in paragraphs:
        if cur and cur_len + len(p) > budget:
            chunks.append("\n".join(cur))
            cur, cur_len = [], 0
        cur.append(p)
        cur_len += len(p)
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def _ref_bearer_order_label(doc: NormalizedDoc) -> str | None:
    """한자 참고표(ref)를 붙일 문항을 고른다. 골든(L9-Q3-W07)에서 확인한 패턴:
    문서에 hanja_glossary가 있으면 제시문이 실제로 분리된(=excerpt_text가 채워진)
    첫 문항에 붙어 있었다. n=1 사례에서 역산한 임시 휴리스틱이라 flag를 단다."""
    if not doc.hanja_glossary:
        return None
    for qa in doc.qa:
        if qa.excerpt_text:
            return qa.order_label
    return None


def _hanja_ref_table(doc: NormalizedDoc) -> dict:
    rows: list[dict] = []
    for g in doc.hanja_glossary:
        rows.append({"n": str(g.order_no), "v": g.term})
        if g.gap_after:
            rows.append({"gap": True})
    return {"title": "", "rows": rows}


def _guide_for(doc: NormalizedDoc, qa: NormalizedQA) -> tuple[dict, list[Flag]]:
    flags: list[Flag] = []
    rt_label, rt_ok = reading_type_label(qa.reading_type)
    if not rt_ok:
        flags.append(Flag(order_no=qa.order_no, kind="derived",
                           message=f"문항 {qa.order_label}: 독해유형(reading_type)이 없어 "
                                   f"라벨을 채우지 못함 - LLM/검수로 실제 독해유형 확인 필요"))
    if doc.band == "mid":
        # SPEC §3.6: 중학생은 캐릭터 없음(라벨만) - guide.img를 아예 넣지 않는다.
        return {"rt": rt_label}, flags

    character, char_ok = character_for_reading_type(qa.reading_type)
    if not char_ok:
        flags.append(Flag(order_no=qa.order_no, kind="derived",
                           message=f"문항 {qa.order_label}: 독해유형 -> 캐릭터 매핑이 "
                                   f"불확실해 기본값({character})을 씀 - 검수 확인 필요"))
    guide = {"img": character, "rt": rt_label, "nm": _STEP2_NM.get(character, "")}
    return guide, flags


def _original_image_for_qa(doc: NormalizedDoc, qa: NormalizedQA, claimed: set[str] = frozenset()) -> str | None:
    """qa.source_page와 같은 쪽의 원본 삽화 파일 경로(없으면 None) - wide 여부를
    정하기 전에 먼저 확인해야 한다(바로 아래 주석 참고). claimed에 있는 파일은
    이미 qaref(참고표)가 전용으로 쓴 것이라 건너뛴다(아래 _slot_for_qa 참고)."""
    for img in doc.images:
        if img.image_type in _ORIGINAL_IMAGE_TYPES and img.source_page and img.source_page == qa.source_page \
                and img.file_path not in claimed:
            return img.file_path
    return None


def _all_images_for_qa(doc: NormalizedDoc, qa: NormalizedQA, claimed: set[str] = frozenset()) -> list[str]:
    """qa.source_page와 같은 쪽의 원본 이미지 전부(claimed 제외) - compare
    카드별 이미지 배정용(2026-09-26 두근두근 한국사 "사라진 조선총독부"/
    "남아있는 삼전도비" 검수에서 발견: 같은 쪽에 이미지가 2장 있는데 기존엔
    첫 번째 것만 골라 나머지가 그냥 버려지고 있었다)."""
    return [img.file_path for img in doc.images
            if img.image_type in _ORIGINAL_IMAGE_TYPES and img.source_page and img.source_page == qa.source_page
            and img.file_path not in claimed]


def _slot_for_qa(doc: NormalizedDoc, qa: NormalizedQA, form: str | None = None,
                  claimed: set[str] = frozenset()) -> tuple[dict, Flag | None]:
    """SPEC §3.5.1 "원본 교재 이미지 우선": 문항의 source_page와 같은 쪽의 원본
    삽화가 있으면 그걸 슬롯에 쓴다. 없으면 생성 지시문 자리표시자를 flag와 함께 둔다.

    choice/choiceList는 선택지 자체가 답이라(사용자 지시 2026-09-23) 일반적인
    "답을 암시하지 않는다"보다 더 구체적으로 "선택지 중 정답 암시 금지"를 넣는다.

    claimed(2026-09-26 열하일기 검수에서 발견): qaref(한자 참고표 등)가 이미
    쓴 이미지는 같은 source_page의 다른(관련 없는) 문항에 또 붙이지 않는다 -
    한자 자원 변천 그림이 옆의 무관한 토론 문항에도 재사용돼 "이미지가 엉뚱한
    쪽에 잘못 들어갔다"는 인상을 줬다. qaband끼리 같은 이미지를 공유하는 기존
    동작(야옹아 등, 특별한 참고표가 아닌 경우)은 그대로 둔다."""
    for img in doc.images:
        if img.image_type in _ORIGINAL_IMAGE_TYPES and img.source_page and img.source_page == qa.source_page \
                and img.file_path not in claimed:
            return {"img": img.file_path, "src": f"원본 {img.source_page}쪽"}, None
    flag = Flag(order_no=qa.order_no, kind="derived", category="placeholder",
                message=f"문항 {qa.order_label}: 원본 삽화가 없어 생성 이미지 지시문이 필요함 - "
                        f"장면(scene)·금지요소(avoid)는 LLM이 채워야 함(현재 자리표시자)")
    avoid = ("선택지 중 어느 것이 정답인지 암시하는 장면을 넣지 않는다(정답을 알려주는 단서 금지).")
    if form not in _LEAKY_FORMS:
        avoid = "질문이 요구하는 답(감정·이유·결과)을 암시하지 않는다."
    return {"scene": "(LLM 생성 필요)", "avoid": avoid}, flag


def build_step2_pages(doc: NormalizedDoc) -> tuple[list[dict], list[Flag]]:
    pages: list[dict] = []
    flags: list[Flag] = []
    claimed_images: set[str] = set()  # qaref가 전용으로 쓴 이미지(아래 참고)

    ref_bearer = _ref_bearer_order_label(doc)
    if ref_bearer:
        flags.append(Flag(kind="derived",
                           message=f"한자 참고표를 문항 {ref_bearer}에 붙이는 규칙은 임시 휴리스틱"
                                   f"(문서 내 제시문이 분리된 첫 문항) - 검수에서 확인 필요"))

    for qa in doc.qa:
        if qa.ui_type in UNSUPPORTED_UI_TYPES:
            flags.append(Flag(order_no=qa.order_no, kind="derived", category="widget_unavailable",
                               message=f"문항 {qa.order_label}: ui_type={qa.ui_type!r}는 렌더러에 "
                                       f"대응 위젯이 아직 없어 자동 생성에서 제외함 - 검수에서 수동 추가"))
            continue

        if qa.form_override is not None:
            # 방식 B(비전) 출처 문항 - layout_hint로 이미 form이 정해져 있으므로
            # ui_type 기반 규칙(momo_book.db 전용)을 건너뛴다. widget_unavailable도
            # 안 남긴다 - "DB에 신호가 없어서" 단순 답란이 된 게 아니라 vision이
            # 실제로 그렇게 봤기 때문(2026-09-24 지시 [1]/[2]).
            q_fields = dict(qa.form_override)
            form_flags: list[Flag] = []
        else:
            q_fields, form_flags = form_for_ui_type(qa.ui_type, qa.ui_config, qa.question_text)
            for f in form_flags:
                f.order_no = qa.order_no
            if qa.ui_type in ("text_long", "text_short"):
                # 비교형(compare)/목록형(list)/서약형(pledge)/말풍선형(speech) 위젯은 DB
                # ui_type에 신호가 없다(momo_book.db 실측: 전부 text_long/short로만 옴) -
                # 이 문항을 더 풍부한 위젯으로 바꿀지는 검수 단계의 창작적 선택으로 남긴다.
                # 사용자 지시(2026-09-23) 4단계 방침: 규칙 엔진은 억지로 추론하지 않고,
                # 검수에서 바꾼 이력(form_change)을 correction_log에 쌓아 나중에 규칙화한다.
                flags.append(Flag(order_no=qa.order_no, kind="derived", category="widget_unavailable",
                                   message=f"문항 {qa.order_label}: 단순 답란(single)으로 매핑함 - "
                                           f"비교형/목록형/서약형/말풍선형 등으로 바꾸는 건 DB에 신호가 "
                                           f"없는 창작적 선택이라 검수 단계에서 결정"))
        flags.extend(form_flags)
        q = {"id": qa.order_label, "t": qa.question_text, **q_fields}
        if qa.form_override is not None:
            # 방식 B 문항은 q.id가 momo_book.db의 order_no와 대응하지 않으므로
            # (2026-09-26 검수에서 발견) 검수 화면 원문 대조가 source_page로
            # 찾도록 힌트를 남긴다 - OLD DB 파이프라인은 안 붙여 골든 비교에
            # 영향이 없다.
            q["src_page"] = qa.source_page

        guide, guide_flags = _guide_for(doc, qa)
        flags.extend(guide_flags)

        has_ref = qa.ref_table is not None or ref_bearer == qa.order_label
        if not qa.excerpt_text:
            # SPEC §4 "중등 제시문이 질문 안에 섞임" - ①단계가 일부러 분리하지 않고
            # 남겨 둔 경계(기존 골든 비교 테스트에서도 같은 경계를 확인했다). 여기서는
            # 별도 제시문 상자를 만들 수 없으니 solo로 내려간다.
            page_type = "solo"
            flags.append(Flag(order_no=qa.order_no, kind="split",
                               message=f"문항 {qa.order_label}: 제시문이 question_text 안에서 아직 "
                                       f"분리되지 않음 - LLM 분리 후 qa/qaband/qaref로 전환 필요"))
            page = {"type": page_type, "step": "STEP 2", "title": "함께 들여다보기", "guide": guide, "q": q}
        else:
            ptype, extra = excerpt_page_type(qa.excerpt_text, has_ref, question_len=len(qa.question_text or ""))
            page = {
                "type": ptype, "step": "STEP 2", "title": "함께 들여다보기", "guide": guide, "q": q,
                "excerpt": {"p": qa.excerpt_page, "text": [qa.excerpt_text]},
            }
            page.update(extra)
            if qa.ref_table is not None:
                # 방식 B: 같은 페이지의 reference_table 항목을 그대로 연결(2026-09-24
                # 지시 [1] "이미 qaref로 구현돼 있으니 연결만") - 어느 문항에 붙일지
                # 추측하는 휴리스틱이 필요 없다(원본 페이지 번호로 이미 확정됨).
                page["ref"] = qa.ref_table
            elif has_ref:
                page["ref"] = _hanja_ref_table(doc)
                flags.append(Flag(order_no=qa.order_no, kind="derived", category="placeholder",
                                   message=f"문항 {qa.order_label}: 참고표 제목(ref.title)이 DB에 "
                                           f"없어 비워 둠 - 검수에서 채워야 함"))

        # solo는 이미지 슬롯이 있어야 성립하는 레이아웃(좌 이미지/우 문항)이라 항상 슬롯을
        # 준다. qa/qaband/qaref는 table·compare처럼 답란이 넓게 필요한 form이면 이미지
        # 자리 대신 wide로 전체 폭을 문항에 준다(골든 L9 문항7 사례) - 단, SPEC §3.5.1
        # "원본 교재 이미지 우선"이 더 상위 원칙이라, 그 쪽에 원본 삽화가 실제로 있으면
        # wide보다 이미지 슬롯을 우선한다(사용자 지시 2026-09-25 - 비전 초안에서 표/
        # 서약형으로 매핑된 문항이 원본 삽화가 있는데도 wide 처리되어 이미지가 통째로
        # 빠지는 회귀를 발견해 고침). L9 문항7처럼 애초에 그 쪽에 원본 삽화가 없는
        # 경우는 여전히 wide로 빠지므로 기존 골든과도 어긋나지 않는다.
        # 2026-09-26: 질문 자체가 길면(LONG_QUESTION_MIN 이상, excerpt_page_type과
        # 같은 기준) form이 table/compare가 아니어도 3층 구조를 위해 wide로 뺀다
        # (젊은 예술가의 초상 8·19쪽 - 긴 단답형 질문이 반쪽 칸에 눌려 있던 문제).
        is_long_question = len(qa.question_text or "") >= LONG_QUESTION_MIN
        compare_cards = q_fields.get("cards") if q_fields.get("form") == "compare" else None
        multi_images = _all_images_for_qa(doc, qa, claimed_images) if compare_cards else []
        if compare_cards and len(multi_images) >= 2 and len(multi_images) >= len(compare_cards):
            # 2026-09-26: compare 카드 수만큼(또는 그 이상) 원본 이미지가 같은
            # 쪽에 있으면 카드마다 하나씩 배정한다(두근두근 한국사 "사라진
            # 조선총독부"/"남아있는 삼전도비" - 기존엔 첫 이미지 하나만 골라
            # 나머지가 버려지고 있었다). 전부 claimed 처리해 다른 문항이 또
            # 못 쓰게 한다.
            for card, img_path in zip(q["cards"], multi_images):
                card["img"] = img_path
                claimed_images.add(img_path)
            page["wide"] = True
        elif page["type"] != "solo" and (q_fields.get("form") in _WIDE_FORMS or is_long_question) and not _original_image_for_qa(doc, qa, claimed_images):
            page["wide"] = True
        else:
            # original_img가 있으면 _slot_for_qa가 어차피 같은 걸 찾아 슬롯을 채운다
            # (자리표시자 flag 없이) - 위에서 이미 확인한 값을 또 계산만 안 할 뿐,
            # 로직은 이 함수 하나로 유지한다.
            slot, slot_flag = _slot_for_qa(doc, qa, form=q_fields.get("form"), claimed=claimed_images)
            page["slot"] = slot
            if slot_flag:
                flags.append(slot_flag)
            if page["type"] == "qaref" and slot.get("img"):
                # 참고표가 쓴 이미지는 같은 쪽의 다른 문항에 또 재사용하지 않는다
                # (열하일기 검수에서 발견한 오배치 인상 - 클래스 docstring 참고).
                claimed_images.add(slot["img"])

        if qa.excerpt_text and len(qa.excerpt_text) > _EXCERPT_SPLIT_THRESHOLD:
            chunks = _split_excerpt_into_pages(qa.excerpt_text, _EXCERPT_PAGE_BUDGET)
            if len(chunks) > 1:
                for chunk in chunks[:-1]:
                    pages.append({
                        "type": "excerpt", "step": "STEP 2", "title": "함께 들여다보기", "guide": guide,
                        "excerpt": {"p": qa.excerpt_page, "text": [chunk]}, "continues": True,
                    })
                page["excerpt"]["text"] = [chunks[-1]]
                page["excerpt"]["continued"] = True
                flags.append(Flag(order_no=qa.order_no, kind="split",
                                   message=f"문항 {qa.order_label}: 제시문이 길어({len(qa.excerpt_text)}자) "
                                           f"{len(chunks)}쪽으로 나눔 - 실제 인쇄 미리보기에서 분량이 "
                                           f"맞는지 검수 필요"))

        pages.append(page)

    return pages, flags
