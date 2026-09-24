"""vision_extract.db(방식 B) -> NormalizedDoc (사용자 지시 2026-09-24 [5]의 기반).

책 수준 메타데이터(제목/저자/레벨/분기/표지문구/배경지식)와 글쓰기(essay)는
이번 비전 파싱의 대상이 아니었으므로(어순 뒤섞임·문항 분리·표 구조가 목표,
STEP3 재구성은 범위 밖) `normalize_document()`(momo_book.db 기반) 결과를
그대로 재사용한다 - discussion_qa/vocab/ox만 vision_item으로 다시 만든다.

한자·참고표(reference_table)는 qaref로 연결한다(2026-09-24 지시 [1]):
"같은 페이지 안에서 reference_table 다음에 나오는 discussion_qa"에 직접
붙인다 - 옛 파이프라인의 "제시문이 분리된 첫 문항" 휴리스틱보다 정확하다
(원본 페이지 번호를 실제로 알고 있으므로).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parent.parent
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from normalize.models import Flag, NormalizedDoc, NormalizedOx, NormalizedQA, NormalizedVocab  # noqa: E402
from normalize.run import normalize_document  # noqa: E402
from normalize.text_repair import repair_text  # noqa: E402
from vision_parse import db as vdb  # noqa: E402
from vision_parse.layout_map import layout_hint_to_form  # noqa: E402

_PAGE_NUM_RE = re.compile(r"\d+")


def _parse_json_field(raw, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return default


def _first_page_num(raw: str | None) -> int | None:
    if not raw:
        return None
    m = _PAGE_NUM_RE.search(raw)
    return int(m.group()) if m else None


def _vocab_from_item(item: dict, order_start: int) -> tuple[list[NormalizedVocab], list[Flag]]:
    """vocab 타입 vision_item 1건(표에 여러 단어를 묶어 옴) -> 단어별
    NormalizedVocab 여러 개. 표가 없으면(선잇기류로 정의 목록을 못 뽑은
    경우 - VISION_LAYOUT_MAPPING_REPORT.md 5절) blanks(단어만)로 채우고
    정의 없음으로 sup 플래그를 남긴다(기존 정규화와 같은 관례)."""
    table = _parse_json_field(item.get("table_json"), None)
    blanks = _parse_json_field(item.get("blanks_json"), [])
    out: list[NormalizedVocab] = []

    if table and table.get("rows"):
        for i, row in enumerate(table["rows"]):
            word = str(row[0]) if row else ""
            definition = str(row[1]).strip() if len(row) > 1 and row[1] else None
            word, _ = repair_text(word)
            v = NormalizedVocab(order_no=order_start + i, word=word or "", definition=definition,
                                 example_sentence=None, book_page=_first_page_num(item.get("page_number")))
            if not definition:
                v.flags.append(Flag(kind="sup", order_no=v.order_no,
                                     message=f"'{word}' 뜻풀이 누락(vision) - 초등 어휘 DB 조회 또는 LLM 보충 필요"))
            out.append(v)
    elif blanks:
        for i, word in enumerate(blanks):
            word, _ = repair_text(str(word))
            v = NormalizedVocab(order_no=order_start + i, word=word or "", definition=None,
                                 example_sentence=None, book_page=None)
            v.flags.append(Flag(kind="sup", order_no=v.order_no,
                                 message=f"'{word}' 뜻풀이 누락(vision, 선잇기류 정의 목록 미추출) - "
                                         f"VISION_LAYOUT_MAPPING_REPORT.md 5절 대상"))
            out.append(v)
    return out, []


def _ox_from_item(item: dict, order_no: int) -> NormalizedOx:
    question = item.get("excerpt_text") or item.get("question_text") or ""
    question, _ = repair_text(question)
    return NormalizedOx(order_no=order_no, question=question or "", answer=None, explanation=None,
                         evidence_page=_first_page_num(item.get("page_number")))


_QUOTE_OR_SPACE_RE = re.compile(r"[\s'‘’\"“”]+")


def _looks_like_step3_essay(question_text: str, base_essay) -> bool:
    """vision이 item_type='essay'로 분류한 항목이 진짜 STEP3(마무리 글쓰기)인지,
    아니면 STEP2 중간에 있는 개방형 질문(예: 야옹아 2-2 말풍선, 실제로는
    speech 위젯이어야 함)인지 구분한다. STEP3 항목은 momo_book.db
    essay_prompt.main_topic과 시작 문장이 거의 같다(실측 확인: L2-Q2-W08 -
    둘 다 "단추를 왜 끝까지 책임져서 키워야 할까요?"로 시작) - 단
    momo_book.db는 둥근따옴표('‘단추’'), vision은 곧은따옴표("'단추'")를 써서
    글자가 완전히 같지는 않다 - 따옴표·공백을 전부 지우고 비교해야 한다
    (처음엔 이걸 놓쳐서 오탐(매칭 실패)이 났었다)."""
    if not base_essay or not base_essay.main_topic:
        return False
    topic = _QUOTE_OR_SPACE_RE.sub("", base_essay.main_topic)
    q = _QUOTE_OR_SPACE_RE.sub("", question_text or "")
    return topic[:15] in q if len(topic) >= 15 else bool(topic) and topic in q


def _looks_like_step1_draw_page(question_text: str, band: str) -> bool:
    """저학년 STEP1 "생각 상자"(그림 칸) 페이지는 layout/step1.py의
    `_draw_page()`가 DB 없이 고정 문구로 이미 만든다(모든 저학년 문서 공통
    템플릿) - vision이 이 안내문을 별도 essay 항목으로도 뽑아 오면
    STEP1 페이지와 중복된다(실측: L2-Q2-W08 3쪽). band가 lower이고
    "생각"+("그리"|"그림"|"표현") 조합이 있으면 이 중복으로 보고 건너뛴다."""
    if band != "lower":
        return False
    q = question_text or ""
    return "생각" in q and any(k in q for k in ("그리", "그림", "표현"))


def build_vision_normalized_doc(doc_id: str) -> NormalizedDoc:
    base = normalize_document(doc_id)  # momo_book.db 기반 - essay/images/책 메타만 재사용

    conn = vdb.get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM vision_item WHERE doc_id = ? ORDER BY page_no, item_index", (doc_id,)
        ).fetchall()
    finally:
        conn.close()
    items = [dict(r) for r in rows]

    qa: list[NormalizedQA] = []
    vocab: list[NormalizedVocab] = []
    ox: list[NormalizedOx] = []
    doc_flags: list[Flag] = []

    vocab_counter = 1
    ox_counter = 1
    qa_counter = 1
    pending_ref: dict | None = None

    for item in items:
        item_type = item.get("item_type")
        shape = item.get("layout_shape")

        # layout_shape='reference_table'는 item_type과 독립된 필드다(실측 확인:
        # 열하일기 한자 표는 item_type='vocab'인데 shape만 reference_table이고,
        # 같은 문서 2쪽의 배경 설명 3건은 item_type='discussion_qa'인데도 shape가
        # reference_table이다 - 답란 없는 순수 소개/배경 내용). item_type이
        # 'vocab'이면 진짜 참고표(한자 표 등)로 보고 qaref에 연결하고, 그 외
        # (discussion_qa/essay)면 답도 참고표도 아닌 소개 문구이므로 qa 페이지를
        # 만들지 않고 건너뛴다(원문 손실이 아니라 "질문이 없는 소개 문단"이라는
        # 뜻 - vision이 이미 그렇게 판정했다).
        if shape == "reference_table" and item_type != "vocab":
            continue

        if item_type == "reference_table" or (shape == "reference_table" and item_type == "vocab"):
            table = _parse_json_field(item.get("table_json"), None)
            rows_out = []
            if table and table.get("rows"):
                for r in table["rows"]:
                    n = str(r[0]) if r else ""
                    v = str(r[1]) if len(r) > 1 else ""
                    if n == "~" or v == "~":
                        rows_out.append({"gap": True})
                    else:
                        rows_out.append({"n": n, "v": v})
            pending_ref = {"title": "", "rows": rows_out}
            continue

        if item_type == "vocab":
            new_vocab, _ = _vocab_from_item(item, vocab_counter)
            vocab.extend(new_vocab)
            vocab_counter += len(new_vocab)
            continue

        if item_type == "ox":
            ox.append(_ox_from_item(item, ox_counter))
            ox_counter += 1
            continue

        if item_type == "essay" and _looks_like_step3_essay(item.get("question_text"), base.essay):
            # 진짜 STEP3(마무리 글쓰기) - base.essay(momo_book.db essay_prompt)를
            # 그대로 쓰므로 여기서는 건너뛴다(모듈 docstring 참고).
            continue

        if item_type == "essay" and _looks_like_step1_draw_page(item.get("question_text"), base.band):
            # STEP1 생각상자와 중복(위 함수 docstring) - step1.py가 이미 만든다.
            continue

        if item_type in ("discussion_qa", "essay"):
            # item_type='essay'인데 STEP3가 아닌 것(예: 야옹아 2-2 말풍선)은 STEP2
            # 중간에 있는 개방형 질문이다 - discussion_qa와 똑같이 qa 페이지로 만든다
            # (실제로 이 케이스가 없으면 discussion_qa 분기와 동일하게 동작).
            fields, tag, notes = layout_hint_to_form(item)
            flags = [Flag(kind="derived", order_no=qa_counter, message=n) for n in notes]

            excerpt_text = item.get("excerpt_text")
            question_text = item.get("question_text") or ""
            if excerpt_text:
                excerpt_text, ef = repair_text(excerpt_text, qa_counter)
                flags.extend(ef)
            if question_text:
                question_text, qf = repair_text(question_text, qa_counter)
                flags.extend(qf)

            ref_table = None
            if pending_ref is not None:
                ref_table = pending_ref
                pending_ref = None

            q = NormalizedQA(
                order_no=qa_counter, order_label=str(qa_counter), reading_type=item.get("reading_type"),
                ui_type="text_long", excerpt_text=excerpt_text, excerpt_page=_first_page_num(item.get("page_number")),
                question_text=question_text or "", model_answer=None, source_page=item.get("page_no"),
                ui_config={}, flags=flags, form_override=fields, ref_table=ref_table,
            )
            qa.append(q)
            qa_counter += 1
            continue

        # cover/essay/unknown item_type - essay는 base.essay를 그대로 쓰고(모듈 docstring),
        # cover는 book.quote(cover_message)를 base에서 그대로 쓴다. 여기서는 건너뛴다.

    if pending_ref is not None:
        doc_flags.append(Flag(kind="derived", category="placeholder",
                               message="reference_table 다음에 붙일 discussion_qa 문항을 못 찾음 - "
                                       "참고표가 문서 끝에 있었거나 뒤에 discussion_qa가 없음"))

    return NormalizedDoc(
        doc_id=doc_id, book_title=base.book_title, book_author=base.book_author, level=base.level,
        band=base.band, quarter=base.quarter, week=base.week, cover_message=base.cover_message,
        background_text=base.background_text, qa=qa, vocab=vocab, ox=ox, hanja_glossary=[],
        essay=base.essay, images=base.images, flags=doc_flags,
    )
