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


_BARE_NUMBER_RE = re.compile(r"^\(?\s*\d+\s*\)?$")
_LABEL_RE = re.compile(r"^(뜻|문장)\s*[:：]?\s*(.*)$")
_LABEL_SPLIT_RE = re.compile(r"/\s*(?:뜻|문장)\s*[:：]?")


def _classify_label_row(row: list) -> tuple[str | None, str | None, str | None]:
    """행 하나에서 "뜻"/"문장" 라벨 칸을 찾는다(2026-09-26 305건 전수조사로
    확인된 3가지 실제 형태 - 예시는 실측 문서):
    - 라벨과 내용이 각각 딴 칸: ["흥정하다","뜻","물건을..."] (L3-Q1-W05)
    - 라벨+내용이 한 칸에: ["요람 (8페이지)","뜻: 사물의... / 문장: [빈칸]"] (L3-Q3-W03)
    - 단어 칸 자체가 없음(라벨이 0번 칸): ["뜻","기쁨, 감격..."] (L3-Q1-W03 -
      이 경우 원본 단어 자체가 vision 추출에서 빠진 것 - 복구 불가, 그대로 보고)
    반환: (label, content, word) - 라벨을 못 찾으면 (None, None, None)."""
    for idx, cell in enumerate(row):
        cell_s = str(cell).strip() if cell is not None else ""
        m = _LABEL_RE.match(cell_s)
        if not m:
            continue
        label = m.group(1)
        inline = _LABEL_SPLIT_RE.split(m.group(2).strip())[0].strip()
        if inline:
            content = inline
        elif idx + 1 < len(row) and row[idx + 1]:
            content = str(row[idx + 1]).strip()
        else:
            content = None
        word = str(row[0]).strip() if idx > 0 and row[0] else None
        return label, content, word
    return None, None, None


def _vocab_from_item(item: dict, order_start: int) -> tuple[list[NormalizedVocab], list[Flag]]:
    """vocab 타입 vision_item 1건(표에 여러 단어를 묶어 옴) -> 단어별
    NormalizedVocab 여러 개.

    2026-09-26 305건 전수조사로 확인된 표 형태(사용자 지시로 이번에 전부
    정리) - vocab 항목의 table_json/blanks_json 조합이 문서마다 다르다:

    1. 선잇기(매칭)형 - vision이 두 가지 형태로 뽑아 온다:
       1a) blanks=단어 목록, table.rows=뜻 목록(헤더 없음, 한 칸짜리 행,
           개수가 blanks와 같음) - 실측: L1-Q1-W03.
       1b) 표 한 장 안에 "단어만 있는 행"과 "뜻만 있는 행"이 섞여 옴(표
           전체가 단어 행 + 뜻 행으로 정확히 나뉨) - 실측: L2-Q1-W10.
       화면에 보이는 순서일 뿐 실제 짝은 선으로 그어야 알 수 있어(사용자
       지시 2026-09-24) 자동으로 짝짓지 않는다 - 단어마다 뜻풀이 "후보
       목록"을 flag로 남긴다. (이전 코드는 1a를 table 우선으로 봐서
       뜻풀이 텍스트 자체를 "단어"로 잘못 넣고 있었다 - 내용이 완전히
       틀린 심각한 버그였다. 이 밖에 item_type='vocab'인데 layout_shape가
       'reference_table'로 잘못 붙어 참고표 경로로 새는 경우도 있었다
       - blanks_json이 함께 있으면 참고표가 아니라 이 매칭형으로 본다.)
    2. "뜻"/"문장" 라벨 행 쌍 - 같은 단어가 두 행(뜻 내용 행 + 학생이 쓸
       예문 빈칸 행)으로 나뉘어 온다(라벨이 딴 칸이든 한 칸에 붙어 오든).
       "뜻" 행만 실제 정의로 쓰고 "문장" 행은 버린다. 아주 드물게 단어
       칸 자체가 비어 있으면(원본 추출 실패) "(단어 미상)"으로 남기고
       강한 flag를 단다 - 억지로 이름을 만들어 붙이지 않는다.
    3. 위 둘 다 아니면 기존 단순 표(1행=1단어, 2번째 칸=정의)로 처리.
    4. table이 아예 없고 blanks만 있는 경우("각 문장에 들어갈 낱말을
       <보기>에서 고르세요"류 빈칸채우기 문항을 vision이 문장·보기 내용
       없이 번호표만 뽑아 온 경우, 52/305건 실측) - 번호표만 있으면 가짜
       "낱말"을 만들지 않고 통째로 버린다(야옹아 2쪽 "답란 3칸 중복" 신고
       원인). 진짜 단어 목록이면 기존처럼 정의 없음 sup 플래그로 채운다.
    """
    table = _parse_json_field(item.get("table_json"), None)
    blanks = _parse_json_field(item.get("blanks_json"), [])
    out: list[NormalizedVocab] = []
    flags: list[Flag] = []
    rows = table["rows"] if table and table.get("rows") else None

    match_words: list[str] | None = None
    match_defs: list[str] | None = None
    if rows and blanks and not table.get("headers") and len(rows) == len(blanks) \
            and all(len(r) <= 1 for r in rows if r):
        # 1a) 선잇기형(blanks=단어, table=한 칸짜리 뜻 목록 - 실측: L1-Q1-W03).
        match_words = [str(b) for b in blanks]
        match_defs = [str(r[0]).strip() for r in rows if r]
    elif rows and len(rows) >= 2:
        word_only = [str(r[0]).strip() for r in rows if r and str(r[0]).strip()
                     and not (len(r) > 1 and str(r[1]).strip())]
        def_only = [str(r[1]).strip() for r in rows if r and len(r) > 1 and str(r[1]).strip()
                    and not str(r[0]).strip()]
        if word_only and def_only and len(word_only) + len(def_only) == len(rows):
            # 1b) 선잇기형(표 한 장에 "단어만 있는 행"과 "뜻만 있는 행"이 섞여
            # 옴 - 실측: L2-Q1-W10). 짝은 마찬가지로 자동으로 안 정한다.
            match_words = word_only
            match_defs = def_only

    if match_words is not None:
        # 1) 선잇기형 - 자동 짝짓기 금지, 후보만 기록.
        cand_text = " / ".join(match_defs)
        for i, word in enumerate(match_words):
            word_r, _ = repair_text(str(word))
            v = NormalizedVocab(order_no=order_start + i, word=word_r or "", definition=None,
                                 example_sentence=None, book_page=_first_page_num(item.get("page_number")))
            v.flags.append(Flag(kind="sup", order_no=v.order_no,
                                 message=f"'{word_r}' 선잇기형 문항 - 뜻풀이 후보(순서 무관, 자동 매칭 안 함): "
                                         f"{cand_text}"))
            out.append(v)
        return out, flags

    if rows and any(_classify_label_row(r)[0] is not None for r in rows if r):
        # 2) "뜻"/"문장" 라벨 행 쌍.
        order: list[str] = []
        seen: dict[str, str | None] = {}
        no_word_n = 0
        for r in rows:
            if not r:
                continue
            label, content, word = _classify_label_row(r)
            if label is None:
                continue
            if word is None:
                no_word_n += 1
                key = f"__noword_{no_word_n}__"
            else:
                key = word
            if key not in seen:
                order.append(key)
                seen[key] = None
            if label == "뜻" and content:
                seen[key] = content
        for i, key in enumerate(order):
            definition = seen[key]
            v = NormalizedVocab(order_no=order_start + i, word="", definition=definition,
                                 example_sentence=None, book_page=_first_page_num(item.get("page_number")))
            if key.startswith("__noword_"):
                v.word = "(단어 미상)"
                v.flags.append(Flag(kind="derived", order_no=v.order_no,
                                     message=f"낱말 자체가 vision 추출에서 빠짐(뜻풀이만 있음: "
                                             f"'{(definition or '')[:30]}') - 원본 확인 후 수동 입력 필요"))
            else:
                word_out, _ = repair_text(key)
                v.word = word_out or ""
                if not definition:
                    v.flags.append(Flag(kind="sup", order_no=v.order_no,
                                         message=f"'{word_out}' 뜻풀이 누락(vision) - 초등 어휘 DB 조회 또는 "
                                                 f"LLM 보충 필요"))
            out.append(v)
        return out, flags

    if rows:
        # 3) 기존 단순 표(1행=1단어, 2번째 칸=정의).
        for i, row in enumerate(rows):
            word = str(row[0]) if row else ""
            definition = str(row[1]).strip() if len(row) > 1 and row[1] else None
            word, _ = repair_text(word)
            v = NormalizedVocab(order_no=order_start + i, word=word or "", definition=definition,
                                 example_sentence=None, book_page=_first_page_num(item.get("page_number")))
            if not definition:
                v.flags.append(Flag(kind="sup", order_no=v.order_no,
                                     message=f"'{word}' 뜻풀이 누락(vision) - 초등 어휘 DB 조회 또는 LLM 보충 필요"))
            out.append(v)
        return out, flags

    if blanks:
        # 4) table 없이 blanks만.
        if all(_BARE_NUMBER_RE.match(str(b).strip()) for b in blanks):
            return [], [Flag(kind="sup",
                              message="빈칸채우기형 어휘 문항의 문장·보기 내용을 추출하지 못해 "
                                      "번호표만 남음(vision) - 재추출 또는 수동 입력 필요")]
        for i, word in enumerate(blanks):
            word, _ = repair_text(str(word))
            v = NormalizedVocab(order_no=order_start + i, word=word or "", definition=None,
                                 example_sentence=None, book_page=None)
            v.flags.append(Flag(kind="sup", order_no=v.order_no,
                                 message=f"'{word}' 뜻풀이 누락(vision, 선잇기류 정의 목록 미추출) - "
                                         f"VISION_LAYOUT_MAPPING_REPORT.md 5절 대상"))
            out.append(v)
    return out, flags


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


def _is_blank(s: str | None) -> bool:
    return not (s or "").strip()


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
    # 페이지 경계로 갈라진 문항 처리(2026-09-26, 긴긴밤 9쪽·젊은 예술가 7·14쪽
    # 검수에서 발견): 워크북이 "제시문은 이 쪽 끝, 질문은 다음 쪽 첫 줄"처럼
    # 지면이 부족하면 한 문항을 페이지 사이에 걸쳐 인쇄하는데, 비전은 한
    # 페이지씩 보므로 이걸 서로 다른 두 항목으로 쪼개 온다. excerpt만 있고
    # question이 빈 항목은 바로 다음 항목이 question만(excerpt 없음) 채워져
    # 있을 때만 합치고, 그 외(다음 항목도 이미 자기 excerpt가 있는 등 안전하게
    # 합칠 수 없는 경우)는 빈 페이지를 만드는 대신 버리고 flag만 남긴다
    # (자동화는 검증 가능한 초안까지만 - SPEC §1).
    held_excerpt_item: dict | None = None

    def _drop_orphan_excerpt(reason: str) -> None:
        # category="page_split"(2026-09-26 사용자 지시) - placeholder와 구분해
        # 검수 화면 플래그 큐 상단에 별도로 모이게 한다(review.js FLAG_PRIORITY).
        doc_flags.append(Flag(
            kind="derived", category="page_split",
            message=f"페이지 경계로 갈라진 제시문을 질문과 합치지 못해 버림({reason}) - "
                    f"원본: \"{(held_excerpt_item['excerpt_text'] or '')[:40]}...\" "
                    f"(원본 {held_excerpt_item['page_no']}쪽) - 검수에서 수동 확인 필요",
        ))

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
        # 2026-09-26 추가 실측(L1-Q1-W03 등): shape='reference_table'인데
        # blanks_json도 함께 채워진 item_type='vocab' 항목은 진짜 참고표가
        # 아니라 "선으로 이어" 매칭 문항을 vision이 참고표로 오인한 것이다
        # (한자 표 등 진짜 참고표는 blanks_json이 비어 있다 - matching 항목만
        # extract.py 프롬프트 지시로 blanks+table을 같이 채운다). 이 경우는
        # 아래 reference_table 분기로 보내지 않고 일반 vocab 처리로 내려보낸다.
        is_mislabeled_vocab_match = (
            shape == "reference_table" and item_type == "vocab"
            and _parse_json_field(item.get("blanks_json"), [])
        )

        if shape == "reference_table" and item_type != "vocab":
            continue

        if not is_mislabeled_vocab_match and (
            item_type == "reference_table" or (shape == "reference_table" and item_type == "vocab")
        ):
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
            new_vocab, item_flags = _vocab_from_item(item, vocab_counter)
            vocab.extend(new_vocab)
            vocab_counter += len(new_vocab)
            doc_flags.extend(item_flags)
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
            excerpt_text = item.get("excerpt_text")
            question_text = item.get("question_text") or ""
            repair_flags: list[Flag] = []
            if excerpt_text:
                excerpt_text, ef = repair_text(excerpt_text, qa_counter)
                repair_flags.extend(ef)
            if question_text:
                question_text, qf = repair_text(question_text, qa_counter)
                repair_flags.extend(qf)

            if held_excerpt_item is not None:
                if _is_blank(question_text) or not _is_blank(excerpt_text):
                    # 다음 항목도 이미 자기 excerpt가 있거나(안전하게 못 합침) 질문이
                    # 또 비어 있음 - 들고 있던 제시문은 버린다.
                    _drop_orphan_excerpt("다음 항목이 짝이 아님")
                    held_excerpt_item = None
                else:
                    # 짝을 찾음: 들고 있던 excerpt + 이번 항목의 question으로 합친다.
                    prev = held_excerpt_item
                    held_excerpt_item = None
                    fields, tag, notes = layout_hint_to_form(item)
                    flags = [Flag(kind="derived", order_no=qa_counter, message=n) for n in notes]
                    flags.extend(prev["repair_flags"])
                    flags.extend(repair_flags)
                    flags.append(Flag(kind="derived", category="page_split", order_no=qa_counter,
                                       message=f"페이지 경계로 갈라졌던 제시문({prev['page_no']}쪽)과 "
                                               f"질문({item.get('page_no')}쪽)을 합침 - 검수에서 확인 필요"))
                    ref_table = None
                    if pending_ref is not None:
                        ref_table = pending_ref
                        pending_ref = None
                    q = NormalizedQA(
                        order_no=qa_counter, order_label=str(qa_counter), reading_type=item.get("reading_type"),
                        ui_type="text_long", excerpt_text=prev["excerpt_text"],
                        excerpt_page=prev["excerpt_page"], question_text=question_text or "",
                        model_answer=None, source_page=item.get("page_no"),
                        ui_config={}, flags=flags, form_override=fields, ref_table=ref_table,
                    )
                    qa.append(q)
                    qa_counter += 1
                    continue

            if _is_blank(excerpt_text) and _is_blank(question_text):
                # 제시문도 질문도 없음 - 앞 항목의 답란이 다음 쪽 첫머리로 밀려나며
                # 생긴 빈 항목(답란만 있는 페이지 경계 잔여물)으로 보고 버린다.
                doc_flags.append(Flag(
                    kind="derived", category="page_split",
                    message=f"제시문·질문이 모두 빈 항목을 버림(원본 {item.get('page_no')}쪽) - "
                            f"앞 문항의 답란이 다음 쪽으로 밀려나며 생긴 잔여물로 추정 - 검수에서 확인 필요",
                ))
                continue

            if not _is_blank(excerpt_text) and _is_blank(question_text):
                # 질문이 아직 안 왔을 수 있음 - 다음 항목을 봐야 판단 가능하니 보류.
                held_excerpt_item = {
                    "excerpt_text": excerpt_text, "excerpt_page": _first_page_num(item.get("page_number")),
                    "page_no": item.get("page_no"), "repair_flags": repair_flags,
                }
                continue

            fields, tag, notes = layout_hint_to_form(item)
            flags = [Flag(kind="derived", order_no=qa_counter, message=n) for n in notes]
            flags.extend(repair_flags)

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

    if held_excerpt_item is not None:
        _drop_orphan_excerpt("문서 끝까지 짝을 못 찾음")

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
