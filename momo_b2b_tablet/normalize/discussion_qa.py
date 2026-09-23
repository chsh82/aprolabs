"""discussion_qa 재조립 - SPEC §4 "컬럼 밀림"/"unknown 행"/"중등 제시문이
질문 안에 섞임" 처리. 실제 DB(momo_book_db/momo_book.db)의 L2-Q2-W08(저학년),
L5-Q3-W10(고학년), L9-Q3-W07(중학생) 3종을 직접 조회해 확인한 패턴을 다룬다:

1) 저학년(L2): order_no마다 ui_type='unknown' 행에 "독해유형 + 제시문
   전문(raw_text)"이 들어있고, 옆에 진짜 text_long 행이 이미 축약된
   excerpt_text를 갖고 있다 - unknown 행에서 독해유형을 뽑아 채우고,
   raw_text가 더 길면 excerpt_text를 그걸로 교체한 뒤 unknown 행은 버린다.
   같은 그룹 안에 "(21쪽)"·숫자만 있는 쓰레기 행도 같이 버린다.

2) 고학년(L5): reading_type은 이미 채워져 있다. 대신 소문항이 갈라지면서
   question_text가 빈 채로 excerpt_text만 중복된 "부모" 행이 남는다 -
   같은 그룹에 질문이 있는 행이 하나라도 있으면 빈 질문 행은 버린다.
   텍스트 자체의 띄어쓰기/어순 손상은 text_repair가 맡는다.

3) 중학생(L9): unknown 행이 두 종류다.
   - 한자 뜻풀이(짧고 한자 포함, 예: "상징하다(象徵--)") → hanja_glossary로 분리.
   - 제시문 전문(독해유형 라벨 + 긴 문단) → 같은 그룹에 진짜 행이 있으면
     병합, 없으면 이 unknown 행 자체를 질문 없는 QA 스텁으로 승격.

4) 중학생 컬럼 밀림(L9 order_no=5 실사례, 사용자가 직접 발견해 지시): 한자
   용어 뒤에 "~"(생략 표시, order_no 5~15가 비어있다는 뜻) 하나만 있는 행도
   있고, "~" 뒤에 번호 매긴(1) 2) ...) 질문이 더 붙어 있는 행도 있다(order_no=19
   행에 "표현하다(表現--)" 용어 + order_no=5의 진짜 질문이 같이 붙어 있었음).
   이런 "용어 \\n~\\n(질문)" 행은 문서 전체를 먼저 훑어 따로 뽑아내고(용어는
   hanja_glossary로, 질문은 "질문 없는" 그룹에 되돌려준다) - 원래 자리(order_no=19)
   에는 QA를 만들지 않는다(전부 이 용어+질문 추출에 흡수됨)."""
from __future__ import annotations

import re
import sqlite3
from collections import defaultdict

from normalize.models import Flag, HanjaGloss, NormalizedQA
from normalize.text_repair import normalize_cjk_compat, repair_text

_LABEL_RE = re.compile(r"^\[?([가-힣]+(?:\s*/\s*[가-힣]+)*)\s*독해\]?\s*\n?")
_HANJA_RE = re.compile(r"[一-鿿]")
_PAGE_CITE_RE = re.compile(r"^\(\d+쪽\)$")

# 한자 용어 뒤에 "~"만 단독으로(줄 경계) 있으면 생략 표시(gap) - 그 뒤에 더
# 내용이 있으면(번호 매긴 질문 등) 그건 다른 order_no로 갈 내용이 컬럼 밀림으로
# 여기 붙은 것이다. "(212~214)" 같은 쪽수 범위와 헷갈리지 않도록 "~"가 줄
# 앞뒤로 독립돼 있을 때만(양옆이 개행) 매칭한다.
_GAP_RE = re.compile(r"^(.+?)\n~\n?(.*)$", re.S)
_NUMBERED_ITEM_RE = re.compile(r"(?:^|\n)\s*(\d+)\)\s*")


def _split_reading_type_label(raw: str) -> tuple[str | None, str]:
    m = _LABEL_RE.match(raw)
    if not m:
        return None, raw
    label = m.group(1).strip()
    body = raw[m.end():].strip()
    return label, body


def _is_hanja_gloss(text: str) -> bool:
    """실 데이터 기준: 한자 뜻풀이는 25자 이하·줄바꿈 없음. "…다/…요"로 끝나는지로
    거르면 "본뜨다, 그리다"처럼 동사형 뜻풀이가 오분류된다(실제로 이 사례를 보고
    고쳤다) - 대신 진짜 헷갈리는 대상인 독해유형 라벨("…독해"/"…독해]")만 명시
    제외한다."""
    if "\n" in text or len(text) > 25:
        return False
    if text.rstrip().endswith(("독해", "독해]")):
        return False
    if _HANJA_RE.search(text):
        return True
    return len(text) <= 20


def _is_garbage_question(text: str | None) -> bool:
    if not text:
        return True
    t = text.strip()
    if not t:
        return True
    return bool(_PAGE_CITE_RE.match(t)) or t.isdigit()


def _split_numbered_questions(text: str) -> list[str]:
    chunks = _NUMBERED_ITEM_RE.split("\n" + text)
    if len(chunks) < 3:
        return []
    bodies = [chunks[i] for i in range(2, len(chunks), 2)]
    return [b.strip() for b in bodies if b.strip()]


def _scan_gloss_gap_rescue(
    rows: list[sqlite3.Row],
) -> tuple[list[HanjaGloss], list[str], set[int]]:
    """문서 전체(모든 order_no)에서 "한자 용어\\n~\\n(질문)" 행을 찾는다. real/unknown
    구분 없이 전부 본다 - L9 order_no=19 행은 ui_type='text_long'(real)인데도
    이 패턴이었다."""
    hanja_entries: list[HanjaGloss] = []
    rescued_questions: list[str] = []
    consumed_ids: set[int] = set()

    for r in rows:
        raw = (r["raw_text"] or "").strip()
        if not raw:
            continue
        m = _GAP_RE.match(raw)
        if not m:
            continue
        term = m.group(1).strip()
        tail = m.group(2).strip()
        if "\n" in term or len(term) > 25:
            continue  # 용어치고 너무 김 - 이 패턴이 아니라고 판단하고 그냥 둠
        # gap_after는 "~" 뒤에 진짜로 아무 내용도 없을 때만 True다(골든 샘플의
        # ref.rows에 {gap:true} 행이 term 4와 16 "사이"에 딱 하나만 있는 걸 보고
        # 확인함) - "~" 뒤에 질문이 붙어 있던 19번(표현하다)은 생략 표시가 아니라
        # 그냥 컬럼 밀림 구분자였으므로 gap_after=False로 둔다.
        term = normalize_cjk_compat(term)  # CJK 호환 코드포인트만 표준화(예: 類) - 한글 자모는 안 건드림
        hanja_entries.append(HanjaGloss(order_no=r["order_no"], term=term, gap_after=not tail))
        consumed_ids.add(r["id"])
        if tail:
            bodies = _split_numbered_questions(tail)
            rescued_questions.extend(bodies if bodies else [tail])

    return hanja_entries, rescued_questions, consumed_ids


def normalize_discussion_qa(rows: list[sqlite3.Row]) -> tuple[list[NormalizedQA], list[HanjaGloss], list[Flag]]:
    doc_flags: list[Flag] = []

    gap_hanja, rescued_questions, consumed_ids = _scan_gloss_gap_rescue(rows)
    hanja_out: list[HanjaGloss] = list(gap_hanja)

    groups: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for r in rows:
        if r["id"] in consumed_ids:
            continue
        groups[r["order_no"]].append(r)

    qa_out: list[NormalizedQA] = []

    for order_no in sorted(groups):
        group = groups[order_no]
        real_rows = [r for r in group if r["ui_type"] != "unknown"]
        unknown_rows = [r for r in group if r["ui_type"] == "unknown"]

        excerpt_candidates: list[tuple[str | None, str, sqlite3.Row]] = []
        for u in unknown_rows:
            raw = (u["raw_text"] or "").strip()
            if not raw:
                continue
            if _is_hanja_gloss(raw):
                hanja_out.append(HanjaGloss(order_no=order_no, term=normalize_cjk_compat(raw)))
                continue
            label, body = _split_reading_type_label(raw)
            if body:
                excerpt_candidates.append((label, body, u))

        # 쓰레기 행(제시문도 질문도 없는) 제거
        kept: list[sqlite3.Row] = []
        for r in real_rows:
            if _is_garbage_question(r["question_text"]) and not (r["excerpt_text"] or "").strip():
                doc_flags.append(Flag(
                    kind="split", order_no=order_no,
                    message=f"제시문·질문이 모두 없는 행 제거 (raw question_text={r['question_text']!r})",
                ))
                continue
            kept.append(r)

        # 소문항이 갈라지며 남은 "빈 질문" 부모 행 제거 - 같은 그룹에 질문 있는
        # 행이 하나라도 있을 때만(그렇지 않으면 유일한 정보 소스이므로 남긴다).
        has_real_question = any((r["question_text"] or "").strip() for r in kept)
        if has_real_question:
            filtered = []
            for r in kept:
                if not (r["question_text"] or "").strip():
                    doc_flags.append(Flag(
                        kind="split", order_no=order_no,
                        message=f"질문 없는 중복 행 제거(order_label={r['order_label']!r}, "
                                f"같은 그룹에 실제 질문 있는 행 존재)",
                    ))
                    continue
                filtered.append(r)
            kept = filtered

        if kept:
            # 그룹(같은 order_no) 안에서 소문항(1-1/1-2 등)이 제시문을 공유하는
            # 경우가 있다 - DB에는 첫 소문항에만 excerpt_text가 채워지고 나머지는
            # 비어 있다(같은 지문을 참고하라는 뜻). golden 샘플에서 실제로 확인된
            # 패턴이라 그룹 전체가 공유할 대표 제시문을 먼저 정해 둔다.
            group_excerpt = next(
                (r["excerpt_text"] for r in kept if (r["excerpt_text"] or "").strip()), None
            )

            for r in kept:
                reading_type = r["reading_type"]
                excerpt_text = r["excerpt_text"] or group_excerpt
                question_text = r["question_text"]
                merge_flags: list[Flag] = []

                # 중학생(L9)처럼 독해유형 라벨이 이 행 자신의 question_text 맨
                # 앞에 붙어 있는 경우(별도 unknown 행이 아예 없음) - 라벨만
                # 기계적으로 떼어 reading_type에 채운다. 라벨 뒤 본문(인용+질문이
                # 아직 안 갈라진 상태)은 그대로 question_text에 남겨 두고, 완전한
                # 분리(인용/질문 구분)는 LLM 단계로 넘긴다(SPEC §4).
                if not reading_type and question_text:
                    own_label, own_body = _split_reading_type_label(question_text.strip())
                    if own_label:
                        reading_type = own_label
                        question_text = own_body
                        merge_flags.append(Flag(
                            kind="split", order_no=order_no,
                            message=f"질문 자체 앞머리에서 독해유형 추출: {own_label!r} "
                                    f"(인용문·질문은 아직 안 갈라짐 - LLM 단계에서 분리 필요)",
                        ))

                for label, body, _u in excerpt_candidates:
                    if not reading_type and label:
                        reading_type = label
                        merge_flags.append(Flag(
                            kind="split", order_no=order_no,
                            message=f"unknown 행에서 독해유형 추출: {label!r}",
                        ))
                    if body and (not excerpt_text or len(body) > len(excerpt_text)):
                        merge_flags.append(Flag(
                            kind="split", order_no=order_no,
                            message="unknown 행의 raw_text가 더 길어 excerpt_text로 채택",
                            before=excerpt_text, after=body,
                        ))
                        excerpt_text = body

                if excerpt_text and not (r["excerpt_text"] or "").strip() and excerpt_text == group_excerpt:
                    merge_flags.append(Flag(
                        kind="split", order_no=order_no,
                        message="같은 그룹의 다른 소문항이 가진 제시문을 공유해서 채움",
                    ))

                repaired_excerpt, excerpt_flags = repair_text(excerpt_text, order_no)
                repaired_question, question_flags = repair_text(question_text, order_no)

                qa = NormalizedQA(
                    order_no=order_no,
                    order_label=r["order_label"] or str(order_no),
                    reading_type=reading_type,
                    ui_type=r["ui_type"],
                    excerpt_text=repaired_excerpt,
                    excerpt_page=r["excerpt_page"],
                    question_text=repaired_question or "",
                    model_answer=r["model_answer"],
                    source_page=r["source_page"],
                    ui_config=_parse_ui_config(r["ui_config"]),
                )
                qa.flags.extend(merge_flags)
                qa.flags.extend(excerpt_flags)
                qa.flags.extend(question_flags)
                qa_out.append(qa)
        else:
            # 진짜 행이 하나도 안 남음 - unknown(제시문)이 유일한 소스.
            for label, body, u in excerpt_candidates:
                repaired_excerpt, excerpt_flags = repair_text(body, order_no)

                if rescued_questions:
                    # 다른 order_no(컬럼 밀림)에 붙어 있던 진짜 질문을 찾음 - derived 아님.
                    # list(...)로 실제 복사해야 한다 - 그냥 대입하면 같은 리스트 객체를
                    # 가리키게 돼서 바로 다음 줄의 clear가 bodies_to_use까지 비워버린다
                    # (실제로 이 버그로 order_no=5가 통째로 사라지는 걸 보고 고쳤다).
                    bodies_to_use = list(rescued_questions)
                    rescued_questions.clear()
                    for i, qtext in enumerate(bodies_to_use, start=1):
                        repaired_q, q_flags = repair_text(qtext, order_no)
                        sub_label = f"{order_no}-{i}" if len(bodies_to_use) > 1 else str(order_no)
                        qa = NormalizedQA(
                            order_no=order_no, order_label=sub_label, reading_type=label,
                            ui_type="text_long", excerpt_text=repaired_excerpt,
                            excerpt_page=u["excerpt_page"], question_text=repaired_q or "",
                            model_answer=None, source_page=u["source_page"],
                        )
                        qa.flags.extend(excerpt_flags)
                        qa.flags.extend(q_flags)
                        qa.flags.append(Flag(
                            kind="split", order_no=order_no,
                            message="질문이 다른 order_no 행에 컬럼 밀림으로 붙어 있던 것을 복원함",
                        ))
                        qa_out.append(qa)
                else:
                    qa = NormalizedQA(
                        order_no=order_no,
                        order_label=u["order_label"] or str(order_no),
                        reading_type=label,
                        ui_type="text_long",
                        excerpt_text=repaired_excerpt,
                        excerpt_page=u["excerpt_page"],
                        question_text="",
                        model_answer=None,
                        source_page=u["source_page"],
                    )
                    qa.flags.extend(excerpt_flags)
                    qa.flags.append(Flag(
                        kind="derived", order_no=order_no,
                        message="이 order_no는 제시문만 DB에 있고 질문이 없음 - 사람/LLM이 질문을 만들어야 함",
                    ))
                    qa_out.append(qa)

    if rescued_questions:
        doc_flags.append(Flag(
            kind="split",
            message=f"컬럼 밀림으로 복원한 질문 {len(rescued_questions)}건을 붙일 곳을 못 찾음 - 사람 확인 필요",
        ))

    return qa_out, hanja_out, doc_flags


def _parse_ui_config(raw) -> dict:
    if not raw:
        return {}
    import json
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {}
