"""essay_prompt 정규화 - SPEC §2.1 essay 페이지(topic/lead/dialog/closing).

L2·L5는 구조가 깨끗하다(main_topic=주제, writing_guide=인용 도입부,
closing_instruction=마무리 안내) - 그대로 매핑.

중등(L9) 6건은 writing_guide 안에 "3단계: 주제 글쓰기…STEP1. …" 처럼
STEP3 리드와 STEP1 질문이 한 필드에 같이 들어있고, main_topic 자체도
"STEP2. …"/"STEP3. …"처럼 번호가 앞에 붙은 채로 들어있으며
essay_outline_question은 늘 비어 있다(2026-09-26 검수에서 발견 - 열하일기
STEP3 페이지에 "STEP1." 텍스트가 리드 안에 그대로 섞여 나오고 소질문
칸은 비어 있었음). 6건 전부 같은 패턴(실측)이라 규칙화한다:
- main_topic 앞의 "STEPn." 번호만 뗀다(문장 자체는 그대로 - 이게 진짜
  주제 문장이다).
- writing_guide 안에서 "STEPn." 마커로 시작하는 조각을 전부 찾아
  소질문(outline)으로 옮기고, 그 앞부분만 진짜 리드로 남긴다(DB의
  essay_outline_question이 이미 채워져 있으면 그쪽을 우선하고 건드리지
  않는다 - 실측상 항상 비어 있었지만 혹시 있는 문서를 덮어쓰지 않기
  위함). 주제 문장(main_topic)도 같은 흐름의 마지막 소질문이라 outline
  끝에 한 번 더 넣는다(사용자 지시 "소질문이 STEP1·2 두 가지" - 열하일기
  기준 STEP1+STEP2 두 개가 모두 소질문으로 보여야 함)."""
from __future__ import annotations

import re
import sqlite3

from normalize.models import Flag, NormalizedEssay, NormalizedEssayOutlineQuestion
from normalize.text_repair import repair_text

_STEP_SPLIT_RE = re.compile(r"(?=STEP\d+[.\)])")
_STEP_PREFIX_RE = re.compile(r"^STEP\d+[.\)]\s*")


def _extract_step_outline(lead: str) -> tuple[str, list[str]]:
    """lead(writing_guide) 안에 섞인 "STEPn. ..." 조각들을 떼어낸다.
    반환: (STEP 마커 이전의 진짜 리드, STEP별 소질문 텍스트 목록 - 번호
    접두어는 뗌. 렌더러가 memos 위젯에서 이미 번호 사각 배지를 따로
    붙이므로 텍스트 안에 또 있으면 중복 표시된다 - discussion_qa 문항
    번호 중복 제거와 같은 이유)."""
    parts = _STEP_SPLIT_RE.split(lead)
    if len(parts) <= 1:
        return lead, []
    real_lead = parts[0].strip()
    steps = [_STEP_PREFIX_RE.sub("", p.strip()) for p in parts[1:] if p.strip()]
    return real_lead, steps


def normalize_essay(essay_row: sqlite3.Row | None, outline_rows: list[sqlite3.Row]) -> NormalizedEssay | None:
    if essay_row is None:
        return None

    lead, lead_flags = repair_text(essay_row["writing_guide"])
    topic, topic_flags = repair_text(essay_row["main_topic"])

    flags: list[Flag] = list(lead_flags) + list(topic_flags)

    step_outline: list[str] = []
    if lead:
        new_lead, step_outline = _extract_step_outline(lead)
        if step_outline:
            flags.append(Flag(
                kind="split",
                message=f"writing_guide에 섞여 있던 STEP 소질문 {len(step_outline)}건을 "
                        f"리드에서 분리해 소질문으로 옮김 - 검수 확인 필요",
                before=lead, after=new_lead,
            ))
            lead = new_lead

    topic_had_step_prefix = bool(topic and _STEP_PREFIX_RE.match(topic))
    if topic_had_step_prefix:
        topic = _STEP_PREFIX_RE.sub("", topic)
        flags.append(Flag(kind="typo", message="main_topic 앞에 붙은 STEP 번호를 뗌", before=essay_row["main_topic"], after=topic))

    outline = []
    for o in sorted(outline_rows, key=lambda r: r["order_no"]):
        text, o_flags = repair_text(o["question_text"])
        flags.extend(o_flags)
        outline.append(NormalizedEssayOutlineQuestion(
            order_no=o["order_no"], role=o["role"], question_text=text or "",
        ))

    if not outline and step_outline:
        # DB의 essay_outline_question이 비어 있고(실측상 6건 전부 이 경우) STEP
        # 소질문을 찾았으면, 리드에서 뗀 것 + 주제 문장 자체(같은 흐름의 마지막
        # 소질문)를 순서대로 소질문으로 채운다.
        for i, text in enumerate(step_outline, start=1):
            outline.append(NormalizedEssayOutlineQuestion(order_no=i, role="body", question_text=text))
        if topic_had_step_prefix:
            outline.append(NormalizedEssayOutlineQuestion(
                order_no=len(outline) + 1, role="conclusion", question_text=topic,
            ))

    essay = NormalizedEssay(
        main_topic=topic or "",
        lead=lead,
        writing_format=essay_row["writing_format"],
        closing_instruction=essay_row["closing_instruction"],
        image_path=essay_row["image_path"],
        outline=outline,
    )
    essay.flags = flags
    return essay
