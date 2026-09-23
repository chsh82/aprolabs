"""essay_prompt 정규화 - SPEC §2.1 essay 페이지(topic/lead/dialog/closing).

L2·L5는 구조가 깨끗하다(main_topic=주제, writing_guide=인용 도입부,
closing_instruction=마무리 안내) - 그대로 매핑.

L9는 writing_guide 안에 "3단계: 주제 글쓰기…STEP1. …" 처럼 STEP3 리드와
STEP1 질문이 한 필드에 같이 들어있고 closing_instruction은 비어 있다 -
이건 규칙으로 안전하게 못 쪼갠다(어디까지가 리드이고 어디부터 STEP1인지
문서마다 다를 수 있음) - lead에 원문 그대로 넣고 `split` 플래그로 사람이
보게 한다."""
from __future__ import annotations

import sqlite3

from normalize.models import Flag, NormalizedEssay, NormalizedEssayOutlineQuestion
from normalize.text_repair import repair_text

_STEP_MARKER = "STEP1"


def normalize_essay(essay_row: sqlite3.Row | None, outline_rows: list[sqlite3.Row]) -> NormalizedEssay | None:
    if essay_row is None:
        return None

    lead, lead_flags = repair_text(essay_row["writing_guide"])
    topic, topic_flags = repair_text(essay_row["main_topic"])

    flags: list[Flag] = list(lead_flags) + list(topic_flags)

    if lead and _STEP_MARKER in lead:
        flags.append(Flag(
            kind="split",
            message=f"writing_guide에 {_STEP_MARKER} 질문이 리드와 함께 섞여 있음 - "
                     f"STEP3 리드와 STEP1 질문을 사람이 나눠야 함",
            before=lead,
        ))

    outline = []
    for o in sorted(outline_rows, key=lambda r: r["order_no"]):
        text, o_flags = repair_text(o["question_text"])
        flags.extend(o_flags)
        outline.append(NormalizedEssayOutlineQuestion(
            order_no=o["order_no"], role=o["role"], question_text=text or "",
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
