"""STEP3 페이지(essay + memos) - essay_prompt/essay_outline_question 기반.
SPEC §2.1, §3.4 "주제 페이지 -> 메모 페이지"."""
from __future__ import annotations

from normalize.models import Flag, NormalizedDoc

from .rules import WRITING_CHARACTER, split_dialog_lines

_GUIDE = {"img": WRITING_CHARACTER, "rt": "주제 글쓰기", "nm": "앤과 이야기 짓기"}
_DEFAULT_CLOSING = "생각한 내용을 엮어서 글쓰기."
_MEMOS_CLOSING = "세 메모를 차례대로 엮으면 한 편의 글의 뼈대가 됩니다. 완성한 글은 원고지에 이어서 씁니다."


def build_step3_pages(doc: NormalizedDoc) -> tuple[list[dict], list[Flag]]:
    if doc.essay is None:
        return [], [Flag(kind="missing", message="essay_prompt 레코드가 없어 STEP3를 만들지 못함")]

    flags: list[Flag] = []
    essay = doc.essay

    dialog = split_dialog_lines(essay.lead)
    if not dialog:
        flags.append(Flag(kind="derived",
                           message="STEP3 도입 인용문(dialog)을 essay.lead(writing_guide)에서 "
                                   "뽑지 못함(따옴표로 감싼 대화문이 없음) - LLM 창작 필요"))
        dialog = [essay.lead] if essay.lead else []

    closing = essay.closing_instruction or ""
    if not closing:
        flags.append(Flag(kind="missing", message="essay_prompt.closing_instruction이 비어 있음"))
        closing = _DEFAULT_CLOSING

    # 2026-09-27 사용자 지시 [1] - 재추출 v2로 같은 쪽(source_page)에 essay
    # 이미지가 2장 이상 나오는 경우가 있음(정렬은 source_page 순, 안정적인
    # 순서를 위해). essay_prompt.image_path가 있으면 그게 1순위(기존 동작
    # 유지), doc.images의 essay 이미지는 1장은 essay 주제 페이지, 2장째는
    # memos(생각 모으기) 페이지에 배분한다 - 한 페이지 한 이미지 원칙을 STEP3
    # 에도 그대로 적용(갤러리로 좁혀 넣지 않음). 3장째부터는 미사용 목록으로
    # 남아 검수 화면에서 수동 배정 대상이 된다.
    essay_images = sorted(
        (img for img in doc.images if img.image_type == "essay"),
        key=lambda img: img.source_page or 0,
    )

    slot: dict
    if essay.image_path:
        slot = {"img": essay.image_path, "src": "원본 이미지"}
        memos_image = essay_images[0] if essay_images else None
    elif essay_images:
        slot = {"img": essay_images[0].file_path, "src": "원본 이미지"}
        memos_image = essay_images[1] if len(essay_images) > 1 else None
    else:
        slot = {"scene": "(LLM 생성 필요)", "avoid": "메모 질문의 답을 암시하는 요소를 넣지 않는다."}
        memos_image = None
        flags.append(Flag(kind="derived", category="placeholder",
                           message="STEP3 이미지가 원본에 없어 생성 지시문이 필요함(자리표시자)"))

    essay_page = {
        "type": "essay", "step": "STEP 3", "title": "내 글로 엮기", "guide": _GUIDE,
        "topic": essay.main_topic, "dialog": dialog, "closing": closing, "slot": slot,
    }

    memos_qs = [
        {"id": f"S3-{i}", "no": str(i), "t": o.question_text, "kind": "memo"}
        for i, o in enumerate(essay.outline, start=1)
    ]
    if not memos_qs:
        flags.append(Flag(kind="missing", message="essay_outline_question이 없어 STEP3 메모 질문을 만들지 못함"))

    memos_page = {
        "type": "memos", "step": "STEP 3", "title": "생각 모으기", "guide": _GUIDE,
        "topic": essay.main_topic, "closing": _MEMOS_CLOSING, "qs": memos_qs,
    }
    if memos_image is not None:
        memos_page["slot"] = {"img": memos_image.file_path, "src": f"원본 {memos_image.source_page}쪽"}
    return [essay_page, memos_page], flags
