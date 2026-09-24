"""비전 결과(layout_hint) -> 조판 레코드(q.form) 매핑 (사용자 지시 2026-09-24 [1]).

SPEC §2.2의 8개 form(single/blanks/table/list/compare/pledge/speech/memo)
중 vision_item 하나가 해당하는 form과 그 필드를 결정한다. 사용자가 준
매핑표를 기본으로 쓰되, 실측 검증(3종 문서, VISION_LAYOUT_MAPPING_REPORT.md
참고) 결과 "table_answer" 한 shape 안에 진짜 table/list/compare 세 가지가
섞여 있다는 걸 확인했다 - 그중 **list만** 행 라벨이 "1." "2." "3." 처럼
순번으로 시작하는지로 안전하게 더 나눌 수 있었다(반례를 못 찾음). compare와
table은 vision_item 데이터만으로는(2행, 순번 없음 - 동일한 모양) 구분할
신호가 없다는 것도 실측으로 확인했다 - 이 경우는 사용자가 준 기본 매핑
그대로(table_answer -> table) 두고, 오분류 가능성을 그대로 보고한다
(VISION_LAYOUT_MAPPING_REPORT.md "한계" 절 참고, 억지로 추가 규칙을 만들지
않음 - 사용자 지시 "선잇기 프롬프트는 뒤로 미루고"와 같은 성격의 백로그).
"""
from __future__ import annotations

import json
import re

_NUMBERED_LABEL_RE = re.compile(r"^\s*\d+[.\)]")


def _parse_json_field(raw: str | None, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return default


def _looks_numbered_list(rows: list[list]) -> bool:
    """table_answer 행 라벨이 전부 "1." "2." "3."류 순번으로 시작하면 list로
    본다(야옹아 2-1 실측 근거). 순번이 하나라도 없으면 False - table/compare와
    안 섞으려고 보수적으로 전부 일치할 때만 인정한다."""
    if len(rows) < 2:
        return False
    labels = [str(r[0]) if r else "" for r in rows]
    return all(_NUMBERED_LABEL_RE.match(lbl) for lbl in labels)


def layout_hint_to_form(item: dict) -> tuple[dict, str]:
    """vision_item 1건(dict, sqlite3.Row도 dict()로 감싸서 전달) ->
    (q에 병합할 form 필드 dict, 판정 근거 태그). 판정 근거 태그는
    VISION_LAYOUT_MAPPING_REPORT.md 집계에 쓴다."""
    shape = item.get("layout_shape")
    table = _parse_json_field(item.get("table_json"), None)
    blanks = _parse_json_field(item.get("blanks_json"), [])
    choices = _parse_json_field(item.get("choices_json"), [])
    question_text = item.get("question_text") or ""
    blank_lines = item.get("blank_lines") or 0

    if shape == "table_answer" and table and table.get("rows"):
        rows = table["rows"]
        if _looks_numbered_list(rows):
            items = [{"hint": None, "prompt": (r[0] if r else "")} for r in rows]
            return {"form": "list", "items": items, "rowKind": "rowTall" if len(rows) <= 3 else "row"}, "table_answer->list(numbered)"
        if len(rows) == 2 and not _looks_numbered_list(rows):
            # 사용자 기본 매핑 그대로(table) - compare와 구분할 신호가 없음을
            # 확인했다(위 모듈 docstring). 억지로 compare로 승격하지 않는다.
            pass
        return {"form": "table", "rows": [{"label": (r[0] if r else ""), "prompt": question_text} for r in rows]}, "table_answer->table"

    if shape == "compare_two_col":
        # vision이 실제로 compare_two_col을 준 경우는 그대로 카드로 - table_json이
        # 있으면 각 행을 카드로, blanks만 있으면 blanks를 카드 제목으로 쓴다.
        cards = []
        if table and table.get("rows"):
            cards = [{"title": (r[0] if r else ""), "prompt": question_text} for r in table["rows"]]
        elif blanks:
            cards = [{"title": b, "prompt": question_text} for b in blanks]
        return {"form": "compare", "cards": cards}, "compare_two_col->compare"

    if shape == "numbered_list":
        items = []
        if blanks:
            items = [{"hint": None, "prompt": b} for b in blanks]
        elif table and table.get("rows"):
            items = [{"hint": None, "prompt": (r[0] if r else "")} for r in table["rows"]]
        return {"form": "list", "items": items, "rowKind": "rowTall" if len(items) <= 3 else "row"}, "numbered_list->list"

    if shape == "choice_options":
        opts = choices or blanks
        if len(opts) == 2:
            return {"form": "choice", "cards": [{"title": o} for o in opts]}, "choice_options->choice(2)"
        return {"form": "choiceList", "options": opts, "single": True}, "choice_options->choiceList(N)"

    if shape == "boxed_form":
        n = blank_lines or (len(table["rows"]) if table and table.get("rows") else 1)
        return {"form": "pledge", "n": n}, "boxed_form->pledge"

    if shape == "speech_bubble":
        starter = item.get("excerpt_text") or ""
        return {"form": "speech", "starter": starter}, "speech_bubble->speech"

    if shape == "ruled_lines":
        kind = "short" if blank_lines and blank_lines <= 1 else "long"
        return {"form": "single", "kind": kind}, "ruled_lines->single"

    if shape == "reference_table":
        # 학생 답란이 없는 참고용 표(예: 열하일기 5-1 한자표) - SPEC의 8개 form
        # 어디에도 "정답 없는 표"는 없다. table로 강제 매핑하면 빈 정답칸이
        # 있는 것처럼 보여서 억지로 끼워 맞추지 않는다 - 별도 태그로만 표시.
        return {"form": "reference", "rows": (table["rows"] if table else [])}, "reference_table->reference(no-answer)"

    # unclear 및 알 수 없는 shape(null 포함) - 사용자 지시대로 single + flag
    return {"form": "single", "kind": "long"}, f"{shape}->single(flag)"
