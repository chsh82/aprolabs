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


def _headers_as_compare_cards(table: dict, question_text: str) -> tuple[list[dict], list[str]] | None:
    """대립하는 두 입장에 각각 답을 쓰는 표는 vision이 표 값(rows)이 아니라
    머리칸(headers)에 "이해할 수 있어요!"/"보존해야 해요!" 같은 진짜 라벨을
    담아 온다(2026-09-26 두근두근 한국사 5·10쪽, 젊은 예술가의 초상 18·21쪽
    검수에서 발견 - rows는 학생이 채울 빈 칸이라 대개 빈 문자열이거나, 드물게
    이미지 캡션 같은 참고 텍스트가 그대로 들어 있다). headers가 2개 이상
    있으면 그걸 compare 카드 제목으로 쓴다 - rows[0]의 빈 문자열을 라벨로
    잘못 쓰던 버그를 고침. rows에 실제 값이 있으면(빈 답란이 아닐 가능성)
    검수용 note를 남기고 카드 자체는 건드리지 않는다(정답을 함부로 안 채움)."""
    headers = [str(h).strip() for h in (table.get("headers") or []) if str(h).strip()]
    if len(headers) < 2:
        return None
    notes: list[str] = []
    rows = table.get("rows") or []
    for r in rows:
        for i, h in enumerate(headers):
            if i < len(r) and str(r[i]).strip():
                notes.append(
                    f"머리칸 '{h}' 아래 원본 표에 값 '{r[i]}'가 있었음 - 학생이 쓸 빈 답란이 "
                    f"아니라 참고 캡션/라벨일 수 있어 검수 확인 필요"
                )
    cards = [{"title": h, "prompt": question_text} for h in headers]
    return cards, notes


def layout_hint_to_form(item: dict) -> tuple[dict, str, list[str]]:
    """vision_item 1건(dict, sqlite3.Row도 dict()로 감싸서 전달) ->
    (q에 병합할 form 필드 dict, 판정 근거 태그, 추가로 남길 플래그 메시지 목록).
    판정 근거 태그는 VISION_LAYOUT_MAPPING_REPORT.md 집계에 쓴다."""
    shape = item.get("layout_shape")
    table = _parse_json_field(item.get("table_json"), None)
    blanks = _parse_json_field(item.get("blanks_json"), [])
    choices = _parse_json_field(item.get("choices_json"), [])
    question_text = item.get("question_text") or ""
    blank_lines = item.get("blank_lines") or 0

    if shape == "table_answer" and table and table.get("rows"):
        rows = table["rows"]
        headers_compare = _headers_as_compare_cards(table, question_text)
        if headers_compare is not None:
            cards, header_notes = headers_compare
            return {"form": "compare", "cards": cards}, "table_answer->compare(headers)", header_notes
        if _looks_numbered_list(rows):
            items = [{"hint": None, "prompt": (r[0] if r else "")} for r in rows]
            return ({"form": "list", "items": items, "rowKind": "rowTall" if len(rows) <= 3 else "row"},
                    "table_answer->list(numbered)", [])
        notes: list[str] = []
        if len(rows) == 2:
            # 사용자 지시(2026-09-24) [2]: compare와 구분할 신호가 없다는 걸
            # 억지로 해결하려 하지 않고, "compare 후보" 플래그만 남겨 검수에서
            # 드롭다운 한 번으로 바꿀 수 있게 한다(야옹아 1번이 이 패턴).
            notes.append(
                f"2행 표(table)로 매핑했지만 라벨이 대립하는 두 항목("
                f"{rows[0][0] if rows[0] else ''!r} / {rows[1][0] if rows[1] else ''!r})"
                f"처럼 보여 compare 후보임 - 검수에서 필요하면 compare로 전환"
            )
        return ({"form": "table", "rows": [{"label": (r[0] if r else ""), "prompt": question_text} for r in rows]},
                "table_answer->table", notes)

    if shape == "compare_two_col":
        # vision이 실제로 compare_two_col을 준 경우는 그대로 카드로. 머리칸에
        # 진짜 라벨이 있으면(2026-09-26, _headers_as_compare_cards 참고) 그걸
        # 우선하고, 없으면 기존처럼 table_json 행 또는 blanks를 카드 제목으로.
        headers_compare = _headers_as_compare_cards(table, question_text) if table else None
        if headers_compare is not None:
            cards, header_notes = headers_compare
            return {"form": "compare", "cards": cards}, "compare_two_col->compare(headers)", header_notes
        cards = []
        if table and table.get("rows"):
            cards = [{"title": (r[0] if r else ""), "prompt": question_text} for r in table["rows"]]
        elif blanks:
            cards = [{"title": b, "prompt": question_text} for b in blanks]
        return {"form": "compare", "cards": cards}, "compare_two_col->compare", []

    if shape == "numbered_list":
        items = []
        if blanks:
            items = [{"hint": None, "prompt": b} for b in blanks]
        elif table and table.get("rows"):
            items = [{"hint": None, "prompt": (r[0] if r else "")} for r in table["rows"]]
        return ({"form": "list", "items": items, "rowKind": "rowTall" if len(items) <= 3 else "row"},
                "numbered_list->list", [])

    if shape == "choice_options":
        opts = choices or blanks
        if len(opts) == 2:
            return {"form": "choice", "cards": [{"title": o} for o in opts]}, "choice_options->choice(2)", []
        return {"form": "choiceList", "options": opts, "single": True}, "choice_options->choiceList(N)", []

    if shape == "boxed_form":
        if "공통점" in question_text and "차이점" in question_text:
            # boxed_form은 서약서·분석란을 시각적으로 구분 못 한다(둘 다 테두리
            # 박스) - 질문이 "공통점과 차이점을 정리해 봅시다"류면 서약서가 아니라
            # 비교 분석표다(2026-09-26 젊은 예술가의 초상 21쪽 검수에서 발견 -
            # 305건 전수조사 결과 이 텍스트 패턴은 이 1건뿐이라 안전하게 규칙화).
            return {"form": "compare", "cards": [
                {"title": "공통점", "prompt": question_text}, {"title": "차이점", "prompt": question_text},
            ]}, "boxed_form(공통점/차이점)->compare", []
        n = blank_lines or (len(table["rows"]) if table and table.get("rows") else 1)
        return {"form": "pledge", "n": n}, "boxed_form->pledge", []

    if shape == "speech_bubble":
        starter = (item.get("excerpt_text") or "").strip()
        if not starter:
            # 시작말이 없으면 말풍선만 빈 채로 붙는다(2026-09-26 야옹아 7쪽
            # 검수에서 발견) - speech 위젯은 starter가 있을 때만 의미가 있어
            # 없으면 single로 내린다(사용자 지시).
            return {"form": "single", "kind": "long"}, "speech_bubble(시작말 없음)->single", []
        return {"form": "speech", "starter": starter}, "speech_bubble->speech", []

    if shape == "ruled_lines":
        kind = "short" if blank_lines and blank_lines <= 1 else "long"
        return {"form": "single", "kind": kind}, "ruled_lines->single", []

    if shape == "reference_table":
        # 학생 답란이 없는 참고용 표(예: 열하일기 5-1 한자표) - qaref.ref로
        # 연결한다(vision_parse/build_normalized.py가 이 반환값을 직접 안
        # 쓰고 ref_table 전용 경로로 처리 - layout_hint_to_form은 q용 form만
        # 다루므로 여기 도달하면 "같은 페이지에 붙일 qa가 없어 독립 항목으로
        # 남은" 예외적인 경우다).
        return {"form": "reference", "rows": (table["rows"] if table else [])}, "reference_table->reference(no-answer)", []

    # unclear 및 알 수 없는 shape(null 포함) - 사용자 지시대로 single + flag
    return {"form": "single", "kind": "long"}, f"{shape}->single(flag)", []
