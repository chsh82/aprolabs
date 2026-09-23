"""② 조판 규칙 엔진의 결정론적 규칙 - SPEC §3, 사용자 지시(2026-09-23) 1~2번.

여기 있는 함수는 전부 순수 함수다(정규화 레코드 필드만 보고 계산, side effect
없음) - 그래서 tests/test_layout_rules.py에서 개별적으로 단위 테스트한다.
LLM 보조가 필요한 부분(하위 질문 파생, 이미지 지시문, 배경지식 원문 등)은
여기서 다루지 않는다 - step1/step2/step3.py가 그 자리에 Flag를 남긴다.
"""
from __future__ import annotations

import re

from normalize.models import Flag

# ---------- 3.6 캐릭터 매핑 (저·고학년 전용 - 중학생은 라벨만) ----------
# SPEC: 사실적->홈즈, 분석적->아로낙스, 추론적->지킬, 적용적->포그, 글쓰기·생각상자->앤,
# 저학년 낱말->도로시. reading_type이 여러 개를 "/"로 묶고 있으면 이 순서로 하나만 고른다
# (사실적을 가장 먼저 보는 것은 §3.6 나열 순서를 그대로 따른 것 - 골든에서 검증 가능한
# 조합은 전부 이 순서와 일치했다. "비판적"은 §3.6 표에 없는 유형이라 매핑 대상이 아니다).
_CHARACTER_BY_READING_TYPE = [
    ("사실적", "holmes"),
    ("분석적", "aronnax"),
    ("추론적", "jekyll"),
    ("적용적", "fogg"),
]
DEFAULT_CHARACTER = "jekyll"  # reading_type이 없거나(예: "비판적"만) 매핑 대상이 아닐 때
VOCAB_CHARACTER = "holmes"
WRITING_CHARACTER = "anne"
LOWER_VOCAB_CHARACTER = "dorothy"


def character_for_reading_type(reading_type: str | None) -> tuple[str, bool]:
    """(캐릭터 자산 키, 규칙만으로 확정됐는지) - reading_type이 없으면 기본값을
    쓰고 False를 돌려줘서 호출자가 flag를 달게 한다."""
    if reading_type:
        for name, char in _CHARACTER_BY_READING_TYPE:
            if name in reading_type:
                return char, True
    return DEFAULT_CHARACTER, False


def reading_type_label(reading_type: str | None) -> tuple[str, bool]:
    """DB의 "추론적/비판적" -> 화면 표시용 "추론적 · 비판적 독해". reading_type이
    비어 있으면(중등 unknown-행 재조립으로 유실된 경우가 실제로 있다 - L9 order_no=3)
    빈 라벨을 돌려주고 False로 표시한다(LLM/검수 보완 대상)."""
    if reading_type:
        return reading_type.replace("/", " · ") + " 독해", True
    return "독해", False


# ---------- 3.4 STEP2: 제시문 길이/구조에 따른 페이지 유형 ----------
EXCERPT_BAND_MAX = 120   # 미만 -> qaband
EXCERPT_WIDE_MIN = 250   # 이상 -> qa, ratio 3:2


def excerpt_page_type(excerpt_text: str | None, has_ref_table: bool) -> tuple[str, dict]:
    """(page type, 추가 page 필드) - SPEC §3.4 "제시문 120자 미만 -> qaband(위 띠).
    긴 제시문(250자 이상) -> qa 3:2"를 그대로 규칙화하고, 중간 길이는 qa 1:1로 둔다.
    excerpt_text가 비어 있으면(중등 제시문이 질문 안에 아직 안 갈라진 경우, ①단계의
    문서화된 경계) 별도 excerpt 상자를 만들 수 없어 "solo"로 내려간다 - 이건 이
    함수가 아니라 호출자가 판단한다(제시문 유무는 이 함수 책임 밖)."""
    n = len(excerpt_text or "")
    if has_ref_table:
        return "qaref", {}
    if n < EXCERPT_BAND_MAX:
        return "qaband", {}
    if n >= EXCERPT_WIDE_MIN:
        return "qa", {"ratio": "3fr 2fr"}
    return "qa", {"ratio": "1fr 1fr"}


# ---------- 2.2 ui_type -> form/kind 매핑 ----------
def form_for_ui_type(ui_type: str, ui_config: dict, question_text: str) -> tuple[dict, list[Flag]]:
    """discussion_qa.ui_type -> layout question(q) 필드 일부. (q_fields, flags)를
    돌려준다 - q_fields는 question()/widget()가 그대로 쓸 kind/form/blanks/starter 등.

    ui_config에 이미 있는 값(starter, blanks 라벨)은 그대로 쓰고, DB에 없는 값
    (blanks의 개별 prompt, table의 rows)은 원본 question_text를 임시로 채우고
    derived 플래그를 남긴다(사용자 지시 3단계 원칙 1: "LLM 보조 - 복합 질문의
    하위 질문 파생")."""
    flags: list[Flag] = []
    if ui_type == "text_long":
        return {"kind": "long"}, flags
    if ui_type == "text_short":
        starter = (ui_config or {}).get("starter") or ""
        fields = {"kind": "short"}
        if starter:
            fields["starter"] = starter
        return fields, flags
    if ui_type == "text_short_multi":
        labels = (ui_config or {}).get("blanks") or []
        blanks = [{"label": label, "prompt": question_text} for label in labels]
        flags.append(Flag(kind="derived",
                           message=f"blanks {len(labels)}개의 개별 prompt(하위 질문)를 "
                                   f"question_text 전체로 임시 채움 - LLM으로 쪼개야 함"))
        return {"kind": "multi", "blanks": blanks}, flags
    if ui_type == "table_compare":
        flags.append(Flag(kind="derived", category="placeholder",
                           message="table_compare 행 구성이 DB에 없음(ui_config 비어있음) - "
                                   "LLM으로 비교 축을 나누고 rows[{label,prompt}]를 채워야 함"))
        return {"form": "table", "rows": [{"label": "", "prompt": question_text}]}, flags
    if ui_type == "choice_multi":
        # 사용자 지시(2026-09-23, "choice_multi 구현") 실태 조사: 71건 중 70건이
        # ui_config.options에 선택지 2~5개를 갖고 있다(1건만 비어있음). "이유 칸"이
        # 필요한지 질문 문구로 구분되는 규칙을 찾으려 했지만(그 이유는/이유에 대해
        # 말해보세요 등) 있어야 할 곳과 없어야 할 곳 둘 다에서 같은 표현이 나와
        # 신뢰할 수 있는 규칙이 안 나왔다 - 사용자 지시대로 이유 칸 없이 생성하고
        # 검수에서 필요하면 추가하는 쪽으로 간다.
        options = (ui_config or {}).get("options") or []
        if not options:
            flags.append(Flag(kind="missing", category="placeholder",
                               message="choice_multi 선택지가 DB에 없음(ui_config 비어있음) - "
                                       "검수에서 채워야 함"))
        return {"form": "choiceList", "options": options, "single": True}, flags
    if ui_type == "choice_ab":
        # 사용자 지시(2026-09-23, "choice_ab 위젯 구현") 실태 조사: 178건 전부
        # ui_config.options에 선택지 2개가 그대로 들어 있음(text_short_multi의
        # blanks처럼 DB 신호가 확실함) - LLM 없이 바로 규칙으로 채운다.
        options = (ui_config or {}).get("options") or []
        if len(options) == 2:
            cards = [{"title": opt} for opt in options]
        else:
            cards = [{"title": ""}, {"title": ""}]
            flags.append(Flag(kind="missing", category="placeholder",
                               message=f"choice_ab 선택지가 2개가 아님(실제 {len(options)}개) - "
                                       f"카드 머리를 비워둠, 검수에서 채워야 함"))
        return {"form": "choice", "cards": cards}, flags
    raise ValueError(f"지원하지 않는 ui_type: {ui_type!r}")


# 렌더러에 대응 위젯이 아직 없는 ui_type(§2.2에 없는 타입) - 지금은 없다.
# choice_ab(양자택일, 178건)는 "choice" 위젯, choice_multi(N지선다, 71건)는
# "choiceList" 위젯으로 둘 다 처리한다(2026-09-23 지시). 앞으로 새 ui_type이
# 나오면 여기 추가한다 - form_for_ui_type이 모르는 타입에 대해 예외를 던지는
# 게 아니라, 호출부(step2.py)가 이 목록으로 먼저 걸러서 안전하게 건너뛰고
# flag만 남긴다.
UNSUPPORTED_UI_TYPES: set[str] = set()


# ---------- 2. part가 둘 이상이면 나눈다 (사용자 지시 2번 마지막 항목) ----------
def needs_multi_part(ui_type: str, ui_config: dict) -> bool:
    if ui_type == "text_short_multi":
        return len(((ui_config or {}).get("blanks")) or []) >= 2
    return False


# ---------- STEP3: writing_guide 안의 대화문을 dialog[] 줄로 쪼갠다 ----------
def split_dialog_lines(lead: str | None) -> list[str]:
    """essay.lead(=DB writing_guide)는 대화 인용문이 있으면 원본 PDF에서부터
    이미 줄바꿈으로 나뉘어 있다(실제로 긴긴밤/야옹아 essay_prompt.writing_guide로
    확인함) - 그 줄바꿈 그대로를 dialog[] 배열로 쓴다. 인용부호(" ")가 없는 줄(=
    대화문이 아니라 그냥 안내문)만 있으면 원본 전체를 한 줄로 돌려준다."""
    if not lead:
        return []
    lines = [ln.strip() for ln in lead.split("\n") if ln.strip()]
    quoted = [ln for ln in lines if "“" in ln or "”" in ln]
    return quoted if quoted else lines


# ---------- 사용자 지시 2번: 비교형/목록형 일반 규칙 ----------
def symmetric_compare_cards(cards: list[dict]) -> list[dict]:
    """비교형(compare) 카드 중 하나라도 번호 칸(n)이 있으면 나머지도 같은 n으로
    맞춘다(양쪽 다 "3가지 이유" - 야옹아 1번). 어느 카드에도 n이 없으면 그대로 둔다."""
    ns = [c["n"] for c in cards if c.get("n")]
    if not ns:
        return cards
    n = max(ns)
    return [{**c, "n": n} for c in cards]


def list_row_kind(item_count: int) -> str:
    """번호 목록형(list)에서 항목이 3개 이하면 rowTall을 쓴다(야옹아 2-1 사례)."""
    return "rowTall" if item_count <= 3 else "row"
