"""layout/rules.py 순수 함수 단위 테스트 - 사용자 지시(2026-09-23) 2단계 원칙.

실행: python tests/test_layout_rules.py
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from layout.rules import (  # noqa: E402
    character_for_reading_type,
    excerpt_page_type,
    form_for_ui_type,
    list_row_kind,
    reading_type_label,
    split_dialog_lines,
    symmetric_compare_cards,
)

_PASS, _FAIL = "[PASS]", "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def run() -> bool:
    # ---------- 캐릭터/독해유형 라벨 ----------
    check(character_for_reading_type("추론적")[0] == "jekyll", "추론적 -> 지킬 박사")
    check(character_for_reading_type("분석적/적용적")[0] == "aronnax", "분석적/적용적 -> 아로낙스(사실적>분석적>추론적>적용적 순)")
    check(character_for_reading_type("적용적")[0] == "fogg", "적용적 -> 포그")
    check(character_for_reading_type("사실적")[0] == "holmes", "사실적 -> 홈즈")
    char, ok = character_for_reading_type("비판적")
    check(ok is False, "비판적만 있으면(§3.6 표에 없음) 매핑 실패로 표시")
    check(character_for_reading_type(None)[1] is False, "reading_type 없으면 매핑 실패로 표시")

    label, ok = reading_type_label("추론적/비판적")
    check(label == "추론적 · 비판적 독해" and ok, "'/' 구분 -> ' · ' 구분 + '독해' 접미(골든 L9 문항5 표기와 일치)")
    check(reading_type_label(None)[1] is False, "reading_type 없으면 라벨도 실패로 표시")

    # ---------- 제시문 길이 -> 페이지 유형(SPEC §3.4) ----------
    check(excerpt_page_type("가" * 50, False) == ("qaband", {}), "120자 미만 -> qaband")
    check(excerpt_page_type("가" * 119, False) == ("qaband", {}), "경계값 119자 -> qaband")
    check(excerpt_page_type("가" * 120, False)[0] == "qa", "경계값 120자부터 qa")
    check(excerpt_page_type("가" * 200, False) == ("qa", {"ratio": "1fr 1fr"}), "120~249자 -> qa 1:1")
    check(excerpt_page_type("가" * 250, False) == ("qa", {"ratio": "3fr 2fr"}), "250자 이상 -> qa 3:2")
    check(excerpt_page_type("가" * 30, True) == ("qaref", {}), "참고표가 있으면 길이와 무관하게 qaref")

    # ---------- ui_type -> form (2.2, DB ui_config 실측 기반) ----------
    fields, flags = form_for_ui_type("text_long", {}, "질문")
    check(fields == {"kind": "long"} and not flags, "text_long -> kind=long, 플래그 없음")

    fields, flags = form_for_ui_type("text_short", {"starter": "이유:"}, "질문")
    check(fields == {"kind": "short", "starter": "이유:"}, "text_short + starter 있음 -> starter 필드 포함")
    fields, _ = form_for_ui_type("text_short", {"starter": ""}, "질문")
    check("starter" not in fields, "text_short + starter 빈 문자열 -> starter 필드 생략(낱말 답란처럼 안내문 없음)")

    fields, flags = form_for_ui_type("text_short_multi", {"blanks": ["단점", "경험"]}, "전체 질문 텍스트")
    check(fields["kind"] == "multi", "text_short_multi -> kind=multi")
    check([b["label"] for b in fields["blanks"]] == ["단점", "경험"], "blanks 라벨은 ui_config 그대로(DB 실측: L9 문항3/8과 정확히 일치)")
    check(len(flags) == 1 and flags[0].kind == "derived", "blanks의 개별 prompt는 LLM 파생 대상이라 derived 플래그 1건")

    fields, flags = form_for_ui_type("table_compare", {}, "질문")
    check(fields["form"] == "table" and len(flags) == 1 and flags[0].kind == "derived",
          "table_compare는 ui_config가 비어 있어(DB 실측) rows를 못 채우고 derived 플래그")

    # ---------- STEP3 대화문 분리 ----------
    lead = "“저기 지평선이 보여?”\n“그러면 나도 있을게요.”\n\n안내문(따옴표 없음)"
    check(split_dialog_lines(lead) == ["“저기 지평선이 보여?”", "“그러면 나도 있을게요.”"],
          "따옴표로 감싼 줄만 dialog로 뽑음(긴긴밤 essay_prompt.writing_guide 실제 줄바꿈 패턴)")
    check(split_dialog_lines("따옴표 없는 안내문") == ["따옴표 없는 안내문"], "따옴표 있는 줄이 하나도 없으면 원문 전체를 한 줄로")
    check(split_dialog_lines(None) == [], "lead가 없으면 빈 배열")

    # ---------- 사용자 지시 2번: 비교형/목록형 일반 규칙 ----------
    cards = [{"title": "찬성", "n": 3}, {"title": "반대"}]
    fixed = symmetric_compare_cards(cards)
    check(fixed[1]["n"] == 3, "비교형: 한쪽에 번호 칸(n)이 있으면 반대쪽도 같은 n으로 맞춤(야옹아 1번 사례)")
    check(symmetric_compare_cards([{"title": "A"}, {"title": "B"}]) == [{"title": "A"}, {"title": "B"}],
          "양쪽 다 번호 칸이 없으면 그대로 둠")

    check(list_row_kind(3) == "rowTall", "번호 목록형: 항목 3개 -> rowTall")
    check(list_row_kind(2) == "rowTall", "번호 목록형: 항목 2개 -> rowTall")
    check(list_row_kind(4) == "row", "번호 목록형: 항목 4개부터 row(rowTall 아님)")

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
