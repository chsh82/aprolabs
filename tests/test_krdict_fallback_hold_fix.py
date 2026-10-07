"""`import_textbook_vocab.py`의 krdict `definitions[0]` 고정 채택 버그 재발 방지
수정안(`resolve_definition`) 회귀 테스트.

배경(`reports/schema_reading_phase8_krdict_fallback_full_audit_20260924.md`,
`reports/literacy_repr_errors_26_verdict_20260924.csv`): 교재(momo_book.db) 원본
정의가 없어 krdict으로 폴백하는 행에서, 기존 코드
(`definition = rep.definition or entry.definitions[0]`)는 krdict에 동음이의
(같은 표제어의 서로 다른 LexicalEntry가 2개 이상)가 있거나, 매칭된 entry
자체가 다의어(Sense가 2개 이상)여도 무조건 파일에 먼저 등장한 뜻
(`definitions[0]`)을 채택했다. 실제로 이 버그가 틀린 뜻을 채운 사례가 최소
4건(유용하다·관대하다·모락모락·선구자) 확인됐다.

수정안(`resolve_definition`, `scripts/literacy/import_textbook_vocab.py`)은:
1. 원본 정의가 있으면 그대로 쓴다(회귀 없음 - 이 테스트의 test_original_definition_wins).
2. krdict 매칭이 아예 없으면 기존과 동일하게 None(보류) - 회귀 없음.
3. **동음이의(homonym_count>1)면 무조건 보류**(entry 자체의 sense가 1개뿐이라도) -
   "어느 동음이의 항목이 맞는지"는 원본 정의 없이는 판단 불가이기 때문.
4. **매칭된 단일 entry가 다의어(len(entry.definitions)>1)면 보류** - "어느 뜻인지"
   판단 불가.
5. 동음이의도 다의어도 아니면(krdict 뜻이 애초에 하나뿐) 그 뜻을 채택 - 이 경우는
   "잘못된 인덱스를 고를 가능성" 자체가 구조적으로 없으므로 안전.

이 테스트는 (a) 합성(fabricated) Entry로 4가지 경계 조건을 검증하고, (b) 실제
krdict 원본 덤프에서 이번에 실제로 문제가 됐던 표제어들(유용하다·관대하다·
모락모락·선구자·바래다)과, 문제가 없었던 표제어(기리다)를 다시 불러와 수정
전/후 동작을 직접 비교한다. **읽기 전용** - `raw/krdict/`만 읽고, 어떤 DB에도
쓰지 않는다(`data/literacy.db` 재적재 없음 - 이 테스트도 그 자체가 실제 적용이
아니라 "수정안이 옳다는 근거"를 남기기 위한 것이다).

실행:
    python tests/test_krdict_fallback_hold_fix.py
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))

from krdict_dump import Entry  # noqa: E402
import import_textbook_vocab as itv  # noqa: E402

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    _results.append((name, condition, detail))
    print(f"{_PASS if condition else _FAIL} {name}" + (f" - {detail}" if detail and not condition else ""))


def make_entry(definitions: list[str], lexical_unit: str = "단어") -> Entry:
    return Entry(
        external_id="test-id",
        headword="테스트어",
        lexical_unit=lexical_unit,
        definitions=definitions,
    )


# ---------------------------------------------------------------------------
# 1) 합성 Entry로 경계 조건 검증
# ---------------------------------------------------------------------------

def test_synthetic_cases() -> None:
    print("\n=== 1) 합성(fabricated) Entry 경계 조건 ===")

    # (1) 원본 정의가 있으면 krdict 상태와 무관하게 그대로 쓴다 (회귀 없음)
    entry_ambiguous = make_entry(["뜻1", "뜻2"])
    definition, hold_reason = itv.resolve_definition("교재 원본 정의", entry_ambiguous, homonym_count=3)
    check("원본 정의가 있으면 동음이의/다의어와 무관하게 원본을 그대로 씀",
          definition == "교재 원본 정의" and hold_reason is None,
          f"got=({definition!r}, {hold_reason!r})")

    # (2) krdict 매칭이 아예 없으면 기존과 동일하게 None, hold_reason도 None(보류
    #     사유는 이미 review_status='보류'로 처리되는 기존 경로라 새로 설명할 필요 없음)
    definition, hold_reason = itv.resolve_definition(None, None, homonym_count=0)
    check("krdict 매칭 자체가 없으면 (None, None) - 기존 동작 그대로",
          definition is None and hold_reason is None,
          f"got=({definition!r}, {hold_reason!r})")

    # (3) 동음이의(homonym_count>1) - entry 자체 sense가 1개뿐이어도 보류해야 함
    entry_single_sense = make_entry(["뜻 하나뿐"])
    definition, hold_reason = itv.resolve_definition(None, entry_single_sense, homonym_count=2)
    check("동음이의(homonym_count=2)면 entry의 sense가 1개뿐이어도 definition=None으로 보류",
          definition is None, f"got definition={definition!r}")
    check("동음이의 보류 사유 텍스트에 '동음이의' 포함",
          bool(hold_reason) and "동음이의" in hold_reason, f"got hold_reason={hold_reason!r}")

    # (4) 다의어(len(entry.definitions)>1) - 동음이의가 아니어도(homonym_count=1) 보류
    entry_polysemy = make_entry(["1번 뜻", "2번 뜻", "3번 뜻"])
    definition, hold_reason = itv.resolve_definition(None, entry_polysemy, homonym_count=1)
    check("다의어(sense 3개, homonym_count=1)면 definition=None으로 보류",
          definition is None, f"got definition={definition!r}")
    check("다의어 보류 사유 텍스트에 '다의어' 포함",
          bool(hold_reason) and "다의어" in hold_reason, f"got hold_reason={hold_reason!r}")

    # (5) 동음이의도 다의어도 아니면(뜻이 하나뿐) - 그 뜻을 채택(회귀 없음, 안전한 경우)
    entry_unambiguous = make_entry(["유일한 뜻"])
    definition, hold_reason = itv.resolve_definition(None, entry_unambiguous, homonym_count=1)
    check("동음이의 없음 + 단일 뜻이면 그 뜻을 그대로 채택(보류 아님)",
          definition == "유일한 뜻" and hold_reason is None,
          f"got=({definition!r}, {hold_reason!r})")

    # (6) entry는 있으나 definitions가 빈 리스트인 방어적 케이스(빈 리스트는 falsy)
    entry_empty = make_entry([])
    definition, hold_reason = itv.resolve_definition(None, entry_empty, homonym_count=1)
    check("entry.definitions가 빈 리스트면 (None, None)",
          definition is None and hold_reason is None,
          f"got=({definition!r}, {hold_reason!r})")


# ---------------------------------------------------------------------------
# 2) 실제 krdict 원본 덤프로 재현 - 문제였던 표제어는 보류, 문제 없던 표제어는 유지
# ---------------------------------------------------------------------------

def test_real_krdict_cases() -> None:
    print("\n=== 2) 실제 krdict 원본 덤프 재현 ===")
    xml_dir = REPO_ROOT / "raw" / "krdict" / "krdict_dump"
    if not xml_dir.exists() or not list(xml_dir.glob("*.xml")):
        check("raw/krdict/krdict_dump 존재(원본 XML 필요)", False, "덤프 없음 - 이 절 건너뜀")
        return

    index = itv.build_krdict_index()

    # (유용하다, 관대하다) = 동음이의 2건, 각 entry는 sense 1개뿐 - 기존 버그 사례
    # (모락모락, 선구자) = 동음이의 없음, 단일 entry가 다의어 - 기존 버그 사례
    # 전부 momo_book.db 원본 정의가 NULL이었던 실제 행(rep_definition=None으로 재현)
    known_buggy = ["유용하다", "관대하다", "모락모락", "선구자"]
    for hw in known_buggy:
        entry, homonym_count = itv.pick_krdict_match(index, hw)
        check(f"{hw}: krdict 매칭 존재", entry is not None, "매칭 실패 - 덤프 버전 변경 의심")
        if entry is None:
            continue
        definition, hold_reason = itv.resolve_definition(None, entry, homonym_count)
        is_ambiguous = homonym_count > 1 or len(entry.definitions) > 1
        check(f"{hw}: 실제 krdict 데이터 기준 동음이의/다의어 존재 확인(재검증)",
              is_ambiguous, f"homonym_count={homonym_count}, senses={len(entry.definitions)}")
        check(f"{hw}: 수정 후 definition=None으로 보류(기존 버그처럼 definitions[0]을 채우지 않음)",
              definition is None, f"got definition={definition!r}")
        check(f"{hw}: hold_reason이 채워짐(사람이 확인할 수 있게)", bool(hold_reason),
              f"got hold_reason={hold_reason!r}")

    # 바래다: 동음이의 3건(색바래다/배웅하다/바라다의 옛말)이 있지만 우연히 첫 채택이
    # 맞았던 케이스(기존 판정표 NO_ISSUE) - 그래도 "우연히 맞았다"는 검증 수단이 없으므로
    # 새 설계 원칙상 반드시 보류돼야 한다(안전 우선 - 사용자 명시적 설계 원칙).
    entry, homonym_count = itv.pick_krdict_match(index, "바래다")
    if entry is not None:
        definition, hold_reason = itv.resolve_definition(None, entry, homonym_count)
        check("바래다: 동음이의 3건 존재(재검증)", homonym_count > 1, f"homonym_count={homonym_count}")
        check("바래다: 기존엔 우연히 맞았지만(NO_ISSUE) 새 설계상 검증 수단이 없으므로 여전히 보류",
              definition is None, f"got definition={definition!r}")

    # 기리다: 동음이의 없음 + 단일 뜻 - 문제 없던 케이스, 회귀 없이 여전히 값을 채워야 함
    entry, homonym_count = itv.pick_krdict_match(index, "기리다")
    if entry is not None:
        definition, hold_reason = itv.resolve_definition(None, entry, homonym_count)
        check("기리다: 동음이의 없음(재검증)", homonym_count <= 1, f"homonym_count={homonym_count}")
        check("기리다: 다의어 아님(재검증, sense 1개)", len(entry.definitions) <= 1,
              f"senses={len(entry.definitions)}")
        check("기리다: 회귀 없이 정상적으로 뜻을 채움(보류 아님)",
              definition == entry.definitions[0] and hold_reason is None,
              f"got=({definition!r}, {hold_reason!r})")


def main() -> int:
    test_synthetic_cases()
    test_real_krdict_cases()

    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_fail = len(_results) - n_pass
    print(f"\n{n_pass}/{len(_results)} passed" + (f", {n_fail} FAILED" if n_fail else ""))
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
