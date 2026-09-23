"""sajaseongeo_parser.py `_DATE_LEAD_RE` 수정안 회귀 테스트.

배경(`reports/schema_reading_phase5_ai_level_audit_and_holds_20260924.md` 5-4절,
`reports/schema_reading_phase6_ai_level_full_audit_*.md` 2-1절): PDF2(`수능_언어영역_성어.pdf`)
의 "1994학년도 1차/2차 수능" 표기는 원래 `_DATE_LEAD_RE`가 인식하는 두 패턴
(`YYYY학년도 예비 시행...`, `YYYY.(수능|N)...`) 어디에도 걸리지 않아, 날짜 스트리핑 루프가
그 토큰에서 멈추고 뒤따르는 실제 정의 텍스트까지 `definition` 필드에 남는 오염이 있었다
(동병상련·사필귀정·상전벽해·새옹지마·설상가상·연목구어·유유상종·이열치열·전화위복·초록동색,
정확히 10건).

이 테스트는 (a) 수정된 정규식이 이 10건을 올바르게 처리하는지(정의에서 날짜 토큰 제거,
`dates`에 포함), (b) 수정이 나머지 188건(198건 - 10건)의 파싱 결과를 전혀 바꾸지 않는지
(byte-identical) 를 "구버전 정규식으로 만든 기준선"과 "현재 모듈의 정규식"을 직접 비교해
검증한다 - 별도 스냅샷 파일 없이 이 테스트만으로 회귀를 판정할 수 있다.

**읽기 전용**: `raw/sajaseongeo/수능_언어영역_성어.pdf`를 읽기만 하고, 어떤 DB에도
쓰지 않는다(`data/literacy.db` 재적재 없음 - 이 테스트는 그 자체가 "실제 적용"이 아니라
"수정안이 옳다는 근거"를 남기기 위한 것이다).

실행:
    python tests/test_sajaseongeo_date_lead_fix.py
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))

import sajaseongeo_parser as sp  # noqa: E402

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    _results.append((name, condition, detail))
    print(f"{_PASS if condition else _FAIL} {name}" + (f" - {detail}" if detail and not condition else ""))


# phase5/phase6가 전수 확인한 오염 10건 (헤드워드 기준, 한글 부분만)
EXPECTED_CONTAMINATED_HEADWORDS_KO = {
    "동병상련", "사필귀정", "상전벽해", "새옹지마", "설상가상",
    "연목구어", "유유상종", "이열치열", "전화위복", "초록동색",
}

_TRAILING_NON_HANGUL_RE = re.compile(r"[가-힣]+")


def _hangul_prefix(headword: str) -> str:
    """헤드워드에서 맨 앞 한글 연속 구간만 뽑는다.

    "유유상종"은 PDF2 추출 과정에서 한자 분할 인덱스가 어긋나 headword에 한자가
    한 번 더 딸려 나오는(예: "유유상종類類") 기존의 별개 버그가 있다(이번 날짜
    정규식 수정과 무관 - 범위 밖). 비교 시 이 잡음을 걷어내기 위한 헬퍼.
    """
    m = _TRAILING_NON_HANGUL_RE.match(headword)
    return m.group(0) if m else headword

# 수정 전 정규식(phase5가 원문 그대로 인용한 버전) - 이 테스트 안에서만 재현하고,
# 실제 모듈은 건드리지 않는다(모듈은 이미 수정된 버전을 그대로 씀).
_OLD_DATE_TOKEN = (
    r"\d{4}학년도\s*예비\s*시행(?:\s*[A-Z]형(?:\s*,\s*[A-Z]형)*)?"
    r"|\d{4}\.(?:수능|\d{1,2})(?:\s*[A-Z]형(?:\s*,\s*[A-Z]형)*)?"
)
_OLD_DATE_LEAD_RE = re.compile(r"^\s*(" + _OLD_DATE_TOKEN + r")\s*(?:/\s*)?")


def _parse_with(date_lead_re: re.Pattern) -> dict[str, sp.Pdf2Entry]:
    """sp._DATE_LEAD_RE를 주어진 패턴으로 일시적으로 바꿔서 PDF2를 파싱한다."""
    original = sp._DATE_LEAD_RE
    sp._DATE_LEAD_RE = date_lead_re
    try:
        entries = sp.iter_pdf2_entries()
    finally:
        sp._DATE_LEAD_RE = original
    return {e.headword: e for e in entries}


def main() -> int:
    old_by_hw = _parse_with(_OLD_DATE_LEAD_RE)
    new_by_hw = _parse_with(sp._DATE_LEAD_RE)  # 모듈에 이미 적용된 수정본

    check("PDF2 파싱 결과 건수는 수정 전후 동일", len(old_by_hw) == len(new_by_hw),
          f"old={len(old_by_hw)} new={len(new_by_hw)}")

    old_contaminated = {hw for hw, e in old_by_hw.items() if "학년도" in e.definition}
    old_contaminated_ko = {_hangul_prefix(hw) for hw in old_contaminated}
    check("수정 전(구 정규식) 재현: 오염 10건 재현됨",
          old_contaminated_ko == EXPECTED_CONTAMINATED_HEADWORDS_KO and len(old_contaminated) == 10,
          f"got={sorted(old_contaminated)}")

    new_contaminated = {hw for hw, e in new_by_hw.items() if "학년도" in e.definition}
    check("수정 후: definition에 '학년도'가 남은 항목 0건", len(new_contaminated) == 0,
          f"still contaminated={sorted(new_contaminated)}")

    # (a) 10건 각각이 올바르게 처리됐는지: 날짜 토큰이 dates로 옮겨가고, definition은
    # 더 짧아지되(날짜 접두어 제거) 실제 정의 본문은 그대로 보존돼야 한다.
    for hw in sorted(old_contaminated):
        old_e, new_e = old_by_hw[hw], new_by_hw[hw]
        check(f"{hw}: 수정 후 definition이 '학년도'로 시작하지 않음",
              not new_e.definition.startswith(("1994", "199")) and "학년도" not in new_e.definition,
              new_e.definition[:40])
        check(f"{hw}: 수정 후 dates에 1994학년도 항목이 추가됨",
              any("1994학년도" in d for d in new_e.dates),
              f"dates={new_e.dates}")
        check(f"{hw}: 수정 후 definition이 구버전 definition의 접미부와 일치(정의 본문 보존)",
              old_e.definition.endswith(new_e.definition) or new_e.definition in old_e.definition,
              f"old={old_e.definition[:40]!r} new={new_e.definition[:40]!r}")

    # (b) 나머지(오염되지 않았던) 항목은 완전히 byte-identical해야 한다 - 회귀 없음 증명.
    untouched = set(old_by_hw) - old_contaminated
    diffs = []
    for hw in untouched:
        old_e, new_e = old_by_hw[hw], new_by_hw.get(hw)
        if new_e is None or old_e.definition != new_e.definition or old_e.dates != new_e.dates:
            diffs.append(hw)
    check(f"수정과 무관한 나머지 {len(untouched)}건은 definition/dates가 완전히 동일(회귀 없음)",
          len(diffs) == 0, f"바뀐 항목={diffs[:10]}")

    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_fail = len(_results) - n_pass
    print(f"\n{n_pass}/{len(_results)} passed" + (f", {n_fail} FAILED" if n_fail else ""))
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
