# -*- coding: utf-8 -*-
"""Phase22 - "정기"(SR_L4CORE_4786) 정의 회귀 테스트.

배경: phase16(2026-09-25)의 문항 생성 스크립트
(scripts/vocab/phase16_build_quiz_pilot_dryrun.py)가 L4 표제어 목록을 소스에
하드코딩해두고 vocabulary_contents.student_definition을 매번 다시 읽지 않아,
"정기" 항목의 정의 문구가 문항 텍스트와 콘텐츠 레코드 사이에서 벌어지는
사고(phase17/20/21)가 있었다. phase21이 원인을 규명하고 스크립트의 하드코딩
문자열 자체는 이미 새 값으로 고쳐져 있음을 확인했으며, phase22가 그에 맞춰
vocabulary_contents.student_definition도 동일한 새 값으로 갱신했다
(scripts/vocab/phase22_apply_jeonggi_student_definition_fix.py).

이 테스트는 향후 누군가 phase16 스크립트를 실수로 구버전 문구로 되돌리는
회귀를 잡기 위한 것이다. 두 가지를 검증한다:
  1) phase16 스크립트를 재실행(build_items만 호출, main()의 파일 출력은
     건드리지 않음)해도 "정기" 문항의 explanation/options에 항상 새 값
     ("기한이나 기간이 일정하게 정해져 있는 것")이 나오고, 구버전 문구
     ("일정한 기간마다 되풀이하도록 정한 것")는 전혀 등장하지 않는지.
  2) 구버전 문구 문자열이 scripts/, app/ 소스 코드 어디에도 남아있지 않은지
     (reports/, data/import/의 과거 기록용 파일은 의도적으로 역사적 사실을
     보존해야 하므로 이 스캔 대상에서 제외한다).

pytest 없이 이 저장소 관례([PASS]/[FAIL] 출력)를 따른다.

실행:
    python tests/test_phase22_jeonggi_definition_regression.py
"""
from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

TARGET_CONTENT_ID = "SR_L4CORE_4786"
OLD_VALUE = "일정한 기간마다 되풀이하도록 정한 것"
NEW_VALUE = "기한이나 기간이 일정하게 정해져 있는 것"

PHASE16_SCRIPT = REPO_ROOT / "scripts" / "vocab" / "phase16_build_quiz_pilot_dryrun.py"

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def _load_phase16_module():
    spec = importlib.util.spec_from_file_location("phase16_build_quiz_pilot_dryrun", PHASE16_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)  # 모듈 최상위 코드만 실행됨 - main()은 __main__ 가드 안이라 호출 안 됨
    return mod


def main() -> int:
    check(PHASE16_SCRIPT.exists(), f"phase16 스크립트 존재: {PHASE16_SCRIPT}")

    # ---- (1) 소스 코드 자체에 구버전 문구가 없는지 ----
    source_text = PHASE16_SCRIPT.read_text(encoding="utf-8")
    check(OLD_VALUE not in source_text, "phase16 스크립트 소스에 구버전 문구가 전혀 없음")
    check(NEW_VALUE in source_text, "phase16 스크립트 소스에 새 값이 존재함")

    # ---- (2) 스크립트를 실제로 재실행(build_items)해서 "정기" 문항 재생성 ----
    mod = _load_phase16_module()
    l4_entry = next((row for row in mod.L4 if row[1] == TARGET_CONTENT_ID), None)
    check(l4_entry is not None, f"L4 하드코딩 목록에서 {TARGET_CONTENT_ID}('정기') 항목을 찾음")
    if l4_entry is not None:
        check(l4_entry[4] == NEW_VALUE, f"L4 튜플의 definition 필드가 새 값과 정확히 일치 (실제={l4_entry[4]!r})")

    items = mod.build_items(mod.L4, "L4") + mod.build_items(mod.L5, "L5")
    jeonggi_items = [it for it in items if it["source_content_id"] == TARGET_CONTENT_ID]
    check(len(jeonggi_items) == 2, f"재실행 결과 '정기' 관련 문항 정확히 2건 생성 (실제 {len(jeonggi_items)}건)")

    for it in jeonggi_items:
        item_id = it["item_id"]
        check(NEW_VALUE in it["explanation"], f"{item_id}: explanation에 새 값 포함")
        check(OLD_VALUE not in it["explanation"], f"{item_id}: explanation에 구버전 문구 없음")
        check(NEW_VALUE in it["options_json"], f"{item_id}: options_json에 새 값 포함")
        check(OLD_VALUE not in it["options_json"], f"{item_id}: options_json에 구버전 문구 없음")

    # ---- (3) 자동검증(validate)도 이 두 문항을 PASS로 판정하는지(회귀로 HOLD가 되지 않았는지) ----
    val = mod.validate(items)
    val_by_id = {v["item_id"]: v for v in val}
    for it in jeonggi_items:
        v = val_by_id[it["item_id"]]
        check(v["status"] == "PASS", f"{it['item_id']}: 스크립트 자체 자동검증 PASS (실제={v['status']}, issues={v['issues']})")

    # ---- (4) 구버전 문구가 scripts/, app/ 소스 코드 어디에도 "실수로" 남아있지 않은지 ----
    # 예외: phase22 적용 스크립트 자체는 하드가드(GATE 3)가 "현재 값이 정확히
    # 구버전 문구와 일치하는지"를 검사해야 하므로 OLD_VALUE를 상수로 의도적으로
    # 포함한다(멱등성 로직의 필수 요소이지 회귀가 아님). 이 테스트 파일 자체도
    # OLD_VALUE를 상수로 갖고 있으므로 함께 제외한다. phase22_update_verdicts.py도
    # 판정표 reason 컬럼에 "구버전 -> 신버전" 변경 이력을 서술하는 감사 로그 문자열
    # 안에서만 구버전 문구를 인용한다(실행 로직이 이 값을 다시 만들어내지 않음 -
    # 순수 서술용 상수라 회귀가 아니다. 처음 이 테스트를 작성했을 때 이 파일이
    # 누락돼 있던 걸 호출 세션이 재실행 중 직접 발견해 여기 추가함).
    ALLOWED_OLD_VALUE_FILES = {
        "scripts/vocab/phase22_apply_jeonggi_student_definition_fix.py",
        "tests/test_phase22_jeonggi_definition_regression.py",
        "scripts/vocab/phase22_update_verdicts.py",
    }
    offenders = []
    for base in (REPO_ROOT / "scripts", REPO_ROOT / "app", REPO_ROOT / "tests"):
        if not base.exists():
            continue
        for py_file in base.rglob("*.py"):
            rel = str(py_file.relative_to(REPO_ROOT)).replace("\\", "/")
            if rel in ALLOWED_OLD_VALUE_FILES:
                continue
            try:
                text = py_file.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            if OLD_VALUE in text:
                offenders.append(rel)
    check(not offenders, f"scripts/, app/, tests/ 어디에도 구버전 문구가 '의도치 않게' 남아있지 않음 "
                          f"(허용된 예외 {sorted(ALLOWED_OLD_VALUE_FILES)} 제외, 발견된 파일: {offenders})")

    return 0


if __name__ == "__main__":
    exit_code = main()
    total = len(_results)
    failed = [label for ok, label in _results if not ok]
    print(f"\n총 {total}건 중 실패 {len(failed)}건")
    sys.exit(1 if failed else exit_code)
