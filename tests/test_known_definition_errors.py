# -*- coding: utf-8 -*-
"""phase34 - 명목GDP(5800) 정의 오류가 파일 기반 제외 목록에 올바르게
등록돼 향후 자동 초안 생성·적재 후보 선택에서 제외되는지 확인한다.
DB에는 쓰지 않는다(literacy.db definition 값은 이 테스트가 건드리지 않음).

실행:
    python tests/test_known_definition_errors.py
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts" / "vocab"))

from known_definition_errors import load_excluded_ids, load_registry, is_known_error  # noqa: E402

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def main() -> bool:
    rows = load_registry()
    check(len(rows) >= 1, f"등록부에 최소 1건 존재(실제 {len(rows)}건)")

    row_5800 = next((r for r in rows if r["term_id"] == "5800"), None)
    check(row_5800 is not None, "명목GDP(5800)가 등록부에 존재")
    if row_5800:
        check(row_5800["error_type"] == "DEFINITION_INVERSION",
              f"오류 유형이 DEFINITION_INVERSION(실제 {row_5800['error_type']})")
        check(row_5800["resolution_status"] == "UNRESOLVED_AWAITING_DRY_RUN",
              "해결 상태가 UNRESOLVED_AWAITING_DRY_RUN(별도 dry-run 전까지 DB 미수정을 구조적으로 보장)")

    check(is_known_error("5800"), "is_known_error('5800') == True")
    check(not is_known_error("6904"), "무관한 term_id(6904, 엑손)는 등록되지 않음")

    excluded = load_excluded_ids()
    check("5800" in excluded, "load_excluded_ids()에 5800 포함(자동 초안·적재 후보 제외 대상)")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
