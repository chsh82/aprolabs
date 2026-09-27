# -*- coding: utf-8 -*-
"""L4·L5/L6 파일럿 문항(80건)의 explanation 필드에 쓰인 한글 조사(은/는,
이라는/라는, 을/를)가 받침 유무 규칙과 일치하는지 독립적으로 재검사한다.

momolib 온라인 QA(2026-09-28)에서 MF_A_SC_SRL4L5PILOT_20260925_L4_003
('집단') 문항의 "무리'이라는" 오류를 발견해 수정한 뒤 추가한 회귀
테스트다. 생성 스크립트(phase16/phase27)가 쓰는 헬퍼 함수를 그대로
신뢰하지 않고, 이 테스트 자체의 별도 구현으로 최종 산출물(JSON 파일)을
재검사한다. 아무것도 쓰지 않는다(읽기 전용).

실행:
    python tests/test_particle_agreement_pilot_items.py
"""
from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFESTS = {
    "l4l5": REPO_ROOT / "data" / "vocab" / "pilot_l4l5_manifest_v1.json",
    "l6": REPO_ROOT / "data" / "vocab" / "pilot_l6_manifest_v1.json",
}

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def has_batchim(ch: str) -> bool | None:
    code = ord(ch) - 0xAC00
    if not (0 <= code < 11172):
        return None
    return (code % 28) != 0


def particle_issue(word: str, used: str, with_batchim: str, without_batchim: str) -> str | None:
    if not word:
        return None
    b = has_batchim(word[-1])
    if b is None:
        return None
    expected = with_batchim if b else without_batchim
    if used != expected:
        return f"'{word}'+'{used}' -> 기대값 '{word}{expected}'(받침 {'있음' if b else '없음'})"
    return None


PATTERN_A = re.compile(r"'([^']+)'(는|은) '([^']+)'(이라는|라는) 뜻입니다")
PATTERN_B = re.compile(r"문장 속 '([^']+)'(은|는)? ?'([^']+)'(을|를) 뜻합니다")


def scan_explanation(pilot_key: str, item_id: str, explanation: str) -> list[str]:
    issues: list[str] = []
    m = PATTERN_A.search(explanation)
    if m:
        lemma, particle1, definition, particle2 = m.groups()
        issue1 = particle_issue(lemma, particle1, "은", "는")
        if issue1:
            issues.append(f"lemma 조사: {issue1}")
        issue2 = particle_issue(definition, particle2, "이라는", "라는")
        if issue2:
            issues.append(f"definition 조사: {issue2}")
        return issues
    m2 = PATTERN_B.search(explanation)
    if m2:
        _, _, definition2, particle3 = m2.groups()
        issue3 = particle_issue(definition2, particle3, "을", "를")
        if issue3:
            issues.append(f"문장형 정의 조사: {issue3}")
        return issues
    issues.append(f"패턴 불일치(수동 확인 필요): {explanation!r}")
    return issues


def main() -> bool:
    total_items = 0
    total_issues: list[tuple[str, str, str]] = []

    for pilot_key, path in MANIFESTS.items():
        check(path.exists(), f"{pilot_key} 매니페스트 파일 존재: {path}")
        if not path.exists():
            continue
        rows = json.loads(path.read_text(encoding="utf-8"))
        check(len(rows) == 40, f"{pilot_key} 문항 수 40건", detail=f"실제 {len(rows)}건")
        for row in rows:
            total_items += 1
            issues = scan_explanation(pilot_key, row["item_id"], row["explanation"])
            for issue in issues:
                total_issues.append((pilot_key, row["item_id"], issue))

    check(total_items == 80, "전체 문항 수 80건", detail=f"실제 {total_items}건")
    check(len(total_issues) == 0, "조사(은/는·이라는/라는·을/를) 오류 0건",
          detail=f"{len(total_issues)}건 발견: {total_issues[:10]}")

    # 이번에 고친 항목이 실제로 고쳐진 채로 남아있는지 고정 회귀 확인
    l4l5_rows = json.loads(MANIFESTS["l4l5"].read_text(encoding="utf-8"))
    target = next((r for r in l4l5_rows if r["item_id"] == "MF_A_SC_SRL4L5PILOT_20260925_L4_003"), None)
    check(target is not None, "회귀 대상 문항 MF_A_SC_SRL4L5PILOT_20260925_L4_003 존재")
    if target is not None:
        check(
            target["explanation"] == "'집단'은 '여러 사람이 모여 이룬 무리'라는 뜻입니다.",
            "MF_A_SC_SRL4L5PILOT_20260925_L4_003 조사 수정 반영 확인",
            detail=repr(target["explanation"]),
        )

    ok = all(r for r, _ in _results)
    print(f"\n{'전체 PASS' if ok else '일부 FAIL'} ({sum(1 for r,_ in _results if r)}/{len(_results)})")
    return ok


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
