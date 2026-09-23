"""level -> band, DB 분기 -> quarter key 매핑. SPEC §3.2/§3.3."""
from __future__ import annotations

import re

_QUARTER_MAP = {
    "1분기(고전)": "winter",
    "2분기(탐구)": "spring",
    "3분기(문학)": "summer",
    "4분기(인문예술)": "autumn",
}


def level_to_band(level: str) -> str:
    m = re.search(r"\d+", level or "")
    if not m:
        raise ValueError(f"level에서 숫자를 못 찾음: {level!r}")
    n = int(m.group())
    if n <= 2:
        return "lower"
    if n <= 6:
        return "elem-upper"
    return "mid"


def quarter_to_key(quarter: str) -> str:
    if quarter in _QUARTER_MAP:
        return _QUARTER_MAP[quarter]
    raise ValueError(f"알 수 없는 분기: {quarter!r} (허용값: {list(_QUARTER_MAP)})")


def level_to_grade(level: str) -> str:
    """"L2"/"L5"/"L9" -> "초등학교 2학년"/"초등학교 5학년"/"중학교 3학년".

    3종 골든 샘플의 tone.grade 값으로 역산: L1~L6는 초등 그대로, L7~L9는
    중학교 (레벨-6)학년(L7=중1, L9=중3)."""
    m = re.search(r"\d+", level or "")
    if not m:
        raise ValueError(f"level에서 숫자를 못 찾음: {level!r}")
    n = int(m.group())
    if n <= 6:
        return f"초등학교 {n}학년"
    return f"중학교 {n - 6}학년"


# SPEC §3.2 "학년대별 답란 줄 수" - 저학년만 기본값(run.py의 LINES와 같은 표)을 덮어쓴다.
TONE_LINE_MM = {"lower": 11, "elem-upper": 9.5, "mid": 8}
TONE_LINES_OVERRIDE = {"lower": {"vocab": [2, 2], "row": [1, 1]}}
