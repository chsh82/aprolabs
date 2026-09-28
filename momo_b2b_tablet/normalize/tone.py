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
#
# 2026-09-27 사용자 지시(1차) - 종이 인쇄 기준으로 잡았던 값이 화면 필기에는 좁을
# 수 있어 실측했다. 1차 태블릿 테스트(은비야=야옹아 교재, 저학년, 실제 학생
# 필기 4건)의 ink_json 좌표를 획 단위로 측정한 결과, 개별 획 높이가 최대
# 12.7mm까지 나왔다(기존 lower=11mm보다 큼 - 실제로 두 줄짜리 답이 서로
# 겹쳐 보이는 이미지로도 확인됨). 그래서 lower를 11 -> 14mm로 올렸다.
#
# 2026-09-27 사용자 지시(2차, 인식 정확도 조사 도중) - 위 필기 4건을 "답 하나의
# 전체 필기 y범위"(모든 획 통틀어)로 다시 재보니: 한 줄짜리 답 2건은 11.3~
# 11.7mm(1차에서 잰 "개별 획" 12.7mm와 같은 범위, 서로 다른 측정 방식으로도
# 앞뒤가 맞음). 그런데 "긴 서술형"(form=single/kind=long, LINES.long=[2,4]
# 최소 2줄) 답 1건은 실제로 2줄에 걸쳐 총 30.4mm를 썼다 - **이 필기가 저장된
# edition(102)의 layout_json에 baked된 tone.line은 11(1차 변경 전 값)**이라
# 당시 실제 박스 높이는 2*11=22mm였고 8.4mm나 넘쳤던 것(현재 14mm로
# 재생성하면 2*14=28mm라 넘침이 2.4mm로 줄지만 완전히는 안 없어짐). 즉 "긴
# 서술형" 한 줄에 실제로 필요한 높이는 대략 15.2mm(=30.4/2)로, 지금 14mm로도
# 아직 살짝 부족하다 - lower를 14 -> 15mm로 한 단계 더 올린다.
#
# **미검증 상태로 남김(중요)**: 1차 변경 때 14mm로 인해 memo/cell 같은 고정
# 줄 수 위젯이 페이지 밖으로 넘쳤던 걸 "야옹아" 재생성 + 브라우저 확인으로
# 잡았는데(아래 TONE_LINES_OVERRIDE 주석 참고), 15mm로 다시 올리면 그 여유가
# 더 줄어들 수 있다 - 이번엔 그 재확인을 안 했다(브라우저 재생성·오버플로
# 검사 필요, 다음에 이어갈 때 제일 먼저 할 것).
#
# 이 측정 자체도 어른이 빠르게 흘려 쓴 필기 기준이라(사용자가 원본 이미지를
# 직접 보고 판단 - "심통났구나"는 답을 알고 봐야 읽히고 1#1은 판독 불가
# 수준) 그대로 믿을 수 없다 - 획 두께(renderer.js INK_BASE_WIDTH)·답란
# 확대 기능을 먼저 적용한 뒤, 실제 학생(초2·초5·중학생 각 5명 이상) 필기로
# 다시 수집해서 최종 확정할 것(사용자 지시).
#
# elem-upper/mid(고학년/중등)는 아직 실제 학생 필기가 하나도 수집되지 않아
# (1차 테스트가 저학년 교재만 다룸) 값을 바꾸지 않았다 - 근거 없이 비례
# 추정하면 오히려 틀릴 수 있어(중등이 저학년보다 꼭 작게 쓴다는 보장이 없음)
# 그대로 두고, 다음 태블릿 테스트에서 고학년/중등 샘플이 모이면 다시 잰다.
TONE_LINE_MM = {"lower": 15, "elem-upper": 9.5, "mid": 8}
# 줄 간격을 14mm로 올리면서(위 설명) 고정 줄 수 위젯(memo 3줄, table cell
# 2~4줄)이 실제로 페이지 밖으로 넘치는 걸 야옹아 재생성 후 확인했다(memos
# 페이지 3행×3줄에서 실측 28mm 초과, 이미지 슬롯과 좁은 표를 나란히 쓰는
# qa 페이지에서 실측 21mm 초과 - 브라우저로 직접 확인). 세 줄 쓰던 memo를
# 두 줄로, 표 칸의 최소 줄 수를 2에서 1로 낮춰 다시 확인해 넘침을 없앴다.
TONE_LINES_OVERRIDE = {"lower": {"vocab": [2, 2], "row": [1, 1], "memo": [2, 2], "cell": [1, 3]}}
