"""
momoai_web 활성 하크니스 반의 course_name을 파싱해 class_code 후보를 생성한다.

CLAUDE.md 7절 규칙:
    class_code = H-{강사이니셜}-{요일3자}{HHMM}
    - 강사이니셜: 성을 뺀 이름 부분의 각 음절 로마자 표기 첫 글자
      (예: 윤영기 -> 이름부분 "영기" -> 영=Y, 기=G -> "YG")
    - 요일3자: 영문 요일 약어 (토=SAT, 금=FRI, 일=SUN 등)
    - HHMM: 24시간제 4자리

이 스크립트는 **목록만 출력한다. class/class_key 테이블에 아무것도 쓰지
않는다** - CLAUDE.md 7절 "처음 보는 키 조합이 나와도 class를 자동
생성하지 않는다"는 원칙에 따라 최종 입력은 운영자가 확인 후 별도로 한다.

입력 데이터: momoai_web instance/momoai.db, 아래 쿼리 결과 스냅샷
(2026-08-31 SSH로 직접 확인, 읽기 전용 - momoai_web DB에 쓰기는 하지 않음)

    SELECT course_code, course_name
    FROM courses
    WHERE course_type = '하크니스' AND status = 'active';
"""

from __future__ import annotations

import io
import re
import sys
from dataclasses import dataclass

# 이미 utf-8로 래핑돼 있으면 다시 감싸지 않는다 - 다른 스크립트가 이 모듈을
# import할 때 이중 래핑으로 "I/O operation on closed file"이 나는 걸 방지.
if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# 2026-08-31 momoai_web 스냅샷 (course_code, course_name)
ACTIVE_HARKNESS_COURSES: list[tuple[str, str]] = [
    ("중1하260228", "중1 하크니스 토 15:00 - 윤영기"),
    ("중2하260228", "중2 하크니스 토 18:30 - 윤영기"),
    ("중1하260306", "중1 하크니스 금 19:40 - 윤영기"),
    ("중2하260301", "중2 하크니스 일 17:30 - 윤영기"),
    ("중1하260308", "중1 하크니스 일 20:00 - 윤영기"),
]

WEEKDAY_3LETTER = {
    "월": "MON", "화": "TUE", "수": "WED", "목": "THU",
    "금": "FRI", "토": "SAT", "일": "SUN",
}

# momoai_web Course.weekday와 동일한 규칙(0=월...6=일) - class_key.weekday에 그대로 쓴다.
WEEKDAY_INT = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6}

# --------------------------------------------------------------------------
# 한글 음절 -> 로마자 초성/이니셜 (성 제외 이름부분에서 각 음절 첫 글자를 뽑는 용도)
# --------------------------------------------------------------------------

_CHOSEONG = ["g", "kk", "n", "d", "tt", "r", "m", "b", "pp", "s", "ss", "", "j", "jj", "ch", "k", "t", "p", "h"]
_JUNGSEONG = ["a", "ae", "ya", "yae", "eo", "e", "yeo", "ye", "o", "wa", "wae", "oe",
              "yo", "u", "wo", "we", "wi", "yu", "eu", "ui", "i"]


def hangul_syllable_initial(ch: str) -> str:
    """한글 음절 하나를 로마자(개정 로마자 표기법)로 옮겼을 때 첫 글자.

    초성이 'ㅇ'(무음)이면 중성(모음) 로마자의 첫 글자를 대신 쓴다.
    예: '영' -> 초성 ㅇ(무음) + 중성 ㅕ(yeo) -> 'Y'
        '기' -> 초성 ㄱ(g) -> 'G'
    한글 음절이 아니면 빈 문자열을 반환한다(호출부에서 걸러냄).
    """
    code = ord(ch) - 0xAC00
    if not (0 <= code < 11172):
        return ""
    cho_idx = code // (21 * 28)
    jung_idx = (code % (21 * 28)) // 28
    cho = _CHOSEONG[cho_idx]
    if cho:
        return cho[0].upper()
    return _JUNGSEONG[jung_idx][0].upper()


def instructor_initials(full_name: str) -> str:
    """성을 뺀 이름부분 각 음절의 로마자 이니셜을 이어붙인다.

    3글자 이름(성 1글자 + 이름 2글자)을 기본 가정으로 첫 글자를 성으로
    보고 나머지를 이름으로 본다. 2글자 이하 이름은 전체를 이름으로 본다.
    """
    name_part = full_name[1:] if len(full_name) > 2 else full_name
    initials = "".join(hangul_syllable_initial(ch) for ch in name_part)
    return initials or "?"


# --------------------------------------------------------------------------
# course_name 파서
# --------------------------------------------------------------------------

@dataclass
class ParsedCourseName:
    teacher: str | None = None
    weekday_kr: str | None = None
    time_hhmm: str | None = None
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    @property
    def ok(self) -> bool:
        return not self.errors


# "{학년} 하크니스 {요일} {H:MM 또는 HH:MM} - {강사명}"
_COURSE_NAME_RE = re.compile(
    r"하크니스\s+(?P<weekday>[월화수목금토일])\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*-\s*(?P<teacher>\S+)"
)


def parse_course_name(course_name: str) -> ParsedCourseName:
    result = ParsedCourseName()
    m = _COURSE_NAME_RE.search(course_name)
    if not m:
        result.errors.append(f"패턴 불일치: {course_name!r}")
        return result

    result.weekday_kr = m.group("weekday")
    result.time_hhmm = f"{int(m.group('hour')):02d}{m.group('minute')}"
    result.teacher = m.group("teacher")
    return result


def build_class_code_candidate(parsed: ParsedCourseName) -> str | None:
    if not parsed.ok:
        return None
    weekday_en = WEEKDAY_3LETTER.get(parsed.weekday_kr)
    if weekday_en is None:
        return None
    initials = instructor_initials(parsed.teacher)
    return f"H-{initials}-{weekday_en}{parsed.time_hhmm}"


def main() -> None:
    print(f"{'course_code':<14} {'강사':<6} {'요일':<4} {'시각':<6} {'class_code 후보':<20} 비고")
    print("-" * 80)

    seen_codes: dict[str, list[str]] = {}
    for course_code, course_name in ACTIVE_HARKNESS_COURSES:
        parsed = parse_course_name(course_name)
        if not parsed.ok:
            print(f"{course_code:<14} {'':<6} {'':<4} {'':<6} {'(파싱 실패)':<20} {'; '.join(parsed.errors)}")
            continue

        candidate = build_class_code_candidate(parsed)
        note = ""
        seen_codes.setdefault(candidate, []).append(course_code)
        print(f"{course_code:<14} {parsed.teacher:<6} {parsed.weekday_kr:<4} "
              f"{parsed.time_hhmm:<6} {candidate:<20} {note}")

    print()
    dup = {code: sources for code, sources in seen_codes.items() if len(sources) > 1}
    if dup:
        print("경고: 같은 class_code 후보로 묶인 course_code가 있습니다(같은 반의 여러 회차일 수도, 우연한 충돌일 수도 있음):")
        for code, sources in dup.items():
            print(f"  {code}: {sources}")
    else:
        print("class_code 후보 5건 모두 서로 다름(중복 없음).")

    print("\n※ 이 목록은 출력만 합니다. class/class_key 테이블에는 아무것도 쓰지 않았습니다.")
    print("   운영자 확인 후 별도로 입력하세요 (CLAUDE.md 7절).")


if __name__ == "__main__":
    main()
