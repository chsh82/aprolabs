"""
모모의 책장 — Zoom 수업 요약 파이프라인 (코어)

이 모듈은 Zoom API 접근 없이 단독으로 동작/검증 가능한 두 부분을 담는다.
  1) SQLite 스키마 (init_db)
  2) 회의명 파서 (parse_topic)

수집기(collector)는 master 스코프 승인 후 별도 모듈로 붙인다.

회의명 규칙:
    [MB-3A] 07차시_카프카_변신
     └반ID   └차시   └텍스트
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from dataclasses import dataclass, field

# --------------------------------------------------------------------------
# 스키마
# --------------------------------------------------------------------------

SCHEMA = """
PRAGMA foreign_keys = ON;

-- Zoom 응답 원문. 절대 가공하지 않는다.
-- 프롬프트/파싱 규칙이 바뀌면 여기서 재처리하며, Zoom을 다시 긁지 않는다.
CREATE TABLE IF NOT EXISTS zoom_summary_raw (
    meeting_uuid    TEXT PRIMARY KEY,
    payload_json    TEXT NOT NULL,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS instructor (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    zoom_email      TEXT NOT NULL UNIQUE,   -- meeting_host_email 조인 키
    active          INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS class (
    id              INTEGER PRIMARY KEY,
    class_code      TEXT NOT NULL UNIQUE,   -- 예: H-YG-SAT1500
    name            TEXT,
    course_type     TEXT NOT NULL DEFAULT '하크니스',
    instructor_id   INTEGER REFERENCES instructor(id),
    active          INTEGER NOT NULL DEFAULT 1
);

-- (강사, 요일, 시각) 조합 → 반. 시간표가 바뀌면 같은 class_id에 키를 하나 더 단다.
-- 학기가 바뀌어도 class 행은 그대로 유지된다.
CREATE TABLE IF NOT EXISTS class_key (
    id              INTEGER PRIMARY KEY,
    class_id        INTEGER NOT NULL REFERENCES class(id),
    instructor_id   INTEGER NOT NULL REFERENCES instructor(id),
    weekday         INTEGER NOT NULL,       -- 0=월 ... 6=일
    start_time      TEXT NOT NULL,          -- 'HH:MM'
    valid_from      TEXT,
    UNIQUE (instructor_id, weekday, start_time)
);

-- 규칙상 처음 보는 (강사,요일,시각) 조합. 자동으로 class를 만들지 않고 여기 쌓는다.
-- 운영자가 확인해 기존 class에 class_key를 추가하거나 새 class를 발급한다.
CREATE TABLE IF NOT EXISTS pending_class_key (
    id              INTEGER PRIMARY KEY,
    instructor_id   INTEGER NOT NULL REFERENCES instructor(id),
    weekday         INTEGER NOT NULL,
    start_time      TEXT NOT NULL,
    sample_topic    TEXT,
    first_seen_at   TEXT NOT NULL DEFAULT (datetime('now')),
    resolved        INTEGER NOT NULL DEFAULT 0,
    UNIQUE (instructor_id, weekday, start_time)
);

CREATE TABLE IF NOT EXISTS student (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    class_id        INTEGER NOT NULL REFERENCES class(id)
);

-- Zoom 회의 1건. raw 1건 = session 1건 (UUID 단위, 그대로 - 가공하지 않는다).
-- 재접속/재시작으로 같은 실제 수업이 meeting_uuid를 여러 번 받으면 session도
-- 여러 건 생긴다 - 이게 정상이다. 회차 단위로 묶는 건 class_meeting이 한다.
-- status: mapped | unmapped | mismatch | out_of_scope
--   mismatch     = 배정표 담당자와 host 불일치
--   out_of_scope = 1차 적용 범위(하크니스 그룹수업) 밖. 수집은 하되 리포트를 만들지 않는다.
-- session_type: regular | makeup
CREATE TABLE IF NOT EXISTS session (
    id                INTEGER PRIMARY KEY,
    meeting_uuid      TEXT NOT NULL UNIQUE REFERENCES zoom_summary_raw(meeting_uuid),
    host_email        TEXT NOT NULL,
    instructor_id     INTEGER REFERENCES instructor(id),
    class_id          INTEGER REFERENCES class(id),
    class_meeting_id  INTEGER REFERENCES class_meeting(id),
    lesson_no         INTEGER,
    text_label        TEXT,
    session_type      TEXT NOT NULL DEFAULT 'regular',
    topic_raw         TEXT NOT NULL,
    started_at        TEXT,
    status            TEXT NOT NULL DEFAULT 'unmapped',
    note              TEXT
);

-- 회차 단위. 같은 (class_id, 날짜)로 묶인 session들을 대표한다 - 리포트는
-- 이 단위로 만든다(session이 아니라). 재접속으로 session이 여러 건이어도
-- class_meeting은 하나다.
CREATE TABLE IF NOT EXISTS class_meeting (
    id              INTEGER PRIMARY KEY,
    class_id        INTEGER NOT NULL REFERENCES class(id),
    meeting_date    TEXT NOT NULL,          -- 'YYYY-MM-DD' (KST 기준)
    lesson_no       INTEGER,
    text_label      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (class_id, meeting_date)
);

-- class_meeting 1건에서 학생 수만큼 생성된다 (1:4 수업이면 4건).
-- status: draft | review | published
CREATE TABLE IF NOT EXISTS report (
    id                INTEGER PRIMARY KEY,
    class_meeting_id  INTEGER NOT NULL REFERENCES class_meeting(id),
    student_id        INTEGER NOT NULL REFERENCES student(id),
    body_md           TEXT,
    status            TEXT NOT NULL DEFAULT 'draft',
    corrected_at      TEXT,   -- 고유명사 교정 완료 시각. NULL이면 correct_reports.py가 아직 처리 안 함
    approved_by       INTEGER REFERENCES instructor(id),
    approved_at       TEXT,
    published_at      TEXT,
    UNIQUE (class_meeting_id, student_id)
);

-- qa_reports.py 배치 결과. report는 절대 건드리지 않고 여기에만 쌓는다.
-- status: fail_masking | fail_length | pass_clean | pass_with_flags
-- 1단계(코드) 실패는 fail_masking/fail_length로 끝나고 2단계(LLM)로 안 간다.
CREATE TABLE IF NOT EXISTS report_qa (
    id              INTEGER PRIMARY KEY,
    report_id       INTEGER NOT NULL REFERENCES report(id),
    status          TEXT NOT NULL,
    fail_reason     TEXT,
    body_length     INTEGER,
    median_length   REAL,
    model           TEXT,
    raw_response    TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (report_id)
);

-- flag_type: 근거없음 | 모순 | 금칙_비교 | 금칙_단정
-- sentence는 report.body_md에서 인용한 문장(이미 마스킹된 상태의 본문에서 뽑음).
CREATE TABLE IF NOT EXISTS report_qa_flag (
    id              INTEGER PRIMARY KEY,
    report_qa_id    INTEGER NOT NULL REFERENCES report_qa(id),
    flag_type       TEXT NOT NULL,
    sentence        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_status      ON session(status);
CREATE INDEX IF NOT EXISTS idx_session_started     ON session(started_at);
CREATE INDEX IF NOT EXISTS idx_session_meeting     ON session(class_meeting_id);
CREATE INDEX IF NOT EXISTS idx_class_meeting_class  ON class_meeting(class_id, meeting_date);
CREATE INDEX IF NOT EXISTS idx_report_status       ON report(status);
CREATE INDEX IF NOT EXISTS idx_report_qa_status    ON report_qa(status);
CREATE INDEX IF NOT EXISTS idx_report_qa_flag_qa   ON report_qa_flag(report_qa_id);
"""


def init_db(path: str = "momo_zoom.db") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# 회의명 파서
# --------------------------------------------------------------------------

@dataclass
class ParsedTopic:
    class_code: str | None = None
    lesson_no: int | None = None
    text_label: str | None = None
    is_makeup: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def session_type(self) -> str:
        return "makeup" if self.is_makeup else "regular"

    @property
    def ok(self) -> bool:
        return not self.errors


# 전각 괄호, 공백 변형, 07차시 / 7차시 / 07강 을 모두 허용한다.
_BRACKET = re.compile(r"[\[\uff3b]\s*([A-Za-z0-9\-]+)\s*[\]\uff3d]")
_LESSON = re.compile(r"(\d{1,2})\s*(?:차시|강)")

# 보강 표식은 반 코드와 별개의 대괄호로 둔다.
# class_code 자체에 하이픈이 들어가므로 접미사(-R) 방식은 쓸 수 없다.
#   [H-YG-SAT1500][보강] 07차시_...
_MAKEUP = re.compile(r"[\[\uff3b]\s*(?:보강|makeup|R)\s*[\]\uff3d]", re.IGNORECASE)


def _normalize(s: str) -> str:
    # 전각 영숫자를 반각으로, 연속 공백을 하나로.
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_topic(topic: str) -> ParsedTopic:
    """회의명에서 반ID, 차시, 텍스트를 뽑는다.

    파싱 실패는 예외를 던지지 않고 errors에 쌓는다.
    호출부는 ok=False 인 건을 버리지 말고 status='unmapped'로 남겨
    관리자 검토 큐에 노출시켜야 한다.
    """
    result = ParsedTopic()
    if not topic or not topic.strip():
        result.errors.append("빈 회의명")
        return result

    s = _normalize(topic)

    m = _MAKEUP.search(s)
    if m:
        result.is_makeup = True
        s = (s[:m.start()] + " " + s[m.end():]).strip()

    m = _BRACKET.search(s)
    if m:
        result.class_code = m.group(1).upper()
        rest = s[m.end():].strip()
    else:
        result.errors.append("반ID 대괄호 없음")
        rest = s

    m = _LESSON.search(rest)
    if m:
        result.lesson_no = int(m.group(1))
        rest = rest[m.end():].strip()
    else:
        result.errors.append("차시 표기 없음")

    label = rest.lstrip("_- ").replace("_", " ").strip()
    if label:
        result.text_label = label
    else:
        result.errors.append("텍스트 라벨 없음")

    return result


# --------------------------------------------------------------------------
# 교차 검증
# --------------------------------------------------------------------------

def resolve_session_status(
    conn: sqlite3.Connection, host_email: str, parsed: ParsedTopic
) -> tuple[str, int | None, int | None, str | None]:
    """(status, instructor_id, class_id, note) 를 돌려준다.

    host_email(결정론적)과 회의명 파싱(휴리스틱)을 교차 검증한다.
    둘이 어긋나면 대타 수업이거나 개설 실수이므로 자동 발행을 막는다.
    """
    row = conn.execute(
        "SELECT id FROM instructor WHERE zoom_email = ? AND active = 1", (host_email,)
    ).fetchone()
    instructor_id = row[0] if row else None
    if instructor_id is None:
        return "unmapped", None, None, f"미등록 호스트: {host_email}"

    if not parsed.ok or parsed.class_code is None:
        return "unmapped", instructor_id, None, "; ".join(parsed.errors)

    row = conn.execute(
        "SELECT id, instructor_id FROM class WHERE class_code = ?", (parsed.class_code,)
    ).fetchone()
    if row is None:
        return "unmapped", instructor_id, None, f"미등록 반: {parsed.class_code}"

    class_id, assigned_id = row
    if assigned_id != instructor_id:
        return "mismatch", instructor_id, class_id, "배정표 담당자와 host 불일치 (대타?)"

    return "mapped", instructor_id, class_id, None


# --------------------------------------------------------------------------
# 자체 테스트
# --------------------------------------------------------------------------

if __name__ == "__main__":
    cases = [
        ("[H-YG-SAT1500] 07차시_카프카_변신", "H-YG-SAT1500", 7, "카프카 변신", False),
        ("[h-yg-sat1500] 7차시_이방인", "H-YG-SAT1500", 7, "이방인", False),
        ("［H-YG-MON1900］ 12강_곰브리치 세계사", "H-YG-MON1900", 12, "곰브리치 세계사", False),
        ("[H-YG-SAT1500]03차시_신화의 숲", "H-YG-SAT1500", 3, "신화의 숲", False),
        # 보강 — 표식 위치가 앞이든 뒤든 인식한다.
        ("[H-YG-SAT1500][보강] 07차시_카프카_변신", "H-YG-SAT1500", 7, "카프카 변신", True),
        ("[보강][H-YG-SAT1500] 07차시_카프카_변신", "H-YG-SAT1500", 7, "카프카 변신", True),
    ]
    for topic, code, lesson, label, makeup in cases:
        p = parse_topic(topic)
        assert p.ok, (topic, p.errors)
        assert (p.class_code, p.lesson_no, p.text_label) == (code, lesson, label), (topic, p)
        assert p.is_makeup is makeup and p.session_type == ("makeup" if makeup else "regular"), (topic, p)

    # 규칙 위반 건은 실패로 잡히되 부분 결과는 살아남아야 한다.
    bad = parse_topic("카프카 변신 수업")
    assert not bad.ok and bad.class_code is None
    partial = parse_topic("[H-YG-SAT1500] 카프카_변신")
    assert not partial.ok and partial.class_code == "H-YG-SAT1500"

    conn = init_db(":memory:")
    conn.execute("INSERT INTO instructor (id, name, zoom_email) VALUES (1, '윤영기', 'momo@example.com')")
    conn.execute("INSERT INTO instructor (id, name, zoom_email) VALUES (2, '강사B', 'b@example.com')")
    conn.execute("INSERT INTO class (id, class_code, instructor_id) VALUES (1, 'H-YG-SAT1500', 1)")
    conn.commit()

    t = parse_topic("[H-YG-SAT1500] 07차시_변신")
    assert resolve_session_status(conn, "momo@example.com", t)[0] == "mapped"
    assert resolve_session_status(conn, "b@example.com", t)[0] == "mismatch"
    assert resolve_session_status(conn, "nobody@example.com", t)[0] == "unmapped"
    assert resolve_session_status(conn, "momo@example.com", parse_topic("아무 제목"))[0] == "unmapped"

    print("all checks passed")
