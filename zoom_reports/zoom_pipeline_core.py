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
    class_code      TEXT NOT NULL UNIQUE,   -- 예: MB-3A
    name            TEXT,
    instructor_id   INTEGER REFERENCES instructor(id),
    FOREIGN KEY (instructor_id) REFERENCES instructor(id)
);

CREATE TABLE IF NOT EXISTS student (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    class_id        INTEGER NOT NULL REFERENCES class(id)
);

-- 수업 회차. raw 1건 = session 1건.
-- status: mapped | unmapped | mismatch  (mismatch = 배정표 담당자와 host 불일치)
CREATE TABLE IF NOT EXISTS session (
    id              INTEGER PRIMARY KEY,
    meeting_uuid    TEXT NOT NULL UNIQUE REFERENCES zoom_summary_raw(meeting_uuid),
    host_email      TEXT NOT NULL,
    instructor_id   INTEGER REFERENCES instructor(id),
    class_id        INTEGER REFERENCES class(id),
    lesson_no       INTEGER,
    text_label      TEXT,
    topic_raw       TEXT NOT NULL,
    started_at      TEXT,
    status          TEXT NOT NULL DEFAULT 'unmapped',
    note            TEXT
);

-- 세션 1건에서 학생 수만큼 생성된다 (1:4 수업이면 4건).
-- status: draft | review | published
CREATE TABLE IF NOT EXISTS report (
    id              INTEGER PRIMARY KEY,
    session_id      INTEGER NOT NULL REFERENCES session(id),
    student_id      INTEGER NOT NULL REFERENCES student(id),
    body_md         TEXT,
    status          TEXT NOT NULL DEFAULT 'draft',
    approved_by     INTEGER REFERENCES instructor(id),
    approved_at     TEXT,
    published_at    TEXT,
    UNIQUE (session_id, student_id)
);

CREATE INDEX IF NOT EXISTS idx_session_status    ON session(status);
CREATE INDEX IF NOT EXISTS idx_session_started   ON session(started_at);
CREATE INDEX IF NOT EXISTS idx_report_status     ON report(status);
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
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


# 전각 괄호, 공백 변형, 07차시 / 7차시 / 07강 을 모두 허용한다.
_BRACKET = re.compile(r"[\[\uff3b]\s*([A-Za-z0-9\-]+)\s*[\]\uff3d]")
_LESSON = re.compile(r"(\d{1,2})\s*(?:차시|강)")


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
        ("[MB-3A] 07차시_카프카_변신", "MB-3A", 7, "카프카 변신"),
        ("[mb-3a] 7차시_이방인", "MB-3A", 7, "이방인"),
        ("［MB-9C］ 12강_곰브리치 세계사", "MB-9C", 12, "곰브리치 세계사"),
        ("[MB-5B]03차시_신화의 숲", "MB-5B", 3, "신화의 숲"),
    ]
    for topic, code, lesson, label in cases:
        p = parse_topic(topic)
        assert p.ok, (topic, p.errors)
        assert (p.class_code, p.lesson_no, p.text_label) == (code, lesson, label), (topic, p)

    # 규칙 위반 건은 실패로 잡히되 부분 결과는 살아남아야 한다.
    bad = parse_topic("카프카 변신 수업")
    assert not bad.ok and bad.class_code is None
    partial = parse_topic("[MB-3A] 카프카_변신")
    assert not partial.ok and partial.class_code == "MB-3A"

    conn = init_db(":memory:")
    conn.execute("INSERT INTO instructor (id, name, zoom_email) VALUES (1, '윤영기', 'momo@example.com')")
    conn.execute("INSERT INTO instructor (id, name, zoom_email) VALUES (2, '강사B', 'b@example.com')")
    conn.execute("INSERT INTO class (id, class_code, instructor_id) VALUES (1, 'MB-3A', 1)")
    conn.commit()

    assert resolve_session_status(conn, "momo@example.com", parse_topic("[MB-3A] 07차시_변신"))[0] == "mapped"
    assert resolve_session_status(conn, "b@example.com", parse_topic("[MB-3A] 07차시_변신"))[0] == "mismatch"
    assert resolve_session_status(conn, "nobody@example.com", parse_topic("[MB-3A] 07차시_변신"))[0] == "unmapped"
    assert resolve_session_status(conn, "momo@example.com", parse_topic("아무 제목"))[0] == "unmapped"

    print("all checks passed")
