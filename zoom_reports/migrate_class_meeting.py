"""
class_meeting 도입 마이그레이션 (기존 momo_zoom.db용, 1회 실행).

zoom_pipeline_core.py의 SCHEMA는 새로 만드는 DB 기준이라 CREATE TABLE
IF NOT EXISTS로는 이미 존재하는 session/report 테이블 구조를 바꿀 수
없다. 이 스크립트가 그 간극을 메운다:

    1) class_meeting 테이블 생성 (신규 - IF NOT EXISTS로 충분)
    2) session에 class_meeting_id 컬럼 추가 (ALTER TABLE, 없을 때만)
    3) report를 새 스키마(class_meeting_id 기준)로 재생성
       - report가 비어있을 때만 안전하게 DROP 후 재생성한다.
         이미 데이터가 있으면 손대지 않고 경고만 남긴다(수동 검토 필요).
    4) status='mapped'인 session을 (class_id, KST 날짜)로 묶어
       class_meeting을 만들고 session.class_meeting_id를 채운다.

재실행해도 안전하다 - 이미 있는 class_meeting은 건너뛰고, 이미 연결된
session은 다시 연결하지 않는다.
"""

from __future__ import annotations

import io
import sqlite3
import sys
from datetime import datetime, timedelta

from zoom_pipeline_core import init_db

# 이미 utf-8로 래핑돼 있으면 다시 감싸지 않는다 - 다른 스크립트가 이 모듈을
# import할 때 이중 래핑으로 "I/O operation on closed file"이 나는 걸 방지.
if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def ensure_class_meeting_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS class_meeting (
            id              INTEGER PRIMARY KEY,
            class_id        INTEGER NOT NULL REFERENCES class(id),
            meeting_date    TEXT NOT NULL,
            lesson_no       INTEGER,
            text_label      TEXT,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (class_id, meeting_date)
        )
        """
    )
    conn.commit()
    print("class_meeting 테이블 확인/생성 완료")


def ensure_session_column(conn: sqlite3.Connection) -> None:
    if _has_column(conn, "session", "class_meeting_id"):
        print("session.class_meeting_id 이미 존재함 - 건너뜀")
        return
    conn.execute("ALTER TABLE session ADD COLUMN class_meeting_id INTEGER REFERENCES class_meeting(id)")
    conn.commit()
    print("session.class_meeting_id 컬럼 추가 완료")


def ensure_report_table(conn: sqlite3.Connection) -> None:
    has_old_fk = _has_column(conn, "report", "session_id")
    has_new_fk = _has_column(conn, "report", "class_meeting_id")

    if has_new_fk and not has_old_fk:
        print("report 이미 새 스키마(class_meeting_id) - 건너뜀")
        return

    count = conn.execute("SELECT COUNT(*) FROM report").fetchone()[0] if _table_exists(conn, "report") else 0
    if count > 0:
        print(f"경고: report에 이미 {count}건의 데이터가 있어 자동 재생성을 건너뜀. 수동 마이그레이션 필요.")
        return

    conn.execute("DROP TABLE IF EXISTS report")
    conn.execute(
        """
        CREATE TABLE report (
            id                INTEGER PRIMARY KEY,
            class_meeting_id  INTEGER NOT NULL REFERENCES class_meeting(id),
            student_id        INTEGER NOT NULL REFERENCES student(id),
            body_md           TEXT,
            status            TEXT NOT NULL DEFAULT 'draft',
            approved_by       INTEGER REFERENCES instructor(id),
            approved_at       TEXT,
            published_at      TEXT,
            UNIQUE (class_meeting_id, student_id)
        )
        """
    )
    conn.commit()
    print("report 테이블을 class_meeting_id 기준 새 스키마로 재생성 완료(기존에 비어있었음)")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return row is not None


def group_sessions_into_class_meetings(conn: sqlite3.Connection) -> int:
    """status='mapped'인 session을 (class_id, KST 날짜)로 묶어 class_meeting을 만들고 연결한다.

    이미 class_meeting_id가 채워진 session은 건드리지 않는다(재실행 안전).
    """
    rows = conn.execute(
        "SELECT id, class_id, started_at FROM session WHERE status = 'mapped' AND class_meeting_id IS NULL"
    ).fetchall()

    linked = 0
    for session_id, class_id, started_at in rows:
        # started_at은 Zoom이 준 UTC 'YYYY-MM-DDTHH:MM:SSZ' - KST 날짜로 변환.
        dt_utc = datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ")
        meeting_date = (dt_utc + timedelta(hours=9)).date().isoformat()

        conn.execute(
            "INSERT INTO class_meeting (class_id, meeting_date) VALUES (?, ?) "
            "ON CONFLICT(class_id, meeting_date) DO NOTHING",
            (class_id, meeting_date),
        )
        class_meeting_id = conn.execute(
            "SELECT id FROM class_meeting WHERE class_id = ? AND meeting_date = ?",
            (class_id, meeting_date),
        ).fetchone()[0]

        conn.execute(
            "UPDATE session SET class_meeting_id = ? WHERE id = ?",
            (class_meeting_id, session_id),
        )
        linked += 1

    conn.commit()
    return linked


def main() -> int:
    # init_db()의 SCHEMA 전체를 그대로 돌리면 새 인덱스(session.class_meeting_id
    # 참조)가 기존 테이블에 아직 없는 컬럼을 찾다가 실패한다. 그래서 먼저 raw
    # 커넥션으로 컬럼/테이블을 추가한 뒤에 init_db()를 호출해 나머지 스키마
    # (인덱스 등)를 마무리한다.
    conn = sqlite3.connect("momo_zoom.db")
    conn.execute("PRAGMA foreign_keys = ON")

    ensure_class_meeting_table(conn)
    ensure_session_column(conn)
    ensure_report_table(conn)
    conn.close()

    conn = init_db()  # 이제 컬럼이 존재하므로 나머지 스키마(인덱스 등) 정상 적용됨
    linked = group_sessions_into_class_meetings(conn)
    print(f"\nsession {linked}건을 class_meeting에 연결")

    total_meetings = conn.execute("SELECT COUNT(*) FROM class_meeting").fetchone()[0]
    print(f"class_meeting 전체: {total_meetings}건")

    print("\nclass_id별 class_meeting 수 (원래 mapped session 수와 비교):")
    for row in conn.execute(
        """
        SELECT c.class_code,
               (SELECT COUNT(*) FROM session s WHERE s.class_id = c.id AND s.status = 'mapped') AS session_cnt,
               (SELECT COUNT(*) FROM class_meeting cm WHERE cm.class_id = c.id) AS meeting_cnt
        FROM class c ORDER BY c.class_code
        """
    ):
        print(f"  {row[0]}: session {row[1]}건 -> class_meeting {row[2]}건")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
