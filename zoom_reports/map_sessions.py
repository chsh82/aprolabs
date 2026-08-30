"""
raw.started_at 기반 세션 매핑 배치.

흐름 (2026-08-31 사용자 지시):
    raw.meeting_start_time(UTC) -> KST 요일 + 시각
    raw.meeting_host_email -> instructor_id
    class_key(instructor_id, weekday) 조회 -> 시작시각 ±30분 이내 매칭
    -> class_id 확정 -> session 적재

회의명 규칙([H-YG-SAT1500] 07차시_...)이 실전에서 아직 지켜지지 않는 것으로
확인돼(collector.py 첫 수집 결과, 73건 전부 토픽이 "OO의 개인 회의실")
회의명 파싱을 매핑의 필수 조건에서 뺐다. 대신 시간표(class_key)로 매핑하고,
회의명 파싱(parse_topic)은 병행해서 되면 쓰고 안 되도 매핑을 막지 않는다 -
나중에 회의명 규칙이 지켜지면 자동으로 lesson_no/text_label/session_type이
채워지는 구조다.

판정:
    class_key 후보 정확히 1건과 ±30분 이내 매칭 -> mapped
    후보 0건                                    -> unmapped, pending_class_key에 기록
    후보 2건 이상과 매칭(시간표가 너무 가까움)      -> mismatch

재실행해도 안전: session.meeting_uuid UNIQUE로 upsert,
pending_class_key도 (instructor_id,weekday,start_time) UNIQUE로 중복 방지.
"""

from __future__ import annotations

import io
import json
import sys
from datetime import datetime, timedelta

from zoom_pipeline_core import init_db, parse_topic

# 이미 utf-8로 래핑돼 있으면 다시 감싸지 않는다 - 다른 스크립트가 이 모듈을
# import할 때 이중 래핑으로 "I/O operation on closed file"이 나는 걸 방지.
if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

MATCH_TOLERANCE_MINUTES = 30


def to_kst(dt_str: str) -> datetime:
    """Zoom이 주는 UTC 'YYYY-MM-DDTHH:MM:SSZ'를 KST naive datetime으로 변환."""
    dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
    return dt + timedelta(hours=9)


def _minute_of_day(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def find_matching_class_keys(conn, instructor_id: int, weekday: int, kst_minute: int) -> list[tuple[int, str, int]]:
    """(instructor_id, weekday)의 class_key 후보 중 ±허용오차 이내인 것들. [(class_id, start_time, diff_min), ...]"""
    rows = conn.execute(
        "SELECT class_id, start_time FROM class_key WHERE instructor_id = ? AND weekday = ?",
        (instructor_id, weekday),
    ).fetchall()
    matches = []
    for class_id, start_time in rows:
        diff = abs(kst_minute - _minute_of_day(start_time))
        if diff <= MATCH_TOLERANCE_MINUTES:
            matches.append((class_id, start_time, diff))
    return matches


def record_pending_key(conn, instructor_id: int, weekday: int, start_time_hhmm: str, sample_topic: str) -> None:
    conn.execute(
        """
        INSERT INTO pending_class_key (instructor_id, weekday, start_time, sample_topic)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(instructor_id, weekday, start_time) DO NOTHING
        """,
        (instructor_id, weekday, start_time_hhmm, sample_topic),
    )


def map_one(conn, meeting_uuid: str, payload: dict) -> tuple[str, dict]:
    """(status, session_fields)를 반환한다."""
    host_email = payload.get("meeting_host_email", "")
    topic = payload.get("meeting_topic", "")
    started_at_raw = payload.get("meeting_start_time")

    parsed_topic = parse_topic(topic)

    fields: dict = {
        "meeting_uuid": meeting_uuid,
        "host_email": host_email,
        "topic_raw": topic,
        "started_at": started_at_raw,
        "lesson_no": parsed_topic.lesson_no,
        "text_label": parsed_topic.text_label,
        "session_type": parsed_topic.session_type,
        "instructor_id": None,
        "class_id": None,
        "status": "unmapped",
        "note": None,
    }

    def finish(status: str) -> tuple[str, dict]:
        fields["status"] = status
        return status, fields

    instr_row = conn.execute(
        "SELECT id FROM instructor WHERE zoom_email = ? AND active = 1", (host_email,)
    ).fetchone()
    if instr_row is None:
        fields["note"] = f"미등록 호스트: {host_email}"
        return finish("unmapped")
    instructor_id = instr_row[0]
    fields["instructor_id"] = instructor_id

    if not started_at_raw:
        fields["note"] = "meeting_start_time 없음"
        return finish("unmapped")

    kst_dt = to_kst(started_at_raw)
    weekday = kst_dt.weekday()  # Python datetime: 0=월...6=일 (momoai_web/class_key와 동일)
    kst_minute = kst_dt.hour * 60 + kst_dt.minute
    kst_hhmm = f"{kst_dt.hour:02d}:{kst_dt.minute:02d}"

    matches = find_matching_class_keys(conn, instructor_id, weekday, kst_minute)

    if len(matches) == 1:
        fields["class_id"] = matches[0][0]
        return finish("mapped")

    if len(matches) == 0:
        record_pending_key(conn, instructor_id, weekday, kst_hhmm, topic)
        fields["note"] = f"매칭되는 class_key 없음 (요일={weekday}, 시각={kst_hhmm} KST)"
        return finish("unmapped")

    fields["note"] = f"class_key {len(matches)}건과 ±{MATCH_TOLERANCE_MINUTES}분 이내 동시 매칭(시간표 겹침): {matches}"
    return finish("mismatch")


def run_mapping(conn) -> dict[str, int]:
    rows = conn.execute("SELECT meeting_uuid, payload_json FROM zoom_summary_raw").fetchall()
    counts = {"mapped": 0, "unmapped": 0, "mismatch": 0}

    for meeting_uuid, payload_json in rows:
        payload = json.loads(payload_json)
        status, fields = map_one(conn, meeting_uuid, payload)
        counts[status] += 1

        conn.execute(
            """
            INSERT INTO session
                (meeting_uuid, host_email, instructor_id, class_id, lesson_no,
                 text_label, session_type, topic_raw, started_at, status, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(meeting_uuid) DO UPDATE SET
                host_email = excluded.host_email,
                instructor_id = excluded.instructor_id,
                class_id = excluded.class_id,
                lesson_no = excluded.lesson_no,
                text_label = excluded.text_label,
                session_type = excluded.session_type,
                topic_raw = excluded.topic_raw,
                started_at = excluded.started_at,
                status = excluded.status,
                note = excluded.note
            """,
            (
                fields["meeting_uuid"], fields["host_email"], fields["instructor_id"],
                fields["class_id"], fields["lesson_no"], fields["text_label"],
                fields["session_type"], fields["topic_raw"], fields["started_at"],
                fields["status"], fields["note"],
            ),
        )

    conn.commit()
    return counts


def main() -> int:
    conn = init_db()
    counts = run_mapping(conn)
    print(f"mapped={counts['mapped']}  unmapped={counts['unmapped']}  mismatch={counts['mismatch']}")

    total_session = conn.execute("SELECT COUNT(*) FROM session").fetchone()[0]
    total_pending = conn.execute("SELECT COUNT(*) FROM pending_class_key WHERE resolved = 0").fetchone()[0]
    print(f"session 전체: {total_session}건, 미해결 pending_class_key: {total_pending}건")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
