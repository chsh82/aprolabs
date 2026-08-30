"""
검토 큐 화면 (FastAPI, momo_zoom.db 독립 앱).

zoom_reports/ 안에서 momo_zoom.db를 직접 여는 별도 앱이다. 메인 aprolabs
FastAPI 앱(app/main.py, 포트 8000)과 무관하다. 인증 없음(내부용, 사용자
지시). 읽기 전용 + "확인함" 버튼(pending_class_key.resolved=1 처리)만 있고
그 외 수정·삭제 기능은 없다.

탭 3개:
  1. 미확인 키 (pending_class_key, resolved=0) - 반복 횟수 내림차순
  2. 미매핑 세션 (session.status='unmapped') - KST 시각 내림차순
  3. 매핑됨 (session.status='mapped') - 반별 그룹, 반 안에서는 날짜순

시각은 전부 KST로 표시한다(map_sessions.to_kst() 재사용).

학생 이름 노출 관련:
    Zoom AI Companion의 summary_overview 본문은 자연어 문장이라 참석 학생
    실명이 문장 속에 그대로 섞여 나온다(실제 데이터로 확인됨). 문자열
    치환 마스킹은 등록 안 된 학생(다른 반, 1:1 개인 수업 등)의 이름은
    걸러낼 수 없어 신뢰할 수 없다고 판단, 탭 2에서 요약 본문 미리보기를
    아예 빼고 meeting_topic만 보여준다(사용자 결정, 2026-08-30). 대신
    회의 길이(분)와 요약 본문 글자 수를 메타데이터로만 보여준다 - 본문
    내용은 절대 노출 안 함(사용자 지시, 2026-08-31).

실행 (fastapi/jinja2/uvicorn이 있는 aprolabs venv 사용):
    ..\\venv\\Scripts\\python.exe -m uvicorn review_app:app --port 8801 --reload
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from map_sessions import to_kst

DB_PATH = Path(__file__).resolve().parent / "momo_zoom.db"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

app = FastAPI(title="Zoom 파이프라인 검토 큐")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def load_pending_keys(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT pk.id, pk.instructor_id, pk.weekday, pk.start_time,
               pk.sample_topic, pk.first_seen_at, i.name AS instructor_name
        FROM pending_class_key pk
        JOIN instructor i ON pk.instructor_id = i.id
        WHERE pk.resolved = 0
        """
    ).fetchall()

    # pending_class_key는 (instructor_id, weekday, start_time) UNIQUE라 중복 저장을
    # 안 한다 - "반복 횟수"는 여기 저장돼 있지 않으므로 unmapped session에서
    # 같은 (강사, 요일, 정확히 같은 KST 분)을 직접 세어서 구한다.
    unmapped = conn.execute(
        "SELECT instructor_id, started_at FROM session "
        "WHERE status = 'unmapped' AND instructor_id IS NOT NULL AND started_at IS NOT NULL"
    ).fetchall()
    counter: Counter[tuple[int, int, str]] = Counter()
    for row in unmapped:
        kst = to_kst(row["started_at"])
        key = (row["instructor_id"], kst.weekday(), f"{kst.hour:02d}:{kst.minute:02d}")
        counter[key] += 1

    result = []
    for row in rows:
        key = (row["instructor_id"], row["weekday"], row["start_time"])
        first_seen_kst = _sqlite_utc_to_kst(row["first_seen_at"])
        result.append({
            "id": row["id"],
            "instructor_name": row["instructor_name"],
            "weekday_kr": WEEKDAY_KR[row["weekday"]],
            "start_time": row["start_time"],
            "repeat_count": counter.get(key, 0),
            "sample_topic": row["sample_topic"],
            "first_seen_at_kst": first_seen_kst,
        })

    result.sort(key=lambda r: -r["repeat_count"])
    return result


def _sqlite_utc_to_kst(sqlite_dt: str | None) -> datetime | None:
    """SQLite datetime('now')는 UTC, 'YYYY-MM-DD HH:MM:SS' 형식이라
    Zoom 타임스탬프용 to_kst()('...T...Z' 형식)로는 못 바꾼다.
    같은 +9시간 규칙만 그대로 적용한다."""
    if not sqlite_dt:
        return None
    from datetime import timedelta
    dt = datetime.strptime(sqlite_dt, "%Y-%m-%d %H:%M:%S")
    return dt + timedelta(hours=9)


def _duration_minutes(payload: dict) -> int | None:
    """meeting_start_time/meeting_end_time(둘 다 UTC) 차이를 분으로. 본문은 안 본다."""
    start_raw = payload.get("meeting_start_time")
    end_raw = payload.get("meeting_end_time")
    if not start_raw or not end_raw:
        return None
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    start = datetime.strptime(start_raw, fmt)
    end = datetime.strptime(end_raw, fmt)
    return round((end - start).total_seconds() / 60)


def load_unmapped_sessions(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT s.id, s.meeting_uuid, s.host_email, s.topic_raw, s.started_at, s.note,
               i.name AS instructor_name, r.payload_json
        FROM session s
        LEFT JOIN instructor i ON s.instructor_id = i.id
        LEFT JOIN zoom_summary_raw r ON s.meeting_uuid = r.meeting_uuid
        WHERE s.status = 'unmapped'
        """
    ).fetchall()

    result = []
    for row in rows:
        kst = to_kst(row["started_at"]) if row["started_at"] else None

        duration_min = None
        overview_len = None
        if row["payload_json"]:
            payload = json.loads(row["payload_json"])
            duration_min = _duration_minutes(payload)
            overview_len = len(payload.get("summary_overview") or "")

        result.append({
            "kst_time": kst,
            "instructor_name": row["instructor_name"] or f"(미등록: {row['host_email']})",
            "topic": row["topic_raw"],
            "duration_min": duration_min,
            "overview_len": overview_len,
            "note": row["note"],
        })

    result.sort(key=lambda r: r["kst_time"] or datetime.min, reverse=True)
    return result


def load_mapped_sessions(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT s.started_at, s.class_meeting_id, c.class_code, c.name AS class_name,
               cm.meeting_date
        FROM session s
        JOIN class c ON s.class_id = c.id
        LEFT JOIN class_meeting cm ON s.class_meeting_id = cm.id
        WHERE s.status = 'mapped'
        """
    ).fetchall()

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        meeting_date = row["meeting_date"]
        if not meeting_date and row["started_at"]:
            meeting_date = to_kst(row["started_at"]).date().isoformat()
        groups[(row["class_code"], row["class_name"])].append({
            "meeting_date": meeting_date,
            "has_class_meeting": row["class_meeting_id"] is not None,
        })

    result = []
    for (class_code, class_name), items in sorted(groups.items(), key=lambda kv: kv[0][0]):
        items.sort(key=lambda x: x["meeting_date"] or "")
        result.append({"class_code": class_code, "class_name": class_name, "meetings": items})
    return result


@app.get("/")
def index(request: Request, tab: str = "pending"):
    if tab not in ("pending", "unmapped", "mapped"):
        tab = "pending"

    conn = get_conn()
    try:
        context = {"request": request, "tab": tab}
        if tab == "pending":
            context["pending_keys"] = load_pending_keys(conn)
        elif tab == "unmapped":
            context["unmapped_sessions"] = load_unmapped_sessions(conn)
        else:
            context["mapped_groups"] = load_mapped_sessions(conn)
        return templates.TemplateResponse("review.html", context)
    finally:
        conn.close()


@app.post("/pending/{pending_id}/resolve")
def resolve_pending(pending_id: int):
    """읽기 전용 원칙의 유일한 예외 - resolved 플래그만 세운다. 삭제/수정 없음."""
    conn = get_conn()
    try:
        conn.execute("UPDATE pending_class_key SET resolved = 1 WHERE id = ?", (pending_id,))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/?tab=pending", status_code=303)
