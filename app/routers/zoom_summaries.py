"""Zoom 수업 요약 (zoom_reports 파이프라인 결과 조회 + 검토·승인).

GET  /zoom-summaries                                  -> 목록 (class_meeting 단위, 반/기간 필터)
GET  /zoom-summaries/{meeting_id}                      -> 상세 (요약 본문 전문 + 리포트 초안 목록)
GET  /zoom-summaries/{meeting_id}/reports/{report_id}  -> 리포트 초안 보기/편집
POST /zoom-summaries/{meeting_id}/reports/{report_id}/save       -> body_md 저장 (draft만)
POST /zoom-summaries/{meeting_id}/reports/{report_id}/approve    -> draft -> review
POST /zoom-summaries/{meeting_id}/reports/{report_id}/unapprove  -> review -> draft

zoom_reports/momo_zoom.db를 연다. 조회(GET)는 SQLite URI mode=ro로 열어
DB 파일 자체가 쓰기를 거부하게 강제한다. 쓰기가 필요한 라우트(save/
approve/unapprove)만 별도의 일반(쓰기 가능) 커넥션을 쓴다 - 읽기 전용
커넥션과 절대 섞지 않는다.

session.status='mapped'인 것만 대상으로 한다. unmapped 세션은 아직 어느
반인지 확정되지 않은 상태라(zoom_reports/review_app.py의 검토 큐 참고)
이 화면에 노출하면 반 정보가 틀릴 수 있고, 그중 상당수는 하크니스 그룹
수업이 아닌 1:1 개인 상담이라 본문을 이 화면에 그대로 보여주면 안 된다.

승인(approve)은 draft -> review 전환만 한다. review -> published(학부모
발행)는 이번 범위 밖 - 별도 단계에서 다룬다. 지금은 강사별 소유자 스코핑도
없다(로그인한 아프로랩스 운영자 전원이 전체 반을 봄) - approved_by에는
현재 로그인 사용자 id를 그대로 기록해두되, zoom_reports 쪽 instructor.id와
같은 값 공간이 아니므로 나중에 소유자 스코핑을 붙일 때 이 필드의 의미를
다시 정리해야 한다.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth import get_current_user_id

router = APIRouter(prefix="/zoom-summaries")
templates = Jinja2Templates(directory="app/templates")

# zoom_reports/ 는 aprolabs와 별개 앱(zoom_reports/CLAUDE.md)이다. 경로는
# ZOOM_DB_PATH로 오버라이드 가능.
ZOOM_DB_PATH = os.getenv("ZOOM_DB_PATH", "zoom_reports/momo_zoom.db")


def get_zoom_db():
    """조회용 - mode=ro라 이 커넥션으로는 쓰기 자체가 SQLite 단에서 막힌다."""
    db_path = Path(ZOOM_DB_PATH).resolve()
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def get_zoom_db_rw():
    """쓰기용 - report 저장/승인/승인취소 라우트에서만 쓴다."""
    db_path = Path(ZOOM_DB_PATH).resolve()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _to_kst(utc_str: str) -> datetime:
    """Zoom의 UTC 'YYYY-MM-DDTHH:MM:SSZ'를 KST naive datetime으로 변환.

    zoom_reports/map_sessions.py의 to_kst()와 같은 규칙(+9시간) - 별개 앱이라
    임포트 대신 그대로 복제(2줄짜리 순수 함수라 결합보다 중복이 낫다고 판단).
    """
    return datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=9)


def _overview_len(conn: sqlite3.Connection, meeting_uuid: str) -> int:
    row = conn.execute(
        "SELECT payload_json FROM zoom_summary_raw WHERE meeting_uuid = ?", (meeting_uuid,)
    ).fetchone()
    if not row:
        return 0
    payload = json.loads(row["payload_json"])
    return len(payload.get("summary_overview") or "")


PAGE_SIZE = 50


@router.get("", response_class=HTMLResponse)
def zoom_summaries_list(
    request: Request,
    class_code: str = "",
    date_from: str = "",
    date_to: str = "",
    instructor_q: str = "",
    class_q: str = "",
    page: int = 1,
    conn: sqlite3.Connection = Depends(get_zoom_db),
):
    query = """
        SELECT cm.id AS class_meeting_id, c.class_code, c.name AS class_name,
               i.name AS instructor_name, cm.meeting_date, s.meeting_uuid
        FROM class_meeting cm
        JOIN class c ON cm.class_id = c.id
        JOIN session s ON s.class_meeting_id = cm.id AND s.status = 'mapped'
        LEFT JOIN instructor i ON c.instructor_id = i.id
        WHERE 1=1
    """
    params: list[str] = []
    if class_code:
        query += " AND c.class_code = ?"
        params.append(class_code)
    if date_from:
        query += " AND cm.meeting_date >= ?"
        params.append(date_from)
    if date_to:
        query += " AND cm.meeting_date <= ?"
        params.append(date_to)
    if instructor_q.strip():
        query += " AND i.name LIKE ?"
        params.append(f"%{instructor_q.strip()}%")
    if class_q.strip():
        query += " AND (c.name LIKE ? OR c.class_code LIKE ?)"
        params.extend([f"%{class_q.strip()}%", f"%{class_q.strip()}%"])
    rows = conn.execute(query, params).fetchall()

    meetings: dict[int, dict] = {}
    for row in rows:
        cmid = row["class_meeting_id"]
        meeting = meetings.setdefault(cmid, {
            "class_meeting_id": cmid,
            "class_code": row["class_code"],
            "class_name": row["class_name"],
            "instructor_name": row["instructor_name"] or "-",
            "meeting_date": row["meeting_date"],
            "char_count": 0,
        })
        meeting["char_count"] += _overview_len(conn, row["meeting_uuid"])

    # 최신 수집분이 위로 오도록 날짜 내림차순(동일 날짜는 반 코드로 안정 정렬).
    meeting_list = sorted(meetings.values(), key=lambda m: (m["meeting_date"], m["class_code"]), reverse=True)

    total = len(meeting_list)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(1, page), total_pages)
    start = (page - 1) * PAGE_SIZE
    page_meetings = meeting_list[start:start + PAGE_SIZE]

    classes = conn.execute("SELECT class_code, name FROM class ORDER BY class_code").fetchall()

    return templates.TemplateResponse("zoom_summaries/index.html", {
        "request": request,
        "meetings": page_meetings,
        "meetings_total": total,
        "page": page,
        "total_pages": total_pages,
        "page_size": PAGE_SIZE,
        "classes": classes,
        "filter_class_code": class_code,
        "filter_date_from": date_from,
        "filter_date_to": date_to,
        "filter_instructor_q": instructor_q,
        "filter_class_q": class_q,
    })


@router.get("/stats", response_class=HTMLResponse)
def zoom_summaries_stats(
    request: Request,
    weeks: int = 8,
    conn: sqlite3.Connection = Depends(get_zoom_db),
):
    """강사별 누적 통계 + 최근 N주 주간 추이.

    /{class_meeting_id} 라우트보다 반드시 먼저 등록돼야 한다 - FastAPI는
    경로 문자열의 {class_meeting_id}를 라우팅 시점에는 타입 체크 없이
    매칭하므로(파이썬 타입힌트는 매칭 후 파싱 단계에서만 적용), 이 라우트가
    뒤에 있으면 "/zoom-summaries/stats" 요청이 먼저 등록된
    "/{class_meeting_id}" 라우트에 잡혀 int 파싱 실패(422)가 난다.
    """
    # 1) 강사별 세션 매핑 현황
    session_rows = conn.execute("""
        SELECT i.id AS instructor_id, i.name,
               SUM(CASE WHEN s.status = 'mapped' THEN 1 ELSE 0 END) AS mapped,
               SUM(CASE WHEN s.status = 'unmapped' THEN 1 ELSE 0 END) AS unmapped
        FROM instructor i
        LEFT JOIN session s ON s.instructor_id = i.id
        GROUP BY i.id
    """).fetchall()

    # 2) 강사별 반 수
    class_rows = conn.execute("""
        SELECT i.id AS instructor_id, COUNT(DISTINCT cl.id) AS classes
        FROM instructor i
        LEFT JOIN class cl ON cl.instructor_id = i.id
        GROUP BY i.id
    """).fetchall()

    # 3) 강사별 학생 시딩 수
    student_rows = conn.execute("""
        SELECT cl.instructor_id, COUNT(*) AS students
        FROM student st
        JOIN class cl ON cl.id = st.class_id
        GROUP BY cl.instructor_id
    """).fetchall()

    # 4) 강사별 리포트 현황 (report는 class_meeting -> class로만 강사와 연결됨)
    report_rows = conn.execute("""
        SELECT cl.instructor_id,
               COUNT(*) AS reports,
               SUM(CASE WHEN r.status = 'draft' THEN 1 ELSE 0 END) AS draft,
               SUM(CASE WHEN r.status = 'review' THEN 1 ELSE 0 END) AS review,
               SUM(CASE WHEN r.status = 'published' THEN 1 ELSE 0 END) AS published
        FROM report r
        JOIN class_meeting cm ON cm.id = r.class_meeting_id
        JOIN class cl ON cl.id = cm.class_id
        GROUP BY cl.instructor_id
    """).fetchall()
    reports_by_instructor = {row["instructor_id"]: row for row in report_rows}
    classes_by_instructor = {row["instructor_id"]: row["classes"] for row in class_rows}
    students_by_instructor = {row["instructor_id"]: row["students"] for row in student_rows}

    instructor_stats = []
    for row in sorted(session_rows, key=lambda r: (r["mapped"] or 0), reverse=True):
        iid = row["instructor_id"]
        rpt = reports_by_instructor.get(iid)
        instructor_stats.append({
            "name": row["name"],
            "mapped": row["mapped"] or 0,
            "unmapped": row["unmapped"] or 0,
            "classes": classes_by_instructor.get(iid, 0),
            "students": students_by_instructor.get(iid, 0),
            "reports": rpt["reports"] if rpt else 0,
            "draft": rpt["draft"] if rpt else 0,
            "review": rpt["review"] if rpt else 0,
            "published": rpt["published"] if rpt else 0,
        })

    totals = {
        "mapped": sum(r["mapped"] for r in instructor_stats),
        "unmapped": sum(r["unmapped"] for r in instructor_stats),
        "reports": sum(r["reports"] for r in instructor_stats),
        "review": sum(r["review"] for r in instructor_stats),
        "published": sum(r["published"] for r in instructor_stats),
    }
    pending_unresolved = conn.execute(
        "SELECT COUNT(*) FROM pending_class_key WHERE resolved = 0"
    ).fetchone()[0]

    # 5) 최근 N주 주간 추이 - class_meeting.meeting_date(실제 수업 날짜) 기준.
    #    report에는 자체 생성일시 컬럼이 없어(스키마상 corrected_at/approved_at뿐),
    #    "그 수업이 있었던 주"를 기준으로 잡는 게 report 집계와도 일관된다.
    meetings_by_week = conn.execute("""
        SELECT strftime('%Y-%W', meeting_date) AS yw,
               MIN(meeting_date) AS week_start, MAX(meeting_date) AS week_end,
               COUNT(*) AS meetings
        FROM class_meeting
        GROUP BY yw
        ORDER BY yw DESC
        LIMIT ?
    """, (weeks,)).fetchall()
    reports_by_week = {
        row["yw"]: row["reports"] for row in conn.execute("""
            SELECT strftime('%Y-%W', cm.meeting_date) AS yw, COUNT(r.id) AS reports
            FROM report r
            JOIN class_meeting cm ON cm.id = r.class_meeting_id
            GROUP BY yw
        """).fetchall()
    }
    weekly_stats = [{
        "week_start": row["week_start"],
        "week_end": row["week_end"],
        "meetings": row["meetings"],
        "reports": reports_by_week.get(row["yw"], 0),
    } for row in meetings_by_week]

    return templates.TemplateResponse("zoom_summaries/stats.html", {
        "request": request,
        "instructor_stats": instructor_stats,
        "totals": totals,
        "pending_unresolved": pending_unresolved,
        "weekly_stats": weekly_stats,
        "weeks": weeks,
    })


@router.get("/{class_meeting_id}", response_class=HTMLResponse)
def zoom_summary_detail(
    request: Request,
    class_meeting_id: int,
    conn: sqlite3.Connection = Depends(get_zoom_db),
):
    class_meeting = conn.execute(
        """
        SELECT cm.id, cm.meeting_date, c.class_code, c.name AS class_name
        FROM class_meeting cm
        JOIN class c ON cm.class_id = c.id
        WHERE cm.id = ?
        """,
        (class_meeting_id,),
    ).fetchone()
    if not class_meeting:
        raise HTTPException(status_code=404, detail="회차를 찾을 수 없습니다.")

    sessions = conn.execute(
        """
        SELECT meeting_uuid, started_at
        FROM session
        WHERE class_meeting_id = ? AND status = 'mapped'
        ORDER BY started_at ASC
        """,
        (class_meeting_id,),
    ).fetchall()

    segments = []
    for s in sessions:
        raw = conn.execute(
            "SELECT payload_json FROM zoom_summary_raw WHERE meeting_uuid = ?", (s["meeting_uuid"],)
        ).fetchone()
        content = ""
        if raw:
            payload = json.loads(raw["payload_json"])
            content = payload.get("summary_content") or payload.get("summary_overview") or ""
        segments.append({
            "started_at_kst": _to_kst(s["started_at"]) if s["started_at"] else None,
            "content": content,
        })

    reports = conn.execute(
        """
        SELECT r.id, r.status, s.name AS student_name
        FROM report r
        JOIN student s ON r.student_id = s.id
        WHERE r.class_meeting_id = ?
        ORDER BY s.name
        """,
        (class_meeting_id,),
    ).fetchall()
    status_counts = {"draft": 0, "review": 0, "published": 0}
    for r in reports:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    return templates.TemplateResponse("zoom_summaries/detail.html", {
        "request": request,
        "class_meeting": dict(class_meeting),
        "segments": segments,
        "reports": reports,
        "status_counts": status_counts,
    })


@router.get("/{class_meeting_id}/reports/{report_id}", response_class=HTMLResponse)
def report_detail(
    request: Request,
    class_meeting_id: int,
    report_id: int,
    conn: sqlite3.Connection = Depends(get_zoom_db),
):
    row = conn.execute(
        """
        SELECT r.id, r.body_md, r.status, r.approved_by, r.approved_at,
               s.name AS student_name, cm.meeting_date, c.class_code, c.name AS class_name
        FROM report r
        JOIN student s ON r.student_id = s.id
        JOIN class_meeting cm ON r.class_meeting_id = cm.id
        JOIN class c ON cm.class_id = c.id
        WHERE r.id = ? AND r.class_meeting_id = ?
        """,
        (report_id, class_meeting_id),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다.")

    qa = conn.execute(
        "SELECT status, fail_reason, body_length, median_length, model, created_at "
        "FROM report_qa WHERE report_id = ?",
        (report_id,),
    ).fetchone()
    qa_flags = []
    if qa:
        qa_flags = conn.execute(
            "SELECT flag_type, sentence FROM report_qa_flag "
            "WHERE report_qa_id = (SELECT id FROM report_qa WHERE report_id = ?) "
            "ORDER BY flag_type",
            (report_id,),
        ).fetchall()

    return templates.TemplateResponse("zoom_summaries/report_detail.html", {
        "request": request,
        "report": dict(row),
        "class_meeting_id": class_meeting_id,
        "qa": dict(qa) if qa else None,
        "qa_flags": qa_flags,
    })


def _get_report_status(conn: sqlite3.Connection, class_meeting_id: int, report_id: int) -> str | None:
    row = conn.execute(
        "SELECT status FROM report WHERE id = ? AND class_meeting_id = ?",
        (report_id, class_meeting_id),
    ).fetchone()
    return row["status"] if row else None


@router.post("/{class_meeting_id}/reports/{report_id}/save")
def report_save(
    class_meeting_id: int,
    report_id: int,
    body_md: str = Form(...),
    conn: sqlite3.Connection = Depends(get_zoom_db_rw),
):
    status = _get_report_status(conn, class_meeting_id, report_id)
    if status is None:
        raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다.")
    if status != "draft":
        raise HTTPException(status_code=400, detail="draft 상태인 리포트만 수정할 수 있습니다.")

    conn.execute("UPDATE report SET body_md = ? WHERE id = ?", (body_md, report_id))
    conn.commit()
    return RedirectResponse(url=f"/zoom-summaries/{class_meeting_id}/reports/{report_id}", status_code=303)


@router.post("/{class_meeting_id}/reports/{report_id}/approve")
def report_approve(
    request: Request,
    class_meeting_id: int,
    report_id: int,
    conn: sqlite3.Connection = Depends(get_zoom_db_rw),
):
    status = _get_report_status(conn, class_meeting_id, report_id)
    if status is None:
        raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다.")
    if status != "draft":
        raise HTTPException(status_code=400, detail="draft 상태인 리포트만 승인할 수 있습니다.")

    user_id = get_current_user_id(request)
    conn.execute(
        "UPDATE report SET status = 'review', approved_by = ?, approved_at = datetime('now') WHERE id = ?",
        (user_id, report_id),
    )
    conn.commit()
    return RedirectResponse(url=f"/zoom-summaries/{class_meeting_id}/reports/{report_id}", status_code=303)


@router.post("/{class_meeting_id}/reports/{report_id}/unapprove")
def report_unapprove(
    class_meeting_id: int,
    report_id: int,
    conn: sqlite3.Connection = Depends(get_zoom_db_rw),
):
    status = _get_report_status(conn, class_meeting_id, report_id)
    if status is None:
        raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다.")
    if status != "review":
        raise HTTPException(status_code=400, detail="review 상태인 리포트만 승인을 취소할 수 있습니다.")

    conn.execute(
        "UPDATE report SET status = 'draft', approved_by = NULL, approved_at = NULL WHERE id = ?",
        (report_id,),
    )
    conn.commit()
    return RedirectResponse(url=f"/zoom-summaries/{class_meeting_id}/reports/{report_id}", status_code=303)
