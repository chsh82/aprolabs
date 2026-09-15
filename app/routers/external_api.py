"""외부 서비스(momoai_web)용 줌 요약 조회 API - JSON 전용, 읽기 전용.

GET /api/external/summaries?teacher=...&date=YYYY-MM-DD[&course_code=...]

app/routers/zoom_summaries.py(브라우저 로그인 기반 HTML 화면)와는 인증
경로가 완전히 다르다 - 세션 쿠키 대신 X-API-Key 헤더로만 통과시킨다.
DB 접근은 zoom_summaries.get_zoom_db()(SQLite mode=ro)를 그대로 재사용해
쓰기 자체가 SQLite 단에서 불가능하게 한다 - 이 파일에는 INSERT/UPDATE/
DELETE를 두지 않는다.

개인정보: 요약 본문(summary_text)에 학생 실명이 들어있을 수 있으므로
이 파일 어디에서도 print/logging으로 응답 바디를 찍지 않는다. 쿼리
파라미터(teacher/date/course_code)는 uvicorn 기본 access log에 URL
형태로만 남는다 - 본문은 그 로그에도 없다.
"""
from __future__ import annotations

import json
import os
import sqlite3

from fastapi import APIRouter, Depends, Header, HTTPException

from app.routers.zoom_summaries import _to_kst, get_zoom_db

router = APIRouter(prefix="/api/external")

EMPTY_SUMMARY_THRESHOLD = 20  # 이 글자 수 미만이면 is_empty=True


def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """X-API-Key 헤더가 EXTERNAL_API_KEY와 정확히 일치할 때만 통과.

    서버에 EXTERNAL_API_KEY 자체가 비어있으면(아직 값을 안 넣은 상태)
    어떤 헤더값과도 매치시키지 않는다 - "빈 문자열끼리 일치"로 인증이
    뚫리는 것을 막기 위한 fail-closed 처리.
    """
    expected = os.getenv("EXTERNAL_API_KEY", "")
    if not expected or not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="invalid api key")


def _build_summary_text(conn: sqlite3.Connection, class_meeting_id: int) -> tuple[str, int]:
    """이 회차에 연결된 mapped 세션들의 요약을 시각순으로 이어붙인다.

    app/routers/zoom_summaries.py의 회차 상세 화면(zoom_summaries_detail)이
    쓰는 것과 동일한 조회·조합 로직 - summary_content 우선, 없으면
    summary_overview, 여러 세션이면 [YYYY-MM-DD HH:MM KST] 헤더로 구분해
    이어붙인다.

    (조립된 텍스트, 실제 요약 본문 글자 수)를 함께 반환한다. 글자 수는
    표시용 [KST] 헤더를 뺀 content만 합산한다 - 헤더를 포함해서 세면
    본문이 완전히 비어 있어도 헤더 길이(20자 안팎) 때문에 char_count가
    20을 넘어버려 is_empty 판정이 절대 True가 될 수 없는 버그가 생긴다
    (실제로 테스트 중 발견함).
    """
    sessions = conn.execute(
        """
        SELECT meeting_uuid, started_at
        FROM session
        WHERE class_meeting_id = ? AND status = 'mapped'
        ORDER BY started_at ASC
        """,
        (class_meeting_id,),
    ).fetchall()

    parts = []
    content_len = 0
    for s in sessions:
        raw = conn.execute(
            "SELECT payload_json FROM zoom_summary_raw WHERE meeting_uuid = ?", (s["meeting_uuid"],)
        ).fetchone()
        content = ""
        if raw:
            payload = json.loads(raw["payload_json"])
            content = payload.get("summary_content") or payload.get("summary_overview") or ""
        content_len += len(content)
        if s["started_at"]:
            header = f"[{_to_kst(s['started_at']).strftime('%Y-%m-%d %H:%M')} KST]"
        else:
            header = "[시각 미확인]"
        parts.append(f"{header}\n{content}")

    return "\n\n".join(parts), content_len


@router.get("/summaries")
def get_summaries(
    teacher: str,
    date: str,
    course_code: str | None = None,
    conn: sqlite3.Connection = Depends(get_zoom_db),
    _: None = Depends(verify_api_key),
):
    """teacher(강사명, 정확히 일치·공백 무시) + date(YYYY-MM-DD)로 반을 찾는다.

    course_code가 오면 결과를 추가로 좁히는 용도로만 쓴다(없어도 동작).
    같은 강사가 같은 날 반이 여럿이면 여러 건 반환 - 호출 쪽(momoai)이
    고른다. 조건에 맞는 게 없으면 found=false를 200으로 돌려준다(에러
    아님 - "그 날 그 강사 수업 자체가 아직 안 잡혔다"는 정상 상태다).
    """
    query = """
        SELECT cm.id AS class_meeting_id, c.class_code, c.name AS course_name,
               i.name AS teacher_name, cm.meeting_date
        FROM class_meeting cm
        JOIN class c ON cm.class_id = c.id
        JOIN instructor i ON c.instructor_id = i.id
        WHERE i.name = ? AND cm.meeting_date = ?
    """
    params: list[str] = [teacher.strip(), date.strip()]
    if course_code:
        query += " AND c.class_code = ?"
        params.append(course_code.strip())

    rows = conn.execute(query, params).fetchall()

    results = []
    for row in rows:
        summary_text, char_count = _build_summary_text(conn, row["class_meeting_id"])
        results.append({
            "course_code": row["class_code"],
            "course_name": row["course_name"],
            "teacher": row["teacher_name"],
            "class_date": row["meeting_date"],
            "summary_text": summary_text,
            "char_count": char_count,
            "is_empty": char_count < EMPTY_SUMMARY_THRESHOLD,
        })

    return {"found": len(results) > 0, "count": len(results), "results": results}
