"""파트너(momolib 등)에게 진행 요약을 통지 - 서버 간 통신, 브라우저를 거치지
않는다(2026-09-28 사용자 설계). "같은 데이터를 두 곳에 쌓지 않는다" 원칙에
따라 상세 답안·필기는 여기서 보내지 않고 요약(진행률/완료 여부/마지막
활동 시각)만 보낸다 - 파트너는 이걸 자기 진도 테이블에 갱신만 하면 된다.
"""
from __future__ import annotations

import logging

import httpx

from . import db, store

logger = logging.getLogger(__name__)

_COMPLETE_THRESHOLD = 0.95  # 표본 몇 개는 학생이 실수로 안 건드릴 수 있어 100%를 요구하지 않음
_NOTIFY_TIMEOUT = 5.0


def _count_answerable_items(layout: dict) -> int:
    """layout_json을 걷어 "문항 단위" 개수를 센다(part_id 1:1은 아니지만 -
    text_short_multi의 blanks 여러 개는 하나로 셈 - 요약용 진행률이라 이
    정도 근사면 충분하다는 판단, 2026-09-28)."""
    n = 0
    for p in layout.get("pages", []):
        if isinstance(p.get("q"), dict):
            n += 1
        if isinstance(p.get("qs"), list):
            n += len(p["qs"])
        if isinstance(p.get("ox"), list):
            n += len(p["ox"])
        if isinstance(p.get("vocab"), list):
            n += len(p["vocab"])
    return n


def progress_summary(edition_id: int, student_id: str) -> dict:
    """{progress(0~1), completed, last_activity_at} - 없으면 progress=0."""
    row = store.get_edition_row(edition_id)
    total = _count_answerable_items(__import__("json").loads(row["layout_json"])) if row else 0

    conn = db.get_connection()
    try:
        answered = conn.execute(
            "SELECT count(*) AS c, max(updated_at) AS last FROM student_answer "
            "WHERE edition_id = ? AND student_id = ? "
            "AND (ink_json IS NOT NULL OR (text IS NOT NULL AND text != ''))",
            (edition_id, student_id),
        ).fetchone()
    finally:
        conn.close()

    answered_n = answered["c"] or 0
    progress = min(1.0, answered_n / total) if total else 0.0
    return {
        "progress": round(progress, 4),
        "completed": progress >= _COMPLETE_THRESHOLD,
        "last_activity_at": answered["last"],
    }


async def notify_partner_progress(partner_id: int, partner_student_id: str, edition_id: int) -> bool:
    """partner 테이블에 notify_url이 설정돼 있으면 진행 요약을 POST한다.
    실패해도 학생 화면 흐름을 막으면 안 되므로(네트워크 문제로 "학습 목록
    으로" 버튼이 안 눌리면 곤란) 예외를 삼키고 False만 돌려준다 - 호출자는
    결과를 무시해도 된다(재시도는 이번 범위 밖 - 다음 통지 때 최신 값으로
    덮어써지므로 유실보다는 지연 문제)."""
    conn = db.get_connection()
    try:
        partner = conn.execute(
            "SELECT notify_url, notify_secret FROM partner WHERE id = ?", (partner_id,)
        ).fetchone()
    finally:
        conn.close()
    if partner is None or not partner["notify_url"]:
        return False

    summary = progress_summary(edition_id, partner_student_id)
    payload = {
        "partner_student_id": partner_student_id,
        "edition_id": edition_id,
        **summary,
    }
    headers = {"X-Aprolabs-Notify-Key": partner["notify_secret"] or ""}
    try:
        async with httpx.AsyncClient(timeout=_NOTIFY_TIMEOUT) as client:
            resp = await client.post(partner["notify_url"], json=payload, headers=headers)
        if resp.status_code >= 400:
            logger.warning("진행 통지 실패(%s): %s %s", partner["notify_url"], resp.status_code, resp.text[:200])
            return False
        return True
    except httpx.HTTPError as e:
        logger.warning("진행 통지 중 네트워크 오류(%s): %s", partner["notify_url"], e)
        return False
