"""파트너 세션 인증 - 사용자 지시(2026-09-27) "URL만 알면 접근 가능한 상태는
정확한 지적입니다. 실제 학생 필기가 들어가기 전에 막아야 하므로 우선순위를
올립니다"에 따라 구현한다.

2단 구조:
  1. 파트너 서버 <-> aprolabs: 파트너별 API 키(태블릿 앱엔 절대 안 들어감)로
     POST /api/partner/sessions를 불러 일회용 launch 토큰(유효 몇 분)을 받는다.
  2. 태블릿 웹뷰: /renderer/viewer.html?...&launch=토큰 으로 들어와 그 토큰을
     POST /api/partner/sessions/exchange로 한 번만 세션(httpOnly 쿠키, 유효
     몇 시간)으로 바꾼다. 이후 답안 저장·인식 API는 이 세션으로만 호출된다.

학생 식별은 파트너가 준 가명(partner_student_id)만 쓴다 - momo_b2b_tablet은
학생의 실명이나 다른 개인정보를 모른다.

개발 모드 우회(RUNTIME_AUTH_DISABLED): 파트너 연동 전에 진행 중이던 태블릿
테스트(손글씨 인식 검증)를 막지 않기 위한 임시 탈출구. 이 값이 참이면
런타임 엔드포인트가 기존처럼 세션 없이 동작한다(student_id는 쿼리
파라미터, 없으면 "dev-anonymous") - 실서비스 전에는 반드시 꺼야 한다.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone

from . import db

SESSION_COOKIE_NAME = "momo_session"
LAUNCH_TOKEN_TTL = timedelta(minutes=5)
SESSION_TTL = timedelta(hours=4)


def dev_auth_disabled() -> bool:
    # 함수로 두는 이유: 모듈 임포트 시점이 아니라 호출 시점 환경변수를 보게
    # 해서, 테스트/운영에서 os.environ을 바꿔 가며 검증할 수 있게 한다.
    return os.environ.get("RUNTIME_AUTH_DISABLED", "false").strip().lower() in ("1", "true", "yes")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _hash_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def create_partner(name: str) -> tuple[int, str]:
    """새 파트너를 등록하고 (partner_id, 평문 API 키)를 돌려준다. 평문 키는
    이때만 보이고 DB엔 해시만 저장되므로, 분실하면 재발급(새 파트너 또는
    rotate_partner_key)해야 한다. edition/partner_admin.py CLI에서 쓴다."""
    api_key = f"pk_{secrets.token_urlsafe(32)}"
    conn = db.get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO partner (name, api_key_hash, created_at) VALUES (?,?,?)",
            (name, _hash_key(api_key), _now_iso()),
        )
        conn.commit()
        return cur.lastrowid, api_key
    finally:
        conn.close()


def verify_partner_key(api_key: str) -> int | None:
    """유효하고 비활성화되지 않은 파트너 키면 partner_id, 아니면 None.
    해시 비교는 hmac.compare_digest로 타이밍 공격을 피한다."""
    if not api_key:
        return None
    key_hash = _hash_key(api_key)
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT id, api_key_hash FROM partner WHERE disabled_at IS NULL"
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        if hmac.compare_digest(row["api_key_hash"], key_hash):
            return row["id"]
    return None


def create_launch_token(partner_id: int, partner_student_id: str, edition_id: int) -> tuple[str, int]:
    """일회용 launch 토큰을 만든다. (token, 유효 시간(초))를 돌려준다."""
    token = secrets.token_urlsafe(32)
    now = _now()
    conn = db.get_connection()
    try:
        conn.execute(
            "INSERT INTO launch_token (token, partner_id, partner_student_id, edition_id, "
            "created_at, expires_at) VALUES (?,?,?,?,?,?)",
            (token, partner_id, partner_student_id, edition_id, now.isoformat(),
             (now + LAUNCH_TOKEN_TTL).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    return token, int(LAUNCH_TOKEN_TTL.total_seconds())


class TokenError(Exception):
    """exchange_launch_token 실패 이유 - api.py가 전부 401로 매핑한다."""


def exchange_launch_token(token: str, edition_id: int) -> tuple[str, int]:
    """launch 토큰을 한 번만 세션으로 바꾼다. (session_id, 유효 시간(초))를
    돌려주거나 TokenError(만료/재사용/edition 불일치/존재하지 않음)를 던진다.
    같은 토큰으로 두 번 부르면 두 번째는 반드시 실패해야 한다(재사용 방지) -
    조회와 사용 표시(UPDATE ... WHERE used_at IS NULL)를 한 트랜잭션으로 묶어
    동시에 두 번 들어와도 하나만 성공하게 한다."""
    conn = db.get_connection()
    try:
        row = conn.execute("SELECT * FROM launch_token WHERE token = ?", (token,)).fetchone()
        if row is None:
            raise TokenError("launch token이 존재하지 않음")
        if row["edition_id"] != edition_id:
            raise TokenError("launch token이 이 edition용이 아님")
        if row["used_at"] is not None:
            raise TokenError("이미 사용된 launch token")
        if _parse_iso(row["expires_at"]) < _now():
            raise TokenError("만료된 launch token")

        now = _now()
        cur = conn.execute(
            "UPDATE launch_token SET used_at = ? WHERE token = ? AND used_at IS NULL",
            (now.isoformat(), token),
        )
        if cur.rowcount == 0:
            # 방금 확인한 뒤 다른 요청이 먼저 used_at을 찍은 경우(동시 재사용 시도)
            raise TokenError("이미 사용된 launch token")

        session_id = secrets.token_urlsafe(32)
        conn.execute(
            "INSERT INTO session (id, partner_id, partner_student_id, edition_id, created_at, expires_at) "
            "VALUES (?,?,?,?,?,?)",
            (session_id, row["partner_id"], row["partner_student_id"], row["edition_id"],
             now.isoformat(), (now + SESSION_TTL).isoformat()),
        )
        conn.commit()
        return session_id, int(SESSION_TTL.total_seconds())
    finally:
        conn.close()


def get_session(session_id: str) -> dict | None:
    """세션이 존재하고 만료 전이면 {partner_id, partner_student_id, edition_id},
    아니면 None."""
    if not session_id:
        return None
    conn = db.get_connection()
    try:
        row = conn.execute("SELECT * FROM session WHERE id = ?", (session_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    if _parse_iso(row["expires_at"]) < _now():
        return None
    return {"partner_id": row["partner_id"], "partner_student_id": row["partner_student_id"],
            "edition_id": row["edition_id"]}
