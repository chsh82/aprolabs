"""검수 화면·편집 API 로그인 - 2026-09-28 사용자 지시 [5], SSO는 같은 날 후속.

인터넷에 그대로 열려 있던 게 발견되어(방화벽도 꺼져 있었음) 필수로 추가한다.
1차로는 REVIEW_ACCOUNTS 환경변수 하나에 "아이디:비밀번호,아이디:비밀번호"
형식으로 계정을 관리하는 HTTP Basic Auth를 붙였다(사용자 본인 + 동료 교사
몇 명 정도 규모라 DB 테이블 없이 충분).

SSO 통합(옵션 B, 같은 날 후속 지시): 기존 aprolabs.co.kr 사이트(app.main)가
쓰는 세션 쿠키(aprolabs_session, itsdangerous 서명)를 domain=".aprolabs.co.kr"로
넓혀 서브도메인끼리 공유하게 했다 - 이 파일에서 같은 비밀키(SECRET_KEY, 여기선
APROLABS_SESSION_SECRET_KEY로 받음)로 그 쿠키를 검증해서, app.main에 이미
로그인돼 있으면 검수 화면도 다시 로그인할 필요가 없다. 이 쿠키가 없거나
검증 실패하면 REVIEW_ACCOUNTS Basic Auth로 그대로 폴백한다(동료 교사가
app.main 계정이 없어도 검수 화면은 쓸 수 있어야 하므로 폐지하지 않음).
"""
from __future__ import annotations

import os
import secrets
import sqlite3
from pathlib import Path

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

_security = HTTPBasic(auto_error=False)

# app.main(app/auth.py)과 반드시 맞춰야 하는 값들 - 이름/만료기간이 어긋나면
# 서명은 맞아도 해독이 안 되거나(max_age) 쿠키를 못 찾는다(COOKIE_NAME).
_SSO_COOKIE_NAME = "aprolabs_session"
_SSO_MAX_AGE = 60 * 60 * 24 * 30  # app.auth.COOKIE_MAX_AGE_LONG과 동일
_APROLABS_DB_PATH = Path(__file__).resolve().parent.parent.parent / "aprolabs.db"


def _sso_serializer() -> URLSafeTimedSerializer | None:
    key = os.environ.get("APROLABS_SESSION_SECRET_KEY", "")
    return URLSafeTimedSerializer(key) if key else None


def _lookup_user_email(user_id: str) -> str | None:
    """app.main의 users 테이블(같은 서버의 별도 SQLite 파일)에서 표시용
    이메일을 찾는다 - correction_log에 UUID보다 사람이 알아볼 수 있는 값을
    남기기 위함. 못 찾아도 인증 자체는 이미 성공한 상태이므로 user_id를
    그대로 쓴다(완전 실패보다 낫다)."""
    if not _APROLABS_DB_PATH.exists():
        return user_id
    try:
        conn = sqlite3.connect(str(_APROLABS_DB_PATH))
        row = conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
    except sqlite3.Error:
        return user_id
    return row[0] if row else user_id


def _verify_sso_cookie(request: Request) -> str | None:
    token = request.cookies.get(_SSO_COOKIE_NAME)
    if not token:
        return None
    serializer = _sso_serializer()
    if serializer is None:
        return None
    try:
        user_id = serializer.loads(token, max_age=_SSO_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None
    return _lookup_user_email(user_id)


def _accounts() -> dict[str, str]:
    raw = os.environ.get("REVIEW_ACCOUNTS", "")
    accounts: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        user, pw = pair.split(":", 1)
        if user.strip():
            accounts[user.strip()] = pw
    return accounts


def _verify(username: str | None, password: str | None) -> str | None:
    """맞으면 사용자명, 아니면 None. 계정이 없거나 비밀번호가 틀려도 항상
    같은 시간이 걸리게(타이밍 사이드채널 방지) 더미 비교를 한 번 더 한다."""
    accounts = _accounts()
    expected = accounts.get(username or "")
    if expected is not None and password is not None and secrets.compare_digest(password, expected):
        return username
    secrets.compare_digest(password or "", password or "x")
    return None


def get_current_editor(request: Request,
                        credentials: HTTPBasicCredentials | None = Depends(_security)) -> str:
    """편집 API 의존성(Depends) - 검증된 계정명(SSO면 email, 아니면
    REVIEW_ACCOUNTS 아이디)을 돌려주며, 이 값을 그대로 correction_log의
    editor/created_by/resolved_by/approved_by에 써서 "누가 고쳤는지"가
    클라이언트가 보낸 값이 아니라 실제 로그인 계정과 묶이게 한다."""
    sso_user = _verify_sso_cookie(request)
    if sso_user:
        return sso_user
    username = _verify(credentials.username if credentials else None,
                        credentials.password if credentials else None)
    if username is None:
        raise HTTPException(401, "인증이 필요합니다.", headers={"WWW-Authenticate": "Basic"})
    return username


def check_basic_auth(request: Request) -> str | None:
    """StaticFiles 마운트(/review)는 FastAPI Depends를 못 쓰므로, 미들웨어에서
    쿠키·Authorization 헤더를 직접 읽어 검증할 때 쓰는 버전. SSO 쿠키를
    먼저 보고, 없으면 Basic Auth로 폴백한다."""
    sso_user = _verify_sso_cookie(request)
    if sso_user:
        return sso_user
    import base64
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("basic "):
        return None
    try:
        decoded = base64.b64decode(header[6:]).decode("utf-8")
        username, _, password = decoded.partition(":")
    except (ValueError, UnicodeDecodeError):
        return None
    return _verify(username, password)
