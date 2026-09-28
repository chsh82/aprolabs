"""검수 화면·편집 API 로그인 - 2026-09-28 사용자 지시 [5].

인터넷에 그대로 열려 있던 게 발견되어(방화벽도 꺼져 있었음) 필수로 추가한다.
계정은 REVIEW_ACCOUNTS 환경변수 하나에 "아이디:비밀번호,아이디:비밀번호"
형식으로 관리한다 - 사용자 본인 + 동료 교사 몇 명 정도 규모라 DB 테이블 없이
이 정도로 충분하고, 계정 추가/삭제도 환경변수 수정 + 재시작으로 끝난다.

HTTP Basic Auth를 쓴다(브라우저 기본 로그인창, 태블릿·노트북 어디서나 동작,
세션/쿠키 인프라 불필요) - HTTPS 뒤에서 서비스되는 게 전제(그 앞엔 항상
https://다.
"""
from __future__ import annotations

import os
import secrets

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials

_security = HTTPBasic(auto_error=False)


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


def get_current_editor(credentials: HTTPBasicCredentials | None = Depends(_security)) -> str:
    """편집 API 의존성(Depends) - 검증된 계정명을 돌려주며, 이 값을 그대로
    correction_log의 editor/created_by/resolved_by/approved_by에 써서
    "누가 고쳤는지"가 클라이언트가 보낸 값이 아니라 실제 로그인 계정과
    묶이게 한다."""
    username = _verify(credentials.username if credentials else None,
                        credentials.password if credentials else None)
    if username is None:
        raise HTTPException(401, "인증이 필요합니다.", headers={"WWW-Authenticate": "Basic"})
    return username


def check_basic_auth(request: Request) -> str | None:
    """StaticFiles 마운트(/review)는 FastAPI Depends를 못 쓰므로, 미들웨어에서
    Authorization 헤더를 직접 읆어 검증할 때 쓰는 버전."""
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
