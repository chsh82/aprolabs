"""
Zoom Server-to-Server OAuth 토큰 발급 + /v2/users 검증 스크립트.

CLAUDE.md "다음 작업 1번" - 대표 계정 자격증명으로 토큰을 받아 계정에
소속된 사용자(강사) 목록이 실제로 조회되는지 확인한다. AI Companion
요약은 type=2(Licensed) 이상에서만 생성되므로, Basic(type=1) 계정이
섞여 있으면 그 강사의 수업은 요약이 안 생길 수 있다는 것도 함께 알린다.

사용법:
    python verify_zoom_auth.py

자격증명은 이 파일과 같은 디렉터리의 .env에서 읽는다
(ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET). 표준 라이브러리만
쓴다(urllib) - 이 스크립트를 위해 requests/python-dotenv를 새로 설치할
필요가 없다.
"""

from __future__ import annotations

import base64
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

def _ensure_utf8(stream):
    """이미 utf-8로 래핑돼 있으면 다시 감싸지 않는다.

    여러 zoom_reports 스크립트가 서로 import할 때(예: collector.py가
    map_sessions.py를 import) 각자 무조건 새 TextIOWrapper를 만들면, 먼저
    만들어진 래퍼가 참조를 잃고 GC되면서 __del__이 하부 buffer까지 닫아버려
    "I/O operation on closed file" 오류가 난다(실제로 겪은 문제).
    """
    if stream.encoding and stream.encoding.lower() == "utf-8":
        return stream
    return io.TextIOWrapper(stream.buffer, encoding="utf-8")


if sys.platform == "win32":
    sys.stdout = _ensure_utf8(sys.stdout)
    sys.stderr = _ensure_utf8(sys.stderr)

OAUTH_TOKEN_URL = "https://zoom.us/oauth/token"
USERS_URL = "https://api.zoom.us/v2/users"
REQUEST_TIMEOUT = 10  # seconds
MAX_RETRIES = 4

# Zoom user.type 코드 (https://developers.zoom.us/docs/api/rest/reference/zoom-api/methods/#operation/users)
USER_TYPE_LABEL = {1: "Basic", 2: "Licensed", 3: "On-prem", 99: "None(SSO 대기)"}


class ZoomAuthError(Exception):
    """토큰 발급 또는 API 호출이 재시도 후에도 실패했을 때."""


def load_env(path: Path) -> dict[str, str]:
    """.env를 최소한으로 직접 파싱한다(표준 라이브러리만 사용).

    'KEY=VALUE' 형태만 인식하고, 빈 줄/주석(#)은 건너뛴다.
    값의 앞뒤 공백과 감싼 따옴표(' 또는 ")는 제거한다.
    """
    env: dict[str, str] = {}
    if not path.exists():
        raise ZoomAuthError(f".env 파일을 찾을 수 없습니다: {path}")

    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            print(f"경고: .env {line_no}번째 줄이 KEY=VALUE 형식이 아니라 건너뜀", file=sys.stderr)
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env[key] = value
    return env


def _http_request_with_retry(req: urllib.request.Request) -> dict:
    """429/5xx는 지수 백오프로 재시도하고, 그 외 HTTP 에러는 본문을 붙여 바로 던진다."""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            if e.code == 429 or e.code >= 500:
                wait = 2 ** attempt
                print(f"  {e.code} 응답 - {wait}초 후 재시도 ({attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
                time.sleep(wait)
                last_error = ZoomAuthError(f"HTTP {e.code}: {body}")
                continue
            raise ZoomAuthError(f"HTTP {e.code}: {body}") from e
        except urllib.error.URLError as e:
            wait = 2 ** attempt
            print(f"  네트워크 오류({e.reason}) - {wait}초 후 재시도 ({attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
            time.sleep(wait)
            last_error = ZoomAuthError(f"네트워크 오류: {e.reason}")
            continue
    raise last_error or ZoomAuthError("알 수 없는 오류로 재시도 소진")


def get_access_token(account_id: str, client_id: str, client_secret: str) -> str:
    """Server-to-Server OAuth 토큰 발급 (account_credentials grant, TTL 1시간)."""
    query = urllib.parse.urlencode({"grant_type": "account_credentials", "account_id": account_id})
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        f"{OAUTH_TOKEN_URL}?{query}",
        method="POST",
        headers={"Authorization": f"Basic {credentials}"},
    )
    data = _http_request_with_retry(req)
    token = data.get("access_token")
    if not token:
        raise ZoomAuthError(f"응답에 access_token이 없음: {data}")
    return token


def list_users(access_token: str) -> list[dict]:
    """/v2/users 전체 페이지를 모아서 반환한다 (표준 엔드포인트, /v2/accounts/{id}/... 아님)."""
    users: list[dict] = []
    next_page_token = ""
    while True:
        params = {"page_size": "100", "status": "active"}
        if next_page_token:
            params["next_page_token"] = next_page_token
        req = urllib.request.Request(
            f"{USERS_URL}?{urllib.parse.urlencode(params)}",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        data = _http_request_with_retry(req)
        users.extend(data.get("users", []))
        next_page_token = data.get("next_page_token", "")
        if not next_page_token:
            break
    return users


def main() -> int:
    env_path = Path(__file__).resolve().parent / ".env"
    try:
        env = load_env(env_path)
        account_id = env.get("ZOOM_ACCOUNT_ID", "")
        client_id = env.get("ZOOM_CLIENT_ID", "")
        client_secret = env.get("ZOOM_CLIENT_SECRET", "")
        missing = [k for k, v in [
            ("ZOOM_ACCOUNT_ID", account_id),
            ("ZOOM_CLIENT_ID", client_id),
            ("ZOOM_CLIENT_SECRET", client_secret),
        ] if not v]
        if missing:
            print(f"오류: .env에 다음 값이 비어있습니다: {', '.join(missing)}", file=sys.stderr)
            return 1

        print("토큰 발급 중...")
        token = get_access_token(account_id, client_id, client_secret)
        print("토큰 발급 성공 (값은 출력하지 않음)")

        print("\n/v2/users 조회 중...")
        users = list_users(token)
        print(f"\n총 {len(users)}명 조회됨\n")

        print(f"{'이름':<12} {'이메일':<32} {'type'}")
        print("-" * 60)
        basic_count = 0
        for u in users:
            user_type = u.get("type")
            label = USER_TYPE_LABEL.get(user_type, str(user_type))
            flag = ""
            if user_type == 1:
                basic_count += 1
                flag = "  ← Basic: AI Companion 요약 미적용"
            print(f"{u.get('first_name', '')}{u.get('last_name', ''):<10} {u.get('email', ''):<32} {label}{flag}")

        if basic_count:
            print(f"\n주의: Basic 계정 {basic_count}명은 AI Companion 요약이 생성되지 않습니다. "
                  "라이선스 배정을 확인하세요.")

        return 0

    except ZoomAuthError as e:
        print(f"\n실패: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
