from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse
import hashlib, os, secrets

SECRET_KEY = os.environ.get("SECRET_KEY", "aprolabs-secret-key-change-in-prod")
COOKIE_NAME = "aprolabs_session"
COOKIE_MAX_AGE = 60 * 60 * 24 * 7  # 7일

serializer = URLSafeTimedSerializer(SECRET_KEY)

_ITERATIONS = 260_000


def hash_password(pw: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), _ITERATIONS)
    return f"pbkdf2:{salt}:{dk.hex()}"


def verify_password(plain: str, hashed: str) -> bool:
    try:
        _, salt, dk_hex = hashed.split(":")
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt.encode(), _ITERATIONS)
    return secrets.compare_digest(dk.hex(), dk_hex)


def make_session_cookie(user_id: str) -> str:
    return serializer.dumps(user_id)


def decode_session_cookie(token: str) -> str | None:
    try:
        return serializer.loads(token, max_age=COOKIE_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def get_current_user_id(request: Request) -> str | None:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    return decode_session_cookie(token)


def require_login(request: Request):
    """라우터 dependency — 미로그인 시 /login 으로 리디렉션"""
    user_id = get_current_user_id(request)
    if not user_id:
        raise HTTPException(status_code=307,
                            headers={"Location": f"/login?next={request.url.path}"})
    return user_id
