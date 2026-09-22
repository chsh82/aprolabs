"""app/vocabulary_quiz 라우터 전용 관리자 인증 - review.py와 quiz.py가 공유.

기존 auth_middleware(app/main.py)가 앱 전체에 로그인을 강제하지만 관리자
여부는 구분하지 않는다(지금까지 계정이 admin 1명뿐이라 구분할 필요가
없었음). 이 모듈의 화면들은 아직 학생 비공개인 콘텐츠를 다루므로 명시적
is_admin 확인을 한 번 더 둔다."""
from __future__ import annotations

from fastapi import HTTPException, Request

from app.auth import get_current_user_id
from app.database import get_db
from app.models.user import User

NOINDEX_HEADERS = {"X-Robots-Tag": "noindex, nofollow", "Cache-Control": "private, no-store"}


def require_admin(request: Request) -> str:
    user_id = get_current_user_id(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다")
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == user_id).first()
    finally:
        db.close()
    if not user or not user.is_admin:
        raise HTTPException(status_code=403, detail="관리자만 접근할 수 있습니다")
    return user_id
