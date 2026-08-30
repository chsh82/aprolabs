"""문해력 DB 학생용 라우터 (동작 확인용 최소 엔드포인트만).

GET /api/literacy/health -> {"status": "ok"}

추후 momoai.kr에서 호출할 때 이 prefix(/api/literacy/*)만 인증 화이트리스트에
넣을 예정 - 지금은 화이트리스트를 수정하지 않으므로 로그인 없이는 접근 안 됨
(app/main.py의 auth_middleware가 그대로 적용됨, 의도된 동작).
"""
from fastapi import APIRouter

router = APIRouter(prefix="/api/literacy")


@router.get("/health")
def health():
    return {"status": "ok"}
