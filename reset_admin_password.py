"""관리자 비밀번호 리셋 스크립트. 서버에서 직접 실행."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app.database import get_db
from app.models.user import User
from app.auth import hash_password

NEW_PW = "apro0914@"
ADMIN_EMAIL = "admin@aprolabs.co.kr"

db = next(get_db())
try:
    admin = db.query(User).filter(User.email == ADMIN_EMAIL).first()
    if not admin:
        print(f"[ERROR] {ADMIN_EMAIL} 계정 없음")
        sys.exit(1)
    admin.hashed_pw = hash_password(NEW_PW)
    db.commit()
    print(f"[OK] {ADMIN_EMAIL} 비밀번호 변경 완료")
finally:
    db.close()
