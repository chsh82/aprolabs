"""읽기 전용 재현 스크립트: 유용하다(literacy.db id=2891)가 /literacy/* 경로에서
실제로 조회되는지 FastAPI TestClient로 직접 확인한다.

실제 비밀번호/세션 토큰을 다루지 않기 위해, auth_middleware가 호출하는
get_current_user_id 함수만 테스트 목적으로 monkeypatch해서 "로그인된 사용자가
있다고 가정했을 때" 어떤 화면이 보이는지 관찰한다(진짜 크리덴셜 생성/노출 없음).
DB는 전혀 쓰지 않는다(GET 요청만 보낸다).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from starlette.testclient import TestClient
import app.main as main_mod

# 테스트 목적으로만: "어떤 로그인된 사용자든" 통과시키는 스텁.
# 실제 비밀번호/토큰은 전혀 사용하지 않음.
def _fake_get_current_user_id(request):
    return "test-user-id-not-a-real-credential"

main_mod.get_current_user_id = _fake_get_current_user_id

client = TestClient(main_mod.app)

print("=== 1) 로그인 없이 /literacy/terms?q=유용하다 ===")
# 실제 미들웨어의 원래 함수로 잠깐 되돌려서 미인증 상태 재확인
import app.auth as auth_mod
real_fn = auth_mod.get_current_user_id
main_mod.get_current_user_id = real_fn
r = client.get("/literacy/terms", params={"q": "유용하다"}, follow_redirects=False)
print("status:", r.status_code, "location:", r.headers.get("location"))

print()
print("=== 2) 로그인된 사용자(스텁)로 /literacy/terms?q=유용하다 ===")
main_mod.get_current_user_id = _fake_get_current_user_id
r = client.get("/literacy/terms", params={"q": "유용하다"}, follow_redirects=False)
print("status:", r.status_code)
body = r.text
idx = body.find("유용하다")
print("응답 본문에 '유용하다' 포함:", idx != -1)
if idx != -1:
    print("주변 텍스트:", body[max(0, idx-50):idx+300].replace("\n", " "))

print()
print("=== 3) /literacy/review/definition?id=2891 (검수 화면, id 직접 지정) ===")
r = client.get("/literacy/review/definition", params={"id": 2891}, follow_redirects=False)
print("status:", r.status_code)
body = r.text
idx = body.find("남의 것이나 이미 용도가 정해져")
print("잘못된 정의 텍스트 노출 여부:", idx != -1)

print()
print("=== 4) /api/literacy/health (학생용 라우터 스텁, 로그인 상태) ===")
r = client.get("/api/literacy/health", follow_redirects=False)
print("status:", r.status_code, r.text)

print()
print("=== 5) /api/literacy/* 에 데이터 반환 엔드포인트가 실제로 존재하는지 라우트 목록 확인 ===")
for route in main_mod.app.routes:
    path = getattr(route, "path", "")
    if path.startswith("/api/literacy") or path.startswith("/literacy"):
        methods = getattr(route, "methods", None)
        print(methods, path)
