# -*- coding: utf-8 -*-
"""Phase22 항목4 - 관리자 화면(play_page)이 "파일럿 후보 0건"(정상 데이터 상태)과
"배치 무결성 오류"(매니페스트 누락/40건 불일치 - 실제 조치가 필요한 배포·데이터
문제)를 구분해서 관리자가 이해할 수 있는 메시지로 보여주는지 검증한다.

phase20까지는 두 경우 모두 체크박스만 비활성화되고 이유가 화면에 드러나지
않았다(로그에만 남음) - phase22가 이를 화면에 표시하도록 고쳤다.

tests/test_admin_level_quiz.py / tests/test_phase19_admin_pilot_mode.py와 같은
방식(pytest 없음, FastAPI TestClient, [PASS]/[FAIL] 출력).

실행:
    VOCABULARY_QUIZ_DB_PATH=<research DB 사본 경로> python tests/test_phase22_admin_error_message.py
(환경변수를 생략하면 DEFAULT_COPY_DB_PATH를 사용한다)
"""
from __future__ import annotations

import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

DEFAULT_COPY_DB_PATH = (
    Path.home() / "AppData" / "Local" / "Temp" / "claude" / "C--Users-aproa" /
    "6e2093a4-82bf-4ee0-833a-e5c458074f06" / "scratchpad" / "vocabulary_quiz_research_copy.db"
)
COPY_DB_PATH = Path(os.environ.get("VOCABULARY_QUIZ_DB_PATH") or DEFAULT_COPY_DB_PATH)
os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(COPY_DB_PATH)

if not COPY_DB_PATH.exists():
    print(f"[FAIL] 사전조건: research DB 사본이 없습니다({COPY_DB_PATH}) - "
          f"먼저 scp로 vocabulary_quiz_research.db 사본을 준비하세요.")
    sys.exit(1)

from fastapi.testclient import TestClient  # noqa: E402
from app.main import app  # noqa: E402
from app.auth import make_session_cookie, COOKIE_NAME  # noqa: E402
from app.vocabulary_quiz.routers import multiformat as mf  # noqa: E402

ADMIN_ID = "de86bad0-e684-457e-8793-075785a65d05"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def run() -> bool:
    admin = TestClient(app, follow_redirects=False)
    admin.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))

    # 1) 정상 상태: 무결성 오류 메시지가 없어야 한다.
    r = admin.get("/vocabulary-quiz/multiformat/play")
    check(r.status_code == 200, "정상 상태 play_page 200")
    html = r.text
    check("파일럿 배치 무결성 오류" not in html, "정상 상태: 무결성 오류 메시지 없음")
    check("L4·L5 파일럿 문항만 출제" in html, "정상 상태: 파일럿 체크박스 노출")

    # 2) 매니페스트를 존재하지 않는 경로로 바꿔 무결성 오류를 재현.
    orig_path = mf.PILOT_MANIFEST_PATH
    mf.PILOT_MANIFEST_PATH = Path("nonexistent_manifest_for_test.json")
    mf._pilot_manifest_rows_cache = None
    try:
        r2 = admin.get("/vocabulary-quiz/multiformat/play")
        check(r2.status_code == 200, "매니페스트 누락 상태에서도 play_page는 200(화면 전체 안 깨짐)")
        html2 = r2.text
        check("파일럿 배치 무결성 오류" in html2, "매니페스트 누락 상태: 무결성 오류 문구가 화면에 노출됨")
        check("text-red-600" in html2, "매니페스트 누락 상태: 경고(빨간색) 스타일 적용")
        after_checkbox = html2.split("pilot-mode-checkbox", 1)[1][:50] if "pilot-mode-checkbox" in html2 else ""
        check("disabled" in after_checkbox, "매니페스트 누락 상태: 체크박스 비활성화")
    finally:
        mf.PILOT_MANIFEST_PATH = orig_path
        mf._pilot_manifest_rows_cache = None

    # 3) 정상 상태로 복원 확인(캐시 초기화 후 다시 정상 렌더링되는지).
    r3 = admin.get("/vocabulary-quiz/multiformat/play")
    check(r3.status_code == 200 and "파일럿 배치 무결성 오류" not in r3.text,
          "매니페스트 원복 후 다시 정상 렌더링(무결성 오류 메시지 사라짐)")

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n{'[PASS]' if n_pass == len(_results) else '[FAIL]'} 총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
