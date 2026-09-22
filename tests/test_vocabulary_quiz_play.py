"""어휘 퀴즈 MVP 회귀 테스트 - 관리자 전용, 로컬 R&D DB 대상.

tests/test_segmenter_regression.py와 같은 방식(독립 스크립트, pytest 없음).
FastAPI TestClient로 실제 앱을 띄워 로그인/권한/출제/채점/세션 흐름을
end-to-end로 검증한다. 사용하는 관리자 계정(admin@aprolabs.co.kr)은 이
저장소의 부트스트랩 관리자 - 테스트가 남긴 세션/attempts는 끝에 정리한다.

실행:
    python tests/test_vocabulary_quiz_play.py
"""
from __future__ import annotations

import io
import re
import sqlite3
import sys
import uuid
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.auth import make_session_cookie, COOKIE_NAME, hash_password  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.user import User  # noqa: E402
from app.vocabulary_quiz.db import get_db_path  # noqa: E402

ADMIN_ID = "de86bad0-e684-457e-8793-075785a65d05"

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def run() -> bool:
    db_path = get_db_path()
    idiom_db = REPO_ROOT / "data" / "vocab" / "idiom.db"
    import hashlib

    def sha(p):
        h = hashlib.sha256()
        h.update(p.read_bytes())
        return h.hexdigest()

    idiom_before = sha(idiom_db) if idiom_db.exists() else None

    test_uid = None
    session_id = None
    try:
        # ---------- 1. 비로그인 접근 차단 ----------
        c0 = TestClient(app, follow_redirects=False)
        r = c0.get("/vocabulary-quiz/play")
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 GET /play 차단(302->login)")
        r = c0.post("/vocabulary-quiz/play")
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 POST /play 차단(302->login)")

        # ---------- 2. 일반 사용자 접근 차단 ----------
        db = next(get_db())
        test_uid = str(uuid.uuid4())
        db.add(User(id=test_uid, email="vqplay.nonadmin.test@example.com",
                     hashed_pw=hash_password("x"), is_admin=False))
        db.commit()
        db.close()

        c1 = TestClient(app, follow_redirects=False)
        c1.cookies.set(COOKIE_NAME, make_session_cookie(test_uid))
        r = c1.get("/vocabulary-quiz/play")
        check(r.status_code == 403, "일반(비관리자) 사용자 GET /play 차단(403)")

        # ---------- 3. 관리자 접근 성공 + 세션 시작 ----------
        client = TestClient(app, follow_redirects=True)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        r = client.get("/vocabulary-quiz/play")
        check(r.status_code == 200, "관리자 GET /play 성공")

        r = client.post("/vocabulary-quiz/play")
        check(r.status_code == 200, "관리자 POST /play(세션 생성) 성공")
        m = re.search(r"/vocabulary-quiz/session/([0-9a-f-]{36})", str(r.url))
        check(m is not None, "세션 생성 후 /session/<id>로 리디렉션됨")
        session_id = m.group(1)

        # ---------- 4. 20문항 중복 없음 + AUTO_HOLD 0개 ----------
        conn = sqlite3.connect(db_path)
        attempts = conn.execute(
            "SELECT item_id, correct_option FROM vocabulary_quiz_attempts WHERE session_id=? ORDER BY order_index",
            (session_id,),
        ).fetchall()
        check(len(attempts) == 20, f"세션에 문항 20개 생성됨(실제 {len(attempts)}개)")
        item_ids = [a[0] for a in attempts]
        check(len(set(item_ids)) == len(item_ids), "20문항 중복 없음")

        placeholders = ",".join("?" * len(item_ids))
        statuses = conn.execute(
            f"""SELECT DISTINCT c.generation_status FROM vocabulary_items i
                JOIN vocabulary_contents c ON c.content_id = i.content_id
                WHERE i.item_id IN ({placeholders})""",
            item_ids,
        ).fetchall()
        check(all(s[0] in ("PRIVATE_SERVER_READY", "PRIVATE_SERVER_READY_CANDIDATE") for s in statuses),
              "출제 문항 전부 PRIVATE_SERVER_READY(_CANDIDATE) - AUTO_HOLD 0개")

        # ---------- 5. 정답 채점 정확성 ----------
        first_item_id, first_correct = attempts[0]
        r = client.post(f"/vocabulary-quiz/session/{session_id}/answer",
                         data={"item_id": first_item_id, "selected_option": first_correct})
        check(r.status_code == 200 and "정답입니다" in r.text, "정답 제출 시 올바르게 채점됨")

        # ---------- 6. 답변 변경 차단 ----------
        r = client.post(f"/vocabulary-quiz/session/{session_id}/answer",
                         data={"item_id": first_item_id, "selected_option": first_correct}, follow_redirects=False)
        check(r.status_code == 409, "이미 답변한 문항 재제출 차단(409)")

        # ---------- 7. 오답 케이스 ----------
        second_item_id, second_correct = attempts[1]
        wrong = 1 if second_correct != 1 else 2
        r = client.post(f"/vocabulary-quiz/session/{session_id}/answer",
                         data={"item_id": second_item_id, "selected_option": wrong})
        check("오답입니다" in r.text, "오답 제출 시 올바르게 채점됨")

        # ---------- 8. 세션 재접속(다음 미응답 문항) ----------
        r = client.get(f"/vocabulary-quiz/session/{session_id}")
        check(r.status_code == 200 and "문제 3 / 20" in r.text, "재접속 시 다음 미응답 문항(3번)으로 정상 이동")

        # ---------- 9. 나머지 18문항 제출(정답으로) ----------
        remaining = conn.execute(
            "SELECT item_id, correct_option FROM vocabulary_quiz_attempts WHERE session_id=? AND order_index > 2 ORDER BY order_index",
            (session_id,),
        ).fetchall()
        for iid, correct in remaining:
            client.post(f"/vocabulary-quiz/session/{session_id}/answer", data={"item_id": iid, "selected_option": correct})

        # ---------- 10. 완료된 세션 재응답 차단 ----------
        r = client.post(f"/vocabulary-quiz/session/{session_id}/answer",
                         data={"item_id": remaining[-1][0], "selected_option": remaining[-1][1]}, follow_redirects=False)
        check(r.status_code == 409, "완료된 세션에 재응답 차단(409)")

        # ---------- 11. 결과 집계 정확성 ----------
        r = client.get(f"/vocabulary-quiz/session/{session_id}/result")
        check(r.status_code == 200, "결과 화면 정상 응답")
        conn2 = sqlite3.connect(db_path)
        correct_count, status = conn2.execute(
            "SELECT correct_count, status FROM vocabulary_quiz_sessions WHERE id=?", (session_id,)
        ).fetchone()
        check(correct_count == 19, f"20문항 중 19개 정답 집계됨(실제 {correct_count})")
        check(status == "completed", "세션 status='completed'로 전환됨")

        # ---------- 12. student_exposure/public_ready 불변 ----------
        n_exp = conn2.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE student_exposure=1").fetchone()[0]
        n_pub = conn2.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE public_ready=1").fetchone()[0]
        check(n_exp == 0 and n_pub == 0, "퀴즈 진행 중 student_exposure/public_ready 변경 없음")

        # ---------- 13. 기존 검수 화면 정상 ----------
        r = client.get("/vocabulary-quiz/review?version=2.1.29")
        check(r.status_code == 200, "기존 QA 검수 허브 정상 응답")

        # ---------- 14. idiom.db 체크섬 불변 ----------
        idiom_after = sha(idiom_db) if idiom_db.exists() else None
        check(idiom_before == idiom_after, "기존 idiom.db 체크섬 불변")

        conn.close()
        conn2.close()

    finally:
        # 테스트가 남긴 세션/응답 정리, 비관리자 테스트 계정 삭제
        conn3 = sqlite3.connect(db_path)
        conn3.execute("PRAGMA foreign_keys=ON")
        if session_id:
            conn3.execute("DELETE FROM vocabulary_quiz_attempts WHERE session_id=?", (session_id,))
            conn3.execute("DELETE FROM vocabulary_quiz_sessions WHERE id=?", (session_id,))
            conn3.commit()
        conn3.close()
        if test_uid:
            db = next(get_db())
            db.query(User).filter(User.id == test_uid).delete()
            db.commit()
            db.close()

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
