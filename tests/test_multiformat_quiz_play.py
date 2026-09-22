"""초등 다유형 어휘 퀴즈(파일럿) API/화면 회귀 테스트 - 관리자 전용, 로컬 R&D DB 대상.

tests/test_vocabulary_quiz_play.py와 같은 방식(pytest 없음, TestClient로 실제
앱 end-to-end). 로컬 vocabulary_quiz_rnd.db에 이미 적재된 1,189개 파일럿
문항(scripts/vocab/import_multiformat_quiz.py --apply로 적재됨)을 대상으로
로그인/권한/5개 유형 출제·채점/세션 흐름/정답 비노출/기존 화면 회귀를 검증한다.

실행:
    python tests/test_multiformat_quiz_play.py
"""
from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import sys
import unicodedata
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
from app.vocabulary_quiz.routers import multiformat as mf  # noqa: E402

ADMIN_ID = "de86bad0-e684-457e-8793-075785a65d05"

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def sha(p: Path):
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


class _FakeItem:
    def __init__(self, item_type: str, answer_payload: dict, options=None, correct_option=None):
        self.item_type = item_type
        self.answer_payload_json = json.dumps(answer_payload, ensure_ascii=False)
        self.options_json = json.dumps(options, ensure_ascii=False) if options else None
        self.correct_option = correct_option


class _FakeAnswerBody:
    def __init__(self, item_id="X", selected_option=None, answer_text=None, answers=None):
        self.item_id = item_id
        self.selected_option = selected_option
        self.answer_text = answer_text
        self.answers = answers


def run() -> bool:
    db_path = get_db_path()
    idiom_db = REPO_ROOT / "data" / "vocab" / "idiom.db"
    idiom_before = sha(idiom_db) if idiom_db.exists() else None

    conn0 = sqlite3.connect(db_path)
    counts_before = {
        t: conn0.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("vocabulary_contents", "vocabulary_items", "vocabulary_review_samples",
                  "vocabulary_multiformat_items")
    }
    conn0.close()
    check(counts_before["vocabulary_multiformat_items"] >= 1189,
          f"로컬 DB에 다유형 파일럿 문항이 이미 적재돼 있음(실제 {counts_before['vocabulary_multiformat_items']}건)")

    test_uid = None
    session_ids: list[str] = []

    try:
        # ---------- 0. 채점 단위 테스트(_grade) - 실제 API 없이도 규칙 검증 ----------
        choice_item = _FakeItem("MEANING_CHOICE", {"correct_option": 2}, options=["a", "b", "c", "d"], correct_option=2)
        r = mf._grade(choice_item, _FakeAnswerBody(selected_option=2))
        check(r["is_correct"] == 1, "선택형 채점 - 정답 선택 시 1")
        r = mf._grade(choice_item, _FakeAnswerBody(selected_option=1))
        check(r["is_correct"] == 0, "선택형 채점 - 오답 선택 시 0")
        try:
            mf._grade(choice_item, _FakeAnswerBody(selected_option=9))
            check(False, "선택형 채점 - selected_option 범위 밖이면 400")
        except Exception as e:
            check(getattr(e, "status_code", None) == 400, "선택형 채점 - selected_option 범위 밖이면 400")

        cloze_item = _FakeItem("CONTEXT_CLOZE", {"answer_text": "사과", "accepted_answers": ["사과"], "input_hint": "2글자"})
        r = mf._grade(cloze_item, _FakeAnswerBody(answer_text="사과"))
        check(r["is_correct"] == 1, "직접입력형 채점 - 정확히 일치하면 1")
        r = mf._grade(cloze_item, _FakeAnswerBody(answer_text="  사과  "))
        check(r["is_correct"] == 1, "직접입력형 채점 - 앞뒤 공백 제거 후 비교")
        nfd_text = unicodedata.normalize("NFD", "사과")
        r = mf._grade(cloze_item, _FakeAnswerBody(answer_text=nfd_text))
        check(r["is_correct"] == 1, "직접입력형 채점 - Unicode NFD 입력도 NFC 정규화 후 일치하면 정답")
        r = mf._grade(cloze_item, _FakeAnswerBody(answer_text="바나나"))
        check(r["is_correct"] == 0, "직접입력형 채점 - 불일치하면 0")

        match_item = _FakeItem("MATCH_WORD_MEANING", {
            "words": ["w1", "w2", "w3", "w4"], "definitions": ["d1", "d2", "d3", "d4"],
            "answers": {"w1": "d1", "w2": "d2", "w3": "d3", "w4": "d4"},
        })
        r = mf._grade(match_item, _FakeAnswerBody(answers={"w1": "d1", "w2": "d2", "w3": "d3", "w4": "d4"}))
        check(r["is_correct"] == 1 and r["correct_count"] == 4, "연결형 채점 - 4쌍 전부 맞으면 is_correct=1")
        r = mf._grade(match_item, _FakeAnswerBody(answers={"w1": "d1", "w2": "dX", "w3": "d3", "w4": "d4"}))
        check(r["is_correct"] == 0 and r["correct_count"] == 3, "연결형 채점 - 부분 정답 시 correct_count만 반영, is_correct=0")

        # ---------- 1. 비로그인 접근 차단 ----------
        c0 = TestClient(app, follow_redirects=False)
        r = c0.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 GET /multiformat/play 차단(302->login)")
        r = c0.post("/api/vocabulary-quiz/sessions", json={"question_count": 3})
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 POST /api/.../sessions 차단(302->login)")

        # ---------- 2. 일반 사용자 접근 차단 ----------
        db = next(get_db())
        test_uid = str(uuid.uuid4())
        db.add(User(id=test_uid, email="vqmf.nonadmin.test@example.com",
                     hashed_pw=hash_password("x"), is_admin=False))
        db.commit()
        db.close()

        c1 = TestClient(app, follow_redirects=False)
        c1.cookies.set(COOKIE_NAME, make_session_cookie(test_uid))
        r = c1.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 403, "일반(비관리자) 사용자 GET /multiformat/play 차단(403)")
        r = c1.post("/api/vocabulary-quiz/sessions", json={"question_count": 3})
        check(r.status_code == 403, "일반(비관리자) 사용자 POST /api/.../sessions 차단(403)")

        # ---------- 3. 관리자: 화면 접근 ----------
        client = TestClient(app, follow_redirects=True)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        r = client.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 200, "관리자 GET /multiformat/play 성공")

        # ---------- 4. 유형별 개별 세션(1문항)으로 5유형 각각 출제/채점 검증 ----------
        for item_type in mf.ITEM_TYPES:
            r = client.post("/api/vocabulary-quiz/sessions", json={"item_types": [item_type], "question_count": 1})
            check(r.status_code == 200, f"[{item_type}] 세션 생성 성공")
            sid = r.json()["session_id"]
            session_ids.append(sid)

            r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/next")
            check(r.status_code == 200 and r.json()["done"] is False, f"[{item_type}] 다음 문항 조회 성공")
            item = r.json()["item"]
            check(item["item_type"] == item_type, f"[{item_type}] 요청한 유형과 출제된 유형 일치")

            # 정답 필드 비노출 확인
            secret_keys = {"correct_option", "answer_text", "accepted_answers", "answers"}
            leaked = secret_keys & set(item.keys())
            check(not leaked, f"[{item_type}] 문제 조회 응답에 정답 필드 비노출({leaked or '없음'})")

            # DB에서 실제 정답을 가져와 정답 제출
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT answer_payload_json, options_json FROM vocabulary_multiformat_items WHERE item_id=?",
                (item["item_id"],),
            ).fetchone()
            conn.close()
            payload = json.loads(row[0])

            if item_type in mf.CHOICE_TYPES:
                body = {"item_id": item["item_id"], "selected_option": payload["correct_option"]}
            elif item_type == "CONTEXT_CLOZE":
                body = {"item_id": item["item_id"], "answer_text": payload["answer_text"]}
            else:
                body = {"item_id": item["item_id"], "answers": payload["answers"]}

            r = client.post(f"/api/vocabulary-quiz/sessions/{sid}/answer", json=body)
            check(r.status_code == 200, f"[{item_type}] 정답 제출 성공")
            data = r.json()
            check(data["is_correct"] is True, f"[{item_type}] 정답 제출 시 is_correct=true")
            check(data["is_last"] is True, f"[{item_type}] 1문항 세션 마지막 문항 완료 처리(is_last=true)")

            # 이미 답변한 문항 재제출 차단
            r = client.post(f"/api/vocabulary-quiz/sessions/{sid}/answer", json=body)
            check(r.status_code == 409, f"[{item_type}] 완료된 세션 재제출 차단(409)")

            r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/result")
            check(r.status_code == 200 and r.json()["correct"] == 1, f"[{item_type}] 결과 집계 정확(1/1 정답)")

        # ---------- 5. 혼합 세션(10문항) - 중복 없음, 진행률, 완료 처리 ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"question_count": 10})
        check(r.status_code == 200, "혼합(전체 유형) 세션 생성 성공")
        mixed_sid = r.json()["session_id"]
        session_ids.append(mixed_sid)

        conn = sqlite3.connect(db_path)
        item_ids = [row[0] for row in conn.execute(
            "SELECT item_id FROM vocabulary_multiformat_responses WHERE session_id=? ORDER BY order_index",
            (mixed_sid,),
        ).fetchall()]
        check(len(item_ids) == 10, f"세션에 문항 10개 생성됨(실제 {len(item_ids)}개)")
        check(len(set(item_ids)) == len(item_ids), "10문항 중복 없음")

        source_versions = {row[0] for row in conn.execute(
            "SELECT DISTINCT source_version FROM vocabulary_multiformat_items WHERE item_id IN (%s)"
            % ",".join("?" * len(item_ids)), item_ids,
        ).fetchall()}
        conn.close()
        check(source_versions == {"2.1.29"}, "출제 문항 전부 source_version=2.1.29")

        last_is_last = None
        for i in range(10):
            r = client.get(f"/api/vocabulary-quiz/sessions/{mixed_sid}/next")
            data = r.json()
            check(data["done"] is False, f"혼합 세션 {i+1}번째 문항 조회 성공")
            item = data["item"]
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT answer_payload_json FROM vocabulary_multiformat_items WHERE item_id=?", (item["item_id"],)
            ).fetchone()
            conn.close()
            payload = json.loads(row[0])
            if item["item_type"] in mf.CHOICE_TYPES:
                body = {"item_id": item["item_id"], "selected_option": 1 if payload["correct_option"] != 1 else 2}
            elif item["item_type"] == "CONTEXT_CLOZE":
                body = {"item_id": item["item_id"], "answer_text": "오답문자열zzz"}
            else:
                body = {"item_id": item["item_id"], "answers": {}}
            r = client.post(f"/api/vocabulary-quiz/sessions/{mixed_sid}/answer", json=body)
            last_is_last = r.json()["is_last"]
        check(last_is_last is True, "혼합 세션 10번째(마지막) 문항 제출 시 is_last=true")

        r = client.get(f"/api/vocabulary-quiz/sessions/{mixed_sid}/next")
        check(r.json()["done"] is True, "완료된 세션은 next 조회 시 done=true")

        r = client.get(f"/api/vocabulary-quiz/sessions/{mixed_sid}/result")
        result = r.json()
        check(result["correct"] == 0, "전부 오답 제출한 혼합 세션 - 정답 0건 집계")
        check(set(result["by_type"].keys()) <= set(mf.ITEM_TYPES), "유형별 결과 키가 알려진 5유형 내에 있음")
        check(len(result["wrong_items"]) == 10, "오답 10건 모두 wrong_items에 기록됨")

        # ---------- 6. 세션 소유권/존재 검사 ----------
        other_uid = str(uuid.uuid4())
        db = next(get_db())
        db.add(User(id=other_uid, email="vqmf.other.admin.test@example.com",
                     hashed_pw=hash_password("x"), is_admin=True))
        db.commit()
        db.close()
        c2 = TestClient(app, follow_redirects=True)
        c2.cookies.set(COOKIE_NAME, make_session_cookie(other_uid))
        r = c2.get(f"/api/vocabulary-quiz/sessions/{mixed_sid}/next")
        check(r.status_code == 403, "다른 관리자 세션 접근 차단(403)")
        r = client.get(f"/api/vocabulary-quiz/sessions/does-not-exist/next")
        check(r.status_code == 404, "존재하지 않는 세션 조회 시 404")
        db = next(get_db())
        db.query(User).filter(User.id == other_uid).delete()
        db.commit()
        db.close()

        # ---------- 7. 알 수 없는 item_type 요청 차단 ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"item_types": ["NOT_A_TYPE"], "question_count": 1})
        check(r.status_code == 400, "알 수 없는 item_type 요청 시 400")

        # ---------- 8. 기존 화면 회귀 ----------
        r = client.get("/vocabulary-quiz/review?version=2.1.29")
        check(r.status_code == 200, "기존 QA 검수 허브 정상 응답(회귀 없음)")
        r = client.get("/vocabulary-quiz/play")
        check(r.status_code == 200, "기존 단일유형 퀴즈 MVP 화면 정상 응답(회귀 없음)")

        # ---------- 9. 원본 데이터/idiom.db 불변 ----------
        conn = sqlite3.connect(db_path)
        counts_after = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("vocabulary_contents", "vocabulary_items", "vocabulary_review_samples")
        }
        n_exp = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE student_exposure=1").fetchone()[0]
        n_pub = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE public_ready=1").fetchone()[0]
        conn.close()
        check(counts_after["vocabulary_contents"] == counts_before["vocabulary_contents"] == 5723,
              "기존 vocabulary_contents 5,723건 불변")
        check(counts_after["vocabulary_items"] == counts_before["vocabulary_items"] == 5723,
              "기존 vocabulary_items 5,723건 불변")
        check(counts_after["vocabulary_review_samples"] == counts_before["vocabulary_review_samples"] == 500,
              "기존 vocabulary_review_samples 500건(검수 표본) 불변")
        check(n_exp == 0 and n_pub == 0, "퀴즈 진행 중 student_exposure/public_ready 변경 없음(전부 0)")

        idiom_after = sha(idiom_db) if idiom_db.exists() else None
        check(idiom_before == idiom_after, "기존 idiom.db 체크섬 불변")

    finally:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        for sid in session_ids:
            conn.execute("DELETE FROM vocabulary_multiformat_responses WHERE session_id=?", (sid,))
            conn.execute("DELETE FROM vocabulary_multiformat_sessions WHERE id=?", (sid,))
        conn.commit()
        conn.close()
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
