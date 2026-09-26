"""Phase18 파일럿 40건(vocabulary_multiformat_items, source_version=
'schema_reading_l4l5_pilot_dryrun_v1') 기능 검증 - research 서버에서 실행.

tests/test_admin_level_quiz.py와 같은 방식(pytest 없음, FastAPI TestClient로 실제
앱 end-to-end, 실행 후 삽입한 테스트 데이터는 전부 삭제)을 그대로 따른다.

이 40건은 source_version이 기존 값(2.1.29)과 달라 정상 세션 생성 엔드포인트
(POST /api/vocabulary-quiz/sessions, 혼합모드/레벨모드 둘 다)로는 절대 선택되지
않는다(설계상 의도). 따라서 이 스크립트는 세션 생성 API를 거치지 않고
vocabulary_multiformat_sessions/vocabulary_multiformat_responses 행을 직접 만들어
(라이브 서버의 실제 admin 계정으로) 문제 조회->응답 제출->채점 흐름 자체가
정상 동작하는지만 검증한다 - 인증 우회는 하지 않는다(HTTP 요청은 전부 실제
require_admin 의존성을 그대로 통과해야 한다).

실행 (research 서버, ~/aprolabs):
    source venv/bin/activate
    python3 scratch/phase18_quiz_pilot/phase18_functional_test.py
"""
from __future__ import annotations

import io
import json
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent / "aprolabs"
if not REPO_ROOT.exists():
    REPO_ROOT = Path.home() / "aprolabs"
sys.path.insert(0, str(REPO_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.auth import make_session_cookie, COOKIE_NAME, hash_password  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.user import User  # noqa: E402
from app.vocabulary_quiz.db import get_db_path  # noqa: E402

ADMIN_ID = "476e5d68-9415-40d2-84a4-29f585e08889"  # admin@aprolabs.co.kr (research 서버 실제 관리자)
SOURCE_VERSION_MARKER = "schema_reading_l4l5_pilot_dryrun_v1"
TEST_ITEM_IDS = [
    "MF_A_SC_SRL4L5PILOT_20260925_L4_001",  # MEANING_CHOICE, 착수
    "MF_C_SC_SRL4L5PILOT_20260925_L4_001",  # CONTEXT_MEANING, 착수
    "MF_A_SC_SRL4L5PILOT_20260925_L5_001",  # MEANING_CHOICE, 가치관
    "MF_C_SC_SRL4L5PILOT_20260925_L5_001",  # CONTEXT_MEANING, 가치관
]

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def run() -> bool:
    db_path = get_db_path()
    conn0 = sqlite3.connect(db_path)
    counts_before = {
        t: conn0.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("vocabulary_contents", "vocabulary_content_levels",
                  "vocabulary_multiformat_items", "vocabulary_multiformat_sessions",
                  "vocabulary_multiformat_responses")
    }
    rows = conn0.execute(
        f"SELECT item_id, item_type, source_content_id, source_version "
        f"FROM vocabulary_multiformat_items WHERE item_id IN ({','.join('?' for _ in TEST_ITEM_IDS)})",
        TEST_ITEM_IDS,
    ).fetchall()
    conn0.close()
    item_meta = {r[0]: {"item_type": r[1], "source_content_id": r[2], "source_version": r[3]} for r in rows}
    check(len(item_meta) == 4, f"테스트 대상 4개 item_id 전부 DB에 존재(실제 {len(item_meta)}건)")
    check(all(v["source_version"] == SOURCE_VERSION_MARKER for v in item_meta.values()),
          "테스트 대상 4건 전부 phase18 marker source_version")

    test_uid = None
    session_id = str(uuid.uuid4())
    inserted_session = False

    try:
        # ---------- 0. 정상 세션 생성 API로는 이 배치가 절대 선택되지 않음(격리 설계 확인) ----------
        client = TestClient(app, follow_redirects=True)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE", "CONTEXT_MEANING"], "question_count": 1,
                                "selected_vocab_level": 4})
        got_ids = set()
        if r.status_code == 200:
            # 우연히도 이 요청이 성공했다면(다른 2.1.29 문항이 있어서), 최소한 우리 파일럿
            # item_id는 절대 섞이지 않아야 한다.
            pass
        # 별도로: 레벨모드/혼합모드 후보 함수를 직접 호출해 파일럿 item_id가 후보에 전혀
        # 없는지 확인 (라우터 내부 함수 재사용 - 코드 자체를 다시 여는 것이 아니라 실제
        # 앱 객체의 실행 결과를 확인하는 것)
        from app.vocabulary_quiz.routers import multiformat as mf  # noqa: E402
        from app.vocabulary_quiz.db import SessionLocal
        vdb = SessionLocal()
        cand4 = mf._select_level_candidates(vdb, 4, ["MEANING_CHOICE", "CONTEXT_MEANING"], "all_candidates")
        cand5 = mf._select_level_candidates(vdb, 5, ["MEANING_CHOICE", "CONTEXT_MEANING"], "all_candidates")
        # 혼합모드(_select_question_items)는 source_version==SOURCE_VERSION 필터가 먼저 걸려
        # SQL 단계에서 이미 후보 집합에 들지 못한다 - 아래 직접 쿼리로 동일 사실을 재확인한다.
        conn_iso = sqlite3.connect(db_path)
        mixed_eligible = conn_iso.execute(
            f"SELECT item_id FROM vocabulary_multiformat_items "
            f"WHERE source_version=? AND is_active=1 AND item_id IN ({','.join('?' for _ in TEST_ITEM_IDS)})",
            (mf.SOURCE_VERSION, *TEST_ITEM_IDS),
        ).fetchall()
        conn_iso.close()
        vdb.close()
        check(not (set(TEST_ITEM_IDS) & set(cand4)), "레벨모드 L4 후보에 파일럿 item_id가 전혀 없음(설계대로 격리)")
        check(not (set(TEST_ITEM_IDS) & set(cand5)), "레벨모드 L5 후보에 파일럿 item_id가 전혀 없음(설계대로 격리)")
        check(len(mixed_eligible) == 0, "혼합모드 필터(source_version=2.1.29) 조건을 만족하는 파일럿 item_id 0건")

        # ---------- 1. 비로그인 차단 ----------
        c0 = TestClient(app, follow_redirects=False)
        r = c0.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code in (302, 401) and ("/login" in r.headers.get("location", "") or r.status_code == 401),
              f"비로그인 GET /multiformat/play 차단(status={r.status_code})")
        r = c0.get("/api/vocabulary-quiz/availability?level=4")
        check(r.status_code in (302, 401), f"비로그인 GET /availability 차단(status={r.status_code})")
        r = c0.post("/api/vocabulary-quiz/sessions/does-not-exist/answer",
                     json={"item_id": TEST_ITEM_IDS[0], "selected_option": 1})
        check(r.status_code in (302, 401), f"비로그인 POST /answer 차단(status={r.status_code})")
        r = c0.get(f"/api/vocabulary-quiz/sessions/does-not-exist/next")
        check(r.status_code in (302, 401), f"비로그인 GET /next 차단(status={r.status_code})")

        # ---------- 2. 로그인 비관리자 403 ----------
        db = next(get_db())
        test_uid = str(uuid.uuid4())
        db.add(User(id=test_uid, email="phase18.nonadmin.test@example.com",
                     hashed_pw=hash_password("x"), is_admin=False))
        db.commit()
        db.close()
        c1 = TestClient(app, follow_redirects=False)
        c1.cookies.set(COOKIE_NAME, make_session_cookie(test_uid))
        r = c1.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 403, "비관리자 GET /multiformat/play 차단(403)")
        r = c1.get(f"/api/vocabulary-quiz/sessions/does-not-exist/next")
        check(r.status_code == 403, "비관리자 GET /next 차단(403)")
        r = c1.post("/api/vocabulary-quiz/sessions/does-not-exist/answer",
                     json={"item_id": TEST_ITEM_IDS[0], "selected_option": 1})
        check(r.status_code == 403, "비관리자 POST /answer 차단(403)")

        # ---------- 3. 관리자 - 세션 직접 구성(정상 선택 API를 우회, 파일럿 item_id 직접 지정) ----------
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "INSERT INTO vocabulary_multiformat_sessions "
            "(id, user_id, source_version, item_types_json, question_count, correct_count, "
            " status, started_at, completed_at, metadata_json) "
            "VALUES (?, ?, ?, ?, ?, 0, 'in_progress', ?, NULL, ?)",
            (session_id, ADMIN_ID, SOURCE_VERSION_MARKER,
             json.dumps(["MEANING_CHOICE", "CONTEXT_MEANING"], ensure_ascii=False),
             len(TEST_ITEM_IDS), now,
             json.dumps({"audience": "ADMIN_ONLY", "note": "phase18_functional_test"}, ensure_ascii=False)),
        )
        for idx, iid in enumerate(TEST_ITEM_IDS, start=1):
            conn.execute(
                "INSERT INTO vocabulary_multiformat_responses (session_id, item_id, order_index, item_type) "
                "VALUES (?, ?, ?, ?)",
                (session_id, iid, idx, item_meta[iid]["item_type"]),
            )
        conn.commit()
        conn.close()
        inserted_session = True
        check(True, f"테스트 세션 {session_id} 직접 삽입 완료(파일럿 item_id 4건, 정상 선택 API 우회)")

        # ---------- 4. 문제 조회 -> 응답 제출 -> 채점 (4문항 전부) ----------
        seen_item_ids = []
        for i in range(len(TEST_ITEM_IDS)):
            r = client.get(f"/api/vocabulary-quiz/sessions/{session_id}/next")
            check(r.status_code == 200, f"[{i+1}/4] GET next 200")
            body = r.json()
            check(body["done"] is False, f"[{i+1}/4] done=False")
            item = body["item"]
            leaked = {"correct_option", "answer_text", "accepted_answers", "answers"} & set(item.keys())
            check(not leaked, f"[{i+1}/4] GET next 응답에 정답 필드 비노출(item_id={item['item_id']})")
            check(item["item_id"] in TEST_ITEM_IDS, f"[{i+1}/4] 반환된 item_id가 파일럿 4건 중 하나")
            seen_item_ids.append(item["item_id"])

            conn = sqlite3.connect(db_path)
            correct_option = conn.execute(
                "SELECT json_extract(answer_payload_json, '$.correct_option') "
                "FROM vocabulary_multiformat_items WHERE item_id=?",
                (item["item_id"],),
            ).fetchone()[0]
            explanation_db = conn.execute(
                "SELECT explanation FROM vocabulary_multiformat_items WHERE item_id=?",
                (item["item_id"],),
            ).fetchone()[0]
            conn.close()

            r = client.post(f"/api/vocabulary-quiz/sessions/{session_id}/answer",
                              json={"item_id": item["item_id"], "selected_option": correct_option})
            check(r.status_code == 200, f"[{i+1}/4] POST answer 200")
            ans = r.json()
            check(ans["is_correct"] is True, f"[{i+1}/4] 정답 제출 -> is_correct=True")
            check(ans["correct_answer"]["correct_option"] == correct_option,
                  f"[{i+1}/4] 채점 응답의 correct_answer.correct_option 일치")
            check(ans["explanation"] == explanation_db, f"[{i+1}/4] 채점 응답의 explanation이 DB와 일치")

        check(sorted(seen_item_ids) == sorted(TEST_ITEM_IDS), "4문항 전부 정확히 한 번씩 출제됨(중복/누락 없음)")

        r = client.get(f"/api/vocabulary-quiz/sessions/{session_id}/next")
        check(r.status_code == 200 and r.json()["done"] is True, "4문항 완료 후 GET next -> done=True")

        # ---------- 5. 결과 조회 ----------
        r = client.get(f"/api/vocabulary-quiz/sessions/{session_id}/result")
        check(r.status_code == 200, "GET result 200")
        result = r.json()
        check(result["total"] == 4 and result["correct"] == 4 and result["accuracy"] == 100.0,
              f"결과 집계 정확(4/4, 100%) - 실제 {result['total']}/{result['correct']}/{result['accuracy']}")
        by_type = result["by_type"]
        check(by_type.get("MEANING_CHOICE", {}).get("total") == 2 and by_type["MEANING_CHOICE"]["correct"] == 2,
              f"MEANING_CHOICE 2/2 (실제 {by_type.get('MEANING_CHOICE')})")
        check(by_type.get("CONTEXT_MEANING", {}).get("total") == 2 and by_type["CONTEXT_MEANING"]["correct"] == 2,
              f"CONTEXT_MEANING 2/2 (실제 {by_type.get('CONTEXT_MEANING')})")
        check(result["wrong_items"] == [], "전부 정답이라 wrong_items 빈 배열")

        # ---------- 6. 비로그인/비관리자가 이 세션에 접근할 수 없음 ----------
        r = c0.get(f"/api/vocabulary-quiz/sessions/{session_id}/result")
        check(r.status_code in (302, 401), f"비로그인 GET result 차단(status={r.status_code})")
        r = c1.get(f"/api/vocabulary-quiz/sessions/{session_id}/result")
        check(r.status_code == 403, "비관리자 GET result 차단(403, require_admin 단계에서 막힘)")

        # ---------- 7. 기존 데이터 불변 확인 ----------
        conn = sqlite3.connect(db_path)
        counts_after_items = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("vocabulary_contents", "vocabulary_content_levels", "vocabulary_multiformat_items")
        }
        conn.close()
        for t in ("vocabulary_contents", "vocabulary_content_levels", "vocabulary_multiformat_items"):
            check(counts_before[t] == counts_after_items[t], f"{t} 행 수 불변(전 {counts_before[t]}, 후 {counts_after_items[t]})")

    finally:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        if inserted_session:
            conn.execute("DELETE FROM vocabulary_multiformat_responses WHERE session_id=?", (session_id,))
            conn.execute("DELETE FROM vocabulary_multiformat_sessions WHERE id=?", (session_id,))
        conn.commit()
        conn.close()
        if test_uid:
            db = next(get_db())
            db.query(User).filter(User.id == test_uid).delete()
            db.commit()
            db.close()
        print(f"\n정리 완료: 테스트 세션({session_id}) 및 테스트 계정({test_uid}) 삭제")

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
