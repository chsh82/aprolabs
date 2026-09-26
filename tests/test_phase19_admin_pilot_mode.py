"""Phase19 - 관리자 화면 "L4·L5 파일럿" 모드(POST /api/vocabulary-quiz/sessions,
pilot_mode=true) 정식 경로 검증.

phase18(reports/schema_reading_phase18_quiz_pilot_apply_20260926.md)이 실제
research DB에 적재한 40건(source_version='schema_reading_l4l5_pilot_dryrun_v1',
MEANING_CHOICE 20 + CONTEXT_MEANING 20, 20개 content_id)을, 기존 일반 출제
경로(SOURCE_VERSION="2.1.29")를 전혀 건드리지 않고 별도로 추가한 파일럿 모드로
"정식 세션 생성 API"를 통해(우회 없이) 조회·풀이·채점할 수 있는지 검증한다.

tests/test_admin_level_quiz.py / tests/test_phase18_quiz_pilot_admin_flow.py와
같은 방식(pytest 없음, FastAPI TestClient로 실제 앱 end-to-end, [PASS]/[FAIL]
출력, 테스트로 만든 데이터는 실행 후 전부 삭제).

로컬 vocabulary_quiz_rnd.db에는 phase18 데이터가 없으므로(로컬은 phase13/14
이전 상태), 이 스크립트는 research DB의 읽기 전용 사본(스크래치 디렉터리에
scp로 내려받은 복사본)에 대해 실행한다 - 운영/연구 서버에는 어떤 것도
쓰지 않는다(배포 없음). 실행 전 아래 COPY_DB_PATH를 관리자가 준비한 사본
경로로 맞춰야 한다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<research DB 사본 경로> python tests/test_phase19_admin_pilot_mode.py
(환경변수를 생략하면 스크립트 하단 DEFAULT_COPY_DB_PATH를 사용한다)
"""
from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import sys
import uuid
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

DEFAULT_COPY_DB_PATH = (
    Path.home() / "AppData" / "Local" / "Temp" / "claude" / "C--Users-aproa" /
    "6e2093a4-82bf-4ee0-833a-e5c458074f06" / "scratchpad" / "vocabulary_quiz_research_copy.db"
)
COPY_DB_PATH = Path(os.environ.get("VOCABULARY_QUIZ_DB_PATH") or DEFAULT_COPY_DB_PATH)
os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(COPY_DB_PATH)  # app 모듈 import 전에 고정해야 engine이 이 경로로 바인딩됨

if not COPY_DB_PATH.exists():
    print(f"[FAIL] 사전조건: research DB 사본이 없습니다({COPY_DB_PATH}) - "
          f"먼저 scp로 vocabulary_quiz_research.db 사본을 준비하세요.")
    sys.exit(1)

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.auth import make_session_cookie, COOKIE_NAME, hash_password  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.user import User  # noqa: E402
from app.vocabulary_quiz.db import get_db_path  # noqa: E402
from app.vocabulary_quiz.routers import multiformat as mf  # noqa: E402

ADMIN_ID = "de86bad0-e684-457e-8793-075785a65d05"  # admin@aprolabs.co.kr, 로컬 aprolabs.db 관리자(test_admin_level_quiz.py와 동일)
PILOT_MARKER = "schema_reading_l4l5_pilot_dryrun_v1"

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


class _ListLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[str] = []

    def emit(self, record):
        self.records.append(record.getMessage())


def run() -> bool:
    db_path = get_db_path()
    check(db_path == COPY_DB_PATH, f"사전조건: 앱이 research DB 사본을 바라봄({db_path})")

    conn0 = sqlite3.connect(db_path)
    counts_before = {
        t: conn0.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("vocabulary_contents", "vocabulary_content_levels", "vocabulary_multiformat_items")
    }
    check(counts_before["vocabulary_multiformat_items"] == 1329,
          f"사전조건: vocabulary_multiformat_items 1,329건(phase18 적재 상태, 실제 {counts_before['vocabulary_multiformat_items']})")
    check(counts_before["vocabulary_contents"] == 5820 and counts_before["vocabulary_content_levels"] == 5820,
          f"사전조건: vocabulary_contents/vocabulary_content_levels 각 5,820건(실제 "
          f"{counts_before['vocabulary_contents']}/{counts_before['vocabulary_content_levels']})")
    pilot_total = conn0.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (PILOT_MARKER,)
    ).fetchone()[0]
    check(pilot_total == 40, f"사전조건: 파일럿 marker 문항 정확히 40건(실제 {pilot_total})")
    conn0.close()

    test_uid = None
    session_ids: list[str] = []
    log_handler = _ListLogHandler()
    log_handler.setLevel(logging.WARNING)
    mf.logger.addHandler(log_handler)
    mf.logger.setLevel(logging.WARNING)

    try:
        # ---------- 사전 확인: _select_pilot_item_ids가 정확히 40건을 3중 검증 전부 통과시킴 ----------
        from app.vocabulary_quiz.db import SessionLocal
        vdb = SessionLocal()
        try:
            pilot_ids = mf._select_pilot_item_ids(vdb, None)
            check(len(pilot_ids) == 40, f"3중 검증 통과 파일럿 후보 정확히 40건(실제 {len(pilot_ids)})")
            check(len(log_handler.records) == 0, f"정상 상태에서는 경고 로그가 없음(실제 {len(log_handler.records)}건)")

            conn = sqlite3.connect(db_path)
            db_pilot_ids = {
                r[0] for r in conn.execute(
                    "SELECT item_id FROM vocabulary_multiformat_items WHERE source_version=?", (PILOT_MARKER,)
                ).fetchall()
            }
            conn.close()
            check(set(pilot_ids) == db_pilot_ids,
                  "3중 검증을 통과한 40건이 DB의 phase18 marker 40건과 정확히 일치(추가/누락 없음)")

            # ---------- 3중 검증 2) content_id 존재/active 위반 시 경고+제외 ----------
            probe_item_id = sorted(pilot_ids)[0]
            conn = sqlite3.connect(db_path)
            probe_cid = conn.execute(
                "SELECT source_content_id FROM vocabulary_multiformat_items WHERE item_id=?", (probe_item_id,)
            ).fetchone()[0]
            sibling_count = conn.execute(
                "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_content_id=? AND source_version=?",
                (probe_cid, PILOT_MARKER),
            ).fetchone()[0]
            conn.execute("UPDATE vocabulary_contents SET is_active=0 WHERE content_id=?", (probe_cid,))
            conn.commit()
            conn.close()

            # phase20 오염 방지 보강 이후: 3중 검증에서 제외된 문항이 생기면(원래 40건 중
            # 일부가 빠짐) 조용히 부분 결과(38/40 등)를 반환하지 않고 PilotBatchIntegrityError로
            # 출제 자체를 중단한다(reports/schema_reading_phase20_*.md) - 예전(phase19) 기대치인
            # "그 문항만 제외되고 나머지는 정상 반환"은 더 이상 유효하지 않아 여기서 갱신한다.
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            try:
                pilot_ids_after = mf._select_pilot_item_ids(vdb, None)
                check(False, f"content_id is_active=0 상태: 조용히 {len(pilot_ids_after)}건 반환됨(PilotBatchIntegrityError가 발생했어야 함)")
            except mf.PilotBatchIntegrityError as exc:
                check(True, f"content is_active=0 상태: PilotBatchIntegrityError로 출제 중단됨(missing={sorted(exc.missing)})")
            check(any("is_active" in r for r in log_handler.records),
                  f"content is_active=0 상태에 대해 경고 로그가 실제로 남음(레코드 {len(log_handler.records)}건)")

            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE vocabulary_contents SET is_active=1 WHERE content_id=?", (probe_cid,))
            conn.commit()
            conn.close()
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            pilot_ids_restored = mf._select_pilot_item_ids(vdb, None)
            check(len(pilot_ids_restored) == 40, f"is_active 원복 후 다시 40건(실제 {len(pilot_ids_restored)})")

            # ---------- 3중 검증 3) level_status != REVIEW_BOUNDARY 위반 시 경고+제외 ----------
            conn = sqlite3.connect(db_path)
            conn.execute(
                "UPDATE vocabulary_content_levels SET level_status='PROVISIONAL_AUTO' "
                "WHERE content_id=? AND level_version=?", (probe_cid, mf.LEVEL_VERSION),
            )
            conn.commit()
            conn.close()

            # phase20 오염 방지 보강 이후: 위와 동일한 이유로 level_status drift도 부분 반환이
            # 아니라 PilotBatchIntegrityError로 출제를 중단한다.
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            try:
                pilot_ids_level_broken = mf._select_pilot_item_ids(vdb, None)
                check(False, f"level_status drift 상태: 조용히 {len(pilot_ids_level_broken)}건 반환됨(PilotBatchIntegrityError가 발생했어야 함)")
            except mf.PilotBatchIntegrityError as exc:
                check(True, f"level_status drift 상태: PilotBatchIntegrityError로 출제 중단됨(missing={sorted(exc.missing)})")
            check(any("level_status" in r for r in log_handler.records),
                  f"level_status 불일치에 대해 경고 로그가 실제로 남음(레코드 {len(log_handler.records)}건)")

            conn = sqlite3.connect(db_path)
            conn.execute(
                "UPDATE vocabulary_content_levels SET level_status='REVIEW_BOUNDARY' "
                "WHERE content_id=? AND level_version=?", (probe_cid, mf.LEVEL_VERSION),
            )
            conn.commit()
            conn.close()
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            pilot_ids_final = mf._select_pilot_item_ids(vdb, None)
            check(len(pilot_ids_final) == 40, f"level_status 원복 후 다시 40건(실제 {len(pilot_ids_final)})")
            check(set(pilot_ids_final) == db_pilot_ids, "원복 후 40건 집합이 DB marker 집합과 다시 정확히 일치")
        finally:
            vdb.close()
            mf.logger.removeHandler(log_handler)

        # ---------- 1. 비로그인 차단 ----------
        c0 = TestClient(app, follow_redirects=False)
        r = c0.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 GET /multiformat/play 차단(302)")
        r = c0.get("/api/vocabulary-quiz/pilot-availability")
        check(r.status_code == 302, "비로그인 GET /pilot-availability 차단(302)")
        r = c0.post("/api/vocabulary-quiz/sessions", json={"pilot_mode": True, "question_count": 4})
        check(r.status_code == 302, "비로그인 POST /sessions(pilot_mode) 차단(302)")

        # ---------- 2. 로그인 비관리자 403 ----------
        db = next(get_db())
        test_uid = str(uuid.uuid4())
        db.add(User(id=test_uid, email="phase19.nonadmin.test@example.com", hashed_pw=hash_password("x"), is_admin=False))
        db.commit()
        db.close()
        c1 = TestClient(app, follow_redirects=False)
        c1.cookies.set(COOKIE_NAME, make_session_cookie(test_uid))
        r = c1.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 403, "비관리자 GET /multiformat/play 차단(403)")
        r = c1.get("/api/vocabulary-quiz/pilot-availability")
        check(r.status_code == 403, "비관리자 GET /pilot-availability 차단(403)")
        r = c1.post("/api/vocabulary-quiz/sessions", json={"pilot_mode": True, "question_count": 4})
        check(r.status_code == 403, "비관리자 POST /sessions(pilot_mode) 차단(403)")

        # ---------- 3. 관리자 화면에 파일럿 모드 UI 존재 ----------
        client = TestClient(app, follow_redirects=True)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        r = client.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 200, "관리자 GET /multiformat/play 200")
        check('id="pilot-mode-checkbox"' in r.text, "화면에 파일럿 모드 체크박스(pilot-mode-checkbox) 존재")
        check("파일럿" in r.text, "화면 문구에 '파일럿' 노출")

        # ---------- 4. 파일럿 가용량 조회 ----------
        r = client.get("/api/vocabulary-quiz/pilot-availability")
        check(r.status_code == 200, "관리자 GET /pilot-availability 200")
        avail = r.json()
        check(avail["available_items"] == 40 and avail["distinct_words"] == 20,
              f"파일럿 가용량 40문항/20어휘(실제 {avail['available_items']}/{avail['distinct_words']})")
        check(avail["by_type"] == {"MEANING_CHOICE": 20, "CONTEXT_MEANING": 20},
              f"유형별 20/20(실제 {avail['by_type']})")

        # ---------- 5. 파일럿 모드 세션 생성 - 정식 API(우회 없음), 전체 40건 한 세션에 ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"pilot_mode": True, "question_count": 40})
        check(r.status_code == 200, "정식 API로 파일럿 세션 생성 성공(40문항)")
        data = r.json()
        sid = data["session_id"]
        session_ids.append(sid)
        check(data["question_count"] == 40, "세션 question_count=40")
        check(data["source_version"] == PILOT_MARKER, "세션 응답 source_version이 파일럿 marker")
        check(data["level_info"] is None, "파일럿 세션의 level_info는 None(레벨모드 아님)")
        check(data["pilot_info"]["candidate_count"] == 40, "세션 생성 응답의 pilot_info.candidate_count=40")

        conn = sqlite3.connect(db_path)
        session_row = conn.execute(
            "SELECT source_version, metadata_json FROM vocabulary_multiformat_sessions WHERE id=?", (sid,)
        ).fetchone()
        conn.close()
        check(session_row[0] == PILOT_MARKER, "DB에 저장된 세션 source_version도 파일럿 marker")
        saved_meta = json.loads(session_row[1])
        check(saved_meta.get("pilot_mode") is True and saved_meta.get("audience") == "ADMIN_ONLY",
              f"세션 metadata_json에 pilot_mode/audience 정확히 기록됨({saved_meta})")

        # ---------- 6. 문제 조회 -> 정답 비노출 -> 제출 -> 채점 (40문항 전부, 정답 20/오답 20) ----------
        seen_item_ids: list[str] = []
        n_correct_submitted = 0
        for i in range(40):
            r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/next")
            if r.status_code != 200:
                check(False, f"[{i+1}/40] GET next 실패(status={r.status_code})")
                break
            body = r.json()
            if body["done"]:
                check(False, f"[{i+1}/40] 40문항 채우기 전 done=True(예상 밖 조기 종료)")
                break
            item = body["item"]
            leaked = {"correct_option", "answer_text", "accepted_answers", "answers"} & set(item.keys())
            if leaked:
                check(False, f"[{i+1}/40] GET next 응답에 정답 필드 노출됨: {leaked}")
            seen_item_ids.append(item["item_id"])

            conn = sqlite3.connect(db_path)
            correct_option = conn.execute(
                "SELECT json_extract(answer_payload_json, '$.correct_option') "
                "FROM vocabulary_multiformat_items WHERE item_id=?", (item["item_id"],),
            ).fetchone()[0]
            conn.close()

            # 짝수 인덱스는 정답, 홀수 인덱스는 의도적 오답 제출 - 채점 로직이 실제로
            # 오답도 정확히 판정하는지(무조건 True를 반환하지 않는지) 함께 확인.
            if i % 2 == 0:
                submit_option = correct_option
                n_correct_submitted += 1
            else:
                submit_option = (correct_option % 4) + 1  # 1~4 범위 내 다른 값
            r = client.post(f"/api/vocabulary-quiz/sessions/{sid}/answer",
                             json={"item_id": item["item_id"], "selected_option": submit_option})
            if r.status_code != 200:
                check(False, f"[{i+1}/40] POST answer 실패(status={r.status_code})")
                continue
            ans = r.json()
            expected_correct = (submit_option == correct_option)
            if ans["is_correct"] != expected_correct:
                check(False, f"[{i+1}/40] 채점 결과 불일치(item={item['item_id']}, 기대={expected_correct}, 실제={ans['is_correct']})")
            if ans["correct_answer"]["correct_option"] != correct_option:
                check(False, f"[{i+1}/40] correct_answer.correct_option 불일치")

        check(len(seen_item_ids) == 40, f"40문항 전부 출제됨(실제 {len(seen_item_ids)})")
        check(len(set(seen_item_ids)) == 40, "40문항 중복 없이 정확히 한 번씩 출제됨")
        check(set(seen_item_ids) == db_pilot_ids, "출제된 40건이 phase18 marker 40건과 정확히 일치")

        r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/next")
        check(r.status_code == 200 and r.json()["done"] is True, "40문항 완료 후 GET next -> done=True")

        # ---------- 7. 결과 조회 ----------
        r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/result")
        check(r.status_code == 200, "GET result 200")
        result = r.json()
        check(result["total"] == 40 and result["correct"] == n_correct_submitted,
              f"결과 집계 정확({n_correct_submitted}/40, 실제 {result['correct']}/{result['total']})")
        check(len(result["wrong_items"]) == 40 - n_correct_submitted,
              f"wrong_items 개수 정확(실제 {len(result['wrong_items'])})")
        check(result["pilot_info"]["pilot_source_version"] == PILOT_MARKER, "result 응답 pilot_info.pilot_source_version 정확")
        check(result["level_info"] is None, "파일럿 세션 result의 level_info는 None")

        # ---------- 8. 일반 출제(2.1.29 경로)에 파일럿 문항이 절대 섞이지 않음(회귀) ----------
        mixed_seen: set[str] = set()
        mixed_session_ok = 0
        for _ in range(20):
            r = client.post("/api/vocabulary-quiz/sessions", json={"question_count": 10})
            if r.status_code != 200:
                check(False, f"일반(혼합모드) 세션 생성 실패(status={r.status_code})")
                continue
            mixed_session_ok += 1
            gid = r.json()["session_id"]
            session_ids.append(gid)
            for _q in range(10):
                nr = client.get(f"/api/vocabulary-quiz/sessions/{gid}/next")
                nb = nr.json()
                if nb["done"]:
                    break
                iid = nb["item"]["item_id"]
                mixed_seen.add(iid)
                conn = sqlite3.connect(db_path)
                co = conn.execute(
                    "SELECT json_extract(answer_payload_json, '$.correct_option') "
                    "FROM vocabulary_multiformat_items WHERE item_id=?", (iid,),
                ).fetchone()
                conn.close()
                if co and co[0]:
                    client.post(f"/api/vocabulary-quiz/sessions/{gid}/answer",
                                json={"item_id": iid, "selected_option": co[0]})
        check(mixed_session_ok == 20, f"일반(혼합모드) 세션 생성 20회 전부 성공(실제 {mixed_session_ok})")
        check(not (mixed_seen & db_pilot_ids),
              f"일반 혼합모드 세션 20회(문항 최대 200개 관찰) 중 파일럿 item_id 0건 혼입(관찰 {len(mixed_seen)}건)")

        # 레벨모드(L4/L5)도 별도로 직접 후보 함수 확인(격리 회귀) - 새 세션(위 3중검증 블록의
        # vdb는 이미 닫혀 있음)
        vdb2 = SessionLocal()
        try:
            cand4 = mf._select_level_candidates(vdb2, 4, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates")
            cand5 = mf._select_level_candidates(vdb2, 5, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates")
        finally:
            vdb2.close()
        check(not (set(cand4) & db_pilot_ids), "레벨모드 L4 후보에 파일럿 item_id 없음(격리 회귀)")
        check(not (set(cand5) & db_pilot_ids), "레벨모드 L5 후보에 파일럿 item_id 없음(격리 회귀)")

        # ---------- 9. 데이터 불변 확인 ----------
        conn = sqlite3.connect(db_path)
        counts_after = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("vocabulary_contents", "vocabulary_content_levels", "vocabulary_multiformat_items")
        }
        exposure = conn.execute("SELECT SUM(student_exposure), SUM(public_ready) FROM vocabulary_contents").fetchone()
        review_boundary_intact = conn.execute(
            "SELECT COUNT(*) FROM vocabulary_content_levels WHERE level_version=? AND level_status='REVIEW_BOUNDARY' "
            "AND content_id IN (SELECT DISTINCT source_content_id FROM vocabulary_multiformat_items WHERE source_version=?)",
            (mf.LEVEL_VERSION, PILOT_MARKER),
        ).fetchone()[0]
        conn.close()
        for t in counts_before:
            check(counts_before[t] == counts_after[t], f"{t} 행 수 불변(전 {counts_before[t]}, 후 {counts_after[t]})")
        check(exposure == (0, 0), f"student_exposure/public_ready 합계 여전히 0/0(실제 {exposure})")
        check(review_boundary_intact == 20, f"파일럿 20개 content_id 전부 REVIEW_BOUNDARY 그대로 유지(실제 {review_boundary_intact}/20)")

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
        print(f"\n정리 완료: 테스트 세션 {len(session_ids)}건 및 테스트 계정({test_uid}) 삭제")

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
