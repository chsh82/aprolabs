"""Phase29 - 관리자 화면 "L6 파일럿" 모드(POST /api/vocabulary-quiz/sessions,
l6_pilot_mode=true) 정식 경로 검증.

phase28(reports/schema_reading_phase28_l6_pilot_apply_20260927.md)이 실제 research
DB에 적재한 40건(source_version='schema_reading_l6_pilot_dryrun_v1', MEANING_CHOICE
20 + CONTEXT_MEANING 20, 20개 L6 content_id)을, 기존 일반 출제(SOURCE_VERSION=
"2.1.29")·기존 L4·L5 파일럿(schema_reading_l4l5_pilot_dryrun_v1)을 전혀 건드리지
않고 별도로 추가한 L6 파일럿 모드로 "정식 세션 생성 API"를 통해(우회 없이)
조회·풀이·채점할 수 있는지 검증한다. 세션을 DB에 직접 구성하는 우회는 쓰지 않는다
(test_phase19_admin_pilot_mode.py와 같은 원칙).

tests/test_phase19_admin_pilot_mode.py와 같은 방식(pytest 없음, FastAPI TestClient로
실제 앱 end-to-end, [PASS]/[FAIL] 출력, 테스트로 만든 데이터는 실행 후 전부 삭제).

research DB 사본에 대해 실행한다 - 운영/연구 서버 원본에는 어떤 것도 쓰지 않는다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<research DB 사본 경로> python tests/test_phase29_l6_pilot_admin_flow.py
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
    "6e2093a4-82bf-4ee0-833a-e5c458074f06" / "scratchpad" / "vocabulary_quiz_research_phase25_copy.db"
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

ADMIN_ID = "476e5d68-9415-40d2-84a4-29f585e08889"  # admin@aprolabs.co.kr, research 서버 실제 관리자
L6_PILOT_MARKER = "schema_reading_l6_pilot_dryrun_v1"
L4L5_PILOT_MARKER = "schema_reading_l4l5_pilot_dryrun_v1"

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
    check(counts_before["vocabulary_multiformat_items"] == 1369,
          f"사전조건: vocabulary_multiformat_items 1,369건(phase28 적재 상태, 실제 "
          f"{counts_before['vocabulary_multiformat_items']})")
    check(counts_before["vocabulary_contents"] == 5902 and counts_before["vocabulary_content_levels"] == 5902,
          f"사전조건: vocabulary_contents/vocabulary_content_levels 각 5,902건(실제 "
          f"{counts_before['vocabulary_contents']}/{counts_before['vocabulary_content_levels']})")
    l6_total = conn0.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (L6_PILOT_MARKER,)
    ).fetchone()[0]
    check(l6_total == 40, f"사전조건: L6 파일럿 marker 문항 정확히 40건(실제 {l6_total})")
    l4l5_total = conn0.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (L4L5_PILOT_MARKER,)
    ).fetchone()[0]
    check(l4l5_total == 40, f"사전조건: 기존 L4·L5 파일럿 marker 문항 그대로 40건(실제 {l4l5_total})")
    conn0.close()

    test_uid = None
    session_ids: list[str] = []
    log_handler = _ListLogHandler()
    log_handler.setLevel(logging.WARNING)
    mf.logger.addHandler(log_handler)
    mf.logger.setLevel(logging.WARNING)

    try:
        # ---------- 사전 확인: _select_l6_pilot_item_ids가 정확히 40건을 전부 통과시킴 ----------
        from app.vocabulary_quiz.db import SessionLocal
        vdb = SessionLocal()
        try:
            l6_ids = mf._select_l6_pilot_item_ids(vdb, None)
            check(len(l6_ids) == 40, f"검증 통과 L6 파일럿 후보 정확히 40건(실제 {len(l6_ids)})")
            check(len(log_handler.records) == 0, f"정상 상태에서는 경고 로그가 없음(실제 {len(log_handler.records)}건)")

            conn = sqlite3.connect(db_path)
            db_l6_ids = {
                r[0] for r in conn.execute(
                    "SELECT item_id FROM vocabulary_multiformat_items WHERE source_version=?", (L6_PILOT_MARKER,)
                ).fetchall()
            }
            conn.close()
            check(set(l6_ids) == db_l6_ids,
                  "검증을 통과한 40건이 DB의 phase28 marker 40건과 정확히 일치(추가/누락 없음)")

            # ---------- vocab_level!=6 위반 시 경고+제외(phase29가 새로 추가한 검사) ----------
            probe_item_id = sorted(l6_ids)[0]
            conn = sqlite3.connect(db_path)
            probe_cid = conn.execute(
                "SELECT source_content_id FROM vocabulary_multiformat_items WHERE item_id=?", (probe_item_id,)
            ).fetchone()[0]
            conn.execute(
                "UPDATE vocabulary_content_levels SET vocab_level=5 WHERE content_id=? AND level_version=?",
                (probe_cid, mf.LEVEL_VERSION),
            )
            conn.commit()
            conn.close()

            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            try:
                mf._select_l6_pilot_item_ids(vdb, None)
                check(False, "vocab_level=5로 조작된 상태: PilotBatchIntegrityError가 발생했어야 함")
            except mf.PilotBatchIntegrityError as exc:
                check(True, f"vocab_level 불일치 상태: PilotBatchIntegrityError로 출제 중단됨(missing={sorted(exc.missing)})")
            check(any("vocab_level" in r for r in log_handler.records),
                  f"vocab_level 불일치에 대해 경고 로그가 실제로 남음(레코드 {len(log_handler.records)}건)")

            conn = sqlite3.connect(db_path)
            conn.execute(
                "UPDATE vocabulary_content_levels SET vocab_level=6 WHERE content_id=? AND level_version=?",
                (probe_cid, mf.LEVEL_VERSION),
            )
            conn.commit()
            conn.close()
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            l6_ids_restored = mf._select_l6_pilot_item_ids(vdb, None)
            check(len(l6_ids_restored) == 40, f"vocab_level 원복 후 다시 40건(실제 {len(l6_ids_restored)})")

            # ---------- student_exposure!=0 위반 시 경고+제외(phase29가 새로 추가한 검사) ----------
            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE vocabulary_contents SET student_exposure=1 WHERE content_id=?", (probe_cid,))
            conn.commit()
            conn.close()

            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            try:
                mf._select_l6_pilot_item_ids(vdb, None)
                check(False, "student_exposure=1로 조작된 상태: PilotBatchIntegrityError가 발생했어야 함")
            except mf.PilotBatchIntegrityError as exc:
                check(True, f"student_exposure 불일치 상태: PilotBatchIntegrityError로 출제 중단됨(missing={sorted(exc.missing)})")
            check(any("student_exposure" in r for r in log_handler.records),
                  f"student_exposure 불일치에 대해 경고 로그가 실제로 남음(레코드 {len(log_handler.records)}건)")

            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE vocabulary_contents SET student_exposure=0 WHERE content_id=?", (probe_cid,))
            conn.commit()
            conn.close()
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            l6_ids_final = mf._select_l6_pilot_item_ids(vdb, None)
            check(len(l6_ids_final) == 40, f"student_exposure 원복 후 다시 40건(실제 {len(l6_ids_final)})")
            check(set(l6_ids_final) == db_l6_ids, "원복 후 40건 집합이 DB marker 집합과 다시 정확히 일치")

            # ---------- 오염 방지: 같은 source_version으로 가짜 문항을 추가해도 안 섞임 ----------
            fake_item_id = "MF_A_SC_SRL6PILOT_FAKE_CONTAMINANT_999"
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA foreign_keys=ON")
            any_content_id = sorted(db_l6_ids)[0]
            real_content_id = conn.execute(
                "SELECT source_content_id FROM vocabulary_multiformat_items WHERE item_id=?", (any_content_id,)
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO vocabulary_multiformat_items "
                "(item_id, item_type, source_content_id, lemma, pos, prompt, options_json, correct_option, "
                " public_payload_json, answer_payload_json, explanation, source_version, is_active) "
                "VALUES (?, 'MEANING_CHOICE', ?, 'fake', '명사', 'fake prompt', '[\"a\",\"b\",\"c\",\"d\"]', 1, "
                " '{\"options\":[\"a\",\"b\",\"c\",\"d\"]}', '{\"correct_option\":1}', 'fake', ?, 1)",
                (fake_item_id, real_content_id, L6_PILOT_MARKER),
            )
            conn.commit()
            conn.close()

            # phase20이 확립한 설계(오염은 "누락"과 다르다): 기대한 40건이 전부 그대로
            # 있으면 조용히 필터링만 하고 출제를 막지 않는다 - PilotBatchIntegrityError는
            # 기대한 40건 중 일부가 "빠졌을 때"만 발생한다(위 vocab_level/student_exposure
            # 테스트가 그 경로). 여기서는 40건이 그대로 있는 오염만 주입했으므로 에러 없이
            # 정확히 40건(가짜 제외)이 반환되는 것이 올바른 동작이다.
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            l6_ids_contaminated = mf._select_l6_pilot_item_ids(vdb, None)
            check(len(l6_ids_contaminated) == 40 and fake_item_id not in l6_ids_contaminated,
                  f"가짜 문항 오염 상태에서도 에러 없이 정확히 40건(가짜 제외)만 반환됨(실제 "
                  f"{len(l6_ids_contaminated)}건, 가짜 포함 여부={fake_item_id in l6_ids_contaminated})")
            check(set(l6_ids_contaminated) == db_l6_ids, "오염 상태에서도 반환 집합이 매니페스트 화이트리스트와 정확히 일치")

            conn = sqlite3.connect(db_path)
            conn.execute("DELETE FROM vocabulary_multiformat_items WHERE item_id=?", (fake_item_id,))
            conn.commit()
            conn.close()
            log_handler.records.clear()
            vdb.close()
            vdb = SessionLocal()
            l6_ids_after_cleanup = mf._select_l6_pilot_item_ids(vdb, None)
            check(len(l6_ids_after_cleanup) == 40 and fake_item_id not in l6_ids_after_cleanup,
                  f"가짜 문항 삭제 후 다시 정확히 40건, 가짜 문항 미포함(실제 {len(l6_ids_after_cleanup)})")
        finally:
            vdb.close()
            mf.logger.removeHandler(log_handler)

        # ---------- 1. 비로그인 차단(302) ----------
        c0 = TestClient(app, follow_redirects=False)
        r = c0.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 GET /multiformat/play 차단(302)")
        r = c0.get("/api/vocabulary-quiz/l6-pilot-availability")
        check(r.status_code == 302, "비로그인 GET /l6-pilot-availability 차단(302)")
        r = c0.post("/api/vocabulary-quiz/sessions", json={"l6_pilot_mode": True, "question_count": 4})
        check(r.status_code == 302, "비로그인 POST /sessions(l6_pilot_mode) 차단(302)")

        # ---------- 2. 로그인 비관리자 403 ----------
        db = next(get_db())
        test_uid = str(uuid.uuid4())
        db.add(User(id=test_uid, email="phase29.nonadmin.test@example.com", hashed_pw=hash_password("x"), is_admin=False))
        db.commit()
        db.close()
        c1 = TestClient(app, follow_redirects=False)
        c1.cookies.set(COOKIE_NAME, make_session_cookie(test_uid))
        r = c1.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 403, "비관리자 GET /multiformat/play 차단(403)")
        r = c1.get("/api/vocabulary-quiz/l6-pilot-availability")
        check(r.status_code == 403, "비관리자 GET /l6-pilot-availability 차단(403)")
        r = c1.post("/api/vocabulary-quiz/sessions", json={"l6_pilot_mode": True, "question_count": 4})
        check(r.status_code == 403, "비관리자 POST /sessions(l6_pilot_mode) 차단(403)")

        # ---------- 3. 관리자 화면에 L4·L5/L6 파일럿 모드가 명확히 구분되어 존재 ----------
        client = TestClient(app, follow_redirects=True)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        r = client.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 200, "관리자 GET /multiformat/play 200")
        check('id="pilot-mode-checkbox"' in r.text, "화면에 기존 L4·L5 파일럿 체크박스 그대로 존재")
        check('id="l6-pilot-mode-checkbox"' in r.text, "화면에 신규 L6 파일럿 체크박스 존재")
        check("L4·L5 파일럿" in r.text and "L6 파일럿" in r.text, "화면 문구에 L4·L5/L6 파일럿이 각각 명확히 구분되어 노출")

        # ---------- 4. L6 파일럿 가용량 조회 ----------
        r = client.get("/api/vocabulary-quiz/l6-pilot-availability")
        check(r.status_code == 200, "관리자 GET /l6-pilot-availability 200")
        avail = r.json()
        check(avail["available_items"] == 40 and avail["distinct_words"] == 20,
              f"L6 파일럿 가용량 40문항/20어휘(실제 {avail['available_items']}/{avail['distinct_words']})")
        check(avail["by_type"] == {"MEANING_CHOICE": 20, "CONTEXT_MEANING": 20},
              f"유형별 20/20(실제 {avail['by_type']})")

        # ---------- 5. pilot_mode와 l6_pilot_mode 동시 선택 거부(422) ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"pilot_mode": True, "l6_pilot_mode": True, "question_count": 1})
        check(r.status_code == 422, f"pilot_mode+l6_pilot_mode 동시 선택 422 거부(실제 {r.status_code})")

        # ---------- 6. L6 파일럿 세션 생성 - 정식 API(우회 없음), 전체 40건 한 세션에 ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"l6_pilot_mode": True, "question_count": 40})
        check(r.status_code == 200, "정식 API로 L6 파일럿 세션 생성 성공(40문항)")
        data = r.json()
        sid = data["session_id"]
        session_ids.append(sid)
        check(data["question_count"] == 40, "세션 question_count=40")
        check(data["source_version"] == L6_PILOT_MARKER, "세션 응답 source_version이 L6 파일럿 marker")
        check(data["level_info"] is None, "L6 파일럿 세션의 level_info는 None(레벨모드 아님)")
        check(data.get("pilot_info") is None, "L6 파일럿 세션의 pilot_info(L4·L5용)는 None")
        check(data["l6_pilot_info"]["candidate_count"] == 40, "세션 생성 응답의 l6_pilot_info.candidate_count=40")

        conn = sqlite3.connect(db_path)
        session_row = conn.execute(
            "SELECT source_version, metadata_json FROM vocabulary_multiformat_sessions WHERE id=?", (sid,)
        ).fetchone()
        conn.close()
        check(session_row[0] == L6_PILOT_MARKER, "DB에 저장된 세션 source_version도 L6 파일럿 marker")
        saved_meta = json.loads(session_row[1])
        check(saved_meta.get("l6_pilot_mode") is True and saved_meta.get("audience") == "ADMIN_ONLY",
              f"세션 metadata_json에 l6_pilot_mode/audience 정확히 기록됨({saved_meta})")

        # ---------- 7. 문제 조회 -> 정답 비노출 -> 제출 -> 채점 (40문항 전부, 정답 20/오답 20) ----------
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

            if i % 2 == 0:
                submit_option = correct_option
                n_correct_submitted += 1
            else:
                submit_option = (correct_option % 4) + 1
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
        check(set(seen_item_ids) == db_l6_ids, "출제된 40건이 phase28 marker 40건과 정확히 일치")

        r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/next")
        check(r.status_code == 200 and r.json()["done"] is True, "40문항 완료 후 GET next -> done=True")

        # ---------- 8. 결과 조회 ----------
        r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/result")
        check(r.status_code == 200, "GET result 200")
        result = r.json()
        check(result["total"] == 40 and result["correct"] == n_correct_submitted,
              f"결과 집계 정확({n_correct_submitted}/40, 실제 {result['correct']}/{result['total']})")
        check(len(result["wrong_items"]) == 40 - n_correct_submitted,
              f"wrong_items 개수 정확(실제 {len(result['wrong_items'])})")
        check(result["l6_pilot_info"]["l6_pilot_source_version"] == L6_PILOT_MARKER,
              "result 응답 l6_pilot_info.l6_pilot_source_version 정확")
        check(result.get("pilot_info") is None, "L6 파일럿 세션 result의 pilot_info(L4·L5용)는 None")
        check(result["level_info"] is None, "L6 파일럿 세션 result의 level_info는 None")

        # ---------- 9. 비로그인/비관리자가 이 세션에 접근할 수 없음 ----------
        r = c0.get(f"/api/vocabulary-quiz/sessions/{sid}/result")
        check(r.status_code == 302, f"비로그인 GET result 차단(302, 실제 {r.status_code})")
        r = c1.get(f"/api/vocabulary-quiz/sessions/{sid}/result")
        check(r.status_code == 403, "비관리자 GET result 차단(403)")

        # ---------- 10. 일반 출제·L4·L5 파일럿과 상호 격리(회귀) ----------
        mixed_seen: set[str] = set()
        mixed_session_ok = 0
        for _ in range(10):
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
        check(mixed_session_ok == 10, f"일반(혼합모드) 세션 생성 10회 전부 성공(실제 {mixed_session_ok})")
        check(not (mixed_seen & db_l6_ids), f"일반 혼합모드 세션 관찰 중 L6 파일럿 item_id 0건 혼입(관찰 {len(mixed_seen)}건)")

        r = client.post("/api/vocabulary-quiz/sessions", json={"pilot_mode": True, "question_count": 40})
        check(r.status_code == 200, "기존 L4·L5 파일럿 세션 생성 여전히 정상 동작(회귀 없음)")
        l4l5_sid = r.json()["session_id"]
        session_ids.append(l4l5_sid)
        check(r.json()["pilot_info"]["candidate_count"] == 40, "기존 L4·L5 파일럿 후보 여전히 40건")
        r = client.get(f"/api/vocabulary-quiz/sessions/{l4l5_sid}/next")
        first_l4l5_item = r.json()["item"]["item_id"]
        check(first_l4l5_item in {row["item_id"] for row in json.load(open(
            REPO_ROOT / "data" / "vocab" / "pilot_l4l5_manifest_v1.json", encoding="utf-8"
        ))}, "L4·L5 파일럿 세션에서 출제된 문항이 L4·L5 매니페스트 소속(L6과 혼입 없음)")

        vdb2 = SessionLocal()
        try:
            cand4 = mf._select_level_candidates(vdb2, 4, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates")
            cand5 = mf._select_level_candidates(vdb2, 5, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates")
            cand6 = mf._select_level_candidates(vdb2, 6, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates")
        finally:
            vdb2.close()
        check(not (set(cand4) & db_l6_ids), "레벨모드 L4 후보에 L6 파일럿 item_id 없음(격리)")
        check(not (set(cand5) & db_l6_ids), "레벨모드 L5 후보에 L6 파일럿 item_id 없음(격리)")
        check(not (set(cand6) & db_l6_ids),
              "레벨모드 L6 후보에 L6 파일럿 item_id 없음(콘텐츠 level_status가 조건에 맞아도 "
              "문항 source_version 필터에서 걸러짐 - phase28 report 4-1절과 동일 근거)")

        # ---------- 11. 데이터 불변 확인 ----------
        conn = sqlite3.connect(db_path)
        counts_after = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("vocabulary_contents", "vocabulary_content_levels", "vocabulary_multiformat_items")
        }
        exposure = conn.execute("SELECT SUM(student_exposure), SUM(public_ready) FROM vocabulary_contents").fetchone()
        review_boundary_intact = conn.execute(
            "SELECT COUNT(*) FROM vocabulary_content_levels WHERE level_version=? AND level_status='REVIEW_BOUNDARY' "
            "AND vocab_level=6 "
            "AND content_id IN (SELECT DISTINCT source_content_id FROM vocabulary_multiformat_items WHERE source_version=?)",
            (mf.LEVEL_VERSION, L6_PILOT_MARKER),
        ).fetchone()[0]
        conn.close()
        for t in counts_before:
            check(counts_before[t] == counts_after[t], f"{t} 행 수 불변(전 {counts_before[t]}, 후 {counts_after[t]})")
        check(exposure == (0, 0), f"student_exposure/public_ready 합계 여전히 0/0(실제 {exposure})")
        check(review_boundary_intact == 20,
              f"L6 파일럿 20개 content_id 전부 vocab_level=6/REVIEW_BOUNDARY 그대로 유지(실제 {review_boundary_intact}/20)")

    finally:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys=ON")
        for sid in session_ids:
            conn.execute("DELETE FROM vocabulary_multiformat_responses WHERE session_id=?", (sid,))
            conn.execute("DELETE FROM vocabulary_multiformat_sessions WHERE id=?", (sid,))
        conn.execute("DELETE FROM vocabulary_multiformat_items WHERE item_id LIKE 'MF_A_SC_SRL6PILOT_FAKE_%'")
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
