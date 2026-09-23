"""관리자 전용 레벨별 어휘 퀴즈(v1) 회귀 테스트 - 로컬 R&D 전용.

admin_level_quiz_v1 패키지의 CLAUDE_CODE_MIGRATION.md "테스트" 절에 나열된
20개 범주 중 코드로 검증 가능한 항목을 다룬다(모바일 렌더링은 브라우저로
별도 수동 확인, 기존 전체 회귀는 다른 테스트 파일들을 함께 실행해 확인).

tests/test_multiformat_quiz_play.py와 같은 방식(pytest 없음, TestClient로
실제 앱 end-to-end).

실행:
    python tests/test_admin_level_quiz.py
"""
from __future__ import annotations

import hashlib
import io
import json
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


EXPECTED_ALL_CANDIDATES = {0: (121, 32), 1: (316, 87), 2: (332, 88), 3: (338, 91), 4: (8, 2), 5: (0, 0), 6: (0, 0)}
EXPECTED_AUTO_ONLY = {0: (113, 30), 1: (244, 67), 2: (4, 1), 3: (316, 85), 4: (0, 0), 5: (0, 0), 6: (0, 0)}


def run() -> bool:
    db_path = get_db_path()
    idiom_db = REPO_ROOT / "data" / "vocab" / "idiom.db"
    idiom_before = sha(idiom_db) if idiom_db.exists() else None

    conn0 = sqlite3.connect(db_path)
    counts_before = {
        t: conn0.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("vocabulary_contents", "vocabulary_items", "vocabulary_review_samples",
                  "vocabulary_multiformat_items", "vocabulary_content_levels")
    }
    conn0.close()

    test_uid = None
    session_ids: list[str] = []

    try:
        # ---------- 5/10. 레벨·유형별 가용량 함수 단위 검증(패키지 기대값과 정확히 대조) ----------
        db = next(get_db())  # noqa: F841  (아래서는 vocabulary_quiz 세션을 별도로 연다)
        from app.vocabulary_quiz.db import SessionLocal
        vdb = SessionLocal()
        try:
            for level, (exp_count, exp_distinct) in EXPECTED_ALL_CANDIDATES.items():
                data = mf._level_availability(vdb, level, "all_candidates", list(mf.LEVEL_MODE_ITEM_TYPES))
                check(data["available_items"] == exp_count and data["distinct_words"] == exp_distinct,
                      f"[all_candidates] L{level} 가용량 정확(기대 {exp_count}/{exp_distinct}, "
                      f"실제 {data['available_items']}/{data['distinct_words']})")
            for level, (exp_count, exp_distinct) in EXPECTED_AUTO_ONLY.items():
                data = mf._level_availability(vdb, level, "auto_only", list(mf.LEVEL_MODE_ITEM_TYPES))
                check(data["available_items"] == exp_count and data["distinct_words"] == exp_distinct,
                      f"[auto_only] L{level} 가용량 정확(기대 {exp_count}/{exp_distinct}, "
                      f"실제 {data['available_items']}/{data['distinct_words']})")

            # ---------- 12. MATCH 복합 문항 전 항목 일치(L3에만 1건, 나머지는 0건) ----------
            match_total = 0
            for level in range(7):
                cands = mf._select_level_candidates(vdb, level, ["MATCH_WORD_MEANING"], "all_candidates")
                if level == 3:
                    check(len(cands) == 1, f"MATCH_WORD_MEANING L3 후보 1건(실제 {len(cands)})")
                else:
                    check(len(cands) == 0, f"MATCH_WORD_MEANING L{level} 후보 0건(실제 {len(cands)})")
                match_total += len(cands)
            check(match_total == 1, f"MATCH_WORD_MEANING 전체 레벨 합계 1건(실제 {match_total})")

            # ---------- 5. L0~L4 순수성: 모든 단일 후보의 실제 content_id 레벨이 선택 레벨과 일치 ----------
            conn = sqlite3.connect(db_path)
            for level in (0, 1, 2, 3, 4):
                cands = mf._select_level_candidates(vdb, level, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates")
                impure = 0
                for iid in cands:
                    row = conn.execute(
                        "SELECT item_type, source_content_id, source_content_ids_json "
                        "FROM vocabulary_multiformat_items WHERE item_id=?", (iid,)
                    ).fetchone()
                    item_type, cid, cids_json = row
                    cids = json.loads(cids_json) if cids_json else [cid]
                    for c in cids:
                        lv = conn.execute(
                            "SELECT vocab_level FROM vocabulary_content_levels "
                            "WHERE content_id=? AND level_version='level_policy_v0.1'", (c,)
                        ).fetchone()
                        if lv is None or lv[0] != level:
                            impure += 1
                check(impure == 0, f"L{level} 후보 {len(cands)}건 전부 선택 레벨과 정확히 일치(위반 {impure}건)")
            conn.close()

            # ---------- 8. inactive 레벨 제외 / 9. 다른 level_version 제외 ----------
            probe_row = vdb.execute(
                __import__("sqlalchemy").text(
                    "SELECT content_id FROM vocabulary_content_levels WHERE vocab_level=0 LIMIT 1"
                )
            ).fetchone()
            probe_cid = probe_row[0]
            before_l0 = mf._matching_level_content_ids(vdb, 0, "all_candidates")
            check(probe_cid in before_l0, "사전조건: 표본 content_id가 원래 L0 매칭 집합에 있음")

            conn = sqlite3.connect(db_path)
            conn.execute(
                "INSERT INTO vocabulary_content_levels "
                "(content_id, vocab_level, level_status, level_version, is_active) "
                "VALUES (?, 0, 'PROVISIONAL_AUTO', 'level_policy_v0.2', 1)",
                (probe_cid,),
            )
            conn.execute(
                "UPDATE vocabulary_content_levels SET is_active=0 "
                "WHERE content_id=? AND level_version='level_policy_v0.1'",
                (probe_cid,),
            )
            conn.commit()
            conn.close()

            vdb.close()
            vdb = SessionLocal()
            after_l0 = mf._matching_level_content_ids(vdb, 0, "all_candidates")
            check(probe_cid not in after_l0, "inactive(is_active=0)로 바뀐 레벨 행은 후보에서 제외됨")

            other_version_rows = vdb.execute(
                __import__("sqlalchemy").text(
                    "SELECT COUNT(*) FROM vocabulary_content_levels WHERE level_version='level_policy_v0.2'"
                )
            ).fetchone()
            check(other_version_rows[0] >= 1, "사전조건: level_policy_v0.2 행이 실제로 삽입됨")
            # _matching_level_content_ids는 LEVEL_VERSION(level_policy_v0.1) 하드코딩 필터라
            # v0.2 행은애초에 조회 대상이 아니다 - after_l0에 probe_cid가 없다는 사실 자체가
            # (a) is_active=0 제외와 (b) v0.2 행이 v0.1 결과에 섞이지 않음을 함께 증명한다.

            conn = sqlite3.connect(db_path)
            conn.execute("DELETE FROM vocabulary_content_levels WHERE content_id=? AND level_version='level_policy_v0.2'", (probe_cid,))
            conn.execute("UPDATE vocabulary_content_levels SET is_active=1 WHERE content_id=? AND level_version='level_policy_v0.1'", (probe_cid,))
            conn.commit()
            conn.close()

            vdb.close()
            vdb = SessionLocal()
            restored_l0 = mf._matching_level_content_ids(vdb, 0, "all_candidates")
            check(probe_cid in restored_l0, "테스트 데이터 원복 확인(다시 L0 매칭 집합에 있음)")
        finally:
            vdb.close()

        # ---------- 1. 비로그인 차단 ----------
        c0 = TestClient(app, follow_redirects=False)
        r = c0.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 302 and "/login" in r.headers.get("location", ""), "비로그인 GET /multiformat/play 차단(302)")
        r = c0.get("/api/vocabulary-quiz/availability?level=1")
        check(r.status_code == 302, "비로그인 GET /availability 차단(302)")
        r = c0.post("/api/vocabulary-quiz/sessions", json={"item_types": ["MEANING_CHOICE"], "question_count": 5,
                                                             "selected_vocab_level": 1})
        check(r.status_code == 302, "비로그인 POST /sessions(레벨 지정) 차단(302)")

        # ---------- 2. 로그인 비관리자 403 ----------
        db = next(get_db())
        test_uid = str(uuid.uuid4())
        db.add(User(id=test_uid, email="admlvl.nonadmin.test@example.com", hashed_pw=hash_password("x"), is_admin=False))
        db.commit()
        db.close()
        c1 = TestClient(app, follow_redirects=False)
        c1.cookies.set(COOKIE_NAME, make_session_cookie(test_uid))
        r = c1.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 403, "비관리자 GET /multiformat/play 차단(403)")
        r = c1.get("/api/vocabulary-quiz/availability?level=1")
        check(r.status_code == 403, "비관리자 GET /availability 차단(403)")
        r = c1.post("/api/vocabulary-quiz/sessions", json={"item_types": ["MEANING_CHOICE"], "question_count": 5,
                                                             "selected_vocab_level": 1})
        check(r.status_code == 403, "비관리자 POST /sessions(레벨 지정) 차단(403)")

        # ---------- 3. 관리자 화면 200 + 레벨 선택기 존재 ----------
        client = TestClient(app, follow_redirects=True)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        r = client.get("/vocabulary-quiz/multiformat/play")
        check(r.status_code == 200, "관리자 GET /multiformat/play 200")
        check('id="level-select"' in r.text, "화면에 레벨 선택기(level-select) 존재")
        check('id="confidence-field"' in r.text, "화면에 후보 신뢰도 선택 UI 존재")
        check("L5" in r.text and "disabled" in r.text, "L5 옵션이 비활성(disabled)으로 렌더링됨")

        # ---------- 4. 기본 confidence_mode=all_candidates ----------
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE"], "question_count": 5, "selected_vocab_level": 1})
        check(r.status_code == 200, "confidence_mode 생략 시 세션 생성 성공(기본값 적용)")
        data = r.json()
        session_ids.append(data["session_id"])
        check(data["level_info"]["confidence_mode"] == "all_candidates", "confidence_mode 기본값이 all_candidates")

        # ---------- 6. L5/L6 비활성 및 직접 요청 안전 처리 ----------
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE"], "question_count": 1, "selected_vocab_level": 5})
        check(r.status_code == 409, "L5 직접 요청 시 크래시 없이 409(후보 0건)")
        body = r.json().get("detail", {})
        check(body.get("available") == 0 and body.get("level") == 5, "L5 409 응답 본문 구조 정확")

        r = client.get("/api/vocabulary-quiz/availability?level=6&item_type=MEANING_CHOICE")
        check(r.status_code == 200 and r.json()["available_items"] == 0, "L6 가용량 조회는 정상 200 + 0건(크래시 없음)")

        # ---------- 7. auto_only 상태 필터(L2=4건/1어휘, L4=0건) ----------
        r = client.get("/api/vocabulary-quiz/availability?level=2&confidence_mode=auto_only&item_type=MEANING_CHOICE&item_type=WORD_FROM_DEFINITION&item_type=CONTEXT_MEANING&item_type=CONTEXT_CLOZE")
        check(r.status_code == 200 and r.json()["available_items"] == 4 and r.json()["distinct_words"] == 1,
              "auto_only L2 가용량 4건/1어휘")
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE"], "question_count": 1, "selected_vocab_level": 4,
                               "confidence_mode": "auto_only"})
        check(r.status_code == 409 and r.json()["detail"]["available"] == 0, "auto_only L4 요청 시 409(후보 0건)")

        # ---------- 11. 부족 수량 차단, 혼합 보충 없음(L4 전체후보=8건, 10건 요청) ----------
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE", "WORD_FROM_DEFINITION", "CONTEXT_MEANING", "CONTEXT_CLOZE"],
                               "question_count": 10, "selected_vocab_level": 4})
        check(r.status_code == 409, "L4 10문항 요청(가용 8건) 시 409 - 다른 레벨/유형 자동 보충 없음")
        check(r.json()["detail"]["available"] == 8 and r.json()["detail"]["code"] == "INSUFFICIENT_LEVEL_CANDIDATES",
              "부족 응답 코드/available 정확")

        # ---------- 13. 레벨 모드 CROSSWORD 서버 거부(422) ----------
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["CROSSWORD"], "question_count": 1, "selected_vocab_level": 1})
        check(r.status_code == 422, "레벨 모드에서 CROSSWORD 요청 시 422")

        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE"], "question_count": 1, "selected_vocab_level": 7})
        check(r.status_code == 422, "잘못된 레벨(7) 요청 시 422")

        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE"], "question_count": 1, "selected_vocab_level": 1,
                               "confidence_mode": "not_a_mode"})
        check(r.status_code == 422, "잘못된 confidence_mode 요청 시 422")

        # ---------- 14. 전체 모드 CROSSWORD 회귀(레벨 미지정) ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"item_types": ["CROSSWORD"]})
        check(r.status_code == 200, "전체 모드 CROSSWORD 세션 생성 여전히 정상(회귀 없음)")
        session_ids.append(r.json()["session_id"])
        check(r.json()["level_info"] is None, "전체 모드 세션의 level_info는 None")

        # ---------- 15. 세션 메타데이터 저장 / 17. 정답 비노출 ----------
        r = client.post("/api/vocabulary-quiz/sessions",
                         json={"item_types": ["MEANING_CHOICE"], "question_count": 3, "selected_vocab_level": 1,
                               "confidence_mode": "all_candidates"})
        check(r.status_code == 200, "L1 3문항 세션 생성 성공")
        sid = r.json()["session_id"]
        session_ids.append(sid)
        conn = sqlite3.connect(db_path)
        meta_row = conn.execute("SELECT metadata_json FROM vocabulary_multiformat_sessions WHERE id=?", (sid,)).fetchone()
        conn.close()
        check(meta_row is not None and meta_row[0] is not None, "세션 행에 metadata_json이 실제로 저장됨")
        meta = json.loads(meta_row[0])
        check(meta == {"audience": "ADMIN_ONLY", "selected_vocab_level": 1, "confidence_mode": "all_candidates",
                       "level_version": "level_policy_v0.1", "requested_count": 3, "candidate_count": meta["candidate_count"],
                       "actual_count": 3, "item_types": ["MEANING_CHOICE"]},
              f"metadata_json 필드 구성 정확({meta})")

        r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/next")
        item = r.json()["item"]
        check(not ({"correct_option", "answer_text", "accepted_answers", "answers"} & set(item.keys())),
              "레벨 모드 문제 조회 응답에도 정답 필드 비노출")

        conn = sqlite3.connect(db_path)
        correct_option = conn.execute(
            "SELECT json_extract(answer_payload_json, '$.correct_option') FROM vocabulary_multiformat_items WHERE item_id=?",
            (item["item_id"],),
        ).fetchone()[0]
        conn.close()
        for _ in range(3):
            r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/next")
            if r.json()["done"]:
                break
            it = r.json()["item"]
            conn = sqlite3.connect(db_path)
            co = conn.execute(
                "SELECT json_extract(answer_payload_json, '$.correct_option') FROM vocabulary_multiformat_items WHERE item_id=?",
                (it["item_id"],),
            ).fetchone()[0]
            conn.close()
            client.post(f"/api/vocabulary-quiz/sessions/{sid}/answer", json={"item_id": it["item_id"], "selected_option": co})

        r = client.get(f"/api/vocabulary-quiz/sessions/{sid}/result")
        check(r.status_code == 200 and r.json()["correct"] == 3, "L1 3문항 세션 전부 정답 처리(결과 집계 정확)")
        li = r.json()["level_info"]
        check(li["selected_vocab_level"] == 1 and li["grade_label"] == "초등 3~4학년" and li["level_version"] == "level_policy_v0.1",
              "결과 화면 level_info 정확(선택 레벨/학년군/정책버전)")

        # ---------- 16. 기존 세션 조회 호환(metadata_json NULL) ----------
        r = client.post("/api/vocabulary-quiz/sessions", json={"item_types": ["MEANING_CHOICE"], "question_count": 1})
        check(r.status_code == 200, "전체 모드(레벨 미지정) 세션 생성 성공")
        sid2 = r.json()["session_id"]
        session_ids.append(sid2)
        it = client.get(f"/api/vocabulary-quiz/sessions/{sid2}/next").json()["item"]
        conn = sqlite3.connect(db_path)
        co = conn.execute(
            "SELECT json_extract(answer_payload_json, '$.correct_option') FROM vocabulary_multiformat_items WHERE item_id=?",
            (it["item_id"],),
        ).fetchone()[0]
        conn.close()
        client.post(f"/api/vocabulary-quiz/sessions/{sid2}/answer", json={"item_id": it["item_id"], "selected_option": co})
        r = client.get(f"/api/vocabulary-quiz/sessions/{sid2}/result")
        check(r.status_code == 200 and r.json()["level_info"] is None,
              "metadata_json이 NULL인(레벨 미지정) 세션도 정상 조회되고 level_info=None")

        # ---------- 기존 데이터 불변 확인 ----------
        conn = sqlite3.connect(db_path)
        counts_after = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            for t in ("vocabulary_contents", "vocabulary_items", "vocabulary_review_samples",
                      "vocabulary_multiformat_items", "vocabulary_content_levels")
        }
        n_exp = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE student_exposure=1").fetchone()[0]
        n_pub = conn.execute("SELECT COUNT(*) FROM vocabulary_contents WHERE public_ready=1").fetchone()[0]
        n_crossword = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE item_type='CROSSWORD'").fetchone()[0]
        conn.close()
        for t in counts_before:
            check(counts_before[t] == counts_after[t], f"{t} 행 수 불변(전 {counts_before[t]}, 후 {counts_after[t]})")
        check(n_crossword == 100, f"CROSSWORD 100세트 불변(실제 {n_crossword})")
        check(n_exp == 0 and n_pub == 0, "student_exposure/public_ready 여전히 0건")

        idiom_after = sha(idiom_db) if idiom_db.exists() else None
        check(idiom_before == idiom_after, "idiom.db 체크섬 불변")

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
