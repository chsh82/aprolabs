"""Phase20 - 파일럿 선정 조건("_select_pilot_item_ids") 오염 방지 회귀 테스트.

배경(reports/schema_reading_phase20_*.md 참고): 기존 _select_pilot_item_ids는
source_version == PILOT_SOURCE_VERSION + is_active=1 + content 존재/활성 +
level_status='REVIEW_BOUNDARY' 3중 검증만 했다. 이 조건은 나중에 다른 작업이
우연히/실수로 같은 source_version 값을 가진 새 문항을 만들면 그 문항도 자동으로
파일럿 풀에 섞여 들어오는 취약점이 있었다(로컬 DB 사본으로 실제 재현됨 - 41건
반환 확인). 수정 후에는 phase18 매니페스트(data/import/
schema_reading_phase18_quiz_pilot_rows_20260926.json)의 고정 40개 item_id
화이트리스트와 교집합하고, 그 결과가 화이트리스트와 정확히 일치하지 않으면
(오염이 남아있거나 반대로 원래 40건 중 일부가 상태 drift로 빠졌거나)
PilotBatchIntegrityError -> HTTPException(500, code=PILOT_BATCH_INTEGRITY_ERROR)로
출제 자체를 중단한다.

이 스크립트는 tests/test_phase19_admin_pilot_mode.py와 같은 방식(pytest 없음,
FastAPI TestClient, [PASS]/[FAIL] 출력)이다. **research DB의 읽기 전용 사본**이
필요하다(로컬 vocabulary_quiz_rnd.db에는 phase18 데이터가 없음) - 운영/연구
서버에는 어떤 것도 쓰지 않는다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<research DB 사본 경로> python tests/test_phase20_pilot_contamination_guard.py
(환경변수를 생략하면 스크립트 하단 DEFAULT_COPY_DB_PATH를 사용한다)

이 스크립트는 전달받은 DB 사본 파일을 직접 변형(오염 INSERT, level_status
UPDATE)해가며 검증하므로, **반드시 재사용 가능한 사본**(원본이 아닌 임시
파일)을 가리켜야 한다. 스크립트는 시작 시 사본을 한 번 더 임시 파일로 복제해
작업하고, 종료 시 그 임시 파일을 삭제한다 - 전달받은 사본 자체는 건드리지 않는다.
"""
from __future__ import annotations

import io
import json
import os
import shutil
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
SOURCE_COPY_DB_PATH = Path(os.environ.get("VOCABULARY_QUIZ_DB_PATH") or DEFAULT_COPY_DB_PATH)

if not SOURCE_COPY_DB_PATH.exists():
    print(f"[FAIL] 사전조건: research DB 사본이 없습니다({SOURCE_COPY_DB_PATH}) - "
          f"먼저 scp로 vocabulary_quiz_research.db 사본을 준비하세요.")
    sys.exit(1)

# 이 스크립트가 실제로 변형해서 쓸 임시 작업 사본 - 전달받은 사본은 건드리지 않는다.
WORK_DB_PATH = SOURCE_COPY_DB_PATH.parent / f"_phase20_guard_work_{uuid.uuid4().hex[:8]}.db"
shutil.copyfile(SOURCE_COPY_DB_PATH, WORK_DB_PATH)
os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(WORK_DB_PATH)  # app 모듈 import 전에 고정

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def main() -> int:
    from app.vocabulary_quiz.db import SessionLocal
    from app.vocabulary_quiz.routers import multiformat as mf

    manifest_ids = mf._expected_pilot_item_ids(None)
    check(len(manifest_ids) == 40, f"매니페스트 화이트리스트 40건 로드 (실제 {len(manifest_ids)}건)")

    # ---- 시나리오 0: 오염/드리프트 없는 정상 상태 ----
    db = SessionLocal()
    try:
        ids = mf._select_pilot_item_ids(db, None)
        check(len(ids) == 40, f"정상 상태: 정확히 40건 (실제 {len(ids)}건)")
        check(set(ids) == manifest_ids, "정상 상태: 매니페스트 화이트리스트와 정확히 일치")
    finally:
        db.close()

    # ---- 시나리오 1: 오염 재현 - 같은 source_version의 가짜 문항 삽입 ----
    con = sqlite3.connect(WORK_DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT * FROM vocabulary_multiformat_items "
        "WHERE source_version=? AND item_type='MEANING_CHOICE' LIMIT 1",
        (mf.PILOT_SOURCE_VERSION,),
    )
    cols = [d[0] for d in cur.description]
    template = dict(zip(cols, cur.fetchone()))
    fake_id = "MF_A_SC_FAKE_PHASE20_GUARD_TEST"
    fake = dict(template)
    fake["item_id"] = fake_id
    fake.pop("id", None)
    placeholders = ",".join("?" for _ in fake)
    colnames = ",".join(fake.keys())
    cur.execute(f"INSERT INTO vocabulary_multiformat_items ({colnames}) VALUES ({placeholders})",
                list(fake.values()))
    con.commit()
    con.close()

    db = SessionLocal()
    try:
        ids = mf._select_pilot_item_ids(db, None)
        check(len(ids) == 40, f"오염 주입 후에도 정확히 40건만 반환 (실제 {len(ids)}건)")
        check(fake_id not in ids, "오염 주입한 가짜 item_id는 후보에서 제외됨")
        check(set(ids) == manifest_ids, "오염 주입 후에도 매니페스트 화이트리스트와 정확히 일치")
    finally:
        db.close()

    # 오염 원복
    con = sqlite3.connect(WORK_DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM vocabulary_multiformat_items WHERE item_id=?", (fake_id,))
    con.commit()
    con.close()

    # ---- 시나리오 2: drift 재현 - 정상 40건 중 하나의 level_status 변경 ----
    con = sqlite3.connect(WORK_DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT source_content_id FROM vocabulary_multiformat_items "
        "WHERE source_version=? LIMIT 1", (mf.PILOT_SOURCE_VERSION,),
    )
    drift_cid = cur.fetchone()[0]
    cur.execute(
        "UPDATE vocabulary_content_levels SET level_status='PROVISIONAL_AUTO' "
        "WHERE content_id=? AND level_version='level_policy_v0.1'", (drift_cid,),
    )
    con.commit()
    con.close()

    db = SessionLocal()
    try:
        try:
            ids = mf._select_pilot_item_ids(db, None)
            check(False, f"drift 상태: 조용히 {len(ids)}건 반환됨(에러가 발생했어야 함)")
        except mf.PilotBatchIntegrityError as exc:
            check(True, f"drift 상태: PilotBatchIntegrityError로 출제 중단됨 (missing={sorted(exc.missing)})")
    finally:
        db.close()

    # API 레벨(HTTPException 500)에서도 동일하게 막히는지 확인
    from fastapi.testclient import TestClient
    from app.main import app
    from app.auth import make_session_cookie, COOKIE_NAME

    ADMIN_ID = "de86bad0-e684-457e-8793-075785a65d05"  # admin@aprolabs.co.kr, 로컬 aprolabs.db 관리자
    client = TestClient(app, follow_redirects=True)
    client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))

    r = client.get("/api/vocabulary-quiz/pilot-availability")
    check(r.status_code == 500 and r.json().get("detail", {}).get("code") == "PILOT_BATCH_INTEGRITY_ERROR",
          f"GET /pilot-availability: drift 상태에서 500 PILOT_BATCH_INTEGRITY_ERROR (실제 {r.status_code})")

    r2 = client.post("/api/vocabulary-quiz/sessions", json={"pilot_mode": True, "question_count": 40})
    check(r2.status_code == 500 and r2.json().get("detail", {}).get("code") == "PILOT_BATCH_INTEGRITY_ERROR",
          f"POST /sessions(pilot_mode): drift 상태에서 500 PILOT_BATCH_INTEGRITY_ERROR (실제 {r2.status_code})")

    # drift 원복
    con = sqlite3.connect(WORK_DB_PATH)
    cur = con.cursor()
    cur.execute(
        "UPDATE vocabulary_content_levels SET level_status='REVIEW_BOUNDARY' "
        "WHERE content_id=? AND level_version='level_policy_v0.1'", (drift_cid,),
    )
    con.commit()
    con.close()

    # ---- 시나리오 3: 원복 후 다시 정상 40건 ----
    db = SessionLocal()
    try:
        ids = mf._select_pilot_item_ids(db, None)
        check(len(ids) == 40 and set(ids) == manifest_ids, f"drift 원복 후 다시 정확히 40건 (실제 {len(ids)}건)")
    finally:
        db.close()

    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        try:
            # Windows에서는 SQLAlchemy engine이 파일 핸들을 쥐고 있으면 삭제가
            # 조용히 실패할 수 있어 - engine을 명시적으로 dispose한 뒤 삭제한다.
            from app.vocabulary_quiz.db import engine as _vq_engine
            _vq_engine.dispose()
        except Exception:
            pass
        try:
            WORK_DB_PATH.unlink(missing_ok=True)
        except Exception as _cleanup_exc:
            print(f"[WARN] 임시 작업 사본 삭제 실패({WORK_DB_PATH}): {_cleanup_exc} - 수동 삭제 필요")

    total = len(_results)
    failed = [label for ok, label in _results if not ok]
    print(f"\n총 {total}건 중 실패 {len(failed)}건")
    sys.exit(1 if failed else 0)
