"""Closed Pilot behavioral evidence logging contract regression test.

This test runs against the local vocabulary quiz DB and cleans up its own rows.
It verifies that runtime exposure/response rows can store the minimum pilot
logging fields required before real student pilot operation.

실행:
    python tests/test_vocabulary_pilot_evidence_logging.py
"""
from __future__ import annotations

import io
import sqlite3
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from app.auth import COOKIE_NAME, make_session_cookie  # noqa: E402
from app.main import app  # noqa: E402
from app.vocabulary_quiz.db import get_db_path  # noqa: E402

ADMIN_ID = "de86bad0-e684-457e-8793-075785a65d05"

_REQUIRED_SESSION_COLUMNS = {
    "student_cohort_id",
    "is_internal_tester",
    "is_verified_student",
}
_REQUIRED_RESPONSE_COLUMNS = {
    "sense_id",
    "service_level",
    "response_time_ms",
    "attempt_no",
    "presented_at",
    "item_status_at_exposure",
    "content_release_version",
}

_PASS = "[PASS]"
_FAIL = "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def run() -> bool:
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    session_ids: list[str] = []
    try:
        session_cols = _columns(conn, "vocabulary_multiformat_sessions")
        response_cols = _columns(conn, "vocabulary_multiformat_responses")
        missing_session = sorted(_REQUIRED_SESSION_COLUMNS - session_cols)
        missing_response = sorted(_REQUIRED_RESPONSE_COLUMNS - response_cols)
        check(not missing_session, f"pilot session evidence columns exist: missing={missing_session}")
        check(not missing_response, f"pilot response evidence columns exist: missing={missing_response}")
        if missing_session or missing_response:
            return False

        client = TestClient(app, follow_redirects=False)
        client.cookies.set(COOKIE_NAME, make_session_cookie(ADMIN_ID))
        body = {
            "selected_vocab_level": 1,
            "confidence_mode": "all_candidates",
            "item_types": ["MEANING_CHOICE"],
            "question_count": 1,
            "student_cohort_id": "pilot-contract-test-cohort",
            "is_internal_tester": True,
            "is_verified_student": False,
        }
        r = client.post("/api/vocabulary-quiz/sessions", json=body)
        check(r.status_code == 200, f"session create with pilot evidence metadata accepted ({r.status_code})")
        if r.status_code != 200:
            print(r.text)
            return False
        session_id = r.json()["session_id"]
        session_ids.append(session_id)

        r = client.get(f"/api/vocabulary-quiz/sessions/{session_id}/next")
        check(r.status_code == 200 and not r.json().get("done"), "next question returns active item")
        if r.status_code != 200:
            print(r.text)
            return False
        item = r.json()["item"]
        item_id = item["item_id"]
        selected = item.get("options", [None])[0]
        if isinstance(selected, dict):
            selected_option = selected.get("option_no") or selected.get("index") or 1
        else:
            selected_option = 1
        r = client.post(
            f"/api/vocabulary-quiz/sessions/{session_id}/answer",
            json={"item_id": item_id, "selected_option": selected_option, "response_time_ms": 1234},
        )
        check(r.status_code == 200, f"answer accepts response_time_ms ({r.status_code})")
        if r.status_code != 200:
            print(r.text)
            return False

        row = conn.execute(
            """
            SELECT s.student_cohort_id, s.is_internal_tester, s.is_verified_student,
                   r.sense_id, r.service_level, r.response_time_ms, r.attempt_no,
                   r.presented_at, r.item_status_at_exposure, r.content_release_version,
                   r.answered_at
            FROM vocabulary_multiformat_sessions s
            JOIN vocabulary_multiformat_responses r ON r.session_id = s.id
            WHERE s.id = ?
            """,
            (session_id,),
        ).fetchone()
        check(row is not None, "response row persisted for pilot evidence check")
        if row is None:
            return False
        (cohort, internal, verified, sense_id, level, response_time, attempt_no,
         presented_at, status_at_exposure, release_version, answered_at) = row
        check(cohort == "pilot-contract-test-cohort", "student_cohort_id stored on session")
        check(internal == 1 and verified == 0, "internal/student cohort flags stored distinctly")
        check(bool(sense_id), "sense_id snapshot stored on response")
        check(level == 1, "service_level snapshot stored on response")
        check(response_time == 1234, "response_time_ms stored on response")
        check(attempt_no == 1, "attempt_no stored on response")
        check(bool(presented_at), "presented_at stored when item is served")
        check(bool(status_at_exposure), "item_status_at_exposure snapshot stored")
        check(bool(release_version), "content_release_version snapshot stored")
        check(bool(answered_at), "answered_at stored")
    finally:
        for sid in session_ids:
            conn.execute("DELETE FROM vocabulary_multiformat_responses WHERE session_id = ?", (sid,))
            conn.execute("DELETE FROM vocabulary_multiformat_sessions WHERE id = ?", (sid,))
        conn.commit()
        conn.close()
    passed = sum(1 for ok, _ in _results if ok)
    total = len(_results)
    print(f"\n결과: {passed}/{total} PASS")
    return passed == total


if __name__ == "__main__":
    raise SystemExit(0 if run() else 1)
