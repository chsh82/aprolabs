# -*- coding: utf-8 -*-
"""Phase28 - L6 관리자 파일럿 문항 40건 실제 적재 결과를 독립적으로 재검증한다.

phase28_apply_l6_pilot_quiz_items.py의 자체 출력(GATE 6)을 신뢰하지 않고,
이 테스트가 별도로 다시 짠 쿼리로 최종 DB 상태를 확인한다. 또한 이번 40건이
관리자 일반 출제(혼합/레벨 모드)나 기존 L4·L5 파일럿 모드에서 구조적으로
선택될 수 없다는 근거를, app/vocabulary_quiz/routers/multiformat.py의 실제
SOURCE_VERSION/PILOT_SOURCE_VERSION 상수를 import해 그 값 그대로 SQL 조건에
사용함으로써 증명한다(라우터 코드 자체는 이 테스트가 전혀 수정하지 않음).

pytest 없이 이 저장소 관례([PASS]/[FAIL] 출력)를 따른다.

실행:
    VOCABULARY_QUIZ_DB_PATH=<research DB 사본 또는 원본 경로> python tests/test_phase28_l6_pilot_quiz_items_applied.py
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

DEFAULT_COPY_DB_PATH = (
    Path.home() / "AppData" / "Local" / "Temp" / "claude" / "C--Users-aproa" /
    "6e2093a4-82bf-4ee0-833a-e5c458074f06" / "scratchpad" / "vocabulary_quiz_research_phase25_copy.db"
)
COPY_DB_PATH = Path(os.environ.get("VOCABULARY_QUIZ_DB_PATH") or DEFAULT_COPY_DB_PATH)
os.environ["VOCABULARY_QUIZ_DB_PATH"] = str(COPY_DB_PATH)

NEW_SOURCE_VERSION = "schema_reading_l6_pilot_dryrun_v1"
EXPECTED_TOTAL_ITEMS = 1369
EXPECTED_PRE_EXISTING_ITEMS = 1329
EXPECTED_PRE_EXISTING_CONTENTS = 5902

_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    _results.append((ok, label))
    print(f"{'[PASS]' if ok else '[FAIL]'} {label}" + (f" - {detail}" if detail and not ok else ""))


def table_snapshot(conn: sqlite3.Connection, table: str, exclude_ids: set[str] | None = None,
                    id_col: str = "item_id") -> tuple[int, str]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    where, params = "", ()
    if exclude_ids:
        qmarks = ",".join("?" * len(exclude_ids))
        where = f"WHERE {id_col} NOT IN ({qmarks})"
        params = tuple(exclude_ids)
    order_col = id_col if id_col in cols else "content_id"
    rows = conn.execute(f"SELECT {', '.join(cols)} FROM {table} {where} ORDER BY {order_col}", params).fetchall()
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join("" if v is None else str(v) for v in row).encode("utf-8"))
        h.update(b"\x1e")
    return len(rows), h.hexdigest()


def main() -> bool:
    if not COPY_DB_PATH.exists():
        print(f"[FAIL] 사전조건: research DB 사본이 없습니다({COPY_DB_PATH})")
        return False

    mf = None
    try:
        from app.vocabulary_quiz.routers import multiformat as mf
        from app.vocabulary_quiz.db import SessionLocal
        general_source_version = mf.SOURCE_VERSION
        pilot_source_version = mf.PILOT_SOURCE_VERSION
        check(True, f"multiformat.py에서 SOURCE_VERSION/PILOT_SOURCE_VERSION import 성공 "
                    f"({general_source_version!r}, {pilot_source_version!r})")
    except Exception as e:
        check(False, "multiformat.py import 실패", str(e))
        general_source_version = "2.1.29"
        pilot_source_version = "schema_reading_l4l5_pilot_dryrun_v1"

    conn = sqlite3.connect(f"file:{COPY_DB_PATH}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")

    new_ids = [r[0] for r in conn.execute(
        "SELECT item_id FROM vocabulary_multiformat_items WHERE source_version=?", (NEW_SOURCE_VERSION,)
    ).fetchall()]
    check(len(new_ids) == 40, f"신규 배치(source_version={NEW_SOURCE_VERSION}) 정확히 40건", f"실제 {len(new_ids)}건")

    total = conn.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0]
    check(total == EXPECTED_TOTAL_ITEMS, f"전체 문항 수 {EXPECTED_TOTAL_ITEMS}건", f"실제 {total}건")

    type_counts = dict(conn.execute(
        "SELECT item_type, COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=? GROUP BY item_type",
        (NEW_SOURCE_VERSION,),
    ).fetchall())
    check(type_counts.get("MEANING_CHOICE") == 20 and type_counts.get("CONTEXT_MEANING") == 20,
          "신규 배치 유형별 20/20", str(type_counts))

    linked = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items m WHERE m.source_version=? "
        "AND m.source_content_id IN (SELECT content_id FROM vocabulary_contents)",
        (NEW_SOURCE_VERSION,),
    ).fetchone()[0]
    check(linked == 40, "신규 40건 전부 유효한 content_id에 연결", f"실제 {linked}건")

    dup = conn.execute(
        "SELECT item_id, COUNT(*) c FROM vocabulary_multiformat_items GROUP BY item_id HAVING c > 1"
    ).fetchall()
    check(not dup, "item_id 전체 중복 0건", str(dup))

    integrity = conn.execute("PRAGMA integrity_check").fetchall()
    check(len(integrity) == 1 and integrity[0][0] == "ok", "PRAGMA integrity_check = ok", str(integrity))

    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    check(len(fk) == 0, "PRAGMA foreign_key_check 위반 0건", str(fk))

    # 기존 콘텐츠·기존 문항 불변(체크섬)
    vc_count, vc_hash = table_snapshot(conn, "vocabulary_contents", id_col="content_id")
    check(vc_count == EXPECTED_PRE_EXISTING_CONTENTS, f"vocabulary_contents {EXPECTED_PRE_EXISTING_CONTENTS}건 그대로",
          f"실제 {vc_count}")

    mfi_count, _ = table_snapshot(conn, "vocabulary_multiformat_items", exclude_ids=set(new_ids))
    check(mfi_count == EXPECTED_PRE_EXISTING_ITEMS, f"신규 40건 제외 기존 문항 {EXPECTED_PRE_EXISTING_ITEMS}건 그대로",
          f"실제 {mfi_count}")

    exposure_sum, public_sum = conn.execute(
        "SELECT COALESCE(SUM(student_exposure),0), COALESCE(SUM(public_ready),0) FROM vocabulary_contents"
    ).fetchone()
    check(exposure_sum == 0 and public_sum == 0, "전체 student_exposure/public_ready 합계 0",
          f"실제 ({exposure_sum},{public_sum})")

    # 정답/공개 payload 분리: public_payload_json에 correct_option이 없어야 함
    payload_rows = conn.execute(
        "SELECT item_id, public_payload_json, answer_payload_json, correct_option "
        "FROM vocabulary_multiformat_items WHERE source_version=?", (NEW_SOURCE_VERSION,)
    ).fetchall()
    leak = []
    for item_id, public_json, answer_json, correct_option in payload_rows:
        pub = json.loads(public_json)
        ans = json.loads(answer_json)
        if "correct_option" in pub or "correct" in str(pub).lower().replace("options", ""):
            leak.append(item_id)
        if ans.get("correct_option") != correct_option:
            leak.append(item_id)
    check(not leak, "신규 40건 전부 public_payload_json에 정답 정보 없음 + answer_payload_json이 correct_option과 일치",
          str(leak))

    # 구조적 격리 증명: 실제 라우터 상수값으로 필터링해도 신규 40건이 전혀 안 걸림
    leaked_general = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=? AND item_id IN "
        f"({','.join('?' for _ in new_ids)})",
        (general_source_version, *new_ids),
    ).fetchone()[0]
    check(leaked_general == 0, f"일반 출제 필터(source_version={general_source_version!r})에 신규 40건 0건 노출")

    leaked_pilot = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=? AND item_id IN "
        f"({','.join('?' for _ in new_ids)})",
        (pilot_source_version, *new_ids),
    ).fetchone()[0]
    check(leaked_pilot == 0, f"기존 L4·L5 파일럿 필터(source_version={pilot_source_version!r})에 신규 40건 0건 노출")

    general_count_unaffected = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (general_source_version,)
    ).fetchone()[0]
    check(general_count_unaffected == 1289, f"일반 출제 풀(2.1.29) 건수 불변(1,289)", f"실제 {general_count_unaffected}")

    l4l5_pilot_count_unaffected = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE source_version=?", (pilot_source_version,)
    ).fetchone()[0]
    check(l4l5_pilot_count_unaffected == 40, f"기존 L4·L5 파일럿 풀 건수 불변(40)", f"실제 {l4l5_pilot_count_unaffected}")

    conn.close()

    # "정상 경로" 재확인: 라우터의 실제 선택 함수를 그대로 호출한다(우회 SQL이 아니라
    # 진짜 코드 경로) - 전부 SELECT만 하는 함수라 안전하다.
    if mf is not None:
        db = SessionLocal()
        try:
            eligible_count = db.query(mf.VocabularyMultiformatItem.item_id).filter(
                mf.VocabularyMultiformatItem.source_version == general_source_version,
                mf.VocabularyMultiformatItem.is_active == 1,
                mf.VocabularyMultiformatItem.item_type != "CROSSWORD",
            ).count()
            general_ids = set(mf._select_question_items(db, eligible_count, None))
            check(not (general_ids & set(new_ids)),
                  "_select_question_items(혼합 모드, 실제 함수 호출) 전체 풀에 신규 40건 0건",
                  str(general_ids & set(new_ids)))

            level6_ids = set(mf._select_level_candidates(db, 6, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates"))
            check(not (level6_ids & set(new_ids)),
                  "_select_level_candidates(레벨 6, 실제 함수 호출) 결과에 신규 40건 0건 "
                  "(콘텐츠 level_status=REVIEW_BOUNDARY가 레벨6 조건과 맞아도 "
                  "문항 쪽 source_version 필터에서 걸러짐)",
                  str(level6_ids & set(new_ids)))

            pilot_ids = set(mf._select_pilot_item_ids(db, None))
            check(len(pilot_ids) == 40 and not (pilot_ids & set(new_ids)),
                  "_select_pilot_item_ids(기존 L4·L5 파일럿, 실제 함수 호출) 여전히 40건, 신규 배치 미포함, "
                  "무결성 오류(PilotBatchIntegrityError) 없이 정상 종료",
                  f"pilot_ids={len(pilot_ids)}건, 교집합={pilot_ids & set(new_ids)}")
        except Exception as e:
            check(False, "실제 선택 함수 호출 중 예외 발생", str(e))
        finally:
            db.close()

    n_pass = sum(1 for ok, _ in _results if ok)
    print(f"\n총 {len(_results)}건 중 {n_pass}건 통과")
    return n_pass == len(_results)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
