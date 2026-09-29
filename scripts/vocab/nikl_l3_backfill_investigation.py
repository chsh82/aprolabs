# -*- coding: utf-8 -*-
"""L3 출제 풀 보강안 조사(읽기 전용) 재현 스크립트.

reports/nikl_l3_backfill_investigation_20260930.md의 모든 수치를
재현한다. 어떤 sqlite3.connect도 mode=ro로만 연다 - 이 스크립트는
DB에 절대 쓰지 않는다(마이그레이션/적재 스크립트가 아니므로
db_path_guard를 쓰지 않고, 대신 모든 연결을 mode=ro로 고정해 쓰기
자체가 원천적으로 불가능하게 한다).

실행:
    VOCABULARY_QUIZ_DB_PATH=<연구 DB 경로> python scripts/vocab/nikl_l3_backfill_investigation.py
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
from pathlib import Path


def connect_ro(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def job1(con: sqlite3.Connection) -> None:
    print("=== Job 1: 337 -> 64 재현 ===")
    cur = con.cursor()
    cur.execute("""
        SELECT mi.item_id, mi.item_type, mi.source_content_id
        FROM vocabulary_multiformat_items mi
        JOIN vocabulary_content_levels vcl ON vcl.content_id = mi.source_content_id AND vcl.is_active=1
        WHERE mi.source_version='2.1.29' AND mi.is_active=1 AND vcl.vocab_level=3
    """)
    rows = cur.fetchall()
    by_type: dict[str, int] = {}
    words = set()
    for r in rows:
        by_type[r["item_type"]] = by_type.get(r["item_type"], 0) + 1
        if r["source_content_id"]:
            words.add(r["source_content_id"])
    print(f"L3 baseline items: {len(rows)} (기대 337)")
    print("by_type:", by_type)
    print("distinct words:", len(words), "(기대 91)")

    cur.execute("""
        SELECT content_id FROM vocabulary_official_grade_reference
        WHERE review_status='RULE_PROPOSED_PENDING_APPROVAL' AND official_grade='4' AND proposed_base_level='L2'
    """)
    rule_a_pool = {r["content_id"] for r in cur.fetchall()}
    print("RULE_A pool size:", len(rule_a_pool), "(기대 1368)")

    survive = [r for r in rows if r["source_content_id"] not in rule_a_pool]
    by_type2: dict[str, int] = {}
    words2 = set()
    for r in survive:
        by_type2[r["item_type"]] = by_type2.get(r["item_type"], 0) + 1
        if r["source_content_id"]:
            words2.add(r["source_content_id"])
    print(f"post-RULE-A items: {len(survive)} (기대 64)")
    print("by_type(post):", by_type2)
    print("distinct words(post):", len(words2), "(기대 17)")

    cur.execute("""
        SELECT item_id, source_content_ids_json FROM vocabulary_multiformat_items
        WHERE source_version='2.1.29' AND is_active=1 AND item_type='MATCH_WORD_MEANING'
    """)
    cur2 = con.cursor()
    cur2.execute("SELECT content_id FROM vocabulary_content_levels WHERE is_active=1 AND vocab_level=3")
    l3set = {r["content_id"] for r in cur2.fetchall()}
    import json
    mwm_l3 = 0
    for r in cur.fetchall():
        cids = json.loads(r["source_content_ids_json"] or "[]")
        if cids and all(c in l3set for c in cids):
            mwm_l3 += 1
    print("MATCH_WORD_MEANING items fully at L3 (앱 실제 선택 로직 기준 추가분):", mwm_l3)


def job2(con: sqlite3.Connection) -> None:
    print("\n=== Job 2: 공식 5등급 49건 전수 조사 ===")
    cur = con.cursor()
    cur.execute("""
        SELECT r.content_id, r.exception_reason, vcl.level_source, vcl.vocab_level
        FROM vocabulary_official_grade_reference r
        JOIN vocabulary_content_levels vcl ON vcl.content_id = r.content_id AND vcl.is_active=1
        WHERE r.official_grade='5' AND vcl.level_source LIKE 'MANUAL_LITERACY%'
    """)
    rows = cur.fetchall()
    print("49건 재현:", len(rows), "(기대 49)")

    def subbatch(cid: str) -> str:
        m = re.match(r"(SR_[A-Z0-9]+)_", cid)
        return m.group(1) if m else cid

    sb: dict[str, int] = {}
    lvl: dict[int, int] = {}
    homonym = 0
    for r in rows:
        sb[subbatch(r["content_id"])] = sb.get(subbatch(r["content_id"]), 0) + 1
        lvl[r["vocab_level"]] = lvl.get(r["vocab_level"], 0) + 1
        if r["exception_reason"] and "동형이의" in r["exception_reason"]:
            homonym += 1
    print("서브배치 분포:", sb)
    print("현재 vocab_level 분포:", lvl)
    print("동형이의어 위험:", homonym, "/ 49")

    cids = [r["content_id"] for r in rows]
    placeholders = ",".join("?" * len(cids))
    cur.execute(
        f"SELECT source_content_id FROM vocabulary_multiformat_items WHERE source_content_id IN ({placeholders}) GROUP BY source_content_id",
        cids,
    )
    linked = {r["source_content_id"] for r in cur.fetchall()}
    print("연결 문항 있는 content_id:", len(linked), "/ 49")


def job3(con: sqlite3.Connection) -> None:
    print("\n=== Job 3: DB 내 추가 후보(3a) + 진짜 공백 규모(3b) ===")
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM vocabulary_official_grade_reference WHERE official_grade='5'")
    total5 = cur.fetchone()[0]
    print("DB 전체 official_grade=5 매칭 건수(=227건 배치 49건과 동일해야 함):", total5)


def main() -> None:
    db_path = os.environ.get("VOCABULARY_QUIZ_DB_PATH")
    if not db_path:
        print("VOCABULARY_QUIZ_DB_PATH 환경변수가 필요합니다(읽기 전용 연결만 수행)")
        sys.exit(1)
    con = connect_ro(db_path)
    job1(con)
    job2(con)
    job3(con)
    con.close()


if __name__ == "__main__":
    main()
