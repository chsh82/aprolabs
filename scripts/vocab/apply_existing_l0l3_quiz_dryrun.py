# -*- coding: utf-8 -*-
"""독립 검증(PASS) 142건만 aprolabs 연구 DB에 비공개로 적재한다.
게이트를 전부 통과해야만 커밋하고, 하나라도 실패하면 트랜잭션을
시작하지 않거나 즉시 롤백한다."""
import json
import os
import sqlite3
import sys
from datetime import datetime

EXPECTED_APP_ENV = "research"
DB_BASENAME = "vocabulary_quiz_research.db"
EXPECTED_SOURCE_VERSION = "schema_reading_existing_l0l3_dryrun_v1"
EXPECTED_ITEM_COUNT = 142
EXPECTED_CONTENT_COUNT = 71

# GATE 1: APP_ENV
app_env = None
with open(os.path.expanduser("~/aprolabs/.env"), encoding="utf-8") as f:
    for line in f:
        if line.startswith("APP_ENV="):
            app_env = line.strip().split("=", 1)[1]
            break
if app_env != EXPECTED_APP_ENV:
    print(f"GATE 1 FAIL: APP_ENV={app_env!r} (기대 {EXPECTED_APP_ENV!r}) - 중단")
    sys.exit(1)
print("GATE 1 PASS: APP_ENV=research 확인")

DB_PATH = os.path.expanduser("~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db")
if os.path.basename(DB_PATH) != DB_BASENAME:
    print("GATE 2 FAIL: research DB가 아님 - 중단")
    sys.exit(1)
print("GATE 2 PASS: 대상 DB 파일명 확인")

with open(os.path.expanduser("~/aprolabs/data/import/existing_l0l3_pass142_items_20260929.json"), encoding="utf-8") as f:
    rows = json.load(f)

if len(rows) != EXPECTED_ITEM_COUNT:
    print(f"GATE 3 FAIL: 입력 건수 {len(rows)} != 기대 {EXPECTED_ITEM_COUNT} - 중단")
    sys.exit(1)
if any(r["source_version"] != EXPECTED_SOURCE_VERSION for r in rows):
    print("GATE 3 FAIL: source_version 불일치 행 있음 - 중단")
    sys.exit(1)
item_ids = [r["item_id"] for r in rows]
if len(set(item_ids)) != len(item_ids):
    print("GATE 3 FAIL: 입력 자체에 item_id 중복 - 중단")
    sys.exit(1)
content_ids = sorted({r["source_content_id"] for r in rows})
if len(content_ids) != EXPECTED_CONTENT_COUNT:
    print(f"GATE 3 FAIL: 콘텐츠 수 {len(content_ids)} != 기대 {EXPECTED_CONTENT_COUNT} - 중단")
    sys.exit(1)
print(f"GATE 3 PASS: 입력 {len(rows)}건(콘텐츠 {len(content_ids)}건), source_version 일치, 중복 없음")

# SQLite Backup API 백업
ts = datetime.now().strftime("%Y%m%d-%H%M%S")
backup_path = os.path.expanduser(f"~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak_l0l3_dryrun_apply_{ts}")
src = sqlite3.connect(DB_PATH)
dst = sqlite3.connect(backup_path)
with dst:
    src.backup(dst)
src.close()
dst.close()
print("BACKUP_OK:", backup_path)

con = sqlite3.connect(DB_PATH)
con.execute("PRAGMA foreign_keys=ON")
cur = con.cursor()

# 사전 스냅샷(기존 데이터 불변 확인용)
cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
pre_content_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
pre_item_total = cur.fetchone()[0]

placeholders = ",".join("?" for _ in content_ids)
cur.execute(f"SELECT content_id, is_active, student_exposure, public_ready, hold_reason FROM vocabulary_contents WHERE content_id IN ({placeholders})", content_ids)
found = {row[0]: row[1:] for row in cur.fetchall()}
missing = [c for c in content_ids if c not in found]
if missing:
    print(f"GATE 4 FAIL: vocabulary_contents에 없는 content_id {len(missing)}건 - 중단")
    con.close()
    sys.exit(1)
bad = [c for c, (active, exp, pub, hold) in found.items() if active != 1 or exp != 0 or pub != 0 or (hold and hold.strip())]
if bad:
    print(f"GATE 4 FAIL: is_active!=1 이거나 student_exposure/public_ready!=0이거나 HOLD인 content_id: {bad} - 중단")
    con.close()
    sys.exit(1)
print(f"GATE 4 PASS: 콘텐츠 {len(content_ids)}건 전부 존재, is_active=1, student_exposure=0, public_ready=0, HOLD 없음")

# 멱등성 사전 점검
item_ph = ",".join("?" for _ in item_ids)
cur.execute(f"SELECT item_id FROM vocabulary_multiformat_items WHERE item_id IN ({item_ph})", item_ids)
existing = {r[0] for r in cur.fetchall()}
to_insert = [r for r in rows if r["item_id"] not in existing]
to_skip = [r for r in rows if r["item_id"] in existing]
print(f"GATE 5: 이미 존재(스킵) {len(to_skip)}건, 신규 삽입 대상 {len(to_insert)}건")

if not to_insert:
    print("신규 삽입 대상 0건 - 멱등 재실행으로 판단, 트랜잭션 없이 종료")
    con.close()
    print("INSERTED=0")
    sys.exit(0)

try:
    cur.execute("BEGIN")
    for r in to_insert:
        options = json.loads(r["options_json"])
        public_payload_json = json.dumps({"options": options}, ensure_ascii=False)
        qa_flags_json = json.dumps({
            "flag": "EXISTING_L0L3_DRYRUN_INDEPENDENT_QA_PASS",
            "generation": "expand_existing_l0l3_quiz_dryrun.py(2026-09-29)",
            "independent_verification": "independent_qa_184.py(2026-09-29) - 생성 스크립트와 별도 구현, 연령적합성/정답유일성/오답중복/문맥자연스러움/조사표기 재검사 PASS",
            "wrong_option_reasons": json.loads(r.get("wrong_option_reasons_json", "[]")),
        }, ensure_ascii=False)
        cur.execute(
            """INSERT INTO vocabulary_multiformat_items
               (item_id, item_type, source_content_id, source_content_ids_json, sense_id, sense_ids_json,
                lemma, pos, prompt, options_json, correct_option, public_payload_json, answer_payload_json,
                explanation, cognitive_level, qa_flags_json, generator_version, source_version, is_active)
               VALUES (?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, 1)""",
            (
                r["item_id"], r["item_type"], r["source_content_id"], r["lemma"], r["pos"], r["prompt"],
                r["options_json"], r["correct_option"], public_payload_json, r["answer_payload_json"],
                r["explanation"], qa_flags_json,
                "existing_l0l3_dryrun_v1_independent_qa_20260929", r["source_version"],
            ),
        )
    con.commit()
    print(f"GATE 6 PASS: 단일 트랜잭션 커밋 완료, 삽입 {len(to_insert)}건")
except Exception as e:
    con.rollback()
    print(f"GATE 6 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
    con.close()
    sys.exit(2)

integrity = cur.execute("PRAGMA integrity_check").fetchall()
fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
fk_ok = len(fk_check) == 0
print(f"GATE 7: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
print(f"GATE 7: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
post_content_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
post_item_total = cur.fetchone()[0]
print(f"GATE 8: 콘텐츠 총수 불변 {pre_content_total} -> {post_content_total} ({'PASS' if pre_content_total == post_content_total else 'FAIL'})")
print(f"GATE 8: 문항 총수 {pre_item_total} -> {post_item_total} (증가분 {post_item_total - pre_item_total}, 기대 {len(to_insert)})")

# 방금 넣은 것들의 공개 플래그 재확인(콘텐츠 쪽 - 문항 자체엔 공개 플래그가 없음)
cur.execute(f"SELECT student_exposure, public_ready FROM vocabulary_contents WHERE content_id IN ({placeholders})", content_ids)
flags_after = cur.fetchall()
all_private = all(f == (0, 0) for f in flags_after)
print(f"GATE 9: 관련 콘텐츠 71건 student_exposure/public_ready 전부 0 유지: {all_private}")

new_ids = [r["item_id"] for r in to_insert]
ph2 = ",".join("?" for _ in new_ids)
cur.execute(f"SELECT item_type, COUNT(*) FROM vocabulary_multiformat_items WHERE item_id IN ({ph2}) GROUP BY item_type", new_ids)
type_counts = dict(cur.fetchall())
print(f"신규 유형별 건수: {type_counts}")

con.close()
print("\nINSERTED=", len(to_insert))
print("SKIPPED=", len(to_skip))
print("BACKUP=", backup_path)
