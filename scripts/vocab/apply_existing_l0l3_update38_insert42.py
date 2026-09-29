# -*- coding: utf-8 -*-
"""38건 오답 텍스트 정정(UPDATE) + 42건 신규 비공개 적재(INSERT)를
단일 트랜잭션으로 적용한다. 예상 기존값이 정확히 일치하는 행만
수정하고, 하나라도 불일치/예상 밖이면 전체 중단(쓰기 자체를 시작하지
않음). 328건 HOLD 44건은 이번 적용 대상에 없음(별도 집합)."""
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime

EXPECTED_APP_ENV = "research"
DB_BASENAME = "vocabulary_quiz_research.db"
EXPECTED_SOURCE_VERSION = "schema_reading_existing_l0l3_dryrun_v1"
EXPECTED_UPDATE_COUNT = 38
EXPECTED_INSERT_COUNT = 42
EXPECTED_INSERT_CONTENT_COUNT = 21

# GATE 1: APP_ENV
app_env = None
with open(os.path.expanduser("~/aprolabs/.env"), encoding="utf-8") as f:
    for line in f:
        if line.startswith("APP_ENV="):
            app_env = line.strip().split("=", 1)[1]
            break
if app_env != EXPECTED_APP_ENV:
    print(f"GATE 1 FAIL: APP_ENV={app_env!r} - 중단")
    sys.exit(1)
print("GATE 1 PASS: APP_ENV=research")

DB_PATH = os.path.expanduser("~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db")
if os.path.basename(DB_PATH) != DB_BASENAME:
    print("GATE 2 FAIL: research DB 아님 - 중단")
    sys.exit(1)
print("GATE 2 PASS: DB 경로 확인:", DB_PATH)


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


pre_apply_hash = sha256_of(DB_PATH)
print("GATE 3: 적용 전 DB 파일 SHA-256:", pre_apply_hash)

# GATE 4: SQLite Backup API 백업 + 복원 가능성 검증
ts = datetime.now().strftime("%Y%m%d-%H%M%S")
backup_path = os.path.expanduser(f"~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak_l0l3_fix38_ins42_{ts}")
src = sqlite3.connect(DB_PATH)
dst = sqlite3.connect(backup_path)
with dst:
    src.backup(dst)
src.close()
dst.close()
backup_hash = sha256_of(backup_path)
print("GATE 4: 백업 파일 생성:", backup_path, "SHA-256:", backup_hash)

verify_con = sqlite3.connect(backup_path)
vcur = verify_con.cursor()
vcur.execute("PRAGMA integrity_check")
verify_integrity = vcur.fetchone()[0]
vcur.execute("SELECT COUNT(*) FROM vocabulary_contents")
verify_content_count = vcur.fetchone()[0]
vcur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
verify_item_count = vcur.fetchone()[0]
verify_con.close()
print(f"GATE 4: 백업 복원 가능성 검증 - integrity={verify_integrity}, "
      f"contents={verify_content_count}, items={verify_item_count}")
if verify_integrity != "ok":
    print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
    sys.exit(1)

with open(os.path.expanduser("~/aprolabs/data/import/existing_l0l3_update38_20260929.json"), encoding="utf-8") as f:
    update_rows = json.load(f)
with open(os.path.expanduser("~/aprolabs/data/import/existing_l0l3_insert42_20260929.json"), encoding="utf-8") as f:
    insert_rows = json.load(f)

if len(update_rows) != EXPECTED_UPDATE_COUNT:
    print(f"GATE 5 FAIL: UPDATE 대상 {len(update_rows)} != 기대 {EXPECTED_UPDATE_COUNT} - 중단")
    sys.exit(1)
if len(insert_rows) != EXPECTED_INSERT_COUNT:
    print(f"GATE 5 FAIL: INSERT 대상 {len(insert_rows)} != 기대 {EXPECTED_INSERT_COUNT} - 중단")
    sys.exit(1)
insert_cids = {r["source_content_id"] for r in insert_rows}
if len(insert_cids) != EXPECTED_INSERT_CONTENT_COUNT:
    print(f"GATE 5 FAIL: INSERT 콘텐츠 수 {len(insert_cids)} != 기대 {EXPECTED_INSERT_CONTENT_COUNT} - 중단")
    sys.exit(1)
if any(r["source_version"] != EXPECTED_SOURCE_VERSION for r in insert_rows):
    print("GATE 5 FAIL: INSERT source_version 불일치 - 중단")
    sys.exit(1)
print(f"GATE 5 PASS: UPDATE {len(update_rows)}건, INSERT {len(insert_rows)}건(콘텐츠 {len(insert_cids)}건)")

con = sqlite3.connect(DB_PATH)
con.execute("PRAGMA foreign_keys=ON")
cur = con.cursor()

# GATE 6: UPDATE 대상 - 현재 DB값이 예상 스냅샷과 정확히 일치하는지 재확인
mismatch = []
for r in update_rows:
    cur.execute("SELECT options_json, correct_option, explanation FROM vocabulary_multiformat_items WHERE item_id=?",
                (r["item_id"],))
    row = cur.fetchone()
    if row is None:
        mismatch.append((r["item_id"], "NOT_FOUND"))
        continue
    cur_options, cur_correct, cur_expl = row
    if (cur_options != r["expected_old_options_json"] or cur_correct != r["expected_old_correct_option"]
            or cur_expl != r["expected_old_explanation"]):
        mismatch.append((r["item_id"], "VALUE_MISMATCH"))
if mismatch:
    print(f"GATE 6 FAIL: 예상 기존값과 다른 행 {len(mismatch)}건 - 전체 중단(쓰기 없음): {mismatch}")
    con.close()
    sys.exit(1)
print(f"GATE 6 PASS: UPDATE 대상 {len(update_rows)}건 전부 예상 기존값과 정확히 일치")

# GATE 7: INSERT 대상 - item_id 전역 중복 없음, 연결 콘텐츠 활성/비공개/HOLD없음
ph = ",".join("?" for _ in insert_rows)
insert_iids = [r["item_id"] for r in insert_rows]
cur.execute(f"SELECT item_id FROM vocabulary_multiformat_items WHERE item_id IN ({ph})", insert_iids)
existing_conflict = {row[0] for row in cur.fetchall()}
if existing_conflict:
    print(f"GATE 7 FAIL: INSERT 대상 중 이미 존재하는 item_id {len(existing_conflict)}건 - 중단: {existing_conflict}")
    con.close()
    sys.exit(1)

ph2 = ",".join("?" for _ in insert_cids)
cur.execute(f"SELECT content_id, is_active, student_exposure, public_ready, hold_reason "
            f"FROM vocabulary_contents WHERE content_id IN ({ph2})", list(insert_cids))
found = {row[0]: row[1:] for row in cur.fetchall()}
missing_cids = insert_cids - set(found.keys())
if missing_cids:
    print(f"GATE 7 FAIL: 연결 콘텐츠 없음 {missing_cids} - 중단")
    con.close()
    sys.exit(1)
bad_cids = [c for c, (active, exp, pub, hold) in found.items()
            if active != 1 or exp != 0 or pub != 0 or (hold and hold.strip())]
if bad_cids:
    print(f"GATE 7 FAIL: 비활성/공개/HOLD 콘텐츠 {bad_cids} - 중단")
    con.close()
    sys.exit(1)
print(f"GATE 7 PASS: INSERT item_id 중복 없음, 콘텐츠 {len(insert_cids)}건 전부 활성·비공개·HOLD없음")

# 사전 스냅샷(무관 데이터 불변 확인용)
cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
pre_content_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
pre_item_total = cur.fetchone()[0]

# GATE 8: 단일 트랜잭션(UPDATE 38 + INSERT 42)
try:
    cur.execute("BEGIN")
    for r in update_rows:
        cur.execute(
            "UPDATE vocabulary_multiformat_items SET options_json=?, correct_option=?, explanation=?, "
            "qa_flags_json=? WHERE item_id=? AND options_json=? AND correct_option=? AND explanation=?",
            (r["new_options_json"], r["new_correct_option"], r["new_explanation"], r["new_qa_flags_json"],
             r["item_id"], r["expected_old_options_json"], r["expected_old_correct_option"],
             r["expected_old_explanation"]),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"UPDATE rowcount!=1 for {r['item_id']}(rowcount={cur.rowcount})")

    for r in insert_rows:
        options = json.loads(r["options_json"])
        public_payload_json = json.dumps({"options": options}, ensure_ascii=False)
        qa_flags_json = json.dumps({
            "flag": "EXISTING_L0L3_DRYRUN_INDEPENDENT_QA_PASS_V2",
            "generation": "expand_existing_l0l3_quiz_dryrun.py(2026-09-29, 연령적합성+오답길이균형 게이트 적용판)",
            "independent_verification": "verify_existing_l0l3_quiz_dryrun_independent.py(2026-09-29 개정판, 4개 선택지 전부 검사) - 184/184 PASS",
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
                "existing_l0l3_dryrun_v2_independent_qa_20260929", r["source_version"],
            ),
        )
    con.commit()
    print(f"GATE 8 PASS: 단일 트랜잭션 커밋 완료(UPDATE {len(update_rows)} + INSERT {len(insert_rows)})")
except Exception as e:
    con.rollback()
    print(f"GATE 8 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
    con.close()
    sys.exit(2)

integrity = cur.execute("PRAGMA integrity_check").fetchall()
fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
fk_ok = len(fk_check) == 0
print(f"GATE 9: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
print(f"GATE 9: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

cur.execute("SELECT COUNT(*) FROM vocabulary_contents")
post_content_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items")
post_item_total = cur.fetchone()[0]
print(f"GATE 10: 콘텐츠 총수 {pre_content_total} -> {post_content_total} ({'PASS(불변)' if pre_content_total == post_content_total else 'FAIL'})")
print(f"GATE 10: 문항 총수 {pre_item_total} -> {post_item_total} (증가분 {post_item_total - pre_item_total}, 기대 +{len(insert_rows)}, 절대값 기대 1553)")

# 이번에 건드린 21건 콘텐츠 공개 플래그 재확인
cur.execute(f"SELECT student_exposure, public_ready FROM vocabulary_contents WHERE content_id IN ({ph2})", list(insert_cids))
flags_after = cur.fetchall()
all_private = all(f == (0, 0) for f in flags_after)
print(f"GATE 11: 신규 연결 콘텐츠 21건 공개 플래그 여전히 0: {all_private}")

con.close()
print("\nUPDATED=", len(update_rows))
print("INSERTED=", len(insert_rows))
print("BACKUP=", backup_path)
print("PRE_APPLY_HASH=", pre_apply_hash)
