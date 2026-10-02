# -*- coding: utf-8 -*-
"""tier1 "기본 레벨 조정" 판정 8건을 실제로 적용한다 - vocabulary_content_
levels.vocab_level만 바꾼다(그 외 어떤 컬럼도, 어떤 다른 테이블도 쓰지
않는다). 기본은 항상 dry-run - 실제 적용은 --apply 플래그가 있어야만 한다.

**대상·목표 레벨을 전부 DB에서 "지금" 다시 읽는다** - 이전 턴에 만든 정적
CSV(`nikl_tier1_level_adjustment_8_application_plan_20261006.csv`)를 믿지
않고, `vocabulary_official_grade_judgments`에서 content_id별 **최신** 판정이
정확히 '기본 레벨 조정'인 행만, 그리고 `vocabulary_official_grade_reference.
proposed_base_level`이 'L0'~'L6' 형식으로 **명시적으로** 채워진 행만 대상으로
삼는다 - 목표가 비어있거나 형식이 이상하면 추정하지 않고 즉시 전체 중단한다.

안전장치(apply_official_grade_reference.py와 같은 GATE 패턴, 대상이
INSERT가 아니라 UPDATE라는 점만 다르다):
  GATE 1: APP_ENV=research만 허용 (db_path_guard.guard_db_path)
  GATE 2: DB 파일명 확인(연구 DB인지)
  GATE 3: 적용 전 DB SHA-256 기록
  GATE 4: SQLite Backup API 백업 + 복원 가능성(integrity_check) 검증
  GATE 5: 대상 8건을 "지금" DB에서 재질의(최신 판정='기본 레벨 조정' AND
          신선함(not stale) AND proposed_base_level이 L0~L6 형식) - 정확히
          8건이 아니면 중단, 목표 불명확 항목 있으면 중단(추정 금지)
  GATE 6: vocabulary_content_levels 전체 행 스냅샷(비대상 불변 확인용)
  GATE 7: 각 대상 행의 현재 vocab_level을 재확인해 3가지로 분류
          - 기대한 현재값(reference.current_vocab_level_snapshot)과 일치
            -> 적용 대상
          - 이미 목표값과 같음 -> 변경 없음(멱등 스킵)
          - 그 외(제3의 값) -> 충돌, 전체 중단(덮어쓰지 않음)
  GATE 8: 단일 트랜잭션 UPDATE, 실패 시 전체 롤백
  GATE 9: integrity_check/foreign_key_check
  GATE 10: 비대상 5,942건이 스냅샷과 바이트 단위로 완전히 동일한지 확인
  GATE 11: public_ready/student_exposure(8건 모두) 그대로 0인지 확인
  GATE 12: RULE_A/B(1,408, review_status='RULE_PROPOSED_PENDING_APPROVAL')·
           tier1 전체 판정 테이블 행 수 불변 확인(이 스크립트가 판정을
           쓰지 않았다는 증거)

실행(dry-run, 기본):
    VOCABULARY_QUIZ_DB_PATH=<db경로> python scripts/vocab/apply_tier1_level_adjustment_8.py

실제 적용:
    APP_ENV=research VOCABULARY_QUIZ_DB_PATH=<연구 DB경로> \\
        python scripts/vocab/apply_tier1_level_adjustment_8.py --apply

멱등성 검증: --apply로 1회 적용한 뒤 같은 커맨드를 다시 실행하면 GATE 7에서
8건 전부 "이미 목표값과 같음"으로 분류되어 UPDATE 0건, 오류 없이 정상
종료해야 한다.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.vocab.db_path_guard import DbPathGuardError, connect_rw, guard_db_path  # noqa: E402

LEVEL_VERSION = "level_policy_v0.1"
EXPECTED_TARGET_COUNT = 8
LEVEL_RE = re.compile(r"^L([0-6])$")


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def reference_version_snapshot(row: dict) -> str:
    """app/vocabulary_quiz/official_grade_review.py의 _reference_version_
    snapshot()과 완전히 동일한 해시 - 판정 신선도(stale) 판정을 서비스 코드와
    다른 방식으로 다시 구현하지 않는다."""
    import json
    blob = json.dumps({
        "official_grade": row["official_grade"],
        "proposed_base_level": row["proposed_base_level"],
        "match_type": row["match_type"],
        "source_file_sha256": row["source_file_sha256"],
        "computed_at": row["computed_at"],
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def find_targets(con: sqlite3.Connection) -> list[dict]:
    cur = con.cursor()
    cur.execute("""
        WITH latest AS (
            SELECT j.*, ROW_NUMBER() OVER (
                PARTITION BY content_id ORDER BY reviewed_at DESC, id DESC
            ) rn
            FROM vocabulary_official_grade_judgments j
        )
        SELECT l.content_id, l.judgment, l.rationale, l.reviewed_at,
               l.source_data_version_at_review,
               r.official_grade, r.proposed_base_level, r.match_type,
               r.source_file_sha256, r.computed_at,
               r.current_vocab_level_snapshot,
               c.lemma
        FROM latest l
        JOIN vocabulary_official_grade_reference r ON r.content_id = l.content_id
        JOIN vocabulary_contents c ON c.content_id = l.content_id
        WHERE l.rn = 1 AND l.judgment = '기본 레벨 조정'
        ORDER BY l.content_id
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    if len(rows) != EXPECTED_TARGET_COUNT:
        raise RuntimeError(
            f"GATE 5 FAIL: 최신 판정='기본 레벨 조정'인 행이 {len(rows)}건 - "
            f"기대 {EXPECTED_TARGET_COUNT}건과 다름. 추정해서 진행하지 않고 중단."
        )

    unclear = []
    stale = []
    for r in rows:
        m = LEVEL_RE.match(r["proposed_base_level"] or "")
        if not m:
            unclear.append((r["content_id"], r["lemma"], r["proposed_base_level"]))
            continue
        r["target_level"] = int(m.group(1))
        snap = reference_version_snapshot(r)
        if r["source_data_version_at_review"] != snap:
            stale.append((r["content_id"], r["lemma"]))

    if unclear:
        raise RuntimeError(
            f"GATE 5 FAIL: 목표 레벨이 명시적이지 않은 항목 발견(추정 금지) - {unclear}"
        )
    if stale:
        raise RuntimeError(
            f"GATE 5 FAIL: 판정 이후 참조 데이터가 재계산되어 만료된(stale) 항목 발견 - "
            f"다시 검토 필요: {stale}"
        )

    print(f"GATE 5 PASS: 대상 {len(rows)}건 전부 '기본 레벨 조정'·목표 레벨 명시적·신선함(비만료) 확인")
    for r in rows:
        print(f"  {r['lemma']}({r['content_id']}): 공식{r['official_grade']}등급, "
              f"현재스냅샷=L{r['current_vocab_level_snapshot']} -> 목표=L{r['target_level']}, "
              f"판정일={r['reviewed_at']}")
    return rows


def snapshot_all_levels(con: sqlite3.Connection) -> dict[str, tuple]:
    cur = con.cursor()
    cur.execute("""
        SELECT content_id, vocab_level, level_status, boundary_flag, level_source,
               level_version, target_grade_band, level_score, level_confidence,
               level_reason_json, is_active
        FROM vocabulary_content_levels WHERE level_version = ?
    """, (LEVEL_VERSION,))
    return {row[0]: row[1:] for row in cur.fetchall()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 적용(기본은 dry-run)")
    parser.add_argument("--database", default=None)
    args = parser.parse_args()

    try:
        db_path = guard_db_path(args.database)
    except DbPathGuardError as e:
        print(f"GATE 1 FAIL: {e}")
        sys.exit(1)
    print("GATE 1 PASS: 가드 통과(APP_ENV=research, VOCABULARY_QUIZ_DB_PATH 실존 확인)")

    import os
    if os.path.basename(db_path) != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: research DB 파일명이 아님 - 중단:", db_path)
        sys.exit(1)
    print("GATE 2 PASS:", "실제 적용 대상" if args.apply else "[DRY-RUN] 대상", "DB 경로:", db_path)

    pre_apply_hash = sha256_of(Path(db_path))
    print("GATE 3: 적용 전 DB 파일 SHA-256:", pre_apply_hash)

    con = connect_rw(db_path)
    con.execute("PRAGMA foreign_keys=ON")

    targets = find_targets(con)

    pre_levels_snapshot = snapshot_all_levels(con)
    print(f"GATE 6: vocabulary_content_levels 전체 스냅샷 {len(pre_levels_snapshot)}건 확보")

    cur = con.cursor()
    to_update, already_applied, conflicts = [], [], []
    for r in targets:
        cid = r["content_id"]
        row = pre_levels_snapshot.get(cid)
        if row is None:
            conflicts.append((cid, r["lemma"], "vocabulary_content_levels 행 없음"))
            continue
        current_level = row[0]
        target = r["target_level"]
        expected_current = r["current_vocab_level_snapshot"]
        if current_level == target:
            already_applied.append((cid, r["lemma"], current_level))
        elif current_level == expected_current:
            to_update.append((cid, r["lemma"], current_level, target))
        else:
            conflicts.append((cid, r["lemma"],
                               f"현재값={current_level}, 기대한 이전값={expected_current}, 목표={target} - 제3의 값"))

    print(f"GATE 7: 적용 대상 {len(to_update)}건, 이미 적용됨(멱등 스킵) {len(already_applied)}건, "
          f"충돌 {len(conflicts)}건")
    for cid, lemma, lvl in already_applied:
        print(f"  (스킵, 이미 L{lvl}) {lemma}({cid})")
    if conflicts:
        print("GATE 7 FAIL: 충돌 발견 - 전체 중단(덮어쓰지 않음)")
        for cid, lemma, detail in conflicts:
            print(f"  - {lemma}({cid}): {detail}")
        con.close()
        sys.exit(1)

    pre_counts = {
        "RULE_A/B(RULE_PROPOSED_PENDING_APPROVAL)": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_reference WHERE review_status='RULE_PROPOSED_PENDING_APPROVAL'"
        ).fetchone()[0],
        "tier1 판정 테이블 전체 행수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_judgments"
        ).fetchone()[0],
        "vocabulary_contents 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0],
        "vocabulary_multiformat_items 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0],
    }

    if not to_update:
        print("\n적용할 변경이 없습니다(전부 이미 적용됨 또는 대상 0건) - 멱등 종료.")
        con.close()
        return

    if not args.apply:
        print(f"\n[DRY-RUN] 실제 UPDATE 안 함. 적용 예정 {len(to_update)}건:")
        for cid, lemma, cur_lvl, tgt in to_update:
            print(f"  {lemma}({cid}): L{cur_lvl} -> L{tgt}")
        con.close()
        return

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{db_path}.bak_tier1_level_adj8_{ts}"
    dst = sqlite3.connect(backup_path)
    with dst:
        con.backup(dst)
    dst.close()
    backup_hash = sha256_of(Path(backup_path))
    print("GATE 4: 백업 생성:", backup_path, "SHA-256:", backup_hash)

    verify_con = sqlite3.connect(backup_path)
    vcur = verify_con.cursor()
    vintegrity = vcur.execute("PRAGMA integrity_check").fetchone()[0]
    vcontents = vcur.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0]
    verify_con.close()
    print(f"GATE 4: 백업 복원 가능성 검증 - integrity={vintegrity}, contents={vcontents}")
    if vintegrity != "ok":
        print("GATE 4 FAIL: 백업 무결성 실패 - 중단")
        con.close()
        sys.exit(1)

    try:
        cur.execute("BEGIN")
        for cid, lemma, cur_lvl, tgt in to_update:
            cur.execute(
                "UPDATE vocabulary_content_levels SET vocab_level = ?, updated_at = datetime('now') "
                "WHERE content_id = ? AND level_version = ?",
                (tgt, cid, LEVEL_VERSION),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"UPDATE가 정확히 1행을 바꾸지 않음(content_id={cid}, affected={cur.rowcount})")
        con.commit()
        print(f"GATE 8 PASS: 단일 트랜잭션 커밋 완료(UPDATE {len(to_update)}건)")
    except Exception as e:  # noqa: BLE001
        con.rollback()
        print(f"GATE 8 FAIL: 예외 발생, 트랜잭션 전체 롤백함: {e}")
        con.close()
        raise

    integrity = cur.execute("PRAGMA integrity_check").fetchall()
    fk_check = cur.execute("PRAGMA foreign_key_check").fetchall()
    integrity_ok = len(integrity) == 1 and integrity[0][0] == "ok"
    fk_ok = len(fk_check) == 0
    print(f"GATE 9: integrity_check={integrity[0][0] if integrity else integrity} ({'PASS' if integrity_ok else 'FAIL'})")
    print(f"GATE 9: foreign_key_check 위반={len(fk_check)}건 ({'PASS' if fk_ok else 'FAIL'})")

    post_levels_snapshot = snapshot_all_levels(con)
    updated_ids = {cid for cid, *_ in to_update}
    non_target_diff = []
    for cid, pre_row in pre_levels_snapshot.items():
        if cid in updated_ids:
            continue
        post_row = post_levels_snapshot.get(cid)
        if post_row != pre_row:
            non_target_diff.append((cid, pre_row, post_row))
    print(f"GATE 10: 비대상 {len(pre_levels_snapshot) - len(updated_ids)}건 불변 확인 - "
          f"{'PASS(전부 동일)' if not non_target_diff else f'FAIL({len(non_target_diff)}건 변경됨!)'}")
    if non_target_diff:
        for cid, pre, post in non_target_diff[:10]:
            print(f"  변경됨: {cid} {pre} -> {post}")

    target_check_ok = True
    for cid, lemma, cur_lvl, tgt in to_update:
        pre_row, post_row = pre_levels_snapshot[cid], post_levels_snapshot[cid]
        if post_row[0] != tgt or post_row[1:] != pre_row[1:]:
            target_check_ok = False
            print(f"  GATE 10 FAIL(대상): {lemma}({cid}) 예상 외 변경 - 이전={pre_row} 이후={post_row}")
    print(f"GATE 10: 대상 {len(to_update)}건은 vocab_level만 바뀌고 나머지 컬럼 불변 - "
          f"{'PASS' if target_check_ok else 'FAIL'}")

    pub_rows = cur.execute(
        f"SELECT content_id, public_ready, student_exposure FROM vocabulary_contents "
        f"WHERE content_id IN ({','.join('?' for _ in targets)})",
        [r["content_id"] for r in targets],
    ).fetchall()
    pub_ok = all(pr == 0 and se == 0 for _, pr, se in pub_rows)
    print(f"GATE 11: 대상 8건 public_ready/student_exposure 전부 0 유지 - {'PASS' if pub_ok else 'FAIL'} ({pub_rows})")

    post_counts = {
        "RULE_A/B(RULE_PROPOSED_PENDING_APPROVAL)": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_reference WHERE review_status='RULE_PROPOSED_PENDING_APPROVAL'"
        ).fetchone()[0],
        "tier1 판정 테이블 전체 행수": cur.execute(
            "SELECT COUNT(*) FROM vocabulary_official_grade_judgments"
        ).fetchone()[0],
        "vocabulary_contents 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_contents").fetchone()[0],
        "vocabulary_multiformat_items 총수": cur.execute("SELECT COUNT(*) FROM vocabulary_multiformat_items").fetchone()[0],
    }
    counts_ok = pre_counts == post_counts
    print(f"GATE 12: RULE_A/B·판정 이력·문항/콘텐츠 총수 불변 - {'PASS' if counts_ok else 'FAIL'}")
    for k in pre_counts:
        print(f"  {k}: {pre_counts[k]} -> {post_counts[k]}")

    con.close()
    if not (integrity_ok and fk_ok and not non_target_diff and target_check_ok and pub_ok and counts_ok):
        raise RuntimeError("사후 검증 실패 - 위 GATE 로그 확인 필요(백업=" + backup_path + ")")

    print("\nBACKUP=", backup_path)
    print("PRE_APPLY_HASH=", pre_apply_hash)
    print(f"\n적용 완료: {len(to_update)}건")
    for cid, lemma, cur_lvl, tgt in to_update:
        print(f"  {lemma}: L{cur_lvl} -> L{tgt}")


if __name__ == "__main__":
    main()
