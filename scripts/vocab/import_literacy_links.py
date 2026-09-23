"""142건 링크 대상을 vocabulary_content_literacy_links에 적재하는 임포터.

입력: build_literacy_link_targets.py가 만든
`schema_reading_link_142_targets_20260924.json`(targets 배열, 142건).

동작:
- 각 target의 content_id가 vocabulary_contents에 실제로 존재하고
  is_active=1인지 확인(없으면 SKIPPED_CONTENT_NOT_FOUND로 표시하고 삽입하지
  않음 - 실패시키지 않고 목록만 남김).
- MULTIPLE_LINKS 4건(속수무책·혼비백산)이나 AMBIGUOUS_SENSE/UNVERIFIED
  4건이 입력 JSON에 섞여 있으면 안 되므로, 정확히 142건인지, 그리고
  vocab_content_id/literacy_term_id 각각 중복이 없는지 방어적으로 재확인
  한다(다르면 즉시 중단).
- UNIQUE(content_id, literacy_term_id) 제약을 이용해 INSERT OR IGNORE로
  삽입 - 재실행해도 이미 있는 행은 건드리지 않는다(멱등성).
- 기본은 dry-run(계획만 출력, 커밋 안 함). --apply로 실제 커밋.

하드 가드: --env-file에서 APP_ENV=research 직접 확인, --db-path basename이
'vocabulary_quiz_research.db'가 아니면 중단.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def read_app_env(env_file: Path) -> str | None:
    if not env_file.exists():
        return None
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("APP_ENV="):
            return line.split("=", 1)[1].strip()
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--targets-json", type=Path, required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    app_env = read_app_env(args.env_file)
    print(f"GATE 1: APP_ENV={app_env!r}")
    if app_env != "research":
        print("GATE 1 FAIL - 중단")
        return 1

    if args.db_path.name != "vocabulary_quiz_research.db":
        print("GATE 2 FAIL: research DB 아님 - 중단")
        return 1

    payload = json.loads(args.targets_json.read_text(encoding="utf-8"))
    targets = payload["targets"]
    print(f"입력 targets 건수: {len(targets)} (기대: 142)")
    if len(targets) != 142:
        print("FAIL: 142건이 아님 - 중단(제외 목록이 섞였을 가능성)")
        return 1

    vc_ids = [t["vocab_content_id"] for t in targets]
    lt_ids = [t["literacy_term_id"] for t in targets]
    if len(vc_ids) != len(set(vc_ids)) or len(lt_ids) != len(set(lt_ids)):
        print("FAIL: content_id/literacy_term_id 중복 발견 - 중단")
        return 1
    print("방어적 재확인 통과: 142건, 중복 0건")

    conn = sqlite3.connect(str(args.db_path))
    conn.execute("PRAGMA foreign_keys=ON")

    existing_content = {
        r[0] for r in conn.execute(
            "SELECT content_id FROM vocabulary_contents WHERE is_active=1"
        ).fetchall()
    }
    already_linked = {
        (r[0], r[1]) for r in conn.execute(
            "SELECT content_id, literacy_term_id FROM vocabulary_content_literacy_links"
        ).fetchall()
    }

    to_insert = []
    skipped_missing_content = []
    already_present = []
    for t in targets:
        cid, tid = t["vocab_content_id"], t["literacy_term_id"]
        if cid not in existing_content:
            skipped_missing_content.append((cid, tid, t["headword"]))
            continue
        if (cid, tid) in already_linked:
            already_present.append((cid, tid, t["headword"]))
            continue
        to_insert.append(t)

    print(f"\n삽입 예정: {len(to_insert)}건")
    print(f"content_id 존재하지 않아 스킵: {len(skipped_missing_content)}건")
    for row in skipped_missing_content:
        print("  SKIPPED_CONTENT_NOT_FOUND:", row)
    print(f"이미 존재(재실행 시 멱등): {len(already_present)}건")

    if not args.apply:
        print("\n--- DRY RUN: 아래 행들을 삽입할 예정 (커밋 안 함) ---")
        for t in to_insert[:10]:
            print(f"  {t['vocab_content_id']} -> literacy_term_id={t['literacy_term_id']} "
                  f"({t['headword']}, source={t['literacy_source']})")
        if len(to_insert) > 10:
            print(f"  ... 외 {len(to_insert) - 10}건")
        conn.close()
        print(f"\nDRY_RUN_INSERT_COUNT={len(to_insert)}")
        print(f"DRY_RUN_SKIPPED_MISSING_CONTENT={len(skipped_missing_content)}")
        print(f"DRY_RUN_ALREADY_PRESENT={len(already_present)}")
        return 0

    inserted = 0
    with conn:
        for t in to_insert:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO vocabulary_content_literacy_links
                    (content_id, literacy_term_id, literacy_source, literacy_headword,
                     link_status, link_method, evidence)
                VALUES (?, ?, ?, ?, 'CANDIDATE', ?, ?)
                """,
                (
                    t["vocab_content_id"], t["literacy_term_id"], t["literacy_source"],
                    t["literacy_headword"], t["link_method"], t["evidence"],
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
    conn.close()

    print(f"\nAPPLY 완료: 신규 삽입 {inserted}건, 스킵(이미 존재) {len(to_insert) - inserted}건, "
          f"content 없어 미시도 {len(skipped_missing_content)}건")
    print(f"APPLY_INSERTED={inserted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
