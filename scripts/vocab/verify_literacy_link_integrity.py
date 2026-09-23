"""교차 DB 참조 무결성 검증 (읽기 전용, 로컬에서 실행).

vocabulary_content_literacy_links는 research 서버 DB에 있고, literacy_term_id가
가리키는 literacy.db terms는 로컬 파일이다 - 두 SQLite가 별도 파일이라 SQL FK를
걸 수 없으므로, 서버에서 SELECT 전용으로 내보낸 링크 테이블 스냅샷(JSON)을
입력으로 받아 로컬 literacy.db(mode=ro)와 대조하는 애플리케이션 레벨 검증을
수행한다(3단계 보고서 4-2절 제안을 그대로 구현).

검사 항목:
1. dangling reference: literacy_term_id가 literacy.db terms.id에 실제로 존재하는가.
2. drift: 연결 시점 스냅샷(literacy_source, literacy_headword)이 현재 literacy.db
   값과 여전히 같은가(다르면 경고만, 실패 아님 - literacy.db가 이 세션 동안
   바뀌지 않았으므로 원칙적으로 drift가 없어야 정상).

사용:
    python scripts/vocab/verify_literacy_link_integrity.py \\
        --links-json <서버에서 내려받은 링크 테이블 스냅샷 JSON> \\
        --literacy-db data/literacy.db
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import io
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--links-json", type=Path, required=True)
    ap.add_argument("--literacy-db", type=Path, required=True)
    args = ap.parse_args()

    links = json.loads(args.links_json.read_text(encoding="utf-8"))
    print(f"검증 대상 링크 행수: {len(links)}")

    uri = f"file:{args.literacy_db.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    conn.row_factory = sqlite3.Row

    term_ids = [int(r["literacy_term_id"]) for r in links]
    placeholders = ",".join("?" * len(term_ids)) if term_ids else "NULL"
    rows = conn.execute(
        f"SELECT id, headword, source FROM terms WHERE id IN ({placeholders})", term_ids
    ).fetchall() if term_ids else []
    by_id = {row["id"]: dict(row) for row in rows}
    conn.close()

    dangling = []
    drifted = []
    ok = 0
    for r in links:
        tid = int(r["literacy_term_id"])
        if tid not in by_id:
            dangling.append(r)
            continue
        current = by_id[tid]
        if current["headword"] != r["literacy_headword"] or current["source"] != r["literacy_source"]:
            drifted.append({"row": r, "current": current})
            continue
        ok += 1

    print(f"OK(스냅샷과 현재 literacy.db 일치): {ok}")
    print(f"DANGLING(literacy.db에 존재하지 않음): {len(dangling)}")
    for d in dangling:
        print("  DANGLING:", d)
    print(f"DRIFTED(headword/source가 스냅샷과 다름): {len(drifted)}")
    for d in drifted:
        print("  DRIFTED:", d)

    all_ok = not dangling and not drifted
    print(f"\n최종: {'PASS' if all_ok else 'FAIL'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
