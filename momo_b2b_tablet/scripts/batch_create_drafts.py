"""305건 전체 초안 생성(2026-09-28, 사용자 지시) - 문서 단위 체크포인트로
중단돼도 이어서 진행 가능. 이미 이 배치(created_by=BATCH_TAG)로 만들어진
doc_id는 건너뛴다(edition 테이블 자체가 체크포인트 - 별도 파일 없음).
전부 draft 상태로 남고(승인은 이 스크립트가 하지 않음), 실행 중 진행 상황은
DB에 바로 커밋되므로 다른 터미널에서도 아래로 실시간 확인 가능:

    sqlite3 edition/edition_store.db \
      "select count(*) from edition where created_by='BATCH_TAG'"

실행: python -m scripts.batch_create_drafts
"""
from __future__ import annotations

import sqlite3
import sys
import time

sys.path.insert(0, ".")

from edition import db
from edition.store import create_draft, NotFound

BATCH_TAG = "batch-305-20260928"
SOURCE_DB = "../momo_book_db/momo_book.db"


def already_done() -> set[str]:
    conn = db.get_connection()
    try:
        rows = conn.execute("SELECT doc_id FROM edition WHERE created_by = ?", (BATCH_TAG,)).fetchall()
        return {r["doc_id"] for r in rows}
    finally:
        conn.close()


def all_doc_ids() -> list[str]:
    conn = sqlite3.connect(f"file:{SOURCE_DB}?mode=ro", uri=True)
    try:
        return [r[0] for r in conn.execute("SELECT doc_id FROM documents ORDER BY doc_id")]
    finally:
        conn.close()


def main() -> int:
    doc_ids = all_doc_ids()
    done = already_done()
    todo = [d for d in doc_ids if d not in done]
    print(f"전체 {len(doc_ids)}건, 이미 완료 {len(done)}건, 남은 {len(todo)}건")

    ok, failed = 0, []
    t0 = time.time()
    for i, doc_id in enumerate(todo, 1):
        try:
            edition_id = create_draft(doc_id, created_by=BATCH_TAG)
            ok += 1
        except NotFound as e:
            failed.append((doc_id, str(e)))
        except Exception as e:  # noqa: BLE001 - 배치 중 한 건 실패해도 나머지는 계속
            failed.append((doc_id, f"{type(e).__name__}: {e}"))
        if i % 20 == 0 or i == len(todo):
            print(f"  {i}/{len(todo)} (누적 성공 {len(done) + ok}/{len(doc_ids)}, {time.time() - t0:.1f}s)")

    print(f"\n완료 - 이번 실행 성공 {ok}건, 실패 {len(failed)}건")
    if failed:
        print("실패 목록:")
        for doc_id, msg in failed:
            print(f"  {doc_id}: {msg}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
