"""이미 만들어진 edition에 vocabFill(보기에서 골라 쓰세요) 페이지 추가 - 2026-09-29.

layout/step1.py는 이제 vocab_fill 테이블이 있는 문서에서 처음부터 이
페이지를 낱말 익히기 바로 뒤에 낸다. 이 스크립트는 그 변경 전에 이미
draft로 만들어져 있던 edition들(vocab_fill 테이블에 있는 59개 문서)에
같은 걸 소급 적용한다.

실행: python scripts/migrate_vocab_fill_page.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from edition import store  # noqa: E402

MOMO_BOOK_DB = ROOT.parent / "momo_book_db" / "momo_book.db"


def _build_page(vocab_page: dict, bank: list, items: list) -> dict:
    return {
        "type": "vocabFill", "step": "STEP 1", "title": "낱말 익히기",
        "guide": vocab_page["guide"],
        "bank": bank,
        "items": items,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    book_conn = sqlite3.connect(MOMO_BOOK_DB)
    book_conn.row_factory = sqlite3.Row
    fill_rows = book_conn.execute("SELECT doc_id, bank_json, items_json FROM vocab_fill").fetchall()
    fill_by_doc = {r["doc_id"]: (json.loads(r["bank_json"]), json.loads(r["items_json"])) for r in fill_rows}
    book_conn.close()

    conn = store.db.get_connection()
    try:
        latest_ids = [r[0] for r in conn.execute("SELECT MAX(id) FROM edition GROUP BY doc_id").fetchall()]
        rows = conn.execute(
            f"SELECT id, doc_id, layout_json FROM edition WHERE id IN ({','.join(map(str, latest_ids))})"
        ).fetchall()
    finally:
        conn.close()

    n_docs = 0
    for row in rows:
        bank_items = fill_by_doc.get(row["doc_id"])
        if bank_items is None:
            continue
        bank, items = bank_items
        layout = json.loads(row["layout_json"])
        pages = layout.get("pages", [])
        vocab_idx = next((i for i, p in enumerate(pages) if p.get("type") in ("vocab", "vocabMatch")), None)
        if vocab_idx is None:
            print(f"[건너뜀] {row['doc_id']}: vocab/vocabMatch 페이지를 못 찾음")
            continue
        if any(p.get("type") == "vocabFill" for p in pages):
            print(f"[건너뜀] {row['doc_id']}: 이미 vocabFill 있음")
            continue

        new_page = _build_page(pages[vocab_idx], bank, items)
        insert_at = vocab_idx + 1
        n_docs += 1
        if args.dry_run:
            print(f"[dry-run] {row['doc_id']} (edition {row['id']}): pages[{insert_at}]에 vocabFill 삽입 예정")
            continue

        target_id = store.patch_edition(
            row["id"], [{"op": "add", "path": f"/pages/{insert_at}", "value": new_page}],
            editor="vocab-fill-migration-script",
            reason='"보기에서 골라 쓰세요" 원본 문항 추가 - 사용자 지시 2026-09-29',
        )
        conn2 = store.db.get_connection()
        try:
            conn2.execute(
                "INSERT INTO edition_flag (edition_id, page_idx, path, kind, message, priority) "
                "VALUES (?,?,?,?,?,0)",
                (target_id, insert_at, f"/pages/{insert_at}", "derived",
                 '"보기에서 골라 쓰세요" - 원본 PDF 학생용판의 숨은 정답 텍스트 레이어를 '
                 "자동 추출한 내용이라 문장이 깨졌거나 정답이 어긋날 수 있음(원본 대조 검수 필요)"),
            )
            conn2.commit()
        finally:
            conn2.close()
        print(f"{row['doc_id']} (edition {target_id}): pages[{insert_at}]에 vocabFill 삽입 완료")

    print(f"\n완료: {n_docs}개 문서{' (dry-run - 실제 반영 안 됨)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
