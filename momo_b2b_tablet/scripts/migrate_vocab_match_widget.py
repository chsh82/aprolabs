"""이미 만들어진 초1·2 edition의 vocab 페이지를 vocabMatch로 교체 - 2026-09-29.

layout/step1.py는 이제 band=lower(초1·2) 문서에서 처음부터 vocabMatch를
낸다. 이 스크립트는 그 변경 전에 이미 draft로 만들어져 있던 edition들(76개
문서)에 같은 걸 소급 적용한다 - patch_edition을 써서 correction_log에
기록을 남긴다.

실행: python scripts/migrate_vocab_match_widget.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from edition import store  # noqa: E402
from layout.step1 import _vocab_shuffle_order  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

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
        layout = json.loads(row["layout_json"])
        if layout.get("tone", {}).get("band") != "lower":
            continue
        ops = []
        for pi, page in enumerate(layout.get("pages", [])):
            if page.get("type") != "vocab":
                continue
            order = _vocab_shuffle_order(row["doc_id"], len(page.get("vocab", [])))
            ops.append({"op": "replace", "path": f"/pages/{pi}/type", "value": "vocabMatch"})
            ops.append({"op": "add", "path": f"/pages/{pi}/order", "value": order})
        if not ops:
            continue
        n_docs += 1
        if args.dry_run:
            print(f"[dry-run] {row['doc_id']} (edition {row['id']}): {len(ops)}개 op")
            continue
        store.patch_edition(
            row["id"], ops, editor="vocab-match-migration-script",
            reason="초1·2 낱말 익히기를 문장 만들기 카드에서 뜻 잇기(vocabMatch)로 교체 - 사용자 지시 2026-09-29",
        )
        print(f"{row['doc_id']} (edition {row['id']}): 패치 완료")

    print(f"\n완료: {n_docs}개 문서{' (dry-run - 실제 반영 안 됨)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
