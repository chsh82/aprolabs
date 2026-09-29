"""이미 만들어진 edition의 "앤와 생각 나누기" 오타를 "앤과"로 고침 - 2026-09-29.

layout/step1.py는 이제 처음부터 "앤과"로 낸다. 이 스크립트는 그 수정
전에 이미 draft로 만들어져 있던 edition들에 같은 걸 소급 적용한다.

실행: python scripts/fix_anne_gwa_typo.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from edition import store  # noqa: E402

OLD = "앤와 생각 나누기"
NEW = "앤과 생각 나누기"


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

    n = 0
    for row in rows:
        layout = json.loads(row["layout_json"])
        ops = []
        for pi, page in enumerate(layout.get("pages", [])):
            guide = page.get("guide")
            if guide and guide.get("nm") == OLD:
                ops.append({"op": "replace", "path": f"/pages/{pi}/guide/nm", "value": NEW})
        if not ops:
            continue
        n += 1
        if args.dry_run:
            print(f"[dry-run] {row['doc_id']} (edition {row['id']}): {len(ops)}개 페이지")
            continue
        store.patch_edition(
            row["id"], ops, editor="typo-fix-script",
            reason='검수: "앤와" -> "앤과" 맞춤법 수정',
        )
        print(f"{row['doc_id']} (edition {row['id']}): {len(ops)}개 페이지 수정")

    print(f"\n완료: {n}개 문서{' (dry-run - 실제 반영 안 됨)' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
