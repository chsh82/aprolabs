"""305건 배치(batch-305-20260928)가 방식 A로 만들어졌는데, 방식 B(vision) 데이터가
이미 다 있다는 게 확인되어 - 전체 문서에 대해 방식 A(현재 edition) vs 방식 B(vision
재생성, API 호출 없음)의 차이를 집계한다. DB는 전혀 쓰지 않음(읽기 전용 비교).

실행: python -m scripts._compare_batch_a_vs_b > /tmp/compare_a_vs_b.json
"""
from __future__ import annotations

import json
import sqlite3
import sys
import traceback

sys.path.insert(0, ".")

from layout.generate import generate_layout  # noqa: E402
from vision_parse.generate import generate_vision_layout  # noqa: E402


def _q_page_count(layout: dict) -> int:
    return sum(1 for p in layout.get("pages", []) if p.get("q"))


def _vocab_empty_def_count(layout: dict) -> tuple[int, int]:
    total = empty = 0
    for p in layout.get("pages", []):
        if p.get("type") in ("vocab", "vocabMatch"):
            for v in p.get("vocab", []):
                total += 1
                if not (v.get("d") or "").strip():
                    empty += 1
    return total, empty


def main() -> int:
    conn = sqlite3.connect("edition/edition_store.db")
    conn.text_factory = str
    doc_ids = [r[0] for r in conn.execute(
        "SELECT doc_id FROM edition WHERE created_by='batch-305-20260928' ORDER BY doc_id"
    )]
    conn.close()

    results = []
    errors = []
    for doc_id in doc_ids:
        row = {"doc_id": doc_id}
        try:
            layout_a, _ = generate_layout(doc_id)
            row["a_q_pages"] = _q_page_count(layout_a)
            row["a_pages"] = len(layout_a.get("pages", []))
            v_total, v_empty = _vocab_empty_def_count(layout_a)
            row["a_vocab_total"] = v_total
            row["a_vocab_empty_def"] = v_empty
        except Exception as e:
            errors.append({"doc_id": doc_id, "side": "A", "error": f"{type(e).__name__}: {e}"})
            continue
        try:
            layout_b, _ = generate_vision_layout(doc_id)
            row["b_q_pages"] = _q_page_count(layout_b)
            row["b_pages"] = len(layout_b.get("pages", []))
            v_total_b, v_empty_b = _vocab_empty_def_count(layout_b)
            row["b_vocab_total"] = v_total_b
            row["b_vocab_empty_def"] = v_empty_b
        except Exception as e:
            errors.append({"doc_id": doc_id, "side": "B", "error": f"{type(e).__name__}: {e}"})
            continue
        row["q_diff"] = row["b_q_pages"] - row["a_q_pages"]
        row["q_missing_ratio"] = (
            round(row["q_diff"] / row["b_q_pages"], 3) if row["b_q_pages"] else None
        )
        results.append(row)

    print(json.dumps({"results": results, "errors": errors, "total_docs": len(doc_ids)},
                      ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
