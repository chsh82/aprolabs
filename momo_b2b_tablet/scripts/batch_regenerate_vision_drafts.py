"""305건 재생성(2026-10-11, 사용자 지시) - batch_create_drafts.py(9/28)가 방식
A(create_draft, momo_book_db 기반)로 305건을 만들었는데, 전 문서에 이미 더 정확한
방식 B(vision) 데이터가 있었다는 게 뒤늦게 확인됐다(scripts/compare_batch_a_vs_b.py
집계 - 평균 26.9% 문항 누락, vocab 40.6% word/definition 뒤바뀜). API 재호출 없이
vision_extract.db에 이미 있는 데이터로 새 버전을 만든다.

제외 대상(_EXCLUDED_DOC_IDS, 8건) - compare_batch_a_vs_b.py에서 방식 A가 방식 B보다
문항이 더 많게 나온 문서. 방식 B가 다른 지표(vocab 등)는 여전히 더 낫지만, 문항 수
자체가 역전되는 경우라 자동 일괄 교체 대상에서 빼고 개별 확인 후 처리한다(사용자
지시 2026-10-11).

문서 단위 체크포인트(batch_create_drafts.py와 동일 패턴) - 이미 이 배치
(created_by=BATCH_TAG)로 만들어진 doc_id는 건너뛴다.

실행: python -m scripts.batch_regenerate_vision_drafts
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from edition import db
from edition.store import _insert_draft
from vision_parse.generate import generate_vision_layout

BATCH_TAG = "batch-305-vision-20261011"
SOURCE_BATCH_TAG = "batch-305-20260928"

# compare_batch_a_vs_b.py 집계에서 방식 A가 방식 B보다 문항이 더 많았던 8건 -
# 방식 B의 다른 이득(vocab 등)과 별개로 문항 수 역전은 개별 확인이 먼저다.
_EXCLUDED_DOC_IDS = {
    "L1-Q4-W12", "L2-Q1-W06", "L2-Q2-W07", "L2-Q4-W06",
    "L3-Q1-W08", "L3-Q4-W02", "L4-Q3-W03", "L4-Q3-W04",
}


def already_done() -> set[str]:
    conn = db.get_connection()
    try:
        rows = conn.execute("SELECT doc_id FROM edition WHERE created_by = ?", (BATCH_TAG,)).fetchall()
        return {r["doc_id"] for r in rows}
    finally:
        conn.close()


def source_doc_ids() -> list[str]:
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT doc_id FROM edition WHERE created_by = ? ORDER BY doc_id", (SOURCE_BATCH_TAG,)
        ).fetchall()
        return [r["doc_id"] for r in rows]
    finally:
        conn.close()


def main() -> int:
    doc_ids = [d for d in source_doc_ids() if d not in _EXCLUDED_DOC_IDS]
    done = already_done()
    todo = [d for d in doc_ids if d not in done]
    print(f"대상 {len(doc_ids)}건(제외 {len(_EXCLUDED_DOC_IDS)}건), "
          f"이미 완료 {len(done)}건, 남은 {len(todo)}건")

    ok, failed = 0, []
    t0 = time.time()
    for i, doc_id in enumerate(todo, 1):
        try:
            layout, flags = generate_vision_layout(doc_id)
            _insert_draft(doc_id, layout, flags, created_by=BATCH_TAG)
            ok += 1
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
