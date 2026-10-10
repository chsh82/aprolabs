"""batch_regenerate_vision_drafts.py가 제외했던 8건을 마저 처리한다(2026-10-11,
사용자 지시). 개별 확인 결과(세션 기록 참고) 문항 내용은 8건 모두 방식 B가 같거나
낫다 - "A가 더 많다"는 집계는 A 쪽 결함(빈 페이지/번호 중복 분할/trivia 혼입)
때문이었다. 다만 어휘(vocab)는 2건(L3-Q1-W08, L4-Q3-W03)에서 방식 B가 실제로
퇴보했다 - 단어를 "(단어 미상)"으로 날리거나 뜻을 통째로 비웠는데, 방식 A는 두
건 다 단어+뜻이 멀쩡했다. 이 2건만 방식 B 레이아웃에 방식 A의 vocab 배열을
그대로 이식한다(나머지 페이지는 방식 B 그대로).

실행: python -m scripts.regenerate_remaining_8
"""
from __future__ import annotations

import sys

sys.path.insert(0, ".")

from edition.store import _insert_draft
from layout.generate import generate_layout
from vision_parse.generate import generate_vision_layout

BATCH_TAG = "batch-305-vision-20261011"

DOC_IDS = [
    "L1-Q4-W12", "L2-Q1-W06", "L2-Q2-W07", "L2-Q4-W06",
    "L3-Q1-W08", "L3-Q4-W02", "L4-Q3-W03", "L4-Q3-W04",
]
# 방식 B의 vocab이 방식 A보다 실제로 퇴보한 문서 - 그 페이지만 A의 vocab으로 교체.
_VOCAB_FROM_A = {"L3-Q1-W08", "L4-Q3-W03"}


def _vocab_page(layout: dict) -> dict | None:
    for p in layout.get("pages", []):
        if p.get("type") in ("vocab", "vocabMatch"):
            return p
    return None


def main() -> int:
    ok, failed = 0, []
    for doc_id in DOC_IDS:
        try:
            layout, flags = generate_vision_layout(doc_id)
            if doc_id in _VOCAB_FROM_A:
                layout_a, _ = generate_layout(doc_id)
                page_b = _vocab_page(layout)
                page_a = _vocab_page(layout_a)
                if page_b is None or page_a is None:
                    raise RuntimeError(f"vocab 페이지를 못 찾음(B={page_b is not None}, A={page_a is not None})")
                page_b["vocab"] = page_a["vocab"]
                flags = [f for f in flags if not (f.order_no is not None and f.kind == "sup"
                                                   and "뜻풀이" in f.message)]
            edition_id = _insert_draft(doc_id, layout, flags, created_by=BATCH_TAG)
            print(f"{doc_id}: edition {edition_id} 생성"
                  + (" (vocab은 방식 A로 보정)" if doc_id in _VOCAB_FROM_A else ""))
            ok += 1
        except Exception as e:  # noqa: BLE001
            failed.append((doc_id, f"{type(e).__name__}: {e}"))

    print(f"\n완료 - 성공 {ok}건, 실패 {len(failed)}건")
    for doc_id, msg in failed:
        print(f"  {doc_id}: {msg}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
