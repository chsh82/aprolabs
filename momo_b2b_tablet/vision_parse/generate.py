"""방식 B(비전) 초안 생성 - layout/generate.py와 같은 조립, 입력만 다르다
(momo_book.db 직접 조회 대신 build_vision_normalized_doc() 결과를 씀).

step1(vocab/ox/배경지식)·step3(essay)는 그대로 재사용한다(build_normalized.py
docstring 참고 - 이번 비전 파싱 대상이 아니었음). step2(discussion_qa)는
qa.form_override/ref_table을 넣어 두면 layout/step2.py가 자동으로 그걸 쓴다
(2026-09-24 [1]/[2] - layout/step2.py 자체를 수정해 두어 여기서 따로 분기할
필요가 없다).

실행:
    python -m vision_parse.generate L2-Q2-W08 --json > out.json
"""
from __future__ import annotations

import sys
from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parent.parent
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from normalize.models import Flag  # noqa: E402
from normalize.tone import TONE_LINE_MM, TONE_LINES_OVERRIDE, level_to_grade  # noqa: E402

from layout.generate import _book_fields  # noqa: E402
from layout.step1 import build_step1_pages  # noqa: E402
from layout.step2 import build_step2_pages  # noqa: E402
from layout.step3 import build_step3_pages  # noqa: E402

from vision_parse.build_normalized import build_vision_normalized_doc  # noqa: E402


def generate_vision_layout(doc_id: str) -> tuple[dict, list[Flag]]:
    doc = build_vision_normalized_doc(doc_id)
    flags: list[Flag] = list(doc.all_flags())

    book, book_flags = _book_fields(doc)
    flags.extend(book_flags)

    tone = {"band": doc.band, "grade": level_to_grade(doc.level), "line": TONE_LINE_MM[doc.band]}
    if doc.band in TONE_LINES_OVERRIDE:
        tone["lines"] = TONE_LINES_OVERRIDE[doc.band]

    pages: list[dict] = [{"type": "cover"}]

    step1_pages, step1_flags = build_step1_pages(doc)
    pages.extend(step1_pages)
    flags.extend(step1_flags)

    step2_pages, step2_flags = build_step2_pages(doc)
    pages.extend(step2_pages)
    flags.extend(step2_flags)

    step3_pages, step3_flags = build_step3_pages(doc)
    pages.extend(step3_pages)
    flags.extend(step3_flags)

    for p in pages:
        p.setdefault("included", True)

    layout = {
        "schema": "momo-edition/1", "doc_id": doc_id, "book": book, "tone": tone,
        "quarter": doc.quarter, "pages": pages, "source": "vision",
    }
    return layout, flags


def main() -> int:
    import argparse
    import io
    import json

    if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(description="vision_extract.db -> layout JSON(방식 B 초안)")
    parser.add_argument("doc_id")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    layout, flags = generate_vision_layout(args.doc_id)

    if args.json:
        print(json.dumps(layout, ensure_ascii=False, indent=2))
        return 0

    print(f"=== {args.doc_id} ({layout['book']['title']}) band={layout['tone']['band']} "
          f"quarter={layout['quarter']} ===")
    print(f"페이지 {len(layout['pages'])}건:")
    for p in layout["pages"]:
        qid = p.get("q", {}).get("id", "")
        form = p.get("q", {}).get("form", "")
        print(f"  {p['type']:8s} {qid:6s} form={form}")
    print(f"\n플래그 {len(flags)}건")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
