"""② 조판 초안 생성 - 정규화 레코드(NormalizedDoc) -> layout JSON(momo-edition/1).
SPEC §9 2단계, 사용자 지시(2026-09-23) "3단계: 조판 규칙 엔진".

결정론적 규칙(band/분기, ui_type->form, part 분할, 원본 이미지 우선, 캐릭터 매핑
등)은 layout/rules.py + step1~3.py에 있고, 여기서는 그걸 한 문서로 조립만 한다.
LLM 보조가 필요한 자리는 전부 Flag로 남기고 그 자리엔 안전한 자리표시자를 채운다
(빈 문자열/제네릭 문구) - 사람이 검수 단계에서 채우기 전까지 렌더러가 깨지지 않게.

실행:
    python -m layout.generate L5-Q3-W10
    python -m layout.generate L5-Q3-W10 --json > out.json
"""
from __future__ import annotations

from normalize.models import Flag, NormalizedDoc
from normalize.run import normalize_document
from normalize.tone import TONE_LINE_MM, TONE_LINES_OVERRIDE, level_to_grade

from .step1 import build_step1_pages
from .step2 import build_step2_pages
from .step3 import build_step3_pages


def _book_fields(doc: NormalizedDoc) -> tuple[dict, list[Flag]]:
    flags: list[Flag] = []
    title, subtitle = doc.book_title, None
    if ": " in doc.book_title:
        title, subtitle = doc.book_title.split(": ", 1)

    digits = "".join(ch for ch in doc.level if ch.isdigit())
    level_label = f"LV {digits}" if digits else doc.level

    cover_img = next((img.file_path for img in doc.images if img.image_type == "cover"), None)
    if not cover_img:
        flags.append(Flag(kind="missing", message="document_image에 cover 행이 없어 book.cover를 못 채움"))
    if not doc.cover_message:
        flags.append(Flag(kind="missing", message="documents.cover_message가 비어 있어 book.quote를 못 채움"))

    book = {
        "id": doc.doc_id, "title": title, "author": doc.book_author or "",
        "byline": doc.book_author or "", "level": level_label, "week": f"{doc.week}주차",
        "quote": doc.cover_message or "", "cover": cover_img or "",
    }
    if subtitle:
        book["subtitle"] = subtitle
    return book, flags


def generate_layout(doc_id: str) -> tuple[dict, list[Flag]]:
    doc = normalize_document(doc_id)
    flags: list[Flag] = list(doc.all_flags())  # ①단계 플래그를 그대로 승계(다시 계산 안 함)

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

    # 사용자 지시(2026-09-23) 4단계 2번: 문항을 미리 빼지 않고 전부 초안으로 만든다.
    # 대신 각 페이지에 included(기본 true)를 둬서, 검수에서 "이 문항 싣지 않음"을
    # JSON Patch로 false 처리할 수 있게 한다 - GET /api/runtime이 이 필드로 거른다.
    for p in pages:
        p.setdefault("included", True)

    layout = {
        "schema": "momo-edition/1", "doc_id": doc_id, "book": book, "tone": tone,
        "quarter": doc.quarter, "pages": pages,
    }
    return layout, flags


def main() -> int:
    import argparse
    import io
    import json
    import sys

    if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    parser = argparse.ArgumentParser(description="정규화 레코드 -> layout JSON(②단계 조판 초안)")
    parser.add_argument("doc_id")
    parser.add_argument("--json", action="store_true", help="layout JSON 전체를 출력")
    args = parser.parse_args()

    layout, flags = generate_layout(args.doc_id)

    if args.json:
        print(json.dumps(layout, ensure_ascii=False, indent=2))
        return 0

    print(f"=== {args.doc_id} ({layout['book']['title']}) band={layout['tone']['band']} "
          f"quarter={layout['quarter']} ===")
    print(f"페이지 {len(layout['pages'])}건:")
    for p in layout["pages"]:
        qid = p.get("q", {}).get("id", "")
        print(f"  {p['type']:8s} {qid}")
    print(f"\n플래그 {len(flags)}건:")
    for f in flags:
        loc = f"order_no={f.order_no} " if f.order_no is not None else ""
        print(f"  [{f.kind}] {loc}{f.message}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
