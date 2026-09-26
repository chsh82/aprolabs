"""이미지 재추출 v2(2026-09-26, 사용자 지시) - parser.py의 extract_images()가
"페이지당 가장 큰 이미지 1장만" 저장하던 제약을 풀고, 1단계(배경지식) 검사
범위를 여러 쪽으로 넓히고, STEP3(글쓰기) 이미지 추출 경로를 추가한다.

기존 momo_book.db/document_image, extracted_images/는 절대 건드리지 않는다
(사용자 지시 "기존 momo_book.db는 보존하고 별도 테이블/DB에 적재") - 결과는
전부 이 스크립트 옆의 새 SQLite(image_reextract_v2.db)와 새 이미지 폴더
(extracted_images/v2/)에만 쓴다. 되돌리려면 그 두 개만 지우면 된다.

추출 방식: PDF 임베드 이미지 직접 추출(PyMuPDF) - 비전/LLM 호출 없음,
비용 $0. 로고·배너처럼 여러 페이지에 반복 등장하는 이미지는 기존과 같이
장식용으로 보고 제외한다.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import fitz

from parser import find_stage_boundaries, load_pages

REPO_ROOT = Path(__file__).resolve().parent
PDF_DIR = REPO_ROOT / "vision_source_pdfs"
OUT_DB = REPO_ROOT / "image_reextract_v2.db"
OUT_IMG_DIR = REPO_ROOT / "extracted_images" / "v2"
MIN_SIDE = 150  # 장식용 아이콘 등 너무 작은 이미지는 애초에 후보에서 제외(관대한 하한)


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_image_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            image_type TEXT NOT NULL,
            source_page INTEGER,
            file_path TEXT NOT NULL,
            width INTEGER,
            height INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_v2_doc ON document_image_v2(doc_id)")
    conn.commit()


def _is_decorative(xref: int, xref_pages: dict) -> bool:
    return len(xref_pages.get(xref, ())) > 1


def _page_images(fitz_doc, page_idx: int, xref_pages: dict, seen_xrefs: set) -> list[tuple[int, dict]]:
    """이 페이지의 비장식·충분히 큰 이미지 전부(문서 전체 기준 xref 중복 제거)."""
    out = []
    for img in fitz_doc[page_idx].get_images(full=True):
        xref = img[0]
        if xref in seen_xrefs or _is_decorative(xref, xref_pages):
            continue
        seen_xrefs.add(xref)
        try:
            info = fitz_doc.extract_image(xref)
        except Exception:
            continue
        if info["width"] < MIN_SIDE or info["height"] < MIN_SIDE:
            continue
        out.append((xref, info))
    return out


def extract_images_v2(pdf_path: Path, stage1_start, stage2_start, stage3_start) -> list[dict]:
    fitz_doc = fitz.open(str(pdf_path))
    xref_pages: dict[int, set[int]] = {}
    for i in range(len(fitz_doc)):
        for img in fitz_doc[i].get_images():
            xref_pages.setdefault(img[0], set()).add(i)

    seen_xrefs: set[int] = set()
    images: list[dict] = []

    # 표지(1페이지) - 기존처럼 이 페이지 전용, 다만 여러 장이면 전부 저장(1장 제한 해제)
    for xref, info in _page_images(fitz_doc, 0, xref_pages, seen_xrefs):
        images.append({"image_type": "cover", "source_page": 1, "info": info})

    # 1단계(배경지식) - 기존엔 시작 페이지 1장만 검사했는데, stage2_start 전까지
    # 여러 쪽에 걸칠 수 있어 범위를 넓힘(사용자 지시 [2]).
    if stage1_start:
        end = (stage2_start or stage1_start + 1)
        for page_no in range(stage1_start, end):
            for xref, info in _page_images(fitz_doc, page_no - 1, xref_pages, seen_xrefs):
                images.append({"image_type": "background", "source_page": page_no, "info": info})

    # 2단계(토론) - 페이지당 1장 제한 해제(사용자 지시 [1]).
    if stage2_start and stage3_start:
        for page_no in range(stage2_start, stage3_start):
            for xref, info in _page_images(fitz_doc, page_no - 1, xref_pages, seen_xrefs):
                images.append({"image_type": "illustration", "source_page": page_no, "info": info})

    # 3단계(글쓰기) - 기존엔 아예 없던 경로(사용자 지시 [3], 두근두근 11쪽/
    # 젊은 예술가 22쪽 누락 사례).
    if stage3_start:
        for page_no in range(stage3_start, len(fitz_doc) + 1):
            for xref, info in _page_images(fitz_doc, page_no - 1, xref_pages, seen_xrefs):
                images.append({"image_type": "essay", "source_page": page_no, "info": info})

    fitz_doc.close()
    return images


def main() -> None:
    # momo_book.db는 항상 읽기 전용(사용자 지시, 프로젝트 표준) - URI로 강제.
    book = sqlite3.connect(f"file:{(REPO_ROOT / 'momo_book.db').as_posix()}?mode=ro", uri=True)
    book.row_factory = sqlite3.Row
    doc_ids = [r[0] for r in book.execute("SELECT doc_id FROM documents ORDER BY doc_id").fetchall()]

    if OUT_DB.exists():
        OUT_DB.unlink()  # 이번 실행 결과로 새로 만듦(기존 momo_book.db는 안 건드림)
    out_conn = sqlite3.connect(str(OUT_DB))
    init_db(out_conn)

    total_saved = 0
    errors: list[tuple[str, str]] = []
    per_doc_counts: dict[str, int] = {}

    for i, doc_id in enumerate(doc_ids):
        pdf_path = PDF_DIR / f"{doc_id}.pdf"
        if not pdf_path.exists():
            errors.append((doc_id, "PDF 없음"))
            continue
        try:
            pages = load_pages(str(pdf_path))
            stage1, stage2, stage3 = find_stage_boundaries(pages)
            images = extract_images_v2(pdf_path, stage1, stage2, stage3)
        except Exception as e:  # noqa: BLE001
            errors.append((doc_id, str(e)))
            continue

        doc_dir = OUT_IMG_DIR / doc_id
        doc_dir.mkdir(parents=True, exist_ok=True)
        type_counters: dict[str, int] = {}
        for img in images:
            t = img["image_type"]
            type_counters[t] = type_counters.get(t, 0) + 1
            n = type_counters[t]
            info = img["info"]
            ext = info["ext"]
            fname = f"{t}_p{img['source_page']}_{n}.{ext}"
            (doc_dir / fname).write_bytes(info["image"])
            rel_path = f"v2/{doc_id}/{fname}"
            out_conn.execute(
                "INSERT INTO document_image_v2 (doc_id, image_type, source_page, file_path, width, height) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (doc_id, t, img["source_page"], rel_path, info["width"], info["height"]),
            )
            total_saved += 1
        per_doc_counts[doc_id] = len(images)
        if (i + 1) % 50 == 0:
            print(f"진행 {i + 1}/{len(doc_ids)}")

    out_conn.commit()
    out_conn.close()
    book.close()

    print()
    print("=== 완료 ===")
    print("전체 문서:", len(doc_ids))
    print("에러:", len(errors))
    for e in errors[:20]:
        print("  ", e)
    print("새로 저장된 이미지 총합:", total_saved)


if __name__ == "__main__":
    main()
