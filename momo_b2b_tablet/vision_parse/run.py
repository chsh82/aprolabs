"""방식 B(비전 파싱) 실행기 - 문서 단위 체크포인트, 실패해도 나머지는 계속.

momo_book_db/vision_source_pdfs/{doc_id}.pdf(사용자 지시 2026-09-24 1번 - 외장
드라이브 의존 제거용 로컬 복사본, _manifest.json에 해시 기록됨)를 읽어 페이지별로
비전 추출하고 vision_extract.db(momo_book.db와 완전히 분리)에 저장한다.

사용법:
    python -m vision_parse.run --doc-ids L5-Q4-W02,L9-Q3-W07
    python -m vision_parse.run --all                    # 305건 전체(체크포인트 재개 가능)
    python -m vision_parse.run --sample 20 --seed 7      # 무작위 표본 20건
    python -m vision_parse.run --all --force             # 이미 완료된 문서도 다시 실행
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PKG_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, PKG_ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import fitz  # noqa: E402

from vision_parse import db as vdb  # noqa: E402
from vision_parse.extract import extract_page_image, MODEL, PROMPT_VERSION  # noqa: E402

SOURCE_PDF_DIR = REPO_ROOT / "momo_book_db" / "vision_source_pdfs"
MANIFEST_PATH = SOURCE_PDF_DIR / "_manifest.json"
MOMO_DB_PATH = REPO_ROOT / "momo_book_db" / "momo_book.db"
RENDER_DPI = 150


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_manifest() -> dict[str, dict]:
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        rows = json.load(f)
    return {r["doc_id"]: r for r in rows}


def _all_doc_ids() -> list[str]:
    conn = sqlite3.connect(f"file:{MOMO_DB_PATH.as_posix()}?mode=ro", uri=True)
    try:
        return [r[0] for r in conn.execute("SELECT doc_id FROM documents ORDER BY doc_id")]
    finally:
        conn.close()


def _insert_page(conn: sqlite3.Connection, doc_id: str, page_no: int, pdf_hash: str, result: dict) -> int:
    parsed = result["parsed"]
    parse_error = parsed.get("parse_error") if isinstance(parsed, dict) else None
    cur = conn.execute(
        "INSERT INTO vision_page (doc_id, page_no, source_pdf_sha256, model, prompt_version, "
        "elapsed_sec, input_tokens, output_tokens, raw_response_json, parse_error, extracted_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (doc_id, page_no, pdf_hash, MODEL, PROMPT_VERSION, result["elapsed_sec"],
         result["usage"]["input_tokens"], result["usage"]["output_tokens"],
         result["raw_text"], parse_error, _now()),
    )
    page_id = cur.lastrowid

    items = parsed.get("page_items", []) if isinstance(parsed, dict) else []
    for idx, item in enumerate(items):
        lh = item.get("layout_hint") or {}
        conn.execute(
            "INSERT INTO vision_item (page_id, doc_id, page_no, item_index, item_type, reading_type, "
            "excerpt_text, question_text, blanks_json, choices_json, table_json, page_number, "
            "layout_shape, blank_lines, cell_size_hint, layout_note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (page_id, doc_id, page_no, idx, item.get("item_type"), item.get("reading_type"),
             item.get("excerpt_text"), item.get("question_text"),
             json.dumps(item.get("blanks") or [], ensure_ascii=False),
             json.dumps(item.get("choices") or [], ensure_ascii=False),
             json.dumps(item.get("table"), ensure_ascii=False) if item.get("table") is not None else None,
             item.get("page_number"), lh.get("shape"), lh.get("blank_lines"),
             lh.get("cell_size_hint"), lh.get("note")),
        )
    return page_id


def run_document(doc_id: str, pdf_hash: str) -> dict:
    pdf_path = SOURCE_PDF_DIR / f"{doc_id}.pdf"
    if not pdf_path.is_file():
        raise FileNotFoundError(f"로컬 복사본이 없음: {pdf_path} (1단계 복사를 먼저 실행)")
    actual_hash = _file_hash(pdf_path)
    if actual_hash != pdf_hash:
        raise ValueError(f"{doc_id}: 로컬 복사본 해시 불일치(manifest={pdf_hash}, 실제={actual_hash})")

    doc = fitz.open(pdf_path)
    mat = fitz.Matrix(RENDER_DPI / 72, RENDER_DPI / 72)

    conn = vdb.get_connection()
    # --force로 재실행하는 경우 같은 (doc_id, prompt_version)의 이전 결과를 먼저
    # 지운다 - UNIQUE 제약과 충돌하지 않게, 그리고 절반만 남는 걸 막기 위해.
    conn.execute(
        "DELETE FROM vision_item WHERE page_id IN "
        "(SELECT id FROM vision_page WHERE doc_id = ? AND prompt_version = ?)",
        (doc_id, PROMPT_VERSION),
    )
    conn.execute(
        "DELETE FROM vision_page WHERE doc_id = ? AND prompt_version = ?",
        (doc_id, PROMPT_VERSION),
    )
    conn.commit()
    total_in = total_out = 0
    total_elapsed = 0.0
    pages_done = 0
    try:
        for i in range(doc.page_count):
            pix = doc[i].get_pixmap(matrix=mat)
            png_bytes = pix.tobytes("png")
            result = extract_page_image(png_bytes)
            # 20건 표본에서 188쪽 중 1쪽이 JSON 파싱 실패였다(모델 응답이 깨진 JSON) -
            # 같은 이미지로 한 번만 재시도(비용은 그 페이지분만 추가, 흔치 않은 일이라
            # 여러 번 재시도할 필요는 없다고 판단).
            if isinstance(result["parsed"], dict) and result["parsed"].get("parse_error"):
                retry = extract_page_image(png_bytes)
                if not (isinstance(retry["parsed"], dict) and retry["parsed"].get("parse_error")):
                    result = retry
            _insert_page(conn, doc_id, i + 1, pdf_hash, result)
            total_in += result["usage"]["input_tokens"]
            total_out += result["usage"]["output_tokens"]
            total_elapsed += result["elapsed_sec"]
            pages_done += 1
        conn.execute(
            "INSERT INTO run_log (doc_id, status, pages_done, total_elapsed_sec, total_input_tokens, "
            "total_output_tokens, error, run_at) VALUES (?,?,?,?,?,?,?,?)",
            (doc_id, "success", pages_done, total_elapsed, total_in, total_out, None, _now()),
        )
        conn.commit()
        return {"status": "success", "pages_done": pages_done, "elapsed_sec": total_elapsed,
                "input_tokens": total_in, "output_tokens": total_out}
    except Exception as e:  # noqa: BLE001 - 문서 단위 실패는 기록만 하고 다음 문서로 계속
        conn.rollback()
        conn.execute(
            "INSERT INTO run_log (doc_id, status, pages_done, total_elapsed_sec, total_input_tokens, "
            "total_output_tokens, error, run_at) VALUES (?,?,?,?,?,?,?,?)",
            (doc_id, "failed", pages_done, total_elapsed, total_in, total_out, str(e), _now()),
        )
        conn.commit()
        return {"status": "failed", "error": str(e), "pages_done": pages_done}
    finally:
        conn.close()
        doc.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="방식 B(비전 파싱) 실행기")
    ap.add_argument("--doc-ids", help="쉼표로 구분한 doc_id 목록")
    ap.add_argument("--all", action="store_true", help="momo_book.db 전체 305건")
    ap.add_argument("--sample", type=int, help="무작위 표본 N건")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--force", action="store_true", help="이미 완료된 문서도 다시 실행")
    args = ap.parse_args()

    vdb.init_db()
    manifest = _load_manifest()

    if args.doc_ids:
        targets = [d.strip() for d in args.doc_ids.split(",") if d.strip()]
    elif args.sample:
        random.seed(args.seed)
        targets = random.sample(_all_doc_ids(), args.sample)
    elif args.all:
        targets = _all_doc_ids()
    else:
        ap.error("--doc-ids, --all, --sample 중 하나는 필요합니다")
        return 2

    todo = []
    skipped = 0
    for doc_id in targets:
        if not args.force and vdb.doc_already_done(doc_id, PROMPT_VERSION):
            print(f"{doc_id}: 이미 완료 - 건너뜀")
            skipped += 1
            continue
        if doc_id not in manifest:
            print(f"{doc_id}: manifest에 없음 - 건너뜀")
            skipped += 1
            continue
        todo.append(doc_id)

    print(f"대상 {len(targets)}건 중 처리할 것 {len(todo)}건(건너뜀 {skipped}건), force={args.force}")
    ok = failed = 0

    # 문서 단위 병렬(ThreadPoolExecutor)도 시도해 봤으나, sqlite3 쓰기 락이
    # busy_timeout/WAL 설정에도 "database is locked"로 즉시 실패하는 걸
    # 반복 확인했다(원인을 더 파는 것보다, 백그라운드로 돌리는 무인 실행이라
    # 안정성이 속도보다 중요하다고 판단 - 순차 실행으로 확정).
    for i, doc_id in enumerate(todo, 1):
        t0 = time.time()
        result = run_document(doc_id, manifest[doc_id]["sha256"])
        dt = time.time() - t0
        if result["status"] == "success":
            ok += 1
            print(f"[{i}/{len(todo)}] {doc_id}: 성공 - {result['pages_done']}쪽, "
                  f"{dt:.1f}s, in={result['input_tokens']} out={result['output_tokens']}")
        else:
            failed += 1
            print(f"[{i}/{len(todo)}] {doc_id}: 실패 - {result['error']}")
    print(f"\n완료: 성공 {ok} / 실패 {failed} / 건너뜀 {skipped}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
