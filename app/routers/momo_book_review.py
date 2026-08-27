# -*- coding: utf-8 -*-
"""
모모의책장 교재DB(momo_book_db/momo_book.db) 검수 화면.
momo_book_db는 이 앱의 SQLAlchemy 모델/DB와 완전히 분리되어 있어서(app/database.py 안 씀),
suneung.py의 _ans_db() 패턴처럼 sqlite3를 직접 열어서 씀.

GET  /momo-review                              -> 문서 목록(검수 상태·낮은신뢰도 건수)
GET  /momo-review/{doc_id}                     -> 문서 상세 검수(섹션별 추출결과 vs 원문)
POST /momo-review/{doc_id}/{table}/{item_id}    -> 항목 수정 후 승인
POST /momo-review/{doc_id}/approve              -> 문서 전체를 approved로
POST /momo-review/{doc_id}/discussion_qa/{item_id}/upload-reference-image
                                                 -> "보기" 이미지 직접 업로드
POST /momo-review/{doc_id}/discussion_qa/{item_id}/upload-excerpt-image
                                                 -> "발췌문" 이미지 직접 업로드
POST /momo-review/{doc_id}/essay_prompt/{item_id}/upload-image
                                                 -> "글쓰기" 이미지 직접 업로드
POST /momo-review/{doc_id}/image/{image_id}/delete
                                                 -> 잘못 캡처된 이미지 삭제(표지/삽화/보기/발췌문/글쓰기)
POST /momo-review/{doc_id}/reparse              -> 원본 PDF로 다시 파싱(파서 수정 후 재추출용)
"""
import os
import sqlite3
import uuid
from urllib.parse import quote

from fastapi import APIRouter, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/momo-review")
templates = Jinja2Templates(directory="app/templates")


def _detail_url(doc_id: str, back: str = "") -> str:
    """상세 페이지로 돌아갈 때 목록의 필터(back)를 그대로 들고 가게 함"""
    return f"/momo-review/{doc_id}?back={quote(back)}" if back else f"/momo-review/{doc_id}"

_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "momo_book_db", "momo_book.db",
)
_IMAGES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "momo_book_db", "extracted_images",
)
_ALLOWED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

# item_id가 있는 4개 테이블만 개별 승인 대상(essay_outline_question은 essay_prompt에 딸려서 별도 승인 없음)
EDITABLE_TABLES = {
    "vocabulary": {
        "fields": ["word", "definition", "book_page", "example_sentence"],
        "id_col": "id",
    },
    "ox_quiz": {
        "fields": ["question", "evidence_page"],
        "id_col": "id",
    },
    "discussion_qa": {
        "fields": ["reading_type", "excerpt_text", "excerpt_page", "excerpt_image_path", "question_text",
                   "reference_text", "reference_image_path", "ui_type", "ui_config"],
        "id_col": "id",
    },
    "essay_prompt": {
        "fields": ["main_topic", "writing_guide", "image_path", "writing_format", "min_length", "closing_instruction"],
        "id_col": "id",
    },
}


def _db():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@router.get("", response_class=HTMLResponse)
def momo_review_list(request: Request, level: str = "", quarter: str = "", week: str = "",
                      status: str = "", q: str = ""):
    """문서 목록: 학년/분기/주차/문서상태 필터 + 검색, 미검토 문서가 위로 오게 정렬"""
    conn = _db()

    filter_options = {
        "levels": [r[0] for r in conn.execute(
            "SELECT DISTINCT level FROM documents WHERE level IS NOT NULL ORDER BY level")],
        "quarters": [r[0] for r in conn.execute(
            "SELECT DISTINCT quarter FROM documents WHERE quarter IS NOT NULL ORDER BY quarter")],
        "weeks": [r[0] for r in conn.execute(
            "SELECT DISTINCT week FROM documents WHERE week IS NOT NULL ORDER BY week")],
    }

    where, params = [], []
    if level:
        where.append("level = ?")
        params.append(level)
    if quarter:
        where.append("quarter = ?")
        params.append(quarter)
    if week.isdigit():
        where.append("week = ?")
        params.append(int(week))
    if status:
        where.append("review_status = ?")
        params.append(status)
    if q:
        where.append("(book_title LIKE ? OR doc_id LIKE ? OR book_author LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    docs = conn.execute(
        f"SELECT doc_id, level, quarter, week, book_title, book_author, review_status, version "
        f"FROM documents {where_sql} "
        f"ORDER BY (review_status = 'approved'), level, quarter, week",
        params,
    ).fetchall()

    # 상세 페이지로 이동한 뒤 다시 목록으로 돌아올 때 이 필터 상태를 그대로 유지하기 위함
    list_query = str(request.query_params)

    pending_counts = {}
    low_conf_counts = {}
    for table in EDITABLE_TABLES:
        for row in conn.execute(
            f"SELECT doc_id, COUNT(*) as c FROM {table} WHERE review_status != 'approved' GROUP BY doc_id"
        ):
            pending_counts[row["doc_id"]] = pending_counts.get(row["doc_id"], 0) + row["c"]
        low_conf_clause = "extraction_confidence < 0.7"
        if table == "discussion_qa":
            low_conf_clause += " OR ui_type = 'unknown'"
        for row in conn.execute(
            f"SELECT doc_id, COUNT(*) as c FROM {table} WHERE {low_conf_clause} GROUP BY doc_id"
        ):
            low_conf_counts[row["doc_id"]] = low_conf_counts.get(row["doc_id"], 0) + row["c"]
    conn.close()

    return templates.TemplateResponse("momo_review/list.html", {
        "request": request,
        "docs": docs,
        "pending_counts": pending_counts,
        "low_conf_counts": low_conf_counts,
        "filter_options": filter_options,
        "filters": {"level": level, "quarter": quarter, "week": week, "status": status, "q": q},
        "list_query": list_query,
    })


@router.get("/{doc_id}", response_class=HTMLResponse)
def momo_review_detail(request: Request, doc_id: str, back: str = ""):
    conn = _db()
    doc = conn.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
    if not doc:
        conn.close()
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")

    def fetch(table, order_col="order_no"):
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM {table} WHERE doc_id = ? ORDER BY {order_col}", (doc_id,)
        )]
        # 검수가 더 필요한 항목(낮은 신뢰도/unknown/미승인)을 위로 올림
        def needs_attention(r):
            if r.get("review_status") != "approved":
                return 0
            if r.get("extraction_confidence") is not None and r["extraction_confidence"] < 0.7:
                return 0
            if r.get("ui_type") == "unknown":
                return 0
            return 1
        rows.sort(key=needs_attention)
        return rows

    vocabulary = fetch("vocabulary")
    ox_quiz = fetch("ox_quiz")
    discussion_qa = fetch("discussion_qa")
    essay_prompts = fetch("essay_prompt", order_col="id")
    for e in essay_prompts:
        e["outline_questions"] = [dict(r) for r in conn.execute(
            "SELECT * FROM essay_outline_question WHERE essay_id = ? ORDER BY order_no", (e["id"],)
        )]
    logs = [dict(r) for r in conn.execute(
        "SELECT * FROM extraction_log WHERE doc_id = ? ORDER BY id", (doc_id,)
    )]
    images = [dict(r) for r in conn.execute(
        "SELECT * FROM document_image WHERE doc_id = ? ORDER BY source_page", (doc_id,)
    )]
    conn.close()

    cover_image = next((im for im in images if im["image_type"] == "cover"), None)
    illustrations_by_page = {im["source_page"]: im for im in images if im["image_type"] == "illustration"}
    images_by_page = {}
    for im in images:
        images_by_page.setdefault(im["source_page"], []).append(im)
    image_by_path = {im["file_path"]: im for im in images}

    return templates.TemplateResponse("momo_review/detail.html", {
        "request": request,
        "doc": doc,
        "vocabulary": vocabulary,
        "ox_quiz": ox_quiz,
        "discussion_qa": discussion_qa,
        "essay_prompts": essay_prompts,
        "logs": logs,
        "images_by_page": images_by_page,
        "cover_image": cover_image,
        "illustrations_by_page": illustrations_by_page,
        "image_by_path": image_by_path,
        "back": back,
    })


@router.post("/{doc_id}/{table}/{item_id}")
async def momo_review_update_item(doc_id: str, table: str, item_id: int, request: Request, back: str = ""):
    """항목 필드 수정 + review_status를 approved로 변경"""
    if table not in EDITABLE_TABLES:
        raise HTTPException(status_code=404, detail="알 수 없는 테이블입니다.")

    form = await request.form()
    fields = EDITABLE_TABLES[table]["fields"]
    set_clauses = []
    values = []
    for f in fields:
        if f in form:
            set_clauses.append(f"{f} = ?")
            values.append(form[f] or None)
    set_clauses.append("review_status = 'approved'")

    conn = _db()
    conn.execute(
        f"UPDATE {table} SET {', '.join(set_clauses)} WHERE id = ? AND doc_id = ?",
        (*values, item_id, doc_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=_detail_url(doc_id, back), status_code=303)


@router.post("/{doc_id}/approve")
def momo_review_approve_document(doc_id: str, back: str = ""):
    """문서 전체를 approved로(하위 항목 개별 검수와 별개로, 문서 단위 최종 승인)"""
    conn = _db()
    conn.execute("UPDATE documents SET review_status = 'approved' WHERE doc_id = ?", (doc_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url=_detail_url(doc_id, back), status_code=303)


async def _upload_slot_image(doc_id, table, item_id, column, image_type, file, back):
    """"보기"/"발췌문"/"글쓰기" 이미지 직접 업로드 공통 로직.
    파일을 extracted_images/{doc_id}/ 안에 저장하고 document_image에 기록한 뒤,
    해당 항목의 {column}을 바로 그 이미지로 설정함."""
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in _ALLOWED_IMAGE_EXT:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 이미지 형식입니다: {ext or '(확장자 없음)'}")

    conn = _db()
    row = conn.execute(
        f"SELECT source_page FROM {table} WHERE id = ? AND doc_id = ?", (item_id, doc_id)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="항목을 찾을 수 없습니다.")
    source_page = row["source_page"]

    doc_dir = os.path.join(_IMAGES_DIR, doc_id)
    os.makedirs(doc_dir, exist_ok=True)
    fname = f"{image_type}_{item_id}_{uuid.uuid4().hex[:8]}{ext}"
    with open(os.path.join(doc_dir, fname), "wb") as f:
        f.write(await file.read())
    file_path = f"{doc_id}/{fname}"

    conn.execute(
        "INSERT INTO document_image (doc_id, image_type, source_page, file_path, extraction_confidence) "
        "VALUES (?, ?, ?, ?, 1.0)",
        (doc_id, image_type, source_page, file_path),
    )
    conn.execute(
        f"UPDATE {table} SET {column} = ? WHERE id = ? AND doc_id = ?",
        (file_path, item_id, doc_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=_detail_url(doc_id, back), status_code=303)


@router.post("/{doc_id}/discussion_qa/{item_id}/upload-reference-image")
async def momo_review_upload_reference_image(doc_id: str, item_id: int, file: UploadFile = File(...), back: str = ""):
    return await _upload_slot_image(doc_id, "discussion_qa", item_id, "reference_image_path", "reference", file, back)


@router.post("/{doc_id}/discussion_qa/{item_id}/upload-excerpt-image")
async def momo_review_upload_excerpt_image(doc_id: str, item_id: int, file: UploadFile = File(...), back: str = ""):
    return await _upload_slot_image(doc_id, "discussion_qa", item_id, "excerpt_image_path", "excerpt", file, back)


@router.post("/{doc_id}/essay_prompt/{item_id}/upload-image")
async def momo_review_upload_essay_image(doc_id: str, item_id: int, file: UploadFile = File(...), back: str = ""):
    return await _upload_slot_image(doc_id, "essay_prompt", item_id, "image_path", "essay", file, back)


@router.post("/{doc_id}/image/{image_id}/delete")
def momo_review_delete_image(doc_id: str, image_id: int, back: str = ""):
    """자동 추출됐거나 업로드된 이미지가 잘못 캡처된 경우 삭제함(표지/삽화/보기 공통).
    파일과 document_image 행을 지우고, 그 이미지를 "보기"로 쓰던 문항이 있으면 연결도 끊음."""
    conn = _db()
    row = conn.execute(
        "SELECT file_path FROM document_image WHERE id = ? AND doc_id = ?", (image_id, doc_id)
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="이미지를 찾을 수 없습니다.")
    file_path = row["file_path"]

    conn.execute("DELETE FROM document_image WHERE id = ? AND doc_id = ?", (image_id, doc_id))
    conn.execute(
        "UPDATE discussion_qa SET reference_image_path = NULL WHERE doc_id = ? AND reference_image_path = ?",
        (doc_id, file_path),
    )
    conn.execute(
        "UPDATE discussion_qa SET excerpt_image_path = NULL WHERE doc_id = ? AND excerpt_image_path = ?",
        (doc_id, file_path),
    )
    conn.execute(
        "UPDATE essay_prompt SET image_path = NULL WHERE doc_id = ? AND image_path = ?",
        (doc_id, file_path),
    )
    conn.commit()
    conn.close()

    abs_path = os.path.join(_IMAGES_DIR, file_path)
    if os.path.isfile(abs_path):
        os.remove(abs_path)

    return RedirectResponse(url=_detail_url(doc_id, back), status_code=303)


@router.post("/{doc_id}/reparse")
def momo_review_reparse(doc_id: str, back: str = ""):
    """원본 PDF를 파서로 다시 돌려서 덮어씀(파서 버그를 고친 뒤 재추출할 때 사용).
    기존에 검수자가 손으로 고친 내용은 다시 파싱하면서 다 지워지니, 확인 후에 눌러야 함."""
    import sys
    momo_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "momo_book_db")
    if momo_dir not in sys.path:
        sys.path.insert(0, momo_dir)
    from loader import load_pdf_into_db

    conn = _db()
    doc = conn.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
    conn.close()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")

    source_file = doc["source_file"]
    if not os.path.isfile(source_file):
        raise HTTPException(
            status_code=400,
            detail=f"원본 PDF를 찾을 수 없습니다: {source_file} (USB/외장 드라이브 연결 확인 필요)",
        )

    doc_meta = {
        "doc_id": doc_id,
        "curriculum_id": doc["curriculum_id"],
        "level": doc["level"],
        "quarter": doc["quarter"],
        "week": doc["week"],
        "book_title": doc["book_title"],
        "book_author": doc["book_author"],
        "isbn": doc["isbn"],
    }
    load_pdf_into_db(source_file, doc_meta, use_llm=True, force=True)
    return RedirectResponse(url=_detail_url(doc_id, back), status_code=303)
