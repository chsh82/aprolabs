# -*- coding: utf-8 -*-
"""
모모의책장 교재DB(momo_book_db/momo_book.db) 검수 화면.
momo_book_db는 이 앱의 SQLAlchemy 모델/DB와 완전히 분리되어 있어서(app/database.py 안 씀),
suneung.py의 _ans_db() 패턴처럼 sqlite3를 직접 열어서 씀.

GET  /momo-review                              -> 문서 목록(검수 상태·낮은신뢰도 건수)
GET  /momo-review/{doc_id}                     -> 문서 상세 검수(섹션별 추출결과 vs 원문)
POST /momo-review/{doc_id}/{table}/{item_id}    -> 항목 수정 후 승인
POST /momo-review/{doc_id}/approve              -> 문서 전체를 approved로
"""
import os
import sqlite3

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/momo-review")
templates = Jinja2Templates(directory="app/templates")

_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "momo_book_db", "momo_book.db",
)

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
        "fields": ["reading_type", "excerpt_text", "excerpt_page", "question_text",
                   "reference_text", "reference_image_path", "ui_type", "ui_config"],
        "id_col": "id",
    },
    "essay_prompt": {
        "fields": ["main_topic", "writing_format", "min_length", "closing_instruction"],
        "id_col": "id",
    },
}


def _db():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@router.get("", response_class=HTMLResponse)
def momo_review_list(request: Request):
    """문서 목록: 검수 상태와 손봐야 할 항목 개수를 같이 보여줌"""
    conn = _db()
    docs = conn.execute(
        "SELECT doc_id, level, quarter, week, book_title, book_author, review_status, version "
        "FROM documents ORDER BY level, quarter, week"
    ).fetchall()

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
    })


@router.get("/{doc_id}", response_class=HTMLResponse)
def momo_review_detail(request: Request, doc_id: str):
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
    })


@router.post("/{doc_id}/{table}/{item_id}")
async def momo_review_update_item(doc_id: str, table: str, item_id: int, request: Request):
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
    return RedirectResponse(url=f"/momo-review/{doc_id}", status_code=303)


@router.post("/{doc_id}/approve")
def momo_review_approve_document(doc_id: str):
    """문서 전체를 approved로(하위 항목 개별 검수와 별개로, 문서 단위 최종 승인)"""
    conn = _db()
    conn.execute("UPDATE documents SET review_status = 'approved' WHERE doc_id = ?", (doc_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/momo-review/{doc_id}", status_code=303)
