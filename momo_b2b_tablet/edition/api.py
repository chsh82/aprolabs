"""④⑦ edition API - SPEC §6 중 4·7단계 완료 기준에 필요한 부분.

의도적으로 안 만든 것(다음 단계 몫):
  - 파트너 세션 교환(/api/partner/sessions) - 인증·API 키는 5단계 이후.
    지금은 launch 토큰 없이 붙는 것만 지원(renderer/adapters/api.js도 launchToken이
    없으면 세션 교환을 건너뛰게 이미 짜여 있다).
  - /slots/{path}/candidates, /print.pdf, /export - 5·6단계.

실행: uvicorn edition.api:app --port 8000  (momo_b2b_tablet/에서)
ANTHROPIC_API_KEY 환경변수가 있어야 /api/runtime/recognize가 동작한다.
"""
from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from pydantic import BaseModel

from . import db, print_pdf as print_pdf_mod, recognize as recognize_mod, store

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
EXTRACTED_DIR = REPO_ROOT / "momo_book_db" / "extracted_images"
RENDERER_DIR = Path(__file__).resolve().parent.parent / "renderer"

db.init_db()

app = FastAPI(title="momo_b2b_tablet edition API")

if ASSETS_DIR.exists():
    app.mount("/static/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
if EXTRACTED_DIR.exists():
    app.mount("/static/extracted", StaticFiles(directory=str(EXTRACTED_DIR)), name="extracted")
if RENDERER_DIR.exists():
    # 실제 배포에서도 aprolabs가 API와 학생용 웹뷰(/runtime/{edition_id})를 같은
    # 서버에서 직접 호스팅하기로 했었다(호스팅·인증 방향 1번) - 그 모양 그대로
    # viewer.html을 이 앱에 정적으로 얹는다. renderer/adapters/api.js가 실제
    # /api/runtime/{edition_id}에 붙는지 여기서 끝까지 확인한다.
    app.mount("/renderer", StaticFiles(directory=str(RENDERER_DIR), html=True), name="renderer")

REVIEW_DIR = Path(__file__).resolve().parent.parent / "review"
if REVIEW_DIR.exists():
    app.mount("/review", StaticFiles(directory=str(REVIEW_DIR), html=True), name="review")


class DraftRequest(BaseModel):
    doc_id: str
    created_by: str | None = None


class PatchRequest(BaseModel):
    patch: list[dict]
    editor: str | None = None
    reason: str | None = None
    expected_rev: int | None = None


class ApproveRequest(BaseModel):
    approved_by: str | None = None


class ResolveFlagRequest(BaseModel):
    resolved_by: str | None = None


class AnswerBody(BaseModel):
    ink: list | None = None
    text: dict | None = None
    ox: str | None = None
    choice: str | list[str] | None = None  # choiceList가 single:false면 배열(복수 선택)


@app.post("/api/editions/draft")
def create_draft(req: DraftRequest):
    try:
        edition_id = store.create_draft(req.doc_id, created_by=req.created_by)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    return {"edition_id": edition_id}


@app.get("/api/editions/{edition_id}")
def get_edition(edition_id: int):
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    return {"id": row["id"], "doc_id": row["doc_id"], "version": row["version"], "rev": row["rev"],
            "status": row["status"], "layout": json.loads(row["layout_json"])}


@app.patch("/api/editions/{edition_id}")
def patch_edition(edition_id: int, req: PatchRequest):
    try:
        new_id = store.patch_edition(edition_id, req.patch, editor=req.editor, reason=req.reason,
                                      expected_rev=req.expected_rev)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    except store.ConflictError as e:
        raise HTTPException(409, str(e)) from e
    except store.PatchError as e:
        raise HTTPException(400, str(e)) from e
    row = store.get_edition_row(new_id)
    return {"id": new_id, "doc_id": row["doc_id"], "version": row["version"], "rev": row["rev"],
            "status": row["status"], "layout": json.loads(row["layout_json"])}


@app.get("/api/editions/{edition_id}/flags")
def list_flags(edition_id: int):
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    return {"flags": store.list_flags(edition_id)}


@app.patch("/api/editions/{edition_id}/flags/{flag_id}")
def resolve_flag(edition_id: int, flag_id: int, req: ResolveFlagRequest):
    store.resolve_flag(flag_id, resolved_by=req.resolved_by)
    return {"ok": True}


@app.post("/api/editions/{edition_id}/approve")
def approve_edition(edition_id: int, req: ApproveRequest):
    try:
        store.approve(edition_id, approved_by=req.approved_by)
    except store.ApprovalBlocked as e:
        raise HTTPException(409, str(e)) from e
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    return {"ok": True}


@app.post("/api/editions/{edition_id}/publish")
def publish_edition(edition_id: int):
    try:
        store.publish(edition_id)
    except store.NotFound as e:
        raise HTTPException(404, str(e)) from e
    except store.PatchError as e:
        raise HTTPException(409, str(e)) from e
    return {"ok": True}


@app.get("/api/editions/{edition_id}/images")
def edition_images(edition_id: int):
    """검수 미리보기 전용 - runtime과 달리 included=false 페이지 이미지도 필요하다."""
    images = store.review_images(edition_id)
    if images is None:
        raise HTTPException(404, "edition not found")
    return {"images": images}


@app.get("/api/editions/{edition_id}/source/{order_no}")
def edition_source_text(edition_id: int, order_no: int):
    """검수 화면의 "원문 대조" - momo_book_db(읽기 전용) 원문 그대로."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    text = store.source_text(row["doc_id"], order_no)
    if text is None:
        raise HTTPException(404, "source not found")
    return text


@app.get("/api/editions/{edition_id}/print-view")
def print_view(edition_id: int):
    """⑥단계 인쇄용 - included=false 페이지 제외 + 홀수 쪽수면 마지막에 빈 면(store.print_view)."""
    view = store.print_view(edition_id)
    if view is None:
        raise HTTPException(404, "edition not found")
    return view


@app.get("/api/editions/{edition_id}/answers")
def get_answers(edition_id: int, student_id: str = "dev-anonymous"):
    """⑥단계 "학생 필기 포함" 인쇄, renderer/adapters/print.js 전용."""
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    return store.load_answers(edition_id, student_id)


@app.get("/api/editions/{edition_id}/print.pdf")
async def print_pdf(edition_id: int, request: Request, ink: bool = False, student_id: str = "dev-anonymous"):
    """⑥단계: A4 2-up PDF. ?ink=true&student_id=... 로 특정 학생 필기 포함/미포함 선택."""
    row = store.get_edition_row(edition_id)
    if row is None:
        raise HTTPException(404, "edition not found")
    base = str(request.base_url).rstrip("/")
    url = (f"{base}/renderer/viewer.html?doc={row['doc_id']}&mode=student&adapter=print"
           f"&edition={edition_id}&ink={'true' if ink else 'false'}&student={student_id}")
    try:
        pdf_bytes = await print_pdf_mod.render_pdf(url)
    except PlaywrightTimeoutError as e:
        raise HTTPException(504, f"인쇄 렌더링 시간 초과: {e}") from e
    filename = f"{row['doc_id']}-edition{edition_id}{'-with-ink' if ink else ''}.pdf"
    return Response(content=pdf_bytes, media_type="application/pdf",
                     headers={"Content-Disposition": f'inline; filename="{filename}"'})


@app.get("/api/runtime/{edition_id}")
def runtime_view(edition_id: int):
    view = store.runtime_view(edition_id)
    if view is None:
        raise HTTPException(404, "edition not found")
    return view


@app.put("/api/runtime/{edition_id}/answers/{part_id}")
def save_answer(edition_id: int, part_id: str, body: AnswerBody, student_id: str = "dev-anonymous"):
    # student_id: 정식 파트너 세션(§호스팅·인증)이 아직 없어 쿼리 파라미터 기본값으로
    # 임시 대체한다 - renderer/adapters/api.js는 이 파라미터를 보내지 않으므로 지금은
    # 항상 "dev-anonymous" 한 명으로 저장된다(5단계 이후 세션 붙이면 교체).
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    store.save_answer(edition_id, part_id, student_id, ink=body.ink, text=body.text, ox=body.ox, choice=body.choice)
    return {"ok": True}


@app.post("/api/runtime/recognize")
async def recognize_handwriting(
    image: UploadFile = File(...),
    prompt: str = Form(...),
    part_id: str = Form(...),
    edition_id: int = Form(...),
    model: str | None = Form(None),
    student_id: str = "dev-anonymous",
):
    # edition_id: SPEC §6대로라면 세션 쿠키로 알아내야 하지만(경로에 없음) 세션이
    # 아직 없어서(§4단계 노트와 동일한 이유) renderer/adapters/api.js가 폼 필드로
    # 같이 보낸다 - 5단계 이후 세션이 생기면 여기서만 바꾸면 된다.
    if store.get_edition_row(edition_id) is None:
        raise HTTPException(404, "edition not found")
    image_bytes = await image.read()
    try:
        result = await recognize_mod.recognize_handwriting(image_bytes, prompt, model=model)
    except recognize_mod.RecognitionError as e:
        status = {"rate_limited": 429, "upstream_error": 502}.get(e.code, 422)
        raise HTTPException(status, detail=e.code) from e
    store.log_recognition(edition_id, student_id, part_id, result["model"], prompt,
                           result["text"], result["unclear"], result["latency_ms"])
    return {"text": result["text"], "unclear": result["unclear"]}


@app.on_event("shutdown")
async def _shutdown():
    await print_pdf_mod.shutdown()
