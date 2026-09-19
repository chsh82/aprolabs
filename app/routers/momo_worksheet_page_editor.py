# -*- coding: utf-8 -*-
"""
페이지 고정 편집(momo_page_editor_plan.md P1). 기존 momo_worksheet_editor.py(문항 단위
편집)는 전혀 수정하지 않고, 같은 프로젝트 저장소(project_store.py) 위에 "페이지 구성표
고정 + 후보 미리보기 + 적용" 계층만 별도 파일로 얹는다 - 공통 템플릿 편집과 분리된
"별도 모드"이자, 기존 문항 편집 화면과도 분리된 화면(운영 DB·기존 프로젝트·기존
편집 화면에 영향 없음).

GET  /momo-worksheet-editor/{project_id}/pages
     -> 페이지 편집 화면(왼쪽 썸네일 / 가운데 선택 페이지 / 오른쪽 직접 수정 패널).
        이 프로젝트에 페이지 구성표가 아직 없으면 여기 들어오는 시점에만 "얼린다"
        (다른 프로젝트를 건드리지 않는 명시적 전환 - 일괄 자동 변환 없음).
GET  /momo-worksheet-editor/{project_id}/pages/{page_id}
     -> 페이지 선택(위와 같은 화면, 선택 페이지만 바뀜).
POST /momo-worksheet-editor/{project_id}/pages/{page_id}/propose
     -> 직접 편집 1건 제출 -> 후보 생성(적용 전, 별도 저장).
POST /momo-worksheet-editor/{project_id}/pages/{page_id}/apply
     -> 후보 적용(원자적 새 리비전 + 새 편집 버전 + QA).
POST /momo-worksheet-editor/{project_id}/pages/{page_id}/cancel
     -> 후보 취소(현재 버전·마지막 정상본 불변).
GET  /momo-worksheet-editor/{project_id}/pages/history
     -> 페이지 리비전 이력 + 복원.
POST /momo-worksheet-editor/{project_id}/pages/restore
     -> 이전 리비전으로 복원(새 리비전으로 기록).
GET  /momo-worksheet-editor/{project_id}/pages/proposal/{proposal_id}/preview/{filename}
     -> 적용 전 후보의 전체 문서 미리보기(정상 버전/PDF 다운로드 경로와 분리됨).
"""
import os
import sys

from fastapi import APIRouter, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/momo-worksheet-editor")
templates = Jinja2Templates(directory="app/templates")

_MOMO_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "momo_book_db")
_EDITOR_DIR = os.path.join(_MOMO_DIR, "worksheet", "editor")
if _EDITOR_DIR not in sys.path:
    sys.path.insert(0, _EDITOR_DIR)

import project_store as ps  # noqa: E402
import page_store as pgs  # noqa: E402


def _render(request, project_id, page_id=None, error=None):
    try:
        meta = ps.load_project(project_id)
    except ps.ProjectError as e:
        raise HTTPException(status_code=404, detail=str(e))

    try:
        manifest = pgs.freeze_manifest(project_id)
    except pgs.PageEditError as e:
        return templates.TemplateResponse("momo_worksheet_editor/pages_error.html", {
            "request": request, "project_id": project_id, "meta": meta, "error": str(e),
        })

    thumbs = pgs.page_thumbnails(manifest)
    selected_id = page_id or next((t["page_id"] for t in thumbs if t["editable"]), thumbs[0]["page_id"] if thumbs else None)
    page = next((p for p in manifest["pages"] if p["page_id"] == selected_id), None)
    items = pgs.page_items(project_id, page) if page and page["layout_type"] in ("halves", "fullpage") else []
    active_proposal = pgs.list_active_proposal(project_id, selected_id) if selected_id else None

    current_preview_url = None
    if meta.get("current_version"):
        idx = os.path.join(ps.project_dir(project_id), "versions", meta["current_version"], "index.html")
        if os.path.isfile(idx):
            current_preview_url = f"/momo-worksheet-editor/{project_id}/preview/{meta['current_version']}/index.html"

    candidate_preview_url = None
    if active_proposal and active_proposal.get("status") == "ok":
        candidate_preview_url = (
            f"/momo-worksheet-editor/{project_id}/pages/proposal/"
            f"{active_proposal['proposal_id']}/preview/index.html"
        )

    return templates.TemplateResponse("momo_worksheet_editor/pages.html", {
        "request": request, "project_id": project_id, "meta": meta, "manifest": manifest,
        "thumbs": thumbs, "selected_id": selected_id, "page": page, "items": items,
        "active_proposal": active_proposal, "current_preview_url": current_preview_url,
        "candidate_preview_url": candidate_preview_url, "error": error,
    })


@router.get("/{project_id}/pages", response_class=HTMLResponse)
def pages_home(request: Request, project_id: str):
    return _render(request, project_id)


@router.get("/{project_id}/pages/history", response_class=HTMLResponse)
def pages_history(request: Request, project_id: str):
    try:
        meta = ps.load_project(project_id)
        manifest = pgs.load_manifest(project_id)
    except (ps.ProjectError, pgs.PageEditError) as e:
        raise HTTPException(status_code=404, detail=str(e))
    history = pgs.list_history(project_id)
    return templates.TemplateResponse("momo_worksheet_editor/pages_history.html", {
        "request": request, "project_id": project_id, "meta": meta,
        "manifest": manifest, "history": history,
    })


@router.post("/{project_id}/pages/restore")
def pages_restore(request: Request, project_id: str, revision_id: str = Form(...), base_revision: str = Form(...)):
    try:
        pgs.restore_revision(project_id, revision_id, base_revision)
    except pgs.PageEditError as e:
        history = pgs.list_history(project_id)
        meta = ps.load_project(project_id)
        manifest = pgs.load_manifest(project_id)
        return templates.TemplateResponse("momo_worksheet_editor/pages_history.html", {
            "request": request, "project_id": project_id, "meta": meta,
            "manifest": manifest, "history": history, "error": str(e),
        })
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}/pages", status_code=303)


@router.get("/{project_id}/pages/{page_id}", response_class=HTMLResponse)
def pages_select(request: Request, project_id: str, page_id: str):
    return _render(request, project_id, page_id=page_id)


@router.post("/{project_id}/pages/{page_id}/propose")
async def pages_propose(
    request: Request, project_id: str, page_id: str,
    item_id: str = Form(...), operation: str = Form(...), base_revision: str = Form(...),
    question_text: str = Form(""), answer_height_mm: str = Form(""),
    image_field: str = Form(""), image_file: UploadFile = File(None),
):
    kwargs = {}
    if operation == "replace_image":
        if image_file is None or not image_file.filename:
            return _render(request, project_id, page_id=page_id, error="업로드할 이미지 파일을 선택해주세요.")
        kwargs["image_field"] = image_field
        kwargs["image_file_bytes"] = await image_file.read()
        kwargs["image_filename"] = image_file.filename
    elif operation == "set_text":
        kwargs["question_text"] = question_text
    elif operation == "set_answer_area":
        kwargs["answer_height_mm"] = answer_height_mm

    try:
        pgs.create_proposal(project_id, page_id, item_id, operation, base_revision, **kwargs)
    except pgs.PageEditError as e:
        return _render(request, project_id, page_id=page_id, error=str(e))
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}/pages/{page_id}", status_code=303)


@router.post("/{project_id}/pages/{page_id}/apply")
def pages_apply(request: Request, project_id: str, page_id: str,
                 proposal_id: str = Form(...), base_revision: str = Form(...)):
    try:
        pgs.apply_proposal(project_id, proposal_id, base_revision)
    except pgs.PageEditError as e:
        return _render(request, project_id, page_id=page_id, error=str(e))
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}/pages/{page_id}", status_code=303)


@router.post("/{project_id}/pages/{page_id}/cancel")
def pages_cancel(request: Request, project_id: str, page_id: str, proposal_id: str = Form(...)):
    try:
        pgs.cancel_proposal(project_id, proposal_id)
    except pgs.PageEditError as e:
        return _render(request, project_id, page_id=page_id, error=str(e))
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}/pages/{page_id}", status_code=303)


@router.get("/{project_id}/pages/proposal/{proposal_id}/preview/{filename:path}")
def proposal_preview_asset(project_id: str, proposal_id: str, filename: str):
    """후보(적용 전) 미리보기 - 정상 버전(versions/)과 물리적으로 다른 경로
    (page_proposals/<...>/preview/)라 PDF 다운로드(/pdf/{version_id})와 절대 섞이지 않는다."""
    base = os.path.join(ps.project_dir(project_id), "page_proposals", proposal_id, "preview")
    path = os.path.normpath(os.path.join(base, filename))
    if not path.startswith(os.path.normpath(base)):
        raise HTTPException(status_code=403, detail="잘못된 경로")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="파일 없음")
    return FileResponse(path)
