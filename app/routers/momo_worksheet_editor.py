# -*- coding: utf-8 -*-
"""
학습지 최소 편집 기능. 운영 DB(momo_book.db)는 전혀 건드리지 않는다 - 편집 내용은
momo_book_db/worksheet/edit_projects/<project_id>/에 파일로만 저장된다
(momo_book_db/worksheet/editor/project_store.py).

기존 생성기(generate.js)·디자인(styles.css)·QA(check.js)는 그대로 재사용한다 - 이 라우터는
편집 프로젝트의 데이터를 "실제 DB에서 뽑은 것과 같은 모양의 data.json"으로 합쳐서
기존 파이프라인에 넘기고 결과를 보여주는 것만 한다.

GET  /momo-worksheet-editor                        -> 편집 프로젝트 목록 + 새 프로젝트 시작
POST /momo-worksheet-editor/new                     -> 새 프로젝트 생성(원본 DB 또는 복원 후보 JSON에서)
GET  /momo-worksheet-editor/{project_id}            -> 편집 화면(문항 목록 + 선택한 문항 편집 폼 + 미리보기)
POST /momo-worksheet-editor/{project_id}/save        -> 저장(버전 생성) + 재조판 + QA
POST /momo-worksheet-editor/{project_id}/restore/{version_id} -> 이전 버전 복원
GET  /momo-worksheet-editor/{project_id}/export      -> zip 내보내기
POST /momo-worksheet-editor/import                   -> zip 불러오기
"""
import json
import os
import subprocess
import sys
from datetime import datetime

from fastapi import APIRouter, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/momo-worksheet-editor")
templates = Jinja2Templates(directory="app/templates")

_MOMO_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "momo_book_db")
_GENERATE_DIR = os.path.join(_MOMO_DIR, "generate")
_EDITOR_DIR = os.path.join(_MOMO_DIR, "worksheet", "editor")
_SCRIPTS_DIR = os.path.join(_MOMO_DIR, "worksheet", "scripts")
_GENERATED_DIR = os.path.join(_MOMO_DIR, "generated")

for p in (_GENERATE_DIR, _EDITOR_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

import project_store as ps  # noqa: E402  momo_book_db/worksheet/editor/project_store.py


def _db_readonly():
    import sqlite3
    db_path = os.path.join(_MOMO_DIR, "momo_book.db")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@router.get("", response_class=HTMLResponse)
def editor_list(request: Request):
    projects = ps.list_projects()
    with _db_readonly() as conn:
        docs = conn.execute(
            "SELECT doc_id, book_title FROM documents WHERE review_status='approved' ORDER BY doc_id"
        ).fetchall()
    return templates.TemplateResponse("momo_worksheet_editor/list.html", {
        "request": request, "projects": projects, "docs": [dict(d) for d in docs],
    })


@router.post("/new")
def editor_new(doc_id: str = Form(...), source: str = Form("original")):
    """source='original'이면 실제 DB에서 바로 추출, 'restoration'이면 이 세션(3차 안정화)이
    만든 RESTORATION_PATCHES.json의 patches를 적용한 데이터로 시작한다 - 어느 쪽이든
    프로젝트 생성 시점에 스냅샷을 얼려서 이후 운영 DB가 바뀌어도 이 프로젝트엔 영향 없다."""
    import extract_worksheet_json as ewj

    data = ewj.extract(doc_id)
    data_origin = "original"
    patch_ids = []
    if source == "restoration":
        patches_path = os.path.join(_MOMO_DIR, "worksheet", "docs", "RESTORATION_PATCHES.json")
        if os.path.isfile(patches_path):
            all_patches = json.load(open(patches_path, encoding="utf-8")).get("patches", [])
            doc_patches = [p for p in all_patches if p["doc_id"] == doc_id and p.get("applied")]
            by_id = {str(q["id"]): q for q in data["step2"]}
            for p in doc_patches:
                table = p.get("table", "discussion_qa")
                # 2026-09-19 8차 안정화: discussion_qa(문항) 단위가 아니라 문서 전체
                # 메타(meta.background_text 등)나 3단계(step3) 전체를 대상으로 하는 패치도
                # 지원한다 - "출처 확인이 안 된 추가 활동은 학생용 출력에서 기본 제외"
                # 요구사항(예: 원본 6쪽짜리 판본에 없는 배경지식/글쓰기 페이지)을 위함.
                # 운영 DB는 안 건드림 - 이 프로젝트의 얼린 데이터 스냅샷에만 적용됨.
                if table == "meta":
                    data["meta"][p["field"]] = p["after"]
                    patch_ids.append(p.get("id") or f"meta.{p['field']}")
                    continue
                if table == "step3":
                    data["step3"] = p["after"]
                    patch_ids.append(p.get("id") or "step3")
                    continue
                if table == "step1":
                    # 2026-09-19 9차 안정화: 1단계(어휘) 전체 구조를 바꾸는 패치(예:
                    # vocab_match/vocab_fillblank 추가) - discussion_qa 문항이 아니라
                    # data["step1"] 딕셔너리에 직접 병합한다.
                    data["step1"][p["field"]] = p["after"]
                    patch_ids.append(p.get("id") or f"step1.{p['field']}")
                    continue
                item = by_id.get(str(p["id"]))
                if not item:
                    continue
                # ui_config는 통째로(중첩 dict) 교체, 그 외 필드(ui_type/question_text/
                # excerpt_image_path/reference_image_path 등)는 평범한 값 대입 - 2026-09-19
                # 6차 안정화: id=1607(문항#1) 복원이 question_text·excerpt_image_path·
                # reference_image_path 패치까지 필요해져서 ui_type/ui_config 두 개로
                # 고정돼 있던 분기를 일반화함(동작은 기존 두 필드에서 그대로, 새 필드가
                # 늘어도 라우터를 또 고칠 필요 없음).
                if p["field"] == "ui_config":
                    item["ui_config"] = p["after"]
                else:
                    item[p["field"]] = p["after"]
                patch_ids.append(p["id"])
            data["issues"] = [i for i in data["issues"] if i.get("row_id") not in {p.get("id") for p in doc_patches}]
            data_origin = "restoration_candidate"

    project_id = ps.create_project(doc_id, data, data_origin=data_origin, restoration_patch_ids=patch_ids)
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}", status_code=303)


def _run_pipeline(project_id):
    """project_store.build_effective_data()로 만든 data.json을
    generate.js(explicit outPath 인자로 edit_projects 쪽에 씀) -> check.js에 그대로 태운다.
    기존 momo_book_worksheet.py의 _run_build()와 같은 패턴이지만, 이 결과는 절대
    generated/<doc_id>/current.json(운영 승인 포인터)을 만들지 않는다 - 그 함수 자체를
    호출하지 않으므로 승격 경로가 물리적으로 없다."""
    data, edit_doc_id, version_id = ps.build_effective_data(project_id)
    if not version_id:
        return {"status": "NOT_SAVED", "message": "아직 저장된 버전이 없음"}

    pdir = ps.project_dir(project_id)
    vdir = os.path.join(pdir, "versions", version_id)
    data_path = os.path.join(vdir, "effective_data.json")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    out_html = os.path.join(vdir, "index.html")
    log = []
    try:
        result = subprocess.run(
            ["node", "generate.js", edit_doc_id, data_path, out_html],
            cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=180,
        )
        log.append(result.stdout)
        if result.stderr:
            log.append("--- stderr ---\n" + result.stderr)
        if result.returncode != 0:
            _write_status(vdir, "FAIL", log)
            return {"status": "FAIL", "message": "generate.js 실패", "log": "\n".join(log)}
    except subprocess.TimeoutExpired:
        _write_status(vdir, "BLOCKED", log)
        return {"status": "BLOCKED", "message": "generate.js 타임아웃", "log": "\n".join(log)}

    report_path = os.path.join(vdir, "check_report.json")
    try:
        result = subprocess.run(
            ["node", "check.js", out_html, data_path, report_path],
            cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=120,
        )
        log.append(result.stdout)
        if result.stderr:
            log.append("--- stderr ---\n" + result.stderr)
    except subprocess.TimeoutExpired:
        _write_status(vdir, "BLOCKED", log)
        return {"status": "BLOCKED", "message": "check.js 타임아웃", "log": "\n".join(log)}

    qa_status = "FAIL"
    if os.path.isfile(report_path):
        try:
            qa_status = json.load(open(report_path, encoding="utf-8")).get("status", "FAIL")
        except (json.JSONDecodeError, OSError):
            pass
    _write_status(vdir, qa_status, log)
    ps.mark_qa_result(project_id, version_id, qa_status)
    return {"status": qa_status, "log": "\n".join(log), "edit_doc_id": edit_doc_id, "version_id": version_id}


def _write_status(vdir, status, log):
    with open(os.path.join(vdir, "build.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(log))
    manifest_path = os.path.join(vdir, "manifest.json")
    manifest = json.load(open(manifest_path, encoding="utf-8")) if os.path.isfile(manifest_path) else {}
    manifest["qa_status"] = status
    manifest["checked_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


@router.get("/{project_id}", response_class=HTMLResponse)
def editor_detail(request: Request, project_id: str, item_id: str = ""):
    try:
        meta = ps.load_project(project_id)
    except ps.ProjectError as e:
        raise HTTPException(status_code=404, detail=str(e))

    base_path = os.path.join(ps.project_dir(project_id), "base_data.json")
    base = json.load(open(base_path, encoding="utf-8"))
    content_overrides, design_overrides = ({}, {})
    if meta.get("current_version"):
        content_overrides, design_overrides = ps.get_version_overrides(project_id, meta["current_version"])

    items = []
    for q in base.get("step2", []):
        items.append({
            "id": q["id"], "order_label": q["order_label"], "ui_type": q["ui_type"],
            "question_text": content_overrides.get(str(q["id"]), {}).get("question_text", q["question_text"]),
            "edited": str(q["id"]) in content_overrides or str(q["id"]) in design_overrides,
            "answer_height_mm": design_overrides.get(str(q["id"]), {}).get("answer_height_mm"),
            "excerpt_image_path": content_overrides.get(str(q["id"]), {}).get("excerpt_image_path", q.get("excerpt_image_path")),
            "reference_image_path": content_overrides.get(str(q["id"]), {}).get("reference_image_path", q.get("reference_image_path")),
        })

    selected = next((i for i in items if str(i["id"]) == str(item_id)), items[0] if items else None)
    versions = list(reversed(ps.list_versions(project_id)))
    assets = ps.list_assets(project_id)

    qa_status = None
    if meta.get("current_version"):
        manifest_path = os.path.join(ps.project_dir(project_id), "versions", meta["current_version"], "manifest.json")
        if os.path.isfile(manifest_path):
            qa_status = json.load(open(manifest_path, encoding="utf-8")).get("qa_status")

    # 미리보기는 "최근 저장이 QA를 통과했으면 그걸, 아니면 마지막 정상본(last_good_version)"을
    # 보여준다 - momo_book_worksheet.py의 "최근 빌드 실패 - 이전 정상본 표시 중" 패턴과
    # 동일한 원칙("실패한 빌드는 정상본으로 승격하지 말고 기존 정상본을 유지").
    preview_version = None
    showing_previous = False
    if qa_status == "PASS":
        preview_version = meta.get("current_version")
    elif meta.get("last_good_version"):
        preview_version = meta["last_good_version"]
        showing_previous = meta.get("current_version") != preview_version

    preview_url = None
    if preview_version:
        idx_path = os.path.join(ps.project_dir(project_id), "versions", preview_version, "index.html")
        if os.path.isfile(idx_path):
            preview_url = f"/momo-worksheet-editor/{project_id}/preview/{preview_version}/index.html"

    return templates.TemplateResponse("momo_worksheet_editor/detail.html", {
        "request": request, "meta": meta, "items": items, "selected": selected,
        "versions": versions, "assets": assets, "qa_status": qa_status, "preview_url": preview_url,
        "preview_version": preview_version, "showing_previous": showing_previous,
    })


@router.get("/{project_id}/preview/{version_id}/{filename:path}")
def editor_preview_asset(project_id: str, version_id: str, filename: str):
    """편집 프로젝트 빌드 결과(index.html)와 그 상대경로 자산(styles.css/이미지)을 그대로
    서빙한다 - generate.js가 만든 출력은 기존 정적 마운트(momo-worksheet-assets)가 아니라
    edit_projects 아래에 있어서 별도 경로가 필요함."""
    base = os.path.join(ps.project_dir(project_id), "versions", version_id)
    path = os.path.normpath(os.path.join(base, filename))
    if not path.startswith(os.path.normpath(base)):
        raise HTTPException(status_code=403, detail="잘못된 경로")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="파일 없음")
    return FileResponse(path)


@router.get("/{project_id}/pdf/{version_id}")
def editor_download_pdf(project_id: str, version_id: str):
    """해당 버전의 index.html을 실제 PDF로 내려받는다(export_pdf.js, 기존
    check.js와 같은 page.pdf() 경로 재사용 - 새 생성 로직 없음). 캐시가 있으면
    재생성 없이 그대로 서빙, 없으면 그 자리에서 만든다."""
    try:
        ps.load_project(project_id)
    except ps.ProjectError as e:
        raise HTTPException(status_code=404, detail=str(e))
    vdir = os.path.join(ps.project_dir(project_id), "versions", version_id)
    html_path = os.path.join(vdir, "index.html")
    if not os.path.isfile(html_path):
        raise HTTPException(status_code=404, detail="이 버전은 아직 빌드된 결과가 없음")
    pdf_path = os.path.join(vdir, "export.pdf")
    if not os.path.isfile(pdf_path) or os.path.getmtime(html_path) > os.path.getmtime(pdf_path):
        result = subprocess.run(
            ["node", "export_pdf.js", html_path, pdf_path],
            cwd=_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8", timeout=90,
        )
        if result.returncode != 0 or not os.path.isfile(pdf_path):
            raise HTTPException(status_code=500, detail=f"PDF 생성 실패: {result.stderr or result.stdout}")
    return FileResponse(pdf_path, filename=f"{project_id}_{version_id}.pdf", media_type="application/pdf")


@router.post("/{project_id}/save")
async def editor_save(
    request: Request, project_id: str,
    item_id: str = Form(...), question_text: str = Form(...),
    answer_height_mm: str = Form(""), image_field: str = Form(""),
    image_file: UploadFile = File(None),
):
    try:
        meta = ps.load_project(project_id)
    except ps.ProjectError as e:
        raise HTTPException(status_code=404, detail=str(e))

    prev_content, prev_design = ({}, {})
    if meta.get("current_version"):
        prev_content, prev_design = ps.get_version_overrides(project_id, meta["current_version"])
    content_overrides = json.loads(json.dumps(prev_content))
    design_overrides = json.loads(json.dumps(prev_design))

    content_overrides.setdefault(item_id, {})["question_text"] = question_text

    if image_file is not None and image_file.filename:
        file_bytes = await image_file.read()
        if file_bytes and image_field in ("excerpt_image_path", "reference_image_path"):
            asset = ps.add_asset(project_id, file_bytes, image_file.filename)
            content_overrides[item_id][image_field] = f"asset:{asset['asset_id']}{asset['ext']}"

    if answer_height_mm.strip():
        try:
            h = float(answer_height_mm)
            design_overrides.setdefault(item_id, {})["answer_height_mm"] = max(15, h)
        except ValueError:
            pass

    ps.save_version(project_id, content_overrides, design_overrides, note=f"item {item_id} 수정")
    _run_pipeline(project_id)
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}?item_id={item_id}", status_code=303)


@router.post("/{project_id}/restore/{version_id}")
def editor_restore(project_id: str, version_id: str):
    try:
        ps.restore_version(project_id, version_id)
    except ps.ProjectError as e:
        raise HTTPException(status_code=404, detail=str(e))
    _run_pipeline(project_id)
    return RedirectResponse(url=f"/momo-worksheet-editor/{project_id}", status_code=303)


@router.get("/{project_id}/export")
def editor_export(project_id: str):
    try:
        ps.load_project(project_id)
    except ps.ProjectError as e:
        raise HTTPException(status_code=404, detail=str(e))
    out_dir = os.path.join(_MOMO_DIR, "worksheet", "edit_projects_exports")
    os.makedirs(out_dir, exist_ok=True)
    out_zip = os.path.join(out_dir, f"{project_id}.zip")
    ps.export_project(project_id, out_zip)
    return FileResponse(out_zip, filename=f"{project_id}.zip", media_type="application/zip")


@router.post("/import")
async def editor_import(request: Request, zip_file: UploadFile = File(...)):
    tmp_dir = os.path.join(_MOMO_DIR, "worksheet", "edit_projects_imports")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, zip_file.filename)
    with open(tmp_path, "wb") as f:
        f.write(await zip_file.read())
    try:
        new_id = ps.import_project(tmp_path)
    except ps.ProjectError as e:
        # 손상되거나 자산이 빠진 프로젝트 - 오류만 보여주고 기존 프로젝트들은 전혀 안 건드림
        # (import_project 자체가 임시 폴더에서 작업하다 실패하면 그 임시 폴더만 지움).
        return templates.TemplateResponse("momo_worksheet_editor/import_error.html", {
            "request": request, "error": str(e),
        }, status_code=400)
    finally:
        if os.path.isfile(tmp_path):
            os.remove(tmp_path)
    return RedirectResponse(url=f"/momo-worksheet-editor/{new_id}", status_code=303)
