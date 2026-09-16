# -*- coding: utf-8 -*-
"""
모모의책장 교재DB(momo_book_db/momo_book.db) -> 학습지 자동 생성 미리보기 화면.
momo_book_review.py와 같은 패턴(sqlite3 직접 접근, app.database 안 씀)을 따른다.

파이프라인은 momo_book_db/generate/extract_worksheet_json.py(파이썬, DB->JSON) +
momo_book_db/worksheet/scripts/generate.js(Node+Playwright, JSON->HTML)로 이미
파일럿 완료된 것을 그대로 재사용한다 - 이 라우터는 그 두 단계를 웹에서 버튼 하나로
돌리고 결과를 iframe으로 보여주는 얇은 래퍼일 뿐, 생성 로직 자체는 건드리지 않는다.

GET  /momo-worksheet             -> 승인된 문서 목록(생성 여부/최근 빌드 시각 표시)
GET  /momo-worksheet/{doc_id}    -> 미리보기(있으면 iframe) + 생성 버튼 + 빌드 로그
POST /momo-worksheet/{doc_id}/build -> extract_worksheet_json.py + generate.js 실행 후 상세로 리다이렉트
"""
import os
import subprocess
import sys
import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/momo-worksheet")
templates = Jinja2Templates(directory="app/templates")

_MOMO_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "momo_book_db")
_DB_PATH = os.path.join(_MOMO_DIR, "momo_book.db")
_GENERATED_DIR = os.path.join(_MOMO_DIR, "generated")
_WORKSHEET_SCRIPTS_DIR = os.path.join(_MOMO_DIR, "worksheet", "scripts")


def _db():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _preview_paths(doc_id):
    doc_dir = os.path.join(_GENERATED_DIR, doc_id)
    return {
        "index": os.path.join(doc_dir, "index.html"),
        "log": os.path.join(doc_dir, "build.log"),
        "url": f"/momo-worksheet-assets/generated/{doc_id}/index.html",
    }


@router.get("", response_class=HTMLResponse)
def momo_worksheet_list(request: Request):
    """승인된 문서만 목록에 올림 - 생성기가 검수 안 된 데이터(오탈자·빈 ui_config 등)를
    그대로 조판하면 결과가 의미 없어서, 검수 완료된 문서로 범위를 좁혀 둔다."""
    conn = _db()
    docs = conn.execute(
        "SELECT doc_id, level, quarter, week, book_title, book_author "
        "FROM documents WHERE review_status = 'approved' "
        "ORDER BY level, quarter, week"
    ).fetchall()
    conn.close()

    rows = []
    for d in docs:
        paths = _preview_paths(d["doc_id"])
        built_at = None
        if os.path.isfile(paths["index"]):
            built_at = datetime.fromtimestamp(os.path.getmtime(paths["index"])).strftime("%Y-%m-%d %H:%M")
        rows.append({**dict(d), "built_at": built_at})

    return templates.TemplateResponse("momo_worksheet/list.html", {
        "request": request, "docs": rows,
    })


@router.get("/{doc_id}", response_class=HTMLResponse)
def momo_worksheet_detail(request: Request, doc_id: str):
    conn = _db()
    doc = conn.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
    conn.close()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")

    paths = _preview_paths(doc_id)
    exists = os.path.isfile(paths["index"])
    built_at = (datetime.fromtimestamp(os.path.getmtime(paths["index"])).strftime("%Y-%m-%d %H:%M")
                if exists else None)
    log = ""
    if os.path.isfile(paths["log"]):
        with open(paths["log"], encoding="utf-8") as f:
            log = f.read()

    return templates.TemplateResponse("momo_worksheet/detail.html", {
        "request": request, "doc": doc, "exists": exists, "built_at": built_at,
        "preview_url": paths["url"], "log": log,
    })


@router.post("/{doc_id}/build")
def momo_worksheet_build(doc_id: str):
    """DB -> JSON(파이썬, 같은 프로세스에서 직접 호출) -> HTML(Node/Playwright, 서브프로세스)
    순서로 돌린다. 둘 다 momo_book_db/generate·worksheet의 기존 파일럿 스크립트 그대로다."""
    conn = _db()
    doc = conn.execute("SELECT doc_id FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
    conn.close()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")

    paths = _preview_paths(doc_id)
    os.makedirs(os.path.dirname(paths["index"]), exist_ok=True)
    log_lines = [f"=== 빌드 시작 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==="]

    generate_dir = os.path.join(_MOMO_DIR, "generate")
    if generate_dir not in sys.path:
        sys.path.insert(0, generate_dir)
    import extract_worksheet_json  # momo_book_db/generate/extract_worksheet_json.py

    try:
        data = extract_worksheet_json.extract(doc_id)
        data_path = os.path.join(_GENERATED_DIR, doc_id, "data.json")
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        import json
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log_lines.append(f"[1/2] data.json 작성 완료 (vocab {len(data['step1']['vocab'])}건 / "
                          f"ox {len(data['step1']['ox'])}건 / discussion_qa {len(data['step2'])}건)")
    except Exception as e:
        log_lines.append(f"[1/2] 실패: {e!r}")
        _write_log(paths["log"], log_lines)
        return RedirectResponse(url=f"/momo-worksheet/{doc_id}", status_code=303)

    result = subprocess.run(
        ["node", "generate.js", doc_id],
        cwd=_WORKSHEET_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8",
    )
    log_lines.append("[2/2] node generate.js 출력:")
    log_lines.append(result.stdout)
    if result.stderr:
        log_lines.append("--- stderr ---")
        log_lines.append(result.stderr)
    log_lines.append("성공" if result.returncode == 0 else f"실패 (exit {result.returncode})")

    _write_log(paths["log"], log_lines)
    return RedirectResponse(url=f"/momo-worksheet/{doc_id}", status_code=303)


def _write_log(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
