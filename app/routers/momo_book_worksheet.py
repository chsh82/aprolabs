# -*- coding: utf-8 -*-
"""
모모의책장 교재DB(momo_book_db/momo_book.db) -> 학습지 자동 생성 미리보기 화면.
momo_book_review.py와 같은 패턴(sqlite3 직접 접근, app.database 안 씀)을 따른다.

파이프라인은 momo_book_db/generate/extract_worksheet_json.py(파이썬, DB->JSON) +
momo_book_db/worksheet/scripts/generate.js(Node+Playwright, JSON->HTML)를 그대로
재사용한다 - 이 라우터는 그 두 단계 + QA(check.js)까지 웹에서 버튼 하나로 돌리고
결과를 iframe으로 보여주는 얇은 래퍼일 뿐, 생성·검사 로직 자체는 건드리지 않는다.

2026-09-18 1차 안정화: "마지막 정상 생성본 보존" 도입.
- generate.js가 매 빌드를 generated/<doc_id>/builds/<build_id>/에 독립적으로 쓴다
  (기존 index.html을 바로 덮어쓰지 않음).
- 이 라우터가 그 빌드에 대해 check.js를 돌리고, PASS일 때만
  generated/<doc_id>/current.json(빌드 id 포인터)을 새 빌드로 교체한다.
  FAIL/BLOCKED면 direct.json을 안 건드려서 이전 정상본이 계속 보인다.
- 문서당 in-process 락으로 동시 빌드를 막는다(같은 doc_id 두 번 겹치면 뒤엣것은
  즉시 "이미 빌드 중" 실패로 반환 - 대기열 없이 단순하게).

GET  /momo-worksheet             -> 승인된 문서 목록(정상본 존재 여부/최근 승격 시각 표시)
GET  /momo-worksheet/{doc_id}    -> 미리보기(정상본 있으면 iframe) + 생성 버튼 + 최근 빌드 상태
POST /momo-worksheet/{doc_id}/build -> extract -> generate.js -> check.js -> (PASS시만) 승격
"""
import json
import os
import subprocess
import sys
import sqlite3
import threading
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

_GENERATE_TIMEOUT_SEC = 180  # Chromium 기동 + 문서 전체 반면 실측 - 넉넉히
_CHECK_TIMEOUT_SEC = 120     # Chromium + PDF 생성 + PyMuPDF 서브프로세스

# 문서별 동시 빌드 방지용 락. 프로세스 하나 안에서만 유효(여러 워커 프로세스로
# 띄우면 별도 보장이 더 필요하지만, 지금 배포 형태는 단일 프로세스라 이걸로 충분).
_build_locks_guard = threading.Lock()
_build_locks = {}


def _lock_for(doc_id):
    with _build_locks_guard:
        lock = _build_locks.get(doc_id)
        if lock is None:
            lock = threading.Lock()
            _build_locks[doc_id] = lock
        return lock


def _db():
    # 읽기전용 - 이 라우터는 documents 테이블에 절대 쓰지 않는다.
    conn = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _current_build(doc_id):
    """generated/<doc_id>/current.json을 읽어 지금 "정상본"으로 보여줄 build_id를 돌려준다.
    없으면 None(아직 한 번도 승격된 빌드가 없음 - 최초 생성 전이거나, 모든 시도가 FAIL)."""
    p = os.path.join(_GENERATED_DIR, doc_id, "current.json")
    if not os.path.isfile(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _preview_paths(doc_id, build_id):
    return {
        "index": os.path.join(_GENERATED_DIR, doc_id, "builds", build_id, "index.html"),
        "url": f"/momo-worksheet-assets/generated/{doc_id}/builds/{build_id}/index.html",
    }


@router.get("", response_class=HTMLResponse)
def momo_worksheet_list(request: Request):
    """승인된 문서만 목록에 올림 - 생성기가 검수 안 된 데이터(오탈자·빈 ui_config 등)를
    그대로 조판하면 결과가 의미 없어서, 검수 완료된 문서로 범위를 좁혀 둔다."""
    with _db() as conn:
        docs = conn.execute(
            "SELECT doc_id, level, quarter, week, book_title, book_author "
            "FROM documents WHERE review_status = 'approved' "
            "ORDER BY level, quarter, week"
        ).fetchall()

    rows = []
    for d in docs:
        current = _current_build(d["doc_id"])
        rows.append({**dict(d), "built_at": current["promoted_at"] if current else None})

    return templates.TemplateResponse("momo_worksheet/list.html", {
        "request": request, "docs": rows,
    })


@router.get("/{doc_id}", response_class=HTMLResponse)
def momo_worksheet_detail(request: Request, doc_id: str):
    with _db() as conn:
        doc = conn.execute("SELECT * FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")

    current = _current_build(doc_id)
    exists = current is not None
    preview_url = _preview_paths(doc_id, current["build_id"])["url"] if exists else None

    latest_log = _latest_build_log(doc_id)

    return templates.TemplateResponse("momo_worksheet/detail.html", {
        "request": request, "doc": doc, "exists": exists,
        "built_at": current["promoted_at"] if exists else None,
        "preview_url": preview_url, "log": latest_log["text"] if latest_log else "",
        "latest_status": latest_log["status"] if latest_log else None,
        "showing_previous": bool(latest_log and latest_log["status"] != "PASS" and exists),
    })


def _latest_build_log(doc_id):
    """빌드 시도 중 가장 최근 것(성공/실패 무관)의 로그·상태. "최근 빌드 실패"와
    "이전 정상본 표시 중"을 화면에서 구분하는 데 쓴다."""
    builds_dir = os.path.join(_GENERATED_DIR, doc_id, "builds")
    if not os.path.isdir(builds_dir):
        return None
    build_ids = sorted(os.listdir(builds_dir), reverse=True)
    if not build_ids:
        return None
    latest = build_ids[0]
    log_path = os.path.join(builds_dir, latest, "build.log")
    manifest_path = os.path.join(builds_dir, latest, "manifest.json")
    text = ""
    if os.path.isfile(log_path):
        with open(log_path, encoding="utf-8") as f:
            text = f.read()
    status = None
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path, encoding="utf-8") as f:
                status = json.load(f).get("status")
        except (json.JSONDecodeError, OSError):
            pass
    return {"text": text, "status": status, "build_id": latest}


@router.post("/{doc_id}/build")
def momo_worksheet_build(doc_id: str):
    """extract_worksheet_json(파이썬, 같은 프로세스) -> generate.js(Node/Playwright,
    서브프로세스) -> check.js(Node/Playwright+PyMuPDF, 서브프로세스) 순서로 돌린다.
    check.js가 PASS를 돌려줄 때만 current.json을 이 빌드로 교체(승격)한다."""
    with _db() as conn:
        doc = conn.execute("SELECT doc_id FROM documents WHERE doc_id = ?", (doc_id,)).fetchone()
    if not doc:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")

    lock = _lock_for(doc_id)
    if not lock.acquire(blocking=False):
        # 대기열 없이 단순 거절 - "같은 문서의 동시 빌드 충돌을 막는다"의 최소 구현.
        return RedirectResponse(url=f"/momo-worksheet/{doc_id}?build_busy=1", status_code=303)

    try:
        _run_build(doc_id)
    finally:
        lock.release()

    return RedirectResponse(url=f"/momo-worksheet/{doc_id}", status_code=303)


def _run_build(doc_id):
    log_lines = [f"=== 빌드 시작 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==="]
    build_id = None
    status = "FAIL"

    try:
        generate_dir = os.path.join(_MOMO_DIR, "generate")
        if generate_dir not in sys.path:
            sys.path.insert(0, generate_dir)
        import extract_worksheet_json  # momo_book_db/generate/extract_worksheet_json.py

        data = extract_worksheet_json.extract(doc_id)
        data_path = os.path.join(_GENERATED_DIR, doc_id, "data.json")
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log_lines.append(f"[1/3] data.json 작성 완료 (vocab {len(data['step1']['vocab'])}건 / "
                          f"ox {len(data['step1']['ox'])}건 / discussion_qa {len(data['step2'])}건 / "
                          f"입력검증 issues {len(data['issues'])}건)")
    except Exception as e:
        log_lines.append(f"[1/3] 실패: {e!r}")
        _write_status_only(doc_id, log_lines, status="FAIL")
        return

    try:
        result = subprocess.run(
            ["node", "generate.js", doc_id],
            cwd=_WORKSHEET_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8",
            timeout=_GENERATE_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        log_lines.append(f"[2/3] generate.js 타임아웃({_GENERATE_TIMEOUT_SEC}초) - 이전 정상본 유지")
        _write_status_only(doc_id, log_lines, status="FAIL")
        return

    log_lines.append("[2/3] node generate.js 출력:")
    log_lines.append(result.stdout)
    if result.stderr:
        log_lines.append("--- stderr ---")
        log_lines.append(result.stderr)

    if result.returncode != 0:
        log_lines.append(f"[2/3] 실패 (exit {result.returncode}) - 이전 정상본 유지")
        _write_status_only(doc_id, log_lines, status="FAIL")
        return

    for line in result.stdout.splitlines():
        if line.startswith("BUILD_DIR="):
            build_id = os.path.basename(line[len("BUILD_DIR="):].strip())
    if not build_id:
        log_lines.append("[2/3] BUILD_DIR을 출력에서 못 찾음 - 이전 정상본 유지")
        _write_status_only(doc_id, log_lines, status="FAIL")
        return

    build_dir = os.path.join(_GENERATED_DIR, doc_id, "builds", build_id)
    index_path = os.path.join(build_dir, "index.html")
    data_copy_path = os.path.join(build_dir, "data.json")
    report_path = os.path.join(build_dir, "check_report.json")

    try:
        check_result = subprocess.run(
            ["node", "check.js", index_path, data_copy_path, report_path],
            cwd=_WORKSHEET_SCRIPTS_DIR, capture_output=True, text=True, encoding="utf-8",
            timeout=_CHECK_TIMEOUT_SEC,
        )
        log_lines.append("[3/3] node check.js 출력:")
        log_lines.append(check_result.stdout)
        if check_result.stderr:
            log_lines.append("--- stderr ---")
            log_lines.append(check_result.stderr)
    except subprocess.TimeoutExpired:
        log_lines.append(f"[3/3] check.js 타임아웃({_CHECK_TIMEOUT_SEC}초) - BLOCKED로 처리, 이전 정상본 유지")
        _finalize_build(doc_id, build_id, log_lines, status="BLOCKED")
        return

    qa_status = "FAIL"
    if os.path.isfile(report_path):
        try:
            with open(report_path, encoding="utf-8") as f:
                qa_status = json.load(f).get("status", "FAIL")
        except (json.JSONDecodeError, OSError):
            qa_status = "FAIL"

    log_lines.append(f"[3/3] QA 결과: {qa_status}")
    _finalize_build(doc_id, build_id, log_lines, status=qa_status)

    if qa_status == "PASS":
        _promote(doc_id, build_id)
        log_lines.append(f"=== 승격 완료: current.json -> {build_id} ===")
    else:
        log_lines.append("=== 승격 안 함(QA 미통과) - 이전 정상본 유지 ===")

    _write_build_log(doc_id, build_id, log_lines)


def _write_status_only(doc_id, log_lines, status):
    """generate.js가 build_id를 아예 만들기 전에 실패한 경우 - build_id 없이
    doc 레벨 로그만 남긴다(승격 대상 자체가 없으므로 current.json은 그대로 둔다)."""
    fallback_dir = os.path.join(_GENERATED_DIR, doc_id, "builds", datetime.now().strftime("%Y-%m-%dT%H-%M-%S-failed"))
    os.makedirs(fallback_dir, exist_ok=True)
    with open(os.path.join(fallback_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({"status": status, "doc_id": doc_id, "created_at": datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(fallback_dir, "build.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines) + "\n")


def _finalize_build(doc_id, build_id, log_lines, status):
    build_dir = os.path.join(_GENERATED_DIR, doc_id, "builds", build_id)
    manifest_path = os.path.join(build_dir, "manifest.json")
    manifest = {}
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path, encoding="utf-8") as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, OSError):
            manifest = {}
    manifest["status"] = status
    manifest["checked_at"] = datetime.now().isoformat()
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    _write_build_log(doc_id, build_id, log_lines)


def _write_build_log(doc_id, build_id, log_lines):
    build_dir = os.path.join(_GENERATED_DIR, doc_id, "builds", build_id)
    os.makedirs(build_dir, exist_ok=True)
    with open(os.path.join(build_dir, "build.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines) + "\n")


def _promote(doc_id, build_id):
    """current.json을 원자적으로 교체한다(임시 파일에 쓰고 os.replace) - 쓰는 도중
    읽는 요청이 절반만 쓰인 파일을 보는 일이 없게 한다."""
    current_path = os.path.join(_GENERATED_DIR, doc_id, "current.json")
    tmp_path = current_path + ".tmp"
    payload = {"build_id": build_id, "promoted_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, current_path)
