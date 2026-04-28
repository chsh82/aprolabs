"""
수능국어DB 파이프라인 라우터
/suneung/upload   → PDF 업로드
/suneung/jobs     → 작업 목록
/suneung/warnings → 전체 경고 목록 (카테고리별 검수)
/suneung/review/{job_id} → 검수 UI
/suneung/review/{job_id}/approve → DB 확정 저장
/suneung/review/{job_id}/warning-judgment → 경고 판정 저장
"""
import os
import re
import uuid
import aiofiles
from typing import List
from fastapi import APIRouter, Depends, Request, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.question import Question, CATEGORIES, SUBCATEGORIES
from app.models.passage import Passage, PipelineJob, ExamPaper
from app.services.pdf_parser import pdf_to_images
from app.services.layout_analyzer import extract_pdf_text, is_digital_pdf, ocr_all_pages
from app.services.segmenter import segment_text, attach_passage_idx
from app.services.tagger import tag_all
from app.services.vision_analyzer import analyze_all_pages, apply_structure_to_text

router = APIRouter(prefix="/suneung")
templates = Jinja2Templates(directory="app/templates")
UPLOAD_DIR = "uploads/suneung"
os.makedirs(UPLOAD_DIR, exist_ok=True)

_BRACKET_RENDER_RE = re.compile(r'\[([A-Z]):START\](.*?)\[([A-Z]):END\]', re.DOTALL)

# <img> 태그 내 줄바꿈 정규화
_IMG_NORMALIZE_RE = re.compile(r'<img\b[\s\S]*?>', re.IGNORECASE)

# 허용할 태그/속성 화이트리스트
_ALLOWED_TAGS = {'img', 'b', 'i', 'u', 'br', 'span', 'div', 'p', 'sub', 'sup'}
_ALLOWED_ATTRS = {
    'img':  {'src', 'alt', 'style', 'width', 'height'},
    'span': {'style', 'class'},
    'div':  {'style', 'class'},
    'p':    {'style'},
}
_SAFE_STYLE_RE = re.compile(
    r'^(text-align:(left|center|right)'
    r'|max-width:[^;]{0,30}'
    r'|display:(block|inline|inline-block)'
    r'|margin:[^;]{0,20}'
    r'|padding:[^;]{0,20}'
    r'|border-left:[^;]{0,40}'
    r'|background:[^;]{0,30}'
    r'|border-radius:[^;]{0,20}'
    r'|white-space:[^;]{0,20}'
    r'|color:[^;]{0,20}'
    r'|font-[^:]{0,20}:[^;]{0,20}'
    r')(;\s*|$)',
    re.IGNORECASE,
)
_TAG_RE = re.compile(r'<(/?)(\w+)([^>]*)>', re.DOTALL)
_ATTR_RE = re.compile(r'(\w[\w-]*)(?:\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|(\S+)))?')


def _safe_style(style_str: str) -> str:
    """style 속성에서 안전한 선언만 유지"""
    parts = [s.strip() for s in style_str.split(';') if s.strip()]
    safe = []
    for p in parts:
        if _SAFE_STYLE_RE.match(p + ';'):
            safe.append(p)
    return '; '.join(safe)


def _sanitize_html(text: str) -> str:
    """<img> 등 허용 태그는 유지하고 <script> 등 위험 태그는 제거"""
    if not text:
        return text

    # 먼저 img 태그 내 줄바꿈 정규화
    text = _IMG_NORMALIZE_RE.sub(lambda m: m.group(0).replace('\n', ' '), text)

    def handle_tag(m: re.Match) -> str:
        closing = m.group(1)   # '/' if closing tag
        tag = m.group(2).lower()
        attrs_raw = m.group(3)

        if tag not in _ALLOWED_TAGS:
            return ''          # 위험 태그 제거

        if closing:
            return f'</{tag}>'

        # self-closing 태그 (img, br)
        if tag in ('img', 'br'):
            allowed_attr_names = _ALLOWED_ATTRS.get(tag, set())
            safe_attrs = []
            for am in _ATTR_RE.finditer(attrs_raw):
                name = am.group(1).lower()
                val = am.group(2) or am.group(3) or am.group(4) or ''
                if name not in allowed_attr_names:
                    continue
                if name == 'style':
                    val = _safe_style(val)
                # src: 허용 경로만 (상대경로 or /uploads/)
                if name == 'src' and not (val.startswith('/uploads/') or val.startswith('http')):
                    continue
                safe_attrs.append(f'{name}="{val}"')
            return f'<{tag} {" ".join(safe_attrs)}>' if safe_attrs else f'<{tag}>'

        # 일반 태그
        allowed_attr_names = _ALLOWED_ATTRS.get(tag, set())
        safe_attrs = []
        for am in _ATTR_RE.finditer(attrs_raw):
            name = am.group(1).lower()
            val = am.group(2) or am.group(3) or am.group(4) or ''
            if name not in allowed_attr_names:
                continue
            if name == 'style':
                val = _safe_style(val)
            safe_attrs.append(f'{name}="{val}"')
        return f'<{tag} {" ".join(safe_attrs)}>' if safe_attrs else f'<{tag}>'

    return _TAG_RE.sub(handle_tag, text)


def _render_brackets(text: str) -> str:
    """[A:START]...[A:END] 마커를 시각적 HTML 블록으로 변환 + HTML sanitize"""
    if not text:
        return text

    # 허용 태그 보존 후 sanitize
    text = _sanitize_html(text)

    def replace(m):
        label = m.group(1)
        content = m.group(2).strip('\n')
        return (
            f'<span class="inline-block text-xs font-bold text-blue-600 mr-1">[{label}]</span>'
            f'<span style="border-left: 3px solid #3b82f6; padding-left: 8px; '
            f'display: block; margin: 4px 0; background: #eff6ff; '
            f'border-radius: 0 4px 4px 0; white-space: pre-wrap;">{content}</span>'
        )

    return _BRACKET_RENDER_RE.sub(replace, text)


templates.env.filters["render_brackets"] = _render_brackets


# ─────────────────────────────────────────
# 정답/해설 파싱
# ─────────────────────────────────────────

def _parse_answer_key(pdf_path: str) -> dict:
    """
    정답/해설 PDF에서 문항번호 → {answer, explanation} 추출.
    반환: {num(int): {"answer": "③", "explanation": str|None}}
    """
    try:
        raw_text, _, _ = extract_pdf_text(pdf_path)
    except Exception:
        return {}

    result = {}

    # 줄 단위로 분석
    # 패턴: "1. ③", "1번 ③", "1) ③", "1 - ③", "① 1번" 등
    # 정답 원문자: ①②③④⑤, 숫자: 1~5
    ANS_RE = re.compile(
        r'^[\s]*(\d{1,2})\s*[번\.\)\-\s]\s*([①②③④⑤])',
        re.MULTILINE,
    )
    # 정답만 있는 표 형식 (줄에 원문자들만 나열)
    TABLE_RE = re.compile(r'([①②③④⑤])')

    matches = list(ANS_RE.finditer(raw_text))

    if matches:
        for i, m in enumerate(matches):
            num = int(m.group(1))
            ans = m.group(2)
            if not (1 <= num <= 45):
                continue
            if num in result:
                continue

            # 해설: 이 정답과 다음 정답 사이 텍스트
            exp_start = m.end()
            exp_end = matches[i + 1].start() if i + 1 < len(matches) else len(raw_text)
            exp_raw = raw_text[exp_start:exp_end].strip()
            exp_clean = re.sub(r'\s+', ' ', exp_raw).strip()

            result[num] = {
                "answer": ans,
                "explanation": exp_clean if len(exp_clean) > 20 else None,
            }
    else:
        # 폴백: 원문자 순서로 1번부터 매핑 (정답표 형식)
        circles = TABLE_RE.findall(raw_text[:500])
        for i, c in enumerate(circles, 1):
            if i <= 45:
                result[i] = {"answer": c, "explanation": None}

    return result


def _base_ctx(db: Session, **kwargs):
    counts = {}
    for cat in CATEGORIES:
        counts[cat] = db.query(Question).filter(Question.category == cat).count()
    counts["전체"] = db.query(Question).count()
    sub_counts = {}
    for cat, subs in SUBCATEGORIES.items():
        for sub in subs:
            sub_counts[f"{cat}:{sub}"] = (
                db.query(Question)
                .filter(Question.category == cat, Question.subcategory == sub)
                .count()
            )
    return {
        "categories": CATEGORIES,
        "subcategories": SUBCATEGORIES,
        "category_counts": counts,
        "sub_counts": sub_counts,
        **kwargs,
    }


# ─────────────────────────────────────────
# 업로드 페이지
# ─────────────────────────────────────────
@router.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, db: Session = Depends(get_db)):
    ctx = _base_ctx(db, request=request)
    return templates.TemplateResponse("suneung/upload.html", ctx)


@router.post("/upload")
async def upload_pdf(
    background_tasks: BackgroundTasks,
    request: Request,
    db: Session = Depends(get_db),
    files: List[UploadFile] = File(...),
    answer_file: UploadFile = File(None),
    source: str = Form(""),
    source_year: str = Form(""),
    exam_type: str = Form("수능"),
    subject: str = Form("국어"),
    grade: str = Form(""),
):
    if not files:
        return RedirectResponse(url="/suneung/upload", status_code=303)

    # 정답/해설 파일 저장 (선택, 모든 job에 공유)
    answer_path = None
    if answer_file and answer_file.filename:
        ans_ext = os.path.splitext(answer_file.filename)[1].lower()
        shared_ans_id = str(uuid.uuid4())
        answer_path = os.path.join(UPLOAD_DIR, f"{shared_ans_id}_answer{ans_ext}")
        async with aiofiles.open(answer_path, "wb") as f:
            await f.write(await answer_file.read())

    for file in files[:100]:  # 최대 100개
        if not file.filename:
            continue

        job_id = str(uuid.uuid4())
        ext = os.path.splitext(file.filename)[1].lower()
        pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}{ext}")

        async with aiofiles.open(pdf_path, "wb") as f:
            await f.write(await file.read())

        from sqlalchemy import func
        from app.routers.crawl import _extract_grade, _extract_sub_type
        max_num = db.query(func.max(PipelineJob.job_number)).scalar() or 0

        # 학년 자동 추출: 폼 입력 → 파일명 → 시험유형 규칙
        resolved_grade = (
            grade.strip()
            or _extract_grade(file.filename, exam_type)
            or _extract_grade("", exam_type)
            or None
        )

        # sub_type 자동 설정: 파일명에서 추출, 국어이면서 비어있으면 "통합"
        resolved_sub_type = _extract_sub_type(file.filename)
        if not resolved_sub_type and subject == "국어":
            resolved_sub_type = "통합"

        job = PipelineJob(
            id=job_id,
            job_number=max_num + 1,
            filename=file.filename,
            file_path=pdf_path,
            source=source or None,
            source_year=int(source_year) if source_year.isdigit() else None,
            exam_type=exam_type,
            subject=subject,
            sub_type=resolved_sub_type or None,
            grade=resolved_grade,
            answer_file_path=answer_path,
            status="parsing",
        )
        db.add(job)
        db.commit()

        background_tasks.add_task(run_pipeline, job_id, pdf_path)

    return RedirectResponse(url="/suneung/jobs", status_code=303)


# ─────────────────────────────────────────
# 파이프라인 실행 (백그라운드)
# ─────────────────────────────────────────
def run_pipeline(job_id: str, pdf_path: str):
    """PDF → 텍스트 추출 → 세그멘테이션 → 태깅"""
    USE_ANTHROPIC = bool(os.getenv("ANTHROPIC_API_KEY")) and \
                    os.getenv("ENABLE_ANTHROPIC", "false").lower() == "true"

    from app.database import SessionLocal

    db = SessionLocal()
    try:
        job = db.get(PipelineJob, job_id)
        if not job:
            return

        # Phase 1: 텍스트 추출 (디지털 PDF 직접 추출 우선)
        job.status = "analyzing"
        db.commit()

        img_dir = os.path.join(UPLOAD_DIR, job_id, "images")
        raw_text, num_pages, manifest = extract_pdf_text(pdf_path, img_save_dir=img_dir)

        page_images = []
        if is_digital_pdf(raw_text, num_pages):
            # 디지털 PDF: 텍스트 추출 성공, 구조 분석용 페이지 이미지 렌더링
            job.status = f"analyzing (digital {num_pages}p)"
            db.commit()
            page_dir = os.path.join(UPLOAD_DIR, job_id, "pages")
            page_images = pdf_to_images(pdf_path, page_dir, dpi=120)
        else:
            # 스캔 PDF: Gemini OCR fallback
            scan_dir = os.path.join(UPLOAD_DIR, job_id, "pages")
            page_images = pdf_to_images(pdf_path, scan_dir)
            job.page_image_paths = page_images
            job.status = f"analyzing (ocr 0/{len(page_images)})"
            db.commit()
            raw_text, _ = ocr_all_pages(page_images, job_id=job_id)

        # Phase 1b: Gemini Vision 구조 분석 (문항 위치, [A] 범위, 밑줄)
        job.status = "vision (0/{})" .format(len(page_images))
        db.commit()
        structure = analyze_all_pages(page_images, job_id=job_id)

        # Phase 1c: 구조 정보를 텍스트에 적용 ([A] 범위, 밑줄 보완)
        raw_text = apply_structure_to_text(raw_text, structure)

        # Phase 2: 구조 파싱 (regex + Gemini 힌트)
        job.status = "segmenting"
        db.commit()

        segments = segment_text(raw_text, job_id=job_id,
                                question_hints=structure.get("questions", []))
        passages_data = segments.get("passages", [])
        questions_data = segments.get("questions", [])
        attach_passage_idx(passages_data, questions_data)

        # Phase 3: 자동 QA
        job.status = "qa_checking"
        db.commit()

        from app.services.qa_validator import validate_segments as _validate_segs
        from app.services.auto_patcher import auto_patch as _auto_patch_segs
        qa_result = _validate_segs({"questions": questions_data}, pdf_path)

        # Phase 4: 자동 패치
        qa_patch_result = None
        if qa_result["issues"]:
            qa_patch_result = _auto_patch_segs(
                {"questions": questions_data},
                qa_result["issues"],
                pdf_path,
            )
            _patched_qs = qa_patch_result["segments"].get("questions", [])
            if isinstance(_patched_qs, str):
                import json as _qjson
                _patched_qs = _qjson.loads(_patched_qs)
            questions_data = _patched_qs
            # Phase 5: 재검증
            qa_result = _validate_segs({"questions": questions_data}, pdf_path)

        job.raw_result = {
            "passages": passages_data,
            "questions": questions_data,
            "ocr_text": raw_text[:3000],
        }
        job.status = "tagging"
        db.commit()

        # Phase 3: 사고력 태깅 (Claude — ENABLE_ANTHROPIC=true 일 때만)
        if USE_ANTHROPIC:
            try:
                tag_all(passages_data, questions_data, job_id=job_id)
            except Exception:
                pass

        # Phase 3.5: verify_agent — ENABLE_ANTHROPIC=true 일 때만
        verify_corrections = []
        if USE_ANTHROPIC:
            try:
                import sys, pathlib
                sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
                from verify_agent import run as verify_run

                job.status = "verifying"
                db.commit()

                result = verify_run(
                    pdf_path=pdf_path,
                    segments_json={"passages": passages_data, "questions": questions_data},
                    dpi=120,
                    verbose=False,
                )

                for i, corrected_p in enumerate(result.segments.passages):
                    if i < len(passages_data):
                        passages_data[i]["content"] = corrected_p.content

                verify_corrections = [
                    {"kind": c.kind, "location": c.location, "message": c.message}
                    for c in result.corrections
                ]
            except Exception as ve:
                import traceback
                verify_corrections = [{
                    "kind": "error",
                    "location": "verify_agent",
                    "message": f"{ve}\n{traceback.format_exc()[-300:]}",
                }]

        # Phase 4: 정답/해설 파싱 (파일이 있는 경우)
        if job.answer_file_path and os.path.exists(job.answer_file_path):
            job.status = "parsing answers"
            db.commit()
            answer_key = _parse_answer_key(job.answer_file_path)
            for q in questions_data:
                num = q.get("number")
                if num and num in answer_key:
                    q["answer"] = answer_key[num]["answer"]
                    if answer_key[num].get("explanation"):
                        q["explanation"] = answer_key[num]["explanation"]

        job.segments = {"passages": passages_data, "questions": questions_data}
        job.raw_result = {
            **(job.raw_result or {}),
            "verify_corrections": verify_corrections,
            "qa_report": {
                "passed": qa_result["passed"],
                "stats": qa_result["stats"],
                "issues": qa_result["issues"],
                "patch_log": qa_patch_result["segments"].get("_patch_log", []) if qa_patch_result else [],
            },
        }
        job.status = "reviewing"
        if qa_patch_result and qa_patch_result["patched"]:
            job.status = "reviewing (auto-patched)"
        if not qa_result["passed"]:
            job.status = "needs_attention"
        db.commit()

    except Exception as e:
        import traceback
        db2 = SessionLocal()
        job2 = db2.get(PipelineJob, job_id)
        if job2:
            job2.status = "error"
            job2.error_message = f"{e}\n{traceback.format_exc()[-500:]}"
            db2.commit()
        db2.close()
    finally:
        db.close()


# ─────────────────────────────────────────
# 작업 목록
# ─────────────────────────────────────────
@router.get("/jobs", response_class=HTMLResponse)
def jobs_list(request: Request, db: Session = Depends(get_db)):
    jobs = db.query(PipelineJob).order_by(PipelineJob.created_at.desc()).all()
    ctx = _base_ctx(db, request=request, jobs=jobs)
    return templates.TemplateResponse("suneung/jobs.html", ctx)


@router.get("/jobs/status/{job_id}")
def job_status(job_id: str, db: Session = Depends(get_db)):
    job = db.get(PipelineJob, job_id)
    if not job:
        return JSONResponse({"status": "not_found"})
    return JSONResponse({
        "status": job.status,
        "error": job.error_message,
    })


@router.post("/jobs/{job_id}/delete")
def delete_job(job_id: str, db: Session = Depends(get_db)):
    """작업 삭제"""
    import shutil
    job = db.get(PipelineJob, job_id)
    if job:
        # 업로드된 파일 및 이미지 삭제
        if job.file_path and os.path.exists(job.file_path):
            os.remove(job.file_path)
        img_dir = os.path.join(UPLOAD_DIR, job_id)
        if os.path.exists(img_dir):
            shutil.rmtree(img_dir)
        db.delete(job)
        db.commit()
    return RedirectResponse(url="/suneung/jobs", status_code=303)


@router.post("/jobs/{job_id}/start")
def start_job(job_id: str, background_tasks: BackgroundTasks,
              db: Session = Depends(get_db)):
    """ready 상태 작업을 파이프라인 시작"""
    job = db.get(PipelineJob, job_id)
    if not job or job.status != "ready":
        return RedirectResponse("/suneung/jobs", status_code=303)
    job.status = "parsing"
    db.commit()
    background_tasks.add_task(run_pipeline, job_id, job.file_path)
    return RedirectResponse("/suneung/jobs", status_code=303)


@router.post("/jobs/{job_id}/reset")
def reset_job(job_id: str, db: Session = Depends(get_db)):
    """파이프라인 결과 초기화 → ready 상태로 되돌림 (PDF 파일 유지)"""
    job = db.get(PipelineJob, job_id)
    if not job:
        return JSONResponse({"ok": False, "error": "not found"})
    job.status = "ready"
    job.segments = None
    job.raw_result = None
    job.error_message = None
    job.page_image_paths = None
    db.commit()
    return JSONResponse({"ok": True})


@router.post("/jobs/bulk-start")
async def bulk_start_jobs(request: Request, background_tasks: BackgroundTasks,
                          db: Session = Depends(get_db)):
    """여러 ready 작업을 한 번에 파이프라인 시작"""
    body = await request.json()
    ids = body.get("ids", [])
    started = 0
    for job_id in ids[:20]:
        job = db.get(PipelineJob, job_id)
        if not job or job.status != "ready":
            continue
        job.status = "parsing"
        db.commit()
        background_tasks.add_task(run_pipeline, job_id, job.file_path)
        started += 1
    return JSONResponse({"ok": True, "started": started})


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, db: Session = Depends(get_db)):
    """진행 중인 작업 취소"""
    job = db.get(PipelineJob, job_id)
    if job and job.status not in ("done", "reviewing"):
        job.status = "error"
        job.error_message = "사용자가 취소했습니다."
        db.commit()
    return RedirectResponse(url="/suneung/jobs", status_code=303)


# ─────────────────────────────────────────
# 검수 UI
# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 정답/해설 관리 (/suneung/answers)
# ─────────────────────────────────────────
import sqlite3 as _sqlite3

_ANS_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "aprolabs.db")


def _ans_db():
    conn = _sqlite3.connect(_ANS_DB)
    conn.row_factory = _sqlite3.Row
    return conn


def _run_answer_pipeline(pdf_path: str, year: int, exam_type: str, subject: str):
    """백그라운드: answer_pipeline.parse() + save_to_db() 실행"""
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
    try:
        from answer_pipeline import parse, save_to_db
        result = parse(pdf_path, source_year=year, exam_type=exam_type, subject=subject)
        save_to_db(result, _ANS_DB)
    except Exception:
        pass


@router.get("/answers/api")
def answer_api_by_year(year: int = 0, qnum: int = 0):
    """검수 UI용: 연도+문항번호로 정답/해설 JSON 반환"""
    conn = _ans_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT * FROM question_explanations WHERE paper_code LIKE ? AND question_number = ? LIMIT 5",
        (f"{year}%", qnum),
    ).fetchall()
    conn.close()
    items = [dict(r) for r in rows]
    # wrong_answers JSON 파싱
    import json as _json
    for it in items:
        try:
            it["wrong_answers"] = _json.loads(it.get("wrong_answers") or "{}")
        except Exception:
            it["wrong_answers"] = {}
    return JSONResponse(items)


@router.get("/answers", response_class=HTMLResponse)
def answers_list(request: Request, db: Session = Depends(get_db)):
    """정답/해설 시험 목록 + 요약"""
    import json as _json
    conn = _ans_db()
    cur = conn.cursor()
    # paper_code별 요약
    qe_rows = cur.execute("""
        SELECT paper_code,
               COUNT(*) as q_total,
               SUM(CASE WHEN correct_answer = 0 THEN 1 ELSE 0 END) as q_zero
        FROM question_explanations
        GROUP BY paper_code
        ORDER BY paper_code DESC
    """).fetchall()
    pe_rows = cur.execute("""
        SELECT paper_code, COUNT(*) as p_total
        FROM passage_explanations
        GROUP BY paper_code
    """).fetchall()
    conn.close()

    pe_map = {r["paper_code"]: r["p_total"] for r in pe_rows}
    exams = []
    for r in qe_rows:
        code = r["paper_code"]
        exams.append({
            "paper_code": code,
            "q_total": r["q_total"],
            "q_zero": r["q_zero"],
            "p_total": pe_map.get(code, 0),
            "accuracy": round((r["q_total"] - r["q_zero"]) / r["q_total"] * 100, 1) if r["q_total"] else 0,
        })

    ctx = _base_ctx(db, request=request, exams=exams)
    return templates.TemplateResponse("suneung/answers.html", ctx)


@router.post("/answers/upload")
async def answers_upload(
    background_tasks: BackgroundTasks,
    request: Request,
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    year: str = Form(""),
    exam_type: str = Form("수능"),
    subject: str = Form("국어"),
):
    """정답해설 PDF 업로드 → answer_pipeline 백그라운드 실행"""
    if not file or not file.filename:
        return RedirectResponse(url="/suneung/answers", status_code=303)

    ans_dir = os.path.join(UPLOAD_DIR, "answers")
    os.makedirs(ans_dir, exist_ok=True)
    file_id = str(uuid.uuid4())
    ext = os.path.splitext(file.filename)[1].lower()
    pdf_path = os.path.join(ans_dir, f"{file_id}{ext}")
    async with aiofiles.open(pdf_path, "wb") as f:
        await f.write(await file.read())

    year_int = int(year) if year.isdigit() else 0
    background_tasks.add_task(_run_answer_pipeline, pdf_path, year_int, exam_type, subject)
    return RedirectResponse(url="/suneung/answers", status_code=303)


@router.get("/answers/{paper_code:path}", response_class=HTMLResponse)
def answer_detail(paper_code: str, request: Request, db: Session = Depends(get_db)):
    """특정 시험의 문항별 정답/해설"""
    import json as _json
    conn = _ans_db()
    cur = conn.cursor()
    questions = [dict(r) for r in cur.execute(
        "SELECT * FROM question_explanations WHERE paper_code = ? ORDER BY question_number",
        (paper_code,),
    ).fetchall()]
    passages = [dict(r) for r in cur.execute(
        "SELECT * FROM passage_explanations WHERE paper_code = ? ORDER BY range_start",
        (paper_code,),
    ).fetchall()]
    conn.close()

    # wrong_answers 파싱
    for q in questions:
        try:
            q["wrong_answers"] = _json.loads(q.get("wrong_answers") or "{}")
        except Exception:
            q["wrong_answers"] = {}

    # 동일 paper_code의 questions 테이블 데이터 (stem/choices 비교용)
    # approve()와 동일한 paper_code 형식이어야 매칭됨
    from app.models.question import Question as QModel
    db_questions = {
        q.question_number: q
        for q in db.query(QModel).filter(QModel.paper_code == paper_code).all()
    }
    matched = sum(1 for q in questions if q["question_number"] in db_questions)

    ctx = _base_ctx(
        db, request=request,
        paper_code=paper_code,
        questions=questions,
        passages=passages,
        db_questions=db_questions,
        matched=matched,
    )
    return templates.TemplateResponse("suneung/answer_detail.html", ctx)


@router.get("/review/{job_id}/pages")
def get_page_images(job_id: str):
    """검수 UI용 PDF 페이지 이미지 목록 반환.
    탐색 순서: pages/ → images/ → 루트 디렉토리 직접
    """
    job_root = os.path.join(UPLOAD_DIR, job_id)

    # 1) 하위 폴더 탐색
    for dir_name in ("pages", "images"):
        dir_path = os.path.join(job_root, dir_name)
        if not os.path.exists(dir_path):
            continue
        files = sorted(
            f for f in os.listdir(dir_path)
            if f.lower().endswith(('.png', '.jpg', '.jpeg'))
        )
        if files:
            return JSONResponse([
                f"/uploads/suneung/{job_id}/{dir_name}/{f}" for f in files
            ])

    # 2) 루트에 바로 있는 경우 (이전 파이프라인 호환)
    if os.path.exists(job_root):
        files = sorted(
            f for f in os.listdir(job_root)
            if f.lower().endswith(('.png', '.jpg', '.jpeg'))
        )
        if files:
            return JSONResponse([
                f"/uploads/suneung/{job_id}/{f}" for f in files
            ])

    return JSONResponse([])


@router.get("/review/{job_id}/extracted-images")
def get_extracted_images(job_id: str):
    """PDF에서 추출된 이미지 목록 반환 (images/ 디렉토리)."""
    img_dir = os.path.join(UPLOAD_DIR, job_id, "images")
    if not os.path.exists(img_dir):
        return JSONResponse([])
    files = sorted(
        f for f in os.listdir(img_dir)
        if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))
    )
    return JSONResponse([
        f"/uploads/suneung/{job_id}/images/{f}" for f in files
    ])


@router.get("/review/{job_id}/images")
def list_job_images(job_id: str):
    """검수 UI용: images/ 디렉토리 이미지 목록 반환."""
    img_dir = os.path.join(UPLOAD_DIR, job_id, "images")
    if not os.path.isdir(img_dir):
        return JSONResponse({"images": []})
    images = []
    for f in sorted(os.listdir(img_dir)):
        if f.lower().endswith(('.png', '.jpg', '.jpeg')):
            images.append({
                "filename": f,
                "url": f"/uploads/suneung/{job_id}/images/{f}",
            })
    return JSONResponse({"images": images})


_AI_TO_ENG = {'오탐': 'odam', '실제오류': 'real_error', '보류': 'pending'}


def _j2v(ai_kr: str) -> str:
    """AI 한글 판정 → human 영문 판정 변환."""
    return _AI_TO_ENG.get(ai_kr, '')


def _extract_judgment(val) -> tuple[str, str]:
    """warning_reviews 값에서 (judgment_str, source) 추출.
    값이 문자열이면 ("odam", "human"), dict이면 (val["judgment"], val["source"]) 반환.
    """
    if isinstance(val, dict):
        return val.get('judgment', ''), val.get('source', 'human')
    return (val or ''), 'human'


@router.get("/warnings", response_class=HTMLResponse)
def warnings_list(request: Request, db: Session = Depends(get_db)):
    """전체 경고 목록 — 카테고리별 그룹 표시"""
    def _categorize(msg: str) -> str:
        if 'PDF 밑줄 텍스트' in msg and ('못' in msg or '찾' in msg):
            return '밑줄못찾음'
        if '텍스트 불일치' in msg and re.search(r'\[[A-E]\]', msg):
            return 'bracket텍스트불일치'
        if re.search(r'\[[A-E]\]', msg) and ('범위 내 텍스트' in msg or '텍스트 미확인' in msg):
            return 'bracket텍스트불일치'
        if '끝 위치 특정 불가' in msg or '시작 위치는 찾았' in msg:
            return 'bracket텍스트불일치'
        if '선택지' in msg and '불일치' in msg:
            return '텍스트불일치'
        if '텍스트 불일치' in msg:
            return '텍스트불일치'
        if '지문을 PDF에서 찾지 못' in msg or '대응하는 PDF 지문' in msg:
            return '지문못찾음'
        if '문항을 PDF에서 찾지 못' in msg or 'PDF에서 해당 문항을' in msg:
            return '문항못찾음'
        return '기타'

    CATEGORY_ORDER = ['텍스트불일치', 'bracket텍스트불일치', '지문못찾음', '문항못찾음', '밑줄못찾음', '기타']
    CATEGORY_DESC = {
        '텍스트불일치':      'JSON 텍스트와 PDF 내용이 다름 — 선택지·지문 수동 비교 필요',
        'bracket텍스트불일치': '[A]~[E] 범위 내 텍스트가 PDF와 불일치 또는 위치 특정 불가',
        '지문못찾음':        'PDF에서 해당 지문을 찾지 못함 — 지문 인식 실패',
        '문항못찾음':        'PDF에서 해당 문항을 찾지 못함 — stem 부족 또는 레이아웃 차이',
        '밑줄못찾음':        'PDF 밑줄 텍스트를 지문 JSON에서 찾지 못함',
        '기타':             '분류되지 않은 경고',
    }

    jobs = db.query(PipelineJob).filter(
        PipelineJob.raw_result.isnot(None)
    ).order_by(PipelineJob.filename).all()

    all_warnings = []
    for job in jobs:
        if not job.raw_result:
            continue
        corrections = job.raw_result.get('verify_corrections', [])
        reviews    = job.raw_result.get('warning_reviews', {})
        ai_reviews = job.raw_result.get('ai_reviews', {})
        for c in corrections:
            if not isinstance(c, dict):
                continue
            if c.get('kind', '').lower() != 'warning':
                continue
            loc = c.get('location', '')
            msg = c.get('message', '')
            cat = _categorize(msg)
            key = f"{loc}|||{msg[:80]}"
            ai_entry    = ai_reviews.get(key, {})
            ai_judgment = ai_entry.get('judgment', '') if isinstance(ai_entry, dict) else ''
            ai_reason   = ai_entry.get('reason', '')   if isinstance(ai_entry, dict) else ''
            human_j, human_source = _extract_judgment(reviews.get(key))
            # 사람↔AI 불일치 여부 (둘 다 판정된 경우만, auto_from_ai는 불일치 제외)
            disagree = bool(
                human_j and ai_judgment
                and human_source != 'auto_from_ai'
                and human_j != _j2v(ai_judgment)
            )
            all_warnings.append({
                'job_id': job.id,
                'filename': job.filename,
                'location': loc,
                'message': msg,
                'category': cat,
                'key': key,
                'judgment': human_j,
                'judgment_source': human_source,
                'ai_judgment': ai_judgment,
                'ai_reason':   ai_reason,
                'disagree':    disagree,
            })

    # grouped[category][filename] = [warning, ...]
    grouped: dict = {cat: {} for cat in CATEGORY_ORDER}
    for w in all_warnings:
        cat = w['category']
        if cat not in grouped:
            grouped[cat] = {}
        fname = w['filename']
        if fname not in grouped[cat]:
            grouped[cat][fname] = []
        grouped[cat][fname].append(w)

    cat_counts = {cat: sum(len(v) for v in grouped[cat].values()) for cat in CATEGORY_ORDER}

    # job_id map: filename → job_id (for 검수 열기 links)
    fname_to_job = {job.filename: job.id for job in jobs}

    judgment_counts = {
        'odam':       sum(1 for w in all_warnings if w['judgment'] == 'odam'),
        'real_error': sum(1 for w in all_warnings if w['judgment'] == 'real_error'),
        'pending':    sum(1 for w in all_warnings if w['judgment'] == 'pending'),
        'none':       sum(1 for w in all_warnings if not w['judgment']),
        'auto':       sum(1 for w in all_warnings if w['judgment_source'] == 'auto_from_ai'),
    }
    ai_counts = {
        'odam':       sum(1 for w in all_warnings if w['ai_judgment'] == '오탐'),
        'real_error': sum(1 for w in all_warnings if w['ai_judgment'] == '실제오류'),
        'pending':    sum(1 for w in all_warnings if w['ai_judgment'] == '보류'),
        'none':       sum(1 for w in all_warnings if not w['ai_judgment']),
        'disagree':   sum(1 for w in all_warnings if w['disagree']),
    }

    ctx = _base_ctx(
        db, request=request,
        all_warnings=all_warnings,
        grouped=grouped,
        cat_counts=cat_counts,
        cat_desc=CATEGORY_DESC,
        category_order=CATEGORY_ORDER,
        fname_to_job=fname_to_job,
        judgment_counts=judgment_counts,
        ai_counts=ai_counts,
        total=len(all_warnings),
    )
    return templates.TemplateResponse("suneung/warnings.html", ctx)


@router.post("/review/{job_id}/warning-judgment")
async def save_warning_judgment(job_id: str, request: Request, db: Session = Depends(get_db)):
    """경고별 판정 결과 저장 (오탐/실제오류/보류/초기화)"""
    data = await request.json()
    key = data.get('key', '')
    judgment = data.get('judgment', '')  # 'odam' | 'real_error' | 'pending' | ''

    job = db.get(PipelineJob, job_id)
    if not job:
        return JSONResponse({"ok": False, "error": "not found"})

    raw = dict(job.raw_result or {})
    reviews = dict(raw.get('warning_reviews', {}))

    if judgment:
        reviews[key] = judgment
    else:
        reviews.pop(key, None)

    raw['warning_reviews'] = reviews
    job.raw_result = raw
    db.commit()

    total_w = sum(
        1 for c in raw.get('verify_corrections', [])
        if isinstance(c, dict) and c.get('kind', '').lower() == 'warning'
    )
    return JSONResponse({"ok": True, "reviewed": len(reviews), "total": total_w})


@router.get("/review/{job_id}", response_class=HTMLResponse)
def review(request: Request, job_id: str, db: Session = Depends(get_db)):
    job = db.get(PipelineJob, job_id)
    if not job:
        return RedirectResponse("/suneung/jobs")
    ctx = _base_ctx(db, request=request, job=job)
    return templates.TemplateResponse("suneung/review.html", ctx)


@router.post("/review/{job_id}/save")
async def save_segments(job_id: str, request: Request, db: Session = Depends(get_db)):
    """검수 UI에서 수정된 JSON을 저장 (아직 DB 확정 아님)"""
    data = await request.json()
    job = db.get(PipelineJob, job_id)
    if not job:
        return JSONResponse({"ok": False})
    job.segments = data
    db.commit()
    return JSONResponse({"ok": True})


@router.post("/review/{job_id}/rerun-agent")
async def rerun_agent(job_id: str, db: Session = Depends(get_db)):
    """verify_agent 만 재실행해서 교정 결과를 갱신"""
    job = db.get(PipelineJob, job_id)
    if not job or not job.segments or not job.file_path:
        return JSONResponse({"ok": False, "error": "job not found or missing data"})

    pdf_path = job.file_path
    if not os.path.exists(pdf_path):
        return JSONResponse({"ok": False, "error": f"PDF 파일 없음: {pdf_path}"})

    try:
        import sys, pathlib
        sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))
        from verify_agent import run as verify_run

        segments = job.segments
        passages_data = list(segments.get("passages", []))
        questions_data = list(segments.get("questions", []))

        result = verify_run(
            pdf_path=pdf_path,
            segments_json={"passages": passages_data, "questions": questions_data},
            dpi=120,
            verbose=False,
        )

        # 교정된 지문 내용 반영
        for i, corrected_p in enumerate(result.segments.passages):
            if i < len(passages_data):
                passages_data[i]["content"] = corrected_p.content

        verify_corrections = [
            {"kind": c.kind, "location": c.location, "message": c.message}
            for c in result.corrections
        ]

        job.segments = {"passages": passages_data, "questions": questions_data}
        job.raw_result = {
            **(job.raw_result or {}),
            "verify_corrections": verify_corrections,
        }
        db.commit()

        return JSONResponse({
            "ok": True,
            "fixed": sum(1 for c in result.corrections if c.kind == "fixed"),
            "warnings": sum(1 for c in result.corrections if c.kind == "warning"),
            "errors": sum(1 for c in result.corrections if c.kind == "error"),
        })

    except Exception as e:
        import traceback
        return JSONResponse({"ok": False, "error": f"{e}\n{traceback.format_exc()[-400:]}"})


@router.post("/review/{job_id}/approve")
def approve(job_id: str, db: Session = Depends(get_db)):
    """검수 완료 → passages + questions DB에 확정 저장"""
    job = db.get(PipelineJob, job_id)
    if not job or not job.segments:
        return RedirectResponse(f"/suneung/review/{job_id}", status_code=303)

    segments = job.segments
    passages_data = segments.get("passages", [])
    questions_data = segments.get("questions", [])

    # ── 문제지 코드 생성 및 ExamPaper 확보 ──────────
    subject = job.subject or "국어"
    exam_type_clean = (job.exam_type or "기타").replace(" ", "")
    year_str = str(job.source_year) if job.source_year else "미지정"
    paper_code = f"{year_str}-{exam_type_clean}-{subject}"

    paper = db.get(ExamPaper, paper_code)
    if not paper:
        paper = ExamPaper(
            paper_code=paper_code,
            source_year=job.source_year,
            exam_type=job.exam_type,
            subject=subject,
        )
        db.add(paper)
        db.flush()

    created_passage_ids = []

    # 기존 지문/문항 번호 확인 (중복 방지)
    existing_passage_seqs = {
        p.paper_seq for p in
        db.query(Passage).filter(Passage.paper_code == paper_code).all()
        if p.paper_seq is not None
    }
    existing_q_nums = {
        q.question_number for q in
        db.query(Question).filter(Question.paper_code == paper_code).all()
        if q.question_number is not None
    }
    # 기존 지문 id 맵 (passage_idx 연결용)
    existing_passage_id_map = {
        p.paper_seq: p.id for p in
        db.query(Passage).filter(Passage.paper_code == paper_code).all()
        if p.paper_seq is not None
    }

    # 지문 저장 (이미 있는 seq는 스킵)
    for seq, p in enumerate(passages_data, start=1):
        if seq in existing_passage_seqs:
            created_passage_ids.append(existing_passage_id_map.get(seq))
            continue
        passage = Passage(
            paper_code=paper_code,
            paper_seq=seq,
            category=subject,
            source=job.source,
            source_year=job.source_year,
            exam_type=job.exam_type,
            subject=subject,
            question_range=p.get("question_range"),
            content=p.get("content"),
            complexity_score=p.get("complexity_score"),
            concepts=p.get("concepts", []),
        )
        db.add(passage)
        db.flush()
        created_passage_ids.append(passage.id)

    # 문항 저장 (이미 있는 question_number는 스킵)
    for q in questions_data:
        q_num = q.get("number")
        if q_num and q_num in existing_q_nums:
            continue
        pidx = q.get("passage_idx")
        passage_id = created_passage_ids[pidx] if pidx is not None and pidx < len(created_passage_ids) else None

        question = Question(
            paper_code=paper_code,
            category=subject,
            subcategory="수능국어DB",
            passage_id=passage_id,
            question_number=q_num,
            stem=q.get("stem"),
            choices=q.get("choices"),
            content=q.get("content") or q.get("stem") or "",
            answer=q.get("answer"),
            explanation=q.get("explanation"),
            difficulty=q.get("difficulty"),
            topic=q.get("topic"),
            tags=q.get("tags", []),
            thinking_types=q.get("thinking_types", []),
            source=job.source,
            source_year=job.source_year,
            status="approved",
        )
        db.add(question)

    job.status = "done"
    db.commit()

    return RedirectResponse(url="/?category=국어&subcategory=수능국어DB", status_code=303)
