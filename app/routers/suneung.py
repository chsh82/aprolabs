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


def _render_brackets(text: str) -> str:
    """[A:START]...[A:END] 마커를 시각적 HTML 블록으로 변환"""
    if not text:
        return text

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

    for file in files[:20]:  # 최대 20개
        if not file.filename:
            continue

        job_id = str(uuid.uuid4())
        ext = os.path.splitext(file.filename)[1].lower()
        pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}{ext}")

        async with aiofiles.open(pdf_path, "wb") as f:
            await f.write(await file.read())

        from sqlalchemy import func
        from app.routers.crawl import _extract_grade
        max_num = db.query(func.max(PipelineJob.job_number)).scalar() or 0

        # 학년 자동 추출: 폼 입력 → 파일명 → 시험유형 규칙
        resolved_grade = (
            grade.strip()
            or _extract_grade(file.filename, exam_type)
            or _extract_grade("", exam_type)
            or None
        )

        job = PipelineJob(
            id=job_id,
            job_number=max_num + 1,
            filename=file.filename,
            file_path=pdf_path,
            source=source or None,
            source_year=int(source_year) if source_year.isdigit() else None,
            exam_type=exam_type,
            subject=subject,
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

        job.raw_result = {
            "passages": passages_data,
            "questions": questions_data,
            "ocr_text": raw_text[:3000],
        }
        job.status = "tagging"
        db.commit()

        # Phase 3: 사고력 태깅 (Claude, 일괄 1회)
        tag_all(passages_data, questions_data, job_id=job_id)

        # Phase 3.5: verify_agent — PDF 이미지 vs JSON 자동 교정
        verify_corrections = []
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

            # 교정된 지문 내용 반영
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
        # 교정 내역을 raw_result에 보존
        job.raw_result = {
            **(job.raw_result or {}),
            "verify_corrections": verify_corrections,
        }
        job.status = "reviewing"
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
        reviews = job.raw_result.get('warning_reviews', {})
        for c in corrections:
            if not isinstance(c, dict):
                continue
            if c.get('kind', '').lower() != 'warning':
                continue
            loc = c.get('location', '')
            msg = c.get('message', '')
            cat = _categorize(msg)
            key = f"{loc}|||{msg[:80]}"
            all_warnings.append({
                'job_id': job.id,
                'filename': job.filename,
                'location': loc,
                'message': msg,
                'category': cat,
                'key': key,
                'judgment': reviews.get(key, ''),
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
