"""독서논술 문항 DB 라우터
/reading-essay        -> 교재 목록 (분기/학년/계열/상태 필터)
/reading-essay/search -> 어휘/OX/토론/글쓰기 통합 검색
/reading-essay/scan    -> 배치 스캔(분기 폴더 훑어서 추출) 화면 + 실행
/reading-essay/scan/upload -> 폴더/파일 선택 또는 드래그 드랍으로 업로드한 PDF만 추출
/reading-essay/quality -> 추출 품질 검수 (계열별 결측치 집계 + 문제 교재 목록)
/reading-essay/{id}    -> 교재 상세(추출 결과 검수)
/reading-essay/{id}/reextract -> 단건 재추출
/reading-essay/{id}/delete    -> 삭제
"""
import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, Request, Form, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.reading_essay import ReadingMaterial, TEMPLATE_FAMILIES, STATUSES
from app.services.reading_essay.scanner import QUARTER_ROOTS, GRADES, scan, process_one
from app.services.reading_essay.quality import family_stats, flagged_materials
from app.services.reading_essay.search import search as search_reading_essay

UPLOAD_DIR = Path("uploads/reading_essay/manual")

router = APIRouter(prefix="/reading-essay")
templates = Jinja2Templates(directory="app/templates")


def _base_ctx(db: Session, **kwargs):
    counts = {}
    for fam in TEMPLATE_FAMILIES:
        counts[fam] = db.query(ReadingMaterial).filter(ReadingMaterial.template_family == fam).count()
    counts["전체"] = db.query(ReadingMaterial).count()
    return {
        "families": TEMPLATE_FAMILIES, "statuses": STATUSES,
        "quarters": list(QUARTER_ROOTS.keys()), "grades": GRADES,
        "family_counts": counts,
        **kwargs,
    }


@router.get("", response_class=HTMLResponse)
def list_materials(
    request: Request, db: Session = Depends(get_db),
    quarter: str = "", grade: str = "", family: str = "", status: str = "", q: str = "",
):
    query = db.query(ReadingMaterial)
    if quarter:
        query = query.filter(ReadingMaterial.quarter == quarter)
    if grade:
        query = query.filter(ReadingMaterial.grade == grade)
    if family:
        query = query.filter(ReadingMaterial.template_family == family)
    if status:
        query = query.filter(ReadingMaterial.status == status)
    if q:
        query = query.filter(ReadingMaterial.title.contains(q))

    materials = query.order_by(
        ReadingMaterial.quarter.desc(), ReadingMaterial.grade, ReadingMaterial.week
    ).all()

    ctx = _base_ctx(
        db, request=request, materials=materials,
        filter_quarter=quarter, filter_grade=grade, filter_family=family,
        filter_status=status, filter_q=q, total=len(materials),
    )
    return templates.TemplateResponse("reading_essay/list.html", ctx)


@router.get("/scan", response_class=HTMLResponse)
def scan_page(request: Request, db: Session = Depends(get_db)):
    ctx = _base_ctx(db, request=request)
    return templates.TemplateResponse("reading_essay/scan.html", ctx)


@router.post("/scan")
def run_scan(
    request: Request, db: Session = Depends(get_db),
    quarters: list[str] = Form(default=[]),
    grades: list[str] = Form(default=[]),
    force: bool = Form(default=False),
    limit: str = Form(default=""),
):
    result = scan(
        db,
        quarters=quarters or None,
        grades=grades or None,
        force=force,
        limit=int(limit) if limit.isdigit() else None,
    )
    ctx = _base_ctx(db, request=request, result=result)
    return templates.TemplateResponse("reading_essay/scan.html", ctx)


@router.post("/scan/upload")
async def scan_upload(
    db: Session = Depends(get_db),
    files: list[UploadFile] = File(...),
    quarter: str = Form(default="직접업로드"),
    grade: str = Form(default="미지정"),
    force: bool = Form(default=False),
):
    """폴더 선택/파일 선택/드래그 드랍으로 올린 PDF만 골라서 추출.
    같은 폴더에서 고른 교사용/학생용 PDF는 그대로 짝지어지므로 scanner.process_one을 재사용.
    """
    job_dir = UPLOAD_DIR / uuid.uuid4().hex
    job_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = []
    for f in files:
        if not f.filename.lower().endswith('.pdf'):
            continue
        dest = job_dir / Path(f.filename).name
        with open(dest, 'wb') as out:
            shutil.copyfileobj(f.file, out)
        saved_paths.append(dest)

    if not saved_paths:
        return JSONResponse({'detail': 'PDF 파일이 없습니다.'}, status_code=400)

    teacher_paths = [p for p in saved_paths if '교사용' in p.name]
    if not teacher_paths:
        # "교사용" 표시가 없는 파일만 올라온 경우 전체를 후보로 처리 시도
        teacher_paths = saved_paths

    counts = {'extracted': 0, 'skipped': 0, 'error': 0, 'total': 0}
    errors = []
    for teacher_path in teacher_paths:
        counts['total'] += 1
        material, outcome = process_one(db, quarter, grade, teacher_path, force=force)
        counts[outcome] = counts.get(outcome, 0) + 1
        if outcome == 'error':
            errors.append({'file': teacher_path.name, 'error': material.error_message})
    counts['errors'] = errors
    return JSONResponse(counts)


@router.get("/search", response_class=HTMLResponse)
def search_page(
    request: Request, db: Session = Depends(get_db),
    q: str = "", quarter: str = "", grade: str = "", family: str = "",
    types: list[str] = None,
):
    content_types = set(types) if types else None
    results = []
    if q or quarter or grade or family or types:
        results = search_reading_essay(
            db, q=q, quarter=quarter or None, grade=grade or None,
            family=family or None, content_types=content_types,
        )
    ctx = _base_ctx(
        db, request=request, results=results, total=len(results),
        filter_q=q, filter_quarter=quarter, filter_grade=grade, filter_family=family,
        filter_types=content_types or {'vocab', 'ox', 'discussion', 'writing'},
    )
    return templates.TemplateResponse("reading_essay/search.html", ctx)


@router.get("/quality", response_class=HTMLResponse)
def quality_page(
    request: Request, db: Session = Depends(get_db),
    family: str = "", issue: str = "",
):
    stats = family_stats(db)
    flagged = flagged_materials(db, family=family or None, issue=issue or None)
    ctx = _base_ctx(
        db, request=request, stats=stats, flagged=flagged,
        filter_family=family, filter_issue=issue,
    )
    return templates.TemplateResponse("reading_essay/quality.html", ctx)


@router.get("/{material_id}", response_class=HTMLResponse)
def detail(material_id: str, request: Request, db: Session = Depends(get_db)):
    material = db.query(ReadingMaterial).filter(ReadingMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404)
    ctx = _base_ctx(db, request=request, material=material)
    return templates.TemplateResponse("reading_essay/detail.html", ctx)


@router.post("/{material_id}/reextract")
def reextract(material_id: str, db: Session = Depends(get_db)):
    material = db.query(ReadingMaterial).filter(ReadingMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404)
    process_one(db, material.quarter, material.grade, material.teacher_pdf_path, force=True)
    return RedirectResponse(url=f"/reading-essay/{material_id}", status_code=303)


@router.post("/{material_id}/delete")
def delete(material_id: str, db: Session = Depends(get_db)):
    material = db.query(ReadingMaterial).filter(ReadingMaterial.id == material_id).first()
    if material:
        db.delete(material)
        db.commit()
    return RedirectResponse(url="/reading-essay", status_code=303)
