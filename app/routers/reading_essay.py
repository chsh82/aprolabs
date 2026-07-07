"""독서논술 문항 DB 라우터
/reading-essay        -> 교재 목록 (분기/학년/계열/상태 필터)
/reading-essay/scan    -> 배치 스캔(분기 폴더 훑어서 추출) 화면 + 실행
/reading-essay/{id}    -> 교재 상세(추출 결과 검수)
/reading-essay/{id}/reextract -> 단건 재추출
/reading-essay/{id}/delete    -> 삭제
"""
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.reading_essay import ReadingMaterial, TEMPLATE_FAMILIES, STATUSES
from app.services.reading_essay.scanner import QUARTER_ROOTS, GRADES, scan, process_one

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
