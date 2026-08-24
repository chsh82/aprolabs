"""
모모의 책장 - 분기별/학년별 주차 커리큘럼
GET  /momo-bookshelf         -> 목록 조회 (연도·분기·학년 필터)
GET  /momo-bookshelf/upload  -> 업로드 화면
POST /momo-bookshelf/upload  -> 엑셀 업로드 -> "연간 전체 리스트" 시트 파싱 -> upsert

업로드 파일은 "모모의책장_DB_..._연간_통합_주차별.xlsx"와 같은 형식으로,
"연간 전체 리스트" 시트에 분기/학년/주차/기간/도서명/저자(역자)/출판사
7개 열이 있어야 함. 같은 (연도, 분기, 학년, 주차) 조합이 이미 있으면
덮어쓰기(upsert)되므로 파일을 다시 올려 수정하는 것도 안전함.
"""
import io
import re
from datetime import datetime

from fastapi import APIRouter, Depends, Request, Form, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import distinct
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.momo_bookshelf import MomoBookshelfWeek, GRADES

router = APIRouter(prefix="/momo-bookshelf")
templates = Jinja2Templates(directory="app/templates")

SHEET_NAME = "연간 전체 리스트"
REQUIRED_HEADERS = ["분기", "학년", "주차", "기간", "도서명", "저자(역자)", "출판사"]


def _parse_week_number(raw) -> int | None:
    if raw is None:
        return None
    m = re.search(r"\d+", str(raw))
    return int(m.group()) if m else None


def _parse_workbook(file_bytes: bytes) -> list[dict]:
    """엑셀 파일에서 '연간 전체 리스트' 시트를 읽어 행 딕셔너리 리스트로 반환. 형식이 안 맞으면 ValueError."""
    from openpyxl import load_workbook

    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    if SHEET_NAME not in wb.sheetnames:
        raise ValueError(f'"{SHEET_NAME}" 시트를 찾을 수 없습니다. (시트 목록: {", ".join(wb.sheetnames)})')

    ws = wb[SHEET_NAME]
    rows_iter = ws.iter_rows(values_only=True)
    header = next(rows_iter, None)
    header_norm = [str(h).strip() if h else "" for h in (header or [])][:7]
    if header_norm != REQUIRED_HEADERS:
        raise ValueError(
            f"헤더가 예상과 다릅니다. 필요한 열: {', '.join(REQUIRED_HEADERS)} / 실제: {', '.join(header_norm)}"
        )

    parsed = []
    for row in rows_iter:
        if not row or not any(row):
            continue
        padded = (tuple(row) + (None,) * 7)[:7]
        quarter, grade, week_raw, date_range, title, author, publisher = padded
        if not (quarter and grade and title):
            continue
        week_number = _parse_week_number(week_raw)
        if week_number is None:
            continue
        author_s = str(author).strip() if author else None
        parsed.append({
            "quarter": str(quarter).strip(),
            "grade": str(grade).strip(),
            "week_number": week_number,
            "date_range": str(date_range).strip() if date_range else None,
            "title": str(title).strip(),
            "author": author_s,
            "publisher": str(publisher).strip() if publisher else None,
            "is_holiday": author_s == "-",
        })
    return parsed


@router.get("", response_class=HTMLResponse)
def bookshelf_list(
    request: Request,
    year: int | None = None,
    quarter: str | None = None,
    grade: str | None = None,
    db: Session = Depends(get_db),
):
    """연도/분기/학년으로 필터링한 커리큘럼 목록"""
    query = db.query(MomoBookshelfWeek)
    if year:
        query = query.filter(MomoBookshelfWeek.year == year)
    if quarter:
        query = query.filter(MomoBookshelfWeek.quarter == quarter)
    if grade:
        query = query.filter(MomoBookshelfWeek.grade == grade)

    weeks = query.order_by(
        MomoBookshelfWeek.grade,
        MomoBookshelfWeek.quarter,
        MomoBookshelfWeek.week_number,
    ).all()

    years = [r[0] for r in db.query(distinct(MomoBookshelfWeek.year)).order_by(MomoBookshelfWeek.year.desc()).all()]
    quarters = sorted(r[0] for r in db.query(distinct(MomoBookshelfWeek.quarter)).all())

    return templates.TemplateResponse("momo_bookshelf/list.html", {
        "request": request,
        "weeks": weeks,
        "years": years,
        "quarters": quarters,
        "grades": GRADES,
        "filter_year": year,
        "filter_quarter": quarter,
        "filter_grade": grade,
        "total_count": db.query(MomoBookshelfWeek).count(),
    })


@router.get("/upload", response_class=HTMLResponse)
def bookshelf_upload_page(request: Request):
    return templates.TemplateResponse("momo_bookshelf/upload.html", {
        "request": request,
        "default_year": datetime.now().year,
    })


@router.post("/upload")
async def bookshelf_upload(
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    year: int = Form(...),
):
    """같은 양식의 엑셀 파일을 업로드해 커리큘럼을 등록/갱신 (같은 연도+분기+학년+주차는 덮어씀)"""
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail=".xlsx 파일만 업로드할 수 있습니다.")

    content = await file.read()
    try:
        rows = _parse_workbook(content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not rows:
        raise HTTPException(status_code=400, detail="파싱된 데이터가 없습니다. 파일 형식을 확인하세요.")

    added, updated = 0, 0
    for r in rows:
        existing = db.query(MomoBookshelfWeek).filter(
            MomoBookshelfWeek.year == year,
            MomoBookshelfWeek.quarter == r["quarter"],
            MomoBookshelfWeek.grade == r["grade"],
            MomoBookshelfWeek.week_number == r["week_number"],
        ).first()
        if existing:
            existing.date_range = r["date_range"]
            existing.title = r["title"]
            existing.author = r["author"]
            existing.publisher = r["publisher"]
            existing.is_holiday = r["is_holiday"]
            existing.updated_at = datetime.now()
            updated += 1
        else:
            db.add(MomoBookshelfWeek(year=year, **r))
            added += 1

    db.commit()
    return JSONResponse({"ok": True, "added": added, "updated": updated, "total": len(rows)})
