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
from app.models.momo_bookshelf import MomoBookshelfWeek, MomoRequiredBook, GRADES

router = APIRouter(prefix="/momo-bookshelf")
templates = Jinja2Templates(directory="app/templates")

SHEET_NAME = "연간 전체 리스트"
REQUIRED_HEADERS = ["분기", "학년", "주차", "기간", "도서명", "저자(역자)", "출판사"]


def _parse_week_number(raw) -> int | None:
    if raw is None:
        return None
    m = re.search(r"\d+", str(raw))
    return int(m.group()) if m else None


def _strip_extension_marker(title: str) -> str:
    """"이솝 이야기 (연장)" -> "이솝 이야기" (같은 책을 연속으로 읽는 재당 주차 표시 제거)"""
    return re.sub(r"\s*\(연장\)\s*$", "", title or "").strip()


def _sync_required_books(db: Session) -> dict:
    """momo_bookshelf_weeks에서 주차를 빼고 학년+분기별 고유 도서만 뽑아 momo_required_books에 반영.
    "(연장)" 재당 주차는 원본과 합치고, 휴강/특강 표시 행은 제외함.
    이미 연결된 ISBN 등 기존 필드는 건드리지 않음(사라진 항목도 삭제하지 않고 유지)."""
    weeks = db.query(MomoBookshelfWeek).filter(MomoBookshelfWeek.is_holiday == False).all()

    unique = {}  # (year, quarter, grade, clean_title) -> (author, publisher)
    for w in weeks:
        clean_title = _strip_extension_marker(w.title)
        if not clean_title:
            continue
        key = (w.year, w.quarter, w.grade, clean_title)
        if key not in unique:
            unique[key] = (w.author, w.publisher)

    added, updated = 0, 0
    for (year, quarter, grade, title), (author, publisher) in unique.items():
        existing = db.query(MomoRequiredBook).filter(
            MomoRequiredBook.year == year,
            MomoRequiredBook.quarter == quarter,
            MomoRequiredBook.grade == grade,
            MomoRequiredBook.title == title,
        ).first()
        if existing:
            if existing.author != author or existing.publisher != publisher:
                existing.author = author
                existing.publisher = publisher
                existing.updated_at = datetime.now()
                updated += 1
        else:
            db.add(MomoRequiredBook(year=year, quarter=quarter, grade=grade, title=title, author=author, publisher=publisher))
            added += 1

    db.commit()
    return {"added": added, "updated": updated, "total": len(unique)}


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
    year: str | None = None,
    quarter: str | None = None,
    grade: str | None = None,
    db: Session = Depends(get_db),
):
    """연도/분기/학년으로 필터링한 커리큘럼 목록"""
    # "전체" 옵션 선택 시 빈 문자열로 넘어오므로 int 파싱 전에 방어
    year_val = int(year) if year else None

    query = db.query(MomoBookshelfWeek)
    if year_val:
        query = query.filter(MomoBookshelfWeek.year == year_val)
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
        "filter_year": year_val,
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
    sync_result = _sync_required_books(db)
    return JSONResponse({
        "ok": True, "added": added, "updated": updated, "total": len(rows),
        "required_books": sync_result,
    })


@router.get("/required", response_class=HTMLResponse)
def required_books_list(
    request: Request,
    year: str | None = None,
    quarter: str | None = None,
    grade: str | None = None,
    db: Session = Depends(get_db),
):
    """모모의 책장 필독서 목록 (커리큘럼에서 주차를 뺀 학년+분기별 고유 도서 + ISBN)"""
    year_val = int(year) if year else None

    query = db.query(MomoRequiredBook)
    if year_val:
        query = query.filter(MomoRequiredBook.year == year_val)
    if quarter:
        query = query.filter(MomoRequiredBook.quarter == quarter)
    if grade:
        query = query.filter(MomoRequiredBook.grade == grade)

    books = query.order_by(
        MomoRequiredBook.grade, MomoRequiredBook.quarter, MomoRequiredBook.title
    ).all()

    years = [r[0] for r in db.query(distinct(MomoRequiredBook.year)).order_by(MomoRequiredBook.year.desc()).all()]
    quarters = sorted(r[0] for r in db.query(distinct(MomoRequiredBook.quarter)).all())

    return templates.TemplateResponse("momo_bookshelf/required.html", {
        "request": request,
        "books": books,
        "years": years,
        "quarters": quarters,
        "grades": GRADES,
        "filter_year": year_val,
        "filter_quarter": quarter,
        "filter_grade": grade,
        "total_count": db.query(MomoRequiredBook).count(),
        "linked_count": db.query(MomoRequiredBook).filter(MomoRequiredBook.isbn13.isnot(None)).count(),
    })


@router.post("/required/sync")
def required_books_sync(db: Session = Depends(get_db)):
    """커리큘럼(momo_bookshelf_weeks)에서 필독서 목록을 다시 동기화 (수동 재실행용)"""
    result = _sync_required_books(db)
    return JSONResponse({"ok": True, **result})
