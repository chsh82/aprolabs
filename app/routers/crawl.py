"""
레전드스터디 기출문제 크롤링 라우터
GET  /crawl           → 크롤링 UI
POST /crawl/search    → 카테고리 페이지에서 글 목록 수집
POST /crawl/files     → 개별 글 페이지에서 국어 PDF 링크 추출
POST /crawl/import    → 선택 PDF 다운로드 → 파이프라인 등록
"""
import os
import re
import uuid
import httpx
from fastapi import APIRouter, Depends, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from bs4 import BeautifulSoup
from sqlalchemy import func
from sqlalchemy.orm import Session
from urllib.parse import unquote

from app.database import get_db
from app.models.passage import PipelineJob

router = APIRouter(prefix="/crawl")
templates = Jinja2Templates(directory="app/templates")

UPLOAD_DIR = "uploads/suneung"
os.makedirs(UPLOAD_DIR, exist_ok=True)

CATEGORY_URL = (
    "https://legendstudy.com/category/"
    "%E2%97%86%EF%BB%BF%20%22%EA%B3%A03%22%EC%9D%84%20%EC%9C%84%ED%95%9C%20%EA%B3%B5%EA%B0%84%20"
    "/3%ED%95%99%EB%85%84%20%EB%AA%A8%EC%9D%98%EA%B3%A0%EC%82%AC%20%EC%A0%84%EA%B3%BC%EB%AA%A9%20%EC%9E%90%EB%A3%8C"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
}


# ─────────────────────────────────────────────
# GET /crawl
# ─────────────────────────────────────────────
@router.get("", response_class=HTMLResponse)
def crawl_page(request: Request):
    import datetime
    current_year = datetime.datetime.now().year
    years = list(range(current_year + 1, 2009, -1))
    return templates.TemplateResponse("crawl/index.html", {
        "request": request,
        "years": years,
    })


# ─────────────────────────────────────────────
# POST /crawl/search  — 글 목록 수집
# ─────────────────────────────────────────────
@router.post("/search")
async def crawl_search(request: Request, db: Session = Depends(get_db)):
    """카테고리 페이지 스캔 → 각 글에서 PDF 수집.
    source_url이 단일 글 URL(/숫자)이면 해당 페이지만 파싱.
    source_url이 비어있으면 기본 CATEGORY_URL 스캔.
    """
    import asyncio
    body = await request.json()
    year_filter     = str(body.get("year", "")).strip()
    pages_to_scan   = int(body.get("pages", 2))
    subj_filter     = body.get("subject", "")
    filetype_filter = body.get("filetype", "")
    source_url      = str(body.get("source_url", "")).strip()

    # source_url 정규화
    if source_url and not source_url.startswith("http"):
        source_url = "https://legendstudy.com/" + source_url.lstrip("/")

    all_files = []

    # ── 단일 글 URL인 경우 (예: legendstudy.com/1700) ──────────────────
    if source_url and re.search(r"legendstudy\.com/\d+$", source_url):
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=_HEADERS) as client:
            try:
                resp = await client.get(source_url)
                all_files = _parse_post_files(resp.text, source_url,
                                              subj_filter, filetype_filter)
            except Exception as e:
                return JSONResponse({"ok": False, "error": str(e)})
        for f in all_files:
            f["is_duplicate"] = _check_duplicate(
                db, f.get("academic_year"), f.get("exam_type"), f.get("sub_type"),
            )
        return JSONResponse({"ok": True, "files": all_files, "total": len(all_files)})

    # ── 카테고리 스캔 ───────────────────────────────────────────────────
    category_url = source_url if source_url else CATEGORY_URL

    posts = []
    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=_HEADERS) as client:
        for page_num in range(1, pages_to_scan + 1):
            url = category_url + (f"?page={page_num}" if page_num > 1 else "")
            try:
                resp = await client.get(url)
                page_posts = _parse_category_page(resp.text)
                if not page_posts:
                    break
                posts.extend(page_posts)
            except Exception as e:
                return JSONResponse({"ok": False, "error": str(e)})

    if year_filter:
        posts = [p for p in posts if _calc_academic_year(p.get("year", ""), p.get("exam_type", "")) == year_filter]
    posts = [p for p in posts if _is_korean_related(p.get("title", ""))]

    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=_HEADERS) as client:
        async def fetch_files(post):
            try:
                resp = await client.get(post["url"])
                return _parse_post_files(resp.text, post["url"],
                                         subj_filter, filetype_filter)
            except Exception:
                return []

        results = await asyncio.gather(*[fetch_files(p) for p in posts])
        for files in results:
            all_files.extend(files)

    for f in all_files:
        f["is_duplicate"] = _check_duplicate(
            db, f.get("academic_year"), f.get("exam_type"), f.get("sub_type"),
        )
    return JSONResponse({"ok": True, "files": all_files, "total": len(all_files)})



# ─────────────────────────────────────────────
# POST /crawl/import  — PDF 다운로드 → 파이프라인
# ─────────────────────────────────────────────
@router.post("/import")
async def crawl_import(request: Request, db: Session = Depends(get_db)):
    body = await request.json()
    files = body.get("files", [])

    if not files:
        return JSONResponse({"ok": False, "error": "선택된 파일 없음"})

    created = []
    async with httpx.AsyncClient(timeout=60, follow_redirects=True, headers=_HEADERS) as client:
        for f in files[:20]:
            pdf_url   = f.get("pdf_url", "").strip()
            title     = f.get("title", "untitled")
            year      = f.get("year")
            exam_type = f.get("exam_type", "")
            subject   = f.get("subject", "국어")
            file_type = f.get("file_type", "문제")
            grade     = f.get("grade", "") or _extract_grade("", exam_type)

            if not pdf_url:
                continue

            try:
                r = await client.get(pdf_url)
                if r.status_code != 200:
                    created.append({"title": title, "ok": False,
                                    "error": f"HTTP {r.status_code}"})
                    continue
            except Exception as e:
                created.append({"title": title, "ok": False, "error": str(e)})
                continue

            job_id   = str(uuid.uuid4())
            filename = f"{title}.pdf"
            pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}.pdf")

            with open(pdf_path, "wb") as fp:
                fp.write(r.content)

            max_num = db.query(func.max(PipelineJob.job_number)).scalar() or 0
            job = PipelineJob(
                id=job_id,
                job_number=max_num + 1,
                filename=filename,
                file_path=pdf_path,
                source=title,
                source_year=int(year) if str(year).isdigit() else None,
                exam_type=exam_type,
                subject=subject,
                grade=grade or None,
                status="ready",
            )
            db.add(job)
            db.commit()

            created.append({"title": title, "ok": True, "job_id": job_id})

    return JSONResponse({"ok": True, "created": created})


# ─────────────────────────────────────────────
# 파싱 유틸
# ─────────────────────────────────────────────
def _parse_category_page(html: str) -> list[dict]:
    """카테고리 페이지 → 글 목록"""
    soup = BeautifulSoup(html, "html.parser")
    posts = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        # 숫자 ID 경로만 (예: /1700)
        if not re.match(r"^https?://legendstudy\.com/\d+$", href) and \
           not re.match(r"^/\d+$", href):
            continue

        title = a.get_text(strip=True)
        if not title or len(title) < 5:
            continue

        url = href if href.startswith("http") else f"https://legendstudy.com{href}"

        year   = _extract_year(title)
        exam   = _extract_exam_type(title)

        posts.append({
            "title": title,
            "url":   url,
            "year":  year,
            "exam_type": exam,
        })

    # 중복 제거
    seen = set()
    unique = []
    for p in posts:
        if p["url"] not in seen:
            seen.add(p["url"])
            unique.append(p)
    return unique


def _parse_post_files(html: str, post_url: str,
                      subj_filter: str = "", filetype_filter: str = "") -> list[dict]:
    """개별 글 페이지 → PDF 파일 목록 (과목/종류 필터 적용)"""
    soup = BeautifulSoup(html, "html.parser")

    title_el   = soup.select_one("h3.title, .title, title")
    page_title = title_el.get_text(strip=True) if title_el else ""
    year       = _extract_year(page_title)
    exam_type  = _extract_exam_type(page_title)
    # page title에서 못 찾으면 URL이나 첫 번째 파일명에서 추출 (아래에서 덮어씀)

    files = []
    for fig in soup.select("figure.fileblock"):
        a = fig.select_one("a[href]")
        if not a:
            continue
        pdf_url = a.get("href", "")
        if not pdf_url or "kakaocdn" not in pdf_url:
            continue

        name_el  = fig.select_one(".name, .filename, span")
        raw_name = name_el.get_text(strip=True) if name_el else ""
        if not raw_name:
            raw_name = unquote(pdf_url.split("/")[-1].split("?")[0])

        subject  = _extract_subject_label(raw_name)
        sub_type = _extract_sub_type(raw_name)
        filetype = _extract_filetype(raw_name)

        # 파일명에서 연도/시험 보완 (페이지 제목에서 못 찾은 경우)
        file_year = _extract_year(raw_name) or year
        file_exam = _extract_exam_type(raw_name) or exam_type

        # 과목 필터
        if subj_filter and subj_filter not in subject:
            continue
        # 파일 종류 필터
        if filetype_filter and filetype != filetype_filter:
            continue

        grade = _extract_grade(raw_name, file_exam) or _extract_grade(page_title, file_exam)
        is_combined = subject == "국어" and not sub_type
        display_title = f"{file_year} {file_exam} {subject}({sub_type}) {filetype}" if file_year else raw_name

        files.append({
            "title":         display_title,
            "filename":      raw_name,
            "pdf_url":       pdf_url,
            "year":          file_year,
            "academic_year": _calc_academic_year(file_year, file_exam, raw_name),
            "exam_type":     file_exam,
            "grade":         grade,
            "subject":       subject,
            "sub_type":      sub_type,
            "file_type":     filetype,
            "is_combined":   is_combined,
            "post_url":      post_url,
        })

    return files


# 과목 키워드 매핑 (파일명 → 표준 과목명)
_SUBJ_KEYWORDS = [
    ("국어", ["국어", "언매", "화작", "언어와매체", "화법과작문"]),
    ("영어", ["영어"]),
    ("수학", ["수학", "미적", "기하", "확통"]),
    ("한국사", ["한국사"]),
    ("사회", ["사회", "경제", "지리", "윤리", "역사", "정치", "법"]),
    ("과학", ["물리", "화학", "생명", "지구"]),
]

def _extract_subject_label(filename: str) -> str:
    for label, keywords in _SUBJ_KEYWORDS:
        if any(k in filename for k in keywords):
            return label
    return "기타"

def _extract_sub_type(filename: str) -> str:
    """세부 과목 (언매/화작/기하/미적 등)"""
    if "언매" in filename or "언어와매체" in filename:
        return "언매"
    if "화작" in filename or "화법과작문" in filename:
        return "화작"
    if "미적" in filename:
        return "미적분"
    if "기하" in filename:
        return "기하"
    if "확통" in filename:
        return "확률과통계"
    return ""

def _is_korean_related(title: str) -> bool:
    keywords = ["수능", "모의고사", "모의평가", "학력평가", "국어"]
    return any(k in title for k in keywords)


def _calc_academic_year(year_str: str, exam_type: str, raw_name: str = "") -> str:
    """시행 연도 + 시험 유형 → 학년도 계산.
    파일명에 '학년도'가 이미 포함된 경우 추출 연도가 곧 학년도이므로 +1 생략.
    수능/모의평가: 학년도 = 시행연도 + 1
    학력평가: 학년도 = 시행연도
    """
    if not year_str or not str(year_str).isdigit():
        return year_str or ""
    if "학년도" in raw_name:
        return year_str
    year = int(year_str)
    if exam_type and any(k in exam_type for k in ("수능", "모의", "모평")):
        return str(year + 1)
    return str(year)


def _extract_year(text: str) -> str:
    # 4자리 연도 (학년도/년 접미사 우선)
    m = re.search(r"(20\d{2})(?:학년도|년)", text)
    if m:
        return m.group(1)
    m = re.search(r"(20\d{2})", text)
    if m:
        return m.group(1)
    # 2자리 연도: "25학년도" → "2025"
    m = re.search(r"(\d{2})학년도", text)
    if m:
        return str(2000 + int(m.group(1)))
    return ""


def _extract_exam_type(text: str) -> str:
    if "수능" in text:
        return "수능"
    if "9월" in text and ("모의" in text or "평가" in text or "_" in text):
        return "9월 모의평가"
    if "6월" in text and ("모의" in text or "평가" in text or "_" in text):
        return "6월 모의평가"
    if "3월" in text:
        return "3월 학력평가"
    if "4월" in text:
        return "4월 학력평가"
    if "7월" in text:
        return "7월 학력평가"
    if "10월" in text:
        return "10월 학력평가"
    return ""


def _extract_subject(filename: str) -> str:
    if "언매" in filename or "언어와매체" in filename:
        return "언매"
    if "화작" in filename or "화법과작문" in filename:
        return "화작"
    return "국어"


def _extract_filetype(filename: str) -> str:
    if "정답" in filename or "해설" in filename:
        return "정답해설"
    return "문제"


# 시험 종류 → 학년 기본값 (파일명에서 명시된 경우 우선)
_EXAM_TO_GRADE = {
    "수능": "고3",
    "6월 모의평가": "고3",
    "9월 모의평가": "고3",
    "3월 학력평가": "고3",
    "4월 학력평가": "고3",
    "7월 학력평가": "고3",
    "10월 학력평가": "고3",
}

def _extract_grade(text: str, exam_type: str = "") -> str:
    """파일명 또는 제목에서 학년 추출. 없으면 시험 종류 규칙 적용."""
    if "고1" in text or "1학년" in text:
        return "고1"
    if "고2" in text or "2학년" in text:
        return "고2"
    if "고3" in text or "3학년" in text:
        return "고3"
    return _EXAM_TO_GRADE.get(exam_type, "")


def _check_duplicate(db, academic_year, exam_type: str, sub_type: str) -> bool:
    """동일 학년도+시험+선택과목 조합이 이미 DB에 존재하는지 확인."""
    from app.models.passage import PipelineJob
    if not academic_year or not str(academic_year).isdigit():
        return False
    results = db.query(PipelineJob).filter(
        PipelineJob.source_year == int(academic_year)
    ).all()
    for job in results:
        if exam_type and exam_type in (job.exam_type or ""):
            if sub_type and sub_type in (job.filename or ""):
                return True
    return False
