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

    # source_urls 리스트 또는 단일 source_url 모두 수용
    raw_urls = body.get("source_urls") or []
    if not raw_urls:
        single = str(body.get("source_url", "")).strip()
        if single:
            raw_urls = [single]

    # 정규화 + 최대 20개
    def _normalize_url(u: str) -> str:
        u = u.strip()
        if u and not u.startswith("http"):
            u = "https://legendstudy.com/" + u.lstrip("/")
        return u

    source_urls = [_normalize_url(u) for u in raw_urls if u.strip()][:20]

    all_files = []

    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=_HEADERS) as client:

        # ── URL이 주어진 경우 ────────────────────────────────────────────
        if source_urls:
            post_urls   = [u for u in source_urls if re.search(r"legendstudy\.com/\d+$", u)]
            cat_urls    = [u for u in source_urls if u not in post_urls]

            # 개별 글 URL: 병렬 파싱
            if post_urls:
                async def fetch_post(url):
                    try:
                        r = await client.get(url)
                        return _parse_post_files(r.text, url, subj_filter, filetype_filter)
                    except Exception:
                        return []
                post_results = await asyncio.gather(*[fetch_post(u) for u in post_urls])
                for files in post_results:
                    all_files.extend(files)

            # 카테고리 URL: 재귀 스캔 병렬
            for cat_url in cat_urls:
                try:
                    posts = await _scan_category_recursive(
                        client, cat_url, pages_to_scan, depth=0, visited=set(),
                    )
                    if year_filter:
                        posts = [p for p in posts
                                 if _calc_academic_year(p.get("year", ""), p.get("exam_type", "")) == year_filter]
                    posts = [p for p in posts if _is_korean_related(p.get("title", ""))]
                    async def fetch_files(post):
                        try:
                            resp = await client.get(post["url"])
                            return _parse_post_files(resp.text, post["url"], subj_filter, filetype_filter)
                        except Exception:
                            return []
                    cat_results = await asyncio.gather(*[fetch_files(p) for p in posts])
                    for files in cat_results:
                        all_files.extend(files)
                except Exception as e:
                    return JSONResponse({"ok": False, "error": str(e)})

        # ── URL 없음 → 기본 카테고리 스캔 ──────────────────────────────
        else:
            try:
                posts = await _scan_category_recursive(
                    client, CATEGORY_URL, pages_to_scan, depth=0, visited=set(),
                )
            except Exception as e:
                return JSONResponse({"ok": False, "error": str(e)})

            if year_filter:
                posts = [p for p in posts
                         if _calc_academic_year(p.get("year", ""), p.get("exam_type", "")) == year_filter]
            posts = [p for p in posts if _is_korean_related(p.get("title", ""))]

            async def fetch_files_default(post):
                try:
                    resp = await client.get(post["url"])
                    return _parse_post_files(resp.text, post["url"], subj_filter, filetype_filter)
                except Exception:
                    return []
            results = await asyncio.gather(*[fetch_files_default(p) for p in posts])
            for files in results:
                all_files.extend(files)

    all_files = _dedup_files(all_files)
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
        for f in files[:100]:
            pdf_url   = f.get("pdf_url", "").strip()
            title     = f.get("title", "untitled")
            year      = f.get("year")
            exam_type = f.get("exam_type", "")
            subject   = f.get("subject", "국어")
            sub_type  = f.get("sub_type", "") or ""
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
            filename = title if title.lower().endswith(".pdf") else f"{title}.pdf"
            pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}.pdf")

            with open(pdf_path, "wb") as fp:
                fp.write(r.content)

            # 합본 감지 → 분할 후 2개 job 등록
            common_meta = dict(
                source=title,
                source_year=int(year) if str(year).isdigit() else None,
                exam_type=exam_type,
                subject=subject,
                grade=grade or None,
                status="ready",
            )
            if subject == "국어" and sub_type == "통합":
                from app.services.split_combined_pdf import is_combined_exam, split_combined_exam
                if is_combined_exam(pdf_path):  # 합본 분할 활성화
                    splits = split_combined_exam(pdf_path, UPLOAD_DIR)
                    if splits:
                        for sp in splits:
                            sp_id = str(uuid.uuid4())
                            max_num = db.query(func.max(PipelineJob.job_number)).scalar() or 0
                            db.add(PipelineJob(
                                id=sp_id,
                                job_number=max_num + 1,
                                filename=sp["filename"],
                                file_path=sp["path"],
                                sub_type=sp["sub_type"],
                                **common_meta,
                            ))
                            db.commit()
                        created.append({"title": title, "ok": True, "split": True,
                                        "job_ids": [s["filename"] for s in splits]})
                        continue  # 원본 합본 job skip

            max_num = db.query(func.max(PipelineJob.job_number)).scalar() or 0
            db.add(PipelineJob(
                id=job_id,
                job_number=max_num + 1,
                filename=filename,
                file_path=pdf_path,
                sub_type=sub_type or None,
                **common_meta,
            ))
            db.commit()

            created.append({"title": title, "ok": True, "job_id": job_id})

    return JSONResponse({"ok": True, "created": created})


# ─────────────────────────────────────────────
# 카테고리 재귀 탐색
# ─────────────────────────────────────────────
def _extract_subcategory_urls(html: str, current_url: str) -> list[str]:
    """카테고리 페이지에서 하위 카테고리 링크 추출."""
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    seen = set()
    for a in soup.select("a[href*='/category/']"):
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = "https://legendstudy.com" + href
        if not href.startswith("http"):
            continue
        # 현재 URL과 동일하거나 상위 카테고리인 경우 제외
        if href == current_url or href in seen:
            continue
        # 현재 URL이 href의 prefix이면 (하위 카테고리) 포함
        # 현재 URL이 href의 prefix가 아니어도 /category/ 포함이면 포함
        seen.add(href)
        urls.append(href)
    return urls


async def _scan_category_recursive(
    client,
    url: str,
    pages_to_scan: int,
    depth: int = 0,
    max_depth: int = 2,
    visited: set = None,
) -> list[dict]:
    """
    카테고리 페이지를 재귀적으로 탐색하여 게시글 목록 반환.
    - 게시글이 있으면 pages_to_scan 페이지까지 수집 후 반환
    - 게시글이 없고 하위 카테고리가 있으면 각 하위 카테고리 재귀 탐색
    """
    import asyncio
    if visited is None:
        visited = set()
    if url in visited or depth > max_depth:
        return []
    visited.add(url)

    # 1페이지 fetch
    resp = await client.get(url)
    html = resp.text
    posts = _parse_category_page(html)
    print(f"[crawl] 카테고리 {url} (depth={depth}): 게시글 {len(posts)}개 발견")

    if posts:
        # 게시글 발견 → 나머지 페이지도 수집
        all_posts = list(posts)
        for page_num in range(2, pages_to_scan + 1):
            paged_url = url + (f"?page={page_num}" if "?" not in url else f"&page={page_num}")
            if paged_url in visited:
                break
            visited.add(paged_url)
            try:
                r = await client.get(paged_url)
                page_posts = _parse_category_page(r.text)
                print(f"[crawl]   page={page_num}: {len(page_posts)}개")
                if not page_posts:
                    break
                all_posts.extend(page_posts)
            except Exception:
                break
        print(f"[crawl] 총 수집: {len(all_posts)}개 게시글")
        return all_posts

    # 게시글 없음 → 하위 카테고리 탐색
    if depth >= max_depth:
        return []

    sub_urls = _extract_subcategory_urls(html, url)
    print(f"[crawl] 하위 카테고리: {len(sub_urls)}개 → {sub_urls[:5]}")
    if not sub_urls:
        return []

    sub_results = await asyncio.gather(
        *[
            _scan_category_recursive(client, su, pages_to_scan, depth + 1, max_depth, visited)
            for su in sub_urls
        ],
        return_exceptions=True,
    )

    all_posts = []
    for r in sub_results:
        if isinstance(r, list):
            all_posts.extend(r)
    return all_posts


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
        if is_combined:
            sub_type = "통합"
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

    # ── fallback: 옛날 게시글 (daumcdn 직접 <a> 링크) ──────────────────
    if not files:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if not any(d in href for d in ["daumcdn", "kakaocdn"]):
                continue
            if not text.lower().endswith(".pdf"):
                continue

            subject  = _extract_subject_label(text)
            sub_type = _extract_sub_type(text)
            filetype = _extract_filetype(text)
            file_year = _extract_year(text) or year
            file_exam = _extract_exam_type(text) or exam_type

            if subj_filter and subj_filter not in subject:
                continue
            if filetype_filter and filetype != filetype_filter:
                continue

            grade = _extract_grade(text, file_exam) or _extract_grade(page_title, file_exam)
            is_combined = subject == "국어" and not sub_type

            files.append({
                "title":         text,
                "filename":      text,
                "pdf_url":       href,
                "year":          file_year,
                "academic_year": _calc_academic_year(file_year, file_exam, text),
                "exam_type":     file_exam,
                "grade":         grade,
                "subject":       subject,
                "sub_type":      sub_type or ("통합" if is_combined else ""),
                "file_type":     filetype,
                "is_combined":   is_combined,
                "is_duplicate":  False,
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


def _dedup_files(files: list[dict]) -> list[dict]:
    """pdf_url 기준 중복 제거."""
    seen = set()
    result = []
    for f in files:
        key = f.get("pdf_url", "")
        if key and key not in seen:
            seen.add(key)
            result.append(f)
    return result


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
