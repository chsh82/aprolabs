"""
EBS 기출문제 크롤링 라우터
GET  /crawl          → 크롤링 UI
POST /crawl/search   → EBS 기출문제 목록 검색
POST /crawl/import   → 선택 PDF 다운로드 → 파이프라인 등록
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

from app.database import get_db
from app.models.passage import PipelineJob

router = APIRouter(prefix="/crawl")
templates = Jinja2Templates(directory="app/templates")

UPLOAD_DIR = "uploads/suneung"
os.makedirs(UPLOAD_DIR, exist_ok=True)

EBS_BASE = "https://www.ebsi.co.kr"
EBS_LIST_API  = f"{EBS_BASE}/ebs/xip/xipc/previousPaperListAjax.ajax"
EBS_MONTH_API = f"{EBS_BASE}/ebs/xip/xipc/previousPaperMonthGet.ajax"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": f"{EBS_BASE}/ebs/xip/xipc/previousPaperList.ebs?targetCd=D300",
    "Origin": EBS_BASE,
}

# 시험 종류 코드 → 한글 (EBS monthList 값 기준)
EXAM_TYPE_LABEL = {
    "01": "3월 학력평가",
    "04": "4월 학력평가",
    "06": "6월 모의평가",
    "07": "7월 학력평가",
    "09": "9월 모의평가",
    "10": "10월 학력평가",
    "11": "수능",
}

# 과목 코드 (subjList 값, EBS 기준)
SUBJ_LABEL = {
    "": "전체",
    "010101": "국어 (언어와매체/화법과작문)",
}


# ─────────────────────────────────────────────
# GET /crawl
# ─────────────────────────────────────────────
@router.get("", response_class=HTMLResponse)
def crawl_page(request: Request):
    import datetime
    current_year = datetime.datetime.now().year
    years = list(range(current_year, 2004, -1))
    return templates.TemplateResponse("crawl/index.html", {
        "request": request,
        "years": years,
        "exam_types": EXAM_TYPE_LABEL,
    })


# ─────────────────────────────────────────────
# POST /crawl/search  (AJAX)
# ─────────────────────────────────────────────
@router.post("/search")
async def crawl_search(request: Request):
    body = await request.json()
    year = str(body.get("year", "")).strip()
    month_list = body.get("monthList", "")   # 예: "11" (수능) or "" (전체)
    subj_list  = body.get("subjList", "")
    page       = int(body.get("page", 1))
    page_size  = int(body.get("pageSize", 50))

    params = {
        "targetCd":    "D300",
        "beginYear":   year,
        "endYear":     "",
        "monthList":   month_list,
        "subjList":    subj_list,
        "sort":        "recent",
        "currentPage": str(page),
        "pageSize":    str(page_size),
    }

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.post(EBS_LIST_API, data=params, headers=_HEADERS)
        html = resp.text
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

    papers = _parse_paper_list(html)
    return JSONResponse({"ok": True, "papers": papers, "count": len(papers)})


# ─────────────────────────────────────────────
# POST /crawl/import  (AJAX)
# ─────────────────────────────────────────────
@router.post("/import")
async def crawl_import(request: Request, background_tasks: BackgroundTasks,
                       db: Session = Depends(get_db)):
    body = await request.json()
    papers = body.get("papers", [])   # list of {title, pdf_url, year, exam_type, subject}

    if not papers:
        return JSONResponse({"ok": False, "error": "선택된 파일 없음"})

    created = []
    for paper in papers[:20]:
        pdf_url   = paper.get("pdf_url", "").strip()
        title     = paper.get("title", "untitled")
        year      = paper.get("year")
        exam_type = paper.get("exam_type", "")
        subject   = paper.get("subject", "국어")

        if not pdf_url:
            continue

        # PDF 다운로드
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                r = await client.get(pdf_url, headers=_HEADERS)
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

        with open(pdf_path, "wb") as f:
            f.write(r.content)

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
            status="parsing",
        )
        db.add(job)
        db.commit()

        from app.routers.suneung import run_pipeline
        background_tasks.add_task(run_pipeline, job_id, pdf_path)
        created.append({"title": title, "ok": True, "job_id": job_id})

    return JSONResponse({"ok": True, "created": created})


# ─────────────────────────────────────────────
# HTML 파싱 유틸
# ─────────────────────────────────────────────
def _parse_paper_list(html: str) -> list[dict]:
    """EBS previousPaperListAjax HTML → paper 목록"""
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for li in soup.select("div.board_qusesion ul li, ul.board_list li"):
        paper = _parse_li(li)
        if paper:
            items.append(paper)

    # 결과가 없으면 raw HTML을 디버그용으로 포함
    if not items and soup.find("ul"):
        pass  # 빈 결과

    return items


def _parse_li(li) -> dict | None:
    """li 요소에서 제목, PDF URL, 메타 정보 추출"""
    # 제목
    title_el = li.select_one(".tit, .subject, h3, h4, .paper_tit, strong")
    title = title_el.get_text(strip=True) if title_el else li.get_text(" ", strip=True)[:80]

    if not title:
        return None

    # PDF 다운로드 링크 — onclick 속성이나 href에서 추출
    pdf_url = None

    # onclick="... paperId='...' ..." 형태
    for el in li.find_all(attrs={"onclick": True}):
        onclick = el.get("onclick", "")
        # 다운로드 관련 onClick
        if "down" in onclick.lower() or "pdf" in onclick.lower():
            # paperId 추출 시도
            m = re.search(r"paperId['\"]?\s*[:=]\s*['\"]?(\w+)", onclick, re.I)
            if m:
                pid = m.group(1)
                pdf_url = (
                    f"{EBS_BASE}/ebs/xip/xipc/previousPaperDown.ebs"
                    f"?paperId={pid}&fileType=pdf&type=pdf"
                )
                break

    # href 에서 직접 .pdf 또는 down 경로
    if not pdf_url:
        for a in li.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower() or "down" in href.lower():
                pdf_url = href if href.startswith("http") else EBS_BASE + href
                break

    # data- 속성에서 추출
    if not pdf_url:
        for el in li.find_all(True):
            for attr in el.attrs:
                if "paper" in attr.lower() or "id" in attr.lower():
                    val = el[attr]
                    if isinstance(val, str) and re.match(r"^\w{5,}$", val):
                        pdf_url = (
                            f"{EBS_BASE}/ebs/xip/xipc/previousPaperDown.ebs"
                            f"?paperId={val}&fileType=pdf&type=pdf"
                        )
                        break
            if pdf_url:
                break

    # 연도 추출
    year_m = re.search(r"(20\d{2})", title)
    year = year_m.group(1) if year_m else ""

    # 시험 종류 추출
    exam_type = ""
    if "수능" in title:
        exam_type = "수능"
    elif "9월" in title or "9모" in title:
        exam_type = "9월 모의평가"
    elif "6월" in title or "6모" in title:
        exam_type = "6월 모의평가"
    elif "3월" in title:
        exam_type = "3월 학력평가"
    elif "4월" in title:
        exam_type = "4월 학력평가"
    elif "7월" in title:
        exam_type = "7월 학력평가"
    elif "10월" in title:
        exam_type = "10월 학력평가"

    return {
        "title": title,
        "year": year,
        "exam_type": exam_type,
        "subject": "국어",
        "pdf_url": pdf_url or "",
    }
