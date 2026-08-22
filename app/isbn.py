"""
알라딘 Open API 기반 도서 ISBN 검색
GET  /isbn              → 검색 UI (material_id 쿼리파라미터가 있으면 해당 독서논술 교재에 결과를 연결)
GET  /isbn/search       → 제목(+저자)으로 알라딘 상품검색 → ISBN/서지정보 반환
POST /isbn/search-batch → 제목 여러 개(한 줄에 하나)를 병렬로 검색
POST /isbn/save         → 검색 결과 1건을 ReadingMaterial.book_* 필드에 저장
GET  /isbn/list         → 독서 리스트(체크해서 모아둔 도서) 목록
POST /isbn/list/add     → 검색 결과 여러 건을 독서 리스트에 추가 (중복 ISBN 자동 제외)
POST /isbn/list/{id}/delete → 독서 리스트에서 1건 삭제

동일한 (title, author, limit) 검색은 isbn_search_cache 테이블에 캐시되어
TTL(기본 7일) 동안 재호출 없이 재사용됨 (알라딘 API 호출량 절약).
"""
import os
import json
import re
import asyncio
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.models.reading_essay import ReadingMaterial
from app.models.isbn_cache import IsbnSearchCache, CACHE_TTL_DAYS
from app.models.reading_list import ReadingListBook

router = APIRouter(prefix="/isbn")
templates = Jinja2Templates(directory="app/templates")

ALADIN_SEARCH_URL = "https://www.aladin.co.kr/ttb/api/ItemSearch.aspx"
BATCH_MAX_TITLES = 30
BATCH_CONCURRENCY = 5

# 독서논술 교재 제목에 흔히 붙는 수업 메타데이터 노이즈 (앞: 학년/읽기레벨/주차, 뒤: 교사용·수업준비·강사코드 등)
_LEADING_TITLE_NOISE_RE = re.compile(r"^\s*(?:(?:초|중|고)[1-6]_?|LV\s*\d+|\d+\s*(?:분기|주차|차시))\s*")
_TRAILING_TITLE_NOISE_TOKENS = [
    r"[가-힣]{1,4}T",       # 담당 강사 코드 (예: 문요한T, 문T)
    r"교사용|학생용",
    r"수업\s*준비",
    r"\d+\s*(?:주차|차시)",
    r"\d+",                 # 위 패턴들을 뗀 뒤 남는 단독 숫자
]


def _clean_material_title_for_search(title: str) -> str:
    """
    교재 제목에서 검색을 방해하는 수업 메타데이터를 제거해 도서 검색에 적합하게 정리.
    예: "금오신화 1주차 수업준비" -> "금오신화", "LV 2 아빠사자와 행복한 아이들" -> "아빠사자와 행복한 아이들"
    책 속 문장/저자명이 제목에 그대로 이어붙은 경우 등은 안전하게 분리할 수 없어 원본을 그대로 둠.
    """
    t = (title or "").strip()
    t = _LEADING_TITLE_NOISE_RE.sub("", t, count=1)

    changed = True
    while changed:
        changed = False
        for token in _TRAILING_TITLE_NOISE_TOKENS:
            m = re.search(rf"[\s_]*(?:{token})[\s_]*$", t)
            if m and m.start() > 0:
                t = t[:m.start()]
                changed = True
                break

    cleaned = t.strip(" _'\"‘’“”")
    return cleaned if cleaned else (title or "").strip()


def _cache_key(title: str, author: str | None, limit: int) -> str:
    return f"{title}|{author or ''}|{limit}"


def _cache_get(key: str) -> dict | None:
    """캐시 조회. TTL 만료됐거나 없으면 None (알라딘 API 호출량 절약용, 세션은 요청과 독립적으로 짧게 열고 닫음)"""
    db = SessionLocal()
    try:
        row = db.query(IsbnSearchCache).filter(IsbnSearchCache.cache_key == key).first()
        if not row or datetime.now() - row.created_at > timedelta(days=CACHE_TTL_DAYS):
            return None
        try:
            return json.loads(row.response_json)
        except json.JSONDecodeError:
            return None
    finally:
        db.close()


def _cache_set(key: str, data: dict) -> None:
    db = SessionLocal()
    try:
        payload = json.dumps(data, ensure_ascii=False)
        row = db.query(IsbnSearchCache).filter(IsbnSearchCache.cache_key == key).first()
        if row:
            row.response_json = payload
            row.created_at = datetime.now()
        else:
            db.add(IsbnSearchCache(cache_key=key, response_json=payload, created_at=datetime.now()))
        db.commit()
    finally:
        db.close()


def _clean_author(author_raw: str) -> str:
    """"우지영 (지은이), 김은재 (그림)" → "우지영, 김은재" """
    return ", ".join(
        re.sub(r"\s*\([^)]*\)\s*$", "", p.strip())
        for p in (author_raw or "").split(",")
        if p.strip()
    )


async def _search_aladin(client: httpx.AsyncClient, title: str, author: str | None, limit: int) -> dict:
    """알라딘 상품검색 호출 + 파싱 (실패 시 HTTPException 발생). 동일 검색은 캐시로 응답."""
    key = _cache_key(title, author, limit)
    cached = _cache_get(key)
    if cached is not None:
        print(f"[ISBN Cache] hit: {key}")
        return cached
    print(f"[ISBN Cache] miss, 알라딘 호출: {key}")

    ttb_key = os.getenv("ALADIN_TTB_KEY")
    if not ttb_key:
        raise HTTPException(status_code=500, detail="ALADIN_TTB_KEY 환경변수가 설정되지 않았습니다.")

    query = f"{title} {author}" if author else title
    query_type = "Keyword" if author else "Title"

    params = {
        "ttbkey": ttb_key,
        "Query": query,
        "QueryType": query_type,
        "SearchTarget": "Book",
        "MaxResults": limit,
        "start": 1,
        "output": "js",
        "Version": "20131101",
    }

    try:
        res = await client.get(ALADIN_SEARCH_URL, params=params)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"알라딘 API 호출에 실패했습니다: {e}")

    try:
        data = json.loads(res.text.strip().rstrip(";"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="알라딘 API 응답을 해석할 수 없습니다.")

    if data.get("errorCode"):
        raise HTTPException(
            status_code=502,
            detail=f"알라딘 API 오류 (errorCode: {data.get('errorCode')}): {data.get('errorMessage')}",
        )

    results = [
        {
            "isbn13": item.get("isbn13", ""),
            "isbn10": item.get("isbn", ""),
            "title": item.get("title", ""),
            "author": _clean_author(item.get("author", "")),
            "publisher": item.get("publisher", ""),
            "pubDate": item.get("pubDate", ""),
            "cover": item.get("cover", ""),
            "link": item.get("link", ""),
        }
        for item in data.get("item", [])
    ]

    result = {"query": query, "count": len(results), "results": results}
    _cache_set(key, result)
    return result


@router.get("", response_class=HTMLResponse)
def isbn_page(
    request: Request,
    material_id: str | None = Query(None),
    title: str | None = Query(None),
    author: str | None = Query(None),
    db: Session = Depends(get_db),
):
    """ISBN 검색 UI. material_id가 있으면 해당 독서논술 교재에 결과를 연결하는 모드로 동작."""
    material = None
    if material_id:
        material = db.query(ReadingMaterial).filter(ReadingMaterial.id == material_id).first()
        if not material:
            raise HTTPException(status_code=404, detail="교재를 찾을 수 없습니다.")

    raw_title = title or (material.title if material else "") or ""
    # 교재 연결 모드일 때만 "N주차_교사용" 같은 수업 메타데이터를 제거 (직접 입력한 검색어는 그대로 둠)
    prefill_title = _clean_material_title_for_search(raw_title) if material else raw_title

    return templates.TemplateResponse("isbn/index.html", {
        "request": request,
        "material": material,
        "prefill_title": prefill_title,
        "prefill_author": author or (material.author if material else "") or "",
    })


@router.get("/search")
async def isbn_search(
    title: str = Query(..., min_length=1),
    author: str | None = Query(None),
    limit: int = Query(5, ge=1, le=10),
):
    """제목(+저자)으로 알라딘 상품검색 → ISBN/서지정보 반환"""
    async with httpx.AsyncClient(timeout=10) as client:
        return await _search_aladin(client, title, author, limit)


@router.post("/search-batch")
async def isbn_search_batch(request: Request):
    """제목 여러 개(한 줄에 하나)를 병렬로 검색. 항목별 실패는 error 필드로 표시하고 나머지는 계속 진행."""
    body = await request.json()
    raw_text = body.get("text", "") or ""
    limit = min(max(int(body.get("limit", 3) or 3), 1), 5)

    titles = list(dict.fromkeys(
        line.strip() for line in raw_text.splitlines() if line.strip()
    ))

    if not titles:
        raise HTTPException(status_code=400, detail="검색할 제목을 한 줄에 하나씩 입력하세요.")
    if len(titles) > BATCH_MAX_TITLES:
        raise HTTPException(
            status_code=400,
            detail=f"한 번에 최대 {BATCH_MAX_TITLES}개까지 검색할 수 있습니다. (입력된 제목: {len(titles)}개)",
        )

    semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

    async def _search_one(client: httpx.AsyncClient, t: str) -> dict:
        async with semaphore:
            try:
                res = await _search_aladin(client, t, None, limit)
                return {"title": t, "count": res["count"], "results": res["results"], "error": None}
            except HTTPException as e:
                return {"title": t, "count": 0, "results": [], "error": e.detail}

    async with httpx.AsyncClient(timeout=10) as client:
        items = await asyncio.gather(*[_search_one(client, t) for t in titles])

    return {"items": items}


@router.post("/save")
async def isbn_save(request: Request, db: Session = Depends(get_db)):
    """검색 결과 1건을 독서논술 교재(ReadingMaterial)의 book_* 필드에 저장"""
    body = await request.json()
    material_id = (body.get("material_id") or "").strip()
    if not material_id:
        raise HTTPException(status_code=400, detail="material_id가 필요합니다.")

    material = db.query(ReadingMaterial).filter(ReadingMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404, detail="교재를 찾을 수 없습니다.")

    material.book_isbn13 = body.get("isbn13") or None
    material.book_isbn10 = body.get("isbn10") or None
    material.book_publisher = body.get("publisher") or None
    material.book_pub_date = body.get("pubDate") or None
    material.book_cover_url = body.get("cover") or None
    material.book_aladin_link = body.get("link") or None
    db.commit()

    return JSONResponse({"ok": True, "material_id": material_id})


@router.get("/list", response_class=HTMLResponse)
def isbn_list_page(request: Request, db: Session = Depends(get_db)):
    """독서 리스트(체크해서 모아둔 도서) 목록"""
    books = db.query(ReadingListBook).order_by(ReadingListBook.created_at.desc()).all()
    return templates.TemplateResponse("isbn/list.html", {"request": request, "books": books})


@router.post("/list/add")
async def isbn_list_add(request: Request, db: Session = Depends(get_db)):
    """검색 결과 여러 건을 독서 리스트에 추가. 이미 있는 ISBN은 건너뜀."""
    body = await request.json()
    items = body.get("items") or []
    if not items:
        raise HTTPException(status_code=400, detail="추가할 항목을 선택하세요.")

    existing_isbns = {
        v for row in db.query(ReadingListBook.isbn13, ReadingListBook.isbn10).all()
        for v in row if v
    }

    added, skipped = 0, 0
    for item in items:
        title = (item.get("title") or "").strip()
        isbn13 = (item.get("isbn13") or "").strip() or None
        isbn10 = (item.get("isbn10") or "").strip() or None
        if not title:
            skipped += 1
            continue
        if (isbn13 and isbn13 in existing_isbns) or (isbn10 and isbn10 in existing_isbns):
            skipped += 1
            continue

        db.add(ReadingListBook(
            title=title,
            isbn13=isbn13,
            isbn10=isbn10,
            author=item.get("author") or None,
            publisher=item.get("publisher") or None,
            pub_date=item.get("pubDate") or None,
            cover_url=item.get("cover") or None,
            aladin_link=item.get("link") or None,
        ))
        if isbn13:
            existing_isbns.add(isbn13)
        if isbn10:
            existing_isbns.add(isbn10)
        added += 1

    db.commit()
    return JSONResponse({"ok": True, "added": added, "skipped": skipped})


@router.post("/list/{book_id}/delete")
def isbn_list_delete(book_id: str, db: Session = Depends(get_db)):
    """독서 리스트에서 1건 삭제"""
    book = db.query(ReadingListBook).filter(ReadingListBook.id == book_id).first()
    if not book:
        raise HTTPException(status_code=404, detail="항목을 찾을 수 없습니다.")
    db.delete(book)
    db.commit()
    return RedirectResponse(url="/isbn/list", status_code=303)
