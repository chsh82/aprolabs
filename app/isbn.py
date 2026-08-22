"""
알라딘 Open API 기반 도서 ISBN 검색
GET  /isbn              → 검색 UI (material_id 쿼리파라미터가 있으면 해당 독서논술 교재에 결과를 연결)
GET  /isbn/search       → 제목(+저자)으로 알라딘 상품검색 → ISBN/서지정보 반환
POST /isbn/search-batch → 제목 여러 개(한 줄에 하나)를 병렬로 검색
POST /isbn/save         → 검색 결과 1건을 ReadingMaterial.book_* 필드에 저장

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
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.models.reading_essay import ReadingMaterial
from app.models.isbn_cache import IsbnSearchCache, CACHE_TTL_DAYS

router = APIRouter(prefix="/isbn")
templates = Jinja2Templates(directory="app/templates")

ALADIN_SEARCH_URL = "https://www.aladin.co.kr/ttb/api/ItemSearch.aspx"
BATCH_MAX_TITLES = 30
BATCH_CONCURRENCY = 5


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
        return cached

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

    return templates.TemplateResponse("isbn/index.html", {
        "request": request,
        "material": material,
        "prefill_title": title or (material.title if material else "") or "",
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
