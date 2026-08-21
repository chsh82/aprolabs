"""
알라딘 Open API 기반 도서 ISBN 검색
GET /isbn         → 검색 UI
GET /isbn/search  → 제목(+저자)으로 알라딘 상품검색 → ISBN/서지정보 반환
"""
import os
import json
import re

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/isbn")
templates = Jinja2Templates(directory="app/templates")

ALADIN_SEARCH_URL = "https://www.aladin.co.kr/ttb/api/ItemSearch.aspx"


def _clean_author(author_raw: str) -> str:
    """"우지영 (지은이), 김은재 (그림)" → "우지영, 김은재" """
    return ", ".join(
        re.sub(r"\s*\([^)]*\)\s*$", "", p.strip())
        for p in (author_raw or "").split(",")
        if p.strip()
    )


@router.get("", response_class=HTMLResponse)
def isbn_page(request: Request):
    """ISBN 검색 UI"""
    return templates.TemplateResponse("isbn/index.html", {"request": request})


@router.get("/search")
async def isbn_search(
    title: str = Query(..., min_length=1),
    author: str | None = Query(None),
    limit: int = Query(5, ge=1, le=10),
):
    """제목(+저자)으로 알라딘 상품검색 → ISBN/서지정보 반환"""
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
        async with httpx.AsyncClient(timeout=10) as client:
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

    results = []
    for item in data.get("item", []):
        results.append({
            "isbn13": item.get("isbn13", ""),
            "isbn10": item.get("isbn", ""),
            "title": item.get("title", ""),
            "author": _clean_author(item.get("author", "")),
            "publisher": item.get("publisher", ""),
            "pubDate": item.get("pubDate", ""),
            "cover": item.get("cover", ""),
            "link": item.get("link", ""),
        })

    return {"query": query, "count": len(results), "results": results}
