"""
알라딘 Open API 기반 도서 ISBN 검색
GET  /isbn         → 검색 UI (material_id 쿼리파라미터가 있으면 해당 독서논술 교재에 결과를 연결)
GET  /isbn/search  → 제목(+저자)으로 알라딘 상품검색 → ISBN/서지정보 반환
POST /isbn/save    → 검색 결과 1건을 ReadingMaterial.book_* 필드에 저장
"""
import os
import json
import re

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.reading_essay import ReadingMaterial

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
