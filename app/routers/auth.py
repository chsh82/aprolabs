from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.user import User
from app.auth import (
    verify_password, make_session_cookie,
    get_current_user_id, COOKIE_NAME, COOKIE_MAX_AGE, COOKIE_MAX_AGE_LONG
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/"):
    # 이미 로그인된 경우 바로 이동
    if get_current_user_id(request):
        return RedirectResponse(next, status_code=302)
    return templates.TemplateResponse(request, "login.html",
                                      {"request": request, "next": next, "error": None})


@router.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form(default="/"),
    remember: str = Form(default=""),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.email == email).first()
    if not user or not verify_password(password, user.hashed_pw):
        return templates.TemplateResponse(request, "login.html", {
            "request": request,
            "next": next,
            "error": "이메일 또는 비밀번호가 올바르지 않습니다.",
        }, status_code=401)

    token = make_session_cookie(user.id)
    max_age = COOKIE_MAX_AGE_LONG if remember == "on" else COOKIE_MAX_AGE
    response = RedirectResponse(next or "/", status_code=302)
    response.set_cookie(COOKIE_NAME, token, max_age=max_age,
                        httponly=True, samesite="lax", domain=".aprolabs.co.kr")
    return response


@router.get("/logout")
def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie(COOKIE_NAME, domain=".aprolabs.co.kr")
    # 2026-09-28 23:15(28643e7) 이전에 발급된 세션 쿠키는 domain 속성이 없는
    # host-only 쿠키다 - domain=".aprolabs.co.kr" 삭제만으로는 그 쿠키가
    # 지워지지 않아 로그아웃이 실패한 것처럼 보인다(실측으로 재현·확인함).
    # 두 형태를 모두 지워야 이전 세션도 확실히 로그아웃된다.
    response.delete_cookie(COOKIE_NAME)
    return response
