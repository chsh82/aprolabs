from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from app.database import get_db, init_db
from app.routers import questions, upload, suneung, dashboard, answer_keys, crawl
from app.routers import auth as auth_router
from app.auth import get_current_user_id

app = FastAPI(title="Aprolabs")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

# 인증 라우터 (로그인/로그아웃 — 보호 불필요)
app.include_router(auth_router.router)

# 보호된 라우터
app.include_router(questions.router)
app.include_router(upload.router)
app.include_router(suneung.router)
app.include_router(dashboard.router)
app.include_router(answer_keys.router)
app.include_router(crawl.router)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """로그인하지 않은 경우 /login 으로 리디렉션"""
    public_paths = {"/login", "/logout"}
    if request.url.path in public_paths or request.url.path.startswith("/static") \
            or request.url.path.startswith("/uploads"):
        return await call_next(request)

    user_id = get_current_user_id(request)
    if not user_id:
        return RedirectResponse(f"/login?next={request.url.path}", status_code=302)

    return await call_next(request)


@app.on_event("startup")
def on_startup():
    from app.models import api_usage  # noqa
    from app.models.passage import ExamPaper  # noqa
    from app.models.answer_key import AnswerKey, AnswerKeyItem  # noqa
    from app.models.user import User  # noqa
    init_db()
    _create_admin()


def _create_admin():
    from app.models.user import User
    from app.auth import hash_password
    db = next(get_db())
    try:
        if not db.query(User).filter(User.email == "admin@aprolabs.co.kr").first():
            admin = User(
                email="admin@aprolabs.co.kr",
                hashed_pw=hash_password("admin0890@"),
                is_admin=True,
            )
            db.add(admin)
            db.commit()
    finally:
        db.close()
