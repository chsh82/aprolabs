import os
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from app.database import get_db, init_db
from app.routers import questions, upload, suneung, dashboard, answer_keys, crawl, reading_essay, momo_bookshelf, momo_book_review, momo_book_worksheet, momo_worksheet_editor, momo_worksheet_page_editor, journal, zoom_summaries, external_api
from app.routers import auth as auth_router
from app.routers import literacy_admin, literacy_api
from app.vocab.routers import quiz_api as vocab_quiz_api
from app.vocab.routers import attempt_api as vocab_attempt_api
from app.vocabulary_quiz.routers import review as vocabulary_quiz_review
from app import isbn
from app.auth import get_current_user_id

app = FastAPI(title="Aprolabs")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")
app.mount("/momo-images", StaticFiles(directory="momo_book_db/extracted_images"), name="momo_images")
# 학습지 자동 생성 미리보기(momo_book_worksheet.py)용 - 생성된 index.html이
# "../../worksheet/build/..."/"../../extracted_images/..." 같은 상대경로로 CSS·이미지를
# 참조하므로(momo_book_db/generated/<doc_id>/index.html 기준), 그 상대경로가 그대로
# 맞아떨어지도록 momo_book_db 하위 구조를 프리픽스만 바꿔 그대로 미러링해서 마운트한다.
# (momo_book_db 전체를 통째로 마운트하지 않는 이유: momo_book.db·node_modules 등을
# 불필요하게 HTTP로 노출하지 않기 위해 실제로 필요한 3개 하위 폴더만 연다.)
# generated/는 git에 안 올리는 산출물 폴더라(.gitignore) 새로 체크아웃한 서버엔 아예
# 없을 수 있음 - StaticFiles는 마운트 시점에 디렉터리가 없으면 즉시 RuntimeError로 앱
# 전체가 기동 실패하므로(2026-09-17 배포 직후 실제로 이 오류로 서비스가 죽었었음),
# 마운트 전에 없으면 만들어 둔다.
os.makedirs("momo_book_db/generated", exist_ok=True)
app.mount("/momo-worksheet-assets/generated", StaticFiles(directory="momo_book_db/generated"), name="momo_worksheet_generated")
app.mount("/momo-worksheet-assets/worksheet/build", StaticFiles(directory="momo_book_db/worksheet/build"), name="momo_worksheet_build")
app.mount("/momo-worksheet-assets/extracted_images", StaticFiles(directory="momo_book_db/extracted_images"), name="momo_worksheet_images")
# 2026-09-19 12차(페이지 편집기) 안정화 - 기존에도 있던 버그를 여기서 발견해 같이 고침:
# momo_worksheet_editor.py의 편집 프로젝트 미리보기 라우트
# (/momo-worksheet-editor/{project_id}/preview/{version_id}/{filename})는 실제 파일
# 깊이(momo_book_db 기준 5단계 아래)에 맞춰 "../../../../../worksheet/build/styles.css"
# 같은 상대경로를 그대로 서빙하는데, 이 라우트의 URL 경로는 4단계 깊이뿐이라 브라우저가
# "../"를 루트에서 그 이상 못 올라가고 클램프해 최종적으로 "/worksheet/build/styles.css"
# (루트 바로 아래)를 요청한다 - 그런데 그 경로엔 지금까지 아무 마운트도 없어서 편집
# 프로젝트 미리보기 화면은 CSS·이미지가 전부 404였다(PDF 다운로드는 파일 시스템 경로를
# 직접 여는 별도 코드 경로라 안 걸렸음 - 실제 PDF는 늘 정상이었다). 페이지 편집기의
# 후보 미리보기(page_proposals/.../preview/)도 같은 클램프 대상이라 이 마운트가 꼭
# 필요하다. momo_book_db 전체를 열지 않고 실제 쓰는 2개 하위 폴더만 루트에 미러링한다.
app.mount("/worksheet/build", StaticFiles(directory="momo_book_db/worksheet/build"), name="worksheet_build_root")
app.mount("/extracted_images", StaticFiles(directory="momo_book_db/extracted_images"), name="extracted_images_root")
app.mount("/vocab/games", StaticFiles(directory="app/vocab/static/games"), name="vocab_games")

# 인증 라우터 (로그인/로그아웃 — 보호 불필요)
app.include_router(auth_router.router)

# 보호된 라우터
app.include_router(questions.router)
app.include_router(upload.router)
app.include_router(suneung.router)
app.include_router(dashboard.router)
app.include_router(answer_keys.router)
app.include_router(crawl.router)
app.include_router(reading_essay.router)
app.include_router(isbn.router)
app.include_router(momo_bookshelf.router)
app.include_router(momo_book_review.router)
app.include_router(momo_book_worksheet.router)
app.include_router(momo_worksheet_editor.router)
app.include_router(momo_worksheet_page_editor.router)
app.include_router(journal.router)
app.include_router(zoom_summaries.router)
app.include_router(literacy_admin.router)
app.include_router(literacy_api.router)
app.include_router(vocab_quiz_api.router)
app.include_router(vocab_attempt_api.router)
app.include_router(vocabulary_quiz_review.router)

# 외부 서비스(momoai_web) 연동용 - 세션 쿠키가 아니라 X-API-Key로 자체 인증하므로
# 아래 auth_middleware의 로그인 리디렉션 대상에서 빼야 한다(안 빼면 미로그인
# 요청이 API 응답 대신 /login 302로 가로채인다).
app.include_router(external_api.router)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """로그인하지 않은 경우 /login 으로 리디렉션"""
    public_paths = {"/login", "/logout"}
    if request.url.path in public_paths or request.url.path.startswith("/static") \
            or request.url.path.startswith("/uploads") \
            or request.url.path.startswith("/api/external"):
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
    from app.models import reading_essay  # noqa
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
                hashed_pw=hash_password("apro0914@"),
                is_admin=True,
            )
            db.add(admin)
            db.commit()
    finally:
        db.close()
