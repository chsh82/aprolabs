from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.database import init_db
from app.routers import questions, upload, suneung, dashboard, answer_keys

app = FastAPI(title="Aprolabs")

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

app.include_router(questions.router)
app.include_router(upload.router)
app.include_router(suneung.router)
app.include_router(dashboard.router)
app.include_router(answer_keys.router)


@app.on_event("startup")
def on_startup():
    from app.models import api_usage  # noqa
    from app.models.passage import ExamPaper  # noqa
    from app.models.answer_key import AnswerKey, AnswerKeyItem  # noqa
    init_db()
