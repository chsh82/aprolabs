"""
정답 DB 라우터
GET  /suneung/answer-keys              → 목록
POST /suneung/answer-keys/upload       → PDF 업로드 + 파싱 후 저장
POST /suneung/answer-keys/{key_id}/delete → 삭제
GET  /suneung/answer-keys/{key_id}     → 상세
POST /suneung/answer-keys/{key_id}/save  → 문항별 정답 저장 (JSON)
POST /suneung/answer-keys/{key_id}/match → 문제지 매칭
POST /suneung/answer-keys/{key_id}/reparse → Gemini Vision 재파싱
"""
import os
import re
import uuid
import json
import tempfile

from fastapi import APIRouter, Depends, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
import aiofiles

from app.database import get_db
from app.models.answer_key import AnswerKey, AnswerKeyItem
from app.models.question import Question, CATEGORIES, SUBCATEGORIES
from app.services.layout_analyzer import extract_pdf_text

router = APIRouter(prefix="/suneung")
templates = Jinja2Templates(directory="app/templates")
UPLOAD_DIR = "uploads/suneung"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ─────────────────────────────────────────
# 공통 컨텍스트
# ─────────────────────────────────────────

def _base_ctx(db: Session, **kwargs):
    counts = {cat: db.query(Question).filter(Question.category == cat).count()
              for cat in CATEGORIES}
    counts["전체"] = db.query(Question).count()
    sub_counts = {}
    for cat, subs in SUBCATEGORIES.items():
        for sub in subs:
            sub_counts[f"{cat}:{sub}"] = (
                db.query(Question)
                .filter(Question.category == cat, Question.subcategory == sub)
                .count()
            )
    return {
        "categories": CATEGORIES,
        "subcategories": SUBCATEGORIES,
        "category_counts": counts,
        "sub_counts": sub_counts,
        **kwargs,
    }


# ─────────────────────────────────────────
# 정답해설 텍스트 파싱
# ─────────────────────────────────────────

def _extract_answers(text: str) -> dict[int, str]:
    """정답표 파싱: {num: '②'} """
    answers: dict[int, str] = {}
    # "1 ②", "1번 ②", "01. ②", "1) ②" 등
    ANS_RE = re.compile(r'(?<!\d)0?(\d{1,2})\s*[번\.\)\-]?\s*([①②③④⑤])')
    for m in ANS_RE.finditer(text):
        num = int(m.group(1))
        if 1 <= num <= 45 and num not in answers:
            answers[num] = m.group(2)
    # 폴백: 원문자만 나열된 표 형식 (첫 500자)
    if not answers:
        CIRCLE_RE = re.compile(r'[①②③④⑤]')
        for i, m in enumerate(CIRCLE_RE.finditer(text[:800]), 1):
            if i <= 45:
                answers[i] = m.group()
    return answers


def _extract_passage_explanations(text: str) -> dict[tuple, str]:
    """지문해설 구간 파싱: {(start_q, end_q): text}"""
    result: dict[tuple, str] = {}
    # 헤더: [1~3], [01~03], ◆[4~9], ■ 4~9번 등
    HDR_RE = re.compile(
        r'(?:^|[◆◇■□▶•\n])\s*\[?\s*0?(\d{1,2})\s*[~\-～]\s*0?(\d{1,2})\s*\]?[^\n]*\n',
        re.MULTILINE,
    )
    matches = list(HDR_RE.finditer(text))
    for i, m in enumerate(matches):
        s, e = int(m.group(1)), int(m.group(2))
        body_start = m.end()
        body_end   = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[body_start:body_end].strip()

        # 지문해설: 첫 번째 개별 문항 헤더 이전 텍스트
        Q_HDR = re.compile(r'(?:^|\n)\s*0?' + str(s) + r'\s*번')
        qm = Q_HDR.search(body)
        p_text = body[:qm.start()].strip() if qm else body[:500].strip()

        if p_text and len(p_text) > 20:
            result[(s, e)] = p_text
    return result


def _extract_question_explanations(text: str) -> dict[int, str]:
    """문항별 해설 파싱: {num: text}"""
    result: dict[int, str] = {}
    # "N번" 구분자로 분리
    Q_SPLIT = re.compile(r'\n\s*0?(\d{1,2})\s*번[^\n]*\n')
    parts = Q_SPLIT.split(text)
    # split → [before, num, body, num, body, ...]
    for j in range(1, len(parts) - 1, 2):
        try:
            num = int(parts[j])
        except ValueError:
            continue
        if 1 <= num <= 45:
            body = parts[j + 1].strip() if j + 1 < len(parts) else ''
            body = re.sub(r'\s+', ' ', body).strip()
            if body and len(body) > 10:
                result[num] = body[:1500]
    return result


def _parse_answer_key_text(pdf_path: str) -> list[dict]:
    """
    텍스트 추출 기반 정답해설 파싱
    → [{question_number, answer, passage_explanation, explanation}]
    """
    try:
        raw_text, _, _ = extract_pdf_text(pdf_path)
    except Exception:
        return []

    answers      = _extract_answers(raw_text)
    passage_expl = _extract_passage_explanations(raw_text)
    q_expl       = _extract_question_explanations(raw_text)

    all_nums = sorted(set(answers) | set(q_expl))
    if not all_nums:
        return []

    items = []
    for num in all_nums:
        # 이 문항 번호가 속하는 지문해설 찾기
        p_text = next(
            (txt for (s, e), txt in passage_expl.items() if s <= num <= e),
            None,
        )
        items.append({
            "question_number":    num,
            "answer":             answers.get(num),
            "passage_explanation": p_text,
            "explanation":        q_expl.get(num),
        })
    return items


# 구 함수명 유지 (호환)
def _parse_answer_key_pdf(pdf_path: str) -> list[dict]:
    return _parse_answer_key_text(pdf_path)


# ─────────────────────────────────────────
# Gemini Vision 파싱
# ─────────────────────────────────────────

_ANSWER_KEY_PROMPT = """이 이미지는 한국 수능/모의고사 국어 시험의 정답·해설 PDF 페이지입니다.

이 페이지에서 찾을 수 있는 모든 문항의 정답과 해설을 JSON 배열로 추출하세요:
[
  {"question_number": 1, "answer": "③", "explanation": "해설 내용 전체..."},
  ...
]

규칙:
- question_number: 문항 번호 (정수, 1~45)
- answer: 정답 원문자 (①②③④⑤ 중 하나, 없으면 null)
- explanation: 해설 전체 내용 (없으면 null, 줄바꿈은 \\n으로)
- 정답표만 있고 해설이 없으면 explanation을 null로
- 이 페이지에 문항이 없으면 빈 배열 []
- JSON 배열만 출력, 다른 설명 없이"""


def _parse_with_gemini(pdf_path: str) -> list[dict]:
    """Gemini Vision으로 정답·해설 PDF 파싱"""
    try:
        from app.services.pdf_parser import pdf_to_images
        from google import genai
        from PIL import Image

        client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))

        with tempfile.TemporaryDirectory() as tmp_dir:
            pages = pdf_to_images(pdf_path, tmp_dir, dpi=150)
            if not pages:
                return []

            all_items: list[dict] = []
            seen: set[int] = set()

            for page_path in pages:
                img = Image.open(page_path)
                resp = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=[_ANSWER_KEY_PROMPT, img],
                )
                text = resp.text.strip()
                text = re.sub(r'^```[a-z]*\n?', '', text, flags=re.MULTILINE)
                text = re.sub(r'\n?```$', '', text, flags=re.MULTILINE)
                try:
                    page_items = json.loads(text.strip())
                    for item in page_items:
                        num = item.get("question_number")
                        if isinstance(num, int) and 1 <= num <= 45 and num not in seen:
                            seen.add(num)
                            all_items.append(item)
                except Exception:
                    pass

            return sorted(all_items, key=lambda x: x.get("question_number", 0))
    except Exception as e:
        return []


# ─────────────────────────────────────────
# 목록
# ─────────────────────────────────────────

@router.get("/answer-keys", response_class=HTMLResponse)
def answer_keys_list(request: Request, db: Session = Depends(get_db)):
    keys = db.query(AnswerKey).order_by(AnswerKey.created_at.desc()).all()
    ctx = _base_ctx(db, request=request, answer_keys=keys)
    return templates.TemplateResponse("suneung/answer_keys.html", ctx)


# ─────────────────────────────────────────
# 업로드
# ─────────────────────────────────────────

@router.post("/answer-keys/upload")
async def answer_keys_upload(
    request: Request,
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
    source_year: str = Form(""),
    exam_type: str = Form("수능"),
    subject: str = Form("국어"),
):
    if not file or not file.filename:
        return RedirectResponse(url="/suneung/answer-keys", status_code=303)

    key_id = str(uuid.uuid4())
    ext = os.path.splitext(file.filename)[1].lower()
    file_path = os.path.join(UPLOAD_DIR, f"{key_id}_answer{ext}")

    async with aiofiles.open(file_path, "wb") as f:
        await f.write(await file.read())

    key = AnswerKey(
        id=key_id,
        filename=file.filename,
        file_path=file_path,
        source_year=int(source_year) if source_year.isdigit() else None,
        exam_type=exam_type or None,
        subject=subject or None,
        status="unmatched",
    )
    db.add(key)
    db.flush()

    parsed = _parse_answer_key_text(file_path)
    for item_data in parsed:
        db.add(AnswerKeyItem(
            key_id=key_id,
            question_number=item_data["question_number"],
            answer=item_data.get("answer"),
            passage_explanation=item_data.get("passage_explanation"),
            explanation=item_data.get("explanation"),
        ))

    db.commit()
    return RedirectResponse(url="/suneung/answer-keys", status_code=303)


# ─────────────────────────────────────────
# 삭제
# ─────────────────────────────────────────

@router.post("/answer-keys/{key_id}/delete")
def answer_key_delete(key_id: str, db: Session = Depends(get_db)):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")
    if key.file_path and os.path.exists(key.file_path):
        try:
            os.remove(key.file_path)
        except Exception:
            pass
    db.delete(key)
    db.commit()
    return RedirectResponse(url="/suneung/answer-keys", status_code=303)


# ─────────────────────────────────────────
# 상세
# ─────────────────────────────────────────

@router.get("/answer-keys/{key_id}", response_class=HTMLResponse)
def answer_key_detail(key_id: str, request: Request, db: Session = Depends(get_db)):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")
    pdf_url = ("/" + key.file_path.replace("\\", "/")) if key.file_path else None
    ctx = _base_ctx(db, request=request, key=key, pdf_url=pdf_url)
    return templates.TemplateResponse("suneung/answer_key_detail.html", ctx)


@router.post("/answer-keys/{key_id}/reparse")
def answer_key_reparse(key_id: str, db: Session = Depends(get_db)):
    """텍스트 기반 정답·해설 재파싱"""
    key = db.get(AnswerKey, key_id)
    if not key or not key.file_path or not os.path.exists(key.file_path):
        return JSONResponse({"ok": False, "error": "파일 없음"})

    items = _parse_answer_key_text(key.file_path)

    for item in key.items:
        db.delete(item)
    db.flush()

    for item_data in items:
        num = item_data.get("question_number")
        if num is None:
            continue
        db.add(AnswerKeyItem(
            key_id=key_id,
            question_number=int(num),
            answer=item_data.get("answer") or None,
            passage_explanation=item_data.get("passage_explanation") or None,
            explanation=item_data.get("explanation") or None,
        ))

    db.commit()
    return JSONResponse({"ok": True, "count": len(items)})


# ─────────────────────────────────────────
# 문항 저장 (JSON)
# ─────────────────────────────────────────

@router.post("/answer-keys/{key_id}/save")
async def answer_key_save(key_id: str, request: Request, db: Session = Depends(get_db)):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")

    body = await request.json()
    items_data = body.get("items", [])

    # 기존 항목 삭제 후 재삽입
    for item in key.items:
        db.delete(item)
    db.flush()

    for item_data in items_data:
        num = item_data.get("question_number")
        if num is None:
            continue
        db.add(AnswerKeyItem(
            key_id=key_id,
            question_number=int(num),
            answer=item_data.get("answer") or None,
            passage_explanation=item_data.get("passage_explanation") or None,
            explanation=item_data.get("explanation") or None,
        ))

    db.commit()
    return JSONResponse({"ok": True})


# ─────────────────────────────────────────
# 문제지 매칭
# ─────────────────────────────────────────

@router.post("/answer-keys/{key_id}/match")
def answer_key_match(
    key_id: str,
    db: Session = Depends(get_db),
    paper_code: str = Form(...),
):
    key = db.get(AnswerKey, key_id)
    if not key:
        raise HTTPException(status_code=404, detail="Not found")

    key.paper_code = paper_code
    key.status = "matched"

    # 해당 paper_code의 Question들에 answer/explanation 업데이트
    for item in key.items:
        q = (
            db.query(Question)
            .filter(
                Question.paper_code == paper_code,
                Question.question_number == item.question_number,
            )
            .first()
        )
        if q:
            if item.answer:
                q.answer = item.answer
            if item.explanation:
                q.explanation = item.explanation

    db.commit()
    return RedirectResponse(url=f"/suneung/answer-keys/{key_id}", status_code=303)
