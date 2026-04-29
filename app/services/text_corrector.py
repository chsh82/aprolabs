"""
2차 텍스트 교정 — DB 저장 후 별도 실행
- PDF 페이지 이미지와 저장된 문항 텍스트를 Gemini Vision으로 비교·교정
- 띄어쓰기, 밑줄(<u>), 특수문자 교정
"""
import os
import json

from google import genai
from PIL import Image

CORRECTION_PROMPT = """다음은 수능 국어 시험지 PDF에서 기계적으로 추출한 텍스트입니다.
이 PDF 페이지 이미지를 보고, 아래 텍스트의 오류를 교정해주세요.

교정 규칙:
1. 띄어쓰기: 붙어있는 단어를 올바르게 띄어쓰기
2. 밑줄: PDF에서 밑줄이 그어진 단어/구절을 <u>밑줄텍스트</u>로 표시
3. 특수문자: ㉠㉡㉢, ⓐⓑⓒ, ①②③ 등 원문자 정확히 복원
4. 오탈자: PDF 원본과 다른 글자 교정
5. 원본 구조(문단, 줄바꿈)는 유지

교정된 텍스트만 출력하세요 (설명 없이).

--- 추출된 텍스트 ---
{text}
--- 끝 ---"""


def correct_question(page_image_path: str, question: dict) -> dict:
    """한 문항의 텍스트를 Gemini Vision으로 교정."""
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))
    img = Image.open(page_image_path)

    original_text = question.get("content", "") or question.get("stem", "")
    prompt = CORRECTION_PROMPT.format(text=original_text[:3000])

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[prompt, img],
    )

    corrected = response.text.strip()
    changes = []
    if corrected != original_text:
        changes.append({
            "field": "content",
            "before_length": len(original_text),
            "after_length": len(corrected),
        })

    return {
        "corrected_text": corrected,
        "changes": changes,
        "original_text": original_text,
    }


def correct_job(job_id: str, db_session) -> dict:
    """한 파일(job)의 모든 문항을 Gemini Vision으로 교정."""
    from app.models.passage import PipelineJob

    job = db_session.get(PipelineJob, job_id)
    if not job:
        return {"error": "job not found"}

    segments = json.loads(job.segments) if isinstance(job.segments, str) else (job.segments or {})
    questions = segments.get("questions", [])
    if isinstance(questions, str):
        questions = json.loads(questions)

    img_dir = os.path.join("uploads/suneung", job_id, "pages")
    if not os.path.isdir(img_dir):
        return {"error": "page images not found"}

    page_images = sorted([
        os.path.join(img_dir, f)
        for f in os.listdir(img_dir)
        if f.lower().endswith((".png", ".jpg"))
    ])

    if not page_images:
        return {"error": "no page images found"}

    results = []
    corrected_count = 0

    for q in questions:
        page_idx = min(len(page_images) - 1, (q.get("number", 1) - 1) // 3)
        try:
            result = correct_question(page_images[page_idx], q)
            if result["changes"]:
                q["content"] = result["corrected_text"]
                corrected_count += 1
            results.append({"number": q.get("number"), "changes": result["changes"]})
        except Exception as e:
            results.append({"number": q.get("number"), "error": str(e)})

    segments["questions"] = questions
    job.segments = segments
    db_session.commit()

    return {
        "corrected": corrected_count,
        "total": len(questions),
        "results": results,
    }
