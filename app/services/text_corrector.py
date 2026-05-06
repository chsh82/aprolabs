"""
2차 텍스트 교정 — DB 저장 후 별도 실행
- PDF 페이지 이미지와 저장된 문항 텍스트를 Gemini Vision으로 비교·교정
- 띄어쓰기, 밑줄(<u>), 특수문자 교정
"""
import os
import re
import json

from google import genai
from PIL import Image

CORRECTION_PROMPT = """당신은 수능 국어 시험지 텍스트 교정 전문가입니다.
PDF 페이지 이미지와 기계 추출된 텍스트를 비교하여 교정해주세요.

## 교정 규칙

### 필수 교정
1. 띄어쓰기: 붙어있는 단어를 올바르게 띄어쓰기
2. 전각 공백(　)을 반각 공백으로 변환, 연속 공백 제거
3. 밑줄: PDF에서 밑줄이 그어진 단어/구절을 <u>텍스트</u>로 표시
4. 특수문자: ㉠㉡㉢, ⓐⓑⓒ, ①②③ 등 원문자 정확히 복원
5. 오탈자: PDF 원본과 다른 글자 교정

### 줄바꿈 교정 (매우 중요)
6. 대화체 줄바꿈: 화자가 바뀔 때 반드시 줄바꿈 삽입
   - 예: "습니다. 사회자:" → "습니다.\n사회자:"
   - 예: "합니다. 찬성 1:" → "합니다.\n찬성 1:"
   - 예: "입니다. 면장:" → "입니다.\n면장:"
7. 발표문 지시문: "(자료를 제시하며)", "(화면을 가리키며)" 등 앞에 줄바꿈
   - 예: "습니다. (자료를 제시하며)" → "습니다.\n(자료를 제시하며)"
8. 문단 나누기: PDF 원본의 문단 구조를 정확히 반영
   - 들여쓰기로 시작하는 새 문단 앞에 반드시 빈 줄(\n\n) 삽입
   - PDF에서 줄 간격이 다른 줄보다 넓은 곳에 빈 줄 삽입
   - 한 문단 안의 줄바꿈은 공백으로 연결 (불필요한 줄바꿈 제거)
   - 예시: "...어려운 일이다. 그러나 최근 연구에 따르면..."
     → "...어려운 일이다.\n\n그러나 최근 연구에 따르면..."
     (PDF 원본에서 "그러나"가 들여쓰기로 새 문단을 시작하는 경우)
9. 시(詩)의 행 구분: 각 행을 줄바꿈으로 분리, 연(聯) 사이는 빈 줄

### 절대 하지 말 것
- 원본에 없는 내용 추가 금지
- 원본에 있는 내용 삭제 금지
- [A:START], [B:END] 등 구조 태그 삽입 금지
- 문항번호 안내문("[15~17] 다음 글을..." 등) 삽입 금지
- 선택지 순서 변경 금지

교정된 텍스트만 출력하세요 (설명 없이).
--- 추출된 텍스트 ---
{text}
--- 끝 ---"""


def _validate_correction(original: str, corrected: str) -> tuple[bool, str]:
    """교정 결과 유효성 검증. (ok여부, 사유) 반환."""
    orig_len = max(len(original), 1)
    ratio = len(corrected) / orig_len
    if ratio < 0.7 or ratio > 1.3:
        return False, f"길이 변화 과다 ({ratio:.2f})"

    orig_choices = len(re.findall(r'[①②③④⑤]', original))
    corr_choices = len(re.findall(r'[①②③④⑤]', corrected))
    if orig_choices > 0 and orig_choices != corr_choices:
        return False, f"선택지 개수 변경 ({orig_choices}→{corr_choices})"

    if re.search(r'\[[A-Z가-힣]:(START|END)\]', corrected):
        return False, "구조 태그 오삽입"

    if re.search(r'\[\d+\s*[~∼]\s*\d+\].*?답하시오', corrected, re.DOTALL):
        return False, "안내문 오삽입"

    return True, "ok"


def _find_page_for_question(question: dict, page_images: list) -> int:
    """문항번호 기반으로 해당 페이지 인덱스를 추정."""
    page_num = question.get("page")
    if page_num and 0 <= page_num - 1 < len(page_images):
        return page_num - 1
    num = question.get("number", 1)
    n = len(page_images)
    # 수능 국어 45문항 기준: 1~15번(앞부분), 16~45번(뒷부분)
    if num <= 15:
        return min(n - 1, (num - 1) * n // 45)
    return min(n - 1, num * n // 45)


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

    valid, reason = _validate_correction(original_text, corrected)
    if not valid:
        return {
            "corrected_text": original_text,
            "changes": [],
            "original_text": original_text,
            "skipped": reason,
        }

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
        page_idx = _find_page_for_question(q, page_images)
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
