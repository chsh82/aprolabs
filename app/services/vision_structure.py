"""
실험용 — Gemini Vision 기반 수능 국어 페이지 구조 인식 모듈.
기존 파이프라인과 독립적으로 동작하며, 어떤 기존 서비스도 import하지 않음.
"""
import os
import re
import sys
import json

STRUCTURE_PROMPT = """당신은 시험지 PDF 페이지의 메타구조를 인식하는 분석기입니다.
본문 텍스트를 추출하지 마세요. **위치와 분류 정보만** 반환하세요.

## 출력 형식 (JSON 객체, 배열 아님)

{
  "passages": [
    {
      "id": "p1",
      "intro_snippet": "[15~17] 다음 글을...",
      "first_5_words": "옛날 어느 마을에 한",
      "covers_questions": [15, 16, 17],
      "position": "top-left"
    }
  ],
  "questions": [
    {
      "number": 15,
      "first_5_words": "윗글에 대한 설명으로 적절한",
      "has_visual": false,
      "position": "mid-left"
    }
  ],
  "visuals": [
    {
      "type": "bogi_box",
      "belongs_to_question": 17,
      "position": "bottom-right"
    }
  ]
}

## 규칙

1. 본문 텍스트 전체를 복사하지 마세요. first_5_words는 정확히 5단어만.
2. passages/questions/visuals는 모두 **객체의 배열**이어야 합니다. 문자열 배열 금지.
3. 페이지에 해당 요소가 없으면 빈 배열 [].
4. position은 "top/mid/bottom" + "-left/-center/-right" 조합.
5. 헤더, 푸터, 페이지번호 무시.
6. JSON만 출력. 설명, 마크다운 코드블록 모두 금지."""

_EMPTY = {"passages": [], "questions": [], "visuals": []}


def _parse_response(raw: str) -> dict:
    """마크다운 코드블록 제거 후 JSON 파싱. 실패 시 빈 구조 반환."""
    text = raw.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```\s*$', '', text)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"[vision_structure] JSON 파싱 실패. 응답 앞 200자:\n{text[:200]}", file=sys.stderr)
        return {}


def _validate_array(arr: list, field: str) -> list:
    """배열의 각 항목이 dict인지 검증. 문자열 등 비정상 항목은 경고 후 제거."""
    valid = []
    for item in arr:
        if isinstance(item, dict):
            valid.append(item)
        else:
            print(f"[vision_structure] {field} 항목이 dict가 아님(무시): {repr(item)[:80]}", file=sys.stderr)
    return valid


def analyze_page_structure(page_image_path: str, page_num: int) -> dict:
    """
    PDF 페이지 이미지 1장을 Gemini Vision으로 분석하여 구조 JSON 반환.

    반환 형식:
    {
        "page": 1,
        "passages": [
            {
                "id": "p1",
                "intro_snippet": "[15~17] 다음 글을...",
                "first_5_words": "옛날 어느 마을에 한",
                "covers_questions": [15, 16, 17],
                "position": "top-left"
            }
        ],
        "questions": [
            {
                "number": 15,
                "first_5_words": "윗글에 대한 설명으로 적절한",
                "has_visual": false,
                "position": "mid-left"
            }
        ],
        "visuals": [
            {
                "type": "bogi_box",
                "belongs_to_question": 17,
                "position": "bottom-right"
            }
        ]
    }
    """
    from google import genai
    from PIL import Image

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))
    img = Image.open(page_image_path)

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[STRUCTURE_PROMPT, img],
    )

    if not response.text:
        print(f"[vision_structure] 빈 응답 (page {page_num})", file=sys.stderr)
        return {"page": page_num, **_EMPTY}

    data = _parse_response(response.text)

    return {
        "page": page_num,
        "passages": _validate_array(data.get("passages", []), "passages"),
        "questions": _validate_array(data.get("questions", []), "questions"),
        "visuals":   _validate_array(data.get("visuals", []),   "visuals"),
    }
