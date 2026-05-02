"""
실험용 — Gemini Vision 기반 수능 국어 페이지 구조 인식 모듈.
기존 파이프라인과 독립적으로 동작하며, 어떤 기존 서비스도 import하지 않음.
"""
import os
import re
import json

STRUCTURE_PROMPT = """이 PDF 페이지는 수능 국어 시험지의 한 페이지입니다.
페이지의 구조를 분석하여 JSON으로 출력하세요.

## 인식할 요소

1. **지문(passage)**: 문항 묶음의 본문
   - 안내문("[15~17] 다음 글을 읽고 물음에 답하시오." 등)이 있다면 intro_text에 포함
   - (가)/(나)/(다) 라벨이 있으면 passage_label에 표시
   - 본문 첫 5~10단어를 first_words에 기록
   - 어떤 문항들에 해당하는지 covers_questions에 배열로 기록

2. **문항(question)**: 1~45번 문항
   - 발문 첫 5~10단어를 first_words에 기록
   - 보기/그림/표가 있으면 has_visual=true, visual_type 명시
   - 보기가 없으면 has_visual=false

3. **시각 요소(visuals)**: <보기> 박스, 표, 그래프, 그림
   - 어느 문항에 속하는지 belongs_to에 표시
   - 종류를 type에 명시 (bogi_box / table / graph / diagram)

## bbox_hint 작성법
페이지를 9등분(상/중/하 × 좌/중/우)했을 때 위치를 자연어로 기술
예: "top-left", "mid-right", "bottom-center"

## 주의사항
- 헤더/푸터/페이지번호는 무시
- 문항번호가 명시된 것만 question으로 인식
- 정확한 좌표가 아닌 대략적 위치만 표시 (bbox_hint)

JSON만 출력하세요. 설명 없이."""


def analyze_page_structure(page_image_path: str, page_num: int) -> dict:
    """
    PDF 페이지 이미지 1장을 Gemini Vision으로 분석하여 구조 JSON 반환.

    반환 형식:
    {
        "page": 1,
        "passages": [
            {
                "id": "p1_1",
                "intro_text": "[15~17] 다음 글을 읽고 물음에 답하시오.",
                "first_words": "옛날 한 마을에...",
                "passage_label": "(가)",   # 없으면 null
                "covers_questions": [15, 16, 17],
                "bbox_hint": "top-left to mid-right"
            }
        ],
        "questions": [
            {
                "number": 15,
                "first_words": "윗글에 대한 설명으로",
                "has_visual": false,
                "visual_type": null,   # "table", "graph", "diagram", "bogi_box"
                "bbox_hint": "mid-right"
            }
        ],
        "visuals": [
            {
                "type": "bogi_box",
                "belongs_to": "question_17",
                "bbox_hint": "bottom-right",
                "description": "토론 자료 제시 박스"
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

    raw = response.text.strip()
    # 마크다운 코드블록 제거
    raw = re.sub(r'^```[a-z]*\n?', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'\n?```$', '', raw, flags=re.MULTILINE)
    raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {}

    return {
        "page": page_num,
        "passages": data.get("passages", []),
        "questions": data.get("questions", []),
        "visuals": data.get("visuals", []),
    }
