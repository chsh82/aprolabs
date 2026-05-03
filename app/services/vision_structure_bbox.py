"""
실험용 — Gemini Vision bbox 좌표 기반 수능 국어 페이지 구조 인식 모듈.
vision_structure.py와 독립적으로 동작하며, 기존 서비스 import 없음.
"""
import os
import re
import sys
import json

BBOX_PROMPT = """시험지 PDF 페이지의 모든 의미 요소를 감지하고 정규화된 bbox 좌표를 반환하세요.

## bbox 좌표 형식
- [ymin, xmin, ymax, xmax]
- 모든 값은 0~1000 범위로 정규화
- ymin/ymax는 페이지 상단 0, 하단 1000
- xmin/xmax는 페이지 좌측 0, 우측 1000

## 감지할 요소

각 요소를 elements 배열에 객체로 추가:
- type: "passage" (지문), "question" (문항), "visual" (보기/표/그림)
- label: 한국어 설명 ("지문 (가)", "문항 15번", "<보기> 박스" 등)
- first_5_words: 영역의 첫 5단어 (정확히 5개)
- bbox: [ymin, xmin, ymax, xmax] 정규화 좌표
- metadata: 추가 정보 (passage는 covers_questions 배열, question은 number와 has_visual, visual은 belongs_to_question)

## 출력 형식 (JSON 객체만)

{
  "elements": [
    {
      "type": "passage",
      "label": "지문 (가)",
      "first_5_words": "옛날 어느 마을에 한",
      "bbox": [50, 30, 450, 480],
      "metadata": {"covers_questions": [15, 16, 17]}
    },
    {
      "type": "question",
      "label": "문항 15번",
      "first_5_words": "윗글에 대한 설명으로 적절한",
      "bbox": [460, 30, 520, 480],
      "metadata": {"number": 15, "has_visual": false}
    }
  ]
}

## 규칙
- 헤더/푸터/페이지번호 제외
- bbox 좌표는 반드시 ymin < ymax, xmin < xmax
- 본문 텍스트 복사 금지 (first_5_words만)
- JSON만 출력, 마크다운 코드블록 금지"""


def _parse_response(raw: str) -> dict:
    """마크다운 코드블록 제거 후 JSON 파싱. 실패 시 빈 구조 반환."""
    text = raw.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```\s*$', '', text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"[vision_structure_bbox] JSON 파싱 실패. 응답 앞 200자:\n{text[:200]}", file=sys.stderr)
        return {}


def _validate_and_convert_bbox(elem: dict, image_width: int, image_height: int) -> dict | None:
    """bbox 검증 + 정규화 좌표 → 픽셀 좌표 변환. 검증 실패 시 None 반환."""
    bbox = elem.get("bbox")
    if not isinstance(bbox, list) or len(bbox) != 4:
        return None

    ymin, xmin, ymax, xmax = bbox

    if not all(isinstance(v, (int, float)) and 0 <= v <= 1000 for v in bbox):
        return None

    if ymin >= ymax or xmin >= xmax:
        return None

    # 면적 0.01% 미만은 무효
    if (ymax - ymin) * (xmax - xmin) < 100:
        return None

    elem["bbox_norm"] = bbox
    elem["bbox_pixel"] = [
        int(xmin * image_width / 1000),
        int(ymin * image_height / 1000),
        int(xmax * image_width / 1000),
        int(ymax * image_height / 1000),
    ]
    return elem


def visualize_bboxes(page_image_path: str, result: dict, output_path: str):
    """
    페이지 이미지에 감지된 bbox를 색상 박스로 그려서 저장.
    passage=빨강, question=파랑, visual=초록
    """
    from PIL import Image, ImageDraw

    img = Image.open(page_image_path).convert("RGB")
    draw = ImageDraw.Draw(img)

    colors = {"passage": "red", "question": "blue", "visual": "green"}

    for elem in result.get("elements", []):
        if "bbox_pixel" not in elem:
            continue
        x0, y0, x1, y1 = elem["bbox_pixel"]
        color = colors.get(elem.get("type"), "gray")
        draw.rectangle([x0, y0, x1, y1], outline=color, width=3)
        label = elem.get("label", "")[:30]
        draw.text((x0 + 5, y0 + 5), label, fill=color)

    img.save(output_path)


def analyze_page_with_bbox(page_image_path: str, page_num: int) -> dict:
    """
    Gemini Vision으로 페이지의 구조를 bbox 좌표와 함께 추출.

    반환:
    {
        "page": 1,
        "image_size": {"width": 1700, "height": 2200},
        "elements": [
            {
                "type": "passage" | "question" | "visual",
                "label": "지문 (가)" | "문항 15번" | "<보기> 박스",
                "first_5_words": "...",
                "bbox_norm": [ymin, xmin, ymax, xmax],
                "bbox_pixel": [x0, y0, x1, y1],
                "metadata": {...}
            }
        ]
    }
    """
    from google import genai
    from PIL import Image

    img = Image.open(page_image_path)
    image_width, image_height = img.size

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[BBOX_PROMPT, img],
    )

    if not response.text:
        print(f"[vision_structure_bbox] 빈 응답 (page {page_num})", file=sys.stderr)
        return {"page": page_num, "image_size": {"width": image_width, "height": image_height}, "elements": []}

    data = _parse_response(response.text)
    raw_elements = data.get("elements", [])

    elements = []
    skipped = 0
    for elem in raw_elements:
        if not isinstance(elem, dict):
            print(f"[vision_structure_bbox] elements 항목이 dict가 아님(무시): {repr(elem)[:80]}", file=sys.stderr)
            skipped += 1
            continue
        converted = _validate_and_convert_bbox(elem, image_width, image_height)
        if converted is None:
            print(f"[vision_structure_bbox] bbox 검증 실패(무시): {elem.get('label','')} bbox={elem.get('bbox')}", file=sys.stderr)
            skipped += 1
            continue
        elements.append(converted)

    if skipped:
        print(f"[vision_structure_bbox] page {page_num}: {skipped}개 요소 스킵됨", file=sys.stderr)

    return {
        "page": page_num,
        "image_size": {"width": image_width, "height": image_height},
        "elements": elements,
    }
