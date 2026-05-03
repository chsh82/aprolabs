"""
실험용 — Vision 의미 분류 + PyMuPDF 좌표 역추적 하이브리드 모듈.
Gemini는 first_5_words + 의미 분류만, 실제 좌표는 PyMuPDF가 담당.
기존 서비스 import 없음.
"""
import os
import re
import sys
import json

import fitz  # PyMuPDF

ANCHOR_PROMPT = """당신은 시험지 PDF 페이지의 메타구조를 인식하는 분석기입니다.
본문 텍스트를 추출하지 마세요. **분류 정보와 첫 5단어만** 반환하세요.

## 출력 형식 (JSON 객체만)

{
  "elements": [
    {
      "type": "passage",
      "label": "지문 (가)",
      "first_5_words": "옛날 어느 마을에 한",
      "metadata": {"covers_questions": [15, 16, 17]}
    },
    {
      "type": "question",
      "label": "문항 15번",
      "first_5_words": "윗글에 대한 설명으로 적절한",
      "metadata": {"number": 15, "has_visual": false}
    },
    {
      "type": "visual",
      "label": "<보기> 박스",
      "first_5_words": "다음은 토론 자료이다",
      "metadata": {"belongs_to_question": 17}
    }
  ]
}

## 규칙
1. first_5_words는 정확히 5단어. 해당 영역의 첫 텍스트에서 가져오세요.
2. elements는 객체의 배열. 문자열 배열 금지.
3. 헤더, 푸터, 페이지번호 제외.
4. type은 "passage" / "question" / "visual" 중 하나.
5. 페이지에 해당 요소가 없으면 빈 배열 [].
6. JSON만 출력. 설명, 마크다운 코드블록 금지."""


# ─────────────────────────────────────────
# Step 1: Gemini Vision 호출
# ─────────────────────────────────────────

def _call_gemini_for_anchors(page_image_path: str) -> dict:
    from google import genai
    from PIL import Image

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))
    img = Image.open(page_image_path)

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[ANCHOR_PROMPT, img],
    )

    if not response.text:
        print("[vision_anchor] Gemini 빈 응답", file=sys.stderr)
        return {"elements": []}

    text = response.text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```\s*$', '', text)
    text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        print(f"[vision_anchor] JSON 파싱 실패. 앞 200자:\n{text[:200]}", file=sys.stderr)
        return {"elements": []}

    elements = data.get("elements", [])
    valid = []
    for elem in elements:
        if not isinstance(elem, dict):
            print(f"[vision_anchor] elements 항목 비정상(무시): {repr(elem)[:80]}", file=sys.stderr)
            continue
        valid.append(elem)
    return {"elements": valid}


# ─────────────────────────────────────────
# Step 2: PyMuPDF 텍스트 앵커 검색
# ─────────────────────────────────────────

def _find_text_position(pdf_path: str, page_num: int, search_text: str) -> tuple | None:
    """
    PDF 특정 페이지에서 search_text의 좌표를 찾음.
    정확 매칭 → 3단어 → 2단어 → 1단어 순으로 폴백.
    반환: (x0, y0, x1, y1) PDF 포인트 좌표, 못 찾으면 None
    """
    doc = fitz.open(pdf_path)
    if page_num - 1 >= len(doc):
        doc.close()
        return None

    page = doc[page_num - 1]

    instances = page.search_for(search_text)
    if instances:
        r = instances[0]
        doc.close()
        return (r.x0, r.y0, r.x1, r.y1)

    words = search_text.strip().split()
    for n in [3, 2, 1]:
        if len(words) >= n:
            partial = " ".join(words[:n])
            instances = page.search_for(partial)
            if instances:
                r = instances[0]
                doc.close()
                return (r.x0, r.y0, r.x1, r.y1)

    doc.close()
    return None


# ─────────────────────────────────────────
# Step 3: 앵커 → 영역 확장
# ─────────────────────────────────────────

def _expand_to_region(pdf_path: str, page_num: int, anchor_pos: tuple,
                      next_anchor_pos: tuple | None) -> tuple:
    """
    앵커 시작점에서 다음 앵커 직전까지를 영역으로 확장.
    다음 앵커 없으면 페이지 하단(푸터 50pt 제외)까지.
    반환: (x0, y0, x1, y1)
    """
    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    page_width = page.rect.width
    page_height = page.rect.height
    doc.close()

    _, y0, _, _ = anchor_pos

    if next_anchor_pos:
        next_y = next_anchor_pos[1]
        return (0, y0, page_width, max(y0 + 1, next_y - 5))
    else:
        return (0, y0, page_width, page_height - 50)


# ─────────────────────────────────────────
# Step 4: 좌표 변환 (PDF 포인트 → 이미지 픽셀)
# ─────────────────────────────────────────

def _pdf_to_image_coords(bbox_pdf: tuple,
                          page_width_pt: float, page_height_pt: float,
                          image_width_px: int, image_height_px: int) -> tuple:
    """PDF 포인트 좌표를 이미지 픽셀 좌표로 변환."""
    x0, y0, x1, y1 = bbox_pdf
    sx = image_width_px / page_width_pt
    sy = image_height_px / page_height_pt
    return (int(x0 * sx), int(y0 * sy), int(x1 * sx), int(y1 * sy))


# ─────────────────────────────────────────
# 메인 함수
# ─────────────────────────────────────────

def extract_structure_with_anchors(pdf_path: str, page_image_path: str, page_num: int) -> dict:
    """
    Vision으로 의미 구조 추출 + PyMuPDF로 좌표 역추적.

    반환:
    {
        "page": 1,
        "elements": [
            {
                "type": "passage" | "question" | "visual",
                "label": "...",
                "first_5_words": "...",
                "bbox": [x0, y0, x1, y1],   # PDF 포인트 단위, 없으면 null
                "anchor_found": true,
                "metadata": {...}
            }
        ],
        "anchor_success_rate": 0.85
    }
    """
    # 1. Vision 호출
    vision_result = _call_gemini_for_anchors(page_image_path)
    elements = vision_result.get("elements", [])

    # 2. 텍스트 앵커 검색
    for elem in elements:
        anchor_text = (elem.get("first_5_words") or "").strip()
        if not anchor_text:
            elem["anchor_found"] = False
            elem["bbox"] = None
            continue
        pos = _find_text_position(pdf_path, page_num, anchor_text)
        if pos:
            elem["_anchor_pos"] = pos
            elem["anchor_found"] = True
        else:
            elem["anchor_found"] = False
            elem["bbox"] = None

    # 3. y좌표 순 정렬 후 영역 확장
    found = [e for e in elements if e.get("anchor_found")]
    found.sort(key=lambda e: e["_anchor_pos"][1])

    for i, elem in enumerate(found):
        next_pos = found[i + 1]["_anchor_pos"] if i + 1 < len(found) else None
        elem["bbox"] = list(_expand_to_region(pdf_path, page_num, elem["_anchor_pos"], next_pos))
        del elem["_anchor_pos"]

    total = max(len(elements), 1)
    success_count = sum(1 for e in elements if e.get("anchor_found"))

    return {
        "page": page_num,
        "elements": elements,
        "anchor_success_rate": round(success_count / total, 3),
    }


# ─────────────────────────────────────────
# 시각화
# ─────────────────────────────────────────

def visualize_anchors(page_image_path: str, pdf_path: str, page_num: int,
                       result: dict, output_path: str):
    """
    감지된 bbox를 페이지 이미지에 색상 박스로 그려 저장.
    passage=빨강, question=파랑, visual=초록, anchor_found=False는 회색 점선.
    """
    from PIL import Image, ImageDraw

    img = Image.open(page_image_path).convert("RGB")
    draw = ImageDraw.Draw(img)
    image_width, image_height = img.size

    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    page_width_pt = page.rect.width
    page_height_pt = page.rect.height
    doc.close()

    colors = {"passage": "red", "question": "blue", "visual": "green"}

    for elem in result.get("elements", []):
        bbox_pdf = elem.get("bbox")
        color = colors.get(elem.get("type"), "gray")
        label = (elem.get("label") or "")[:30]

        if bbox_pdf and elem.get("anchor_found"):
            x0, y0, x1, y1 = _pdf_to_image_coords(
                bbox_pdf, page_width_pt, page_height_pt, image_width, image_height
            )
            draw.rectangle([x0, y0, x1, y1], outline=color, width=3)
            draw.text((x0 + 5, y0 + 5), label, fill=color)
        else:
            # 앵커 못 찾은 요소: 상단에 회색 텍스트만 표시
            draw.text((10, 10 + result["elements"].index(elem) * 20),
                      f"[미매칭] {label}", fill="gray")

    img.save(output_path)
