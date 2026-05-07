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
      "metadata": {"number": 15, "has_visual": true}
    },
    {
      "type": "visual",
      "label": "<보기> 박스 (문항 15번)",
      "first_5_words": "다음은 토론 자료이다",
      "metadata": {"visual_kind": "bogi_box", "belongs_to_question": 15}
    }
  ]
}

## visual 요소 인식 규칙 (매우 중요)

다음은 반드시 별도의 visual 요소로 출력하세요. question 안에 포함시키지 마세요:
- <보기> 라고 표시된 박스 (문항 발문과 선택지 사이에 위치하는 경우 많음)
- 표(테이블) 형태의 자료
- 그림, 도표, 그래프
- 시(詩)나 산문 인용 박스

특히 <보기>의 경우:
- 문항 안에 위치하더라도 반드시 별도 visual 요소로 분리
- type: "visual"
- label: "<보기> 박스 (문항 N번)"
- metadata: {"visual_kind": "bogi_box", "belongs_to_question": N}
- first_5_words: 박스 안 첫 5단어

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
        model="gemini-2.5-pro",
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
# Step 3: 컬럼 판정 + 영역 확장
# ─────────────────────────────────────────

def _detect_column(anchor_pos: tuple, page_width: float) -> str:
    """앵커 x 좌표로 좌측/우측 컬럼 판정."""
    x0, _, _, _ = anchor_pos
    return "left" if x0 < page_width * 0.5 else "right"


def _expand_to_region(pdf_path: str, page_num: int, anchor_pos: tuple,
                      elem_type: str,
                      current_column: str,
                      all_anchors_with_column: list,
                      anchor_index: int) -> tuple:
    """
    같은 컬럼 안에서 다음 앵커까지 영역 확장.

    Args:
        elem_type: "passage" | "question" | "visual"
        all_anchors_with_column: [(pos, column, idx, type), ...] 정렬됨
        anchor_index: 현재 앵커의 인덱스
    반환: (x0, y0, x1, y1)
    """
    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    page_width = page.rect.width
    page_height = page.rect.height
    doc.close()

    _, y0, _, _ = anchor_pos
    mid_x = page_width * 0.5

    if current_column == "left":
        col_x0, col_x1 = 30, mid_x - 5
    else:
        col_x0, col_x1 = mid_x + 5, page_width - 30

    # visual은 고정 높이로 처리 (다음 앵커까지 확장하면 question 영역 침범)
    if elem_type == "visual":
        return (col_x0, y0 - 5, col_x1, y0 + 150)

    # passage/question: 같은 컬럼의 다음 non-visual 앵커까지 확장
    next_y = page_height - 50
    for i in range(anchor_index + 1, len(all_anchors_with_column)):
        next_pos, next_col, _, next_type = all_anchors_with_column[i]
        if next_col == current_column and next_type != "visual":
            next_y = next_pos[1] - 5
            break

    return (col_x0, y0, col_x1, max(y0 + 1, next_y))


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

    # 3. 페이지 폭 가져오기
    doc = fitz.open(pdf_path)
    page_width = doc[page_num - 1].rect.width
    doc.close()

    # 4. 컬럼 판정 + (좌측 컬럼 먼저, y) 순으로 정렬
    found = [e for e in elements if e.get("anchor_found")]
    for elem in found:
        elem["_column"] = _detect_column(elem["_anchor_pos"], page_width)

    found.sort(key=lambda e: (0 if e["_column"] == "left" else 1, e["_anchor_pos"][1]))

    anchors_meta = [(e["_anchor_pos"], e["_column"], i, e["type"]) for i, e in enumerate(found)]

    for i, elem in enumerate(found):
        elem["bbox"] = list(_expand_to_region(
            pdf_path, page_num,
            elem["_anchor_pos"], elem["type"], elem["_column"],
            anchors_meta, i,
        ))
        del elem["_anchor_pos"]
        del elem["_column"]

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
