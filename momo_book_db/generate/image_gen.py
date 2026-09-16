# -*- coding: utf-8 -*-
"""Gemini(2.5 Flash Image)로 문항 발췌문에 어울리는 삽화를 생성한다.

momo_book_review.py의 "발췌문 이미지 업로드"와 같은 슬롯(discussion_qa.excerpt_image_path)에
꽂아 넣는 용도 - 사람이 파일을 올리는 대신 AI가 만든 이미지를 그 자리에 넣는 것뿐, 검수
워크플로(승인/삭제/재생성)는 기존 업로드 흐름을 그대로 탄다.

스타일은 실제 실측(2026-09-17)으로 확인한 "따뜻한 색감의 평면 동화풍 삽화"가 기존 교재
삽화 톤과 가장 잘 어울려서 고정값으로 둠 - DESIGN_GUIDE.md의 "새 색을 추가하지 않는다"
정신에 맞춰, 이미지 안에서도 문서의 분기색 계열을 유지하도록 팔레트 힌트를 넣는다.
"""
import os

MODEL = "gemini-2.5-flash-image"

# worksheet/build/styles.css의 분기 테마(q-winter/spring/summer/autumn)와 짝을 맞춘 팔레트 힌트.
# momo_book_db/generate/extract_worksheet_json.py의 season_class()가 만드는 값과 동일한 키를 씀.
SEASON_PALETTE = {
    "q-winter": "deep navy blue and warm dark brown tones, a classic-literature winter mood",
    "q-spring": "fresh green and warm cream tones, a curious spring mood",
    "q-summer": "teal and sky-blue tones, a literary summer mood",
    "q-autumn": "warm brown, gold and sepia tones, a humanities autumn mood",
}

STYLE_SUFFIX = (
    "Flat, warm, gentle children's-book illustration style with soft muted colors and simple "
    "shapes, not photorealistic. absolutely no text, letters, numbers, or writing anywhere in "
    "the image. square composition, calm and educational mood, suitable as an illustration in "
    "a Korean elementary/middle-school reading-comprehension workbook."
)


def build_prompt(question_text, excerpt_text, quarter_class):
    palette = SEASON_PALETTE.get(quarter_class, SEASON_PALETTE["q-autumn"])
    context = (excerpt_text or question_text or "").strip()[:300]
    return (
        "Create an illustration for a Korean elementary reading-comprehension workbook.\n"
        f'Scene/topic context (Korean, for reference only - do not render this text): "{context}"\n'
        f"Color palette: {palette}.\n{STYLE_SUFFIX}"
    )


def generate_illustration(question_text, excerpt_text, quarter_class, out_path):
    """성공하면 out_path에 PNG를 쓰고 True, 실패하면 False(이미지 파트가 안 온 경우 등)."""
    from google import genai

    prompt = build_prompt(question_text, excerpt_text, quarter_class)
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))
    response = client.models.generate_content(model=MODEL, contents=[prompt])

    for part in response.candidates[0].content.parts:
        if getattr(part, "inline_data", None):
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(part.inline_data.data)
            return True
    return False
