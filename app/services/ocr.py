import os
from google import genai
from PIL import Image

def _client():
    return genai.Client(api_key=os.getenv("GEMINI_API_KEY", ""))


async def extract_text_from_image(image_path: str) -> str:
    """이미지에서 문항 텍스트를 OCR로 추출"""
    image = Image.open(image_path)

    prompt = """이 이미지에서 시험 문항 텍스트를 추출해주세요.

규칙:
- 문항 번호, 지문, 보기, 선택지를 모두 포함할 것
- 수식은 텍스트로 최대한 표현할 것 (예: x²+2x+1)
- 표나 그래프는 [표], [그래프] 등으로 표시할 것
- 이미지에 문항이 없으면 "문항 없음"으로 응답할 것
- 원문 그대로 추출하고 내용을 요약하거나 수정하지 말 것"""

    response = _client().models.generate_content(
        model="gemini-2.0-flash",
        contents=[prompt, image],
    )
    return response.text.strip()
