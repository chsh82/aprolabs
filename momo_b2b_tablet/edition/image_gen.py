"""검수 화면 페이지별 이미지 생성 - 2026-09-28 사용자 지시 [3].

일괄 생성이 아니라 검수 중 그 페이지에서 바로 만드는 방식. 제공자는
IMAGE_PROVIDER 환경변수로 교체 가능(edition/recognize.py의 _PROVIDERS 패턴을
그대로 따름) - 지금은 openai만 구현.
"""
from __future__ import annotations

import base64
import io
import os
import time
import uuid
from pathlib import Path

import openai
from PIL import Image

ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
GENERATED_DIR = ASSETS_DIR / "generated"
GENERATED_DIR.mkdir(parents=True, exist_ok=True)

# 슬롯 실측 비율(renderer.js의 RATIOS와 동일한 5종) -> (가로비, 세로비)
RATIO_TO_WH = {"1:1": (1, 1), "4:3": (4, 3), "3:4": (3, 4), "3:2": (3, 2), "2:1": (2, 1)}

# 2026-09-28 사용자 지시 - 폭 60~80mm에 150dpi 이상(350px 이상) 나오는 크기.
_MIN_PX = 350
_TARGET_MM = 70.0  # 60~80mm 중간값
_DPI = 150
_MM_PER_INCH = 25.4

_QUARTER_STYLE = {
    "winter": "차분하고 정제된 색조의 겨울 고전 분기 하우스 스타일",
    "spring": "따뜻하고 생기 있는 색조의 봄 탐구 분기 하우스 스타일",
    "summer": "선명하고 시원한 색조의 여름 문학 분기 하우스 스타일",
    "autumn": "따뜻한 갈색 톤의 가을 인문 분기 하우스 스타일",
}


def _target_px(ratio: str) -> tuple[int, int]:
    w_ratio, h_ratio = RATIO_TO_WH.get(ratio, (1, 1))
    width_px = max(_MIN_PX, round(_TARGET_MM / _MM_PER_INCH * _DPI))
    height_px = round(width_px * h_ratio / w_ratio)
    return width_px, height_px


def build_prompt(scene: str, avoid: str, quarter: str | None) -> str:
    style = _QUARTER_STYLE.get(quarter or "", "")
    parts = [scene.strip()]
    if style:
        parts.append(style)
    if avoid and avoid.strip():
        parts.append(f"다음은 그리지 않는다: {avoid.strip()}")
    parts.append("한국 초중고 교재에 어울리는 삽화 스타일. 글자나 텍스트는 넣지 않는다.")
    return " / ".join(p for p in parts if p)


def _crop_to_ratio(img: Image.Image, ratio: str) -> Image.Image:
    w_ratio, h_ratio = RATIO_TO_WH.get(ratio, (1, 1))
    target = w_ratio / h_ratio
    cur = img.width / img.height
    if abs(cur - target) < 0.01:
        return img
    if cur > target:
        new_w = round(img.height * target)
        x0 = (img.width - new_w) // 2
        return img.crop((x0, 0, x0 + new_w, img.height))
    new_h = round(img.width / target)
    y0 = (img.height - new_h) // 2
    return img.crop((0, y0, img.width, y0 + new_h))


# ============ OpenAI ============
# gpt-image-1은 1024x1024 / 1024x1536 / 1536x1024만 지원한다(5종 비율을 전부
# 못 커버함) - 가장 가까운 지원 크기로 생성한 뒤 Pillow로 목표 비율에 맞춰
# 중앙 크롭한다.
_OPENAI_SIZE_SQUARE = "1024x1024"
_OPENAI_SIZE_PORTRAIT = "1024x1536"
_OPENAI_SIZE_LANDSCAPE = "1536x1024"

# 2026-09-28 - gpt-image-1 공식 가격(quality=medium 기준, USD) 추정치. 실제
# 청구서와 다를 수 있어 화면에는 "추정 비용"이라고 표시한다.
_OPENAI_COST_USD = {
    _OPENAI_SIZE_SQUARE: 0.04,
    _OPENAI_SIZE_PORTRAIT: 0.06,
    _OPENAI_SIZE_LANDSCAPE: 0.06,
}

_openai_client: openai.AsyncOpenAI | None = None


def _get_openai_client() -> openai.AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = openai.AsyncOpenAI()  # OPENAI_API_KEY 환경변수를 그대로 씀
    return _openai_client


def _openai_size_for_ratio(ratio: str) -> str:
    w_ratio, h_ratio = RATIO_TO_WH.get(ratio, (1, 1))
    if w_ratio == h_ratio:
        return _OPENAI_SIZE_SQUARE
    return _OPENAI_SIZE_LANDSCAPE if w_ratio > h_ratio else _OPENAI_SIZE_PORTRAIT


async def _generate_openai(prompt: str, ratio: str) -> dict:
    size = _openai_size_for_ratio(ratio)
    client = _get_openai_client()
    t0 = time.monotonic()
    try:
        res = await client.images.generate(model="gpt-image-1", prompt=prompt, size=size, n=1)
    except openai.APIError as e:
        raise RuntimeError(f"OpenAI 이미지 생성 실패: {e}") from e
    elapsed_ms = round((time.monotonic() - t0) * 1000)
    img = Image.open(io.BytesIO(base64.b64decode(res.data[0].b64_json))).convert("RGB")
    img = _crop_to_ratio(img, ratio)
    target_w, target_h = _target_px(ratio)
    if img.width > target_w * 1.2:
        img = img.resize((target_w, target_h), Image.LANCZOS)
    return {"image": img, "model": "gpt-image-1", "elapsed_ms": elapsed_ms,
            "cost_usd": _OPENAI_COST_USD.get(size, 0.04)}


_PROVIDERS = {"openai": _generate_openai}


async def generate_candidates(scene: str, avoid: str, quarter: str | None,
                                ratio: str, count: int) -> list[dict]:
    """2~3장 생성해서 파일로 저장하고, store.save_image_candidates에 넘길 수
    있는 dict 리스트를 돌려준다(DB에는 아직 안 씀 - 호출자 책임)."""
    provider_name = os.environ.get("IMAGE_PROVIDER", "openai")
    fn = _PROVIDERS.get(provider_name)
    if fn is None:
        raise ValueError(f"지원하지 않는 IMAGE_PROVIDER: {provider_name}")

    prompt = build_prompt(scene, avoid, quarter)
    results = []
    for _ in range(count):
        r = await fn(prompt, ratio)
        filename = f"{uuid.uuid4().hex}.png"
        r["image"].save(GENERATED_DIR / filename, "PNG")
        results.append({
            "provider": provider_name, "model": r["model"], "prompt": prompt,
            "file_path": f"generated/{filename}",
            "width": r["image"].width, "height": r["image"].height,
            "cost_usd": r["cost_usd"], "elapsed_ms": r["elapsed_ms"],
        })
    return results
