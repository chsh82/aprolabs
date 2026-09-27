"""⑦ 손글씨 -> 글자 인식 - SPEC §8 "인식 프롬프트(현행)"을 그대로 쓴다(프롬프트
자체는 renderer.js의 openRead()가 이미 그 문구로 만들어서 보낸다 - 여기서는
그걸 받아 비전 모델을 부르고 결과를 검증만 한다). 프롬프트는 제공자와 무관하게
완전히 동일한 문자열을 그대로 넘긴다.

사용자 지시(2026-09-23) 7단계: 모델을 바꿔 호출할 수 있는 옵션을 남겨 둔다(정확도
비교용) - model 인자/환경변수 둘 다로 override 가능.

사용자 지시(2026-09-27): 기본 제공자를 Gemini로 바꾼다("아직 실제 필기로
검증 전이라 모델 등급은 비교 후 확정" - 그래서 RECOGNITION_MODEL 기본값은
잠정치다, 아래 _DEFAULT_MODELS 참고). 기존 Claude(anthropic) 경로는 비교와
장애 시 대체용으로 그대로 남긴다 - RECOGNITION_PROVIDER=anthropic으로
호출하면 이전과 동일하게 동작한다.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time

import anthropic
from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types

# 제공자별 기본 모델 - 아직 실제 필기로 등급을 확정하기 전이라 잠정치다.
# 사용자 지시: 우선 Gemini 3.1 Flash-Lite와 3.8 Flash를 비교 대상에 넣을 것 -
# 그중 더 싸고 빠른 3.1 Flash-Lite를 기본값으로 잡아 두고, 비교 결과가 나오면
# 이 값을 바꾼다(RECOGNITION_MODEL 환경변수로 배포 단위 기본값을 바로 덮어쓸 수
# 있으니 코드를 고치지 않고도 교체 가능).
_DEFAULT_MODELS = {
    "gemini": "gemini-3.1-flash-lite",
    "anthropic": "claude-sonnet-5",
}

DEFAULT_PROVIDER = os.environ.get("RECOGNITION_PROVIDER", "gemini").lower()


def _default_model(provider: str) -> str:
    return os.environ.get("RECOGNITION_MODEL") or _DEFAULT_MODELS.get(provider, _DEFAULT_MODELS["gemini"])


_anthropic_client: anthropic.AsyncAnthropic | None = None
_genai_client: genai.Client | None = None


def _get_anthropic_client() -> anthropic.AsyncAnthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.AsyncAnthropic()  # ANTHROPIC_API_KEY 환경변수를 그대로 씀
    return _anthropic_client


def _get_genai_client() -> genai.Client:
    global _genai_client
    if _genai_client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RecognitionError("upstream_error", "GEMINI_API_KEY 환경변수가 설정되지 않음")
        _genai_client = genai.Client(api_key=api_key)
    return _genai_client


class RecognitionError(Exception):
    """renderer.js의 COPY 매핑(rate_limited/refused/empty_completion/invalid_json/
    upstream_error)과 코드를 맞춘다 - 그대로 HTTP 응답 detail로 나간다."""
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_JSON_RE = re.compile(r"\{.*\}", re.S)


def _extract_json(text: str) -> dict:
    m = _JSON_RE.search(text)
    if not m:
        raise RecognitionError("invalid_json", f"응답에서 JSON을 찾을 수 없음: {text[:200]!r}")
    try:
        parsed = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        raise RecognitionError("invalid_json", f"JSON 파싱 실패: {e}") from e
    if not isinstance(parsed, dict):
        raise RecognitionError("invalid_json", "JSON이 객체가 아님")
    return parsed


async def _recognize_anthropic(image_bytes: bytes, prompt: str, model: str) -> str:
    client = _get_anthropic_client()
    try:
        resp = await client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/png",
                        "data": base64.b64encode(image_bytes).decode("ascii"),
                    }},
                    {"type": "text", "text": prompt},
                ],
            }],
        )
    except anthropic.RateLimitError as e:
        raise RecognitionError("rate_limited", str(e)) from e
    except anthropic.APIConnectionError as e:
        raise RecognitionError("upstream_error", str(e)) from e
    except anthropic.APIStatusError as e:
        raise RecognitionError("upstream_error", str(e)) from e

    if getattr(resp, "stop_reason", None) == "refusal":
        raise RecognitionError("refused", "모델이 이 이미지에 대한 응답을 거부함")
    return "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")


async def _recognize_gemini(image_bytes: bytes, prompt: str, model: str) -> str:
    client = _get_genai_client()
    try:
        resp = await client.aio.models.generate_content(
            model=model,
            contents=[
                genai_types.Part.from_bytes(data=image_bytes, mime_type="image/png"),
                prompt,
            ],
        )
    except genai_errors.ClientError as e:
        code = "rate_limited" if e.code == 429 else "upstream_error"
        raise RecognitionError(code, str(e)) from e
    except genai_errors.ServerError as e:
        raise RecognitionError("upstream_error", str(e)) from e

    # Gemini 세이프티 필터가 막으면 candidates가 비거나 text가 없다(Claude의
    # stop_reason=="refusal"과 같은 자리) - 아래 공통 empty_completion 체크가 잡는다.
    return resp.text or ""


async def _recognize_openai(image_bytes: bytes, prompt: str, model: str) -> str:
    # 사용자 지시에 provider 값으로만 자리가 있고 모델명·SDK 세부사항은 아직
    # 안 정해졌다 - 추측으로 구현하지 않는다. 필요해지면 여기만 채우면 된다.
    raise RecognitionError("upstream_error", "openai provider는 아직 구현되지 않음")


_PROVIDERS = {"gemini": _recognize_gemini, "anthropic": _recognize_anthropic, "openai": _recognize_openai}


async def recognize_handwriting(image_bytes: bytes, prompt: str, model: str | None = None,
                                 provider: str | None = None) -> dict:
    """(image_bytes, prompt) -> {text, unclear, model, provider, latency_ms}. 실패하면 RecognitionError."""
    if not image_bytes:
        raise RecognitionError("image_rejected", "빈 이미지")
    provider = (provider or DEFAULT_PROVIDER).lower()
    call = _PROVIDERS.get(provider)
    if call is None:
        raise RecognitionError("upstream_error", f"지원하지 않는 provider: {provider!r}")
    model = model or _default_model(provider)

    t0 = time.monotonic()
    text_out = await call(image_bytes, prompt, model)
    latency_ms = int((time.monotonic() - t0) * 1000)

    if not text_out.strip():
        raise RecognitionError("empty_completion", "모델 응답이 비어 있음")

    parsed = _extract_json(text_out)
    text = parsed.get("text")
    if not isinstance(text, str) or not text.strip():
        raise RecognitionError("empty_completion", "text 필드가 비어 있음")
    try:
        unclear = int(parsed.get("unclear") or 0)
    except (TypeError, ValueError):
        unclear = 0

    return {"text": text, "unclear": unclear, "model": model, "provider": provider, "latency_ms": latency_ms}
