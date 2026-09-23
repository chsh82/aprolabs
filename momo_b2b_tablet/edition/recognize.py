"""⑦ 손글씨 -> 글자 인식 - SPEC §8 "인식 프롬프트(현행)"을 그대로 쓴다(프롬프트
자체는 renderer.js의 openRead()가 이미 그 문구로 만들어서 보낸다 - 여기서는
그걸 받아 Claude API 비전을 부르고 결과를 검증만 한다).

사용자 지시(2026-09-23) 7단계: 모델을 바꿔 호출할 수 있는 옵션을 남겨 둔다(정확도
비교용) - model 인자/환경변수 둘 다로 override 가능.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time

import anthropic

# 기본 모델 - 환경변수(RECOGNITION_MODEL)로 배포 단위 기본값을 바꿀 수 있고,
# 호출마다 model 인자로 그 위에 덮어쓸 수 있다(사용자가 같은 필기를 여러 모델로
# 비교해 볼 수 있게).
DEFAULT_MODEL = os.environ.get("RECOGNITION_MODEL", "claude-sonnet-5")

_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic()  # ANTHROPIC_API_KEY 환경변수를 그대로 씀
    return _client


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


async def recognize_handwriting(image_bytes: bytes, prompt: str, model: str | None = None) -> dict:
    """(image_bytes, prompt) -> {text, unclear, model, latency_ms}. 실패하면 RecognitionError."""
    if not image_bytes:
        raise RecognitionError("image_rejected", "빈 이미지")
    model = model or DEFAULT_MODEL
    client = _get_client()
    t0 = time.monotonic()
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
    latency_ms = int((time.monotonic() - t0) * 1000)

    if getattr(resp, "stop_reason", None) == "refusal":
        raise RecognitionError("refused", "모델이 이 이미지에 대한 응답을 거부함")

    text_out = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
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

    return {"text": text, "unclear": unclear, "model": model, "latency_ms": latency_ms}
