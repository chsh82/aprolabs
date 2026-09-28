"""⑦ 손글씨 -> 글자 인식 - SPEC §8 "인식 프롬프트(현행)"을 그대로 쓴다(프롬프트
자체는 renderer.js의 openRead()가 이미 그 문구로 만들어서 보낸다 - 여기서는
그걸 받아 비전 모델을 부르고 결과를 검증만 한다). 프롬프트는 제공자와 무관하게
완전히 동일한 문자열을 그대로 넘긴다.

사용자 지시(2026-09-23) 7단계: 모델을 바꿔 호출할 수 있는 옵션을 남겨 둔다(정확도
비교용) - model 인자/환경변수 둘 다로 override 가능.

사용자 지시(2026-09-27, 1차): 기본 제공자를 Gemini로. → (2차, 같은 날) 약관
조사 결과 Gemini Developer API의 "Age Requirements" 조항이 18세 미만이
이용하거나 이용할 가능성이 있는 API Client에서의 사용을 금지하고(유·무료
구분 없음, 교육용 예외 없음) 있어 이 프로젝트(초·중등 학생 대상)와 충돌 -
사용자가 기본값을 anthropic으로 되돌리라고 지시함("조항이 명확하고 교육용
예외가 없다. 비용 차이가 위험을 감수할 이유가 안 된다. B2B 계약에서 약관
위반 상태로는 답할 수 없다"). 자세한 근거·Anthropic이 요구하는 안전조치는
SPEC_손글씨_인식_제공자_정책.md 참고.

Gemini 경로는 코드에 남겨 둔다(GEMINI_API_KEY가 있고 RECOGNITION_PROVIDER=
gemini나 provider="gemini" 인자를 명시적으로 줄 때만 호출됨 - 키가 없으면
호출 자체가 RecognitionError로 막힌다. 나중에 계약 경로가 정리되면 기본값만
다시 바꾸면 됨).

사용자 지시(2026-09-27, 3차) - 1차 태블릿 테스트로 모은 실제 필기 3건에
Haiku 4.5/Sonnet 5/Opus 5.5를 전부 돌려 비교한 결과, Opus 5.5가 세 건
전부에서 뚜렷하게 더 정확했다(예: "은비야 동물은 나중에 키우자"를 Opus만
unclear=0으로 정확히 읽음 - Haiku/Sonnet은 완전히 다른 문장으로 오독).
그래서 anthropic 기본 모델을 claude-sonnet-5 -> claude-opus-5-5로 바꿨다.

사용자 지시(2026-09-27, 4차) - OpenAI(gpt-5.5)까지 넣어 같은 필기 3건을
재비교(반복 시도 포함)한 뒤 다시 짚어보니, Opus가 이겼던 건 "은비야 동물은
나중에 키우자"(길게 이어 쓴 필기체) 1건뿐이었다(Sonnet 4/4 오독, Opus
3/3 정답). 나머지 "심통났구나"는 오히려 **Sonnet이 2/3 정답, Opus는 0/2
정답**(둘 다 "심흥났구나"로 같은 실수 반복)이었고 "수두룩하다"는 둘 다
정답. 즉 Opus가 전반적으로 더 정확한 게 아니라 특정 샘플 하나에 치우친
차이였다. 지연시간은 Opus 3.6~8초, Sonnet 1.6~3.6초로 Sonnet이 뚜렷이
빠르다 - 사용자가 지연시간을 우선해 anthropic 기본 모델을
claude-opus-5-5 -> claude-sonnet-5로 되돌리라고 지시함(표본 3건뿐이라
계속 관찰 필요 - 자세한 수치는 대화 기록 참고).
"""
from __future__ import annotations

import base64
import json
import os
import re
import time

import anthropic
import openai
from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types

# 제공자별 기본 모델. gemini 쪽은 Gemini 3.1 Flash-Lite와 3.8 Flash를 비교
# 대상으로 넣어 뒀던 값을 그대로 남겨 둔다(계약 경로 정리 후 다시 쓸 수 있게) -
# 기본 제공자가 anthropic이라 지금은 호출되지 않는다. anthropic 쪽은 재비교
# (2026-09-27, 위 docstring 4차)로 claude-sonnet-5를 기본값으로 확정했다 -
# Opus 우위는 표본 1건에 치우친 결과였고, Sonnet이 3~4배 빠르다.
# openai는 아직 기본 제공자가 아니라 비교용으로만 씀(SPEC_손글씨_인식_제공자_정책.md
# §6 - 약관상 미성년자 대상 서비스 자체는 허용되지만 정확도·지연시간 모두
# Anthropic보다 못했다) - `client.models.list()`로 실존 확인한 최신 안정
# 모델 gpt-5.5를 잠정 기본값으로 둔다.
# RECOGNITION_MODEL 환경변수로 배포 단위 기본값을 코드 수정 없이 덮어쓸 수 있다.
_DEFAULT_MODELS = {
    "gemini": "gemini-3.1-flash-lite",
    "anthropic": "claude-sonnet-5",
    "openai": "gpt-5.5",
}

DEFAULT_PROVIDER = os.environ.get("RECOGNITION_PROVIDER", "anthropic").lower()


def _default_model(provider: str) -> str:
    return os.environ.get("RECOGNITION_MODEL") or _DEFAULT_MODELS.get(provider, _DEFAULT_MODELS["gemini"])


_anthropic_client: anthropic.AsyncAnthropic | None = None
_genai_client: genai.Client | None = None
_openai_client: openai.AsyncOpenAI | None = None


def _get_anthropic_client() -> anthropic.AsyncAnthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.AsyncAnthropic()  # ANTHROPIC_API_KEY 환경변수를 그대로 씀
    return _anthropic_client


def _get_openai_client() -> openai.AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = openai.AsyncOpenAI()  # OPENAI_API_KEY 환경변수를 그대로 씀
    return _openai_client


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
    # 2026-09-27: 정확도 비교용으로 실제 구현(Responses API + input_image data URL -
    # `client.models.list()`로 모델 존재, `client.responses.create()` 호출로 이미지
    # 인식 자체를 실측 확인한 뒤에 채워 넣음, 추측 구현 아님).
    client = _get_openai_client()
    data_url = f"data:image/png;base64,{base64.b64encode(image_bytes).decode('ascii')}"
    try:
        resp = await client.responses.create(
            model=model,
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": data_url},
                ],
            }],
        )
    except openai.RateLimitError as e:
        raise RecognitionError("rate_limited", str(e)) from e
    except openai.APIConnectionError as e:
        raise RecognitionError("upstream_error", str(e)) from e
    except openai.APIStatusError as e:
        raise RecognitionError("upstream_error", str(e)) from e

    return resp.output_text or ""


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
