"""literacy 자동화 스크립트 공용 Gemini 호출.

저장소 관례(verify_agent.py, app/services/text_corrector.py 등)를 그대로
따른다 - google.genai SDK, gemini-2.0-flash 모델, GEMINI_API_KEY.
"""
from __future__ import annotations

import json
import os
import time

from google import genai
from google.genai import types

MODEL = "gemini-3.6-flash"  # gemini-2.0-flash 폐기됨(2026-09-02 API 오류로 확인) - API가 권장한 대체 모델
MAX_RETRIES = 3
RETRY_DELAY = 2.0


def get_client() -> genai.Client:
    api_key = os.environ["GEMINI_API_KEY"]
    return genai.Client(api_key=api_key)


def call_gemini_json(client: genai.Client, prompt: str, max_output_tokens: int = 4096) -> dict:
    """JSON 모드로 호출하고 파싱된 dict를 반환한다. 실패 시 재시도."""
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.models.generate_content(
                model=MODEL,
                contents=[prompt],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    max_output_tokens=max_output_tokens,
                ),
            )
            return json.loads(resp.text)
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
    raise last_err
