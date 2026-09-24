"""방식 B(비전 파싱) - PDF 페이지 이미지 -> 구조화 JSON.

`PARSING_METHOD_COMPARISON.md`의 실험을 거쳐 확정한 프롬프트(v3):
layout_hint.shape를 9종으로 세분화하고, note는 "unclear"일 때만 채워 비용을
아낀다(사용자 지시 2026-09-24: "구분이 애매하면 hint를 비우고 flag를 남기게
할 것. 억지로 찍지 말 것"). 답란 줄 수(blank_lines)·칸 크기(cell_size_hint)도
함께 받는다.
"""
from __future__ import annotations

import base64
import json

import anthropic

PROMPT_VERSION = "v3"
MODEL = "claude-sonnet-5"

_LAYOUT_SHAPES = """- table_answer: 머리칸(제목행)이 있고 그 아래/옆에 학생이 채울 빈 답칸이 있는 표
- numbered_list: 1) 2) 3)처럼 번호가 매겨진 목록(답이 목록 형태로 여러 개)
- compare_two_col: 좌우 2단으로 나뉘어 서로 다른 두 대상을 비교하는 레이아웃(표 형식이 아니어도 됨)
- choice_options: 선택지가 나열되고 그중 하나를 고르는 형태(선택형/O·X 포함)
- boxed_form: 서약서·계약서·안내문처럼 테두리로 감싸인 정형 양식(제목+서명란 등)
- speech_bubble: 말풍선 안에 시작 문구나 대화가 있고 그 안/옆에 답을 쓰는 형태
- ruled_lines: 제목 없이 그냥 밑줄/괘선만 있는 단순 답란
- reference_table: 학생이 답을 쓰는 칸이 없고, 참고로 읽기만 하는 표(뜻풀이 표, 연표, 정보표)
- unclear: 위 8가지 중 어느 것에도 확신 있게 맞아떨어지지 않음 - 반드시 이 값을 쓰고 note에 왜 애매한지 적을 것(추측해서 다른 값을 찍지 말 것)
"""

PROMPT = """이 이미지는 초등/중학생 독서논술 학습지(학생용 워크북) PDF의 한 페이지입니다.
이 페이지에 있는 모든 문항/항목을 구조화된 JSON으로 추출하세요.

**반드시 지킬 것:**
- 원문 글자를 고치지 마세요. 맞춤법, 띄어쓰기, 오타가 있어도 원문 그대로 옮기세요.
- 표나 번호 목록처럼 시각적으로 구조화된 내용은 그 구조(행/열, 번호)를 그대로 살려서 옮기세요.
- 이 페이지에 실제로 있는 내용만 적으세요. 없는 항목은 빈 배열/null로 두세요.
- layout_hint.shape가 애매하면 절대 추측해서 찍지 마세요 - "unclear"를 쓰고 note에 이유를 적으세요.
- **"낱말의 뜻을 찾아 선으로 이어 보세요" 같은 매칭(선잇기) 문제는 양쪽을 전부 뽑으세요**:
  왼쪽 낱말은 "blanks"에, 오른쪽 뜻풀이 목록은 "table"에
  `{"headers": [], "rows": [["뜻1"], ["뜻2"], ...]}` 형태로(왼쪽과 같은 순서일
  필요는 없습니다 - 화면에 보이는 순서 그대로) 담으세요. 왼쪽 낱말 개수와
  오른쪽 뜻풀이 개수가 다르면 다른 그대로 both 다 옮기세요 - 짝을 맞추려고
  하나를 빼거나 추측하지 마세요.

각 항목(item)마다 다음 필드를 채우세요:
- "item_type": "vocab"(어휘) | "ox"(OX 퀴즈) | "discussion_qa"(제시문+질문형 독해 문항) | "essay"(글쓰기) | "cover"(표지/차례) 중 하나
- "reading_type": 독해유형 라벨(예: "추론적", "비판적/적용적"). 없으면 null
- "excerpt_text": 책에서 인용한 제시문(따옴표 안 원문). 없으면 null
- "question_text": 학생에게 묻는 실제 질문. 없으면 null
- "blanks": 답란이 여러 개로 나뉘어 있고 각각 라벨이 있으면 그 라벨 목록(문자열 배열). 없으면 빈 배열
- "choices": 객관식/OX 선택지 목록. 없으면 빈 배열
- "table": 표 형태 내용이 있으면 {"headers": [...], "rows": [[...], ...]} 형태로. 없으면 null
- "page_number": 이 항목의 제시문 출처로 명시된 쪽수(예: "(106)"). 이미지 다른 곳의 쪽수와 헷갈리지
  마세요 - 이 항목 바로 옆에 붙어 있는 것만. 없으면 null
- "layout_hint": 이 항목 답란의 실제 시각적 양식:
  {"shape": 아래 9가지 중 정확히 하나,
   "blank_lines": 답을 쓰는 빈 줄이 몇 개 보이는지(정수, 안 보이면 0),
   "cell_size_hint": "small"|"medium"|"large" 중 답칸/답란 하나의 대략적 크기(작은 빈칸 vs 반 페이지짜리 큰 칸),
   "note": shape가 "unclear"일 때만 왜 애매한지 짧게(한 문장 이내) 적으세요. 그 외에는
   반드시 null - 설명을 덧붙이지 마세요(비용 절감을 위해 꼭 필요할 때만 채움)}

layout_hint.shape로 쓸 수 있는 9가지:
""" + _LAYOUT_SHAPES + """

아래 JSON 형식으로만 답하세요. 다른 설명은 붙이지 마세요.
{"page_items": [{"item_type": "...", "reading_type": ..., "excerpt_text": ..., "question_text": ...,
  "blanks": [...], "choices": [...], "table": ..., "page_number": ..., "layout_hint": {...}}, ...]}
"""

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()  # ANTHROPIC_API_KEY 환경변수를 그대로 씀
    return _client


def extract_page_image(image_bytes: bytes, model: str = MODEL) -> dict:
    """페이지 이미지(PNG bytes) 하나를 구조화 JSON으로 추출.

    반환: {elapsed_sec, usage: {input_tokens, output_tokens}, parsed: {...} 또는
    {"parse_error": ..., "raw": ...}}."""
    import time

    img_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

    t0 = time.time()
    resp = _get_client().messages.create(
        model=model,
        # 20건 표본 실행 중 내용이 빽빽한 페이지 1건이 4096 토큰에서 잘려 JSON이
        # 깨지는 걸 실측으로 확인했다(output_tokens=4096 정확히 도달) - 여유를 둔다.
        max_tokens=8192,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": img_b64}},
                {"type": "text", "text": PROMPT},
            ],
        }],
    )
    elapsed = time.time() - t0
    text = "".join(b.text for b in resp.content if b.type == "text")
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        parsed = json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError) as e:
        parsed = {"parse_error": str(e), "raw": text}

    return {
        "elapsed_sec": elapsed,
        "usage": {"input_tokens": resp.usage.input_tokens, "output_tokens": resp.usage.output_tokens},
        "parsed": parsed,
        "raw_text": text,
    }
