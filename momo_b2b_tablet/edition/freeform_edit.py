"""검수 화면 자유 편집 요청 - 2026-09-29 사용자 지시 [3].

프리셋(edition/presets.py)은 "LLM을 거치지 않는 고정 패치만" 계산하는데,
프리셋으로 안 풀리는 편집(예: "이 페이지 답란을 좀 더 넓게 써줘")은 검수자가
자연어로 설명하면 LLM이 JSON Patch를 만들어준다. 프리셋과 같은 미리보기 ->
적용 2단계 흐름을 그대로 따른다(edition/api.py의 preset-preview/apply와 동형).

안전장치: LLM에게는 그 페이지 객체 "루트 기준" 상대경로로만 patch를 만들게
하고, 여기서 /pages/{page_idx}를 접두사로 붙인다 - 그래서 LLM이 아무리
잘못 짜도 다른 페이지나 문서 전체 구조는 건드릴 수 없다. 또한 붙이기 전에
그 페이지 객체 하나에 실제로 적용까지 시도해봐서(jsonpatch), 문법적으로
안 맞는 patch는 미리보기 단계에서 걸러낸다.
"""
from __future__ import annotations

import json
import os

import anthropic
import jsonpatch
import jsonpointer

_DEFAULT_MODEL = "claude-sonnet-5"

_anthropic_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.AsyncAnthropic()  # ANTHROPIC_API_KEY 환경변수를 그대로 씀
    return _anthropic_client


class FreeformEditError(Exception):
    """LLM 응답 파싱 실패·유효하지 않은 patch·적용 불가 등 - 화면에 그대로 보여줄 이유."""


_SCHEMA_NOTE = """이 문서는 초중고 학습지 한 페이지의 JSON 구조다. 아래 필드만 건드릴 수 있다
(그 외 필드는 절대 추가·삭제·이름변경 하지 말 것 - 특히 "type"은 절대 바꾸지 말 것):

- mirror: true/false - 2단 레이아웃(qa/qaband/qaref/solo 타입)의 좌우를 통째로 뒤집는다.
- titleTop: true/false - 문항 제목을 페이지 중앙 상단으로 옮긴다.
- wide: true/false - qa/qaband/qaref에서 2단을 풀어 제시문->문항->답란 3층 전체 폭 구조로.
- slot.scene / slot.avoid: 이미지 생성 지시문 텍스트. slot 자체를 add/remove도 가능.
- q.kind: 답란 종류 문자열(long/short/blank/blankTall/cell/row/rowTall/cardInk/memo/vocab/inline/one) 중 하나로 replace.

이 필드들의 조합으로 요청을 해결할 수 없으면(완전히 새로운 위젯을 요구하거나
이 스키마에 없는 배치를 요구하면) ops를 빈 배열로 두고 summary에 왜 안 되는지,
대신 무엇을 해볼 수 있는지 설명해라."""


def build_prompt(page_json: dict, request_text: str) -> str:
    return f"""너는 학습지 편집 도구의 보조 편집기다. 아래 페이지 JSON에 검수자의
자연어 요청을 RFC 6902 JSON Patch로 변환해라. path는 이 페이지 객체를
루트("/")로 보고 쓴다(예: "/mirror", "/slot/scene") - 문서 전체가 아니라
이 페이지 하나만 기준으로 삼는다.

{_SCHEMA_NOTE}

현재 페이지 JSON:
{json.dumps(page_json, ensure_ascii=False, indent=2)}

검수자 요청: "{request_text}"

반드시 아래 형식의 JSON 객체 하나만 출력해라(설명·코드블록 등 다른 텍스트 없이):
{{"ops": [ ...RFC 6902 JSON Patch... ], "summary": "무엇을 어떻게 바꿨는지 한국어 한두 문장"}}"""


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        raise FreeformEditError(f"LLM 응답을 JSON으로 해석할 수 없습니다: {e}") from e


async def generate_patch(page_json: dict, request_text: str, page_idx: int) -> dict:
    """{ops (실제 layout에 적용 가능한 /pages/{page_idx}/... 절대경로), summary}."""
    client = _get_client()
    prompt = build_prompt(page_json, request_text)
    try:
        res = await client.messages.create(
            model=os.environ.get("FREEFORM_EDIT_MODEL", _DEFAULT_MODEL),
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.APIError as e:
        raise FreeformEditError(f"편집 요청 처리 실패: {e}") from e

    raw = "".join(b.text for b in res.content if getattr(b, "text", None))
    parsed = _extract_json(raw)
    ops = parsed.get("ops", [])
    summary = parsed.get("summary", "")
    if not isinstance(ops, list):
        raise FreeformEditError("LLM이 ops를 배열로 주지 않았습니다.")
    if not ops:
        raise FreeformEditError(summary or "이 요청은 처리할 수 없습니다 - 더 구체적으로 설명해주세요.")

    # 페이지 객체 하나에만 먼저 적용해봐서 문법·경로 오류를 미리보기 단계에서 걸러낸다.
    try:
        jsonpatch.JsonPatch(ops).apply(json.loads(json.dumps(page_json)))
    except (jsonpatch.JsonPatchException, jsonpointer.JsonPointerException) as e:
        raise FreeformEditError(f"이 편집을 적용할 수 없습니다: {e}") from e

    # 실제 문서에 적용할 때는 /pages/{page_idx}를 접두사로 붙인다 - LLM이
    # 아무리 잘못 짜도 다른 페이지·문서 구조는 건드릴 수 없게 하는 안전장치.
    fixed_ops = [{**op, "path": f"/pages/{page_idx}{op.get('path', '')}"} for op in ops]
    return {"ops": fixed_ops, "summary": summary or "(설명 없음)"}
