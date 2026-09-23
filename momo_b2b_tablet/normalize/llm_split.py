"""① 정규화 LLM 보조 - "질문 자체 앞머리에서 독해유형 추출" 뒤에 남는, 인용문(제시문)과
질문이 아직 한 덩어리인 question_text를 갈라낸다(SPEC §4 "LLM 보조 작업: 제시문·질문
분리"). 규칙만으로는 인용부호 안에 마침표/물음표가 섞여 있어 경계를 확신할 수 없는
문항(전체 349건, `normalize/discussion_qa.py`의 "own_label" 처리 이후 남는 것)이 대상.

사용자 지시(2026-09-23) 프롬프트 4원칙을 그대로 반영한다:
  1. 원문의 글자를 고치지 않는다(맞춤법·띄어쓰기·오타 그대로).
  2. 자르기만 한다 - 요약하거나 다시 쓰지 않는다.
  3. 쪽수 표기는 제시문 쪽에 붙인다.
  4. 애매하면 분리하지 말고 confidence를 낮춰 플래그로 넘긴다.

이 모듈은 표본(30건) 검토용으로 먼저 쓰고, 사람이 정확도를 확인한 뒤 349건 전체
적용 여부를 결정한다(아직 discussion_qa.py 파이프라인에 자동 연결하지 않았다).
"""
from __future__ import annotations

import json
import os
import re

import anthropic

DEFAULT_MODEL = os.environ.get("NORMALIZE_SPLIT_MODEL", "claude-sonnet-5")

_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic()  # ANTHROPIC_API_KEY 환경변수를 그대로 씀
    return _client


SPLIT_PROMPT_TEMPLATE = """다음은 초등/중학생 독서논술 학습지 문항에서, 독해유형 라벨(예:
"[분석적 / 추론적]")은 이미 떼어낸 나머지 부분입니다. 이 안에는 책에서 인용한 제시문과
실제 질문이 아직 한 덩어리로 섞여 있습니다.

원문:
\"\"\"
{body_text}
\"\"\"

**반드시 지킬 4가지 규칙:**
1. 원문의 글자를 단 하나도 고치지 마세요 - 맞춤법, 띄어쓰기, 오타가 있어도 원문 그대로
   옮기세요(paraphrase·교정 금지).
2. 자르기만 하세요. 요약하거나 다시 쓰지 마세요 - 원문에 있는 문자만 두 조각으로 나눕니다.
3. 쪽수 표기(예: "(174)", "(212~214)")는 제시문(excerpt) 쪽에 붙이세요.
4. 어디서 잘라야 할지 애매하면 억지로 나누지 말고, excerpt_text를 null로 둔 채
   question_text에 원문 전체를 그대로 넣고 confidence를 낮게(0.5 이하) 주세요.

아래 JSON 형식으로만 답하세요. 다른 설명은 붙이지 마세요.
{{"excerpt_text": "...또는 null", "question_text": "...", "confidence": 0.0~1.0,
  "note": "판단 근거를 한 문장으로(애매했던 경우 왜 애매했는지 포함)"}}
"""

_JSON_RE = re.compile(r"\{.*\}", re.S)


class SplitError(Exception):
    pass


def _extract_json(text: str) -> dict:
    m = _JSON_RE.search(text)
    if not m:
        raise SplitError(f"응답에서 JSON을 찾을 수 없음: {text[:200]!r}")
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError as e:
        raise SplitError(f"JSON 파싱 실패: {e}") from e


def _char_multiset(text: str) -> dict:
    """공백을 무시한 문자 빈도 - 규칙 1(글자 무변경) 검증용. 공백은 조각 사이
    경계에서 자연스럽게 달라질 수 있어 제외한다(예: 양쪽 다 trailing space)."""
    counts: dict[str, int] = {}
    for ch in text:
        if ch.isspace():
            continue
        counts[ch] = counts.get(ch, 0) + 1
    return counts


def verify_no_rewrite(original: str, excerpt: str | None, question: str) -> tuple[bool, str]:
    """분리된 두 조각을 합친 문자(공백 제외)가 원문과 정확히 같은 멀티셋인지 확인한다.
    다르면 모델이 규칙 1/2(글자 무변경, 자르기만)를 어긴 것으로 간주 - 검수자가 봐야 함."""
    combined = (excerpt or "") + (question or "")
    orig_counts = _char_multiset(original)
    combined_counts = _char_multiset(combined)
    if orig_counts == combined_counts:
        return True, "일치"
    missing = {k: orig_counts[k] - combined_counts.get(k, 0) for k in orig_counts
               if orig_counts[k] > combined_counts.get(k, 0)}
    extra = {k: combined_counts[k] - orig_counts.get(k, 0) for k in combined_counts
             if combined_counts[k] > orig_counts.get(k, 0)}
    return False, f"문자 불일치 - 누락:{missing} 추가:{extra}"


def split_excerpt_question(body_text: str, model: str = DEFAULT_MODEL) -> dict:
    """반환: {excerpt_text, question_text, confidence, note, rewrite_ok, rewrite_detail}.
    rewrite_ok=False면 verify_no_rewrite가 문자 불일치를 감지한 것 - LLM 응답을
    신뢰하지 말고 사람이 확인해야 한다(자체 검증, SPEC §4 "결과에 confidence와
    검수 플래그" 방침의 일부로 추가)."""
    client = _get_client()
    resp = client.messages.create(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": SPLIT_PROMPT_TEMPLATE.format(body_text=body_text)}],
    )
    text = "".join(block.text for block in resp.content if block.type == "text")
    result = _extract_json(text)

    excerpt = result.get("excerpt_text")
    question = result.get("question_text", "")
    ok, detail = verify_no_rewrite(body_text, excerpt, question)
    result["rewrite_ok"] = ok
    result["rewrite_detail"] = detail
    return result
