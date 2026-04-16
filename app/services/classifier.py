import os
import json
import anthropic

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


async def classify_question(content: str, category: str = "") -> dict:
    """문항 텍스트를 분석해서 자동 분류"""
    category_hint = f"\n대분류는 '{category}'로 지정되어 있습니다." if category else ""

    prompt = f"""다음 시험 문항을 분석해서 JSON으로 분류해주세요.{category_hint}

문항:
{content}

다음 형식으로만 응답하세요 (JSON):
{{
  "subject": "세부 과목명",
  "unit": "단원명",
  "topic": "세부주제",
  "difficulty": "상/중/하 중 하나",
  "question_type": "객관식/주관식/서술형 중 하나",
  "tags": ["태그1", "태그2"],
  "answer_hint": "정답 또는 정답 힌트 (알 수 있는 경우, 없으면 null)"
}}

판단이 어려우면 null로 응답하세요."""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}]
    )

    text = message.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text)
    except Exception:
        return {}
