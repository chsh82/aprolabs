"""
Phase 3: 사고력 유형 태깅 (Claude) - 현재 비활성화, Anthropic API 미사용 환경
태깅이 필요한 경우 나중에 별도 재분류 배치로 실행 예정.
"""
import os
import json

THINKING_TYPES = [
    "이항대립", "전제추론", "유추", "인과관계", "비교대조",
    "사례적용", "관점분석", "논거평가", "구조파악"
]


def tag_all(passages_data: list, questions_data: list, job_id: str = None) -> None:
    """Claude API를 이용한 태깅. ANTHROPIC_API_KEY 없으면 skip."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except Exception:
        return

    passages_summary = [
        {"idx": i, "content_preview": (p.get("content") or "")[:300]}
        for i, p in enumerate(passages_data)
    ]
    questions_summary = [
        {"idx": i, "number": q.get("number"), "stem": (q.get("stem") or "")[:200]}
        for i, q in enumerate(questions_data)
    ]

    prompt = f"""수능 국어 시험을 분석합니다.

사고력 유형 목록: {json.dumps(THINKING_TYPES, ensure_ascii=False)}

지문 목록:
{json.dumps(passages_summary, ensure_ascii=False, indent=2)}

문항 목록:
{json.dumps(questions_summary, ensure_ascii=False, indent=2)}

아래 JSON 형식으로만 응답하세요:
{{
  "passages": [
    {{"idx": 0, "complexity_score": 0.0~1.0, "concepts": ["개념1", "개념2"]}}
  ],
  "questions": [
    {{
      "idx": 0,
      "thinking_types": ["사고력유형1"],
      "difficulty": "상/중/하",
      "topic": "세부주제",
      "tags": ["태그1", "태그2"]
    }}
  ]
}}

JSON 외 다른 텍스트는 출력하지 마세요."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}]
        )
    except Exception:
        return

    try:
        from app.database import SessionLocal
        from app.models.api_usage import ApiUsage, calc_cost
        db = SessionLocal()
        db.add(ApiUsage(
            service="claude", model="claude-sonnet-4-6", purpose="tag",
            job_id=job_id,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
            cost_usd=calc_cost("claude-sonnet-4-6", message.usage.input_tokens, message.usage.output_tokens),
        ))
        db.commit()
        db.close()
    except Exception:
        pass

    text = message.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0]

    try:
        result = json.loads(text)
    except Exception:
        return

    for p_tag in result.get("passages", []):
        idx = p_tag.get("idx")
        if idx is not None and idx < len(passages_data):
            passages_data[idx]["complexity_score"] = p_tag.get("complexity_score")
            passages_data[idx]["concepts"] = p_tag.get("concepts", [])

    for q_tag in result.get("questions", []):
        idx = q_tag.get("idx")
        if idx is not None and idx < len(questions_data):
            questions_data[idx]["thinking_types"] = q_tag.get("thinking_types", [])
            questions_data[idx]["difficulty"] = q_tag.get("difficulty")
            questions_data[idx]["topic"] = q_tag.get("topic")
            questions_data[idx]["tags"] = q_tag.get("tags", [])
