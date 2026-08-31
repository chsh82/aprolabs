"""문항 생성 프롬프트 구성. docs/literacy/05-퀴즈생성.md 5절 참고.

few-shot 5개는 raw/schema-reading/의 어휘퀴즈DB.xlsx(선생님이 직접 만들어 쓰시던
검증된 문항 338개)에서 오답의 방향(반대/인접/부분오류)이 뚜렷한 것을 골라, 형식
(종결형·마침표·길이)만 교정했다. 오답을 새로 창작하지 않고 기존 문항을 교정하는
방식으로 만들었다 - 사용자가 전후 비교를 직접 확인하고 승인했다(2026-08-31).

교정 과정에서 "다르다"의 원본 오답 2개("거의 유사하다"/"조금 비슷하다")는 폐기했다.
정도만 다를 뿐 정답과 같은 방향(둘 다 "완전히 같지는 않다"는 의미)으로 수렴해서
정답이 두 개가 되는 문제가 있었다 - 이 교훈이 RULES_TEXT의 "수렴 금지" 규칙이다.
"""
from __future__ import annotations

FEW_SHOT_EXAMPLES = [
    {
        "word": "가르다",
        "correct": "쪼개거나 나누어 따로따로 되게 하다.",
        "distractors": [
            "덮거나 가려서 보이지 않게 하다.",
            "묶거나 연결하여 붙이다.",
            "합치거나 모아서 하나로 만들다.",
        ],
    },
    {
        "word": "구분",
        "correct": "일정한 기준에 따라 전체를 몇 개로 갈라 나눔.",
        "distractors": [
            "비슷한 것들을 같은 종류끼리 모아 둠.",
            "여러 개로 나뉜 것을 하나로 합쳐 놓음.",
            "일정한 기준에 따라 크기 순서로 늘어놓음.",
        ],
    },
    {
        "word": "다르다",
        "correct": "비교가 되는 두 대상이 서로 같지 아니하다.",
        "distractors": [
            "비교가 되는 두 대상이 서로 완전히 같다.",
            "비교가 되는 두 대상이 서로 짝을 이루고 있다.",
            "비교가 되는 두 대상이 서로 관련이 없다.",
        ],
    },
    {
        "word": "감상",
        "correct": "주로 예술 작품을 이해하여 즐기고 평가함.",
        "distractors": [
            "주로 예술 작품을 훼손하거나 망가뜨림.",
            "주로 예술 작품을 새로 만들어 냄.",
            "주로 예술 작품을 팔거나 사서 거래함.",
        ],
    },
    {
        "word": "규칙",
        "correct": "여러 사람이 다 같이 지키기로 작정한 법칙. 또는 제정된 질서.",
        "distractors": [
            "개인이 혼자 정한 취향이나 선호.",
            "그때그때 형편에 따라 임시로 정해 둔 약속.",
            "지키지 않아도 되는 특별한 예외 사항.",
        ],
    },
]

RULES_TEXT = """오답 3개를 만들 때 반드시 지킬 것:

1. 방향을 섞어라 - 반대(정답과 반대되는 뜻), 인접(비슷하지만 다른 뜻),
   부분오류(일부만 맞고 핵심이 틀림) 세 방향을 섞어서 만든다.

2. 오답이 정답 쪽으로 수렴하면 안 된다. "정도만 다른" 오답(예: 정답이
   "다르다"인데 오답이 "조금 비슷하다")은 정답과 양립 가능해서 정답이
   두 개가 되는 결과를 낳는다. 오답은 정답과 명백히 다른 사실을 말해야
   한다.

3. 종결 형태를 정답과 일치시켜라. 정답이 "~하다."로 끝나면 오답도 전부
   "~하다."로 끝난다. 마침표 유무까지 맞춘다.

4. 길이를 정답의 ±50% 이내로 맞춰라. 정답이 20자면 오답은 10~30자.

5. 도입구(문장 서두)를 정답과 통일하는 건 정답의 문장 구조가 그것을
   자연스럽게 허용할 때만 한다. 억지로 맞춰서 문장이 어색해지면 하지
   않는다 - 맞춰야 하는 건 종결형·길이·마침표이고, 문장 구조는 자연스러움이
   우선이다.

6. 오답에 "아니다", "않다", "못하다" 같은 단순 부정어로만 만들지 않는다.

7. 정답 문장은 절대 다시 쓰지 마라. 그대로 사용한다."""


def _format_examples() -> str:
    lines = ["아래는 형식을 지킨 예시다(방향을 참고하고, 그대로 베끼지는 마라):\n"]
    for ex in FEW_SHOT_EXAMPLES:
        lines.append(f"- 단어: {ex['word']}")
        lines.append(f"  정답: {ex['correct']}")
        for d in ex["distractors"]:
            lines.append(f"  오답: {d}")
        lines.append("")
    return "\n".join(lines)


def build_batch_prompt(batch: list[dict], context_label: str | None = None) -> str:
    """batch: [{"term_id", "headword", "category"(어휘/속담), "definition", "level",
    "sense_category"}, ...] 최대 10개. context_label은 5-5절의 배치 묶음 기준
    (같은 sense_category 또는 level)을 사람이 읽을 문자열로 넘긴 것 - 프롬프트에
    도메인 맥락을 알려주는 용도일 뿐 정답 텍스트를 대체하지 않는다."""
    kind = batch[0]["category"] if batch else "어휘"
    noun = "낱말" if kind == "어휘" else "속담"

    parts = [
        f"너는 초중고 학생용 국어 {noun} 4지선다 문항의 오답을 만드는 역할이다.",
        f"아래 {len(batch)}개 {noun}마다 정답 뜻풀이가 주어진다. 각각에 대해 오답 3개씩 만들어라.",
    ]
    if context_label:
        parts.append(f"이번 배치의 공통 주제/레벨: {context_label}")
    parts.append("")
    parts.append(RULES_TEXT)
    parts.append("")
    parts.append(_format_examples())
    parts.append("이제 아래 대상에 대해 오답을 만들어라:\n")

    for item in batch:
        parts.append(f"- term_id: {item['term_id']}")
        parts.append(f"  {noun}: {item['headword']}")
        parts.append(f"  정답: {item['definition']}")
        parts.append("")

    parts.append(
        "출력은 다른 설명 없이 아래 JSON 형식만 출력하라(코드블록 없이, "
        "순수 JSON 텍스트만):\n"
        '{"items": [{"term_id": 123, "distractors": ["오답1", "오답2", "오답3"]}, ...]}\n'
        f"배열에는 위 {len(batch)}개 term_id가 전부, 그리고 그것만 있어야 한다."
    )
    return "\n".join(parts)
