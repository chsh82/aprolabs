# -*- coding: utf-8 -*-
"""B 발췌 프롬프트 골든셋 테스트 — extract_teacher.py 통합 전 독립 실행."""
import os, json, re
import anthropic
from parser import pdftext

client = anthropic.Anthropic()

SYSTEM_PROMPT = """너는 논술 교재 교사용 PDF 원문에서, 주어진 문항 목록 각각에 대해 '정답 텍스트'를
원문 그대로 찾아 발췌하는 도구다. 절대 새로 쓰거나 보완하지 않는다.

절대 원칙:
1. 오직 발췌만 한다. [교사용 원문]에 실제로 있는 문장을 문자 그대로 가져온다.
   요약·의역·재구성·보완 금지. 발췌한 텍스트는 원문의 연속된 부분 문자열이어야 한다.
2. 해당 문항의 답이 원문에 실제로 없으면(교사용 가이드에 답이 비어있는 경우) 빈
   문자열("")을 반환하라. 답이 안 보인다고 다른 문항 내용으로 채우거나 지어내지 마라.
3. 문항 번호(예: "3.")는 문서 안에서 여러 번 등장할 수 있다(다른 단계/섹션에서 번호가
   반복됨). 번호만으로 위치를 정하지 말고, 함께 주어진 [발문] 텍스트와 내용이 일치하는
   지점을 찾아라.
4. 교사용 답변이 "라벨 + 답" 표 형식일 수 있다. 예: 발문 뒤에 그 발문을 짧게 되풀이한
   짧은 라벨(예: "왜 그 인물이 되어보고 싶은가요?", "스크루지가 얻은 교훈은?")이 붙고,
   그 옆/아래에 실제 서술형 답이 붙는 구조. 이때 발췌 대상은 라벨이 아니라 그 옆의
   실제 답 내용이다. 라벨은 발문을 요약한 것뿐이라 답이 아니다.
5. "나는 <　　>책의 주인공이 되어보고 싶어요!" 같은 빈칸 채우기 템플릿 문장 자체는
   정답이 아니라 학생이 채울 빈 틀이다. 이런 템플릿 문장은 건너뛰고, 그 뒤에 오는
   진짜 서술형 답변만 발췌하라.
6. 정답이 여러 문단/여러 표 행으로 나뉘어 있으면 전부 순서대로 이어서 발췌한다.
7. 다음 문항, 다음 작품 제목, "더 알아보기"(작가소개) 같은 이후 섹션의 텍스트는
   정답에 절대 포함하지 않는다. 정답은 해당 문항의 발문과 다음 경계 사이에만 있다.

출력은 JSON 객체 하나만. 키는 문항 번호("2", sub_seq 있으면 "2-1" 형식),
값은 발췌한 정답 텍스트(문자열, 없으면 ""). 마크다운이나 설명 없이 JSON만 출력하라."""

with open('materials.json', encoding='utf-8') as f:
    MATS = {m['work_title']: m for m in json.load(f)}

def teacher_path(title):
    m = MATS[title]
    return m['teacher'] if os.path.isabs(m['teacher']) else os.path.join('data', m['teacher'])

GOLDEN_PROMPTS = {
    "크리스마스 캐롤 2주차": {
        "1": "스크루지는 첫 번째 유령과의 만남에서 무엇을 배웠다고 생각할까요? 지난 시간에 읽은 내용을 떠올리며 스크루지가 얻은 교훈에 대해 생각해 봅시다.",
        "2": "두 번째 유령이 뿌린 향료의 정체는 무엇일까요? 왜 그는 가난한 사람들의 음식에 향료를 뿌려준 것일까요?",
        "6": "마지막 유령은 스크루지에게 단 한마디의 말도 하지 않습니다. 여러분이 미래의 유령이라면 스크루지에게 무슨 말을 해주었을까요?",
    },
    "키다리 아저씨 2주차": {
        "1": "여러분은 어떤 책의 주인공이 되어보고 싶은가요?",
    },
    "크리스마스 캐롤 1주차": {
        "1": "이야기의 주인공 스크루지에 대한 설명입니다. 스크루지는 어떤 사람 같나요?",
    },
    "마법의 수프": {
        "2": "여러분은 시에라리온의 대통령에 당선되었습니다! 국민들을 위해 다이아몬드를 어떻게 이용하면 좋을지, 공주와 왕자의 입장에서 생각해볼까요?",
        "5": "학교에 가기 싫어 모험을 시작한 헤르만이 되어 선택을 해봅시다! 지각의 변명거리를 만들까요? 차라리 결석을 할까요? 아니면 또 다른 선택이 있을까요?",
        "6": "엄마와 아빠는 학교에 가지 않은 헤르만을 왜 혼내지 않았을까요? 헤르만의 비밀 여행은 헤르만에게 어떤 의미였을까요?",
    },
}

EXPECTED = {
    ("크리스마스 캐롤 2주차", "1"): "착하고 순수했",
    ("크리스마스 캐롤 2주차", "2"): "크리스마스에 맛있는 음식을 먹을 수 있도록",
    ("크리스마스 캐롤 2주차", "6"): "",
    ("키다리 아저씨 2주차", "1"): "걸리버 여행기의 주인공이 되어",
    ("크리스마스 캐롤 1주차", "1"): "돈밖에 모르는 욕심쟁이",
    ("마법의 수프", "2"): "서로 싸우지 않게",
    ("마법의 수프", "5"): None,
    ("마법의 수프", "6"): "외롭고 쓸쓸한 마음",
}

LABEL_HINTS = ["왜 그 인물이", "스크루지가 얻은 교훈은", "유령의 향료는", "왜 가난한",
               "왜 헤르만을", "주인공 스크루지는 어떤 사람", "지각의 변명거리를 만든다"]


def extract_via_llm(teacher_text, prompts):
    items_block = "\n".join(f"{k}. 발문: {v}" for k, v in prompts.items())
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001", max_tokens=2000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content":
                   f"[교사용 원문]\n{teacher_text}\n\n[문항 목록]\n{items_block}\n\n"
                   f"위 원칙대로 각 문항의 정답을 발췌해서 JSON으로 출력하라."}])
    text = "".join(b.text for b in msg.content if b.type == "text")
    text = re.sub(r"^```json|```$", "", text.strip()).strip()
    return json.loads(text)


for title, prompts in GOLDEN_PROMPTS.items():
    teacher_text = pdftext(teacher_path(title))
    result = extract_via_llm(teacher_text, prompts)
    print(f"\n=== {title} ===")
    for seq, answer in result.items():
        exp = EXPECTED.get((title, seq))
        has_label_leak = any(h in answer for h in LABEL_HINTS)
        if exp is None:
            status = "? (미확정, 육안 확인)"
        elif exp == "":
            status = "OK(빈답 유지)" if answer == "" else "FAIL(지어냄!)"
        else:
            status = "OK" if exp in answer else "FAIL(기대텍스트 없음)"
        if has_label_leak:
            status += " + LABEL누출!"
        print(f"[{status}] seq{seq}: {answer[:100]!r}")
