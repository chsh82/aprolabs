"""samples/*.layout.json(사람이 손으로 검수·확정한 골든 시안)에서 텍스트를
뽑아 정규화 결과와 비교하는 헬퍼. golden에는 검수 단계에서 고친 오타가
들어있고(정규화는 이걸 고치면 안 됨), Stage 1은 아직 인용 쪽수·참고 노트
같은 걸 질문 텍스트에서 마저 분리하지 않은 상태라(그건 Stage 3/LLM 몫) -
그래서 정확히 같아야 한다고 요구하지 않는다:

- 완전 일치 대신 difflib 유사도(ratio)로 비교한다.
- 내 텍스트가 golden보다 긴 경우(인용/노트가 안 떨어져 나가 붙어있음)를
  감안해, golden이 내 텍스트 안에 '거의' 그대로 들어있는지(prefix 유사도)도
  같이 본다 - 둘 중 하나라도 임계값을 넘으면 통과.
"""
from __future__ import annotations

import difflib
import json
import re
from pathlib import Path

SAMPLES_DIR = Path(__file__).resolve().parent.parent / "samples"

# 이 임계값 아래로 떨어지면 "단순 오타/공백 차이"로 보기 어렵다고 판단한다.
SIMILARITY_THRESHOLD = 0.75


def _norm(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def similar(a: str | None, b: str | None) -> float:
    """a, b 중 하나가 다른 하나를 포함하는 관계까지 고려한 최대 유사도.

    Stage 1은 아직 인용문·참고노트를 질문 텍스트에서 분리하지 않으므로,
    golden(분리 후)의 짧은 문장이 내 텍스트(분리 전, 더 긴) 안의 "중간"에
    있을 수도 있다(예: [라벨] "인용문" 실제질문 순서라 질문이 뒤에 옴) -
    앞부분만 보는 prefix 비교로는 못 잡는다. 그래서 "짧은 쪽 글자가 긴 쪽
    어디엔가 매칭되는 비율"(포함 비율)도 같이 본다."""
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return 0.0
    whole = difflib.SequenceMatcher(None, na, nb).ratio()
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    sm = difflib.SequenceMatcher(None, longer, shorter)
    containment = sum(block.size for block in sm.get_matching_blocks()) / len(shorter)
    return max(whole, containment)


def load_golden(doc_id: str) -> dict:
    path = SAMPLES_DIR / f"{doc_id}.layout.json"
    return json.loads(path.read_text(encoding="utf-8"))


def extract_golden_questions(golden: dict) -> dict[str, str]:
    """{order_label: question_text} - qa/qaband/qaref/solo 페이지의 q.t를 전부 모은다."""
    out = {}
    for p in golden.get("pages", []):
        q = p.get("q")
        if isinstance(q, dict) and q.get("id") and q.get("t"):
            out[q["id"]] = q["t"]
    return out


def extract_golden_excerpts(golden: dict) -> dict[str, str]:
    """{order_label: excerpt_text} - q.id가 있는 페이지의 excerpt.text를 합쳐서."""
    out = {}
    for p in golden.get("pages", []):
        q = p.get("q")
        excerpt = p.get("excerpt")
        if isinstance(q, dict) and q.get("id") and isinstance(excerpt, dict):
            texts = excerpt.get("text") or []
            out[q["id"]] = " ".join(texts)
    return out


def extract_golden_vocab(golden: dict) -> list[dict]:
    for p in golden.get("pages", []):
        if p.get("type") == "vocab":
            return p.get("vocab") or []
    return []


def extract_golden_ox(golden: dict) -> list[dict]:
    for p in golden.get("pages", []):
        if p.get("type") == "oxp":
            return p.get("ox") or []
    return []


def extract_golden_hanja_rows(golden: dict) -> list[dict]:
    for p in golden.get("pages", []):
        ref = p.get("ref")
        if isinstance(ref, dict) and ref.get("rows"):
            return ref["rows"]
    return []
