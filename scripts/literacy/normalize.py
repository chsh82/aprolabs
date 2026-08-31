"""표제어 정제 - 여러 소스(교재 어휘 등)가 공통으로 쓰는 재사용 모듈.

docs/literacy/03-교재어휘적재.md 5절 규칙을 그대로 구현한다:
1. 페이지 표기가 **확실한** 패턴만 정확히 매치해서 제거한다. 실제 교재DB
   67건을 보고 뽑은 변형: `(NN쪽)`, `(NNp)`, `(pNN)`, `(p.NN)`,
   `(페NN이지)`(OCR로 "NN페이지"가 뒤섞인 형태). 애매한 것(예: `(성치=성하지)`
   같은 활용형 설명, `(추호도 없다)` 같은 예시 구, `(진저리)` 같은 동의어
   참조, `(어)`처럼 짧은 단독 음절)은 페이지 표기인지 확신할 수 없어
   손대지 않고 `other_parens`로만 보고한다 - 패턴을 임의로 늘리지 말라는
   지시 때문.
2. 괄호 안이 한자(CJK 통합 한자 블록)면 제거하되 `origin`으로 이관한다.
3. 앞뒤 공백·개행을 제거한다.
4. 위 1·2로 처리되지 않은 나머지 괄호는 지우지 않고 그대로 두되,
   `other_parens`에 내용을 남겨서 dry-run 보고에 쓴다.

정제 전 원문은 호출부가 `note`에 그대로 보존해야 한다(이 모듈은 원문
자체를 CleanResult.original에 담아 돌려주기만 한다).
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# 페이지 표기로 확신할 수 있는 변형만 나열한다(67건 실측 기반):
#   (111쪽) / (25p) / (p34) / (p.17) / (페150이지) - 마지막은 "150페이지"가
#   OCR로 숫자와 "페"/"이지"가 뒤섞인 형태.
_PAGE_ANNOTATION_RE = re.compile(
    r"\s*\((?:\d+쪽|\d+[pP]|[pP]\d+|[pP]\.\d+|페\d+이지)\)\s*$"
)
_HANJA_RE = re.compile(r"\s*\(([一-鿿]+)\)\s*$")
_ANY_PAREN_RE = re.compile(r"\([^)]*\)")


@dataclass
class CleanResult:
    original: str
    cleaned: str
    origin: str | None = None          # 한자 등 원어(괄호에서 분리)
    other_parens: list[str] | None = None  # 처리 안 된 나머지 괄호 내용(있으면)


def clean_headword(raw: str) -> CleanResult:
    text = _PAGE_ANNOTATION_RE.sub("", raw)

    origin = None
    match = _HANJA_RE.search(text)
    if match:
        origin = match.group(1)
        text = _HANJA_RE.sub("", text)

    text = text.strip()

    other_parens = _ANY_PAREN_RE.findall(text)

    return CleanResult(original=raw, cleaned=text, origin=origin, other_parens=other_parens or None)


def is_suspected_swap(cleaned: str) -> bool:
    """word/definition 컬럼이 뒤바뀐 것으로 의심되는 표제어인지.

    기준은 종결부호(. 또는 。)로 끝나는지 하나뿐이다(사용자 결정 - 공백/길이
    기준은 범위가 넓어서 정상 표제어를 걸러낼 위험이 있어 제외함).
    """
    return cleaned.endswith(".") or cleaned.endswith("。")


def has_space(cleaned: str) -> bool:
    """정제된 표제어에 공백이 남아 있는지 - 어휘 표제어는 단어 하나라 공백이
    있으면 안 됨(사용자 결정). 컬럼 뒤바뀜 의심(is_suspected_swap)이 먼저
    걸러내므로, 여기 걸리는 건 그 외의 "단어가 아닌" 항목(작가 소개 문단
    등)이거나 "산술 평균"류의 정상 복합어일 수 있다 - 호출부가 전체 목록을
    보고해서 사람이 확인해야 한다.
    """
    return " " in cleaned
