"""텍스트 손상 복구 - SPEC §4 "제시문 문장부호 밀림", "줄바꿈 하이픈 띄어쓰기".

규칙 기반으로 안전하게 고칠 수 있는 것만 고친다(문장부호 앞 공백, 중복
공백). 단어 내부가 아예 뒤섞인 경우("뿔이 없긴 하지만그래도, 코뿔소야
그들이.")는 규칙으로 못 고친다 - 그런 행은 원문을 그대로 두고 플래그만
남긴다(SPEC §4: "규칙 기반 복원 + LLM 교정, 원문과 diff 표시" - LLM 교정은
이 모듈의 책임이 아니라 검수 단계 호출로 넘긴다).
"""
from __future__ import annotations

import re
import unicodedata

from normalize.models import Flag

# 문장부호 앞에 공백이 낀 경우 - " ." -> "." 처럼 안전하게 제거 가능
_SPACE_BEFORE_PUNCT = re.compile(r"\s+([.,!?])")
_MULTI_SPACE = re.compile(r"[ \t]{2,}")

def normalize_cjk_compat(text: str) -> str:
    """CJK 호환용 표의문자(U+F900~U+FAFF)만 표준 코드포인트로 되돌린다 - 예:
    U+F9D0('類'의 호환 변형) -> U+985E(표준 '類'). 문자열 전체에 NFKC를
    걸면 한글 호환 자모(예: 'ㆍ' U+318D)까지 다른 코드포인트로 바뀌어서
    오히려 golden과 달라지는 사례를 실제로 확인했다(정규화가 이미 맞는
    걸 깨버림) - 그래서 실제로 문제였던 이 좁은 범위만 건드린다."""
    if not any(0xF900 <= ord(ch) <= 0xFAFF for ch in text):
        return text
    return "".join(
        unicodedata.normalize("NFKC", ch) if 0xF900 <= ord(ch) <= 0xFAFF else ch
        for ch in text
    )


# 원문(정리 전)에 한글 사이 공백 3개 이상이 있으면 단순 띄어쓰기 실수가
# 아니라 어순/단어가 흐트러진 것으로 의심한다(사람이 봐야 함) - 실제로
# "그래도,    코뿔소야그들이." 같은 사례에서 이 패턴을 확인했다. _MULTI_SPACE로
# 공백을 지운 "뒤" 텍스트를 검사하면 이 신호 자체가 사라지므로(실제로 이
# 버그를 만들고 고쳤다) 반드시 정리 "전" 원문에 대해 검사해야 한다.
_SUSPICIOUS_RESIDUE = re.compile(r"[가-힣][,.]?\s{3,}[가-힣]")


def repair_text(text: str | None, order_no: int | None = None) -> tuple[str | None, list[Flag]]:
    if not text:
        return text, []

    flags: list[Flag] = []
    before = text

    # CJK 호환용 표의문자를 표준 코드포인트로 되돌린다(위 normalize_cjk_compat
    # 참고 - 골든 샘플 대조 중 "類推"의 "類"가 이런 문제였음을 실제로 확인).
    text = normalize_cjk_compat(text)
    if text != before:
        flags.append(Flag(
            kind="typo", message="CJK 호환용 코드포인트를 표준 코드포인트로 정규화",
            before=before, after=text, order_no=order_no,
        ))
        before = text

    if _SUSPICIOUS_RESIDUE.search(before):
        flags.append(Flag(
            kind="split",
            message="한글 사이에 공백이 3개 이상 연속됨 - 단순 띄어쓰기가 아니라 어순/단어가 "
                     "흐트러진 것으로 의심됨, LLM 교정 필요",
            before=before, order_no=order_no,
        ))

    cleaned = _SPACE_BEFORE_PUNCT.sub(r"\1", text)
    cleaned = _MULTI_SPACE.sub(" ", cleaned)
    cleaned = cleaned.strip()

    if cleaned != before:
        flags.append(Flag(
            kind="typo", message="문장부호 앞 공백/중복 공백 규칙 기반 정리",
            before=before, after=cleaned, order_no=order_no,
        ))

    return cleaned, flags
