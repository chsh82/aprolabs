"""① 정규화 레코드 - SPEC §1/§4.

원본 DB 행을 그대로 옮기지 않는다. discussion_qa의 컬럼 밀림/unknown 행을
재조립하고, 텍스트 손상을 표시하고, 어휘/OX 누락을 플래그한 뒤 이 형태로
낸다. 다음 단계(② 조판 초안)가 이 레코드를 입력으로 받는다.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Flag:
    """정규화 과정에서 사람이 확인해야 할 지점. kind는 다음 7가지만 쓴다:

    - sup:     LLM/사전DB가 자동으로 채울 수 있는 누락(예: 어휘 뜻풀이).
    - missing: 원본에 값 자체가 없음, LLM 대상 아님(예: OX 정답 null -
               교사용 교재 등 사람이 찾아야 함). sup와 헷갈리지 말 것.
    - derived: 원문에 없던 내용을 새로 만들어야 함(예: 질문 자체가 DB에 없음).
    - split:   컬럼 밀림/unknown 행 재조립, 문장 분리 등 구조적 재구성.
    - typo:    규칙 기반으로 고친(또는 고쳐야 할) 텍스트 손상.
    - lowres:  이미지 해상도 경고.
    - ocr:     (예약 - 손글씨/스캔 인식 관련, 이 모듈에서는 아직 안 씀)
    """
    kind: str
    message: str
    before: str | None = None
    after: str | None = None
    order_no: int | None = None
    # ②→④단계 전용(선택): edition_flag에 저장할 때 kind 대신 이 값을 쓴다. 사용자
    # 지시(2026-09-23) 4단계 1번 - widget_unavailable(위젯 신호 없음)/placeholder
    # (자리표시자, approve 차단 대상)처럼 정규화 7종 분류와는 다른 축으로 다시 갈라야
    # 할 때만 채운다(layout/step*.py). 비워두면 edition/store.py가 kind를 그대로 쓴다.
    category: str | None = None


@dataclass
class HanjaGloss:
    """중등 unknown 행 중 한자 뜻풀이 목록 - qaref.ref.rows({n,v})의 재료.

    gap_after: 이 용어 뒤에 "~"(생략 표시)가 있었다는 뜻 - 실제 데이터(L9-Q3-W07)에서
    확인됨: 표제어 1~4, 16~19가 있고 5~15는 생략돼 있는데, 그 경계에 "~"만 남아있다.
    조판 단계(ref.rows)에서 {gap:true} 행으로 표시할 재료로 그대로 들고 있는다."""
    order_no: int
    term: str  # 원문 그대로(괄호 안 한자 포함), 예: "상징하다(象徵--)"
    gap_after: bool = False


@dataclass
class NormalizedQA:
    order_no: int
    order_label: str
    reading_type: str | None
    ui_type: str  # 'unknown'이 여기 남으면 안 된다(완료 기준)
    excerpt_text: str | None
    excerpt_page: int | None
    question_text: str
    model_answer: str | None
    source_page: int | None
    ui_config: dict = field(default_factory=dict)
    flags: list[Flag] = field(default_factory=list)


@dataclass
class NormalizedVocab:
    order_no: int
    word: str
    definition: str | None
    example_sentence: str | None
    book_page: int | None = None
    flags: list[Flag] = field(default_factory=list)


@dataclass
class NormalizedOx:
    order_no: int
    question: str
    answer: bool | None
    explanation: str | None
    evidence_page: int | None = None
    flags: list[Flag] = field(default_factory=list)


@dataclass
class NormalizedEssayOutlineQuestion:
    order_no: int
    role: str  # intro/body/conclusion
    question_text: str


@dataclass
class NormalizedEssay:
    main_topic: str
    lead: str | None  # writing_guide에서 분리된 인용/도입부
    writing_format: str
    closing_instruction: str | None
    image_path: str | None
    outline: list[NormalizedEssayOutlineQuestion] = field(default_factory=list)
    flags: list[Flag] = field(default_factory=list)


@dataclass
class NormalizedImage:
    image_type: str
    source_page: int | None
    file_path: str
    extraction_confidence: float | None


@dataclass
class NormalizedDoc:
    doc_id: str
    book_title: str
    book_author: str | None
    level: str
    band: str  # lower|elem-upper|mid (SPEC §3.2, level에서 파생)
    quarter: str  # winter|spring|summer|autumn (SPEC §3.3, DB quarter에서 파생)
    week: int
    cover_message: str | None
    background_text: str | None
    qa: list[NormalizedQA] = field(default_factory=list)
    vocab: list[NormalizedVocab] = field(default_factory=list)
    ox: list[NormalizedOx] = field(default_factory=list)
    hanja_glossary: list[HanjaGloss] = field(default_factory=list)
    essay: NormalizedEssay | None = None
    images: list[NormalizedImage] = field(default_factory=list)
    flags: list[Flag] = field(default_factory=list)  # 문서 단위 플래그(essay 분리 등)

    def all_flags(self) -> list[Flag]:
        out = list(self.flags)
        for q in self.qa:
            out.extend(q.flags)
        for v in self.vocab:
            out.extend(v.flags)
        for o in self.ox:
            out.extend(o.flags)
        if self.essay:
            out.extend(self.essay.flags)
        return out

    def unresolved_unknown_count(self) -> int:
        return sum(1 for q in self.qa if q.ui_type == "unknown")
