"""어휘 게임 DB(`data/vocab/idiom.db`) SQLAlchemy 모델.

data/vocab/schema.sql을 그대로 매핑한다. idiom/inclusion_evidence/hanja/
idiom_hanja/example/relation/topic_link/corpus_hit 8개는 이미 schema.sql로
생성·적용된 기존 테이블 - 여기 선언은 문서화 + 향후 재생성(--drop 등) 대비
목적이며 create_all이 다시 만들지는 않는다. attempt만 신규(HANDOFF.md 5절,
migrations/001_add_attempt.py에서 실제로 추가한다).

기존 app/literacy/models.py의 Column(...) 선언 방식(Mapped[...] 신문법 아님)을
그대로 따른다.
"""
from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
    text,
)

from app.vocab.db import Base


class Idiom(Base):
    """표제어."""

    __tablename__ = "idiom"

    idiom_id = Column(Integer, primary_key=True, autoincrement=True)
    headword = Column(Text, nullable=False, unique=True)  # 우공이산
    hanja = Column(Text, nullable=False)  # 愚公移山
    literal = Column(Text, nullable=True)  # 직역
    meaning = Column(Text, nullable=True)  # 뜻풀이 (중등 기준)
    meaning_easy = Column(Text, nullable=True)  # 초등용 쉬운 풀이
    origin_source = Column(Text, nullable=True)  # 출전
    origin_story = Column(Text, nullable=True)  # 유래 요약

    # 난이도 3축 (0.0~1.0) - 단일 등급 대신 축을 분리해 저장한다
    hanja_score = Column(Float, nullable=True)
    abstraction_score = Column(Float, nullable=True)
    frequency_score = Column(Float, nullable=True)

    level_min = Column(Integer, nullable=True, index=True)  # 권장 최소 학년(1~12), 3축에서 산출
    level_note = Column(Text, nullable=True)  # 등급 산정 근거 메모

    status = Column(Text, nullable=False, server_default=text("'draft'"), index=True)
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
    updated_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))

    __table_args__ = (
        CheckConstraint("status IN ('draft','reviewed','published')", name="ck_idiom_status"),
    )


class InclusionEvidence(Base):
    """표제어 선정 근거 - source_type: textbook > exam > media > momo."""

    __tablename__ = "inclusion_evidence"

    evidence_id = Column(Integer, primary_key=True, autoincrement=True)
    idiom_id = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False, index=True)
    source_type = Column(Text, nullable=False)
    detail = Column(Text, nullable=True)  # '중2 국어 미래엔 3단원' / '2021 수능 국어 12번'
    grade_band = Column(Text, nullable=True)  # 'elem' | 'mid' | 'high'
    hit_count = Column(Integer, nullable=True, server_default=text("1"))

    __table_args__ = (
        CheckConstraint(
            "source_type IN ('textbook','exam','media','momo')", name="ck_evidence_source_type"
        ),
    )


class Hanja(Base):
    """한자 낱글자."""

    __tablename__ = "hanja"

    char = Column(Text, primary_key=True)  # 愚
    hun = Column(Text, nullable=True)  # 어리석을
    eum = Column(Text, nullable=True)  # 우
    grade = Column(Text, nullable=True)  # 한자능력검정 급수
    is_basic900 = Column(Integer, nullable=True, server_default=text("0"))


class IdiomHanja(Base):
    """성어-한자 연결 (구성 글자 순서)."""

    __tablename__ = "idiom_hanja"

    idiom_id = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False)
    position = Column(Integer, nullable=False)  # 1~4 (1부터 시작 - literacy의 term_hanja와 달리 0-index 아님)
    char = Column(Text, ForeignKey("hanja.char"), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("idiom_id", "position"),
        CheckConstraint("position BETWEEN 1 AND 4", name="ck_idiom_hanja_position"),
    )


class Example(Base):
    """용례 - 성어당 최소 2개, 성격이 다른 문장을 섞는다."""

    __tablename__ = "example"

    example_id = Column(Integer, primary_key=True, autoincrement=True)
    idiom_id = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False, index=True)
    sentence = Column(Text, nullable=False)
    context_type = Column(Text, nullable=True)
    grade_band = Column(Text, nullable=True)
    source = Column(Text, nullable=True)  # 인용이면 출처, 창작이면 NULL

    __table_args__ = (
        CheckConstraint(
            "context_type IN ('situation','essay','literature','media','dialogue')",
            name="ck_example_context_type",
        ),
    )


class Relation(Base):
    """성어 간 관계 - confusable(혼동쌍)이 출제에서 가장 활용도가 높다."""

    __tablename__ = "relation"

    relation_id = Column(Integer, primary_key=True, autoincrement=True)
    idiom_a = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False)
    idiom_b = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False)
    rel_type = Column(Text, nullable=False)
    note = Column(Text, nullable=True)  # 혼동쌍이면 '무엇이 다른가'를 한 문장으로

    __table_args__ = (
        UniqueConstraint("idiom_a", "idiom_b", "rel_type", name="uq_relation"),
        CheckConstraint("rel_type IN ('synonym','antonym','confusable')", name="ck_relation_type"),
        CheckConstraint("idiom_a <> idiom_b", name="ck_relation_distinct"),
    )


class TopicLink(Base):
    """논술 주제 연결 (모모의 책장 커리큘럼 연동)."""

    __tablename__ = "topic_link"

    idiom_id = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False)
    topic = Column(Text, nullable=False)  # '정의', '노력과 성취', '권력과 책임'

    __table_args__ = (
        PrimaryKeyConstraint("idiom_id", "topic"),
    )


class CorpusHit(Base):
    """교재 등장 기록 (PDF 코퍼스 추출 결과) - frequency_score와 교재 연결에 쓴다."""

    __tablename__ = "corpus_hit"

    hit_id = Column(Integer, primary_key=True, autoincrement=True)
    idiom_id = Column(Integer, ForeignKey("idiom.idiom_id", ondelete="CASCADE"), nullable=False, index=True)
    doc_id = Column(Text, nullable=True)
    material = Column(Text, nullable=True)
    lesson = Column(Text, nullable=True)
    snippet = Column(Text, nullable=True)


class Attempt(Base):
    """학습 기록 - HANDOFF.md 5절, 스키마 최초 도입분(migrations/001_add_attempt.py).

    ms(응답 시간)를 반드시 기록한다 - 맞혔지만 오래 걸린 문항이 그 학생의
    약한 지점이고, 재출제 간격 산정과 만점자용 복습 문항 선정에 쓰인다.
    """

    __tablename__ = "attempt"

    attempt_id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(Text, nullable=False)
    idiom_id = Column(Integer, ForeignKey("idiom.idiom_id"), nullable=True, index=True)
    game = Column(Text, nullable=False)  # 'archery' | 'suez' | ...
    format = Column(Text, nullable=False)
    is_correct = Column(Integer, nullable=False)
    ms = Column(Integer, nullable=True)  # 응답까지 걸린 시간
    answered_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))

    __table_args__ = (
        Index("idx_attempt_student", "student_id", "idiom_id"),
    )
