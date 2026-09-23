"""일반 어휘 퀴즈 DB(`data/vocab/vocabulary_quiz_rnd.db`) SQLAlchemy 모델.

data/vocab/vocabulary_quiz_schema.sql을 그대로 매핑한다. 기존 app/vocab/
models.py(idiom.db)의 Column(...) 선언 방식을 따르되, Base는 이 모듈
전용 db.py의 것을 쓴다(idiom.db와 별도 engine/세션 - 섞이면 안 된다).
"""
from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    text,
)

from app.vocabulary_quiz.db import Base


class VocabularyImportBatch(Base):
    __tablename__ = "vocabulary_import_batches"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','DRY_RUN_OK','DRY_RUN_FAILED','COMPLETED','FAILED')",
            name="ck_batch_status",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(Text, nullable=False, index=True)
    source_filename = Column(Text, nullable=True)
    source_sha256 = Column(Text, nullable=True)
    started_at = Column(Text, nullable=True)
    completed_at = Column(Text, nullable=True)
    status = Column(Text, nullable=False, server_default=text("'PENDING'"))
    content_count = Column(Integer, nullable=True)
    item_count = Column(Integer, nullable=True)
    inserted_count = Column(Integer, nullable=True)
    updated_count = Column(Integer, nullable=True)
    unchanged_count = Column(Integer, nullable=True)
    failed_count = Column(Integer, nullable=True)
    validation_result = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)


class VocabularyContent(Base):
    __tablename__ = "vocabulary_contents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(Text, nullable=False, unique=True)
    sense_id = Column(Text, nullable=True, index=True)
    lexical_entry_id = Column(Text, nullable=True)
    batch_id = Column(Text, nullable=True)
    lemma = Column(Text, nullable=False, index=True)
    pos = Column(Text, nullable=True)
    canonical_definition = Column(Text, nullable=True)
    student_definition = Column(Text, nullable=True)
    example_sentence = Column(Text, nullable=True)
    example_target_form = Column(Text, nullable=True)
    generation_method = Column(Text, nullable=True)
    qa_method = Column(Text, nullable=True)
    generation_status = Column(Text, nullable=True, index=True)
    student_exposure = Column(Integer, nullable=False, server_default=text("0"))
    public_ready = Column(Integer, nullable=False, server_default=text("0"))
    quality_batch_id = Column(Text, nullable=True)
    hold_reason = Column(Text, nullable=True)
    merge_source = Column(Text, nullable=True)
    source_version = Column(Text, nullable=False)
    is_active = Column(Integer, nullable=False, server_default=text("1"))
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
    updated_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))


class VocabularyItem(Base):
    __tablename__ = "vocabulary_items"
    __table_args__ = (
        CheckConstraint("correct_option BETWEEN 1 AND 4", name="ck_item_correct_option"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Text, nullable=False, unique=True)
    content_id = Column(Text, ForeignKey("vocabulary_contents.content_id", ondelete="CASCADE"),
                         nullable=False, index=True)
    sense_id = Column(Text, nullable=True, index=True)
    batch_id = Column(Text, nullable=True)
    lemma = Column(Text, nullable=True)
    item_type = Column(Text, nullable=True)
    stem = Column(Text, nullable=True)
    option_1 = Column(Text, nullable=True)
    option_2 = Column(Text, nullable=True)
    option_3 = Column(Text, nullable=True)
    option_4 = Column(Text, nullable=True)
    correct_option = Column(Integer, nullable=False)
    explanation = Column(Text, nullable=True)
    generation_status = Column(Text, nullable=True)
    student_exposure = Column(Integer, nullable=False, server_default=text("0"))
    public_ready = Column(Integer, nullable=False, server_default=text("0"))
    quality_batch_id = Column(Text, nullable=True)
    merge_source = Column(Text, nullable=True)
    source_version = Column(Text, nullable=False)
    is_active = Column(Integer, nullable=False, server_default=text("1"))
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
    updated_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))


class VocabularyReviewSample(Base):
    __tablename__ = "vocabulary_review_samples"
    __table_args__ = (
        CheckConstraint(
            "review_status IN ('UNREVIEWED','PASS','REVISE','EXCLUDE')",
            name="ck_review_status",
        ),
        UniqueConstraint("content_id", "sample_version", name="uq_review_content_version"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_version = Column(Text, nullable=False, index=True)
    item_id = Column(Text, ForeignKey("vocabulary_items.item_id", ondelete="CASCADE"), nullable=False)
    content_id = Column(Text, ForeignKey("vocabulary_contents.content_id", ondelete="CASCADE"), nullable=False)
    sampling_group = Column(Text, nullable=True)
    sampling_seed = Column(Integer, nullable=False)
    review_status = Column(Text, nullable=False, server_default=text("'UNREVIEWED'"), index=True)
    review_note = Column(Text, nullable=True)
    reviewed_at = Column(Text, nullable=True)
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
    updated_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))


class VocabularyQaBatch(Base):
    """사람이 표본을 검수한 세션 단위 요약(승인 의사결정) 기록.
    vocabulary_review_samples(개별 항목 상태)와는 별개다."""
    __tablename__ = "vocabulary_qa_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(Text, nullable=False, index=True)
    reviewed_sample_count = Column(Integer, nullable=False)
    pass_count = Column(Integer, nullable=False)
    revise_count = Column(Integer, nullable=False)
    exclude_count = Column(Integer, nullable=False)
    critical_error_count = Column(Integer, nullable=False)
    major_error_count = Column(Integer, nullable=False)
    minor_error_count = Column(Integer, nullable=False)
    approved_for_rnd_quiz = Column(Integer, nullable=False, server_default=text("0"))
    approved_for_public = Column(Integer, nullable=False, server_default=text("0"))
    reviewed_at = Column(Text, nullable=False)
    notes = Column(Text, nullable=True)
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))


class VocabularyQuizSession(Base):
    __tablename__ = "vocabulary_quiz_sessions"
    __table_args__ = (
        CheckConstraint("status IN ('in_progress', 'completed')", name="ck_quiz_session_status"),
    )

    id = Column(Text, primary_key=True)
    user_id = Column(Text, nullable=False, index=True)
    source_version = Column(Text, nullable=False)
    question_count = Column(Integer, nullable=False)
    correct_count = Column(Integer, nullable=False, server_default=text("0"))
    status = Column(Text, nullable=False, server_default=text("'in_progress'"))
    started_at = Column(Text, nullable=False)
    completed_at = Column(Text, nullable=True)


class VocabularyQuizAttempt(Base):
    __tablename__ = "vocabulary_quiz_attempts"
    __table_args__ = (
        CheckConstraint("selected_option IS NULL OR selected_option BETWEEN 1 AND 4",
                         name="ck_attempt_selected_option"),
        CheckConstraint("correct_option BETWEEN 1 AND 4", name="ck_attempt_correct_option"),
        UniqueConstraint("session_id", "item_id", name="uq_attempt_session_item"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("vocabulary_quiz_sessions.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Text, nullable=False)
    order_index = Column(Integer, nullable=False)
    selected_option = Column(Integer, nullable=True)
    correct_option = Column(Integer, nullable=False)
    is_correct = Column(Integer, nullable=True)
    answered_at = Column(Text, nullable=True)


class VocabularyMultiformatItem(Base):
    """초등 다유형 퀴즈(파일럿) 파생 문항. 유형마다 필요한 필드가 달라 정답
    정보는 answer_payload_json 하나로 정규화한다(채점 로직이 유형 분기 없이
    이 필드만 보면 되게) - API가 이 컬럼을 클라이언트에 내려주면 안 된다."""
    __tablename__ = "vocabulary_multiformat_items"
    __table_args__ = (
        CheckConstraint(
            "item_type IN ('MEANING_CHOICE','WORD_FROM_DEFINITION','CONTEXT_MEANING',"
            "'CONTEXT_CLOZE','MATCH_WORD_MEANING','CROSSWORD')",
            name="ck_mf_item_type",
        ),
        CheckConstraint("correct_option IS NULL OR correct_option BETWEEN 1 AND 4",
                         name="ck_mf_item_correct_option"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Text, nullable=False, unique=True)
    item_type = Column(Text, nullable=False, index=True)
    source_content_id = Column(Text, ForeignKey("vocabulary_contents.content_id"), nullable=True, index=True)
    source_content_ids_json = Column(Text, nullable=True)
    sense_id = Column(Text, nullable=True)
    sense_ids_json = Column(Text, nullable=True)
    lemma = Column(Text, nullable=True)
    pos = Column(Text, nullable=True)
    prompt = Column(Text, nullable=False)
    options_json = Column(Text, nullable=True)
    correct_option = Column(Integer, nullable=True)
    public_payload_json = Column(Text, nullable=True)
    answer_payload_json = Column(Text, nullable=False)
    explanation = Column(Text, nullable=True)
    cognitive_level = Column(Integer, nullable=True)
    qa_flags_json = Column(Text, nullable=True)
    generator_version = Column(Text, nullable=True)
    source_version = Column(Text, nullable=False, index=True)
    is_active = Column(Integer, nullable=False, server_default=text("1"))
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
    updated_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))


class VocabularyMultiformatImportBatch(Base):
    __tablename__ = "vocabulary_multiformat_import_batches"
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','DRY_RUN_OK','DRY_RUN_FAILED','COMPLETED','FAILED')",
            name="ck_mf_batch_status",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(Text, nullable=False, index=True)
    source_filename = Column(Text, nullable=True)
    source_sha256 = Column(Text, nullable=True)
    seed = Column(Integer, nullable=True)
    selected_words = Column(Integer, nullable=True)
    started_at = Column(Text, nullable=True)
    completed_at = Column(Text, nullable=True)
    status = Column(Text, nullable=False, server_default=text("'PENDING'"))
    item_count = Column(Integer, nullable=True)
    item_type_counts_json = Column(Text, nullable=True)
    inserted_count = Column(Integer, nullable=True)
    updated_count = Column(Integer, nullable=True)
    unchanged_count = Column(Integer, nullable=True)
    validation_result = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)


class VocabularyMultiformatSession(Base):
    __tablename__ = "vocabulary_multiformat_sessions"
    __table_args__ = (
        CheckConstraint("status IN ('in_progress', 'completed')", name="ck_mf_session_status"),
    )

    id = Column(Text, primary_key=True)
    user_id = Column(Text, nullable=False, index=True)
    source_version = Column(Text, nullable=False)
    item_types_json = Column(Text, nullable=True)
    question_count = Column(Integer, nullable=False)
    correct_count = Column(Integer, nullable=False, server_default=text("0"))
    status = Column(Text, nullable=False, server_default=text("'in_progress'"))
    started_at = Column(Text, nullable=False)
    completed_at = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=True)  # 관리자 레벨별 출제(v1)의 선택 조건 스냅샷 -
    # {audience, selected_vocab_level, confidence_mode, level_version, requested_count,
    # candidate_count, actual_count, item_types}. 기존 세션은 NULL - 결과 화면에서 "레벨 미지정"으로 표시.


class VocabularyMultiformatResponse(Base):
    __tablename__ = "vocabulary_multiformat_responses"
    __table_args__ = (
        UniqueConstraint("session_id", "item_id", name="uq_mf_response_session_item"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Text, ForeignKey("vocabulary_multiformat_sessions.id", ondelete="CASCADE"), nullable=False)
    item_id = Column(Text, ForeignKey("vocabulary_multiformat_items.item_id"), nullable=False)
    order_index = Column(Integer, nullable=False)
    item_type = Column(Text, nullable=False)
    submitted_payload_json = Column(Text, nullable=True)
    is_correct = Column(Integer, nullable=True)
    correct_count = Column(Integer, nullable=True)
    total_count = Column(Integer, nullable=True)
    attempt_count = Column(Integer, nullable=False, server_default=text("0"))
    hint_used = Column(Integer, nullable=False, server_default=text("0"))
    answered_at = Column(Text, nullable=True)


class VocabularyContentLevel(Base):
    """vocabulary_contents 1건당 정책 버전별로 여러 행을 가질 수 있다(정책이
    바뀌면 새 level_version으로 새 행 추가, 과거 후보도 보존) - UNIQUE는
    content_id 단독이 아니라 (content_id, level_version) 복합키. 아직 학생
    출제/공개(student_exposure, public_ready)에는 전혀 연결되지 않은
    "자동 후보" 단계."""
    __tablename__ = "vocabulary_content_levels"
    __table_args__ = (
        CheckConstraint("vocab_level BETWEEN 0 AND 6", name="ck_vcl_vocab_level"),
        CheckConstraint("level_confidence IS NULL OR (level_confidence BETWEEN 0 AND 1)",
                         name="ck_vcl_level_confidence"),
        CheckConstraint("level_status IN ('PROVISIONAL_AUTO', 'REVIEW_BOUNDARY')", name="ck_vcl_level_status"),
        UniqueConstraint("content_id", "level_version", name="uq_vcl_content_version"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(Text, ForeignKey("vocabulary_contents.content_id"), nullable=False, index=True)
    vocab_level = Column(Integer, nullable=False)
    target_grade_band = Column(Text, nullable=True)
    level_score = Column(Float, nullable=True)
    level_confidence = Column(Float, nullable=True)
    level_status = Column(Text, nullable=False)
    boundary_flag = Column(Integer, nullable=False, server_default=text("0"))
    level_source = Column(Text, nullable=True)
    level_version = Column(Text, nullable=False, index=True)
    level_reason_json = Column(Text, nullable=True)
    is_active = Column(Integer, nullable=False, server_default=text("1"))
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
    updated_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
