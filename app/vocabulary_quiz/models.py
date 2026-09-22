"""일반 어휘 퀴즈 DB(`data/vocab/vocabulary_quiz_rnd.db`) SQLAlchemy 모델.

data/vocab/vocabulary_quiz_schema.sql을 그대로 매핑한다. 기존 app/vocab/
models.py(idiom.db)의 Column(...) 선언 방식을 따르되, Base는 이 모듈
전용 db.py의 것을 쓴다(idiom.db와 별도 engine/세션 - 섞이면 안 된다).
"""
from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
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
