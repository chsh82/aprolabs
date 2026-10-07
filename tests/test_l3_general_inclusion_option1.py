"""L3 보강 옵션1 일반 L3 포함 로컬 리허설 회귀 테스트.

실행:
    pytest tests/test_l3_general_inclusion_option1.py -q

이 테스트는 in-memory SQLite만 사용하며 실제 연구 DB를 읽거나 쓰지 않는다.
"""
from __future__ import annotations

from fastapi import Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.vocabulary_quiz.db import Base
from app.vocabulary_quiz.models import (
    VocabularyContent,
    VocabularyContentLevel,
    VocabularyMultiformatItem,
)
from app.vocabulary_quiz.routers import multiformat as mf


def _content(content_id: str, source_version: str = mf.SOURCE_VERSION) -> VocabularyContent:
    return VocabularyContent(
        content_id=content_id,
        lemma=content_id,
        source_version=source_version,
        is_active=1,
        public_ready=0,
        student_exposure=0,
    )


def _level(content_id: str, level: int, status: str) -> VocabularyContentLevel:
    return VocabularyContentLevel(
        content_id=content_id,
        vocab_level=level,
        level_status=status,
        level_version=mf.LEVEL_VERSION,
        is_active=1,
    )


def _item(item_id: str, content_id: str, source_version: str, item_type: str = "MEANING_CHOICE") -> VocabularyMultiformatItem:
    return VocabularyMultiformatItem(
        item_id=item_id,
        item_type=item_type,
        source_content_id=content_id,
        lemma=content_id,
        prompt=f"prompt {item_id}",
        answer_payload_json='{"correct_option": 1}',
        source_version=source_version,
        is_active=1,
    )


def _session_with_fixture():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    mf._l3_general_inclusion_option1_rows_cache = None
    whitelist_rows = mf._load_l3_general_inclusion_option1_rows()
    wl_l3 = whitelist_rows[0]
    wl_l2 = next(row for row in whitelist_rows if row["content_id"] != wl_l3["content_id"])

    db.add_all([
        _content("BASE-L3"),
        _content("BASE-L2"),
        _content(wl_l3["content_id"], wl_l3["source_version"]),
        _content(wl_l2["content_id"], wl_l2["source_version"]),
        _content("UNWHITELISTED-L3", mf.GRADE5_L3_BATCH1_SOURCE_VERSION),
        _level("BASE-L3", 3, "PROVISIONAL_AUTO"),
        _level("BASE-L2", 2, "PROVISIONAL_AUTO"),
        _level(wl_l3["content_id"], 3, "REVIEW_BOUNDARY"),
        _level(wl_l2["content_id"], 2, "REVIEW_BOUNDARY"),
        _level("UNWHITELISTED-L3", 3, "REVIEW_BOUNDARY"),
        _item("BASE-L3-I", "BASE-L3", mf.SOURCE_VERSION),
        _item("BASE-L2-I", "BASE-L2", mf.SOURCE_VERSION),
        _item(wl_l3["item_id"], wl_l3["content_id"], wl_l3["source_version"], wl_l3["item_type"]),
        _item(wl_l2["item_id"], wl_l2["content_id"], wl_l2["source_version"], wl_l2["item_type"]),
        _item("UNWHITELISTED-L3-I", "UNWHITELISTED-L3", mf.GRADE5_L3_BATCH1_SOURCE_VERSION),
    ])
    db.commit()
    return db, wl_l3, wl_l2


def test_l3_all_candidates_includes_only_option1_whitelist_items():
    db, wl_l3, _ = _session_with_fixture()
    try:
        candidates = set(mf._select_level_candidates(db, 3, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates"))
        assert "BASE-L3-I" in candidates
        assert wl_l3["item_id"] in candidates
        assert "UNWHITELISTED-L3-I" not in candidates
    finally:
        db.close()


def test_l3_auto_only_does_not_include_review_boundary_option1_items():
    db, wl_l3, _ = _session_with_fixture()
    try:
        candidates = set(mf._select_level_candidates(db, 3, list(mf.LEVEL_MODE_ITEM_TYPES), "auto_only"))
        assert candidates == {"BASE-L3-I"}
        assert wl_l3["item_id"] not in candidates
    finally:
        db.close()


def test_non_l3_level_does_not_include_option1_whitelist_even_if_level_row_matches():
    db, _, wl_l2 = _session_with_fixture()
    try:
        candidates = set(mf._select_level_candidates(db, 2, list(mf.LEVEL_MODE_ITEM_TYPES), "all_candidates"))
        assert candidates == {"BASE-L2-I"}
        assert wl_l2["item_id"] not in candidates
    finally:
        db.close()


def test_level_none_availability_remains_source_version_only():
    db, _, _ = _session_with_fixture()
    try:
        data = mf.get_availability(
            response=Response(),
            level=None,
            confidence_mode="all_candidates",
            item_type=[],
            db=db,
            admin="admin@example.com",
        )
        assert data["available_items"] == 2
        assert data["by_type"] == {"MEANING_CHOICE": 2}
    finally:
        db.close()
