"""1순위 40콘텐츠 공개검토 - 순수 로직(라우트 없음). 라우터에서만 import.

1순위 정의: vocabulary_multiformat_items 중 momolib에 실제로 이식된
파일럿 배치(source_version IN (schema_reading_l4l5_pilot_dryrun_v1,
schema_reading_l6_pilot_dryrun_v1))가 참조하는 고유 source_content_id
전체(40건, momolib 쪽 1순위 정의와 동일한 기준)."""
from __future__ import annotations

import hashlib
import json

from sqlalchemy.orm import Session

from app.vocabulary_quiz.models import VocabularyContent, VocabularyContentLevel, VocabularyMultiformatItem
from app.vocabulary_quiz.models_publish_review import VocabularyPublishReview

TIER1_SOURCE_VERSIONS = ("schema_reading_l4l5_pilot_dryrun_v1", "schema_reading_l6_pilot_dryrun_v1")

CONTENT_HASH_FIELDS = ("lemma", "pos", "canonical_definition", "student_definition",
                       "example_sentence", "example_target_form")
ITEM_HASH_FIELDS = ("prompt", "options_json", "correct_option", "explanation")


def _hash_row(values: dict) -> str:
    blob = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def content_hash(content: VocabularyContent) -> str:
    return _hash_row({f: getattr(content, f) for f in CONTENT_HASH_FIELDS})


def item_hash(item: VocabularyMultiformatItem) -> str:
    return _hash_row({f: getattr(item, f) for f in ITEM_HASH_FIELDS})


def tier1_content_ids(db: Session) -> list[str]:
    rows = (
        db.query(VocabularyMultiformatItem.source_content_id)
        .filter(VocabularyMultiformatItem.source_version.in_(TIER1_SOURCE_VERSIONS))
        .filter(VocabularyMultiformatItem.source_content_id.isnot(None))
        .distinct()
        .all()
    )
    return sorted({r[0] for r in rows})


def linked_items(db: Session, content_id: str) -> list[VocabularyMultiformatItem]:
    direct = (
        db.query(VocabularyMultiformatItem)
        .filter(VocabularyMultiformatItem.source_content_id == content_id)
        .filter(VocabularyMultiformatItem.source_version.in_(TIER1_SOURCE_VERSIONS))
        .order_by(VocabularyMultiformatItem.item_id)
        .all()
    )
    return direct


def current_item_hash_pairs(db: Session, content_id: str) -> list[list[str]]:
    items = linked_items(db, content_id)
    return sorted([[it.item_id, item_hash(it)] for it in items])


def latest_review(db: Session, content_id: str) -> VocabularyPublishReview | None:
    return (
        db.query(VocabularyPublishReview)
        .filter(VocabularyPublishReview.content_id == content_id)
        .order_by(VocabularyPublishReview.reviewed_at.desc(), VocabularyPublishReview.id.desc())
        .first()
    )


def review_history(db: Session, content_id: str) -> list[VocabularyPublishReview]:
    return (
        db.query(VocabularyPublishReview)
        .filter(VocabularyPublishReview.content_id == content_id)
        .order_by(VocabularyPublishReview.reviewed_at.desc(), VocabularyPublishReview.id.desc())
        .all()
    )


def review_is_stale(db: Session, review: VocabularyPublishReview, content: VocabularyContent) -> bool:
    if review.content_hash_at_review != content_hash(content):
        return True
    try:
        stored_pairs = json.loads(review.item_hashes_at_review_json)
    except (TypeError, ValueError):
        return True
    return current_item_hash_pairs(db, content.content_id) != stored_pairs


def save_review(db: Session, content_id: str, verdict: str, rationale: str,
                 reviewer_user_id: str, reviewer_email: str | None) -> VocabularyPublishReview:
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == content_id).first()
    review = VocabularyPublishReview(
        content_id=content_id,
        verdict=verdict,
        rationale=rationale,
        reviewer_user_id=reviewer_user_id,
        reviewer_email=reviewer_email,
        content_hash_at_review=content_hash(content),
        item_hashes_at_review_json=json.dumps(current_item_hash_pairs(db, content_id), ensure_ascii=False),
    )
    db.add(review)
    db.commit()
    db.refresh(review)
    return review


def content_cautions(content: VocabularyContent, level: VocabularyContentLevel | None) -> list[str]:
    """자동으로 판단 가능한 주의 사항만 나열한다(추측성 문구 없음)."""
    notes: list[str] = []
    if level is None:
        notes.append("연결된 레벨 정보가 없습니다.")
    elif level.level_status == "REVIEW_BOUNDARY" or level.boundary_flag:
        notes.append("학년 경계 미확정 상태(REVIEW_BOUNDARY) - 학년 재검토 대상입니다.")
    if content.hold_reason and content.hold_reason.strip():
        notes.append(f"HOLD 사유 있음: {content.hold_reason}")
    if not content.is_active:
        notes.append("콘텐츠 is_active=False 상태입니다.")
    # 이미 momolib으로 이식된 뒤 momolib 쪽에서만 수정된 것으로 확인된 알려진
    # 차이 - aprolabs 연구 DB 쪽 데이터는 이 코드가 자동으로 고치지 않는다.
    if content.content_id == "SR_L4CORE_4812":
        notes.append(
            "참고: momolib에 이식된 뒤 momolib 쪽에서만 조사(은/는·이라는/라는) 오류가 "
            "수정된 이력이 있습니다(이 화면이 보여주는 이 문항의 설명 텍스트는 아직 "
            "수정 전 원본 상태 - 2026-09-28 확인, 이 코드가 자동으로 동기화하지 않음)."
        )
    return notes
