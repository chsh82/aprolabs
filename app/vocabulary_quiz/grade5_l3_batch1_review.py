"""L3 중등 보강 1차(2026-10-07) 콘텐츠·문항 검수 - 순수 로직(라우트 없음).

**사람의 L3 판정과 콘텐츠 검수 상태를 분리**하는 설계: 이 배치의 30건은
전부 사람이 이미 L3로 판정했다(`vocabulary_grade5_candidate_judgments`,
레벨 판정) - 그 판정을 "문항이 승인됐다"로 확대 해석하지 않는다. 콘텐츠·
문항 검수 판정은 `vocabulary_publish_reviews`(app/vocabulary_quiz/
models_publish_review.py, momolib 1순위 공개검토와 같은 테이블·같은
verdict 체계를 재사용 - 새 테이블을 만들지 않음)에 **이 화면을 통해서만,
사람이 실제로 버튼을 눌러야** 쓰인다. 검수 전에는 어떤 판정도 미리
채워 넣지 않는다(기본값 "검수 전").

momolib 1순위 공개검토(app/vocabulary_quiz/publish_review.py)와는 완전히
다른 대상(content_id 집합)을 쓰는 별도 모듈이다 - 그 파일의
TIER1_SOURCE_VERSIONS(momolib 이식 완료 콘텐츠)을 건드리지 않는다(momolib
무관 요구사항)."""
from __future__ import annotations

import hashlib
import json

from sqlalchemy.orm import Session

from app.vocabulary_quiz.models import VocabularyContent, VocabularyMultiformatItem
from app.vocabulary_quiz.models_publish_review import VocabularyPublishReview

SOURCE_VERSION = "nikl_grade5_l3_batch1_v1"

CONTENT_HASH_FIELDS = ("lemma", "pos", "canonical_definition", "student_definition", "example_sentence")
ITEM_HASH_FIELDS = ("prompt", "options_json", "correct_option", "explanation")


def _hash_row(values: dict) -> str:
    blob = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def content_hash(content: VocabularyContent) -> str:
    return _hash_row({f: getattr(content, f) for f in CONTENT_HASH_FIELDS})


def item_hash(item: VocabularyMultiformatItem) -> str:
    return _hash_row({f: getattr(item, f) for f in ITEM_HASH_FIELDS})


def batch1_content_ids(db: Session) -> list[str]:
    rows = (
        db.query(VocabularyContent.content_id)
        .filter(VocabularyContent.source_version == SOURCE_VERSION)
        .distinct()
        .all()
    )
    return sorted({r[0] for r in rows})


def ordered_batch1_content_ids(db: Session) -> list[str]:
    ids = batch1_content_ids(db)
    rows = []
    for cid in ids:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == cid).first()
        rows.append((content.lemma if content else "", cid))
    rows.sort(key=lambda r: r[0])
    return [r[1] for r in rows]


def next_content_id(db: Session, content_id: str) -> str | None:
    ordered = ordered_batch1_content_ids(db)
    if content_id not in ordered:
        return None
    idx = ordered.index(content_id)
    if idx + 1 < len(ordered):
        return ordered[idx + 1]
    return None


def linked_items(db: Session, content_id: str) -> list[VocabularyMultiformatItem]:
    """현재 활성(is_active=1) 문항만 - 2026-10-07 오답 개정으로 29단어는
    기존 문항이 비활성화되고 새 item_id로 다시 적재됐다(원본은 감사
    자료로 DB에 그대로 남아 있지만 is_active=0이라 여기서는 안 보인다).
    이 필터가 없으면 개정된 콘텐츠의 신선도 해시가 "구+신 문항 2배"로
    잘못 계산된다."""
    return (
        db.query(VocabularyMultiformatItem)
        .filter(VocabularyMultiformatItem.source_content_id == content_id)
        .filter(VocabularyMultiformatItem.source_version == SOURCE_VERSION)
        .filter(VocabularyMultiformatItem.is_active == 1)
        .order_by(VocabularyMultiformatItem.item_id)
        .all()
    )


# 2026-10-07 오답 개정에서 제외·보류한 content_id - scripts/vocab/
# grade5_batch1_distractors_v2.py의 HOLD와 같은 값이다(app 코드가 scripts/
# 디렉터리를 import하면 배포 경로에 따라 깨지기 쉬워, 적용 결과만 여기
# 상수로 그대로 옮겨 적었다 - 두 곳의 값이 어긋나면 안 되므로 바꿀 때
# 반드시 같이 바꿀 것).
HELD_CONTENT_IDS: frozenset[str] = frozenset({
    "G5-0fa0e0a975e55d66",  # 벨기에 - 국가명이라 의미 기준 오답 설계가 어려워 보류
})


def is_revised(content_id: str) -> bool:
    """이 content_id가 2026-10-07 오답 개정 대상(29건)인지."""
    return content_id not in HELD_CONTENT_IDS


def inactive_items(db: Session, content_id: str) -> list[VocabularyMultiformatItem]:
    """비활성화된(is_active=0) 원본 문항 - 감사·before/after 비교 화면용.
    2026-10-07 오답 개정으로 비활성화된 29단어의 원본 58문항이 여기 해당."""
    return (
        db.query(VocabularyMultiformatItem)
        .filter(VocabularyMultiformatItem.source_content_id == content_id)
        .filter(VocabularyMultiformatItem.source_version == SOURCE_VERSION)
        .filter(VocabularyMultiformatItem.is_active == 0)
        .order_by(VocabularyMultiformatItem.item_id)
        .all()
    )


def hold_reason(content_id: str) -> str | None:
    if content_id not in HELD_CONTENT_IDS:
        return None
    return (
        "국가명은 '의미' 기준 오답(유의어/행위·결과/부분·전체 등)을 적용하기 어렵고, "
        "다른 나라 특징을 오답으로 쓰려면 검증 안 된 사실을 새로 끌어와야 해 이번 "
        "개정에서는 보류했습니다(수량을 채우려고 억지로 넣지 않음)."
    )


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


def human_level_note(content_id: str) -> str:
    """사람의 L3 판정 출처를 명시 - 이 화면의 콘텐츠 검수 판정과 절대
    섞이지 않는다는 걸 화면에서도 매번 다시 보여준다."""
    return (
        "사람이 L3로 직접 판정(vocabulary_grade5_candidate_judgments, "
        f"candidate_id={content_id}) - 이 레벨 판정은 아래 콘텐츠·문항 검수와는 "
        "별개입니다. 레벨 판정이 문항 승인을 의미하지 않습니다."
    )
