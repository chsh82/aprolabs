"""L3 중등 보강 콘텐츠·문항 검수 - 배치 단위로 재사용되는 공용 로직.

**사람의 L3 판정과 콘텐츠 검수 상태를 분리**하는 설계: 이 배치들의 모든
건은 전부 사람이 이미 L3로 판정했다(`vocabulary_grade5_candidate_
judgments`, 레벨 판정) - 그 판정을 "문항이 승인됐다"로 확대 해석하지
않는다. 콘텐츠·문항 검수 판정은 `vocabulary_publish_reviews`(app/
vocabulary_quiz/models_publish_review.py, momolib 1순위 공개검토와 같은
테이블·같은 verdict 체계를 재사용)에 **이 화면을 통해서만, 사람이 실제로
버튼을 눌러야** 쓰인다. 검수 전에는 어떤 판정도 미리 채워 넣지 않는다
(기본값 "검수 전").

momolib 1순위 공개검토(app/vocabulary_quiz/publish_review.py)와는 완전히
다른 대상(content_id 집합)을 쓰는 별도 모듈이다.

**배치 레지스트리(2026-10-07 2차 배치 추가 시 도입)**: 라우터·템플릿·
`vocabulary_publish_reviews` 테이블을 배치마다 복제하지 않기 위해, 배치별
차이(source_version·보류 content_id·보류 사유)만 `BATCHES`에 등록하고
나머지 로직은 전부 공용 함수로 공유한다. 새 배치를 추가할 때는 이
딕셔너리에 항목만 추가하면 된다(라우터·템플릿 변경 불필요)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.vocabulary_quiz.models import VocabularyContent, VocabularyMultiformatItem
from app.vocabulary_quiz.models_publish_review import VocabularyPublishReview

CONTENT_HASH_FIELDS = ("lemma", "pos", "canonical_definition", "student_definition", "example_sentence")
ITEM_HASH_FIELDS = ("prompt", "options_json", "correct_option", "explanation")


@dataclass(frozen=True)
class L3BatchConfig:
    batch_id: str
    label: str  # 화면에 보이는 이름, 예: "1차"
    source_version: str
    held_content_ids: frozenset[str] = field(default_factory=frozenset)
    held_reasons: dict[str, str] = field(default_factory=dict)
    # 콘텐츠 자체를 DB에 적재하지 않은 보류 항목(예: 2차의 아멘·파키스탄 - 1차
    # '벨기에'와 달리 애초에 콘텐츠 작성을 하지 않음)의 표시용 lemma. 이 사전에
    # 있는 content_id는 vocabulary_contents에 행이 없어도 목록에 "보류" 행으로
    # 보여준다(상세 페이지 링크는 없음 - 검토할 콘텐츠 자체가 없으므로).
    unloaded_held_lemmas: dict[str, str] = field(default_factory=dict)


BATCHES: dict[str, L3BatchConfig] = {
    "batch1": L3BatchConfig(
        batch_id="batch1",
        label="1차",
        source_version="nikl_grade5_l3_batch1_v1",
        held_content_ids=frozenset({"G5-0fa0e0a975e55d66"}),  # 벨기에
        held_reasons={
            "G5-0fa0e0a975e55d66": (
                "국가명은 '의미' 기준 오답(유의어/행위·결과/부분·전체 등)을 적용하기 어렵고, "
                "다른 나라 특징을 오답으로 쓰려면 검증 안 된 사실을 새로 끌어와야 해 이번 "
                "개정에서는 보류했습니다(수량을 채우려고 억지로 넣지 않음)."
            ),
        },
    ),
    "batch2": L3BatchConfig(
        batch_id="batch2",
        label="2차",
        source_version="nikl_grade5_l3_batch2_v1",
        held_content_ids=frozenset({"G5-b5364a7010af6ca0", "G5-88ade883dcf18ae7"}),  # 아멘, 파키스탄
        unloaded_held_lemmas={
            "G5-b5364a7010af6ca0": "아멘",
            "G5-88ade883dcf18ae7": "파키스탄",
        },
        held_reasons={
            "G5-b5364a7010af6ca0": (
                "특정 종교(기독교) 기도·예배 의례에서 쓰는 용어 - 일반 교육용 어휘 문항으로 "
                "다루기에는 종교적 중립성 문제가 있어 보류했습니다(1차 '벨기에'와 같은 성격의 "
                "편집 판단)."
            ),
            "G5-88ade883dcf18ae7": (
                "국가명 - 1차 '벨기에'와 동일한 사유로 보류. 의미 기준 오답을 적용하기 어렵고, "
                "다른 나라 특징을 오답으로 쓰려면 검증 안 된 사실이 필요해 사실 오류 위험이 "
                "있습니다."
            ),
        },
    ),
}

DEFAULT_BATCH = "batch1"


def get_batch_config(batch_id: str | None) -> L3BatchConfig:
    cfg = BATCHES.get(batch_id or DEFAULT_BATCH)
    if cfg is None:
        raise ValueError(f"알 수 없는 배치: {batch_id}")
    return cfg


def batch_config_for_source_version(source_version: str) -> L3BatchConfig | None:
    for cfg in BATCHES.values():
        if cfg.source_version == source_version:
            return cfg
    return None


def _hash_row(values: dict) -> str:
    blob = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def content_hash(content: VocabularyContent) -> str:
    return _hash_row({f: getattr(content, f) for f in CONTENT_HASH_FIELDS})


def item_hash(item: VocabularyMultiformatItem) -> str:
    return _hash_row({f: getattr(item, f) for f in ITEM_HASH_FIELDS})


def batch_content_ids(db: Session, cfg: L3BatchConfig) -> list[str]:
    rows = (
        db.query(VocabularyContent.content_id)
        .filter(VocabularyContent.source_version == cfg.source_version)
        .distinct()
        .all()
    )
    return sorted({r[0] for r in rows})


def ordered_batch_content_ids(db: Session, cfg: L3BatchConfig) -> list[str]:
    ids = batch_content_ids(db, cfg)
    rows = []
    for cid in ids:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == cid).first()
        rows.append((content.lemma if content else "", cid))
    rows.sort(key=lambda r: r[0])
    return [r[1] for r in rows]


def next_content_id(db: Session, cfg: L3BatchConfig, content_id: str) -> str | None:
    ordered = ordered_batch_content_ids(db, cfg)
    if content_id not in ordered:
        return None
    idx = ordered.index(content_id)
    if idx + 1 < len(ordered):
        return ordered[idx + 1]
    return None


def linked_items(db: Session, cfg: L3BatchConfig, content_id: str) -> list[VocabularyMultiformatItem]:
    """현재 활성(is_active=1) 문항만 - 1차 배치는 2026-10-07 오답 개정으로 기존
    문항이 비활성화되고 새 item_id로 다시 적재된 이력이 있어(원본은 감사 자료로
    DB에 남지만 is_active=0이라 여기서는 안 보임) 이 필터가 꼭 필요하다."""
    return (
        db.query(VocabularyMultiformatItem)
        .filter(VocabularyMultiformatItem.source_content_id == content_id)
        .filter(VocabularyMultiformatItem.source_version == cfg.source_version)
        .filter(VocabularyMultiformatItem.is_active == 1)
        .order_by(VocabularyMultiformatItem.item_id)
        .all()
    )


def inactive_items(db: Session, cfg: L3BatchConfig, content_id: str) -> list[VocabularyMultiformatItem]:
    """비활성화된(is_active=0) 원본 문항 - 감사·before/after 비교 화면용."""
    return (
        db.query(VocabularyMultiformatItem)
        .filter(VocabularyMultiformatItem.source_content_id == content_id)
        .filter(VocabularyMultiformatItem.source_version == cfg.source_version)
        .filter(VocabularyMultiformatItem.is_active == 0)
        .order_by(VocabularyMultiformatItem.item_id)
        .all()
    )


def is_held(cfg: L3BatchConfig, content_id: str) -> bool:
    return content_id in cfg.held_content_ids


def hold_reason(cfg: L3BatchConfig, content_id: str) -> str | None:
    return cfg.held_reasons.get(content_id)


def current_item_hash_pairs(db: Session, cfg: L3BatchConfig, content_id: str) -> list[list[str]]:
    items = linked_items(db, cfg, content_id)
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


def review_is_stale(db: Session, cfg: L3BatchConfig, review: VocabularyPublishReview, content: VocabularyContent) -> bool:
    if review.content_hash_at_review != content_hash(content):
        return True
    try:
        stored_pairs = json.loads(review.item_hashes_at_review_json)
    except (TypeError, ValueError):
        return True
    return current_item_hash_pairs(db, cfg, content.content_id) != stored_pairs


def save_review(db: Session, cfg: L3BatchConfig, content_id: str, verdict: str, rationale: str,
                 reviewer_user_id: str, reviewer_email: str | None) -> VocabularyPublishReview:
    content = db.query(VocabularyContent).filter(VocabularyContent.content_id == content_id).first()
    review = VocabularyPublishReview(
        content_id=content_id,
        verdict=verdict,
        rationale=rationale,
        reviewer_user_id=reviewer_user_id,
        reviewer_email=reviewer_email,
        content_hash_at_review=content_hash(content),
        item_hashes_at_review_json=json.dumps(current_item_hash_pairs(db, cfg, content_id), ensure_ascii=False),
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


def risk_review_info(db: Session, cfg: L3BatchConfig, content_id: str) -> dict | None:
    """위험 기반 검수(2026-10-07 도입) - 생성과 분리된 AI 재검토 + 고정시드
    표본 검토 결과를 `vocabulary_multiformat_items.qa_flags_json`의
    `risk_review` 키에서 읽어온다. **이 정보는 사람 승인이 아니다** -
    `vocabulary_publish_reviews`와는 완전히 분리된 참고 정보일 뿐이고,
    검수하지 않은 문항에 승인 판정을 자동으로 채워 넣지 않는다(이 함수는
    읽기만 하며, latest_review()/save_review()가 다루는 사람 판정 테이블은
    전혀 건드리지 않는다).

    표본(sampled)은 콘텐츠당 두 유형(MEANING_CHOICE/CONTEXT_MEANING) 중
    **한쪽에만** 걸릴 수 있다(고정 시드로 유형까지 배정하므로) - 그래서
    item_id 정렬 순서상 먼저 나온 항목 하나만 보고 판단하면 안 되고, 활성
    문항 전체를 합쳐서(OR) "이 콘텐츠가 표본에 포함됐는가"를 판단해야
    한다. risk_category/risk_reason은 두 유형이 같은 오답 집합을 공유해
    항상 동일하므로 아무 항목에서나 가져와도 된다."""
    merged: dict | None = None
    for item in linked_items(db, cfg, content_id):
        if not item.qa_flags_json:
            continue
        flags_list = json.loads(item.qa_flags_json)
        flags = flags_list[0] if flags_list else {}
        rr = flags.get("risk_review")
        if not rr:
            continue
        if merged is None:
            merged = dict(rr)
        if rr.get("sampled"):
            merged["sampled"] = True
            merged["sample_seed"] = rr.get("sample_seed")
            merged["sample_verdict"] = rr.get("sample_verdict")
    return merged
