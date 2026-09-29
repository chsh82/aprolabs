"""tier1(31건) 국립국어원 공식 등급 빠른 검수 - 순수 로직(라우트 없음).

tier1 정의: vocabulary_official_grade_reference.priority_tier == 1
(data/import/nikl_exceptions_priority_tiers_20260929.csv에서 계산해
적재한 값 그대로 - 이 코드는 우선순위를 다시 계산하지 않고 이미 적재된
값만 읽는다). data/import/nikl_tier1_quick_review_20260929.csv의 31건과
같은 집합이어야 한다(라우터/서비스 계층에서 매번 재계산하지 않고
DB의 priority_tier 컬럼을 그대로 신뢰 - 그 컬럼 자체가 이미 검증된
산출물이기 때문)."""
from __future__ import annotations

import hashlib
import json

from sqlalchemy.orm import Session

from app.vocabulary_quiz.models import VocabularyContent, VocabularyContentLevel
from app.vocabulary_quiz.models_official_grade_review import (
    VocabularyOfficialGradeJudgment,
    VocabularyOfficialGradeReference,
)

GRADE_TO_LEVEL_LABEL = {
    "1": "L0(기초)", "2": "L0", "3": "L1", "4": "L2",
}


def tier1_reference_rows(db: Session) -> list[VocabularyOfficialGradeReference]:
    return (
        db.query(VocabularyOfficialGradeReference)
        .filter(VocabularyOfficialGradeReference.priority_tier == 1)
        .all()
    )


def ordered_tier1_content_ids(db: Session) -> list[str]:
    """목록 화면과 완전히 같은 순서(현재 레벨 → 표제어 가나다순)."""
    rows = tier1_reference_rows(db)
    keyed = []
    for r in rows:
        content = db.query(VocabularyContent).filter(VocabularyContent.content_id == r.content_id).first()
        keyed.append((r.current_vocab_level_snapshot if r.current_vocab_level_snapshot is not None else 99,
                       content.lemma if content else "", r.content_id))
    keyed.sort(key=lambda t: (t[0], t[1]))
    return [k[2] for k in keyed]


def next_content_id(db: Session, content_id: str) -> str | None:
    ordered = ordered_tier1_content_ids(db)
    if content_id not in ordered:
        return None
    idx = ordered.index(content_id)
    if idx + 1 < len(ordered):
        return ordered[idx + 1]
    return None


def short_example(content: VocabularyContent | None, limit: int = 60) -> str:
    if content is None:
        return "-"
    text = content.student_definition or content.canonical_definition or ""
    text = text.split("\n")[0].strip()
    if len(text) > limit:
        return text[:limit] + "…"
    return text or "-"


def diff_reason(ref: VocabularyOfficialGradeReference) -> str:
    grade_label = GRADE_TO_LEVEL_LABEL.get(ref.official_grade or "", ref.proposed_base_level or "미확정")
    current = f"L{ref.current_vocab_level_snapshot}" if ref.current_vocab_level_snapshot is not None else "?"
    parts = [f"공식 {ref.official_grade or '-'}등급(→{grade_label}) vs 현재 {current}"]
    if ref.exception_reason:
        parts.append(ref.exception_reason)
    return " · ".join(parts)


def _reference_version_snapshot(ref: VocabularyOfficialGradeReference) -> str:
    """참조 테이블 행이 재계산되면(예: 새 공식 자료 버전 반영, 등급/제안레벨
    변경) 이 값이 달라져 기존 판정이 만료 표시된다."""
    blob = json.dumps({
        "official_grade": ref.official_grade,
        "proposed_base_level": ref.proposed_base_level,
        "match_type": ref.match_type,
        "source_file_sha256": ref.source_file_sha256,
        "computed_at": ref.computed_at,
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def latest_judgment(db: Session, content_id: str) -> VocabularyOfficialGradeJudgment | None:
    return (
        db.query(VocabularyOfficialGradeJudgment)
        .filter(VocabularyOfficialGradeJudgment.content_id == content_id)
        .order_by(VocabularyOfficialGradeJudgment.reviewed_at.desc(), VocabularyOfficialGradeJudgment.id.desc())
        .first()
    )


def judgment_history(db: Session, content_id: str) -> list[VocabularyOfficialGradeJudgment]:
    return (
        db.query(VocabularyOfficialGradeJudgment)
        .filter(VocabularyOfficialGradeJudgment.content_id == content_id)
        .order_by(VocabularyOfficialGradeJudgment.reviewed_at.desc(), VocabularyOfficialGradeJudgment.id.desc())
        .all()
    )


def judgment_is_stale(judgment: VocabularyOfficialGradeJudgment, ref: VocabularyOfficialGradeReference) -> bool:
    if ref is None:
        return True
    return judgment.source_data_version_at_review != _reference_version_snapshot(ref)


def save_judgment(db: Session, content_id: str, judgment_value: str, rationale: str,
                   reviewer_user_id: str, reviewer_email: str | None) -> VocabularyOfficialGradeJudgment:
    ref = (
        db.query(VocabularyOfficialGradeReference)
        .filter(VocabularyOfficialGradeReference.content_id == content_id)
        .first()
    )
    if ref is None:
        raise ValueError(f"참조 데이터 없음: {content_id}")
    row = VocabularyOfficialGradeJudgment(
        content_id=content_id,
        judgment=judgment_value,
        rationale=rationale or None,
        reviewer_user_id=reviewer_user_id,
        reviewer_email=reviewer_email,
        source_data_version_at_review=_reference_version_snapshot(ref),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def content_level(db: Session, content_id: str) -> VocabularyContentLevel | None:
    return db.query(VocabularyContentLevel).filter(VocabularyContentLevel.content_id == content_id).first()
