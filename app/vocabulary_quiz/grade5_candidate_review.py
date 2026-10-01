"""공식 5등급 신규 어휘 1차 배치(100건) 검수 - 순수 로직(라우트 없음).

배치 정의: vocabulary_grade5_candidate_batch.batch_no == 1(이번 100건).
이 코드는 후보를 재선정하지 않고 이미 적재된 행만 읽는다 - 선정 로직은
scripts/vocab/nikl_grade5_candidate_list.py, 적재는 scripts/vocab/
apply_grade5_candidate_batch.py가 전담."""
from __future__ import annotations

import hashlib
import json
import secrets

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.vocabulary_quiz.models_grade5_candidate_review import (
    VocabularyGrade5CandidateBatch,
    VocabularyGrade5CandidateJudgment,
)

DEFAULT_BATCH_NO = 1


def available_batch_numbers(db: Session) -> list[int]:
    """실제 적재된 batch_no 목록(오름차순) - 목록 화면의 배치 선택기용."""
    rows = db.query(VocabularyGrade5CandidateBatch.batch_no).distinct().order_by(
        VocabularyGrade5CandidateBatch.batch_no
    ).all()
    return [r[0] for r in rows]


def ordered_batch_rows(db: Session, batch_no: int = DEFAULT_BATCH_NO) -> list[VocabularyGrade5CandidateBatch]:
    """표제어 가나다순 - 재현 가능하고 세션 간 일관된 순서."""
    return (
        db.query(VocabularyGrade5CandidateBatch)
        .filter(VocabularyGrade5CandidateBatch.batch_no == batch_no)
        .order_by(VocabularyGrade5CandidateBatch.lemma, VocabularyGrade5CandidateBatch.candidate_id)
        .all()
    )


def ordered_candidate_ids(db: Session, batch_no: int = DEFAULT_BATCH_NO) -> list[str]:
    return [r.candidate_id for r in ordered_batch_rows(db, batch_no)]


def get_candidate(db: Session, candidate_id: str) -> VocabularyGrade5CandidateBatch | None:
    return (
        db.query(VocabularyGrade5CandidateBatch)
        .filter(VocabularyGrade5CandidateBatch.candidate_id == candidate_id)
        .first()
    )


def latest_judgment(db: Session, candidate_id: str) -> VocabularyGrade5CandidateJudgment | None:
    return (
        db.query(VocabularyGrade5CandidateJudgment)
        .filter(VocabularyGrade5CandidateJudgment.candidate_id == candidate_id)
        .order_by(VocabularyGrade5CandidateJudgment.reviewed_at.desc(), VocabularyGrade5CandidateJudgment.id.desc())
        .first()
    )


def judgment_history(db: Session, candidate_id: str) -> list[VocabularyGrade5CandidateJudgment]:
    return (
        db.query(VocabularyGrade5CandidateJudgment)
        .filter(VocabularyGrade5CandidateJudgment.candidate_id == candidate_id)
        .order_by(VocabularyGrade5CandidateJudgment.reviewed_at.desc(), VocabularyGrade5CandidateJudgment.id.desc())
        .all()
    )


def latest_judgments_map(db: Session, batch_no: int = DEFAULT_BATCH_NO) -> dict[str, VocabularyGrade5CandidateJudgment]:
    """후보별 최신 판정 1개만 - 집계·진행률은 반드시 이 함수를 통해서만
    계산한다(판정 이력 전체 행 수를 세면 안 됨)."""
    ids = ordered_candidate_ids(db, batch_no)
    out: dict[str, VocabularyGrade5CandidateJudgment] = {}
    for cid in ids:
        j = latest_judgment(db, cid)
        if j is not None:
            out[cid] = j
    return out


def progress_stats(db: Session, batch_no: int = DEFAULT_BATCH_NO) -> dict:
    ids = ordered_candidate_ids(db, batch_no)
    latest_map = latest_judgments_map(db, batch_no)
    counts = {"L3": 0, "L4": 0, "경계 유지": 0, "제외": 0}
    for j in latest_map.values():
        counts[j.judgment] = counts.get(j.judgment, 0) + 1
    judged = len(latest_map)
    total = len(ids)
    return {
        "total": total, "judged": judged, "unjudged": total - judged,
        "counts": counts,
    }


def unjudged_candidate_ids(db: Session, batch_no: int = DEFAULT_BATCH_NO) -> list[str]:
    ids = ordered_candidate_ids(db, batch_no)
    latest_map = latest_judgments_map(db, batch_no)
    return [cid for cid in ids if cid not in latest_map]


def next_unjudged_candidate_id(db: Session, current_candidate_id: str,
                                batch_no: int = DEFAULT_BATCH_NO) -> str | None:
    """현재 항목 다음부터 '미검수'인 첫 후보를 찾는다(이미 검수된 항목은
    건너뛴다) - tier1의 단순 '다음 순번'과 다른 점."""
    ids = ordered_candidate_ids(db, batch_no)
    if current_candidate_id not in ids:
        return None
    latest_map = latest_judgments_map(db, batch_no)
    idx = ids.index(current_candidate_id)
    for cid in ids[idx + 1:]:
        if cid not in latest_map:
            return cid
    # 뒤쪽에 미검수가 없으면 앞쪽(목록 맨 앞부터 현재 항목 전까지)도 훑는다 -
    # 중간에 건너뛴 미검수 항목을 나중에 채우는 흐름을 지원.
    for cid in ids[:idx]:
        if cid not in latest_map:
            return cid
    return None


def new_submission_token() -> str:
    return secrets.token_urlsafe(16)


def _batch_version_snapshot(row: VocabularyGrade5CandidateBatch) -> str:
    """배치 행이 재계산되면(예: 다음 배치 재적재 시 값이 바뀌는 경우) 이
    값이 달라져 기존 판정이 만료 표시된다 - tier1과 동일한 패턴."""
    blob = json.dumps({
        "lemma": row.lemma, "pos": row.pos, "homonym_number": row.homonym_number,
        "official_grade": row.official_grade, "proposed_level_note": row.proposed_level_note,
        "source_file_sha256": row.source_file_sha256, "computed_at": row.computed_at,
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def judgment_is_stale(judgment: VocabularyGrade5CandidateJudgment,
                       row: VocabularyGrade5CandidateBatch) -> bool:
    if row is None:
        return True
    return judgment.source_data_version_at_review != _batch_version_snapshot(row)


def save_judgment(db: Session, candidate_id: str, judgment_value: str, rationale: str,
                   reviewer_user_id: str, reviewer_email: str | None,
                   submission_token: str) -> tuple[VocabularyGrade5CandidateJudgment, bool]:
    """판정을 append-only로 저장한다. 반환값은 (행, duplicate_detected).

    이중 클릭/중복 요청 방지: vocabulary_grade5_candidate_judgments에는
    (candidate_id, submission_token) 부분 유니크 인덱스가 걸려 있다
    (submission_token IS NOT NULL일 때만 - 마이그레이션 참고). 같은 폼
    렌더링에서 나온 같은 토큰으로 두 번째 요청이 들어오면(더블클릭,
    브라우저 재전송 등) DB가 IntegrityError를 던지고, 이 함수는 그걸
    잡아 새 행을 만들지 않은 채 '이미 저장된 바로 그 행'을 돌려준다
    (duplicate_detected=True). 상세 페이지를 다시 열면 새 토큰이
    발급되므로, 의도적인 재판정은 같은 judgment 값이라도 정상적으로
    새 이력 행이 된다(duplicate_detected=False)."""
    row = get_candidate(db, candidate_id)
    if row is None:
        raise ValueError(f"후보 데이터 없음: {candidate_id}")

    j = VocabularyGrade5CandidateJudgment(
        candidate_id=candidate_id,
        judgment=judgment_value,
        rationale=rationale or None,
        reviewer_user_id=reviewer_user_id,
        reviewer_email=reviewer_email,
        source_data_version_at_review=_batch_version_snapshot(row),
        submission_token=submission_token,
    )
    db.add(j)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = (
            db.query(VocabularyGrade5CandidateJudgment)
            .filter(
                VocabularyGrade5CandidateJudgment.candidate_id == candidate_id,
                VocabularyGrade5CandidateJudgment.submission_token == submission_token,
            )
            .first()
        )
        if existing is None:
            raise
        return existing, True
    db.refresh(j)
    return j, False
