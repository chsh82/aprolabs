"""공식 5등급(중1~3 경계 신호) 신규 어휘 확장 1차 배치(100건) 검수 모델.

vocabulary_grade5_candidate_batch(scripts/vocab/apply_grade5_candidate_
batch.py로 적재한 순수 스테이징 테이블, PK=candidate_id - content_id가
아님, 아직 vocabulary_contents에 존재하지 않는 신규 어휘라서)와, 그 위에
사람 판정만 append-only로 쌓는 VocabularyGrade5CandidateJudgment 두
모델을 담는다.

tier1(models_official_grade_review.py)과 같은 설계 원칙: 판정은 이
테이블에만 쓴다 - vocabulary_contents/vocabulary_content_levels의
vocab_level/level_status/boundary_flag/student_exposure/public_ready,
문항, 매니페스트는 이 코드 경로 어디에서도 절대 바꾸지 않는다. 사람
판정을 자동으로 채우는 코드는 이 파일에도, 서비스 계층에도, 라우터에도
없다 - 폼 제출이 있어야만 행이 생긴다.

tier1과 다른 점: tier1은 이미 존재하는 content_id를 검수하지만, 이
배치는 DB에 아직 없는 "신규 후보"를 검수한다 - 그래서 판정 선택지도
다르다(L3/L4/경계 유지/제외, "기본 레벨 조정/현재 유지"가 아님)."""
from __future__ import annotations

from sqlalchemy import CheckConstraint, Column, Integer, Text, text

from app.vocabulary_quiz.db import Base

JUDGMENT_CHOICES = ("L3", "L4", "경계 유지", "제외")
JUDGMENT_LABELS = {
    "L3": "L3(중1~2)", "L4": "L4(중3)", "경계 유지": "경계 유지", "제외": "제외",
}


class VocabularyGrade5CandidateBatch(Base):
    """읽기 전용 스테이징 - 이 모델을 통한 쓰기는 이 앱 어디에도 없다(적재는
    scripts/vocab/apply_grade5_candidate_batch.py가 SSH 원샷 스크립트로
    수행했고, 이 서빙 코드는 그 결과를 조회만 한다). '제외' 판정이 들어와도
    이 테이블 행은 절대 삭제·수정하지 않는다(원천 삭제 아님 - 사용자 지시)."""
    __tablename__ = "vocabulary_grade5_candidate_batch"

    candidate_id = Column(Text, primary_key=True)
    batch_no = Column(Integer, nullable=False)
    lemma = Column(Text, nullable=False)
    pos = Column(Text, nullable=False)
    homonym_number = Column(Integer, nullable=True)
    official_meaning_short = Column(Text, nullable=True)
    specialized_domain_flag = Column(Integer, nullable=False)
    polysemy_risk_flag = Column(Integer, nullable=False)
    proper_noun_risk_flag = Column(Integer, nullable=False)
    selection_reason = Column(Text, nullable=True)
    official_grade = Column(Text, nullable=False)
    proposed_level_note = Column(Text, nullable=False)
    source_report_seq = Column(Integer, nullable=False)
    source_file_sha256 = Column(Text, nullable=False)
    computed_at = Column(Text, nullable=False)
    computed_by_script = Column(Text, nullable=False)


class VocabularyGrade5CandidateJudgment(Base):
    __tablename__ = "vocabulary_grade5_candidate_judgments"
    __table_args__ = (
        CheckConstraint(
            "judgment IN ('L3', 'L4', '경계 유지', '제외')",
            name="ck_vg5cj_judgment",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    candidate_id = Column(Text, nullable=False, index=True)
    judgment = Column(Text, nullable=False)
    rationale = Column(Text, nullable=True)
    # users.id는 별도 DB 파일에 있어 FK를 걸 수 없다 - 판정 시점 값을
    # 스냅샷으로 남긴다(vocabulary_official_grade_judgments와 동일 이유).
    reviewer_user_id = Column(Text, nullable=False, index=True)
    reviewer_email = Column(Text, nullable=True)
    reviewed_at = Column(Text, nullable=False, server_default=text("(datetime('now'))"))
    # 판정 당시 배치 행의 전체 필드 스냅샷(해시) - 그 배치 행이 나중에
    # 재계산되면(이번 배치 수명 동안은 일어나지 않지만, 다음 배치부터의
    # 일관성을 위해 메커니즘은 동일하게 둔다) 이 값과 달라져 판정이
    # 자동으로 "만료" 표시된다.
    source_data_version_at_review = Column(Text, nullable=False)
    # 이중 클릭/중복 요청 방지 - 같은 (candidate_id, submission_token) 쌍은
    # DB 유니크 인덱스(uq_grade5_candidate_judgments_token)가 두 번째
    # INSERT를 IntegrityError로 막는다. 상세 페이지를 새로 열 때마다
    # 새 토큰이 발급되므로, 의도적 재판정은 다른 토큰으로 정상 저장된다.
    submission_token = Column(Text, nullable=True)
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))


class VocabularyGrade5CandidateModelPrediction(Base):
    """Gemini(또는 추후 다른 모델)의 레벨 제안 - 사람 판정
    (VocabularyGrade5CandidateJudgment)과 완전히 분리된 별도 테이블이다.
    이 테이블은 review 화면 어디에서도 렌더링하지 않는다(사람에게 자동
    제안·근거를 보여주지 않는다는 사용자 지시) - 라우터/서비스 계층
    어디에도 이 모델을 읽어 템플릿에 넘기는 코드가 없다(비교·분석
    스크립트에서만 직접 조회)."""
    __tablename__ = "vocabulary_grade5_candidate_model_predictions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    candidate_id = Column(Text, nullable=False, index=True)
    model_name = Column(Text, nullable=False)
    model_version = Column(Text, nullable=False)
    prompt_template_hash = Column(Text, nullable=False)
    predicted_judgment = Column(Text, nullable=True)  # L3/L4/경계 유지/검토 필요, 오류 시 NULL
    predicted_reason = Column(Text, nullable=True)
    grounded = Column(Integer, nullable=True)
    borderline = Column(Integer, nullable=True)
    api_error = Column(Integer, nullable=False, server_default=text("0"))
    error_detail = Column(Text, nullable=True)
    computed_at = Column(Text, nullable=False)
