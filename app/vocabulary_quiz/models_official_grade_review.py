"""tier1(31건) 국립국어원 공식 등급 빠른 검수 - 판정 모델.

vocabulary_official_grade_reference(scripts/vocab/apply_official_grade_
reference.py로 이미 적재된 순수 참조 테이블, 5,950건)를 읽기 전용으로
매핑하는 VocabularyOfficialGradeReference와, 그 위에 사람 판정만
append-only로 쌓는 VocabularyOfficialGradeJudgment 두 모델을 담는다.

vocabulary_publish_reviews(app/vocabulary_quiz/models_publish_review.py)와
완전히 같은 설계 원칙: 판정은 이 테이블에만 쓴다 - vocabulary_contents/
vocabulary_content_levels의 vocab_level/level_status/boundary_flag/
student_exposure/public_ready, 문항, 매니페스트는 이 코드 경로 어디에서도
절대 바꾸지 않는다. 사람 판정을 자동으로 채우는 코드는 이 파일에도,
서비스 계층에도, 라우터에도 없다 - 폼 제출이 있어야만 행이 생긴다."""
from __future__ import annotations

from sqlalchemy import CheckConstraint, Column, Integer, Text, text

from app.vocabulary_quiz.db import Base

JUDGMENT_CHOICES = ("기본 레벨 조정", "현재 유지", "뜻 확인", "보류")


class VocabularyOfficialGradeReference(Base):
    """읽기 전용 매핑 - 이 모델을 통한 쓰기는 이 앱 어디에도 없다(적재는
    scripts/vocab/apply_official_grade_reference.py가 SSH 원샷 스크립트로
    수행했고, 이 서빙 코드는 그 결과를 조회만 한다)."""
    __tablename__ = "vocabulary_official_grade_reference"

    content_id = Column(Text, primary_key=True)
    official_grade = Column(Text, nullable=True)
    proposed_base_level = Column(Text, nullable=True)
    proposed_base_level_note = Column(Text, nullable=True)
    match_type = Column(Text, nullable=False)
    standard_homonym_number = Column(Integer, nullable=True)
    exception_reason = Column(Text, nullable=True)
    exception_reason_secondary = Column(Text, nullable=True)
    priority_tier = Column(Integer, nullable=True)
    review_status = Column(Text, nullable=False)
    human_approval_status = Column(Text, nullable=True)
    current_vocab_level_snapshot = Column(Integer, nullable=True)
    current_level_status_snapshot = Column(Text, nullable=True)
    level_source = Column(Text, nullable=True)
    source_report_seq = Column(Integer, nullable=False)
    source_file_sha256 = Column(Text, nullable=False)
    source_version_label = Column(Text, nullable=False)
    computed_at = Column(Text, nullable=False)
    computed_by_script = Column(Text, nullable=False)


class VocabularyOfficialGradeJudgment(Base):
    __tablename__ = "vocabulary_official_grade_judgments"
    __table_args__ = (
        CheckConstraint(
            "judgment IN ('기본 레벨 조정', '현재 유지', '뜻 확인', '보류')",
            name="ck_vogj_judgment",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(Text, nullable=False, index=True)
    judgment = Column(Text, nullable=False)
    rationale = Column(Text, nullable=True)
    # users.id는 별도 DB 파일에 있어 FK를 걸 수 없다 - 판정 시점 값을
    # 스냅샷으로 남긴다(vocabulary_publish_reviews와 동일 이유).
    reviewer_user_id = Column(Text, nullable=False, index=True)
    reviewer_email = Column(Text, nullable=True)
    reviewed_at = Column(Text, nullable=False, server_default=text("(datetime('now'))"))
    # 판정 당시 참조 테이블 행의 official_grade+proposed_base_level+
    # computed_at+source_file_sha256을 스냅샷(해시)으로 남긴다 - 참조
    # 테이블이 나중에 재계산되면(예: 새 공식 자료 버전 반영) 이 값과
    # 달라져 판정이 자동으로 "만료" 표시된다.
    source_data_version_at_review = Column(Text, nullable=False)
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
