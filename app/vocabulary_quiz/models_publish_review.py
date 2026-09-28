"""1순위(momolib 파일럿 연결) 40콘텐츠 공개검토 판정 - append-only.

기존 vocabulary_review_samples(app/vocabulary_quiz/models.py)와는 다른
목적의 별도 테이블이다: review_samples는 "이 문항이 QA 표본으로서
PASS/REVISE/EXCLUDE인가"를 다루고, 이 테이블은 "momolib에 이미 이식된
1순위 콘텐츠를 실제 공개해도 되는가"를 다룬다 - 서로 다른 화면·다른
질문이라 같은 테이블을 재사용하지 않는다(app/vocabulary_quiz/routers/
review.py와 충돌 없음).

판정 저장은 이 테이블에만 쓴다 - vocabulary_contents/vocabulary_content_
levels의 student_exposure/public_ready/level_status/boundary_flag는
이 코드 경로에서 절대 바꾸지 않는다(운영 승격은 완전히 별도 절차이며,
이 기능 자체를 아직 구현하지 않았다)."""
from __future__ import annotations

from sqlalchemy import CheckConstraint, Column, Integer, Text, text

from app.vocabulary_quiz.db import Base

VERDICT_CHOICES = ("APPROVED_CANDIDATE", "NEEDS_FIX", "HOLD")
VERDICT_LABELS = {
    "APPROVED_CANDIDATE": "승인후보",
    "NEEDS_FIX": "수정필요",
    "HOLD": "보류",
}


class VocabularyPublishReview(Base):
    __tablename__ = "vocabulary_publish_reviews"
    __table_args__ = (
        CheckConstraint(
            "verdict IN ('APPROVED_CANDIDATE', 'NEEDS_FIX', 'HOLD')",
            name="ck_vpr_verdict",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(Text, nullable=False, index=True)
    verdict = Column(Text, nullable=False)
    rationale = Column(Text, nullable=False)
    # users.id는 app/database.py의 별도 SQLite 파일(main.db 등)에 있어
    # 이 DB(vocabulary_quiz_research.db)에서 FK를 걸 수 없다 - 판정
    # 시점의 값을 그대로 스냅샷으로 남긴다(감사 목적, FK 미사용은 의도).
    reviewer_user_id = Column(Text, nullable=False, index=True)
    reviewer_email = Column(Text, nullable=True)
    reviewed_at = Column(Text, nullable=False, server_default=text("(datetime('now'))"))
    content_hash_at_review = Column(Text, nullable=False)
    item_hashes_at_review_json = Column(Text, nullable=False)
    created_at = Column(Text, nullable=True, server_default=text("(datetime('now'))"))
