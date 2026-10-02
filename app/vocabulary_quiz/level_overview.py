"""어휘 레벨 현황 - 공식등급/현재 출제 레벨/사람 판정(tier1)/동형이의·전문
의미 예외/연결 문항 수를 한 화면에서 조회하는 **읽기 전용** 통합 뷰.

절대 하지 않는 것: vocab_level/level_status/boundary_flag/student_exposure/
public_ready를 바꾸는 코드, 문항·매니페스트를 바꾸는 코드, 판정을 쓰는 코드
(판정은 tier1 화면(`/vocab-official-grade-review/`)에서만 하고, 여기서는 그
결과를 보여주기만 한다). Gemini 등 모델 제안은 이 파일 어디에도 없다 -
`VocabularyGrade5Candidate*` 계열 모델은 이 모듈에서 아예 import하지 않는다.

범주(`category`) 분류 로직은 `scripts/vocab/build_db_mapping_dryrun.py`의
dry-run과 **완전히 동일한 우선순위**를 그대로 포팅했다(tier1 > RULE_A/B >
공식5등급 > 미매칭위험 > 해당없음) - 두 코드가 서로 다른 결과를 내면 둘 중
하나가 버그이므로, 분류 함수를 다시 쓰지 않고 그 스크립트의 로직을 1:1로
옮겼다."""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.vocabulary_quiz.models import VocabularyContent, VocabularyContentLevel, VocabularyItem, VocabularyMultiformatItem
from app.vocabulary_quiz.models_official_grade_review import (
    VocabularyOfficialGradeJudgment,
    VocabularyOfficialGradeReference,
)

LEVEL_VERSION = "level_policy_v0.1"
RISK_MATCH_TYPES = {"매칭없음", "다중후보", "단일일치_동형이의주의", "표제어만일치"}
GRADE_LABELS = {
    0: "초등 1~2학년", 1: "초등 3~4학년", 2: "초등 5~6학년",
    3: "중등 1~2학년", 4: "중등 3학년", 5: "고등 1학년", 6: "고등 2~3학년",
}
CATEGORY_LABELS = {
    "tier1_기본레벨조정": "tier1: 기본 레벨 조정(사람 승인)",
    "tier1_현재유지": "tier1: 현재 유지(사람 승인)",
    "tier1_기타판정": "tier1: 뜻 확인/보류",
    "RULE_A": "RULE_A(정책 후보, 미승인)",
    "RULE_B": "RULE_B(정책 후보, 미승인)",
    "공식5등급_미검수": "공식5등급 미검수(경계 유지)",
    "미매칭다중후보의미위험_별도검토": "미매칭·다중후보·의미위험(별도 검토)",
    "해당없음": "해당없음(정책 변경 없음)",
}
# 필터용 묶음 - "변경후보"=이미 승인됐거나 정책상 바뀔 수 있는 쪽,
# "검토필요"=자동 적용 대상이 아니라 사람이 봐야 하는 쪽.
CHANGE_CANDIDATE_CATEGORIES = {"tier1_기본레벨조정", "RULE_A", "RULE_B"}
NEEDS_REVIEW_CATEGORIES = {"공식5등급_미검수", "미매칭다중후보의미위험_별도검토", "tier1_기타판정"}


def categorize(ref: VocabularyOfficialGradeReference | None, tier1_judgment: str | None) -> str:
    if tier1_judgment == "기본 레벨 조정":
        return "tier1_기본레벨조정"
    if tier1_judgment == "현재 유지":
        return "tier1_현재유지"
    if tier1_judgment in ("뜻 확인", "보류"):
        return "tier1_기타판정"
    if ref is None:
        return "해당없음"
    if ref.review_status == "RULE_PROPOSED_PENDING_APPROVAL":
        return "RULE_A" if ref.proposed_base_level == "L2" else "RULE_B"
    if ref.official_grade == "5":
        return "공식5등급_미검수"
    if ref.match_type in RISK_MATCH_TYPES:
        return "미매칭다중후보의미위험_별도검토"
    return "해당없음"


@dataclass
class Row:
    content_id: str
    lemma: str
    pos: str | None
    현재레벨: int | None
    grade_label: str | None
    level_status: str | None
    boundary_flag: int | None
    official_grade: str | None
    proposed_base_level: str | None
    match_type: str | None
    exception_reason: str | None
    exception_reason_secondary: str | None
    tier1_judgment: str | None
    tier1_reviewed_at: str | None
    category: str = ""
    linked_item_count: int = 0


def _latest_tier1_judgments(db: Session) -> dict[str, VocabularyOfficialGradeJudgment]:
    rows = db.query(VocabularyOfficialGradeJudgment).order_by(
        VocabularyOfficialGradeJudgment.content_id,
        VocabularyOfficialGradeJudgment.reviewed_at.desc(),
        VocabularyOfficialGradeJudgment.id.desc(),
    ).all()
    latest: dict[str, VocabularyOfficialGradeJudgment] = {}
    for j in rows:
        latest.setdefault(j.content_id, j)  # 먼저 나온(=가장 최신, ORDER BY 덕분) 것만 남김
    return latest


def _linked_item_counts(db: Session) -> dict[str, int]:
    """vocabulary_items(기존 단일 4지선다) + vocabulary_multiformat_items(다유형,
    모든 source_version - 파일럿/미리보기 포함) 전부를 합산한다. 레벨 모드 출제
    가능 여부(2.1.29 풀만 보는 _select_level_candidates)와는 다른 목적의
    카운트다 - "이 콘텐츠에 문항이 있기는 한가"를 보는 용도."""
    counts: dict[str, int] = {}
    for content_id, n in (
        db.query(VocabularyItem.content_id, func.count())
        .filter(VocabularyItem.is_active == 1)
        .group_by(VocabularyItem.content_id)
        .all()
    ):
        counts[content_id] = counts.get(content_id, 0) + n

    mf_items = db.query(
        VocabularyMultiformatItem.source_content_id,
        VocabularyMultiformatItem.source_content_ids_json,
        VocabularyMultiformatItem.item_type,
    ).filter(VocabularyMultiformatItem.is_active == 1).all()
    for source_content_id, source_content_ids_json, item_type in mf_items:
        if item_type == "MATCH_WORD_MEANING":
            for cid in json.loads(source_content_ids_json or "[]"):
                counts[cid] = counts.get(cid, 0) + 1
        elif source_content_id:
            counts[source_content_id] = counts.get(source_content_id, 0) + 1
    return counts


def load_all_rows(db: Session) -> list[Row]:
    """5,950건 전체를 한 번에 메모리로 읽어 매핑한다(매 요청마다 - content_id
    기준 조인만 쓴다, lemma 조인 없음). 5,950행은 요청당 한 번 읽기에
    충분히 작다."""
    contents = db.query(VocabularyContent).filter(VocabularyContent.is_active == 1).all()
    levels = {
        cl.content_id: cl
        for cl in db.query(VocabularyContentLevel).filter(
            VocabularyContentLevel.level_version == LEVEL_VERSION,
            VocabularyContentLevel.is_active == 1,
        ).all()
    }
    refs = {r.content_id: r for r in db.query(VocabularyOfficialGradeReference).all()}
    tier1 = _latest_tier1_judgments(db)
    item_counts = _linked_item_counts(db)

    rows: list[Row] = []
    for c in contents:
        lvl = levels.get(c.content_id)
        ref = refs.get(c.content_id)
        j = tier1.get(c.content_id)
        row = Row(
            content_id=c.content_id, lemma=c.lemma, pos=c.pos,
            현재레벨=lvl.vocab_level if lvl else None,
            grade_label=GRADE_LABELS.get(lvl.vocab_level) if lvl else None,
            level_status=lvl.level_status if lvl else None,
            boundary_flag=lvl.boundary_flag if lvl else None,
            official_grade=ref.official_grade if ref else None,
            proposed_base_level=ref.proposed_base_level if ref else None,
            match_type=ref.match_type if ref else None,
            exception_reason=ref.exception_reason if ref else None,
            exception_reason_secondary=ref.exception_reason_secondary if ref else None,
            tier1_judgment=j.judgment if j else None,
            tier1_reviewed_at=j.reviewed_at if j else None,
            linked_item_count=item_counts.get(c.content_id, 0),
        )
        row.category = categorize(ref, row.tier1_judgment)
        rows.append(row)
    return rows


@dataclass
class ListResult:
    rows: list[Row]
    total_matched: int
    total_all: int
    category_counts: dict[str, int] = field(default_factory=dict)
    page: int = 1
    page_size: int = 50
    total_pages: int = 1


def filter_and_paginate(
    all_rows: list[Row], *, level: int | None = None, judgment: str | None = None,
    change_candidate_only: bool = False, needs_review_only: bool = False,
    q: str = "", page: int = 1, page_size: int = 50,
) -> ListResult:
    category_counts: dict[str, int] = {}
    for r in all_rows:
        category_counts[r.category] = category_counts.get(r.category, 0) + 1

    filtered = all_rows
    if level is not None:
        filtered = [r for r in filtered if r.현재레벨 == level]
    if judgment == "__none__":
        filtered = [r for r in filtered if r.tier1_judgment is None]
    elif judgment:
        filtered = [r for r in filtered if r.tier1_judgment == judgment]
    if change_candidate_only:
        filtered = [r for r in filtered if r.category in CHANGE_CANDIDATE_CATEGORIES]
    if needs_review_only:
        filtered = [r for r in filtered if r.category in NEEDS_REVIEW_CATEGORIES]
    if q:
        filtered = [r for r in filtered if q in r.lemma]

    total_matched = len(filtered)
    total_pages = max(1, (total_matched + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    page_rows = filtered[start:start + page_size]

    return ListResult(
        rows=page_rows, total_matched=total_matched, total_all=len(all_rows),
        category_counts=category_counts, page=page, page_size=page_size, total_pages=total_pages,
    )
