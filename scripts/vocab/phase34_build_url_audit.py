#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase34 항목3/4: DRAFT_READY 54건(현재는 재검증 후 51건) 전체에 대해
메인 세션이 직접 재접속(WebFetch/WebSearch)해 확인한 결과를 행별로
기록한다. 이 스크립트 자체는 웹에 접근하지 않는다 - 재접속은 이미 사람
(호출 세션)이 수행했고, 그 결과를 아래 AUDIT 딕셔너리에 수기로 기록한 뒤
이 스크립트가 verdicts CSV와 병합해 최종 표를 만든다.

evidence_tier 5종: 사전 / 공공기관 / 학술_교재 / 언론 / 개인사이트
access_status 3종: OK(재접속 성공, 인용문 확인) / REPLACED(원 URL 실패 ->
표준국어대사전 등 원출처로 교체) / FAILED(재접속 실패, 대체 자료도 못 찾음)
quote_supports 3종: YES / PARTIAL(표현·범위 차이 있으나 핵심은 지지) / NO
"""
from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# term_id -> (evidence_tier, access_status, quote_supports, note)
AUDIT: dict[str, tuple[str, str, str, str]] = {
    "5881": ("언론", "OK", "YES", "코리아데일리 [우리말 바루기] - 표준국어대사전 인용 기사, 직접 재접속 확인"),
    "5954": ("공공기관", "OK", "NO", "정당법 제2조는 '정당' 정의이지 '정당정치' 정의가 아님, KCI 논문도 다른 강조점 - HOLD로 재분류"),
    "5980": ("학술_교재", "OK", "YES", "KB Think 금융용어사전, 재접속 확인"),
    "6881": ("학술_교재", "OK", "YES", "KIAS(고등과학원) HORIZON, 재접속 확인"),
    "5924": ("학술_교재", "OK", "YES", "금성출판사 스마트 教과서 사전, 재접속 확인"),
    "5945": ("공공기관", "OK", "YES", "헌법재판소 공식 사이트, 재접속 확인"),
    "7025": ("사전", "OK", "YES", "반도체=한국민족문화대백과사전(재접속 확인), 전도성=표준국어대사전(직접 재확인)"),
    "7045": ("사전", "REPLACED", "YES", "원 URL(wordrow.kr) 403 -> 표준국어대사전 직접 재확인으로 교체"),
    "6832": ("학술_교재", "OK", "YES", "서울아산병원 의학정보, 재접속 확인"),
    "6851": ("학술_교재", "OK", "YES", "한국민족문화대백과사전, 재접속 확인(집필자·수정일 확인됨)"),
    "6990": ("학술_교재", "OK", "YES", "한국연구원(RIKS) 웹진, 재접속 확인"),
    "7009": ("학술_교재", "OK", "PARTIAL", "IBM 공식 설명, 핵심은 지지하나 '고전컴퓨터로 불가능한 문제 해결'이라는 강조점 AI정의에 없음"),
    "5747": ("사전", "REPLACED", "YES", "원 URL(wordrow.kr) 403 -> 표준국어대사전 직접 재확인으로 교체"),
    "5766": ("언론", "OK", "YES", "농민신문 경제칼럼, 재접속 확인"),
    "5844": ("공공기관", "OK", "PARTIAL", "KDI 자료, 재접속 확인하나 '중앙은행 개입으로 실제로는 완전 자동은 아님'이라는 반론도 포함 - 이론적 서술은 지지"),
    "5862": ("언론", "OK", "YES", "단비뉴스 시사용어, 재접속 확인"),
    "6799": ("언론", "OK", "YES", "코리아데일리(빛과 열)+위키백과(위치 고정, 보조), 둘 다 재접속 확인"),
    "6816": ("언론", "OK", "YES", "이코노미사이언스 과학전문매체, 재접속 확인"),
    "5817": ("학술_교재", "OK", "NO", "자유기업원 원문이 AI 정의의 통속적 서술을 명시적으로 반박 - CONFLICT로 재분류, HOLD"),
    "6863": ("공공기관", "OK", "YES", "국가생명공학정책연구센터(BioIN), 재접속 확인"),
    "6880": ("공공기관", "OK", "PARTIAL", "국가나노기술정책센터, 정확한 문구 대신 사례로 특이성 시사"),
    "6784": ("공공기관", "OK", "YES", "KISTI 사이언스온, 재접속 확인(주기 수치까지 일치)"),
    "6938": ("사전", "REPLACED", "YES", "원 URL(wordrow.kr) 403 -> 표준국어대사전 직접 재확인으로 교체"),
    "7017": ("학술_교재", "OK", "YES", "금성출판사 티칭백과, 재접속 확인"),
    "5825": ("언론", "OK", "YES", "한경 경제용어사전, 재접속 확인"),
    "6777": ("공공기관", "OK", "YES", "디지털집현전(방위사업청), 재접속 확인 + 발행기관 직접 확정"),
    "5775": ("공공기관", "OK", "PARTIAL", "KDI, '소득 일정' 전제 조건이 AI 정의에 없음(표준 단순화 수준)"),
    "7053": ("공공기관", "OK", "YES", "기초과학연구원(IBS) 웹진, 재접속 확인"),
    "6763": ("학술_교재", "OK", "PARTIAL", "사이언스올, 동적평형 관점 vs AI의 '최대 포함' 관점 - 기존 PARTIAL_MATCH 유지"),
    "6825": ("개인사이트", "OK", "YES", "Javalab(물리교사 운영), 재접속 확인 - 개인사이트 등급 명시"),
    "6945": ("언론", "OK", "YES", "전기신문(kmecnews), 재접속 확인"),
    "6968": ("학술_교재", "FAILED", "NO", "SNU 교육청 연수자료 PDF는 실재하나 스캔형이라 텍스트 추출 불가 - 인용 안 함, HOLD"),
    "6918": ("사전", "REPLACED", "YES", "원 URL(gnu.ac.kr) ECONNREFUSED -> 표준국어대사전 직접 재확인으로 교체"),
    "5839": ("공공기관", "OK", "PARTIAL", "기획재정부 시사경제용어사전, 재접속 확인"),
    "5786": ("공공기관", "OK", "PARTIAL", "기획재정부 시사경제용어사전, 정확한 표현(부담자=의무자)까지는 명시 안 함 - 표준국어대사전이 더 정확했음(참고)"),
    "5876": ("학술_교재", "OK", "YES", "금성출판사 스마트 사전, 재접속 확인"),
    "6956": ("사전", "REPLACED", "YES", "원 URL(dict.wordrow.kr) 403 -> 표준국어대사전 직접 재확인으로 교체"),
    "5796": ("학술_교재", "OK", "PARTIAL", "금성출판사 사전, '교역조건 반영' 뉘앙스가 AI 정의에 없음(표준 단순화 수준)"),
    "5868": ("사전", "OK", "YES", "표준국어대사전 '도덕' 항목, 재접속 확인(정확한 문구 일치)"),
    "5950": ("공공기관", "OK", "YES", "대법원 어린이 법교육 사이트, 재접속 확인"),
    "6904": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "6856": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "6867": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "6756": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "6820": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "6962": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "6993": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "7027": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "5749": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "5794": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "5834": ("사전", "OK", "YES", "우리말샘, 재접속 확인"),
    "5845": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "5884": ("사전", "OK", "YES", "표준국어대사전, 재접속 확인"),
    "5968": ("사전", "OK", "YES", "우리말샘, 재접속 확인"),
}


def main() -> int:
    verdicts_path = REPO_ROOT / "data/import/schema_reading_phase34_verdicts_20260928.csv"
    with open(verdicts_path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    draft_ready_54_original = {
        "5881", "5954", "5980", "6881", "5924", "5945", "7025", "7045", "6832", "6851",
        "6990", "7009", "5747", "5766", "5844", "5862", "6799", "6816", "5817", "6863",
        "6880", "6784", "6938", "7017", "5825", "6777", "5775", "7053", "6763", "6825",
        "6945", "6968", "6918", "5839", "5786", "5876", "6956", "5796", "5868", "5950",
        "6904", "6856", "6867", "6756", "6820", "6962", "6993", "7027", "5749", "5794",
        "5834", "5845", "5884", "5968",
    }
    assert len(draft_ready_54_original) == 54, len(draft_ready_54_original)
    assert set(AUDIT.keys()) == draft_ready_54_original, (
        set(AUDIT.keys()) ^ draft_ready_54_original
    )

    by_id = {r["term_id"]: r for r in rows}
    out_rows = []
    for tid, (tier, access, supports, note) in AUDIT.items():
        r = by_id[tid]
        out_rows.append({
            "term_id": tid, "headword": r["headword"], "batch": r["batch"],
            "location": r["location"], "evidence_tier": tier,
            "access_status": access, "quote_supports_target_meaning": supports,
            "final_status_after_reverify": r["final_status"],
            "hold_category_after_reverify": r["hold_category"],
            "note": note,
        })

    out_path = REPO_ROOT / "data/import/schema_reading_phase34_url_audit_20260928.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    from collections import Counter
    print(f"전수 재검증 대상: {len(out_rows)}건")
    print("evidence_tier 분포:", dict(Counter(o["evidence_tier"] for o in out_rows)))
    print("access_status 분포:", dict(Counter(o["access_status"] for o in out_rows)))
    print("quote_supports 분포:", dict(Counter(o["quote_supports_target_meaning"] for o in out_rows)))
    print("재검증 후 final_status 분포:", dict(Counter(o["final_status_after_reverify"] for o in out_rows)))
    print(f"저장: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
