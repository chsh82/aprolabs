# -*- coding: utf-8 -*-
"""L3 중등 보강 2차 - 위험 기반 검수(생성과 분리된 AI 재검토 + 고정시드 표본)
결과 데이터. 전수 사람 검수 대신 쓰는 절차의 1회차 산출물이다.

**이 파일의 판정은 사람 승인이 아니다** - `vocabulary_publish_reviews`에는
아무것도 쓰지 않는다(검수 화면에서 사람이 버튼을 눌러야만 그 테이블에
행이 생기는 기존 원칙 그대로). 여기 기록은 `vocabulary_multiformat_items.
qa_flags_json`의 `risk_review` 키에만 추가되어, 검수 화면의 "위험 항목"/
"표본 검수" 필터가 읽는 참고 정보로만 쓰인다.

== 1단계: 기존 자동검사 재사용 ==
build_grade5_batch2_dryrun.py가 이미 76문항 전수에 대해 기계적 검사
(정답 유일성·예문 내 표제어 존재·정답이 오답에도 포함되는지)를 돌려
전부 PASS였다(reports/nikl_grade5_l3_batch2_dryrun_20261007.md 4절) -
이번에 다시 돌리지 않고 그 결과를 그대로 재사용한다(STRUCTURAL_FAIL 0건).

== 2단계: 생성과 분리된 AI 재검토 ==
이번에 새로 수행한 기계적 검사: 보기 4개의 글자 수를 비교해 "정답만
유난히 짧거나 길면(평균 오답의 1.6배 초과 또는 0.6배 미만) 뜻을 몰라도
보기 길이만으로 맞힐 단서가 된다"는 기준으로 전수 스캔한 결과,
종잡다·축하문 2건이 걸렸다(정답이 평균 오답보다 눈에 띄게 짧음).
이어서 38어휘 전체를 복수 정답 가능성·오답 관련성·문맥 자연스러움
관점에서 직접 다시 읽어 '앎'을 별도로 추가 플래그했다(오답 자체는
문제없으나 "앎"이라는 개념 자체가 추상적 탈동사명사라 사람이 한 번 더
보는 게 안전하다고 판단 - 틀렸다는 뜻이 아니라 신중을 기하는 플래그).

== 결과 ==
RISK_FLAGGED 3건(앎, 종잡다, 축하문)은 사람 확인 대기열로, 나머지
PASS_POOL 35건 중 고정 시드(20261007)로 12문항을 표본 선정해 재검토
(3단계) - 아래 SAMPLE_REVIEW 참고, 전부 PASS(결함 0건)."""
from __future__ import annotations

SCHEMA_VERSION = "risk_based_sampling_v1"
REVIEWED_AT = "2026-10-07"
REVIEWER = "AI(Claude Sonnet 5) - 생성과 분리된 재검토, 사람 승인 아님"
SAMPLE_SEED = 20261007

# lemma -> (risk_category, risk_reason) - None/None이면 위험 없음(NONE)
RISK_CATEGORY: dict[str, tuple[str, str]] = {
    "앎": (
        "MEANING_UNCERTAIN",
        "추상적 탈동사명사(앎=아는 상태)라 인지활동 오답(탐구/암기/상상)과의 "
        "경계가 미묘함 - 오답 자체가 틀렸다는 뜻은 아니며, 개념이 추상적이라 "
        "사람이 한 번 더 확인하는 게 안전하다고 판단한 신중 플래그.",
    ),
    "종잡다": (
        "AI_FLAG_LENGTH_CUE",
        "정답(13자)이 평균 오답(22자)보다 눈에 띄게 짧아, 뜻을 몰라도 "
        "'가장 짧은 보기'를 고르는 단서가 될 위험(길이 단서).",
    ),
    "축하문": (
        "AI_FLAG_LENGTH_CUE",
        "정답(16자)이 평균 오답(27자)보다 눈에 띄게 짧아 같은 유형의 위험"
        "(길이 단서).",
    ),
}

# 3단계 표본(고정 시드 20261007, PASS 풀 35건 중 random.Random(seed).sample로
# 12단어 추출 후 가나다순 정렬, 짝수 인덱스=MEANING_CHOICE/홀수 인덱스=
# CONTEXT_MEANING으로 유형을 번갈아 배정 - 재현 가능, 좋은 문항만 고른 것이
# 아니라 추출 후 기계적으로 배정한 순서 그대로). 재현 스크립트:
#   rng = random.Random(20261007)
#   pool = sorted(38어휘 중 RISK_CATEGORY에 없는 35건)
#   sorted(rng.sample(pool, 12))
SAMPLE_REVIEW: dict[tuple[str, str], str] = {
    ("시찰", "MEANING_CHOICE"): "PASS",
    ("아무개", "CONTEXT_MEANING"): "PASS",
    ("열등하다", "MEANING_CHOICE"): "PASS",
    ("자전축", "CONTEXT_MEANING"): "PASS",
    ("중심각", "MEANING_CHOICE"): "PASS",
    ("지근지근", "CONTEXT_MEANING"): "PASS",
    ("춘곤증", "MEANING_CHOICE"): "PASS",
    ("치어", "CONTEXT_MEANING"): "PASS",
    ("타향", "MEANING_CHOICE"): "PASS",
    ("품팔이", "CONTEXT_MEANING"): "PASS",
    ("해명하다", "MEANING_CHOICE"): "PASS",
    ("화전민", "CONTEXT_MEANING"): "PASS",
}

assert len(RISK_CATEGORY) == 3
assert len(SAMPLE_REVIEW) == 12
assert all(v == "PASS" for v in SAMPLE_REVIEW.values())  # 이번 표본은 결함 0건
