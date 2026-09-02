"""level_min(1~12학년) 계산식 - 이 파일이 유일한 계산 장소다.

data/vocab/schema.sql의 설계 의도(quiz-api.md "난이도 파라미터" 절):
난이도 3축(hanja_score/abstraction_score/frequency_score)을 따로 저장해
두는 이유는 등급 기준이 바뀌어도 전수 재검토 없이 계산식만 고치면 되게
하기 위함이다. scripts/vocab/calc_level.py를 포함해 level_min이 필요한
곳은 전부 이 파일의 함수만 호출한다 - 계산식을 다른 곳에 복제하지 않는다.

임시 규칙(2026-09-03, 사용자 결정):
- 한자 급수(hanja.grade) 데이터가 아직 없어서 hanja_score는 구성 한자
  4개의 획수 평균으로 대신한다(scripts/vocab/populate_hanja_strokes.py가
  Unicode Unihan 데이터로 hanja.stroke_count를 채워 둔다). 급수 데이터가
  들어오면 compute_hanja_score()만 급수 기반으로 바꾸면 된다 - 호출부는
  그대로 둔다.
- abstraction_score는 지금 산정 방법이 없어 항상 None이다.
- frequency_score는 inclusion_evidence 건수로 대신한다(exam 근거는
  가중치를 더 준다).
- level_min은 literacy DB에서 이관된 원래 레벨이 있으면 그걸 최우선으로
  쓰고(level_min_from_literacy_level), 없는 항목만 hanja_score/
  frequency_score로 추정한다(estimate_level_min).
"""
from __future__ import annotations

# --- hanja_score (임시: 획수 평균 기반) ---
# 154개 표제어의 평균 획수 분포(2026-09-03 실측: 최소 2.75, 최대 15.0)에
# 맞춘 임시 보정값 - 급수 데이터로 교체되면 이 상수들도 같이 정리한다.
MIN_AVG_STROKES = 3.0
MAX_AVG_STROKES = 15.0

# --- frequency_score (임시: inclusion_evidence 가중합 기반) ---
EXAM_EVIDENCE_WEIGHT = 2
OTHER_EVIDENCE_WEIGHT = 1
FREQUENCY_WEIGHTED_CAP = 5  # 가중합이 이 값 이상이면 frequency_score = 1.0

# literacy DB level(0~6, 2개 학년을 묶은 척도) -> vocab level_min(1~12학년)의
# "권장 최소 학년" 매핑. app/literacy/migrations/002_add_level.py의 표와
# 같다 - 구간의 하한을 취한다(level_min은 "최소" 학년이므로).
LITERACY_LEVEL_TO_GRADE_MIN = {0: 1, 1: 3, 2: 5, 3: 7, 4: 9, 5: 10, 6: 12}


def compute_hanja_score(stroke_counts: list[int]) -> float | None:
    """구성 한자 획수 평균을 0.0~1.0으로 정규화. 획수 데이터가 하나도 없으면 None."""
    if not stroke_counts:
        return None
    avg = sum(stroke_counts) / len(stroke_counts)
    clamped = max(MIN_AVG_STROKES, min(MAX_AVG_STROKES, avg))
    return round((clamped - MIN_AVG_STROKES) / (MAX_AVG_STROKES - MIN_AVG_STROKES), 4)


def compute_frequency_score(evidence_source_types: list[str]) -> float | None:
    """inclusion_evidence의 source_type 목록 -> 0.0~1.0. 근거가 없으면 None."""
    if not evidence_source_types:
        return None
    weighted = sum(
        EXAM_EVIDENCE_WEIGHT if s == "exam" else OTHER_EVIDENCE_WEIGHT
        for s in evidence_source_types
    )
    return round(min(weighted, FREQUENCY_WEIGHTED_CAP) / FREQUENCY_WEIGHTED_CAP, 4)


def level_min_from_literacy_level(literacy_level: int) -> int | None:
    """literacy DB level(0~6) -> vocab level_min(1~12). 매핑 밖 값이면 None."""
    return LITERACY_LEVEL_TO_GRADE_MIN.get(literacy_level)


def estimate_level_min(
    hanja_score: float | None,
    abstraction_score: float | None,
    frequency_score: float | None,
) -> int | None:
    """literacy 값이 없을 때만 쓰는 추정치. hanja_score 없이는 추정 불가(None).

    abstraction_score는 항상 None이라 지금은 반영되지 않는다 - 나중에
    산정 방법이 생기면 이 함수에 추가한다(계산식을 이 함수 밖으로 복제하지
    말 것).
    """
    if hanja_score is None:
        return None
    grade = 1 + hanja_score * 11  # 1~12
    if frequency_score is not None:
        # 자주 노출된(특히 exam) 성어는 더 이른 학년에도 다뤄질 가능성이
        # 높다고 보고 소폭 하향 조정한다 - hanja_score가 주된 근거다.
        grade -= frequency_score * 2
    return max(1, min(12, round(grade)))
