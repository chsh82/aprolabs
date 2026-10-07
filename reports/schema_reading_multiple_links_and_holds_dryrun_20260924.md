# 보류된 출처 링크 규칙 — dry-run (2번 작업)

- 작성일: 2026-09-24
- 범위: `vocabulary_content_literacy_links`에 아직 삽입되지 않은 MULTIPLE_LINKS
  4행(속수무책·혼비백산) + 의미 보류 4건(시샘·평론·능가하다·설상가상)의 현재
  상태를 재확인하고, 실제로 링크 가능하다고 판단되는 것만 **dry-run**으로
  제시한다. **DB에는 아무것도 쓰지 않았다** — 아래 모든 값은 `mode=ro`로
  `data/literacy.db`를 직접 재조회해 검증했다(2026-09-24 재조회).

---

## 1. 속수무책 — 두 출처 모두 CANDIDATE로 보존 (dry-run)

`literacy.db` 재조회 결과(이번 세션):

| id | source | definition | review_status |
|---|---|---|---|
| 3185 | momo-textbook | 손을 묶은 것처럼 어찌할 도리가 없어 꼼짝 못 함. | 검수전 |
| 7201 | sajaseongeo-pdf | 손을 묶은 것처럼 어찌할 도리가 없어 꼼짝 못함. | 검수완료 |

**판단**: 두 정의는 띄어쓰기("못 함." vs "못함.") 하나만 다르고 의미가 완전히
동일하다 — phase5의 기존 판단("공백 1개 차이")이 그대로 재확인됨. 두 출처를
모두 보존할 근거가 충분하다.

**dry-run(실제 삽입 없음)**:

| content_id | literacy_term_id | literacy_source | literacy_headword | link_status | link_method | evidence |
|---|---|---|---|---|---|---|
| `SC_V1976_B076_042` | 3185 | momo-textbook | 속수무책 | CANDIDATE | `multi_source_both_preserved_v1` | 두 출처 정의 사실상 동일(공백 차이만) - momo-textbook 쪽도 보존 |
| `SC_V1976_B076_042` | 7201 | sajaseongeo-pdf | 속수무책 | CANDIDATE | `multi_source_both_preserved_v1` | 두 출처 정의 사실상 동일(공백 차이만) - sajaseongeo-pdf 쪽도 보존, `review_status=검수완료`로 더 신뢰도 높음 |

`UNIQUE(content_id, literacy_term_id)`가 복합키라 이 두 행은 동시에 존재할 수
있음(phase4 `prove_multiple_links_schema_capability.py`로 이미 스키마 증명됨).
**어느 쪽 출처의 레벨도 `vocabulary_content_levels`에 자동 적용하지 않는다**
— 이 테이블은 레벨 컬럼 자체가 없다(phase4 설계).

---

## 2. 혼비백산 — 여전히 미링크 유지 (조건 미충족 확인)

`literacy.db` 재조회 결과(이번 세션):

| id | source | definition | review_status |
|---|---|---|---|
| 3015 | momo-textbook | 몹시 놀라 넋을 잃음을 이르는. 말 | 검수전 |
| 7306 | sajaseongeo-pdf | 혼백이 어지러이 흩어진다는 뜻으로, 몹시 놀라 넋을 잃음을 이르는 말. | 검수완료 |

momo-textbook 쪽(id=3015)이 여전히 구두점/어순 손상 상태(phase6/7/8이 발견한
대표선정 오류의 일부, phase8 dry-run 6건 중 하나로 이미 수정안이 준비됐지만
**아직 DB에 미적용**) — 지시대로 **"손상된 교재 대표 뜻의 정비가 확인될
때까지" 이번에도 링크하지 않는다.** 1번 작업(phase8)의 6건 dry-run이 실제
적용된 뒤, momo-textbook 쪽 정의가 정정된 걸 재확인하고 나서 다시 이 판단을
꺼낼 것.

---

## 3. 시샘·평론·능가하다·설상가상 — 의미 연결 보류 유지 (상태 재확인만)

phase5/phase6이 이미 정리한 상태에서 변화 없음 — 근거가 해결되지 않아 계속
보류:

| headword | 상태 | 근거(요약, 상세는 `reports/schema_reading_review_cards/`) |
|---|---|---|
| 시샘 | HOLD | S정의 출처가 momo_book.db 원본에도 없음(NULL) - "시새움" 표제어 자체가 literacy.db에 없음. phase6이 재확인한 새로운 막힌 지점, 이번 세션에서 해소된 바 없음 |
| 평론 | HOLD | 손상 아님(원본과 동일 확인됨, phase6) - 순수 의미 범위(상위/하위 개념) 문제만 남아 여전히 미해결 |
| 능가하다 | HOLD | 쉼표 탈락이 momo_book.db 원본 교재 추출 단계에서부터 있었음(phase6) - 실제 지면 대조 전까지 확정 불가, 이번 세션에서 지면 대조 진행 안 함 |
| 설상가상 | HOLD | 오염 원인(파서 날짜 패턴)은 phase6/8이 특정했으나 **이건 사자성어 10건 배치(이미 적용됨, phase7)와 다른 개별 건**이다 - 설상가상 자체의 의미 연결 여부는 여전히 별개 미해결 사안으로 HOLD 유지 |

---

## 요약

| headword | 링크 상태 | 실제 DB 반영 |
|---|---|---|
| 속수무책 | dry-run 2행 준비됨(CANDIDATE, 두 출처 보존) | **미적용** - 사용자 승인 후 적용 대상 |
| 혼비백산 | 미링크 유지 | 변화 없음 |
| 시샘 | 의미 연결 보류 | 변화 없음 |
| 평론 | 의미 연결 보류 | 변화 없음 |
| 능가하다 | 의미 연결 보류 | 변화 없음 |
| 설상가상 | 의미 연결 보류 | 변화 없음 |

이번 세션은 `data/literacy.db`를 mode=ro로만 조회했고 어떤 DB에도 쓰지
않았다. `vocabulary_content_literacy_links`(서버 research DB)도 이번엔
접속하지 않았다(속수무책의 dry-run은 로컬 literacy.db 재조회만으로 완결).
