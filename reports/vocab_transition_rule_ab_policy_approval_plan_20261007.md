# RULE_A/B 대량 레벨 전환 승인안 — 2026-10-07

## 0. 이번 단계 범위

이번 문서는 RULE_A/B 대량 레벨 전환의 정책 승인안을 정리한다.

수행한 것:

- 기존 정합성/실행준비/옵션1 적용/관리자 smoke 문서 재검토
- live 연구 DB 읽기 전용 재확인
- 적용 선택지, 영향, 리스크, 승인 문구, 롤백 원칙 정리

수행하지 않은 것:

- DB UPDATE/INSERT/DELETE 없음
- 배포 없음
- 학생 공개/allowlist/feature flag 변경 없음
- 문항 본문/정답/활성 상태 변경 없음
- 사람 판정 이력 변경 없음

## 1. 현재 기준선

기준:

- 서버 repo: `/home/chsh82/aprolabs`
- 서버 HEAD/origin/main: `6b33147`
- 연구 DB: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- DB 조회 방식: SQLite `mode=ro` + `PRAGMA query_only=ON`
- DB integrity: `ok`

현재 상태:

- L3 옵션1 일반 L3 연결은 완료되어 관리자 all_candidates에서 동작 확인됨.
- L3 옵션1 level_status는 `REVIEW_BOUNDARY`라 auto_only에는 포함되지 않음.
- 옵션1 연결 후 관리자 availability:
  - L3 all_candidates: `149어휘 / 444문항`
  - L3 auto_only: `80어휘 / 297문항`
- RULE_A/B는 아직 미적용.

live DB 재확인 결과:

| 항목 | 건수 |
|---|---:|
| RULE_A 후보 | 1,368 |
| RULE_B 후보 | 40 |
| RULE_A/B 합계 | 1,408 |
| 기타 pending 패턴 | 0 |
| RULE_A/B 대상 public/student 노출 플래그 비0 | 0 |
| 옵션1 L3 REVIEW_BOUNDARY 연결 콘텐츠 | 64 |

pending 패턴 분포:

| official_grade | proposed_base_level | 현재 vocab_level | level_status | 건수 |
|---:|---|---:|---|---:|
| 3 | L1 | 2 | PROVISIONAL_AUTO | 7 |
| 3 | L1 | 2 | REVIEW_BOUNDARY | 33 |
| 4 | L2 | 3 | PROVISIONAL_AUTO | 1,315 |
| 4 | L2 | 3 | REVIEW_BOUNDARY | 53 |

## 2. RULE 정의

### RULE_A

정의:

- `review_status = RULE_PROPOSED_PENDING_APPROVAL`
- 공식 등급 `3`이 아니라 `4`
- 제안 기본 레벨 `L2`
- 현재 서비스 레벨 `L3`

적용 방향:

- `L3 -> L2`

대상:

- 1,368 content rows

정책 의미:

- 공식 4등급 어휘를 중등 1~2학년군 L3가 아니라 초등 고학년/중간 단계에 가까운 L2로 낮추는 대량 정책 전환.

### RULE_B

정의:

- `review_status = RULE_PROPOSED_PENDING_APPROVAL`
- 공식 등급 `3`
- 제안 기본 레벨 `L1`
- 현재 서비스 레벨 `L2`

적용 방향:

- `L2 -> L1`

대상:

- 40 content rows

정책 의미:

- 공식 3등급 어휘를 L2가 아니라 L1로 낮추는 소규모 정책 전환.

## 3. 예상 영향

기존 정합성 보고 기준의 공급량 시뮬레이션에 현재 옵션1 연결 상태를 반영하면 다음과 같다.

### 현재 상태

| 레벨/모드 | 현재 공급량 |
|---|---:|
| L1 all_candidates | 89어휘 / 324문항 |
| L1 auto_only | 67어휘 / 244문항 |
| L2 all_candidates | 92어휘 / 346문항 |
| L2 auto_only | 6어휘 / 23문항 |
| L3 all_candidates | 149어휘 / 444문항 |
| L3 auto_only | 80어휘 / 297문항 |

### RULE_A/B 모두 적용 후 예상

옵션1 64콘텐츠는 `REVIEW_BOUNDARY`라 L3 all_candidates에만 남고 auto_only에는 들어가지 않는다.

| 레벨/모드 | RULE_A/B 후 예상 |
|---|---:|
| L1 all_candidates | 91어휘 / 332문항 |
| L1 auto_only | 67어휘 / 244문항 |
| L2 all_candidates | 164어휘 / 617문항 |
| L2 auto_only | 78어휘 / 289문항 |
| L3 all_candidates | 75어휘 / 170문항 |
| L3 auto_only | 8어휘 / 31문항 |

핵심 변화:

- L2 공급량은 크게 늘어난다.
- L3 공급량은 크게 줄어든다.
- 옵션1 64콘텐츠 연결 덕분에 L3 all_candidates는 최소 공급량이 `75어휘 / 170문항` 수준으로 유지된다.
- 그러나 L3 auto_only는 `8어휘 / 31문항`까지 감소할 수 있다.

## 4. 선택지

### 선택지 A — RULE_A/B 모두 보류, 현 상태 유지

내용:

- RULE_A 1,368건 미적용
- RULE_B 40건 미적용
- 현재 옵션1 L3 연결 상태 유지

장점:

- 데이터 대량 변경 위험이 없다.
- L3 관리자 all_candidates 공급량 `149/444`를 유지한다.
- 현재 운영 안정성을 보존한다.

단점:

- 공식 등급 기반 정책 후보 1,408건이 계속 pending 상태로 남는다.
- L2 공급량 부족 문제는 그대로다.

추천 상황:

- 교육정책 결정을 더 검토하고 싶을 때.
- L3 공급량 감소를 아직 감수하기 어려울 때.

### 선택지 B — RULE_B 40건만 먼저 적용

내용:

- RULE_B 40건만 `L2 -> L1`
- RULE_A 1,368건은 보류

장점:

- 변경 규모가 작다.
- 공식 3등급 -> L1 전환만 먼저 검증할 수 있다.
- 대량 RULE_A보다 롤백·검수가 쉽다.

단점:

- L2 공급량 확대 효과는 거의 없다. 오히려 L2에서 40건이 빠진다.
- 전체 정책 정리 효과가 제한적이다.

추천 상황:

- 소규모 선행 배치를 통해 절차를 검증하고 싶을 때.

### 선택지 C — RULE_A 1,368건만 적용

내용:

- RULE_A 1,368건 `L3 -> L2`
- RULE_B 40건은 보류

장점:

- L2 공급량 부족을 크게 개선한다.
- 가장 큰 정책 후보 집합을 정리한다.

단점:

- L3 공급량이 크게 감소한다.
- 교육정책상 대량 전환이라 승인 책임이 크다.
- L3 auto_only는 크게 줄어들 수 있다.

추천 상황:

- 공식 4등급=L2 정책 방향을 확정할 수 있을 때.

### 선택지 D — RULE_A/B 모두 적용

내용:

- RULE_A 1,368건 `L3 -> L2`
- RULE_B 40건 `L2 -> L1`

장점:

- pending 정책 후보 1,408건을 한 번에 정리한다.
- L1/L2/L3 정책 체계를 가장 일관되게 맞춘다.
- 시뮬레이션/검증 대상이 명확하다.

단점:

- 변경 규모가 크다.
- L3 공급량 감소가 가장 크다.
- 적용 후 관리자 퀴즈 공급 구성이 크게 바뀐다.

추천 상황:

- 공식 등급 기반 자동 정책을 대표님이 명시적으로 승인할 때.

## 5. Davinci 권장안

현재 권장안은 `선택지 B`가 아니라 `선택지 A 또는 C`다.

판단:

- 목표가 “안정성 최우선”이면 선택지 A가 맞다.
- 목표가 “L2 공급량 확보 및 정책 후보 정리”라면 선택지 C가 실질적이다.
- RULE_B 40건만 먼저 적용하는 선택지 B는 절차 rehearsal로는 좋지만, 공급량 문제 해결 효과가 작다.
- RULE_A/B 모두 적용하는 선택지 D는 정책적으로 깔끔하지만, L3 auto_only 감소 폭이 커서 바로 권장하기는 어렵다.

따라서 운영 관점 추천 순서는 다음과 같다.

1. `선택지 C` — RULE_A만 적용: L2 공급량 확보 목적이 명확할 때.
2. `선택지 A` — 보류: L3 공급 안정성을 우선할 때.
3. `선택지 D` — 둘 다 적용: 정책 일괄 정리를 대표님이 확정할 때.
4. `선택지 B` — RULE_B만 적용: 절차 검증용 소규모 적용이 필요할 때.

## 6. 실제 적용 시 공통 안전 절차

승인 후에도 바로 DB를 바꾸지 않고 아래 순서로 진행한다.

1. live DB 읽기 전용 사전 재검증
   - integrity_check
   - RULE_A/B target count
   - pending pattern 분포
   - 공개 플래그 0 확인
   - tier1/option1 교집합 제외 확인
2. rollback manifest 생성
   - content_id
   - 기존 vocab_level
   - 기존 level_status
   - 신규 vocab_level
   - 적용 rule
3. DB 전체 백업 생성
   - 경로, 크기, SHA-256 기록
4. 격리 DB 복사본 dry-run
   - 예상 UPDATE 건수 확인
   - 공급량 재계산
   - 롤백 리허설 가능 여부 확인
5. 실제 DB 단일 트랜잭션 적용
   - 승인된 rule만 적용
   - 영향 행 수 불일치 시 rollback
6. 사후 검증
   - integrity_check / foreign_key_check
   - 대상 조건 잔여 0 확인
   - 공개 플래그 불변 확인
   - L1/L2/L3 availability 재계산
7. 보고서/상태파일 작성
8. 필요 시 관리자 API smoke 재검증

## 7. 중단 조건

다음 중 하나라도 발생하면 실제 적용을 중단한다.

- RULE_A count가 1,368이 아님
- RULE_B count가 40이 아님
- 기타 pending 패턴이 0이 아님
- public_ready 또는 student_exposure가 0이 아닌 대상이 발견됨
- tier1 또는 옵션1 보강 대상과 교집합이 발견됨
- dry-run UPDATE 수와 target manifest 수가 다름
- DB integrity_check 실패
- DB 경로가 연구 DB가 아님
- rollback manifest 생성 실패

## 8. 승인 문구

### 선택지 A 승인 문구

`RULE_A/B는 현 시점에서 보류합니다. 현재 L3 옵션1 연결 상태만 유지하고, DB write/배포/학생 공개 변경 없이 다음 검토 단계로 진행하세요.`

### 선택지 B 승인 문구

`RULE_B 40건만 연구 DB에서 L2에서 L1로 변경하는 것을 승인합니다. RULE_A는 보류합니다. DB 백업, 대상 ID 고정, rollback manifest, 격리 dry-run, 단일 트랜잭션 적용, 사후 검증까지 포함하며 학생 공개/allowlist/feature flag 변경은 승인하지 않습니다.`

### 선택지 C 승인 문구

`RULE_A 1,368건만 연구 DB에서 L3에서 L2로 변경하는 것을 승인합니다. RULE_B는 보류합니다. DB 백업, 대상 ID 고정, rollback manifest, 격리 dry-run, 단일 트랜잭션 적용, 사후 검증까지 포함하며 학생 공개/allowlist/feature flag 변경은 승인하지 않습니다.`

### 선택지 D 승인 문구

`RULE_A 1,368건은 L3에서 L2로, RULE_B 40건은 L2에서 L1로 연구 DB에 일괄 적용하는 것을 승인합니다. DB 백업, 대상 ID 고정, rollback manifest, 격리 dry-run, 단일 트랜잭션 적용, 사후 검증까지 포함하며 학생 공개/allowlist/feature flag 변경은 승인하지 않습니다.`

## 9. 결론

RULE_A/B는 단순 기술 변경이 아니라 교육정책 성격의 대량 레벨 전환이다.

현재 다빈치 권장:

- L2 공급량 확보가 목표라면 `선택지 C — RULE_A만 적용`.
- L3 공급 안정성과 위험 최소화가 목표라면 `선택지 A — 보류`.
- 둘 다 적용하는 `선택지 D`는 가능하지만, L3 auto_only가 매우 작아지는 점을 명시적으로 감수해야 한다.

다음 단계는 대표님이 A/B/C/D 중 하나를 선택하면, 선택 범위에 맞춘 dry-run 스크립트와 rollback manifest를 먼저 생성하는 것이다.
