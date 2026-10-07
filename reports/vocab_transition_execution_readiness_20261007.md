# vocab_transition_execution_readiness_20261007

- 작성 시각(KST): 2026-10-07 15:06:50 +0900
- 업무명: 어휘 연구 프로젝트 이후 작업 리드 및 실행 준비
- 범위: 첫 정합성 보고서 이후 실제 DB/서비스 변경 전 실행 준비 완료
- 입력 산출물:
  - `C:/Users/aproa/AppData/Local/hermes/cache/documents/doc_370afed4b625_vocab_project_hermes_handoff_20261007.md`
  - `reports/vocab_transition_reconcile_001_report_20261007.md`
  - `reports/vocab_transition_reconcile_001_status.yaml`
- 기준 git HEAD: `6ac220e`
- 승인 경계: 읽기 전용 조사, 보고서 정정, 파일 기반 분석, 격리 DB/코드 테스트, 실행계획 구체화, 관련 보고/상태파일 작성까지만 수행. 실제 RULE_A/B 일괄 적용, 연구 DB 쓰기, 배포, 학생 공개/allowlist/flag 전환, momolib 작업은 미수행.

## 1. 실행 준비 결론

첫 정합성 보고서 기준으로 다음 실행 단계는 서로 독립된 3개 실행 단위로 분리한다.

1. RULE_A 적용: 공식 4등급·현재 L3 1,368건을 L2로 낮추는 정책 후보 적용.
2. RULE_B 적용: 공식 3등급·현재 L2 40건을 L1로 낮추는 정책 후보 적용.
3. L3 보강 연결: 현재 일반 레벨 선택 풀에 연결되지 않은 L3 보강 콘텐츠를 레벨행/선택 조건/화이트리스트 기준으로 연결.

현재 실행 준비 수준:

| 구분 | 준비 상태 | 실제 적용 상태 |
|---|---|---|
| RULE_A | 대상/방향/검증/중단조건 정의 완료 | 미적용 |
| RULE_B | 대상/방향/검증/중단조건 정의 완료 | 미적용 |
| L3 보강 64/67 연결 | 64 안전 연결안과 67 확장 검토안을 분리 완료 | 미적용 |
| DB writes | 0 | 변경 없음 |
| deployment | none | 배포 없음 |
| 학생 공개/allowlist/flag | 변경 금지 유지 | 변경 없음 |

## 2. 현재 확인된 기준선

첫 정합성 보고서의 읽기 전용 조회 결과를 이번 실행계획의 기준선으로 채택한다.

| 항목 | 값 |
|---|---:|
| `vocabulary_contents` | 6,018 |
| `vocabulary_content_levels` | 5,950 |
| `vocabulary_multiformat_items` | 1,747 |
| `vocabulary_official_grade_reference` | 5,950 |
| RULE_A 후보 | 1,368 |
| RULE_B 후보 | 40 |
| RULE_A/B 합계 | 1,408 |
| tier1 최신 판정 | 31 |
| tier1 ∩ RULE_A/B | 0 |
| 이미 적용된 tier1 조정 | 8 |
| L3 보강 DB 콘텐츠 | 68 |
| 현재 연결 대상 매니페스트 | 67 |
| L3 보강 레벨행 | 0 |
| 현재 안전 연결 가정 | 64 콘텐츠, 128 문항 |

중요 정정 사항:

- RULE_A/B는 사람 개별 승인 집합이 아니라 정책 후보 집합이다.
- RULE_A/B와 tier1 31건은 교집합 0이다.
- tier1 조정 8건은 이미 적용된 별도 작업이며 재적용 대상으로 넣지 않는다.
- L3 보강 67건 레벨행 0은 실제 누락 상태로 확인되었지만, INSERT는 아직 승인되지 않았다.
- 보강 source_version만 넓히면 벨기에처럼 매니페스트 제외 항목이 혼입될 수 있으므로 명시 whitelist가 필요하다.

## 3. 독립 실행 단위 1 — RULE_A

### a. 목적

공식 4등급 기준으로 현재 L3에 남아 있는 1,368건을 내부 정책에 따라 L2로 조정한다. L3 공급량을 줄이는 효과가 크므로, 교육정책 승인 없이는 적용하지 않는다.

### b. 적용 대상과 제외 대상

적용 대상:

- `vocabulary_official_grade_reference.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'`
- 공식 등급 4
- 제안 레벨 L2
- 현재 기준 스냅샷 L3
- 첫 정합성 보고서 기준 1,368건

제외 대상:

- RULE_B 40건
- tier1 최신 판정 31건 전체
- 이미 적용된 tier1 조정 8건
- 공식 매칭 불명확/다중후보/표제어만 일치/매칭 없음 항목
- 전문 의미 예외 집합
- L3 보강 batch1/batch2 신규 콘텐츠
- 공개 플래그, 문항, 매니페스트, 사람 판정 테이블

### c. 변경될 가능성이 있는 테이블/컬럼/파일

실제 적용 승인 시 변경 가능성이 있는 항목:

- 연구 DB `vocabulary_content_levels`
  - `level` 또는 현재 서비스 스키마의 레벨 컬럼: L3 -> L2
  - `level_status`: 기존 정책을 보존하거나 승인 문구에 명시된 값만 사용
  - `updated_at`, `updated_by`, `note`류 감사 컬럼이 있으면 배치 식별자 기록
- 연구 DB `vocabulary_official_grade_reference`
  - 승인 후 상태 전환이 필요하다고 별도 승인된 경우에만 `review_status`류 컬럼 변경 가능
- 로컬/서버 파일
  - 적용 SQL 또는 Python 스크립트
  - dry-run 결과 JSON/CSV
  - 적용 보고서/상태파일

변경하면 안 되는 항목:

- `vocabulary_multiformat_items` 문항 본문/정답/활성 상태
- 학생 공개 플래그(`public_ready`, `student_exposure` 등)
- 사람 판정 append-only 이력
- 기존 매니페스트 파일
- momolib 관련 파일/DB

### d. 사전 검증 쿼리/테스트

실제 적용 직전에는 같은 DB 파일에 대해 읽기 전용으로 아래를 확인한다. 컬럼명은 현재 스키마 조회 후 정확한 명칭으로 치환한다.

```sql
PRAGMA integrity_check;

-- RULE_A 대상 수가 1,368인지 재확인
SELECT COUNT(*)
FROM vocabulary_official_grade_reference r
JOIN vocabulary_content_levels l ON l.content_id = r.content_id
WHERE r.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'
  AND r.official_grade = 4
  AND r.proposed_level = 2
  AND l.level = 3;

-- RULE_A/B 외 패턴이 섞이지 않았는지 확인
SELECT r.official_grade, r.proposed_level, l.level, COUNT(*)
FROM vocabulary_official_grade_reference r
JOIN vocabulary_content_levels l ON l.content_id = r.content_id
WHERE r.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'
GROUP BY r.official_grade, r.proposed_level, l.level
ORDER BY 1,2,3;

-- tier1 최신 대표 31건과 교집합 0 확인
-- 실제 쿼리는 서비스의 최신 판정 대표 선택 로직과 동일하게 작성한다.

-- 공개 플래그가 작업 전 0/비공개 상태인지 확인
SELECT COUNT(*) AS exposed_count
FROM vocabulary_contents c
JOIN vocabulary_official_grade_reference r ON r.content_id = c.content_id
JOIN vocabulary_content_levels l ON l.content_id = c.content_id
WHERE r.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'
  AND r.official_grade = 4
  AND r.proposed_level = 2
  AND l.level = 3
  AND (c.public_ready <> 0 OR c.student_exposure <> 0);
```

테스트:

- 적용 스크립트는 먼저 격리 DB 복사본에서 실행한다.
- dry-run 출력 target_id 목록과 실제 UPDATE 대상 목록의 SHA-256을 비교한다.
- UPDATE 전후 영향 건수가 정확히 1,368인지 확인한다.
- 앱 선택 함수 또는 동일 SQL 시뮬레이션으로 L1/L2/L3 all_candidates/auto_only 공급량을 재계산한다.

### e. 백업·롤백 원칙

- 실제 적용 전 DB 파일 전체 백업을 생성하고 경로·크기·SHA-256을 기록한다.
- 대상 ID, 기존 level/status 값, 신규 level/status 값을 별도 rollback manifest로 저장한다.
- 적용은 단일 트랜잭션으로 실행한다.
- 롤백은 전체 DB 복구가 아니라 우선 대상 manifest 기반 역방향 UPDATE를 원칙으로 한다.
- 단, 적용 후 다른 변경이 없다는 것이 확인된 긴급 상황에서만 전체 DB 백업 복원을 고려한다.

### f. 실제 적용 시 필요한 대표 승인 문구

`RULE_A 1,368건(공식 4등급·현재 L3·제안 L2)을 연구 DB에서 L2로 일괄 변경하는 것을 승인합니다. DB 백업, 대상 ID 고정, dry-run, 단일 트랜잭션 적용, 사후 검증까지 포함하며 학생 공개/allowlist/feature flag/배포는 승인하지 않습니다.`

### g. 적용 후 검증 방법

- UPDATE 영향 행 수 = 1,368 확인.
- RULE_A 조건으로 남은 대상 0 확인.
- 같은 content_id들이 L2에 존재하고 L3에 남아 있지 않은지 확인.
- RULE_B, tier1, 보강 콘텐츠가 변경되지 않았는지 anti-join으로 확인.
- 공개 플래그와 문항 행 수가 변경되지 않았는지 전후 카운트 비교.
- 공급량 재계산: L2 증가, L3 감소가 예상표와 일치하는지 확인.
- 상태파일에 DB writes 수, 백업 경로, 적용 시각, 검증 결과 기록.

### h. 리스크와 중단 조건

리스크:

- L3 all_candidates 공급량이 크게 감소한다.
- 공식 등급과 서비스 레벨 대응 정책에 대한 교육적 이견이 있을 수 있다.
- 조인 기준이 표제어 중심이면 의미 불일치 항목이 포함될 위험이 있다.

중단 조건:

- 대상 수가 1,368과 다름.
- tier1 또는 예외 집합과 교집합이 발견됨.
- 공개 플래그가 켜진 대상이 발견됨.
- UPDATE 영향 행 수가 dry-run 대상 수와 다름.
- integrity_check 실패 또는 DB 경로 불일치.

## 4. 독립 실행 단위 2 — RULE_B

### a. 목적

공식 3등급 기준으로 현재 L2에 남아 있는 40건을 내부 정책에 따라 L1로 조정한다. RULE_A와 방향·대상·효과가 다르므로 별도 승인/적용 단위로 둔다.

### b. 적용 대상과 제외 대상

적용 대상:

- `vocabulary_official_grade_reference.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'`
- 공식 등급 3
- 제안 레벨 L1
- 현재 기준 스냅샷 L2
- 첫 정합성 보고서 기준 40건

제외 대상:

- RULE_A 1,368건
- tier1 최신 판정 31건 전체
- 이미 적용된 tier1 조정 8건
- 공식 등급 4 이상 후보
- 공식 매칭 불명확/예외/전문 의미 항목
- L3 보강 콘텐츠
- 공개 플래그, 문항, 매니페스트, 사람 판정 테이블

### c. 변경될 가능성이 있는 테이블/컬럼/파일

실제 승인 시 변경 가능성이 있는 항목:

- 연구 DB `vocabulary_content_levels`
  - 레벨 컬럼: L2 -> L1
  - 감사 컬럼이 있으면 배치 식별자 기록
- 선택적으로 승인된 경우에만 `vocabulary_official_grade_reference.review_status` 상태 전환
- 적용 스크립트, dry-run 산출물, 적용 보고서/상태파일

변경 금지:

- 문항 row 및 item_id
- 공개 플래그
- 사람 판정 이력
- 매니페스트
- 배포 설정

### d. 사전 검증 쿼리/테스트

```sql
PRAGMA integrity_check;

SELECT COUNT(*)
FROM vocabulary_official_grade_reference r
JOIN vocabulary_content_levels l ON l.content_id = r.content_id
WHERE r.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'
  AND r.official_grade = 3
  AND r.proposed_level = 1
  AND l.level = 2;

SELECT COUNT(*) AS exposed_count
FROM vocabulary_contents c
JOIN vocabulary_official_grade_reference r ON r.content_id = c.content_id
JOIN vocabulary_content_levels l ON l.content_id = c.content_id
WHERE r.review_status = 'RULE_PROPOSED_PENDING_APPROVAL'
  AND r.official_grade = 3
  AND r.proposed_level = 1
  AND l.level = 2
  AND (c.public_ready <> 0 OR c.student_exposure <> 0);
```

테스트:

- 격리 DB 복사본에서 적용 리허설.
- 대상 ID manifest 고정 후 실제 UPDATE 대상과 비교.
- RULE_A 미적용 상태와 RULE_A 적용 후 상태 모두에서 독립적으로 실행 가능한지 멱등성 확인.
- L1/L2 공급량 재계산.

### e. 백업·롤백 원칙

- DB 전체 백업 + RULE_B 대상 rollback manifest를 둘 다 남긴다.
- RULE_A와 같은 날 적용하더라도 manifest와 작업 로그를 분리한다.
- 단일 트랜잭션으로 40건만 변경한다.
- 롤백은 RULE_B manifest 기반 L1 -> 기존 L2 복구를 우선한다.

### f. 실제 적용 시 필요한 대표 승인 문구

`RULE_B 40건(공식 3등급·현재 L2·제안 L1)을 연구 DB에서 L1로 일괄 변경하는 것을 승인합니다. DB 백업, 대상 ID 고정, dry-run, 단일 트랜잭션 적용, 사후 검증까지 포함하며 RULE_A 적용 여부와 독립 실행 단위로 관리하고 학생 공개/allowlist/feature flag/배포는 승인하지 않습니다.`

### g. 적용 후 검증 방법

- UPDATE 영향 행 수 = 40 확인.
- RULE_B 조건으로 남은 대상 0 확인.
- RULE_A 후보 수가 변하지 않았는지 확인.
- tier1/보강/문항/공개 플래그가 변경되지 않았는지 확인.
- L1/L2 공급량 변화가 예상과 일치하는지 확인.
- 상태파일에 독립 실행 결과 기록.

### h. 리스크와 중단 조건

리스크:

- 40건은 작지만 L1 난이도 체감에 직접 영향을 준다.
- RULE_A와 같이 실행하면 원인별 공급량 변화 추적이 흐려질 수 있다.

중단 조건:

- 대상 수가 40과 다름.
- RULE_A 또는 tier1과 교집합이 발견됨.
- UPDATE 영향 행 수가 40이 아님.
- 공개 플래그가 켜진 대상이 발견됨.
- 스키마/컬럼명이 계획과 다름.

## 5. 독립 실행 단위 3 — L3 보강 64/67건 연결

### a. 목적

L3 보강 batch1/batch2 콘텐츠를 일반 L3 관리자 출제 후보로 연결한다. 현재 DB에는 보강 콘텐츠가 존재하지만 `vocabulary_content_levels` 레벨행이 0이라 일반 레벨 선택 풀에는 들어오지 않는다.

### b. 적용 대상과 제외 대상

우선 안전 적용 대상:

- 1차 v2 개별승인 29 콘텐츠, 58 문항
- 2차 개별승인 35 콘텐츠, 70 문항
- 합계 64 콘텐츠, 128 문항
- 현재 매니페스트 및 최신 publish review 기준으로 승인·신선함이 확인된 집합

확장 검토 대상:

- 2차 미개별승인 3 콘텐츠, 6 문항: 수시, 숙련, 순서도
- 위험 기반 배치 검수는 완료되었으나 개별 publish review는 없음
- 대표 승인 또는 추가 개별 검토 후 최대 67 콘텐츠, 134 문항까지 연결 가능

제외 대상:

- 벨기에 1 콘텐츠, 2 문항: DB와 publish review에는 남아 있으나 현재 개정 매니페스트에서 제외
- 비활성 v1 문항 58개
- L6 미착수분, 공식 5등급 미검수 16,800건
- 기존 2.1.29 일반 문항 row
- 학생 공개/allowlist/feature flag

### c. 변경될 가능성이 있는 테이블/컬럼/파일

실제 승인 시 변경 가능성이 있는 항목:

- 연구 DB `vocabulary_content_levels`
  - 대상 content_id에 신규 L3 레벨행 INSERT
  - `level_status`: 대표 결정에 따라 `REVIEW_BOUNDARY` 또는 `PROVISIONAL_AUTO`
  - 정책/출처/메모/감사 컬럼이 있으면 batch id 기록
- 일반 레벨 선택 로직 또는 매니페스트 구성 파일
  - 보강 source_version 포함 여부
  - 명시 whitelist 적용 여부
- 새 manifest/whitelist 파일
  - 64 안전 연결 대상 또는 67 확장 대상의 content_id/item_id 고정
- 보고서/상태파일/dry-run 결과

변경 금지:

- 기존 item_id 수정 또는 source_version 바꿔치기
- 비활성 v1 문항 재활성화
- 학생 공개 플래그
- 기존 응답/세션 데이터
- momolib 이식

### d. 사전 검증 쿼리/테스트

```sql
PRAGMA integrity_check;

-- 보강 source_version별 콘텐츠/문항/활성 상태 확인
SELECT source_version,
       COUNT(DISTINCT content_id) AS contents,
       COUNT(*) AS items,
       SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_items
FROM vocabulary_multiformat_items
WHERE source_version IN ('nikl_grade5_l3_batch1_v1', 'nikl_grade5_l3_batch2_v1')
GROUP BY source_version;

-- 보강 콘텐츠 레벨행 0 재확인
SELECT COUNT(*)
FROM vocabulary_content_levels l
WHERE l.content_id IN (
  SELECT DISTINCT content_id
  FROM vocabulary_multiformat_items
  WHERE source_version IN ('nikl_grade5_l3_batch1_v1', 'nikl_grade5_l3_batch2_v1')
);

-- whitelist 대상과 DB source_version 대상의 차집합 확인
-- 실제 쿼리는 고정 manifest table/temp table 또는 VALUES 목록으로 수행한다.

-- 공개 플래그가 켜진 보강 콘텐츠가 없는지 확인
SELECT COUNT(*) AS exposed_count
FROM vocabulary_contents
WHERE content_id IN (...whitelist...)
  AND (public_ready <> 0 OR student_exposure <> 0);
```

테스트:

- 64 안전 whitelist와 67 확장 whitelist를 파일로 분리한다.
- 격리 DB 복사본에서 INSERT 리허설을 수행한다.
- 중복 INSERT 방지: 이미 레벨행이 있으면 실패/중단 또는 멱등 skip 정책을 명확히 한다.
- 선택 함수 리허설:
  - 현재 + 보강 64 연결
  - RULE_A/B 후 + 보강 64 연결
  - 필요 시 67 연결
- `level_status=REVIEW_BOUNDARY`와 `PROVISIONAL_AUTO`별 auto_only 포함 차이를 별도 표로 검증한다.
- 과거 응답 조회가 item_id FK로 보존되는지 코드 경로를 확인한다.

### e. 백업·롤백 원칙

- DB 전체 백업을 만든다.
- INSERT 대상 content_id, inserted row id, level_status, source/batch id를 manifest로 저장한다.
- 레벨행 INSERT는 단일 트랜잭션으로 수행한다.
- 롤백은 해당 batch id 또는 manifest에 해당하는 신규 level rows DELETE를 원칙으로 한다.
- 기존 레벨행이 있는 content_id가 발견되면 전체 중단하고 원인 분석 후 재승인 받는다.
- 선택 로직/manifest 파일 변경은 git diff로 분리하고 DB 변경과 같은 보고서에 연결한다.

### f. 실제 적용 시 필요한 대표 승인 문구

64건 안전 연결 승인 문구:

`L3 보강 승인분 64콘텐츠·128문항을 일반 L3 관리자 출제 후보로 연결하기 위해 연구 DB에 L3 레벨행을 추가하고, source_version 확장이 아닌 명시 whitelist 기준으로 선택되도록 준비/적용하는 것을 승인합니다. level_status는 [REVIEW_BOUNDARY 또는 PROVISIONAL_AUTO 중 선택]로 하며, DB 백업, 대상 ID 고정, 격리 리허설, 단일 트랜잭션, 사후 검증까지 포함합니다. 학생 공개/allowlist/feature flag/배포는 승인하지 않습니다.`

67건 확장 승인 문구:

`L3 보강 64콘텐츠에 더해 batch2 미개별승인 3콘텐츠(수시·숙련·순서도)를 [추가 개별검토 후 또는 현 배치검수 완료 근거로] 포함하여 총 67콘텐츠·134문항을 연결하는 것을 승인합니다. 벨기에는 제외하며, level_status는 [REVIEW_BOUNDARY 또는 PROVISIONAL_AUTO 중 선택]로 합니다. 학생 공개/allowlist/feature flag/배포는 승인하지 않습니다.`

### g. 적용 후 검증 방법

- 신규 레벨행 수 = 승인 대상 수(64 또는 67) 확인.
- 벨기에 제외 확인.
- 비활성 v1 문항이 일반 선택 풀에 포함되지 않는지 확인.
- L3 공급량 재계산:
  - 현재 + 보강 64 연결: L3 all_candidates 149 어휘 / 444 문항 예상
  - RULE_A/B 후 + 보강 64 연결: L3 all_candidates 75 어휘 / 170 문항 예상
  - auto_only는 `level_status` 선택에 따라 달라짐
- 기존 응답 조회가 기존 item_id를 그대로 읽는지 확인.
- 공개 플래그, allowlist, feature flag 변화 0 확인.

### h. 리스크와 중단 조건

리스크:

- source_version만 확장하면 벨기에 또는 미승인 항목이 혼입될 수 있다.
- `level_status`를 공급량 확보 목적으로 정하면 검수 의미가 왜곡된다.
- 기존 전용 검수/응시 경로와 일반 레벨모드 경로가 같은 항목을 다른 기준으로 보여줄 수 있다.
- 비활성 v1 문항을 실수로 다시 노출할 수 있다.

중단 조건:

- whitelist와 실제 DB content_id/item_id가 불일치.
- 승인 대상 수가 64 또는 67과 다름.
- 벨기에가 포함됨.
- 기존 레벨행이 이미 존재함.
- 비활성 문항이 선택 SQL에 포함됨.
- 공개 플래그 변경이 필요해지는 경우.

## 6. 상호 독립성과 권장 실행 순서

독립성:

- RULE_A와 RULE_B는 대상 조건과 레벨 이동 방향이 다르며 서로 독립적으로 실행 가능하다.
- L3 보강 연결은 신규 보강 콘텐츠의 레벨행/선택 조건 문제이며 RULE_A/B의 기존 공식 참조 후보 변경과 별개다.
- 단, 공급량 검증표는 조합별로 달라지므로 사후 검증은 각 실행 단위별과 통합 시나리오별로 모두 수행해야 한다.

권장 순서:

1. 대표 의사결정: RULE_A/B 적용 범위와 보강 연결 범위/level_status 선택.
2. 승인된 단위별 target manifest 생성 및 SHA-256 기록.
3. 격리 DB 리허설.
4. 실제 DB 백업.
5. 승인된 단위만 단일 트랜잭션 적용.
6. 사후 검증 보고서 작성.
7. 배포 또는 학생 공개가 필요하면 별도 승인 단계로 분리.

## 7. 지금 대표 승인 없이 완료한 준비 작업

- 인계서의 승인 경계를 재확인했다.
- 첫 정합성 보고서의 기준선과 정정사항을 실행 계획 기준으로 고정했다.
- RULE_A, RULE_B, L3 보강 연결을 서로 독립 실행 단위로 분리했다.
- 각 실행 단위별 목적, 대상/제외, 변경 가능 테이블/컬럼/파일, 사전 검증, 백업/롤백, 승인 문구, 사후 검증, 리스크/중단 조건을 문서화했다.
- 승인 요청안을 3개 이하 옵션으로 정리했다.
- DB writes 0, deployment none 원칙을 유지했다.
- 기존 사람 판정, DB 레벨, 문항, 매니페스트, 공개 플래그를 변경하지 않았다.

## 8. 남은 결정·실행 전 차단점

대표 결정 필요:

1. RULE_A/B를 적용할지, 각각 분리할지, 보류할지.
2. L3 보강 연결을 64 안전 연결로 할지, 67 확장 연결로 할지, 보류할지.
3. 보강 `level_status`를 `REVIEW_BOUNDARY`로 둘지 `PROVISIONAL_AUTO`로 둘지.

실행 전 차단점:

- 실제 DB 백업 생성 승인 필요.
- 실제 UPDATE/INSERT 승인 필요.
- 적용 스크립트 실행 권한과 DB 경로 재확인 필요.
- source_version 확장이 아니라 whitelist 기반 연결을 구현할지 코드 변경 범위 승인 필요.
- 학생 공개/배포는 별도 승인 없이는 계속 금지.

## 9. 이번 작업에서 변경한 파일

이번 후속 작업으로 생성 예정/생성된 파일:

- `reports/vocab_transition_execution_readiness_20261007.md`
- `reports/vocab_transition_execution_readiness_status.yaml`
- `reports/vocab_transition_approval_options_20261007.md`

기존 첫 작업 산출물은 읽기 기준으로 사용했으며 이번 작업에서 수정하지 않았다.

## 10. 준수 확인

- DB writes: 0
- deployment: none
- 학생 공개/allowlist/feature flag 변경: none
- 사람 판정 변경: none
- DB 레벨 변경: none
- 문항 변경: none
- 매니페스트 변경: none
- momolib 작업: none
