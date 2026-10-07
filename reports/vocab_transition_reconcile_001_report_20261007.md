# vocab-transition-reconcile-001 보고서

- 작성 시각(KST): 2026-10-07 15:02:43 +0900
- 범위: 6ac220e RULE_A/B 전환 보고 정합성 재확인 및 정정
- 원칙: 읽기 전용. DB 레벨·문항·매니페스트·공개 플래그·사람 판정·momolib·배포 변경 없음.
- 조회 DB: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- DB 접근 방식: SQLite `file:...?mode=ro` + `PRAGMA query_only=ON`; 없는 경로 자동 생성 미사용.

## 1. 기준선

| 항목 | 값 |
|---|---:|
| DB 파일 크기 | 17,223,680 bytes |
| DB integrity_check | ok |
| vocabulary_contents | 6,018 |
| vocabulary_content_levels | 5,950 |
| vocabulary_multiformat_items | 1,747 |
| vocabulary_official_grade_reference | 5,950 |
| vocabulary_official_grade_judgments | 32 |
| vocabulary_grade5_candidate_judgments | 212 |

주의: `vocabulary_contents=6,018`은 5,950 기본 콘텐츠 + L3 보강 68 DB 콘텐츠행이다. 그러나 현재 L3 보강 “연결 가정”에서 쓰는 배치 매니페스트는 1차 29콘텐츠·58문항 + 2차 38콘텐츠·76문항 = 67콘텐츠·134문항이다. 1차 DB에는 벨기에 1콘텐츠·2문항이 남아 있으나 현재 개정 매니페스트에서는 제외된 상태로 구분해야 한다.

## 2. RULE_A/B 대상 집합 정정

| 구분 | 조건 | 건수 | 제안 방향 | 상태 |
|---|---|---:|---|---|
| RULE_A | review_status=`RULE_PROPOSED_PENDING_APPROVAL`, 공식4등급, proposed L2, 현재 스냅샷 L3 | 1,368 | L3 -> L2 | 정책 후보, 미적용 |
| RULE_B | review_status=`RULE_PROPOSED_PENDING_APPROVAL`, 공식3등급, proposed L1, 현재 스냅샷 L2 | 40 | L2 -> L1 | 정책 후보, 미적용 |
| 기타 패턴 | 같은 review_status이나 위 조건 외 | 0 | - | 없음 |
| 합계 |  | 1,408 |  | 미적용 |

정정 결론:

- 6ac220e 보고의 “1,408건 전부 L3->L2” 표현은 부정확하다. 정확히는 RULE_A 1,368건 L3->L2 + RULE_B 40건 L2->L1이다.
- RULE_A/B 1,408건은 사람 개별 승인 집합이 아니라 `vocabulary_official_grade_reference.review_status='RULE_PROPOSED_PENDING_APPROVAL'`인 정책 후보 집합이다.
- 실제 레벨 변경은 아직 승인·적용되지 않았다. tier1 조정 8건과 섞으면 안 된다.

## 3. tier1 31건과 RULE_A/B 교집합

| 항목 | 결과 |
|---|---:|
| tier1 최신 대표 판정 content_id | 31 |
| 최신 판정: 현재 유지 | 23 |
| 최신 판정: 기본 레벨 조정 | 8 |
| tier1 31건 ∩ RULE_A/B 1,408건 | 0 |

정정 결론:

- 6ac220e 보고의 “1,408건 중 사람 승인 31건, 8건만 적용 가능” 취지의 설명은 정합하지 않다.
- tier1 31건은 RULE_A/B 자동 후보와 교집합이 0이다.
- tier1 “기본 레벨 조정” 8건은 이미 별도 작업으로 적용되어 현재 DB 레벨이 제안 레벨과 일치한다.
- 이 8건의 `public_ready`와 `student_exposure`는 모두 0으로 유지되어 공개 승인과 혼동되지 않았다.

### tier1 조정 8건 현재 DB 확인

| content_id | 표제어 | 목표 | 현재 DB 레벨 | level_status | public_ready | student_exposure |
|---|---|---:|---:|---|---:|---:|
| SC_V19102_B102_042 | 잇몸 | L1 | 1 | REVIEW_BOUNDARY | 0 | 0 |
| SC_V19107_B107_041 | 정다각형 | L2 | 2 | PROVISIONAL_AUTO | 0 | 0 |
| SC_V19135_B135_034 | 효과음 | L2 | 2 | PROVISIONAL_AUTO | 0 | 0 |
| SC_V1923_B023_045 | 공배수 | L2 | 2 | PROVISIONAL_AUTO | 0 | 0 |
| SC_V1939_B039_009 | 단옷날 | L2 | 2 | PROVISIONAL_AUTO | 0 | 0 |
| SC_V1985_B085_015 | 아이디 | L1 | 1 | REVIEW_BOUNDARY | 0 | 0 |
| SC_V1987_B087_032 | 약분하다 | L2 | 2 | PROVISIONAL_AUTO | 0 | 0 |
| SC_V1992_B092_009 | 예금되다 | L2 | 2 | REVIEW_BOUNDARY | 0 | 0 |

## 4. L3 보강 레벨행 0 주장 확인

| 기준 | 콘텐츠 | 활성 문항 | 비활성 문항 | 레벨행(level_policy_v0.1) | 공개 플래그 |
|---|---:|---:|---:|---:|---|
| DB source_version=`nikl_grade5_l3_batch1_v1` | 30 | 60 | 58 | 0 | 전부 0 |
| DB source_version=`nikl_grade5_l3_batch2_v1` | 38 | 76 | 0 | 0 | 전부 0 |
| DB 합계 | 68 | 136 | 58 | 0 | 전부 0 |
| 현재 연결 대상 매니페스트 기준 | 67 | 134 | - | 0 | 전부 0 |
| 개별 공개검토 승인(APPROVED_CANDIDATE) | 65 | 130 | - | 0 | 전부 0 |
| 매니페스트 연결 가정의 승인분 | 64 | 128 | - | 0 | 전부 0 |

세부 정정:

- “보강 67건 레벨행 0”이라는 관찰은 현재 연결 대상 매니페스트 기준으로는 맞다. 실제 DB의 보강 source_version 콘텐츠 전체로 보면 68건 모두 레벨행이 0이다.
- 이는 다른 DB 조회나 조인 오류가 아니라 실제로 `vocabulary_content_levels`에 보강 콘텐츠 레벨행이 아직 INSERT되지 않은 상태다.
- 단, “사람 L3 판정”과 “공개검토 APPROVED_CANDIDATE”와 “매니페스트 연결 대상”은 서로 다르다.
  - grade5 후보 레벨 판정은 68건 모두 최신 `L3`다.
  - publish review 최신 `APPROVED_CANDIDATE`는 65건이다.
  - batch2의 수시·숙련·순서도 3건은 publish review가 없다.
  - 현재 연결 가정은 1차 개정 매니페스트 29건 + 2차 승인 35건 = 64건·128문항을 쓰는 것이 안전하다.
- 벨기에는 DB와 publish review에는 승인된 1차 콘텐츠로 남아 있으나, 현재 개정 매니페스트에서는 제외된 항목으로 취급해야 한다.

## 5. 동일 조건 공급량 비교표

조건:

- 실제 일반 레벨 선택 함수와 같은 핵심 조건을 SQL로 재현했다.
- 일반 풀은 `vocabulary_multiformat_items.source_version='2.1.29'`, 활성 문항, CROSSWORD 제외.
- all_candidates는 `PROVISIONAL_AUTO + REVIEW_BOUNDARY`, auto_only는 `PROVISIONAL_AUTO`만 포함한다.
- MATCH_WORD_MEANING은 포함된 모든 content_id가 선택 레벨에 있어야 포함했다.
- 아래 RULE_A/B 적용 후 수치는 실제 DB 변경이 아니라 읽기 전용 시뮬레이션이다.

| 레벨 | 모드 | 단계 | 레벨행 | 문항 보유 고유어휘 | 문항 수 | 유형별 |
|---|---|---|---:|---:|---:|---|
| L1 | all_candidates | 현재 | 1,637 | 89 | 324 | MC89, WFD89, CM89, CC57 |
| L1 | all_candidates | RULE_A/B 후 | 1,677 | 91 | 332 | MC91, WFD91, CM91, CC59 |
| L1 | auto_only | 현재 | 1,285 | 67 | 244 | MC67, WFD67, CM67, CC43 |
| L1 | auto_only | RULE_A/B 후 | 1,292 | 67 | 244 | MC67, WFD67, CM67, CC43 |
| L2 | all_candidates | 현재 | 1,625 | 92 | 346 | MC92, WFD92, CM92, CC70 |
| L2 | all_candidates | RULE_A/B 후 | 2,953 | 164 | 617 | MC164, WFD163, CM164, CC120, MWM6 |
| L2 | auto_only | 현재 | 26 | 6 | 23 | MC6, WFD6, CM6, CC5 |
| L2 | auto_only | RULE_A/B 후 | 1,334 | 78 | 289 | MC78, WFD77, CM78, CC56 |
| L3 | all_candidates | 현재 | 1,819 | 85 | 316 | MC85, WFD84, CM85, CC61, MWM1 |
| L3 | all_candidates | RULE_A/B 후 | 451 | 11 | 42 | MC11, WFD11, CM11, CC9 |
| L3 | auto_only | 현재 | 1,677 | 80 | 297 | MC80, WFD79, CM80, CC58 |
| L3 | auto_only | RULE_A/B 후 | 362 | 8 | 31 | MC8, WFD8, CM8, CC7 |

6ac220e의 CSV와 차이가 나는 부분:

- 이번 재조회는 tier1 8건이 이미 실제 DB에 적용된 이후의 현재값을 기준으로 했다.
- 그래서 auto_only L1/L2의 RULE_A/B 후 수치가 6ac220e 보고의 파일 기반 계산(예: L1 69/252, L2 80/296)과 다르다.
- 6ac220e 계산은 “RULE_A/B + tier1 8건도 가상 적용”을 섞은 이전 파일 기반 시뮬레이션의 흔적이 포함된 것으로 보인다. 현재 DB 기준 정합 보고에서는 tier1 8건을 이미 적용된 기준선으로 취급해야 한다.

### 보강 연결 가정 포함 비교

| 시나리오 | L3 all_candidates 문항보유/문항수 | L3 auto_only 문항보유/문항수 | 비고 |
|---|---:|---:|---|
| 현재 일반 풀 | 85 / 316 | 80 / 297 | 보강 source_version 제외 |
| RULE_A/B 후 일반 풀 | 11 / 42 | 8 / 31 | 실제 적용 아님 |
| 현재 + 보강 64 연결 | 149 / 444 | 정책값에 따라 80/297 또는 144/425 | 보강 level_status를 PROVISIONAL_AUTO로 둘 때만 auto_only 반영 |
| RULE_A/B 후 + 보강 64 연결 | 75 / 170 | 정책값에 따라 8/31 또는 72/159 | 보강을 REVIEW_BOUNDARY로 두면 auto_only 미반영 |

보강 64 연결은 현재 매니페스트·개별 공개검토 승인 기준이다. DB의 source_version만 넓히면 벨기에까지 포함되어 65건이 들어갈 수 있으므로, 실제 구현 시 매니페스트 화이트리스트 또는 명시 대상 집합을 같이 고정해야 한다.

## 6. 전환안 정리

실행 준비 절차는 다음처럼 분리해야 한다.

1. RULE_A/B 정책 결정
   - RULE_A 1,368과 RULE_B 40을 독립적으로 승인할지 결정한다.
   - tier1 31건과 섞지 않는다. tier1 8건은 이미 적용 완료, tier1 23건은 현재 유지다.
2. 보강 연결 결정
   - 보강 레벨행을 INSERT할지 여부, `level_status`를 REVIEW_BOUNDARY로 둘지 PROVISIONAL_AUTO로 둘지 결정이 필요하다.
   - 공급량 확보를 위해 `level_status`를 정하지 말고, 판정 근거와 노출 정책 기준으로 정해야 한다.
3. 매니페스트/선택 조건 정합성
   - 보강 source_version을 일반 레벨 선택에 추가하더라도 매니페스트 또는 명시 대상 집합을 고정해야 벨기에 같은 DB 잔존·매니페스트 제외 항목이 섞이지 않는다.
   - 기존 batch1/batch2 전용 검수·응시 경로와 일반 레벨모드 경로를 분리 유지해야 한다.
4. 과거 세션 영향
   - 기존 응답 조회는 item_id FK를 직접 읽으므로, 레벨 숫자 변경과 새 매니페스트 파일 생성은 과거 응답의 item_id 자체를 바꾸지 않는다.
   - 단, 기존 item row를 수정하거나 source_version을 바꿔치기하면 과거 세션 의미가 변할 수 있으므로 금지한다.
5. 실제 적용 시 승인 필요
   - RULE_A/B UPDATE, 보강 레벨행 INSERT, 일반 선택 함수 source_version 확장, 새 매니페스트 생성은 모두 별도 승인 후 백업·dry-run·단일 트랜잭션·사후 검증으로 진행해야 한다.

## 7. 남은 결정사항

| 결정 | 선택지 | 영향 |
|---|---|---|
| RULE_A/B 적용 범위 | RULE_A만 / RULE_B만 / 둘 다 / 보류 | L1/L2/L3 공급량과 교육 정책에 직접 영향 |
| RULE_A/B 승인 방식 | 정책 일괄 승인 / 위험 기반 표본 검수 후 승인 / 전수 개별 검수 | 사람 검수 비용과 적용 근거 수준 결정 |
| 보강 레벨행 생성 여부 | 보강 64만 / 67 전체 / DB 68 전체 / 보류 | 일반 L3 선택 가능 여부 결정 |
| 보강 level_status | REVIEW_BOUNDARY / PROVISIONAL_AUTO | auto_only에 보강을 포함할지 결정 |
| 벨기에 처리 | 매니페스트 제외 유지 / 재포함 재검토 | source_version 기반 선택 확장 시 혼입 방지 필요 |
| batch2 미개별검토 3건 | 현 상태 유지 / 별도 검토 후 추가 연결 | 현재 연결 64 -> 최대 67로 증가 가능 |
| 구현 방식 | 매니페스트 화이트리스트 병행 / source_version만 확장 | 안전성 차이 큼. source_version만 확장은 비권장 |

## 8. 생성 파일 및 읽기 전용 준수

생성 파일:

- `reports/vocab_transition_reconcile_001_report_20261007.md`
- `reports/vocab_transition_reconcile_001_status.yaml`

검증 근거 파일:

- `C:/Users/aproa/AppData/Local/hermes/profiles/davinci/cache/scratch/vocab_transition_remote_readonly_20261007.json`
- `C:/Users/aproa/AppData/Local/hermes/profiles/davinci/cache/scratch/vocab_publish_readonly_20261007.json`

준수 확인:

- DB는 `mode=ro` 및 `PRAGMA query_only=ON`으로 조회했다.
- DB INSERT/UPDATE/DELETE/DDL 없음.
- 배포 없음.
- 학생 공개/allowlist/feature flag 변경 없음.
- 문항, 매니페스트, 사람 판정, 공개 플래그 변경 없음.
- momolib, L6 미착수분, v4 분류기, 무관 워크시트 작업 없음.

참고: 원격 `/tmp`에 조회용 임시 Python 파일 2개를 복사해 실행했으나 DB·서비스·저장소 상태는 변경하지 않았다. 이 단일 실행 환경에서 `/tmp` 삭제 명령은 승인 차단되어 수행하지 못했다.
