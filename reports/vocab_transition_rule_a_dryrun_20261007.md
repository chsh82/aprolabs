# RULE_A dry-run 및 rollback manifest — 2026-10-07

## 범위

대표님 지시에 따라 Davinci 권장안인 `선택지 C — RULE_A 1,368건만 적용`의 실제 DB 적용 전 단계까지 진행했다.

이번 단계에서 수행한 것:

- live 연구 DB 읽기 전용 사전 검증
- RULE_A 대상 ID 고정
- rollback manifest CSV 생성
- 연구 DB 복사본에서 dry-run UPDATE 실행
- dry-run DB에서 앱 선택 함수 기준 availability 재계산

수행하지 않은 것:

- live DB UPDATE/INSERT/DELETE 없음
- 배포 없음
- 서비스 재시작 없음
- 학생 공개/allowlist/feature flag 변경 없음
- RULE_B 변경 없음

## 기준 환경

- 서버 repo: `/home/chsh82/aprolabs`
- 서버 HEAD/origin/main: `6b33147`
- live DB: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- live DB SHA-256: `967154ed125ac12a94675541c97a8bcd9fa2c60454a6ada2f0ac2f9776b59d1a`
- live DB size: `17281024` bytes
- dry-run DB copy: `/home/chsh82/aprolabs/tmp/vocabulary_quiz_research_rule_a_dryrun_20261007-224108.db`

## 사전 검증 결과

| 항목 | 결과 |
|---|---:|
| DB integrity_check | ok |
| RULE_A 후보 | 1,368 |
| RULE_B 후보 | 40 |
| 기타 pending 패턴 | 0 |
| RULE_A 대상 public/student 노출 플래그 비0 | 0 |
| RULE_A ∩ 옵션1 보강 콘텐츠 | 0 |

## 생성 산출물

서버:

- `/home/chsh82/aprolabs/reports/vocab_transition_rule_a_dryrun_manifest_20261007_20261007-224108.csv`
- `/home/chsh82/aprolabs/reports/vocab_transition_rule_a_dryrun_status_20261007_20261007-224108.json`

로컬 복사본:

- `reports/vocab_transition_rule_a_dryrun_manifest_20261007_20261007-224108.csv`
- `reports/vocab_transition_rule_a_dryrun_status_20261007_20261007-224108.json`
- `reports/vocab_transition_rule_a_dryrun_20261007.md`

manifest 컬럼:

- `rule`
- `content_id`
- `level_row_id`
- `lemma`
- `official_grade`
- `proposed_base_level`
- `review_status`
- `old_vocab_level`
- `old_level_status`
- `new_vocab_level`
- `new_level_status`
- `level_version`
- `public_ready`
- `student_exposure`

이 manifest는 실제 적용 시 rollback manifest 역할을 한다. 즉 `level_row_id` 기준으로 `old_vocab_level`과 `old_level_status`를 되돌릴 수 있다.

## dry-run 적용 결과

연구 DB 원본이 아니라 복사본에만 적용했다.

| 항목 | 결과 |
|---|---:|
| dry-run UPDATE rows | 1,368 |
| dry-run 후 RULE_A 잔여 | 0 |
| dry-run 후 RULE_B 유지 | 40 |
| dry-run DB integrity_check | ok |
| dry-run DB SHA-256 | `2de8d2591304e550bce3c0ced2236953c8341e64ccfad5c48c8a7fb04d8926f7` |

## 앱 선택 함수 기준 availability 변화

실제 서비스 선택 함수 `_level_availability()`를 dry-run DB 경로로 실행했다.

### 적용 전

| 레벨/모드 | 어휘 | 문항 |
|---|---:|---:|
| L1 all_candidates | 89 | 324 |
| L1 auto_only | 67 | 244 |
| L2 all_candidates | 92 | 346 |
| L2 auto_only | 6 | 23 |
| L3 all_candidates | 149 | 444 |
| L3 auto_only | 80 | 297 |

### RULE_A dry-run 후

| 레벨/모드 | 어휘 | 문항 |
|---|---:|---:|
| L1 all_candidates | 89 | 324 |
| L1 auto_only | 67 | 244 |
| L2 all_candidates | 166 | 625 |
| L2 auto_only | 78 | 289 |
| L3 all_candidates | 75 | 170 |
| L3 auto_only | 8 | 31 |

## 해석

RULE_A만 적용해도 L2 공급량은 크게 늘어난다.

- L2 all_candidates: `92/346 -> 166/625`
- L2 auto_only: `6/23 -> 78/289`

대신 L3 공급량은 크게 줄어든다.

- L3 all_candidates: `149/444 -> 75/170`
- L3 auto_only: `80/297 -> 8/31`

L3 all_candidates가 75/170으로 남는 것은 옵션1 64콘텐츠가 `REVIEW_BOUNDARY`로 연결되어 있기 때문이다. auto_only에는 옵션1이 들어가지 않으므로 L3 auto_only는 8/31로 작아진다.

## 다음 승인 경계

아직 live DB에는 적용하지 않았다.

실제 적용을 원하면 다음 문구에 해당하는 별도 승인이 필요하다.

`RULE_A 1,368건만 연구 DB에서 L3에서 L2로 변경하는 것을 승인합니다. RULE_B는 보류합니다. DB 백업, 대상 ID 고정, rollback manifest, 격리 dry-run 결과 기준 단일 트랜잭션 적용, 사후 검증까지 포함하며 학생 공개/allowlist/feature flag 변경은 승인하지 않습니다.`

승인 후에도 실제 적용 전에는 live DB 백업을 먼저 만들고, 단일 트랜잭션으로 반영한 뒤 availability와 integrity를 다시 읽어 검증해야 한다.
