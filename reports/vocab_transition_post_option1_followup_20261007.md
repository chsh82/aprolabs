# vocab_transition_post_option1_followup_20261007

- 작성 시각(KST): 2026-10-07 15:33:30 +0900
- 업무명: 어휘 연구 프로젝트 — 옵션 1 적용 이후 후속 점검
- 범위: post-apply 검증, 출제/선택 조건 영향 분석, 코드/배포 필요성 확인, 다음 승인안 정리
- 승인 경계: RULE_A/RULE_B 실제 적용, 추가 DB 쓰기, 배포, 학생 공개, allowlist/flag 전환, momolib 작업 금지
- 코드 기준:
  - 로컬 repo HEAD: `6ac220e`
  - 연구 서버 repo HEAD: `b8fbe7c`
- DB 기준: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- 기준 산출물:
  - `reports/vocab_transition_option1_l3_backfill_apply_20261007.md`
  - `reports/vocab_transition_option1_l3_backfill_apply_status.yaml`
  - `reports/vocab_transition_option1_l3_backfill_manifest_20261007.csv`
  - `reports/vocab_transition_execution_readiness_20261007.md`
  - `reports/vocab_transition_approval_options_20261007.md`
  - `C:/Users/aproa/AppData/Local/hermes/cache/documents/doc_370afed4b625_vocab_project_hermes_handoff_20261007.md`

## 1. 수행 요약

옵션 1 적용 후 상태를 연구 서버 실제 DB에서 읽기 전용으로 재확인했다. 옵션1 64개 content_id에는 `level_policy_v0.1 / L3 / REVIEW_BOUNDARY / is_active=1` 레벨행이 64개 존재한다. 공개 플래그는 여전히 0이고, RULE_A/RULE_B는 각각 1,368 / 40으로 미적용 상태다. DB integrity/FK도 정상이다.

코드 분석 결과, 일반 다중유형 출제와 일반 레벨별 출제는 `SOURCE_VERSION = "2.1.29"`만 후보로 읽는다. L3 보강 64건은 레벨행이 생겼더라도 문항 source_version이 `nikl_grade5_l3_batch1_v1`/`nikl_grade5_l3_batch2_v1`이므로 일반 레벨 출제에는 아직 보이지 않는다. 다만 관리자 L3 보강 전용 모드에서는 batch별 manifest whitelist 경로로 보인다.

## 2. post-apply DB 재확인

검증 방식:

- 연구 서버에서 DB를 `file:...?mode=ro`로 열고 `PRAGMA query_only=ON` 설정.
- 로컬 옵션1 manifest를 연구 서버 임시 경로에 복사해 동일 SHA-256 확인 후 조회 기준으로 사용.
- DB 쓰기 쿼리, UPDATE/INSERT/DELETE, 배포 명령은 실행하지 않음.

검증 결과:

| 항목 | 결과 |
|---|---:|
| DB size | 17,281,024 bytes |
| DB SHA-256 | `967154ed125ac12a94675541c97a8bcd9fa2c60454a6ada2f0ac2f9776b59d1a` |
| manifest SHA-256 | `95ff2da82db1da7f278abc49fb58322b0f61ed067942d0caf499972da9021d77` |
| integrity_check | `ok` |
| foreign_key_check violations | 0 |
| vocabulary_contents | 6,018 |
| vocabulary_content_levels | 6,014 |
| vocabulary_multiformat_items | 1,747 |
| vocabulary_official_grade_reference | 5,950 |
| vocabulary_publish_reviews | 147 |

옵션1 대상 검증:

| 항목 | 결과 |
|---|---:|
| manifest distinct content_id | 64 |
| manifest item_id | 128 |
| L3 REVIEW_BOUNDARY level rows | 64 |
| active target items | 128 |
| target content public/student flag nonzero | 0 |
| all content public/student flag nonzero | 0 |

옵션1 레벨행 상세:

| vocab_level | level_status | level_version | is_active | rows | contents |
|---:|---|---|---:|---:|---:|
| 3 | REVIEW_BOUNDARY | level_policy_v0.1 | 1 | 64 | 64 |

옵션1 문항 구성:

| source_version | item_type | items | contents |
|---|---|---:|---:|
| nikl_grade5_l3_batch1_v1 | CONTEXT_MEANING | 29 | 29 |
| nikl_grade5_l3_batch1_v1 | MEANING_CHOICE | 29 | 29 |
| nikl_grade5_l3_batch2_v1 | CONTEXT_MEANING | 35 | 35 |
| nikl_grade5_l3_batch2_v1 | MEANING_CHOICE | 35 | 35 |

RULE_A/B 상태:

| 항목 | 결과 | 판정 |
|---|---:|---|
| RULE_A remaining | 1,368 | 미적용 유지 |
| RULE_B remaining | 40 | 미적용 유지 |

## 3. 출제/선택 조건 영향

자세한 코드 영향 분석은 별도 파일 `reports/vocab_transition_selection_code_impact_20261007.md`에 기록했다.

핵심 경로:

- `app/vocabulary_quiz/routers/multiformat.py:94` — `SOURCE_VERSION = "2.1.29"`
- `app/vocabulary_quiz/routers/multiformat.py:713~727` — 일반 혼합 후보 `_select_question_items()`가 `2.1.29`만 조회
- `app/vocabulary_quiz/routers/multiformat.py:756~789` — 일반 레벨 후보 `_select_level_candidates()`가 레벨 matching 후에도 item source_version `2.1.29`만 조회
- `app/vocabulary_quiz/routers/multiformat.py:1315~1338` — L3 보강 전용 availability는 batch source_version + manifest whitelist 사용
- `app/vocabulary_quiz/routers/multiformat.py:1568~1645` — L3 보강 전용 session 생성은 batch별 별도 source_version 저장
- `app/vocabulary_quiz/routers/quiz.py:39~60` — 기존 단일 4지선다 MVP도 source/content `2.1.29`만 조회

현재 일반 L3 레벨 출제 공급량:

| 조건 | words | items |
|---|---:|---:|
| current code all_candidates | 85 | 316 |
| current code auto_only | 80 | 297 |

옵션1 whitelist와 보강 source_version을 별도 코드로 포함한다고 가정한 시뮬레이션:

| 조건 | words | items | 해석 |
|---|---:|---:|---|
| all_candidates + 옵션1 whitelist | 149 | 444 | 일반 L3 후보에 연결 가능 |
| auto_only 현행 | 80 | 297 | 옵션1 64건은 REVIEW_BOUNDARY라 auto_only 불포함 |

관리자 전용 L3 보강 모드 기준 옵션1 대상은 128문항 모두 후보 조건을 통과한다. 일반 레벨 출제에는 아직 연결되지 않았다.

## 4. 추가 DB 변경 없이 정리한 다음 필요 작업

### 4.1 whitelist/manifest 기반 포함

필요하다. source_version 조건만 넓히면 승인 대상 64건 외 항목이 섞일 수 있다. 일반 L3 레벨 출제에 연결하려면 옵션1 manifest 64 content_id / 128 item_id를 고정 whitelist로 쓰는 설계가 필요하다.

### 4.2 source_version 조건 수정

일반 레벨 출제에 옵션1 64건을 보이게 하려면 필요하다. 수정 대상 후보는 다음이다.

- `_select_level_candidates()`
- `_level_availability()` / `/api/vocabulary-quiz/availability`
- 일반 세션 생성 시 `source_version`/metadata 기록 방식

일반 전체/혼합 출제까지 확장할지는 별도 결정이 필요하다. 이번 후속 점검에서는 수정하지 않았다.

### 4.3 관리자 전용 모드와 일반 모드 분리

분리 유지가 필요하다. L3 보강 전용 모드는 배치별 검수/리허설/고정 응시에 유용하고, 일반 레벨 모드는 `level_status`, whitelist, source_version 혼입 방지 규칙을 별도로 가져야 한다.

### 4.4 기존 세션/결과 조회 영향

현재 `vocabulary_multiformat_responses`에서 옵션1 item_id를 참조하는 기존 응답은 0건이다. 기존 결과 조회 영향은 없다. 향후 일반 레벨 모드에 포함할 경우, 세션 `source_version`이 현재 일반 경로처럼 항상 `2.1.29`로 저장되면 다중 source 세션의 감사성이 약해진다. metadata에 whitelist id/source_versions를 남기는 설계가 필요하다.

## 5. 수행한 읽기 전용/격리 수준 테스트

실행 명령/검증:

1. 연구 서버 접속/상태 확인
   - `ssh aprolabs 'git -C /home/chsh82/aprolabs rev-parse --short HEAD ...'`
   - 결과: remote HEAD `b8fbe7c`, DB 존재 확인.
2. 연구 DB 읽기 전용 검증 스크립트
   - DB 연결: `file:/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db?mode=ro`
   - `PRAGMA query_only=ON`
   - 결과: integrity ok, FK 0, 옵션1 64 레벨행 확인.
3. 코드 정적 분석
   - `app/vocabulary_quiz/routers/multiformat.py`, `app/vocabulary_quiz/routers/quiz.py` line-level 확인.
4. 문법 확인
   - `python -m py_compile app/vocabulary_quiz/routers/multiformat.py app/vocabulary_quiz/routers/quiz.py`
   - 결과: exit code 0, 출력 없음.

주의: 검증 스크립트와 manifest 사본은 연구 서버 `/home/chsh82/aprolabs/tmp/` 아래 임시 파일로만 복사했다. DB 파일, 앱 코드, 배포 상태는 변경하지 않았다.

## 6. 배포/쓰기/공개 상태

| 항목 | 결과 |
|---|---|
| 추가 DB writes | 0 |
| RULE_A/RULE_B 적용 | 없음 |
| 공개 플래그 변경 | 없음 |
| 학생 공개/allowlist/feature flag | 없음 |
| 코드 변경 | 보고서 3개 생성 외 없음 |
| 배포 | 없음 |
| momolib 작업 | 없음 |

## 7. 다음 대표님 의사결정안

A) 관리자 L3 보강 전용 모드만 연결 확인하고 일반 레벨 출제는 보류
- 현재 전용 모드에서는 64콘텐츠/128문항 후보가 보인다.
- 추가 코드/DB/배포 없이 상태를 유지한다.
- 학생 공개도 계속 없음.

B) 일반 L3 관리자 레벨 출제 연결 코드 수정 리허설 승인
- 옵션1 manifest whitelist 기반으로 `selected_vocab_level=3 + all_candidates`에서 64건을 포함하도록 로컬/격리 리허설 코드를 작성한다.
- source_version 단순 확장이 아니라 manifest whitelist, REVIEW_BOUNDARY 정책, 세션 metadata 감사를 포함한다.
- 리허설 산출물/diff/테스트만 만든 뒤 실제 배포는 별도 승인으로 분리한다.

C) RULE_A/B는 계속 보류하고, 공급량 변화표만 유지
- RULE_A 1,368 / RULE_B 40은 현 상태로 미적용 유지한다.
- L3 공급량은 현재 코드 기준 85 words / 316 items, whitelist 포함 가정 149 words / 444 items로 관리한다.
- 정책 일괄 전환은 별도 교육정책 승인 전까지 진행하지 않는다.

## 8. 산출물

생성 파일:

1. `C:/Users/aproa/aprolabs/reports/vocab_transition_post_option1_followup_20261007.md`
2. `C:/Users/aproa/aprolabs/reports/vocab_transition_post_option1_followup_status.yaml`
3. `C:/Users/aproa/aprolabs/reports/vocab_transition_selection_code_impact_20261007.md`

## 9. 미해결/주의

- 연구 서버 HEAD(`b8fbe7c`)와 로컬 HEAD(`6ac220e`) 차이는 계속 존재한다. 이번 분석은 코드 구조상 관련 파일이 로컬에도 존재하고, DB는 연구 서버 실제 파일을 읽어 확인했다.
- 일반 레벨 출제 연결은 코드 변경 없이는 더 판단하기 어렵지 않다. 결론은 명확하다: 현재는 `2.1.29 only`라 일반 레벨 출제에 안 보이며, 보이게 하려면 승인된 코드 리허설/수정이 필요하다.
- 실제 DB 추가 쓰기나 배포가 필요한 단계는 여기서 중단하고 승인 요청안으로 남겼다.
