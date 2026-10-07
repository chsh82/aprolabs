# vocab_transition_selection_code_impact_20261007

- 작성 시각(KST): 2026-10-07 15:33:30 +0900
- 범위: 옵션 1 L3 레벨행 적용 이후 출제/선택 코드 영향 분석
- 코드 기준:
  - 로컬 repo: `C:/Users/aproa/aprolabs`, HEAD `6ac220e`
  - 연구 서버 repo: `/home/chsh82/aprolabs`, HEAD `b8fbe7c`
- DB 기준: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- 수행 원칙: 코드/배포/DB 추가 변경 없음. 읽기 전용 DB 조회와 정적 코드 분석만 수행.

## 1. 결론

현재 일반 다중유형 출제와 일반 레벨별 출제는 코드 상수 `SOURCE_VERSION = "2.1.29"`에 묶여 있다. 옵션 1로 64개 content_id에 L3 레벨행이 생겼지만, 해당 문항의 `source_version`은 `nikl_grade5_l3_batch1_v1` 또는 `nikl_grade5_l3_batch2_v1`이므로 일반 레벨 출제의 SQL 후보 조회에는 들어오지 않는다.

즉, 옵션 1 DB INSERT만으로는 다음 상태가 된다.

| 경로 | 옵션1 64건 노출 여부 | 이유 |
|---|---|---|
| 관리자 L3 보강 전용 모드 | 보임 | 별도 `grade5_l3_batch1_mode`/`grade5_l3_batch2_mode`가 batch source_version + manifest whitelist를 사용 |
| 관리자 일반 레벨 출제 `selected_vocab_level=3` | 안 보임 | `_select_level_candidates()`가 `VocabularyMultiformatItem.source_version == SOURCE_VERSION("2.1.29")`만 조회 |
| 관리자 전체/혼합 일반 출제 | 안 보임 | `_select_question_items()`도 `SOURCE_VERSION("2.1.29")`만 조회 |
| 학생 공개/allowlist/flag | 안 보임 | 이번 작업에서 public/student flag, allowlist, feature flag 변경 없음 |

## 2. 관련 코드 위치

### `app/vocabulary_quiz/routers/quiz.py`

- line 39~41: `QUESTION_COUNT = 20`, `SOURCE_VERSION = "2.1.29"`, `ELIGIBLE_STATUSES = (...)`
- line 44~60: `_select_question_items()`가 `VocabularyItem.source_version == SOURCE_VERSION` 및 `VocabularyContent.source_version == SOURCE_VERSION`으로 단일 4지선다 MVP 후보를 제한한다.
- line 85~88: 세션 row도 `source_version=SOURCE_VERSION`으로 저장한다.

이 파일은 기존 단일 4지선다 `/vocabulary-quiz/play` 경로다. L3 보강 다중유형 문항은 `vocabulary_multiformat_items` 기반이라 직접 경로는 아니지만, 동일하게 일반 출제 소스가 `2.1.29`로 고정되어 있음을 보여준다.

### `app/vocabulary_quiz/routers/multiformat.py`

- line 94: `SOURCE_VERSION = "2.1.29"`
- line 103~114: 레벨별 출제 상수. `LEVEL_VERSION = "level_policy_v0.1"`, `CONFIDENCE_MODES = {all_candidates: PROVISIONAL_AUTO+REVIEW_BOUNDARY, auto_only: PROVISIONAL_AUTO}`.
- line 180~212: L3 보강 batch1/batch2의 별도 source_version, manifest 경로, expected count 정의.
- line 331~377: `_load_l3_batch_manifest_rows()`, `_expected_l3_batch_item_ids()`가 Git 관리 매니페스트를 whitelist로 사용.
- line 380~461: `_select_l3_batch_item_ids()`가 batch source_version, item/content active, 비공개 플래그, manifest whitelist를 검증한다. 현재 주석은 "레벨행이 없다"는 과거 상태를 전제로 하나 옵션 1 이후 DB에는 레벨행이 생겼다. 로직 자체는 여전히 레벨행을 보지 않는다.
- line 713~727: `_select_question_items()` 일반 혼합 후보는 `VocabularyMultiformatItem.source_version == SOURCE_VERSION`으로 제한.
- line 756~789: `_select_level_candidates()` 레벨 후보는 먼저 `vocabulary_content_levels`에서 matching content_id를 구하지만, 최종 item 조회는 `VocabularyMultiformatItem.source_version == SOURCE_VERSION`으로 제한. 이것이 옵션1 64건이 일반 L3 레벨 출제에 안 보이는 직접 원인이다.
- line 1209~1248: `/api/vocabulary-quiz/availability`도 일반 가용량 계산에서 같은 `_level_availability()`/`SOURCE_VERSION` 경로를 사용.
- line 1315~1338: `/api/vocabulary-quiz/grade5-l3-batch1-availability`는 별도 L3 보강 전용 가용량 조회.
- line 1568~1645: `_create_l3_batch_session()`은 별도 L3 보강 세션 생성. 세션 `source_version`에는 batch source_version을 저장한다.
- line 1652~1768: `create_session()`에서 모드 플래그가 있으면 별도 경로로 조기 반환하고, 그렇지 않으면 일반 레벨/혼합 출제(`SOURCE_VERSION="2.1.29"`)로 진행한다.

## 3. 읽기 전용 DB 검증 결과

검증 스크립트는 연구 서버 DB를 `file:...?mode=ro`로 열고 `PRAGMA query_only=ON`을 설정했다.

| 항목 | 결과 |
|---|---:|
| manifest SHA-256 | `95ff2da82db1da7f278abc49fb58322b0f61ed067942d0caf499972da9021d77` |
| integrity_check | `ok` |
| foreign_key_check violations | 0 |
| 옵션1 manifest content_id | 64 |
| 옵션1 manifest item_id | 128 |
| 옵션1 L3 REVIEW_BOUNDARY 레벨행 | 64 |
| 옵션1 active item | 128 |
| 옵션1 content public/student 노출 비0 | 0 |
| 전체 content public/student 노출 비0 | 0 |
| RULE_A remaining | 1,368 |
| RULE_B remaining | 40 |

옵션1 대상 문항 구성:

| source_version | item_type | items | contents |
|---|---|---:|---:|
| nikl_grade5_l3_batch1_v1 | CONTEXT_MEANING | 29 | 29 |
| nikl_grade5_l3_batch1_v1 | MEANING_CHOICE | 29 | 29 |
| nikl_grade5_l3_batch2_v1 | CONTEXT_MEANING | 35 | 35 |
| nikl_grade5_l3_batch2_v1 | MEANING_CHOICE | 35 | 35 |

## 4. 일반 L3 공급량 영향

현재 코드 그대로의 일반 L3 레벨 출제(`SOURCE_VERSION="2.1.29"`)는 옵션1 이후에도 다음과 같다.

| 조건 | words | items | by_type |
|---|---:|---:|---|
| all_candidates | 85 | 316 | CONTEXT_CLOZE 61, CONTEXT_MEANING 85, MATCH_WORD_MEANING 1, MEANING_CHOICE 85, WORD_FROM_DEFINITION 84 |
| auto_only | 80 | 297 | CONTEXT_CLOZE 58, CONTEXT_MEANING 80, MEANING_CHOICE 80, WORD_FROM_DEFINITION 79 |

whitelist + 보강 source_version을 별도 코드로 포함한다고 가정한 시뮬레이션은 다음과 같다.

| 조건 | words | items | by_type |
|---|---:|---:|---|
| all_candidates + 옵션1 whitelist | 149 | 444 | CONTEXT_CLOZE 61, CONTEXT_MEANING 149, MATCH_WORD_MEANING 1, MEANING_CHOICE 149, WORD_FROM_DEFINITION 84 |
| auto_only 현행 | 80 | 297 | REVIEW_BOUNDARY인 옵션1 64건은 auto_only에 들어가지 않음 |

해석: 옵션1 64건은 `level_status=REVIEW_BOUNDARY`로 들어갔기 때문에 `all_candidates`에는 정책상 포함 가능하지만, `auto_only`에는 포함되지 않는다. 다만 실제 일반 레벨 출제에 포함하려면 코드에서 `SOURCE_VERSION=2.1.29 only` 제한을 안전하게 확장해야 한다.

## 5. 다음 코드 작업 필요성

### whitelist/manifest 기반 포함 필요 여부

필요하다. 단순히 `source_version IN ('2.1.29','nikl_grade5_l3_batch1_v1','nikl_grade5_l3_batch2_v1')`로 넓히면 미승인/제외 항목(예: 벨기에, batch2 미개별승인 3건)이 혼입될 수 있다. 옵션1 manifest 64 content_id / 128 item_id를 기준으로 whitelist를 둔 뒤 일반 레벨 출제에 연결하는 설계가 안전하다.

### source_version 조건 수정 필요 여부

일반 레벨 출제에 옵션1 64건을 보이게 하려면 필요하다. 수정 후보는 `_select_level_candidates()`와 `/availability` 계산 경로다. 일반 전체/혼합 출제까지 확장할지는 별도 정책 결정이 필요하다.

### 관리자 전용 모드와 일반 모드 분리 필요 여부

분리 유지가 필요하다.

- 전용 L3 보강 모드는 batch별 검수/리허설/고정 응시에 유용하므로 유지.
- 일반 레벨 L3 모드는 `level_status`, whitelist, item_type, source_version 혼입 방지 규칙을 명시적으로 적용해야 한다.
- 학생 공개/allowlist/feature flag와는 계속 분리해야 한다.

### 기존 세션/결과 조회 영향

기존 세션/응답은 `vocabulary_multiformat_responses.item_id`로 문항을 참조한다. 이번 옵션1 대상 item_id가 기존 응답에 사용된 건수는 0이었다. 코드 변경 없이도 기존 결과 조회에는 영향이 없다. 향후 일반 레벨 출제로 포함되어 새 세션이 생성되면 세션 `source_version`을 어떻게 저장할지 결정해야 한다. 현재 일반 경로는 항상 세션 `source_version=2.1.29`로 저장하므로, 다중 source 혼합 일반 레벨 세션에서는 metadata에 포함 source/whitelist 정보를 남기거나 세션 source_version 의미를 재정의해야 한다.

## 6. 코드/배포 판단

- 현재 코드가 일반 레벨 출제에서 source_version 2.1.29만 읽는 이유는 의도적 격리 설계다. 각 파일럿/보강 배치를 별도 source_version과 manifest whitelist로 분리해 기존 일반 출제를 오염시키지 않도록 구성되어 있다.
- 옵션1은 DB 레벨행만 보강했으므로, 일반 레벨 출제 연결은 아직 미완료다.
- 다음 단계는 실제 코드 변경 전 설계 승인이다. 승인 없이 source_version 조건 변경, whitelist 파일 추가, 배포를 진행하면 안 된다.
