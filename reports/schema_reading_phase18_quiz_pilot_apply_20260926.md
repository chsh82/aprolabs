# 스키마리딩x어휘 DB 통합 — 18단계: L4·L5 파일럿 문항 40건 실제 적재(vocabulary_multiformat_items) + 기능 검증

- 작성일: 2026-09-26 (`date` 명령으로 시스템 현재 날짜 직접 확인)
- 대상: `reports/schema_reading_phase17_quiz_pilot_semantic_review_20260926.md` /
  `data/import/schema_reading_phase17_quiz_pilot_semantic_review_20260926.csv`·`.jsonl`
  (MEANING_CHOICE 20 + CONTEXT_MEANING 20, 전부 AUTO_PASS 40/40 + SEMANTIC_PASS 40/40,
  "정기" 해설 수정 반영된 최신본) — 이 40건을 **처음으로 실제 research DB에 적재**했다.
- 참조(재조사하지 않고 그대로 인용): phase16 dry-run 보고서, phase17 의미 검토
  보고서, phase13/14 적재 보고서·스크립트(게이트 패턴).
- 범위: research DB 쓰기는 아래 3절 "단일 트랜잭션 적재" 한 번만 수행했다.
  `literacy.db`는 이번 세션에서 열지 않았다(작업 범위상 불필요 — phase17이 이미
  원문 대조를 끝냈고, 이번 세션은 그 확정 결과물을 그대로 옮기기만 함). git
  commit/push는 하지 않았다(호출 세션 판단 대기).

---

## 0. 핵심 요약

1. **적재 결과: 40건 전부 신규 삽입 완료** (`vocabulary_multiformat_items`,
   MEANING_CHOICE 20 + CONTEXT_MEANING 20). 배치 마커
   `source_version='schema_reading_l4l5_pilot_dryrun_v1'`.
2. 게이트 전부 PASS (1~6, 3절). 백업 1건 생성·검증 완료, 롤백 불필요.
3. 적재 후 검증(4절) 전부 PASS: 유형별 20/20, 콘텐츠 연결 40/40, 중복 0, 고아 0,
   `integrity_check`=ok, `foreign_key_check` 위반 0, 멱등성 재실행 확인(2회차 삽입
   0건, 스킵 40건), 기존 `vocabulary_contents`/`vocabulary_content_levels`(각
   5,820건) 체크섬 전후 완전 동일, 기존 `vocabulary_multiformat_items` 1,289건
   체크섬도 전후 완전 동일(신규 40건 제외 재계산).
4. **중요 설계 결정 — 이 40건은 관리자의 "일반" 출제 흐름(혼합모드·레벨모드
   전부)에서 구조적으로 절대 선택되지 않는다.** `app/vocabulary_quiz/routers/
   multiformat.py`가 `SOURCE_VERSION="2.1.29"`를 하드코딩해 두 선택 함수
   (`_select_question_items`/`_select_level_candidates`) 양쪽 다 이 값과
   일치하는 문항만 후보로 삼는다(1절 조사 결과). 이번 40건의 `source_version`을
   지시대로 `schema_reading_l4l5_pilot_dryrun_v1`로 그대로 두었으므로, **이
   하드코딩 필터 자체가 phase13/14의 `level_status='REVIEW_BOUNDARY'`와 같은
   취지의 "관리자 검토용, 일반 노출 경로 차단" 역할을 한다** — 별도로
   `is_active=0` 처리를 하지 않아도(오히려 정상값 1 유지) 이미 완전히
   격리된다. 5절 기능 테스트에서 이 격리를 직접 확인했고, 실제 문제
   조회→응답→채점 흐름은 세션을 직접 구성(정상 선택 API 우회)하는 방식으로
   검증했다 — 이는 향후 이 배치를 정식 편입시키려면 `source_version`을
   `2.1.29`로 바꾸는 **의도적인 별도 마이그레이션**이 필요함을 뜻한다(phase13의
   `vocabulary_items` 방어선과 동일한 철학, 7절 리스크 1 참고).
5. 기능 테스트(5절) 57건 + 오답 경로 추가 확인 전부 PASS: 비로그인 차단(302
   로그인 리다이렉트), 비관리자 차단(403), 문제 조회 응답에 정답 필드 비노출,
   제출 후에만 정답·해설 노출, 정답/오답 채점 정확, 결과 집계 정확, 테스트로
   만든 세션·계정은 전부 정리(삭제) 완료, 기존 데이터 행 수 불변.

---

## 1. 작업 1 — 기존 적재 방식·관리자 출제 쿼리 조사

### 1-1. `vocabulary_multiformat_items` 실제 스키마 (서버에서 직접 재확인)

`PRAGMA table_info`/`sqlite_master.sql`을 서버에서 직접 조회(과거 기록을
믿지 않고 재확인):

```
id, item_id(UNIQUE), item_type
  CHECK IN ('MEANING_CHOICE','WORD_FROM_DEFINITION','CONTEXT_MEANING',
            'CONTEXT_CLOZE','MATCH_WORD_MEANING','CROSSWORD'),
source_content_id (REFERENCES vocabulary_contents(content_id)),
source_content_ids_json, sense_id, sense_ids_json, lemma, pos, prompt(NOT NULL),
options_json, correct_option(NULL 허용, 1~4), public_payload_json,
answer_payload_json(NOT NULL), explanation, cognitive_level, qa_flags_json,
generator_version, source_version(NOT NULL), is_active(NOT NULL DEFAULT 1),
created_at, updated_at
```

저장소의 마이그레이션 스크립트(`scripts/vocab/
migrate_vocabulary_quiz_add_multiformat_tables.py`)에 적힌 CHECK 목록에는
`CROSSWORD`가 빠져 있었지만, 서버 라이브 스키마에는 이미 포함돼 있었다 —
이후 다른 마이그레이션(CROSSWORD 지원 추가, `scripts/vocab/
migrate_vocabulary_quiz_add_crossword_support.py`로 추정)이 테이블을 재생성해
CHECK를 갱신한 것으로 보인다. **"과거 기록을 믿지 말고 서버에서 재확인하라"는
지시가 실제로 유효했던 사례.**

### 1-2. 기존 임포터(`scripts/vocab/import_multiformat_quiz.py`) 로직

- `item_id` 기준 upsert(`INSERT ... ON` 대신 존재 여부로 분기, 신규/갱신/불변
  3분류). 컬럼별 의미:
  - `source_content_id`: 단일 어휘형(`MEANING_CHOICE`/`WORD_FROM_DEFINITION`/
    `CONTEXT_MEANING`/`CONTEXT_CLOZE`)만 사용.
  - `source_content_ids_json`: 복합형(`MATCH_WORD_MEANING`/`CROSSWORD`)만
    사용, 배열 JSON.
  - `public_payload_json`: 정답이 빠진 표시용 정보(선택형은 `options`만).
  - `answer_payload_json`: 채점용 정답(선택형은 `{"correct_option": N}`).
  - `item_type` 허용값: 위 6개 그대로.
  - `source_version`(문항 테이블): **아카이브 라벨(`--version`)이 아니라
    문항이 참조하는 콘텐츠 버전**(`item["source_version"]`)을 저장 — 기존
    1,289건은 전부 `2.1.29` 단일 값(서버 조회로 재확인).
  - `is_active`: 스키마 기본값 1, 임포터가 별도로 건드리지 않음(항상 활성).
- **콘텐츠 자격 검사(HARD 게이트)**: `validate()`가 각 문항의
  `source_content_id`(또는 복합형의 `content_ids`)가 `vocabulary_contents`에
  존재하고, `generation_status IN ('PRIVATE_SERVER_READY',
  'PRIVATE_SERVER_READY_CANDIDATE')` **AND** `student_exposure=0` **AND**
  `public_ready=0`이어야 통과시킨다. **이번 40건이 참조하는 20개 content_id는
  phase13/14가 의도적으로 `generation_status=NULL`로 남겨 뒀으므로(정식
  화이트리스트 회피 방어선), 이 기존 임포터의 표준 HARD 게이트를 그대로
  썼다면 전부 `ineligible_content`로 거부됐을 것이다.** 이는 애초에 "새로
  게이트 기반 스크립트를 phase13/14 패턴으로 직접 작성하라"는 지시와 일치하는
  이유이기도 하다 — 기존 임포터를 그대로 재사용하지 않고, 3절의 전용
  스크립트를 새로 작성했다.

### 1-3. 관리자 출제 쿼리 (`app/vocabulary_quiz/routers/multiformat.py`)

- **혼합모드** `_select_question_items()`: `source_version == SOURCE_VERSION`
  (모듈 상수, `"2.1.29"` 하드코딩) **AND** `is_active == 1` **AND**
  (지정 시) `item_type IN (...)`. 무작위 표본.
- **레벨모드(admin_level_quiz_v1)** `_select_level_candidates()`: 위와 동일한
  `source_version`/`is_active` 조건 + 단일 어휘형은
  `source_content_id`가, 복합형은 `source_content_ids_json`의 전부가
  `vocabulary_content_levels`(`level_version='level_policy_v0.1'`,
  `is_active=1`, `vocab_level=선택레벨`, `level_status IN
  confidence_mode에 해당하는 값`)와 일치해야 후보에 포함. `confidence_mode`
  기본값(`all_candidates`)은 `PROVISIONAL_AUTO`/`REVIEW_BOUNDARY` 둘 다
  허용 — 즉 phase13/14가 매긴 `REVIEW_BOUNDARY`는 레벨모드 관점에서는 이미
  "노출 가능" 신뢰도 등급이다(문항 테이블 쪽의 `source_version` 필터가
  없었다면 이번 40건도 레벨모드 기본값으로 바로 걸렸을 것).
- **두 함수 모두 `source_version`을 하드코딩된 상수와 정확히 일치시켜야만
  후보에 든다** — 0절 4번, 3-3절에서 이 사실을 적재 설계에 반영했다.

---

## 2. 작업 2 — dry-run 사전 검사

로컬에서 `data/import/schema_reading_phase17_quiz_pilot_semantic_review_20260926.csv`
40행을 직접 파싱해 확인(재사용, 재생성 안 함):

| 검사 | 결과 |
|---|---|
| 총 행수 | 40 (MEANING_CHOICE 20 + CONTEXT_MEANING 20) |
| `source_content_id` distinct 개수 | 20 (각 content_id당 정확히 2건 — MEANING_CHOICE 1 + CONTEXT_MEANING 1) |
| **콘텐츠 연결** | **40/40** (아래 20개 content_id, 전부 CSV에서 정확히 2번씩만 등장) |
| `item_id` 중복 | 0건 |
| `semantic_verdict` 전부 `SEMANTIC_PASS` | True (40/40) |
| `auto_validation_status` 전부 `PASS` | True (40/40) |
| `source_version` 값 | 전부 `schema_reading_l4l5_pilot_dryrun_v1` (단일값, 40/40) |

20개 content_id: `SR_L4CORE_4749/4750/4786/4794/4796/4801/4812/4813/4825/4827`
(L4 10개) + `SR_L5CORE_4835/4836/4837/4843/4848/4849/4859/4868/4904/4906`
(L5 10개).

서버 research DB(mode=ro 조회)에서 위 20개를 직접 대조:

| 검사 | 결과 |
|---|---|
| `vocabulary_contents`에 20개 전부 존재 | PASS |
| 20개 전부 `is_active=1` | PASS |
| 20개 전부 `student_exposure=0`/`public_ready=0` | PASS(phase13/14 이후 아무도 변경 안 함) |
| 20개 전부 `generation_status` | NULL(phase13/14 설계대로) |
| `vocabulary_content_levels`에서 20개 전부 `level_status='REVIEW_BOUNDARY'`, `level_version='level_policy_v0.1'`, `vocab_level`이 L4는 4/L5는 5 | PASS |
| 기존 `vocabulary_multiformat_items`(1,289건) 중 이 20개 content_id를 참조하는 행 | **0건** (구조적으로 중복/충돌 불가능) |
| 이번 40개 `item_id`가 기존 1,289건과 겹치는지 | **0건 충돌** |

**40/40 콘텐츠 연결, 중복 0, 충돌 0 — 전부 통과해 3절로 진행.**

---

## 3. 작업 3 — 게이트 기반 단일 트랜잭션 적재

phase13/14 패턴(`scripts/vocab/phase13_apply_l4_core.py`)을 그대로 재사용해
`scripts/vocab/phase18_apply_quiz_pilot.py`를 신규 작성했다(적재 대상 테이블만
`vocabulary_multiformat_items`로 다름). rows-json은
`scripts/vocab/phase18_build_rows_from_phase17.py`로 phase17 CSV에서 결정론적으로
생성한다(재실행 시 바이트 단위로 동일 산출물 재확인 완료).

### 3-1. 컬럼 값 설계

- `source_version = 'schema_reading_l4l5_pilot_dryrun_v1'` (phase17 CSV 값
  그대로) — 0절 4번에서 설명한 대로, 이 값 자체가 관리자 일반 출제 흐름에서의
  격리 장치.
- `is_active = 1` (정상값 유지 — `source_version` 불일치만으로 이미 완전히
  격리되므로 추가로 비활성화할 필요가 없음. `is_active=0`으로 두면 오히려
  "고장난 문항"처럼 보여 향후 정식 편입 시 혼동을 줄 수 있어 피했다).
- `options_json`/`correct_option`/`answer_payload_json`/`explanation`: CSV
  값을 그대로 옮김(phase16/17이 이미 구조적·의미적 검증을 마친 값).
- `public_payload_json`: 기존 임포터의 `_build_public_payload()`와 동일한
  규칙(선택형은 `{"options": [...]}`만) 적용.
- `qa_flags_json`: phase16/17 산출물 경로, `expert_review_status`,
  `l5_grade_caveat`, `auto_validation_status`, `semantic_verdict`,
  `semantic_reason`, `wrong_option_reasons`, 그리고 위 "source_version 격리"
  근거를 JSON으로 그대로 보존 — 적재 후에도 DB만 봐도 이 문항이 왜 이
  상태인지 추적 가능하게 함(phase13의 `level_reason_json`과 같은 취지).
- `generator_version = 'schema_reading_phase16_phase17_pilot_v1'`.
- `sense_id`/`sense_ids_json`/`source_content_ids_json`/`cognitive_level`:
  전부 NULL(단일 어휘형, 별도 sense 체계 없음).

### 3-2. 게이트별 pass/fail

| 게이트 | 내용 | 결과 |
|---|---|---|
| 1a | `.env`의 `APP_ENV=research` 재확인 | **PASS** |
| 1b | 실행 중 `uvicorn app.main:app` 프로세스(PID 1289515)의 `/proc/<pid>/environ` — `APP_ENV`/`VOCABULARY_QUIZ_DB_PATH`가 `.env`와 byte-for-byte 일치 | **PASS** |
| 2 | db-path basename이 `vocabulary_quiz_research.db`인지 하드가드 | **PASS** |
| — | SQLite Backup API 백업(`vq_research_backup.py`, 라벨 `phase18-quiz-pilot-pre-migration`) | **PASS** |
| — | 백업 무결성(`integrity_check`=ok, FK 위반 0)·행수 일치(원본=백업, `vocabulary_contents`/`vocabulary_content_levels` 각 5,820=5,820) | **PASS** |
| — | 적재 전 체크섬 스냅샷(`vocabulary_contents`/`vocabulary_content_levels`는 `vq_checksum_core_tables.py`, `vocabulary_multiformat_items`는 전용 인라인 스크립트) | **PASS**(기록 완료, 4절에서 전후 대조) |
| 3 | 입력 40건 각각의 `source_content_id`가 `vocabulary_contents`에 존재 + `is_active=1` + `student_exposure=0`/`public_ready=0` (dangling·부적격 콘텐츠 참조 방지) | **PASS** — 20개 전부 통과 |
| 4 | `item_id` 40개 중 이미 존재하는 것 스킵 분류(멱등성 사전 점검) | **PASS** — 1차 실행 시 스킵 0건, 신규 40건 |
| 5 | 40건 단일 트랜잭션 INSERT (`BEGIN`~`COMMIT`, 하나라도 예외 시 전체 롤백) | **PASS** — 예외 0건, 40건 전부 커밋 |
| 6 | 커밋 후 `PRAGMA integrity_check`/`foreign_key_check`, 유형별 건수(20/20), content_id 연결(40/40), item_id 중복(0) | **PASS** (전부) |

**하나도 fail하지 않아 롤백을 실행하지 않았다.**

### 3-3. 백업/증적

- 백업 경로: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase18-quiz-pilot-pre-migration-20260926-064003`
- 백업 SHA-256: `874754ac4da1f646efa726eaeddf2220a25fe74544c32be0cf45caca2f69c66a`
- 백업 크기: 12,312,576 bytes
- 백업 검증: 행수 일치(`vocabulary_contents`/`vocabulary_content_levels` 각 5,820=5,820), `integrity_check`=ok, `foreign_key_check` 위반 0
- 적재 전 체크섬(`vocabulary_contents`=`93afbf92afaf0a835994312fa43e97004b85784a432257140ee4dcc37db6f955`,
  `vocabulary_content_levels`=`13e5013dee58d516d9aa404fd93bbaf7fb622e0da89cb2baeae4baba1de0fbfe`,
  `vocabulary_multiformat_items`(1,289건)=`426dd8dab04a697468b286eaef05dfed3f310113a03d597d59280c05baa7a8f3`)
- 스크립트: `scripts/vocab/phase18_build_rows_from_phase17.py`(rows-json 생성),
  `scripts/vocab/phase18_apply_quiz_pilot.py`(게이트+단일 트랜잭션 적재, 서버 실행),
  `scripts/vocab/vq_research_backup.py`/`scripts/vocab/vq_checksum_core_tables.py`(기존
  phase4/7/13/14 스크립트 그대로 재사용)
- 서버 산출물: `~/scratch/phase18_quiz_pilot/`(`phase18_rows.json`,
  `checksum_before.json`, `checksum_after.json`, `mfi_checksum_before.json`)
- 로컬 산출물: `data/import/schema_reading_phase18_quiz_pilot_rows_20260926.json`
  (실제 적재에 쓰인 40건 rows-json)

### 3-4. 롤백 절차 (참고용, 실행 안 함 — 모든 게이트 PASS)

```bash
ssh aprolabs
cp /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.rollback-before-restore-$(date +%Y%m%d-%H%M%S)
cp /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase18-quiz-pilot-pre-migration-20260926-064003 \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
sqlite3 /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db "PRAGMA integrity_check;"
```

이 백업 하나로 이번 40건 삽입(과 그 외 아무 변화도 없음)을 완전히 되돌릴 수
있다.

---

## 4. 작업 4 — 적재 후 검증

| 항목 | 결과 |
|---|---|
| 문항 유형별 정확히 20/20 | **PASS** — `{'CONTEXT_MEANING': 20, 'MEANING_CHOICE': 20}` |
| 콘텐츠 연결 40/40(전부 유효한 content_id) | **PASS** |
| 신규 40건 distinct content_id | 20 (각 2건씩) |
| 중복 문항(`item_id` 중복) | **0건** |
| 고아 문항(연결된 content_id가 `vocabulary_contents`에 없음) | **0건**(테이블 전체 1,329건 기준) |
| `PRAGMA integrity_check` | ok |
| `PRAGMA foreign_key_check` | 위반 0건 |
| **멱등성 재실행** | 2회차 실행 결과 `INSERTED=0`, `SKIPPED=40` — 추가 삽입 0건 |
| 기존 `vocabulary_contents`(5,820건) 체크섬 | 적재 전후 완전 동일(`93afbf92...`) |
| 기존 `vocabulary_content_levels`(5,820건) 체크섬 | 적재 전후 완전 동일(`13e5013d...`) |
| `sum(student_exposure)`/`sum(public_ready)` | 0/0 (적재 전후 불변) |
| 기존 `vocabulary_multiformat_items`(1,289건, 신규 40건 제외 재계산) 체크섬 | 적재 전후 완전 동일(`426dd8da...`) |
| `vocabulary_multiformat_items` 전체 행수 | 1,289 → **1,329**(신규 40건만 증가) |
| `vocabulary_content_literacy_links`(phase4) 행수 | 142건, 불변(이번 세션이 건드리지 않음) |
| literacy.db | 이번 세션에서 아예 열지 않음(작업 범위상 불필요) |
| 코드 변경 | 신규 스크립트/테스트 파일만 추가, 기존 앱 코드(`app/`) 수정 0건 |
| git 작업 | 없음(commit/push 없음) |

---

## 5. 작업 5 — 로컬 기능 테스트 (실제 research 서버, 관리자 계정)

`tests/test_admin_level_quiz.py`와 동일한 방식(pytest 없음, FastAPI
`TestClient`로 실제 앱 end-to-end, 테스트로 만든 데이터는 실행 후 전부 삭제)을
그대로 따라 `tests/test_phase18_quiz_pilot_admin_flow.py`를 작성해 research
서버(`~/aprolabs`, `APP_ENV=research`, `VOCABULARY_QUIZ_DB_PATH`가 실제
research DB를 가리킴)에서 직접 실행했다. 실제 관리자 계정
(`admin@aprolabs.co.kr`, research `aprolabs.db`의 `users.is_admin=1` 확인)의
ID로 세션 쿠키를 만들어 인증을 정상적으로(우회 없이) 통과시켰다 —
비밀번호/토큰은 사용하지 않았고(쿠키 서명 방식) 어디에도 출력하지 않았다.

**0절에서 설명한 대로 이 40건은 정상 세션 생성 API(`POST /api/vocabulary-quiz/
sessions`, 혼합모드·레벨모드 둘 다)로는 절대 선택되지 않으므로**, 문제
조회→응답→채점 흐름 자체는 `vocabulary_multiformat_sessions`/
`vocabulary_multiformat_responses`에 파일럿 `item_id` 4건(L4 MEANING_CHOICE 1
+ CONTEXT_MEANING 1, L5 MEANING_CHOICE 1 + CONTEXT_MEANING 1)을 직접 지정한
세션을 만들어 검증했다(세션 **생성 API**만 우회했을 뿐, 이후 모든 HTTP
요청(next/answer/result)은 실제 `require_admin` 인증 의존성을 그대로
통과해야 했다 — 인증 자체를 우회하지 않았다).

### 5-1. 격리 설계 확인

| 검사 | 결과 |
|---|---|
| 레벨모드 L4 후보(`_select_level_candidates`)에 파일럿 4건 포함 여부 | **0건 포함**(격리 확인) |
| 레벨모드 L5 후보에 파일럿 4건 포함 여부 | **0건 포함**(격리 확인) |
| 혼합모드 필터(`source_version='2.1.29'` AND `is_active=1`) 조건을 만족하는 파일럿 item_id | **0건** |

### 5-2. 출제→응답→채점 흐름 (4문항: L4×2 + L5×2, MEANING_CHOICE×2 + CONTEXT_MEANING×2)

- `GET .../next` → `POST .../answer`(정답 제출) → 4문항 반복 → `GET .../next`
  `done=True` → `GET .../result`: **전부 정상 동작**.
- 결과 집계: `total=4`, `correct=4`, `accuracy=100.0`,
  `by_type={"MEANING_CHOICE": 2/2, "CONTEXT_MEANING": 2/2}`, `wrong_items=[]`.
- 채점 응답의 `correct_answer.correct_option`/`explanation`이 DB 원본과 정확히 일치.
- **추가 확인(오답 경로)**: 별도로 문항 1건에 오답을 제출해 `is_correct=False`,
  `correct_answer`가 정답을 정확히 반환, `result.wrong_items`에 해당 문항이
  올바르게 기록됨을 확인(채점 로직이 항상 참을 반환하는 게 아님을 실증) —
  검증 후 세션 삭제.

### 5-3. 비로그인 접근 차단

| 요청 | 결과 |
|---|---|
| `GET /vocabulary-quiz/multiformat/play` (쿠키 없음) | **302** `/login`으로 리다이렉트 |
| `GET /api/vocabulary-quiz/availability?level=4` | **302** |
| `POST /api/vocabulary-quiz/sessions/.../answer` | **302** |
| `GET /api/vocabulary-quiz/sessions/.../next` | **302** |
| `GET /api/vocabulary-quiz/sessions/{실제 파일럿 세션}/result` | **302** |

앱 전체 `auth_middleware`가 미로그인 요청을 API/페이지 구분 없이 로그인
페이지로 리다이렉트한다(기존 `test_admin_level_quiz.py`와 동일 동작, 재확인).

### 5-4. 로그인했지만 관리자가 아닌 경우 차단

임시 비관리자 테스트 계정(`is_admin=False`)을 만들어 확인 — 테스트 종료 후
계정 삭제 완료.

| 요청 | 결과 |
|---|---|
| `GET /vocabulary-quiz/multiformat/play` | **403** |
| `GET /api/vocabulary-quiz/sessions/.../next` | **403** |
| `POST /api/vocabulary-quiz/sessions/.../answer` | **403** |
| `GET /api/vocabulary-quiz/sessions/{실제 파일럿 세션}/result` | **403** |

### 5-5. 정답 사전 비노출

- `GET .../next` 응답의 `item` 객체를 4문항 전부 직접 검사 —
  `{correct_option, answer_text, accepted_answers, answers}` 중 **어느 필드도
  포함되지 않음**(선택형이라 `options`만 내려감).
- 정답·해설(`correct_answer`/`explanation`)은 **`POST .../answer` 이후
  응답에만** 나타남 — 조회(GET) 단계에서는 절대 노출되지 않음을 4문항 전부
  실측으로 확인.

### 5-6. 사후 정리 및 데이터 불변

- 테스트로 만든 세션(`vocabulary_multiformat_sessions`/
  `vocabulary_multiformat_responses`)과 비관리자 테스트 계정은 **테스트 종료
  즉시 삭제**했고, 서버에서 재조회해 실제로 0건임을 재확인했다.
- 테스트 전후 `vocabulary_contents`(5,820)/`vocabulary_content_levels`
  (5,820)/`vocabulary_multiformat_items`(1,329) 행 수 완전히 동일.

**5절 총 57건 체크 + 오답 경로 추가 확인, 전부 PASS, 실패 0건.**

---

## 6. 산출물

### 로컬(`C:\Users\aproa\aprolabs`, 커밋하지 않음)

- 본 보고서: `reports/schema_reading_phase18_quiz_pilot_apply_20260926.md`
- 스크립트: `scripts/vocab/phase18_build_rows_from_phase17.py`(rows-json 생성),
  `scripts/vocab/phase18_apply_quiz_pilot.py`(게이트 기반 단일 트랜잭션 적재)
- 테스트: `tests/test_phase18_quiz_pilot_admin_flow.py`(HTTP 레벨 기능 테스트,
  재실행 가능 — 실행 시 테스트 세션/계정을 스스로 만들고 정리함)
- 데이터: `data/import/schema_reading_phase18_quiz_pilot_rows_20260926.json`
  (실제 적재된 40건 rows-json, `phase18_build_rows_from_phase17.py` 재실행 시
  바이트 단위로 동일 재생성 확인함)

### 서버(`aprolabs`, research)

- 백업: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase18-quiz-pilot-pre-migration-20260926-064003`
- 작업 스크립트/체크섬/중간 산출물: `~/scratch/phase18_quiz_pilot/`
- 실제 변경: `vocabulary_quiz_research.db`의 `vocabulary_multiformat_items`에
  40행 신규 삽입(item_id `MF_A_SC_SRL4L5PILOT_20260925_*`/
  `MF_C_SC_SRL4L5PILOT_20260925_*`) — 그 외 기존 테이블/행 변경 없음(4절
  체크섬으로 증명)

---

## 7. 리스크 (우선순위 순)

1. **이 40건은 `source_version` 불일치로 관리자 일반 출제 흐름에서 완전히
   보이지 않는다** — 이는 의도된 안전장치이지만, 향후 실제로 관리자가 이
   문항들을 "정식으로" 연습해 보려면 `source_version`을 `2.1.29`로 바꾸는
   **의도적인 별도 마이그레이션**이 필요하다(자동으로 편입되지 않음). 이때
   반드시 함께 확인해야 할 것: (a) 20개 content_id의 `student_exposure`/
   `public_ready`를 여전히 0으로 유지할지 아니면 이 시점에 공개로 전환할지
   결정, (b) `vocabulary_content_levels.level_status`가 여전히
   `REVIEW_BOUNDARY`인 채로 괜찮은지(레벨모드 기본값 `all_candidates`에서는
   이미 노출 가능한 등급), (c) S(교과개념어)가 섞여 있지 않은지(이번 20개는
   전부 V, phase16 리스크 2 재확인).
2. **전문가(교과/국어) 최종 검수 미실시** — phase16/17이 이미 지적한 리스크가
   그대로 유지된다. 이번 세션은 "관리자가 이 문항으로 실제 놀아볼 수 있는
   상태로 만드는 것"까지만 했고, 사람 전문가 검수는 다음 단계 판단 사항이다.
3. **오답 선택지 다양성 제한**(phase16 리스크 3, 재확인만 하고 조치 안 함) —
   같은 배치 안 다른 표제어의 정의에서만 오답을 뽑는 방식이라, 배치 규모가
   커지면 오답 전략을 재검토할 필요가 있다.
4. **'정기' 문항의 해설 표현**은 phase17 4절이 이미 "되풀이" 뉘앙스를 제거하는
   방향으로 수정을 완료한 상태로 적재됐다(phase17 CSV 최신본 기준) — 추가
   조치 불필요.
5. 게이트 실패는 없었으므로 **롤백을 실행하지 않았다** — 백업(3-3절)은
   그대로 보존돼 있다.
