# 스키마리딩·어휘 퀴즈 DB 통합 — 1단계 기준선 보고서 (읽기 전용)

- 작성일: 2026-09-23
- 범위: `data/import/schema_reading_phase1_baseline_v1.zip` 지시문에 따른 읽기 전용 조사만 수행. **DB 마이그레이션, 원천 적재, 코드 배포, 학생 공개는 전혀 실행하지 않았다.**
- 확인된 git HEAD: 로컬 `68dc8c0` = 서버 `68dc8c0` (동일, 최신 배포와 일치)

## 1. 실제 환경/DB 경로 (추측 없이 확인)

| 항목 | 로컬 | 서버(연구용) |
|---|---|---|
| `APP_ENV` | 미설정(코드 기본값 사용) | `research` (.env 값 = 실행 중 uvicorn 프로세스 `/proc/<PID>/environ`과 byte-for-byte 일치, PID 1062904) |
| `VOCABULARY_QUIZ_DB_PATH` | 미설정 → `app/vocabulary_quiz/db.py`의 `_resolve_db_path()` 기본값 `data/vocab/vocabulary_quiz_rnd.db` | `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db` |
| `systemctl is-active aprolabs` | 해당 없음 | `active` |

두 경로 모두 `~/aprolabs_data/vocabulary_quiz/` 하위 연구용 DB 규칙과 일치한다. 운영/고객용 프로젝트 흔적 없음.

## 2. `vocabulary_%` 테이블 현황 (`audit_research_db.py` 결과 + 보충 조회)

패키지의 `audit_research_db.py`를 **읽기 전용**(`mode=ro`, `PRAGMA query_only=ON`)으로 로컬·서버 양쪽에서 실행했다. 서버 실행은 `/tmp/audit_research_db.py`에 스크립트만 업로드하고, DB에는 쓰기를 전혀 하지 않았다.

- `integrity_check`: 로컬/서버 모두 `['ok']`
- `foreign_key_check` 오류 건수: 로컬/서버 모두 `0`
- 발견된 `vocabulary_%` 테이블: 12개 (양쪽 동일)

핵심 5개 테이블 행 수 (로컬=서버 모두 동일):

| 테이블 | 행 수 |
|---|---|
| `vocabulary_contents` | 5,723 |
| `vocabulary_items` | 5,723 |
| `vocabulary_review_samples` | 500 |
| `vocabulary_multiformat_items` | 1,289 (CONTEXT_MEANING 300 / MEANING_CHOICE 300 / WORD_FROM_DEFINITION 299 / CONTEXT_CLOZE 215 / CROSSWORD 100 / MATCH_WORD_MEANING 75) |
| `vocabulary_content_levels` | 5,723 |

서버 전용(실사용 반영, 정상):
- `vocabulary_multiformat_sessions` 7건, `vocabulary_multiformat_responses` 61건, `vocabulary_quiz_sessions` 4건, `vocabulary_quiz_attempts` 80건 — 로컬은 대부분 0/소수. 원본 행 내용은 출력하지 않았다(도구 자체 제약 준수).

`generation_status`(서버 `vocabulary_contents`): `PRIVATE_SERVER_READY_CANDIDATE` 3,856 / `PRIVATE_SERVER_READY` 1,867. `student_exposure`=0, `public_ready`=0 (5,723건 전부) — 외부 공개 없음 확인.

## 3. `vocabulary_content_levels` 분포 — audit 도구 한계 발견

`audit_research_db.py`는 `distributions`를 하드코딩된 컬럼명 목록으로만 조회한다:

```python
categories = ("level", "service_level", "vocabulary_level", "source_version",
              "generation_status", "review_status", "student_exposure",
              "public_ready", "item_type")
```

그런데 이 프로젝트의 실제 컬럼명은 `vocab_level`/`level_status`이고, 목록에는 `vocabulary_level`만 있어 매칭되지 않는다(스크립트는 매칭 실패 시 조용히 건너뛴다). 그 결과 로컬·서버 audit JSON 모두 `vocabulary_content_levels.distributions`가 빈 `{}`로 나왔다 — **이건 데이터 문제가 아니라 패키지 스크립트의 컬럼명 목록 누락**이다. 이번 단계는 읽기 전용이라 스크립트를 수정하지 않고, 대신 보충 SQL로 직접 확인했다(서버, 읽기 전용):

```
vocab_level:   0=612, 1=1635, 2=1621, 3=1825, 4=30  (5=0, 6=0)
level_status:  PROVISIONAL_AUTO=3580, REVIEW_BOUNDARY=2143
level_version: level_policy_v0.1 = 5723 (전량)
is_active:     1 = 5723 (전량)
```

이전 어휘 레벨 적재 검증 보고서(`reports/vocabulary_level_import_verification_20260923.md`)의 수치와 정확히 일치한다.

## 4. 로컬↔서버 데이터 정합성 (내용 해시 비교)

`audit_research_db.py`의 원시 `sha256_rows`(전 컬럼 포함)는 로컬과 서버가 5개 테이블 모두 **다르게** 나왔다. 원인을 직접 조사한 결과, 전부 설명 가능한 정상적인 차이였다:

1. **`created_at`/`updated_at` 타임스탬프** — 동일 마이그레이션/임포트 스크립트를 로컬과 서버에서 서로 다른 시각에 실행했으므로 당연히 다르다. 이 두 컬럼을 제외하고 재해시하면:
   - `vocabulary_contents`: 로컬=서버 `0b085f61...` **일치**
   - `vocabulary_items`: 로컬=서버 `06eb999d...` **일치**
   - `vocabulary_content_levels`: 로컬=서버 `8cc45af7...` **일치**
2. **`vocabulary_review_samples`**: 타임스탬프 제외 후에도 101/500건이 달랐다 — 행별 diff 결과, 전부 `review_status`(`UNREVIEWED`→`PASS`)와 `review_note`가 서버에서만 채워져 있었다. `/vocabulary-quiz/review` 화면을 통해 **서버에서 실제로 101건이 검수됨** — 정상적인 온라인 사용 흔적이며 로컬에는 반영되지 않는 것이 맞다(로컬 DB로 되돌려 쓰지 않았음).
3. **`vocabulary_multiformat_items`**: 타임스탬프 제외 후에도 100건이 달랐으나, 원인은 내부 surrogate PK `id` 값이 로컬/서버 간 1씩 밀려 있는 것뿐(삽입 순서 차이로 인한 autoincrement 오프셋). `id`를 제외한 22개 컬럼 전체를 비교하면 **1,289건 전부 완전히 일치**(`item_id`, `public_payload_json`, `answer_payload_json` 포함). 기존 문항 데이터 훼손 없음.

**결론: 5개 핵심 테이블 모두 로컬↔서버 데이터가 (정상적으로 설명되는 차이를 제외하면) 완전히 일치한다.**

## 5. 학습도구어(V) vs 교과 스키마 개념어(S) 구분 필드

`app/vocabulary_quiz` 쪽(`vocabulary_contents`, `vocabulary_content_levels`, `vocabulary_multiformat_items`)에는 **V/S를 구분하는 필드가 전혀 없다.** 컬럼 목록을 직접 확인했고 관련 필드 없음을 재확인했다.

**다만 조사 중 중요한 사실을 발견했다**: 스키마리딩 원천자료는 이미 **완전히 별도의, 이미 배포되어 있는 모듈**에 적재가 끝나 있었다 — 아래 6절 참고. 그 모듈(`app/literacy`, `terms` 테이블)에는 `source` 컬럼 값으로 V/S가 사실상 구분된다:
- `source='schemareading-tooldict'` = 학습 도구어(V) 성격, 1,321건
- `source='schemareading-schema'` = 교과 스키마 개념어(S) 성격, 2,061건 (`subject_category`에 사회/과학/인문철학 대분류, `sense_category`에 중분류까지 있음)

즉 V/S 구분 자체는 **명시적 boolean 필드는 아니지만 `source` 값으로 이미 구현되어 있다** — 단, `vocabulary_quiz` 쪽이 아니라 `app/literacy` 쪽에 있고, 두 모듈 사이에는 어떤 연결(FK·공유 ID)도 없다.

## 6. week/교과(subject)/주제(topic) 연결 구조

`vocabulary_quiz` 쪽에는 없음(재확인).

`app/literacy.terms`에는 부분적으로 존재한다:
- `subject_category`(대분류, 예: 사회/과학/인문철학) — schema 소스 2,061건 100% 채움
- `sense_category`(중분류) — schema 소스 2,061건 100% 채움(퀴즈 오답 생성이 의존하는 필드라 반드시 채운다고 문서에 명시)
- "주차"(week) 정보는 **구조화된 컬럼이 아니라 `note`(자유 텍스트)에만 남아 있음** — 정식 week 연결 구조는 없음
- `schema_topics`/`schema_topic_terms`류의 정식 다대다 연결 테이블은 **존재하지 않는다** — `subject_category`/`sense_category`는 `terms` 행에 인라인된 텍스트 필드일 뿐, 별도 topic 엔티티가 없다

## 7. `app/literacy` 모듈 발견 — 원천자료는 이미 별도 시스템에 적재 완료 상태

README의 지시(5번)에 따라 "스키마리딩 참고자료.zip"과 "internal_vocab_upper_restore_v1.zip"을 로컬(`data/import/`, 저장소 전체)과 서버에서 찾았다. **두 파일명은 로컬·서버 어디에도 존재하지 않는다.** 대신 다음을 발견했다:

- `docs/literacy/04-스키마리딩어휘.md`, `docs/literacy/04-스키마리딩어휘적재.md` — "Phase 3-2" 작업 완료 보고서. 스키마리딩 원천자료(학습 도구어 사전 1,321건 + 스키마 어휘 목록 2,061건)가 **이미 적재되어 검증까지 끝난 상태**임을 설명한다.
- `raw/schema-reading/` (gitignored, git에는 없음, 로컬 디스크에만 존재):
  - `학습 도구어 사전 레벨1~6 (0925최종버전).xlsx`
  - `스키마 어휘 목록(레벨2~5 완성).xlsx`
  - `어휘퀴즈DB.xlsx` (기존 4지선다 문항 — 문서에 "이번 단계에서는 다루지 않음"으로 명시된 미적재 파일과 동일한 것으로 보임. 정확히 338건인지는 문서에만 언급되어 있고 이번 조사에서 직접 열어 세지는 않았다. `app/literacy.quiz_items`의 실제 행 수는 252건이며 이는 이 xlsx에서 온 것이 아니라 `sajaseongeo-pdf` 등 다른 source의 이력이다 — 즉 이 xlsx 유래 문항은 여전히 미적재 상태로 보인다.)
- `app/literacy/` — 완전히 독립된 모듈. 전용 DB(`data/literacy.db`), 전용 `Base`/`engine`/`SessionLocal`(`app/literacy/db.py`), 전용 라우터(`app/main.py`에 `literacy_admin`/`literacy_api` 등록되어 **이미 배포·마운트되어 있음**).
- `data/literacy.db`는 **서버에도 이미 존재**(`/home/chsh82/aprolabs/data/literacy.db`, 최종 수정 9/1). 로컬·서버 `terms` 행 수/소스별 분포/레벨 분포를 읽기 전용으로 대조한 결과 **완전히 일치**:

```
terms 총 7,312건
  krdict 2,884 / schemareading-schema 2,061 / schemareading-tooldict 1,321 /
  momo-textbook 821 / sajaseongeo-pdf 225
level 분포: None=165, 0=494, 1=1914, 2=2109, 3=848, 4=412, 5=677, 6=693
quiz_items 252건
```

**"internal_vocab_upper_restore_v1.zip"에 해당하는 파일은 어디서도 찾지 못했다.** 이 이름이 가리키는 자료가 `app/literacy`의 어느 source(예: `momo-textbook` 821건, 또는 `krdict` 2,884건)를 의미하는지, 아니면 아직 반입되지 않은 별도 자료인지 이번 조사만으로는 판단할 수 없다 — **막힌 지점**으로 보고한다. 다음 단계 전에 사용자 확인이 필요하다.

## 8. `terms.level`(0~6)과 `vocabulary_content_levels.vocab_level`(0~6)의 관계

두 값은 **이름만 같을 뿐 서로 다른 정책 버전으로 독립 산출된 값**이며, 현재 어떤 코드도 둘을 연결하지 않는다.
- `vocabulary_content_levels.level_version = 'level_policy_v0.1'` (5,723건 전부)
- `app/literacy.terms.level`에는 별도 버전 컬럼이 없다 — `docs/literacy/04-스키마리딩어휘.md`가 문서상 유일한 정책 근거다.

두 정책의 학년 대응표를 나란히 놓으면:

| 값 | `docs/literacy/04-스키마리딩어휘.md` (`terms.level`) | `app/vocabulary_quiz` 코드 `GRADE_LABELS` | 사용자가 이번 지시문에서 명시한 현재 서비스 기준 |
|---|---|---|---|
| 5 | 고1~2 | "고등 1~2학년" | **고1** |
| 6 | 고3 | "고등 3학년" | **고2·3** |

`terms.level` 정책과 `vocabulary_quiz` 코드의 `GRADE_LABELS`는 **서로 일치한다**(둘 다 L5=고1~2, L6=고3). 하지만 이번 지시문에서 사용자가 확정한 현재 서비스 기준(L5=고1, L6=고2·3)은 **이 두 기존 값 모두와 다르다.**

**이건 코드가 틀렸다고 단정할 문제가 아니라, 세 값 중 무엇이 맞는 최신 기준인지 사용자 결정이 필요한 지점이다.** 이번 단계는 읽기 전용이므로 `GRADE_LABELS`나 `04-스키마리딩어휘.md`를 임의로 고치지 않았다.

## 9. 관리자 전용 레벨별 퀴즈 — 실제 사용 테이블/조건 추적

`app/vocabulary_quiz/routers/multiformat.py` 기준(라인 번호는 현재 배포 커밋 `68dc8c0`):

```python
# L98-109
LEVEL_VERSION = "level_policy_v0.1"
VALID_LEVELS = frozenset(range(7))
CONFIDENCE_MODES = {
    "all_candidates": ("PROVISIONAL_AUTO", "REVIEW_BOUNDARY"),
    "auto_only": ("PROVISIONAL_AUTO",),
}
GRADE_LABELS = {0: "초등 1~2학년", ..., 5: "고등 1~2학년", 6: "고등 3학년"}
LEVEL_MODE_ITEM_TYPES = tuple(t for t in ITEM_TYPES if t != "CROSSWORD")
```

- `_matching_level_content_ids(db, level, confidence_mode)` (L192) — `VocabularyContentLevel`을 `level_version == 'level_policy_v0.1'` AND `vocab_level == level` AND `level_status IN CONFIDENCE_MODES[confidence_mode]` AND `is_active`로 필터링해 `content_id` 집합을 반환.
- `_select_level_candidates(db, level, item_types, confidence_mode)` (L203) — 위 집합을 이용해 단일 항목은 `source_content_id`가 집합에 포함되는지, 복합 항목(MATCH_WORD_MEANING)은 `source_content_ids_json`의 **모든** content_id가 집합에 포함되는지로 후보를 고른다. CROSSWORD는 애초에 `LEVEL_MODE_ITEM_TYPES`에서 제외되어 레벨 모드 후보에 들어오지 않는다.
- `GET /api/vocabulary-quiz/availability`(L230 부근) — 위 함수로 후보 수만 미리 계산해 보여주는 표시 전용 엔드포인트(세션 생성 없음).
- `POST /api/vocabulary-quiz/sessions`(L426~) — `level`/`confidence_mode` 유효성 검사 → 후보 수가 요청 문항 수보다 적으면 자동 혼합 없이 `409 INSUFFICIENT_LEVEL_CANDIDATES` → 세션에 `metadata_json`으로 `{selected_vocab_level, confidence_mode, level_version, requested_count, candidate_count, actual_count, item_types}` 저장.
- 즉 **레벨 필터링은 오직 `vocabulary_content_levels.vocab_level`/`level_status`/`level_version`/`is_active`에만 의존**하며, `app/literacy.terms.level`이나 스키마리딩 원천자료는 전혀 참조하지 않는다(참조할 연결 고리 자체가 없다).

## 10. 최소 스키마 변경안 (제안, 미적용)

통합 계획서(`schema_reading_vocabulary_integration_plan_v2.md`)가 제안한 `source_batches`/`source_entries`/`source_sense_links`/`schema_topics`/`schema_topic_terms`/`level_mapping_policy`/`legacy_quiz_candidates`를, **위에서 확인한 실제 테이블에 맞춰** 다음과 같이 재매핑할 것을 제안한다. 기존에 역할이 이미 있는 테이블은 새로 만들지 않는다.

| 계획서 제안 | 실제 대응/제안 | 근거 |
|---|---|---|
| `source_entries`(원천 행 보존) | **신규 불필요** — `app/literacy.terms`가 이미 이 역할(원천 원문 보존, `source`+`external_id` UNIQUE, `review_status`, `note`)을 수행 중. 재사용. | 7절 |
| `source_sense_links`(현재 의미 연결) | `vocabulary_quiz`가 필요로 하는 것은 "이 literacy term이 어떤 `vocabulary_contents.content_id`에 대응하는가"인데, **지금은 두 시스템을 잇는 컬럼이 전혀 없다.** 최소 변경: `vocabulary_contents`에 nullable `literacy_term_id INTEGER` 컬럼 하나만 추가(FK는 별도 SQLite 파일이라 물리적으로 불가 — 애플리케이션 레벨 참조 정수만). 신규 링크 테이블은 만들지 않는다. | 두 DB가 물리적으로 분리된 SQLite 파일이라는 제약(FK 불가)을 고려 |
| `schema_topics`/`schema_topic_terms` | **신규 필요** — 현재 `subject_category`/`sense_category`는 `terms`에 인라인된 텍스트일 뿐 정규화된 topic 엔티티가 없다(6절). 다만 이번 단계 지시문은 "실제 구조에 맞춘 최소안"만 요구하므로, 우선 `terms.subject_category`/`sense_category`를 그대로 유지하고 **정규화 테이블 신설은 실제 주제 기반 출제 요구가 구체화된 다음 단계로 미루는 것**을 제안한다(현재 어떤 화면도 subject/topic 필터를 쓰지 않음 — 과설계 방지). | YAGNI, 9절에서 확인한 현재 라우터는 subject/topic을 전혀 쓰지 않음 |
| `level_mapping_policy` | **신규 필요할 수 있음** — 8절에서 드러난 3-way 불일치(`terms.level` 정책 vs `vocabulary_content_levels` 정책 vs 사용자가 이번에 확정한 최신 기준)를 코드에 흩어진 상수(`GRADE_LABELS`)가 아니라 버전 관리되는 정책 테이블로 옮겨야 향후 재조정이 안전해진다. 단, **이번 단계에서 만들지 않는다** — 어떤 값이 맞는지부터 사용자 결정이 먼저다(8절). | 사용자 지시: "옛 단계 1~6을 현재 L0~6에 자동 매핑하지 말 것" |
| `legacy_quiz_candidates`(옛 338문항용) | **미적재 유지** — `raw/schema-reading/어휘퀴즈DB.xlsx` 유래 문항은 `app/literacy.quiz_items`에도 아직 들어가지 않았다(252건은 다른 source). 이번 지시문이 명시적으로 적재를 금지했으므로 스키마 설계만 다음 단계로 남긴다. | 금지 사항 명시 |
| `source_batches`(수집 이력) | **신규 불필요** — `app/literacy.collection_runs`(12건)가 이미 이 역할. | 3번 문서 확인 |

**결론: 이번 단계에서 실제로 필요한 최소 변경은 `vocabulary_contents`에 nullable 정수 컬럼 1개(`literacy_term_id`) 추가뿐**이며, 이것도 다음 단계(2단계) dry-run 설계 시점에 적용할 항목이지 이번 1단계에서는 적용하지 않았다.

## 11. 다음 단계(2단계) dry-run 임포터에 필요한 검증 기준 (계획만, 미구현)

- **행 수**: 대상 source(`schemareading-tooldict`+`schemareading-schema`, 이 조사 시점 3,382건)와 `literacy_term_id`가 채워질 `vocabulary_contents` 후보 수가 일치해야 함 — 단, `vocabulary_contents`(5,723건)와 `terms`(7,312건)는 표제어 집합이 다를 수 있으므로 매칭은 문자열 lemma 완전일치가 아니라 사람이 확인 가능한 대조표로 시작.
- **중복/누락**: `terms.headword`가 `vocabulary_contents.lemma`와 겹치되 `source`가 다른 66건(5절 문서에 명시)은 **병합 금지** 규칙을 그대로 승계 — 동형이의어 오염 방지.
- **멱등성**: `literacy_term_id` 백필은 자연키(`vocabulary_contents.content_id`) 기준 upsert로, 재실행 시 `inserted=0/updated=0/unchanged=N`이 나와야 함.
- **레벨 정책 확정 전 게이트**: 8절의 L5/L6 불일치가 해소되기 전에는 `terms.level`을 `vocab_level`에 자동 반영하지 않는다 — 사용자 지시("자동 매핑 금지")를 다음 단계에서도 명시적 가드(예: `--confirm-level-policy` 플래그 없이는 실행 거부)로 코드화할 것을 제안.
- **노출 금지 게이트**: 기존 패턴대로 `student_exposure`/`public_ready`를 절대 건드리지 않는 assertion을 dry-run 스크립트에도 포함.

## 12. 막힌 지점 / 사용자 결정 필요 항목

1. **"internal_vocab_upper_restore_v1.zip"의 정체 불명** — 로컬·서버 어디서도 찾지 못함. `app/literacy`의 기존 source(`momo-textbook` 821 / `krdict` 2,884) 중 하나를 가리키는 것인지, 별도로 준비 중인 자료인지 확인 필요.
2. **L5/L6 학년 대응 확정 필요** — `terms.level` 정책 문서와 `vocabulary_quiz` 코드의 `GRADE_LABELS`는 서로 일치(L5=고1~2/L6=고3)하지만, 이번 지시문에서 사용자가 명시한 "현재 서비스 기준"(L5=고1/L6=고2·3)과는 다르다. 어느 쪽이 최신 권위 기준인지 확정되어야 `level_mapping_policy` 설계와 2단계 dry-run을 시작할 수 있다.
3. `raw/schema-reading/어휘퀴즈DB.xlsx`(338문항 후보로 추정)의 정확한 문항 수를 이번 조사에서는 열어 세지 않았다 — 열람 자체가 "적재"는 아니지만, 이번 단계 범위(구조 조사) 밖이라 확인하지 않고 남겨둔다.

이 세 가지를 제외하면 이번 1단계 조사는 막힘 없이 완료되었다.
