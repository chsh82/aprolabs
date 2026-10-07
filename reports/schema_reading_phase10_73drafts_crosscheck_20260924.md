# 스키마리딩x어휘 DB 통합 — 10단계: 73건 초안 행별 대조·L4~L6 V/S 재집계·GRADE_LABELS 코드 위치·작업 대기열 (읽기 전용)

- 작성일: 2026-09-24 (`date` 명령으로 시스템 현재 날짜 확인)
- 범위: `reports/schema_reading_phase9_l4_l6_baseline_20260924.md`(9단계)가 확립한
  사실(vocab_level 5/6=0건, vocab_level 4=30건/출제가능 8건, literacy.db V/S 소스
  정의, S level4·5=730건 AI0, S level6 검수완료 576건=전부 AI 자동생성 태그,
  V/S 전체 사람 검수 0건 등)은 재조사하지 않고 그대로 인용·재사용했다.
- **이번 세션의 DB 접근도 전부 읽기 전용이다.** 로컬 `data/literacy.db`는
  `mode=ro`+`PRAGMA query_only=ON`으로만 열었다. 서버
  `vocabulary_quiz_research.db`도 SSH에서 `sqlite3 -readonly`로만 열었다.
  INSERT/UPDATE/DELETE/DDL을 코드에 전혀 포함하지 않았다. 73건을 자동
  적재하거나 검수 완료로 표시하지 않았다. 레벨 재분류·학생 공개·git
  commit/push·배포 전부 수행하지 않았다.

---

## 0. 핵심 요약 (먼저 읽는 사람용)

1. **73건은 전부 학습도구어(V, `schemareading-tooldict`) L4/L5 초안이며, L6은
   0건, 스키마 개념어(S)는 0건이다** — 73건(L4 39/L5 34) 원본 zip
   (`internal_vocab_upper_restore_v1/data/student_content_drafts_73_v1.json`)
   자체 QA 파일이 이미 `by_original_level: {L4:39, L5:34}`, `errors: []`로
   기록하고 있고, 이번 세션이 literacy.db와 직접 대조해도 동일하게 확인됐다.
2. **73건 전부(100%)가 literacy.db V(`schemareading-tooldict`)에서 표제어+품사
   일치를 찾았다** — `NO_MATCH`·`SOURCE_HAS_BUT_EXCLUDED`는 0건이다. 그 중
   **71건은 뜻풀이 핵심 의미까지 실제로 일치(FULL_MATCH)**, **2건("부여",
   "수집")은 표제어·품사는 같지만 literacy.db 정의가 73건 패키지 동봉
   822건 감사자료의 `source_definition`/`atomic_definition`과 범위·표현이
   달라 자동으로 동일 의미로 확정할 수 없어 `HEADWORD_ONLY_MATCH`로
   내렸다**(1-3절).
3. **73건은 vocabulary_quiz `vocabulary_contents`(5,723건, `is_active` 무관 전수)와
   표제어 일치가 0건이다** — 서버에서 직접 재쿼리해 확인했다(1-4절). 즉 73건은
   "서버에 이미 준비된 문항"과 전혀 무관하며, 지금 당장 출제에 보탤 수 있는
   건수는 0건이다(4절 요약표 참고).
4. **73건 패키지는 literacy.db·vocabulary_quiz DB와 완전히 독립적으로 만들어진
   별도 내부 작업물이다** — 패키지 자체 문서(`SCHEMA_READING_REFERENCE_REVIEW.md`,
   `CONTINUOUS_WORK_REPORT.md`)가 "기존 v2.1.29 5,723개 콘텐츠와 표제어 일치
   0행"이라고 이미 밝히고 있고, 이번 세션이 서버 재쿼리로 그대로 재확인했다.
   literacy.db와는 이번 세션이 처음 대조했다 — 그 결과가 위 2번이다.
5. **L4·L5의 literacy.db V/S(합 1,616건)는 원문 뜻풀이가 100% 보존돼 있고
   AI 자동생성 태그가 없다**(phase9 재확인, 2절). 73건은 이 중 V(tooldict)
   L4/L5 267건(단일 의미 일치 후보)의 일부로서 **이미 학생용 뜻풀이·예문
   초안이 작성된 상태**이지만, 서버 canonical 의미와의 재검증·의미 QA를
   거치지 않았고 `student_exposure=0`, `public_ready=0`, `current_content_id=null`
   상태다 — **"확인 가능한 후보"이지 "출제 가능한 문항"이 아니다.**
6. **S(schema) level=6의 "검수완료" 576건은 이번 세션도 phase9와 동일하게
   미검증으로 재분류했다** — 94.7%가 `[AI 자동 생성 뜻풀이]` 태그를 갖고
   있어 사람 검수가 아니다(2절). S level=4·5(730건)도 AI 태그는 0건이지만
   `review_status`가 100% `검수전`이므로 "검수됨"이 아니라 "미검증(표본검수
   우선 대상)"으로 그대로 유지했다.
7. **GRADE_LABELS류 학년 라벨 불일치 코드는 최소 3곳**(`app/vocabulary_quiz/
   routers/multiformat.py`의 `GRADE_LABELS`, `scripts/literacy/
   auto_review_level.py`의 `LEVEL_TABLE`, `app/literacy/migrations/
   002_add_level.py`의 마이그레이션 주석)이며, 특히 `auto_review_level.py`의
   `LEVEL_TABLE`은 실제 Gemini 프롬프트에 그대로 주입돼 **앞으로 생성되는
   속담·관용구·사자성어의 `level` 값에 옛 경계("고1~2"/"고3")가 계속 반영되고
   있다** — vocabulary_quiz 쪽보다 오히려 이쪽이 "현재 진행형으로 잘못된 값을
   만들어내고 있다"는 점에서 더 시급하다(3절).

---

## 1. 작업 1 — 73건 행별 대조

### 1-1. 원본 파일 확인 및 SHA-256 재확인 결과

- 파일: `data/import/internal_vocab_upper_restore_v1.zip`
  (로컬 크기 773,687바이트, 수정시각 2026-09-23 23:56)
- **SHA-256 재계산 결과가 사용자가 제공한 값과 일치하지 않는다**:
  - 이번 세션 실측: `ed83a8f9cc8d577780f243c9e95b38a7b01b90da00b53f988d137dc54ccda389`
  - 사용자 제공(메모): `05060bf917681fada39e41e8a93a398b257901562c8ae6ce652f2459db5ca4f0`
  - 두 값 모두 64자 정상 길이의 SHA-256 문자열이지만 **완전히 다른 해시다.**
    파일이 두 시점 사이에 재압축됐거나(zip은 내부 타임스탬프·순서에 따라
    바이트가 달라질 수 있음), 사용자가 이전에 확인한 파일과 현재 파일이
    다를 가능성이 있다. **이 불일치를 억지로 무시하지 않고 4-4절 리스크에
    최우선으로 남긴다.** 다만 zip 내부에 사용자가 지목한 두 파일
    (`student_content_drafts_73_v1.json`,
    `academic_L4_L6_sense_audit_822_v1.jsonl`)이 정확한 경로·이름으로
    실재하고, 내용도 사용자 설명(L4 39/L5 34=73건, 822건 의미 감사)과
    정확히 일치했으므로, **내용 기준으로는 사용자가 지목한 파일이 맞다고
    판단하고 그대로 사용했다** — 다만 해시 재확인은 다음에 사용자가 직접
    한 번 더 해보는 것을 권장한다.
- 73건 파일: `internal_vocab_upper_restore_v1/data/student_content_drafts_73_v1.json`
  — 필드: `source_entry_id, lemma, pos, original_level, candidate_lexical_entry_id,
  candidate_sense_id, student_definition_draft, example_sentence_draft,
  target_form, generation_status, current_content_id, student_exposure,
  public_ready, required_next_gate`. 전 행 `current_content_id=null`,
  `student_exposure=0`, `public_ready=0`(패키지 자체가 비공개·미출제 상태임을
  스키마로 강제).
- 822건 파일: `internal_vocab_upper_restore_v1/data/academic_L4_L6_sense_audit_822_v1.jsonl`
  — 필드: `source_entry_id, lemma, pos, source_level, source_definition,
  atomic_definition, historic_candidate_sense_id, historic_candidate_lexical_entry_id,
  historic_master_word_id, server_content_id, service_level_candidate,
  verdict(SAME_SENSE_CANDIDATE/AMBIGUOUS_REVIEW/NO_VERIFIED_LINK),
  reason_codes, level_mapping_status, definition_part_count`.
  `verdict` 분포: SAME_SENSE_CANDIDATE 334, AMBIGUOUS_REVIEW 359,
  NO_VERIFIED_LINK 129(합 822). `source_level` 분포: L4 316, L5 313, L6 193.
  **`server_content_id`는 822건 전부 `null`**이다 — 이 패키지 자체가 작성될
  때부터 vocabulary_quiz 서버 콘텐츠와 연결된 적이 없다는 뜻.
- **연결 키**: 두 파일은 `source_entry_id`(예: `INTERNAL_ACADEMIC:Level4:63`)로
  1:1 대응한다. 73건 각각의 `source_entry_id`로 822건 파일을 조회하면
  전부 `verdict='SAME_SENSE_CANDIDATE'`인 행이 정확히 하나씩 나온다
  (`assemble_drafts_73.py`가 이 불변 조건을 자체 검증하고 있음을 코드로
  확인했다 — `UNVERIFIED_SOURCE_SENSE`/`SENSE_ID_MISMATCH` 등 오류가 0건).

### 1-2. literacy.db 대조 방법

로컬 `data/literacy.db`(mode=ro)에서 73건의 `lemma` 73개(중복 없음, 전부
고유)로 `terms.headword IN (...)` 직접 조회 → 매칭 75행(스키마 일치 아님,
동음이의 등으로 1개 표제어에 여러 행 존재 가능). 소스별 매칭 분포:
`schemareading-tooldict` 73건, `momo-textbook` 1건("생략" 중복), `schemareading-schema`
1건("시각" — 별도 동음이의어, 1-3절 참고). **73개 표제어 전부 `schemareading-tooldict`(V)에서
최소 1개 행을 찾았다(누락 0건)**, 그 중 73건 모두 `pos`도 초안과 동일했다
(품사 불일치 0건).

### 1-3. 4개 상태 분류 결과

| 상태 | 건수 | 비고 |
|---|---:|---|
| **FULL_MATCH** | **71** | 표제어·품사·핵심 뜻풀이가 literacy.db V 정의 및 822건 감사자료의 `atomic_definition`과 실제로 일치(아래 예시 참고) |
| **HEADWORD_ONLY_MATCH** | **2** | "부여"(`INTERNAL_ACADEMIC:Level5:210`), "수집"(`INTERNAL_ACADEMIC:Level5:302`) — 표제어·품사는 일치하나 뜻풀이 범위가 다름(아래 상세) |
| **SOURCE_HAS_BUT_EXCLUDED** | **0** | literacy.db V/S에서 `review_status='제외'`인 행 자체가 0건(전수 확인, 2-1절) — 73건 어느 것도 정책 제외 이력과 겹치지 않음 |
| **NO_MATCH** | **0** | 73개 표제어 전부 literacy.db V에서 매칭됨 |

전체 행별 상세는 다음 파일에 저장했다(73행 전부, `status`/`reason` 포함):

- `data/import/schema_reading_phase10_73drafts_crosscheck_20260924.csv`
- `data/import/schema_reading_phase10_73drafts_crosscheck_20260924.jsonl`

두 파일의 컬럼: `source_entry_id, lemma, pos_draft, original_level_pkg, status,
reason, candidate_sense_id, audit_verdict, audit_historic_candidate_sense_id,
audit_atomic_definition, audit_source_definition, student_definition_draft,
example_sentence_draft, literacy_db_term_id, literacy_db_source,
literacy_db_pos, literacy_db_definition, literacy_db_review_status,
literacy_db_level, literacy_db_all_matches_count, vocabulary_quiz_match`.

**FULL_MATCH 판정 근거(예시, 실제 대조한 문장 그대로)**:

| 표제어 | 초안(학생용) | literacy.db V 정의 | 822건 audit atomic_definition |
|---|---|---|---|
| 원칙 | 어떤 일을 할 때 계속 지켜야 하는 기본 규칙 | 어떤 행동이나 이론 따위에서 일관되게 지켜야 하는 기본적인 규칙이나 법칙. | (좌와 동일) |
| 규칙 | 여러 사람이 함께 지키기로 정한 약속이나 질서 | 여러 사람이 다 같이 지키기로 작정한 법칙. 또는 제정된 질서. | (좌와 동일) |
| 상상 | 실제로 보거나 겪지 않은 일을 마음속에 그려 보는 것 | 실제로 경험하지 않은 현상이나 사물에 대하여 마음속으로 그려 봄. | (좌와 동일) |

71건 모두 이 패턴이다: literacy.db V 정의와 822건 감사자료의
`atomic_definition`/`source_definition`이 **문자 그대로 동일**했고, 초안의
학생용 뜻풀이는 그 사전적 정의를 쉬운 말로 바꾼 것으로 핵심 의미가 어긋나지
않았다(직접 71건 전부 읽고 대조함, 표제어 문자열만 보고 판정하지 않음).

**HEADWORD_ONLY_MATCH 2건 상세(왜 강등했는지)**:

- **부여**(`INTERNAL_ACADEMIC:Level5:210`): 초안 뜻풀이 "누군가에게 역할이나
  권한을 나누어 주는 것" / 822건 감사자료의 `atomic_definition`=`source_definition`은
  "나누어 줌."(매우 짧은 사전 정의). 그런데 **literacy.db V의 실제 정의는
  "사람에게 권리, 명예, 임무 따위를 지니도록 해주거나, 사물이나 일에 가치,
  의의 따위를 붙여 줌."**으로 훨씬 구체적이고 범위가 다르다. 822건 감사자료가
  참조한 원천(내부 학습도구어 사전 xlsx의 짧은 정의)과 literacy.db에 실제
  적재된 정의(더 긴 표준국어대사전류 정의로 추정)가 서로 다른 버전/출처일
  가능성이 있어, "literacy.db 기준으로 뜻까지 일치"를 자동 확정할 수 없다.
- **수집**(`INTERNAL_ACADEMIC:Level5:302`): 초안 "여러 곳에서 필요한 것을
  거두어 모으는 것" / 822건 감사자료 정의 "거두어 모음."(짧음) vs
  **literacy.db V 실제 정의 "취미나 연구를 위하여 여러 가지 물건이나 재료를
  찾아 모음. 또는 그 물건이나 재료."**(취미·연구 맥락 한정) — 마찬가지로
  범위가 다르다.
- 두 건 모두 표제어·품사는 확실히 일치하고 큰 틀의 의미(주다/모으다)도
  겹치지만, **"학습도구어 사전 원본"과 "literacy.db에 실제 적재된 정의"가
  서로 다른 버전으로 보인다는 새로운 사실**을 발견한 것이므로, 사용자의
  "표제어 문자열만 보고 판정하지 말 것" 원칙에 따라 자동으로 FULL_MATCH로
  올리지 않고 사람 재검수 대상으로 남겼다.

**참고(동음이의어 주의, 판정에는 영향 없음)**: "시각"(`INTERNAL_ACADEMIC:Level5:307`)은
literacy.db에서 V(관점 뜻, 초안과 일치)와 S(`schemareading-schema` level4,
"눈으로 빛을 받아들이는 감각"이라는 전혀 다른 뜻) 양쪽에 동음이의 표제어가
있다. 초안은 V쪽 뜻과 정확히 일치해 FULL_MATCH로 판정했지만, **향후 표제어
기준으로만 콘텐츠를 연결하면 이런 동음이의 충돌이 재발할 수 있다**는
구조적 위험을 남겨 둔다(4-4절 리스크 2).

### 1-4. vocabulary_quiz `vocabulary_contents`(5,723건) 대조 — 0건 확인

서버 `vocabulary_quiz_research.db`(`sqlite3 -readonly`)에서 73개 표제어
전부를 `vocabulary_contents.lemma IN (...)`로 직접 재쿼리(`is_active` 조건
없이 전수) → **매칭 0행**. 73건 패키지 자체 문서가 주장한 "5,723개 콘텐츠와
표제어 일치 0행"을 이번 세션이 서버에서 독립적으로 재현·확인했다. 즉 73건은
vocabulary_quiz 쪽에는 아무런 흔적도 없다 — **73건의 잠재적 연결 대상은
literacy.db뿐이다.**

---

## 2. 작업 2 — L4~L6 V/S 재집계(미검증 재분류 반영) + 빈 구간

phase9(2-1·2-2·2-3절)의 V/S 소스 정의(`schemareading-tooldict`=V,
`schemareading-schema`=S, 예외 0건)와 "검수완료 710건 전부 AI 자동생성
태그"라는 사실을 그대로 재인용한다. 이번 세션은 그 위에 **(a) L4~L6만
따로 다시 집계**하고 **(b) 사용자가 이번에 명시한 재분류 규칙**(S
level4·5의 AI태그 0건을 "검수됨"으로 부르지 않음, S level6 "검수완료"
576건을 AI생성 근거로 미검증 재분류)을 적용했다.

### 2-1. L4~L6 V/S 건수 및 상태(재분류 반영)

| 레벨(원본) | V(tooldict) | V 상태 | S(schema) | S 상태(재분류 반영) |
|---:|---:|---|---:|---|
| 4 | 85 | 100% 검수전, AI태그 0(미검증·원문 신뢰 가능) | 223 | 100% 검수전, AI태그 0(미검증·원문 신뢰 가능, "검수됨" 아님) |
| 5 | 108 | 100% 검수전, AI태그 0(미검증·원문 신뢰 가능) | 507 | 100% 검수전, AI태그 0(미검증·원문 신뢰 가능, "검수됨" 아님) |
| 6 | 85 | 84건 검수전+AI0 / **1건은 `review_status='검수완료'`이지만 AI태그 있음 → 미검증 재분류** ⇒ **85건 전부 미검증** | 608 | **576건(94.7%) `review_status='검수완료'`이나 전부 AI 자동생성 태그 → 미검증 재분류. 32건만 AI 미태그(진짜 원본, 상대적 우선순위 최상단)** ⇒ **608건 전부 미검증, 그 중 32건이 "AI 오염 없는" 서브그룹** |
| 합계 | 278 | | 1,338 | |

뜻풀이 결측은 이 1,616건(V 278+S 1,338) 전체에서 0건(phase9 전수 확인
재인용). **"사람이 실제로 검수완료 처리한 행"은 V/S 3,382건 전체에서
0건**(phase9 3-3절 재인용) — 이번 세션도 이 원칙을 그대로 지켜, S
level6 576건과 V level6 1건을 review_status 값과 무관하게 전부
"미검증"으로 집계했다.

### 2-2. 빈 구간 — 교과·주차·주제

S(schema)만 `subject_category`·주차(`note`의 "주차: N주차")·소분류(주제)
패턴이 있다(V는 이 구조 자체가 없음, 2-3절).

| 레벨 | 교과 분포 | 주차 결측 | 소분류(주제) 수 |
|---:|---|---|---:|
| 4 | 사회 121 / 과학 102 / **인문 철학 0건** | 1~20주차 전부 존재(결측 없음) | 42개 |
| 5 | 사회 216 / 과학 263 / 인문 철학 28 | 사회·과학은 1~20주차 전부 존재. **인문 철학은 1~4주차만 있고 5~20주차(16개 주차) 결측** | 44개 |
| 6 | 사회 304 / 과학 304 / **인문 철학 0건** | 1~20주차 전부 존재(결측 없음) | 40개 |

**새로 확인한 빈 구간(이번 세션 신규 발견)**:

1. **인문 철학 교과가 L4·L6에는 아예 없다(0건)** — S(schema) 전체
   2,061건 중 인문 철학은 L5의 28건이 전부다. L4·L6에서 "개인과 국가",
   "민법" 같은 인문·사회사상 계열 주제를 다루려면 새 콘텐츠 제작이
   필요하다(기존 S 재활용 불가).
2. **L5 인문 철학은 28건 전부가 1~4주차에만 몰려 있고 5~20주차가 완전히
   비어 있다** — 20주차 커리큘럼을 인문 철학까지 채우려면 이 구간이
   가장 시급한 공백이다.
3. 사회·과학은 L4/L5/L6 모두 20주차 전 구간에 최소 1건 이상 있어
   상대적으로 안전하지만, 소분류(주제) 단위로는 최소 빈도 주제들이
   존재한다(예: L4 42개 소분류 중 다수가 6~7건에 불과해 주제당 문항
   다양성은 낮음).

### 2-3. V(tooldict)의 구조적 한계 — 주차/교과 개념 자체가 없음

L4~L6 V 278건(`external_id` L4-NNN/L5-NNN/L6-NNN 형식)을 전수 확인한 결과
**주차/소분류 패턴이 0건**이었다(phase9가 확인한 "나선형 반복" 재출현
표시도 L4~L6 구간에는 0건 — 해당 표시는 하위 레벨에만 있었다). 즉 V는
애초에 "과목 중립 범용 도구어"로 설계돼 주차·교과 커리큘럼과 묶이지
않는다 — 이것은 빈 구간이라기보다 **V 축의 원래 설계**이므로 S와 같은
기준으로 "빈 구간"을 논할 수 없다는 점을 명시한다.

---

## 3. 작업 3 — GRADE_LABELS 등 출제 전 수정 대상 코드 위치

저장소 전체(`app/vocabulary_quiz/`, `app/literacy/`, `scripts/literacy/`,
`scripts/vocab/`, `tests/`, `docs/`)를 `GRADE_LABEL`, `고등 1~2학년`,
`고등 3학년`, `고1~2`, `고2~3` 패턴으로 전수 grep한 결과, **확정 기준
(L5=고1, L6=고2·3)과 다른 옛 경계("고1~2"/"고3")가 남아 있는 위치는
아래 3곳이다**(`scripts/vocab/`에는 매칭 없음 — 폴더 자체가 없거나 관련
상수가 없음).

| 파일 | 위치 | 내용 | 실제 사용처 | 심각도 |
|---|---|---|---|---|
| `app/vocabulary_quiz/routers/multiformat.py` | L105-108 `GRADE_LABELS` 딕셔너리(`5: "고등 1~2학년", 6: "고등 3학년"`) | L246(`_level_availability` API 응답 `grade_label`), L720(`create_session` 결과의 `level_info.grade_label`), L753(`play_page`가 템플릿에 `level_grade_labels`로 그대로 전달) | 관리자 화면(`multiformat_play.html` L15 레벨 선택기, L599 결과 텍스트)에 옛 라벨이 그대로 노출됨 | 중간 — 현재 L5/L6 데이터가 0건이라 실사용 노출은 없지만, 데이터가 채워지는 순간 바로 노출됨 |
| `scripts/literacy/auto_review_level.py` | L48-56 `LEVEL_TABLE` 문자열 상수(`5: 고1~2`, `6: 고3`) | **L78에서 `build_prompt()`가 이 표를 그대로 Gemini 프롬프트에 주입** — 속담·관용구·사자성어(`level IS NULL`)의 `level` 값을 AI가 이 표를 기준으로 판정해 자동 확정함(사람 확인 없음, 스크립트 docstring 자체가 명시) | 이 스크립트를 다시 돌리면 **지금도 옛 경계 기준으로 새 `level` 값이 계속 생성된다** | **가장 시급** — 유일하게 "현재 진행형으로 잘못된 값을 만들어내는" 코드 경로 |
| `app/literacy/migrations/002_add_level.py` | L10-14 마이그레이션 설계 주석(`level 5 = 고1~2`, `level 6 = 고3`) | 코드 로직에는 영향 없음(주석/독스트링), 과거 마이그레이션 기록 | 낮음 — 이미 적용된 과거 마이그레이션의 설계 근거 기록이라 수정 실익이 적음(과거 기록을 사후 수정하면 오히려 이력 왜곡 위험) |

**참고 위치(코드 아님, 문서)**: `docs/literacy/04-스키마리딩어휘.md`(L70-73)와
`docs/literacy/04-스키마리딩어휘적재.md`(L24-27)에도 동일한 옛 경계
표가 있다 — 코드는 아니지만 향후 구현자가 참고할 문서이므로 GRADE_LABELS
수정 시 같이 갱신 대상에 포함해야 한다.

**관련 테스트 파일**: `tests/test_admin_level_quiz.py`(L312)는 `grade_label`을
직접 assert하지만 **level=1("초등 3~4학년")만 검사**하므로 L5/L6 라벨을
고치더라도 이 테스트는 깨지지 않는다(회귀 위험 낮음 — 현재 L5/L6 데이터가
0건이라 애초에 이 경로를 테스트하지 못하고 있다는 뜻이기도 하다).
`tests/test_vocabulary_level_import.py`(L44)는 `target_grade_label`이라는
별도 필드(레벨 0 고정값)를 테스트 픽스처로 쓸 뿐 `GRADE_LABELS`와 무관하다.
**둘 다 이번 조사에서는 수정하지 않았다**(코드 수정 자체가 이번 작업
범위 밖).

**주의**: 위 위치 목록만 보고했고, 실제 코드 수정이나 literacy.db의 기존
`level` 숫자 값 일괄 재배정은 수행하지 않았다(사용자 지시에 따라 범위
밖으로 남김).

---

## 4. 작업 4 — L4/L5/L6 작업 대기열 (3분류) + 73건 실제 연결 가능 수

### 4-1. 3단계 대기열

| 레벨 | 현행 의미 확인 가능 | 학생용 콘텐츠 작성 필요 | 원천 근거 부족 |
|---|---|---|---|
| **L4** | V 85건(AI0) + S 223건(AI0) = **308건**, 뜻풀이 100% 원문 보존 | 308건 중 학생용 초안 있는 건 **39건**(73건 중 L4분, V만) → **269건 미작성**(V 46건+S 223건) | 0건(L4는 V/S 전부 뜻풀이 존재·AI미생성) |
| **L5** | V 108건(AI0) + S 507건(AI0) = **615건**, 뜻풀이 100% 원문 보존(단 S 인문철학 28건은 5~20주차 공백이라 커리큘럼상 활용 폭이 좁음) | 615건 중 학생용 초안 있는 건 **34건**(73건 중 L5분, V만) → **581건 미작성**(V 74건+S 507건) | 0건 |
| **L6** | V 85건 + S 32건(AI 미태그 "진짜" 원본) = **117건** | 117건 전부 학생용 초안 **0건**(73건은 L6을 포함하지 않음) | **S 576건**(AI 자동생성 뜻풀이, 사람 검증 수단 없음 — phase6 GROUNDED=0 원칙 재적용) |

(참고: krdict/사자성어의 AI_ONLY 2,872건·POLICY_CONFLICT 62건은 phase6·phase9가
이미 확인한 대로 V/S와 표제어 교집합이 0건이라 이 표에서 제외했다 — 별도
트랙.)

### 4-2. 73건의 실제 연결 가능 수와 사유 (대기열과 별도로 명확히 제시)

| 구분 | 건수 | 사유 |
|---|---:|---|
| FULL_MATCH — 즉시 "확인된 후보"로 활용 가능 | **71** | 표제어·품사·핵심 뜻풀이가 literacy.db V 정의 및 822건 감사자료 `atomic_definition`과 일치. **다만 "즉시 출제 가능"은 아니다** — 서버 `content_id` 발급, `vocabulary_content_levels` 신규 레벨(v0.1의 L5/L6 라벨 문제 선해결 필요), 별도 사람 의미 QA·학생 노출 승인이 전부 남아 있다 |
| HEADWORD_ONLY_MATCH — 별도 정책/재검수 판단 필요 | **2** | "부여"·"수집" — literacy.db 실제 정의가 822건 감사자료가 근거로 든 정의와 범위가 달라(1-3절), 자동으로 동일 의미 확정 불가. 사람이 두 정의 중 어느 쪽을 기준으로 삼을지 정책 결정을 먼저 해야 한다 |
| SOURCE_HAS_BUT_EXCLUDED | 0 | literacy.db V/S에 review_status='제외' 이력 자체가 없음(2-1절 재확인) |
| NO_MATCH | 0 | 73개 표제어 전부 literacy.db V에서 매칭됨 |

**결론**: 73건 중 **71건**이 literacy.db 기준으로 "확인된 후보"이지만,
**73건 전부가 vocabulary_quiz 서버에는 0% 연결돼 있다**(1-4절). 즉 73건이
당장 벌어주는 것은 "새로 만들 콘텐츠의 초안 작업량 절감"이지, "지금 출제
가능한 문항 수 증가"가 아니다.

### 4-3. "이미 준비된 문항 수" vs "확인 후 새로 만들 수 있는 후보 수" — 핵심 구분표

| 구분 | vocab_level 4 | vocab_level 5 | vocab_level 6 |
|---|---:|---:|---:|
| **(A) 서버에 이미 준비된 문항 수**(vocabulary_quiz, `all_candidates` 기준, phase9 1-3절 재인용) | **8건**(auto_only는 0건) | **0건** | **0건** |
| **(B) 확인 후 새로 만들 수 있는 후보 수 — literacy.db V/S 중 AI 미생성·뜻풀이 원문 보존분** | 308건(V85+S223) | 615건(V108+S507) | 117건(V85+S32, S576 AI생성분 제외) |
| **(B) 중 이미 학생용 초안까지 작성된 것(73건 내역)** | 39건(V만) | 34건(V만) | 0건 |
| **(B) 중 초안조차 없는 것** | 269건 | 581건 | 117건 |
| **(C) 원천은 있으나 근거 부족(AI 자동생성, 사람검증 수단 없음)** | 0건 | 0건 | 576건(S) |

**(A)와 (B)는 전혀 다른 파이프라인 단계에 있는 숫자다** — (A)는
vocabulary_quiz DB에 이미 `content_id`가 발급되고 레벨 태깅까지 끝나
관리자가 지금 바로 플레이할 수 있는 문항이고, (B)는 literacy.db에
원문 뜻풀이는 있지만 vocabulary_quiz로 전혀 이관되지 않아 **신규
마이그레이션·의미 QA·레벨 라벨 정정(3절)을 전부 거쳐야 (A)로 전환되는
후보군**이다. 73건은 (B) 안에서 "초안 작업"이라는 한 단계를 먼저 끝낸
부분집합일 뿐, (A)에는 어떤 기여도 하지 않는다.

### 4-4. 가장 중요한 리스크 (우선순위 순)

1. **73건 zip 파일의 SHA-256이 사용자가 사전에 확인한 값과 다르다**(1-1절) —
   내용은 사용자 설명과 정확히 일치해 그대로 사용했지만, 해시 불일치의
   원인(재압축/버전 차이)을 사용자가 직접 한 번 더 확인해야 한다. 이
   불일치를 무시하고 넘어가면 나중에 "어느 버전이 검증된 버전인지"를
   특정할 수 없게 된다.
2. **`scripts/literacy/auto_review_level.py`가 지금도 옛 학년 경계
   ("고1~2"/"고3")로 새 `level` 값을 계속 만들어내고 있다**(3절) — 이
   스크립트를 재실행하기 전에 `LEVEL_TABLE`부터 고쳐야, 앞으로 생성되는
   속담·관용구·사자성어 레벨이 확정 기준(L5=고1, L6=고2~3)과 어긋나지
   않는다. vocabulary_quiz의 `GRADE_LABELS`는 데이터가 0건이라 상대적으로
   덜 급하지만, 이쪽은 "지금 실행하면 바로 틀린 값이 새로 생긴다"는 점에서
   더 급하다.
3. **S(schema) level=6 576건("검수완료" 라벨)을 review_status만 보고
   신뢰하면 안 된다** — phase9가 이미 확립했고 이번 세션도 재확인했다.
   이번에 추가로 확인한 것은 **73건에는 이 오염된 576건이 전혀 섞여 있지
   않다**(73건은 전부 L4/L5 V, AI 미태그)는 점 — 73건 자체는 이 리스크에서
   자유롭지만, 향후 L6 작업 시 이 576건을 다시 "검수됨"으로 오인하지 않게
   주의해야 한다.
4. **73건 중 2건("부여", "수집")은 literacy.db 실제 정의와 822건 감사자료의
   근거 정의가 서로 다른 버전으로 보인다**(1-3절) — 이는 "학습도구어 사전"
   원본과 literacy.db에 실제 적재된 정의 사이에 **알려지지 않은 버전
   불일치가 있을 수 있다**는 새로운 신호다. 이 2건만의 문제가 아니라, 나머지
   미작성 967건(V/S 원문 확인 가능분 중 초안 없는 것)에도 같은 종류의
   불일치가 섞여 있을 수 있으므로, 앞으로 초안을 작성할 때마다 literacy.db
   정의를 직접 대조하는 절차를 넣는 것이 안전하다.
5. **표제어 동음이의 충돌 위험**("시각" 사례, 1-3절) — V와 S에 같은
   표제어가 서로 다른 뜻으로 존재할 수 있으므로, 앞으로 콘텐츠를 연결할 때
   표제어 문자열만으로 자동 매칭하면 안 되고 `sense_id`/정의 대조가
   반드시 필요하다.

---

## 5. 산출물

- 본 보고서: `reports/schema_reading_phase10_73drafts_crosscheck_20260924.md`
- `data/import/schema_reading_phase10_73drafts_crosscheck_20260924.csv` —
  73건 행별 대조 결과(상태/사유/literacy.db 매칭 상세/822건 감사자료 근거 포함)
- `data/import/schema_reading_phase10_73drafts_crosscheck_20260924.jsonl` —
  위 CSV와 동일 내용의 JSONL
- 원본 zip은 그대로 유지, 건드리지 않음: `data/import/internal_vocab_upper_restore_v1.zip`
  (스크래치패드에 읽기 전용으로 풀어서만 대조, 저장소에는 원본 zip 외
  추가 파일을 넣지 않았다)
- 재현에 쓴 쿼리는 전부 본문에 기술된 SQL/조건 그대로 재현 가능(로컬
  스크립트는 세션 스크래치패드에만 있고 저장소에는 남기지 않음)
