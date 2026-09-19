# 모모의책장 교재DB — 문항DB 구조 조사 보고서

- 대상 DB: `momo_book_db/momo_book.db` (SQLite, 별도 파일 — 앱의 `aprolabs.db`와 무관)
- 조사 시점 데이터 기준: `documents` 305건 / `discussion_qa` 2,513건 / `vocabulary` 939건 / `ox_quiz` 824건 / `essay_prompt` 295건 / `essay_outline_question` 723건 / `document_image` 1,052건 / `extraction_log` 895건
- 이 문서는 조사만 수행한 결과이며, 조사 과정에서 코드나 DB는 전혀 수정하지 않았습니다(전부 SELECT/PRAGMA 조회).

---

## 1. 스키마

### 1-1. CREATE TABLE 원문 (`sqlite_master` 그대로)

```sql
CREATE TABLE documents (
    doc_id          TEXT PRIMARY KEY,   -- 예: L5-Q4-W10
    curriculum_id   TEXT,               -- momo_bookshelf_weeks.id 참조
    level           TEXT,
    quarter         TEXT,
    week            INTEGER,
    book_title      TEXT NOT NULL,
    book_author     TEXT,
    isbn            TEXT,
    source_file     TEXT NOT NULL,
    source_format   TEXT,
    source_hash     TEXT,
    version         INTEGER DEFAULT 1,
    parsed_at       TEXT,
    review_status   TEXT DEFAULT 'pending'
, cover_message TEXT, background_text TEXT)

CREATE TABLE vocabulary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    order_no        INTEGER NOT NULL,
    word            TEXT NOT NULL,
    definition      TEXT,
    book_page       INTEGER,
    example_sentence TEXT,
    source_page     INTEGER,
    raw_text        TEXT,
    extraction_confidence REAL,
    review_status   TEXT DEFAULT 'pending'
)

CREATE TABLE ox_quiz (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    order_no        INTEGER NOT NULL,
    question        TEXT NOT NULL,
    answer          TEXT,
    evidence_page   INTEGER,
    explanation     TEXT,
    source_page     INTEGER,
    raw_text        TEXT,
    extraction_confidence REAL,
    review_status   TEXT DEFAULT 'pending'
)

CREATE TABLE discussion_qa (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    order_no        INTEGER NOT NULL,
    order_label     TEXT,               -- 원문 그대로("1","4-1","4-2"...) - 2026-08-26 추가
    reading_type    TEXT,
    excerpt_text    TEXT,
    excerpt_page    INTEGER,
    question_text   TEXT NOT NULL,
    ui_type         TEXT,
    ui_config       TEXT,
    model_answer    TEXT,
    source_page     INTEGER,
    raw_text        TEXT,
    extraction_confidence REAL,
    review_status   TEXT DEFAULT 'pending'
, reference_text TEXT, reference_image_path TEXT, excerpt_image_path TEXT)

CREATE TABLE essay_prompt (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    main_topic      TEXT NOT NULL,
    writing_format  TEXT,
    min_length      INTEGER,
    source_page     INTEGER,
    raw_text        TEXT,
    extraction_confidence REAL,
    review_status   TEXT DEFAULT 'pending'
, closing_instruction TEXT, writing_guide TEXT, image_path TEXT)

CREATE TABLE essay_outline_question (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    essay_id        INTEGER NOT NULL REFERENCES essay_prompt(id),
    order_no        INTEGER NOT NULL,
    question_text   TEXT NOT NULL,
    role            TEXT
)

CREATE TABLE document_image (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    image_type      TEXT NOT NULL,
    source_page     INTEGER,
    file_path       TEXT NOT NULL,
    extraction_confidence REAL
)

CREATE TABLE extraction_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT REFERENCES documents(doc_id),
    level           TEXT,
    stage           TEXT,
    message         TEXT,
    created_at      TEXT
)
```

(`sqlite_sequence`는 SQLite가 AUTOINCREMENT 관리용으로 자동 생성한 테이블이라 생략)

컬럼 뒤에 붙은 `, 컬럼명 TYPE, ...` 형태는 최초 `CREATE TABLE` 이후 `ALTER TABLE ... ADD COLUMN`으로 나중에 추가된 컬럼입니다(SQLite는 컬럼 추가 이력을 그대로 SQL 문자열에 이어붙여서 보여줌). 즉 `documents.cover_message`/`background_text`, `discussion_qa.reference_text`/`reference_image_path`/`excerpt_image_path`, `essay_prompt.closing_instruction`/`writing_guide`/`image_path`는 모두 개발 중 추가된 컬럼입니다.

### 1-2. 테이블 간 관계

`PRAGMA foreign_key_list`로 확인한 FK (SQLite는 기본적으로 FK를 강제하지 않으며, 이 DB의 연결부(`loader.py`)는 `PRAGMA foreign_keys = ON`을 켜고 씀):

| 자식 테이블 | 자식 컬럼 | 부모 테이블 | 부모 컬럼 |
|---|---|---|---|
| vocabulary | doc_id | documents | doc_id |
| ox_quiz | doc_id | documents | doc_id |
| discussion_qa | doc_id | documents | doc_id |
| essay_prompt | doc_id | documents | doc_id |
| document_image | doc_id | documents | doc_id |
| extraction_log | doc_id | documents | doc_id |
| essay_outline_question | essay_id | essay_prompt | id |

- 조인 키는 전부 `documents.doc_id` (문자열, 예: `L5-Q4-W10`) 하나로 통일. `essay_outline_question`만 `essay_prompt.id`(정수 PK)로 한 단계 더 들어감.
- `documents.curriculum_id`는 **다른 물리 DB 파일**인 `aprolabs.db`의 `momo_bookshelf_weeks.id`를 가리키는 텍스트 값입니다. SQLite는 별도 파일 간 FK를 제약으로 걸 수 없어서 `PRAGMA foreign_key_list(documents)`에는 이 관계가 전혀 나타나지 않습니다(주석으로만 표시됨). `momo_bookshelf_weeks`에는 `grade`(학년, 예: "초등 1학년"), `year`, `quarter`, `week_number`, `title`, `author`, `is_holiday` 컬럼이 있습니다.

### 1-3. 인덱스

```sql
-- PRAGMA index_list(테이블명) 결과
documents               -> sqlite_autoindex_documents_1 (PRIMARY KEY, doc_id)
vocabulary               -> (없음)
ox_quiz                  -> (없음)
discussion_qa            -> (없음)
essay_prompt             -> (없음)
essay_outline_question   -> (없음)
document_image           -> (없음)
extraction_log            -> (없음)
```

**`documents.doc_id` PK 말고는 인덱스가 전혀 없습니다.** 자식 테이블들의 `doc_id` 컬럼(가장 많이 조인/필터에 쓰이는 컬럼)에도 인덱스가 없어서, `WHERE doc_id = ?` 조회가 전부 풀스캔입니다. 지금 규모(수백~수천 행)에서는 체감 문제가 없지만, 데이터가 몇 배 늘어나면 `doc_id`에 인덱스를 추가하는 걸 권장합니다.

---

## 2. 분류 축의 실제 값과 건수

### 2-1. 문항 유형 (`discussion_qa.ui_type`) — 전체 7종, 생략 없음

| ui_type | 건수 |
|---|---|
| text_long | 819 |
| text_short_multi | 469 |
| unknown | 411 |
| text_short | 394 |
| choice_ab | 178 |
| table_compare | 171 |
| choice_multi | 71 |

`unknown`은 파서가 유형을 못 정한 항목(411건, 전체의 16%)으로, 고정 조판 렌더러가 이 값을 만나면 어떤 입력 UI를 그릴지 결정할 수 없는 상태입니다.

### 2-2. 독해 유형 라벨 (`discussion_qa.reading_type`) — 고유값 38개, 상위 20 + 나머지

| reading_type | 건수 |
|---|---|
| (NULL) | 1265 |
| 추론적 | 341 |
| 비판적 | 129 |
| 추론적/비판적 | 120 |
| 추론적/적용적 | 110 |
| 적용적 | 91 |
| 추론적/분석적 | 84 |
| 사실적/추론적 | 83 |
| 분석적 | 58 |
| 비판적/적용적 | 43 |
| 분석적/적용적 | 39 |
| 비판적/분석적 | 31 |
| 사실적/분석적 | 19 |
| 사실적 | 18 |
| 사실적/적용적 | 17 |
| 추론하며 읽기 | 8 |
| 사실적/비판적 | 8 |
| 추론적/분석적/적용적 | 4 |
| 전반부 줄거리 요약 | 4 |
| 영상으로 보기 | 4 |
| **나머지 18종 합계** | **37** |

- NULL이 절반(1265/2513, 50.3%)로 가장 많음 — 파서가 이 필드를 못 채운 문항이 절반 이상.
- "나머지 18종"에는 `추론적/용적/독해적`, `사실적/츄론적`, `바펀적` 같은 명백한 오타/폰트깨짐 값이 섞여 있음(3-특이사항에서 다시 언급).

### 2-3. 레벨(LV) (`documents.level`) — 전체 9종

| level | 문서 수 |
|---|---|
| L5 | 40 |
| L1 | 39 |
| L6 | 38 |
| L4 | 38 |
| L3 | 38 |
| L2 | 37 |
| L9 | 27 |
| L8 | 27 |
| L7 | 21 |

- **학년 라벨은 이 DB 안에 없습니다.** `level`(L1~L9)만 있고, "초등 1학년"/"중학교 2학년" 같은 학년 문자열은 `aprolabs.db`의 `momo_bookshelf_weeks.grade`에만 존재합니다. 다만 L1~L6=초등 1~6학년, L7~L9=중학 1~3학년으로 코드 관례상 고정 매핑되어 있어(배치 스크립트 `momo_book_db/batch_run.py`의 `GRADE_FOLDER_TO_LEVEL`), `momo_bookshelf_weeks`를 조인하지 않고도 `level` 문자열만으로 학년을 알 수 있습니다.

### 2-4. 분기 (`documents.quarter`) — 전체 4종

| quarter | 건수 |
|---|---|
| 4분기(인문예술) | 97 |
| 1분기(고전) | 96 |
| 3분기(문학) | 81 |
| 2분기(탐구) | 31 |

### 2-5. 난이도 — **없음**

이 DB의 어떤 테이블에도 "난이도"를 나타내는 컬럼이 없습니다. `extraction_confidence`(REAL, 값은 0.6/0.9/1.0 세 종류만 존재)는 파서가 얼마나 확신하고 추출했는지를 나타내는 **추출 신뢰도**이며 문항의 난이도와는 무관합니다. 난이도 기반 조판/필터링이 필요하다면 별도 컬럼을 추가해야 합니다.

### 2-6. 도서(book_title) — 고유 220종, 상위 20 + 나머지

| book_title | 문서 수 |
|---|---|
| 곰브리치 세계사 | 4 |
| 모모 | 3 |
| 레 미제라블 | 3 |
| 80일간의 세계 일주 | 3 |
| 호밀밭의 파수꾼 | 2 |
| 해저 2만 리 | 2 |
| 한눈에 반한 세계 미술관 | 2 |
| 피터 팬 | 2 |
| 플라톤의 국가 정의를 꿈꾸다 | 2 |
| 프랑켄슈타인 | 2 |
| 푸른 사자 와니니 | 2 |
| 페스트 | 2 |
| 파랑새 | 2 |
| 톰 소여의 모험 | 2 |
| 키다리 아저씨 | 2 |
| 크리스마스 캐럴 | 2 |
| 크리스마스 선물 | 2 |
| 초정리 편지 | 2 |
| 청소년을 위한 주제로 보는 조선왕조실록 | 2 |
| 처음 읽는 월든 | 2 |
| **나머지 200종 합계** | **260** |

대부분(200/220)의 도서는 문서 1건뿐이고("연장" 주차가 없는 단권 구성), 2건 이상은 대개 "1주차/2주차"로 나뉜 같은 책입니다.

---

## 3. 텍스트 길이 분포 (글자수, Python `len()` 기준 — 한글 1글자=1)

| 컬럼 | 비어있지 않은 건수/전체 | min | 중앙값 | 평균 | 최대 |
|---|---|---|---|---|---|
| discussion_qa.excerpt_text | 788/2513 | 16 | 157 | 183.0 | 891 |
| discussion_qa.question_text | 2501/2513 | 1 | 100 | 160.2 | 2296 |
| discussion_qa.reference_text | 0/2513 | - | - | - | - (전부 NULL) |
| essay_prompt.main_topic | 295/295 | 1 | 31 | 57.1 | 385 |
| essay_prompt.writing_guide | 154/295 | 6 | 109 | 175.0 | 938 |
| essay_prompt.closing_instruction | 172/295 | 15 | 16 | 20.2 | 111 |
| essay_outline_question.question_text | 723/723 | 13 | 58 | 75.5 | 661 |
| vocabulary.definition | 553/939 | 6 | 21 | 23.7 | 72 |
| vocabulary.example_sentence | 0/939 | - | - | - | - (전부 NULL) |
| ox_quiz.question | 824/824 | 4 | 43 | 44.9 | 111 |
| documents.cover_message | 303/305 | 1 | 39 | 43.2 | 135 |
| documents.background_text | 145/305 | 46 | 451 | 628.2 | 3769 |

### 최대 길이 상위 3건 (id + 앞 100자)

**discussion_qa.excerpt_text**
- id=3129, 891자: `경상도 순변사 이일은 먼저 경상도 지역의 수령들에게 격서를 보내어 각각 군사를 거느리고 대구로 모이라고 명령했다. 그러나 각 지역 수령의 군사들은 대구로 향하는 도중에 왜적이 가까`
- id=3128, 736자: (위와 거의 동일한 문장으로 시작 — 같은 발췌문이 두 문항에 각각 그대로 저장됨, 3-특이사항 참고)
- id=3252, 647자: `"전하, 신이 억울하게 죽어 가슴에 맺힌 원한을 풀지 못하고 원수를 갚지 못할까 하옵더니, 오늘날 전하의 크신 덕으로 신의 원수를 갚아주시고 역적을 없애 주시니, 신이 비로소 눈을`

**discussion_qa.question_text**
- id=3592, 2296자: `거짓이 드러날 위기에서도 당당하고 뻔뻔하게 행동함 거짓말쟁이의 뇌를 해부한다면 · 모모의 책장 5 【2단계】질문과 토론 [사실적 독해, 비판적 독해] "심리학은 거짓말을 다룬다. `
- id=1303, 2280자: `[적용적 독해] 철학적으로! 정의의! 원칙이! 능력,! 미덕 ,! 도덕적! 자격! 등을! 고려하지! 않고! 정립되어야! 한 다는! 주장은! 롤스의! 자유주의보다! 일반적인! 주장의`
- id=2603, 2031자: `[사실적분석적/ 독해 ] 이른바$ '천재$시대이자$' '질풍노도라$' 불리던$이$시기의$ 키워드는$ 단연$ '자연이었다자연' .$ 은$ 우주만물을$ 지칭하는$ '외적$ 자연뿐만$'`

**documents.background_text**
- id=L8-Q3-W07, 3769자: `시적인 문체로 자연과 인간의 순수한 동화를 노래한 심미주의자 삶의 궤적: 강원도 평창(봉평)에서 태어나 서울대 영문학과를 졸업한 엘리...`
- id=L9-Q4-W08, 2692자: `1 단계.# 올바름 (Right) 과# 좋음 (Good)# ! ! '올바름(right)' 과! '좋음(good) 은! 무엇이! 도덕적으로! 올바른! 행동! 혹은! 규칙이지또는`
- id=L9-Q4-W07, 2553자: `1 단계.# 존# 롤스와# 케이크# 자르기 단언컨대,) 케이크를) 좋아하지) 않는) 사람은) 없습니다 .) 누구나) 케이크를) 좋아하니까요 .) 따라서) 우리는) 무엇보다도) `)`

**고정 A4 조판 관점에서 중요**: `question_text`가 최대 2,296자, `background_text`가 최대 3,769자까지 나옵니다. 한 페이지 상/하반부 블록에 이 길이가 그대로 들어가면 확실히 넘칩니다. 위 최대 길이 예시 중 2건(id=2603, L9-Q4-W07/W08)은 폰트 깨짐으로 `$`/`#`/`!` 같은 잡문자가 섞여 실제 표시 글자수보다 부풀려진 값이라는 점도 감안해야 합니다(4-특이사항).

---

## 4. 부속 데이터가 어디에 붙어 있는지

| 항목 | 저장 방식 |
|---|---|
| 발췌문/지문 | **같은 행의 컬럼** (`discussion_qa.excerpt_text`, TEXT). 별도 테이블 없음. |
| 이미지/삽화 | **별도 테이블** `document_image` (`doc_id`, `image_type`, `source_page`, `file_path`, `extraction_confidence`). `file_path`는 `momo_book_db/extracted_images/{doc_id}/파일명` 형태의 **상대경로 문자열**(실제 파일 시스템 경로, DB 안에 바이너리 없음). |
| 표 형태 문항 | 구조적 데이터 없음 — `discussion_qa.ui_type='table_compare'`이면 `ui_config`(JSON 문자열)에 `columns`/`rows` **배열**을 넣어두지만, 표의 실제 셀 값(학생 답)은 저장 대상이 아니고 표의 틀(열 이름 등)만 있음. 원문 표 자체가 이미지로만 존재하는 경우도 있음(`reference_image_path`로 표를 이미지째 붙임). |
| 어휘 문항 | **분리된 컬럼**: `vocabulary.word`(단어), `definition`(뜻), `book_page`(책 속 출처 페이지)가 각각의 컬럼. `example_sentence`는 컬럼은 있지만 100% NULL(파서가 안 채움). |
| OX 문항 | **분리된 컬럼**: `ox_quiz.question`(진술문), `answer`(정답), `evidence_page`(근거 페이지), `explanation`(해설). 단 `answer`/`explanation`은 100% NULL(파서가 안 채움 — 정답이 필요하면 검수자가 직접 입력해야 함). |
| 정답·해설 | 컬럼은 존재(`discussion_qa.model_answer`, `ox_quiz.answer`, `ox_quiz.explanation`)하지만 **전부 0% 채워짐(전체 NULL)**. 학생용 렌더에는 안 쓴다고 하셨으니 문제 없지만, 이 컬럼들을 신뢰해서 값을 꺼내 쓰면 안 됩니다. |

### 발췌문의 1:1 / 1:N 관계 — 명시적 FK 없이 "순서"로만 암묵 연결

- `excerpt_text`는 문항(행) 하나에 딸린 컬럼일 뿐, 발췌문 자체를 가리키는 독립 ID나 테이블이 없습니다.
- 같은 문서 안에서 완전히 똑같은 `excerpt_text` 문자열이 두 문항 이상에 그대로 중복 저장된 경우가 있는지 확인했는데 **0건**입니다 — 즉 발췌문이 여러 문항에 "복사"되어 있진 않습니다.
- 대신 하위 세부 문항(`order_label`이 "4-1","4-2","4-3"처럼 하이픈이 붙은 것들)을 확인해보면, 하위 문항들의 `excerpt_text`는 **거의 항상 NULL**입니다. 즉 "4"번 문항이 발췌문을 갖고 있고, "4-1"/"4-2"/"4-3"은 그 발췌문을 **암묵적으로 공유**하면서 자기 컬럼에는 저장하지 않는 구조입니다. 어떤 발췌문이 몇 개 문항에 걸쳐 있는지 알려면 "같은 `order_no`를 공유하는 행들을 같은 발췌문 그룹으로 본다"는 **애플리케이션 레벨 규칙**을 직접 구현해야 하며, DB 스키마만으로는 이 관계가 드러나지 않습니다.

---

## 5. 데이터 품질

### 5-1. 테이블별 전체 행 수

| 테이블 | 행 수 |
|---|---|
| documents | 305 |
| vocabulary | 939 |
| ox_quiz | 824 |
| discussion_qa | 2513 |
| essay_prompt | 295 |
| essay_outline_question | 723 |
| document_image | 1052 |
| extraction_log | 895 |

### 5-2. 주요 컬럼 NULL/빈 문자열 비율

| 컬럼 | NULL/빈값 비율 |
|---|---|
| documents.isbn | 305/305 (100.0%) — **전혀 채워지지 않음** |
| documents.book_author | 0/305 (0.0%) |
| documents.curriculum_id | 0/305 (0.0%) |
| vocabulary.book_page | 584/939 (62.2%) |
| vocabulary.word | 0/939 (0.0%) |
| ox_quiz.evidence_page | 463/824 (56.2%) |
| discussion_qa.excerpt_page | 1723/2513 (68.6%) |
| discussion_qa.source_page | 0/2513 (0.0%) |
| discussion_qa.order_label | 0/2513 (0.0%) |
| discussion_qa.ui_type | 0/2513 (0.0%, 다만 그중 411건이 문자열 값 `"unknown"`) |
| discussion_qa.extraction_confidence | 0/2513 (0.0%) |
| essay_prompt.min_length | 295/295 (100.0%) — **전혀 채워지지 않음** |
| essay_prompt.writing_format | 0/295 (0.0%, 다만 값 종류는 검증 안 함) |

(2-2, 3, 4절에서 이미 다룬 `reference_text`/`example_sentence`/정답·해설 계열 컬럼도 전부 0%로 마찬가지)

### 5-3. 검수 상태 컬럼과 상태별 건수

모든 콘텐츠 테이블에 `review_status` 컬럼이 있고(`documents`/`vocabulary`/`ox_quiz`/`discussion_qa`/`essay_prompt`), 값은 `'pending'` 또는 `'approved'` 둘 중 하나입니다(기본값 `'pending'`).

| 테이블 | approved | pending |
|---|---|---|
| documents | 7 | 298 |
| vocabulary | 8 | 931 |
| ox_quiz | 10 | 814 |
| discussion_qa | 17 | 2496 |
| essay_prompt | 2 | 293 |

**검수 완료 비율이 매우 낮습니다(전체적으로 1% 미만~2.3%).** "검수 완료 문항만 골라 쓴다"는 조건을 지금 그대로 적용하면 자동 생성 가능한 문항 풀이 거의 없다는 뜻이므로, 교재 생성 페이지를 만들 때 검수 완료 기준을 어떻게 잡을지(문서 단위 `documents.review_status='approved'`로 볼지, 문항 단위로 볼지) 미리 정해야 합니다. 참고로 두 기준은 서로 안 맞을 수 있습니다 — `documents.review_status='approved'`인 문서 안에도 개별 `discussion_qa` 행은 `pending`으로 남아있는 경우가 실제로 있습니다(검수 화면의 "문서 전체 승인" 버튼은 문서 레벨 상태만 바꾸고 하위 항목 상태는 그대로 둠).

### 5-4. 중복으로 보이는 문항

두 종류의 "중복"이 있고, 성격이 다릅니다.

**(a) `(doc_id, order_no)` 중복 — 351개 그룹. 대부분은 버그가 아니라 설계된 동작.**
`order_no`는 "4-1"/"4-2"/"4-3"처럼 세부 문항으로 나뉜 경우 부모 번호(4)를 그대로 공유하도록 만들어져 있어서, 이 자체는 정상입니다. 다만 실제로 들여다보면 다음 두 가지 이상 패턴이 섞여 있습니다.
  - 정상 패턴: `id=1812 order_label='6-1'`, `id=1813 order_label='6-2'` (진짜 하위 문항)
  - **의심 패턴**: `id=1811 order_label='6'`, 그 옆에 `order_label='6-1'`, `'6-2'`가 또 있음 — 즉 "6"이라는 부모 라벨의 행이 세부 문항들과 별개로 하나 더 존재. 이 "6" 행의 `question_text`를 열어보면 독해유형 라벨 텍스트("추론하며 읽기 / 적용하며 읽기 '내가 그랬어요...")가 그대로 문항 텍스트에 섞여 들어간 경우가 있어, 파서가 헤더 줄을 문항으로 잘못 잡은 것으로 보입니다.
  - **더 명확한 오류 패턴**: `L1-Q1-W03`에서 `order_no=5`에 대해 두 행 모두 `order_label='5'`(하이픈 없음, 즉 하위 문항 표시가 전혀 없음)로 겹쳐 있고, 하나는 `question_text='상상하며 읽기'`(독해유형 라벨만 들어간 사실상 빈 문항), 다른 하나가 진짜 질문입니다. → **판별 기준**: 같은 `(doc_id, order_no)`에서 `order_label`에 하이픈이 없는 행이 2개 이상이면 의심, 그중 `question_text`가 독해유형 라벨과 완전히 같거나 아주 짧으면 잘못 생성된 행일 가능성이 높습니다.

**(b) `question_text = ''`(완전 빈 문자열) 중복 — 3개 문서, 그중 `L5-Q4-W03`은 5건.**
`raw_text`까지 같이 빈 문자열인 완전히 빈 행들입니다. 이건 명백한 추출 실패이고, 지금 상태로 자동 조판에 넣으면 빈 블록이 그려집니다. → **판별 기준**: `question_text`가 빈 문자열이거나 NULL인 행은 렌더링 전에 반드시 걸러내야 합니다.

**(doc_id, order_label) 조합도 유니크하지 않습니다** — 전체 2,513행 중 고유 조합은 2,273개뿐(240개 조합이 중복). 즉 **`order_no`도 `order_label`도 안전한 자연키가 아니며, 안정적으로 한 문항을 가리킬 수 있는 건 오직 정수 PK(`id`)뿐**입니다. (6절과 연결됨)

---

## 6. ID 안정성

- 문항의 기본 키는 각 테이블의 `id`(INTEGER PRIMARY KEY AUTOINCREMENT)입니다.
- **재파싱하면 이 ID는 유지되지 않고 바뀝니다.** 코드 근거: `momo_book_db/loader.py`의 `save_document()`는 기존 문서(`existing`)가 있으면 먼저 `_delete_document_children(conn, doc_id)`를 호출해 `vocabulary`/`ox_quiz`/`discussion_qa`/`essay_prompt`(+`essay_outline_question`)/`document_image`/`extraction_log`를 **전부 DELETE**한 뒤, 파서가 새로 뽑은 결과를 처음부터 다시 INSERT합니다(`loader.py:50-176`). SQLite의 AUTOINCREMENT는 삭제된 행의 번호를 재사용하지 않고 계속 증가하므로, 재파싱 후 같은 문항이라도 새 `id`를 받습니다. 검수 화면의 "원본 다시 파싱" 버튼(`app/routers/momo_book_review.py`의 `momo_review_reparse`)이 바로 이 경로를 `force=True`로 호출합니다.
- 재파싱 시 유지되는 것은 `documents.doc_id`(문자열 PK, 예: `L5-Q4-W10`) 뿐입니다. `documents` 자체는 DELETE 없이 UPDATE되므로 `doc_id`는 안정적입니다.
- **문항 단위로 안정적으로 가리킬 다른 키는 지금 존재하지 않습니다.** `(doc_id, order_no)`와 `(doc_id, order_label)` 둘 다 5-4절에서 확인했듯 유니크하지 않고, 재파싱하면 순서/개수 자체가 바뀔 수도 있습니다(파서 로직이 개선되면 이전엔 안 잡히던 문항이 새로 생기거나 사라짐).
- **생성된 교재가 나중에도 같은 문항을 가리켜야 한다면**, 다음 중 하나가 필요합니다.
  1. 교재를 한 번 생성한 뒤로는 그 교재가 참조한 `doc_id`들을 **재파싱하지 않기로 운영 규칙을 정하기** (가장 간단하지만 파서 개선 반영이 늦어짐)
  2. 재파싱 시에도 유지되는 **안정적인 콘텐츠 지문(hash)** 같은 새 컬럼을 추가해서, 재파싱 후에도 "이전 문항과 같은 콘텐츠"를 매칭할 수 있게 하기 (지금 스키마에는 없음 — 추가 개발 필요)
  3. 교재 생성 시점에 참조한 문항의 **본문 스냅샷을 별도로 떠서** 교재 쪽에 저장하기(문항DB의 최신 상태와 별개로 굳어진 사본을 갖는 방식)

---

## 7. 실제 레코드 샘플 (5건, 조인 포함, 값 그대로)

### 7-1. 표 형태 + 이미지가 딸린 문항 (`discussion_qa.id=840`, `document_image` 조인)

```json
{
  "id": 840,
  "doc_id": "L3-Q4-W02",
  "order_no": 4,
  "order_label": "4-1",
  "reading_type": "사실적/추론적",
  "excerpt_text": null,
  "excerpt_page": null,
  "question_text": "이번엔 무엇이 달라졌는지 조금 더 자세히 살펴봅시다. 아래의 단어는 지금의 한글과 어떤 점이 다른가요? 옛날 사람들은 한글을 어떻게 적었을까요?",
  "ui_type": "table_compare",
  "ui_config": "{}",
  "model_answer": null,
  "source_page": 4,
  "raw_text": "이번엔 무엇이 달라졌는지 조금 더 자세히 살펴봅시다. 아래의 단\n글과 어떤 점이 다른가요? 옛날 사람들은 한글을 어떻게 적었을까요?\n예전의 한글               달라진 점\n장우나",
  "extraction_confidence": 0.9,
  "review_status": "pending",
  "reference_text": null,
  "reference_image_path": "L3-Q4-W02/reference_840_b0c25eed.png",
  "excerpt_image_path": null,
  "joined_document_image": {
    "id": 728,
    "doc_id": "L3-Q4-W02",
    "image_type": "reference",
    "source_page": 4,
    "file_path": "L3-Q4-W02/reference_840_b0c25eed.png",
    "extraction_confidence": 1.0
  }
}
```

(이 항목의 `ui_config`가 `"{}"`(빈 객체)인 점을 주의 — table_compare인데 columns/rows 정보가 없는 경우도 있습니다. 실제 표는 이미지(`reference_image_path`)로만 존재.)

### 7-2. 2択 선택형 문항 (`discussion_qa.id=112`)

```json
{
  "id": 112,
  "doc_id": "L5-Q4-W10",
  "order_no": 1,
  "order_label": "1",
  "reading_type": "비판적",
  "excerpt_text": "나는 제니 방적기에 대해서 잘은 모르지만 아마 그 기계는 노동자들이 하는 일을 대신 해 줄 수 있었을 거야. 그럼 어떻게 될지 상상이 가니, 혜리야? \"그렇다면... 공장주들이 사람을 쓰지 않고 제니 방적기만 쓰려고 하겠네요? 그럼 노동자들이 일자리를 잃게 되고요.\"",
  "excerpt_page": 44,
  "question_text": "기계 때문에 일자리를 잃은 사람들은 기계를 부수기 시작했어요. 이른바 '러다이트'은 이후 새로운 기술의 등장에 반대하는 사람을 의미하게 되었어요. 여러분은 '러다이트'에 대해 어떻게 생각하나요?",
  "ui_type": "choice_ab",
  "ui_config": "{\"options\": [\"동의할 수 있어요!\", \"동의할 수 없어요!\"]}",
  "model_answer": null,
  "source_page": 3,
  "raw_text": null,
  "extraction_confidence": 1.0,
  "review_status": "approved",
  "reference_text": null,
  "reference_image_path": null,
  "excerpt_image_path": null
}
```

### 7-3. 빈칸 여러 개(단답 복수) 문항 (`discussion_qa.id=113`)

```json
{
  "id": 113,
  "doc_id": "L5-Q4-W10",
  "order_no": 2,
  "order_label": "2",
  "reading_type": "추론적",
  "excerpt_text": "\"복지는 '생활할 만한 환경'을 말하는 거야. ~ 사회복지를 제일 먼저 언급한 벤담이지.\"",
  "excerpt_page": 63,
  "question_text": "제레미 벤담은 왜 모든 국민이 행복하게 살 수 있도록 국가 역할을 해야 한다는 '사회복지(사회보장제도)'를 생각하게 되었을까요? 산업혁명 이후 영국의 모습을 떠올려보고 그 이유를 생각해봅시다. * 사회보장제도: 사회구성원의 삶을 향상하기 위해 공공의 재원으로 최저생활을 보장해 주는 제도.",
  "ui_type": "text_short_multi",
  "ui_config": "{\"blanks\": [\"산업혁명 이후 영국 사람들의 삶\", \"사회복지가 필요해!!\"]}",
  "model_answer": null,
  "source_page": 4,
  "raw_text": null,
  "extraction_confidence": 1.0,
  "review_status": "approved",
  "reference_text": null,
  "reference_image_path": null,
  "excerpt_image_path": null
}
```

### 7-4. 어휘 문항 (`vocabulary.id=49`)

```json
{
  "id": 49,
  "doc_id": "L5-Q4-W10",
  "order_no": 1,
  "word": "양해",
  "definition": "남의 사정을 잘 헤아려 너그러이 받아들임.",
  "book_page": 48,
  "example_sentence": null,
  "source_page": 2,
  "raw_text": null,
  "extraction_confidence": 1.0,
  "review_status": "approved"
}
```

### 7-5. 글쓰기 문항 + 하위 단계 질문 조인 (`essay_prompt.id=13`, `essay_outline_question` 조인)

```json
{
  "id": 13,
  "doc_id": "L5-Q4-W10",
  "main_topic": "공리주의는 왜 등장했을까요? (최대 다수의 최대 행복)",
  "writing_format": "text_long",
  "min_length": null,
  "source_page": 9,
  "raw_text": null,
  "extraction_confidence": 0.7,
  "review_status": "approved",
  "closing_instruction": null,
  "writing_guide": null,
  "image_path": null,
  "joined_outline_questions": [
    {
      "id": 37,
      "essay_id": 13,
      "order_no": 1,
      "question_text": "산업혁명 이후 영국 사람들의 삶은 어떻게 변화했나요? (긍정적인 부분과 부정적인 부분을 모두 함께 적어 보세요.)",
      "role": "intro"
    },
    {
      "id": 38,
      "essay_id": 13,
      "order_no": 2,
      "question_text": "벤담은 왜 영국 사회를 더 좋게 만들기 위해서는 공리주의 원칙, 즉 최대 다수의 최대 행복이 필요하다고 생각했을까요?",
      "role": "body"
    },
    {
      "id": 39,
      "essay_id": 13,
      "order_no": 3,
      "question_text": "최대 다수의 최대 행복에 대한 여러분의 생각을 적어보며 글을 마무리해 보세요.",
      "role": "conclusion"
    }
  ]
}
```

---

## 8. 조회용 쿼리

### 8-1. 기존 코드에 이미 있는 예

`app/routers/momo_book_review.py`의 `momo_review_list()` 함수(79~135번째 줄)가 검수 화면 목록에서 **문서 단위**로 학년(`level`)/분기(`quarter`)/주차(`week`)/상태(`status`)를 필터링합니다. 다만 이건 문서(교재 1권) 단위 필터이고, "유형(ui_type)"까지 포함한 **문항 단위** 필터 예시는 코드베이스에 없어서 아래에 새로 작성했습니다.

### 8-2. "레벨 + 도서 + 유형 + 검수완료" 조건으로 문항을 뽑는 SQL (실제 실행해서 결과 확인함)

```sql
SELECT dq.id, dq.order_label, dq.ui_type, dq.review_status
FROM discussion_qa dq
JOIN documents d ON d.doc_id = dq.doc_id
WHERE d.level = 'L5'
  AND d.book_title = '나의 행복과 모두의 행복'
  AND dq.ui_type = 'text_short_multi'
  AND dq.review_status = 'approved'
ORDER BY dq.order_no;
```

실행 결과 (3건):
```
{'id': 113, 'order_label': '2', 'ui_type': 'text_short_multi', 'review_status': 'approved'}
{'id': 122, 'order_label': '3', 'ui_type': 'text_short_multi', 'review_status': 'approved'}
{'id': 118, 'order_label': '6', 'ui_type': 'text_short_multi', 'review_status': 'approved'}
```

`document_image`까지 함께 받으려면 `LEFT JOIN document_image di ON di.file_path = dq.reference_image_path` 형태로 추가 조인하면 됩니다(5절 참고: 이미지 링크 컬럼이 채워진 행이 전체 중 5건뿐이라 대부분 NULL이 나올 것).

---

## 특이사항 (조사 중 발견한 스키마-데이터 불일치/이상 항목)

1. **인덱스가 PK 하나뿐**: 모든 자식 테이블의 `doc_id`(가장 많이 조회에 쓰이는 컬럼)에 인덱스가 없음 (1-3절).
2. **`(doc_id, order_no)`도 `(doc_id, order_label)`도 유니크하지 않음**: 안정적인 자연키가 없고, PK(`id`)만 유일함. 게다가 그 PK조차 재파싱하면 바뀜(5-4절, 6절).
3. **정답/해설 계열 컬럼 전부 미사용**: `discussion_qa.model_answer`, `ox_quiz.answer`, `ox_quiz.explanation`, `vocabulary.example_sentence`, `discussion_qa.reference_text`, `essay_prompt.min_length`, `documents.isbn` — 스키마상 존재하지만 실제 값 채움률이 0%(정확히 0건). 이 컬럼들에 값이 있을 것으로 가정하고 코드를 짜면 안 됨.
4. **폰트 손상 교재의 텍스트가 그대로 들어가 있음**: 예) `L9-Q4-W07`/`L9-Q4-W08`(벤담 & 싱어), `L2-Q4-W03`류 일부 — 원문 PDF 폰트가 깨진 상태로 파싱되어 `$`, `#`, `!` 같은 글자가 단어 사이에 낀 채로 DB에 저장됨(3절 예시 참고). 글자수 통계(3절)도 이 잡문자 때문에 실제보다 부풀려져 있을 수 있음. 자동 조판 전에 이 문서들은 별도 정제나 재검수가 필요.
5. **`reading_type` 값에 오타가 다수 존재**: `추론적/용적/독해적`, `사실적/츄론적`, `바펀적` 등(2-2절). 값이 자유 텍스트로 저장되어 있어 정확한 enum 매칭이 불가능한 항목이 있음.
6. **파서가 헤더/라벨 줄을 문항으로 잘못 잡은 흔적**: `question_text`에 독해유형 라벨 문구가 그대로 섞여 들어간 행, 또는 `question_text`가 독해유형 라벨과 완전히 동일한 "사실상 빈 문항" 행이 존재(5-4절 (a)).
7. **완전히 빈 문항 행**: `question_text`와 `raw_text`가 둘 다 빈 문자열인 행이 3개 문서에 걸쳐 9건 존재(`L5-Q4-W03`에 5건 포함) — 추출 실패가 그대로 저장된 사례.
8. **검수 완료율이 매우 낮음**: 문서 기준 7/305(2.3%), 문항 기준 대부분 1% 미만. "검수 완료만" 조건으로 필터링하면 지금은 풀이 거의 비어있음.
9. **`ui_config`의 `rows` 필드 타입이 일관되지 않음**: 어떤 행은 `"rows": ["장점", "단점"]`(배열, 실제 행 라벨), 어떤 행은 `"rows": 1`(정수, 행 개수)로 저장되어 있어 파싱 코드에서 타입 분기가 필요함(2-1/7-1절 예시).
10. **`ui_type='table_compare'`인데 `ui_config`가 빈 객체(`"{}"`)인 경우가 있음**(7-1절 샘플) — 표의 틀 정보 없이 이미지로만 표가 존재하는 경우와, 아직 `ui_config`를 안 채운 경우를 구분할 방법이 DB 안에 없음.
11. **`curriculum_id`는 다른 물리 DB(`aprolabs.db`)를 가리키는 소프트 링크**: SQLite FK로 강제되지 않고, 이 DB만 백업/이동하면 참조가 끊어질 수 있음(1-2절).
