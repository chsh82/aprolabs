-- ============================================================
-- 일반 어휘 퀴즈 DB (초등 대상, 외부 파이프라인 적재분) - 로컬 R&D 전용
-- SQLite / aprolabs app/vocabulary_quiz 연동용
--
-- data/vocab/idiom.db(사자성어 전용, app/vocab)와는 완전히 다른 콘텐츠
-- 도메인이자 별도 모듈이다 - 이 파일이 잘못되어도 idiom.db는 전혀
-- 영향받지 않는다(물리적으로 다른 파일, 다른 DB 연결).
-- ============================================================

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- 1. 적재 배치 이력 - 배치(버전)마다 1행.
-- ------------------------------------------------------------
CREATE TABLE vocabulary_import_batches (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    version          TEXT NOT NULL,
    source_filename  TEXT,
    source_sha256    TEXT,
    started_at       TEXT,
    completed_at     TEXT,
    status           TEXT NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING','DRY_RUN_OK','DRY_RUN_FAILED','COMPLETED','FAILED')),
    content_count    INTEGER,
    item_count       INTEGER,
    inserted_count   INTEGER,
    updated_count    INTEGER,
    unchanged_count  INTEGER,
    failed_count     INTEGER,
    validation_result TEXT,   -- JSON: {"hard": [...], "soft": [...]}
    notes            TEXT
);

CREATE INDEX idx_batches_version ON vocabulary_import_batches(version);

-- ------------------------------------------------------------
-- 2. 어휘 콘텐츠 (표제어 + 뜻풀이 + 예문)
--    외부 파이프라인의 server_staging_candidate_content_v*.csv 매핑.
-- ------------------------------------------------------------
CREATE TABLE vocabulary_contents (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id             TEXT NOT NULL UNIQUE,   -- 'SC_V191_B001_001'
    sense_id               TEXT,
    lexical_entry_id       TEXT,
    batch_id               TEXT,                    -- 원본 CSV의 batch_id(파이프라인 배치, 위 1번 테이블과 다른 개념)
    lemma                  TEXT NOT NULL,
    pos                    TEXT,
    canonical_definition   TEXT,
    student_definition     TEXT,
    example_sentence       TEXT,
    example_target_form    TEXT,
    generation_method      TEXT,
    qa_method              TEXT,
    generation_status      TEXT,
    student_exposure       INTEGER NOT NULL DEFAULT 0,
    public_ready           INTEGER NOT NULL DEFAULT 0,
    quality_batch_id       TEXT,
    hold_reason            TEXT,
    merge_source           TEXT,
    source_version         TEXT NOT NULL,   -- 이 행을 마지막으로 적재/갱신한 데이터 버전('2.1.29')
    is_active               INTEGER NOT NULL DEFAULT 1,   -- 소프트 삭제
    created_at               TEXT DEFAULT (datetime('now')),
    updated_at               TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_vc_sense_id   ON vocabulary_contents(sense_id);
CREATE INDEX idx_vc_lemma      ON vocabulary_contents(lemma);
CREATE INDEX idx_vc_status     ON vocabulary_contents(generation_status);

-- ------------------------------------------------------------
-- 3. 어휘 문항 (4지선다) - content_id로 콘텐츠와 연결.
--    server_staging_candidate_items_v*.csv 매핑.
-- ------------------------------------------------------------
CREATE TABLE vocabulary_items (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id            TEXT NOT NULL UNIQUE,   -- 'IT_V191_B001_001'
    content_id         TEXT NOT NULL REFERENCES vocabulary_contents(content_id) ON DELETE CASCADE,
    sense_id           TEXT,
    batch_id           TEXT,
    lemma              TEXT,
    item_type          TEXT,
    stem               TEXT,
    option_1           TEXT,
    option_2           TEXT,
    option_3           TEXT,
    option_4           TEXT,
    correct_option     INTEGER NOT NULL CHECK (correct_option BETWEEN 1 AND 4),
    explanation        TEXT,
    generation_status  TEXT,
    student_exposure   INTEGER NOT NULL DEFAULT 0,
    public_ready       INTEGER NOT NULL DEFAULT 0,
    quality_batch_id   TEXT,
    merge_source       TEXT,
    source_version     TEXT NOT NULL,
    is_active          INTEGER NOT NULL DEFAULT 1,
    created_at         TEXT DEFAULT (datetime('now')),
    updated_at         TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_vi_content_id ON vocabulary_items(content_id);
CREATE INDEX idx_vi_sense_id   ON vocabulary_items(sense_id);

-- ------------------------------------------------------------
-- 4. 샘플링 검수 - 원본 콘텐츠/문항과 분리된 별도 테이블.
--    검수 결과가 vocabulary_contents/items의 공개 상태를 자동으로
--    바꾸지 않는다(운영 승격은 별도 절차).
-- ------------------------------------------------------------
CREATE TABLE vocabulary_review_samples (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_version  TEXT NOT NULL,           -- '2.1.29'
    item_id         TEXT NOT NULL REFERENCES vocabulary_items(item_id) ON DELETE CASCADE,
    content_id      TEXT NOT NULL REFERENCES vocabulary_contents(content_id) ON DELETE CASCADE,
    sampling_group  TEXT,                     -- 층화 그룹 라벨(JSON) - 재현성/설명용
    sampling_seed   INTEGER NOT NULL,
    review_status   TEXT NOT NULL DEFAULT 'UNREVIEWED'
                    CHECK (review_status IN ('UNREVIEWED','PASS','REVISE','EXCLUDE')),
    review_note     TEXT,
    reviewed_at     TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE (content_id, sample_version)
);

CREATE INDEX idx_review_version ON vocabulary_review_samples(sample_version);
CREATE INDEX idx_review_status  ON vocabulary_review_samples(review_status);

-- ------------------------------------------------------------
-- 5. QA 배치 검수 이력 - 사람이 표본을 검수한 "세션" 단위 요약 기록.
--    vocabulary_review_samples(개별 항목 상태)와 별개로, "이 버전을
--    R&D 퀴즈/운영에 써도 되는가"를 판단한 의사결정 자체를 남긴다.
-- ------------------------------------------------------------
CREATE TABLE vocabulary_qa_batches (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    version                TEXT NOT NULL,
    reviewed_sample_count  INTEGER NOT NULL,
    pass_count             INTEGER NOT NULL,
    revise_count           INTEGER NOT NULL,
    exclude_count          INTEGER NOT NULL,
    critical_error_count   INTEGER NOT NULL,
    major_error_count      INTEGER NOT NULL,
    minor_error_count      INTEGER NOT NULL,
    approved_for_rnd_quiz  INTEGER NOT NULL DEFAULT 0,
    approved_for_public    INTEGER NOT NULL DEFAULT 0,
    reviewed_at            TEXT NOT NULL,
    notes                  TEXT,
    created_at             TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_qa_batches_version ON vocabulary_qa_batches(version);

-- ------------------------------------------------------------
-- 6. 어휘 퀴즈 MVP - 세션(1회 플레이) + 응답(문항별 1행).
--    관리자 전용, 학생 비공개. 정답은 서버가 vocabulary_items.correct_option
--    기준으로 채점 - 클라이언트가 보낸 정답 여부를 신뢰하지 않는다.
-- ------------------------------------------------------------
CREATE TABLE vocabulary_quiz_sessions (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    source_version  TEXT NOT NULL,
    question_count  INTEGER NOT NULL,
    correct_count   INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed')),
    started_at      TEXT NOT NULL,
    completed_at    TEXT
);

CREATE INDEX idx_quiz_sessions_user ON vocabulary_quiz_sessions(user_id, status);

CREATE TABLE vocabulary_quiz_attempts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id       TEXT NOT NULL REFERENCES vocabulary_quiz_sessions(id) ON DELETE CASCADE,
    item_id          TEXT NOT NULL,
    order_index      INTEGER NOT NULL,     -- 세션 내 문제 순서(1부터) - 진행 화면 표시/다음 문제 판단용
    selected_option  INTEGER CHECK (selected_option BETWEEN 1 AND 4),  -- NULL = 아직 미응답
    correct_option   INTEGER NOT NULL CHECK (correct_option BETWEEN 1 AND 4),
    is_correct       INTEGER,
    answered_at      TEXT,
    UNIQUE (session_id, item_id)
);

CREATE INDEX idx_quiz_attempts_session ON vocabulary_quiz_attempts(session_id, order_index);

-- ============================================================
-- 검수/집계용 뷰
-- ============================================================

CREATE VIEW v_vq_orphan_items AS
SELECT i.item_id, i.content_id
FROM vocabulary_items i
LEFT JOIN vocabulary_contents c ON c.content_id = i.content_id
WHERE c.content_id IS NULL;

CREATE VIEW v_vq_content_without_item AS
SELECT c.content_id
FROM vocabulary_contents c
LEFT JOIN vocabulary_items i ON i.content_id = c.content_id
WHERE i.item_id IS NULL;

CREATE VIEW v_vq_review_progress AS
SELECT sample_version, review_status, COUNT(*) AS n
FROM vocabulary_review_samples
GROUP BY sample_version, review_status;

-- ------------------------------------------------------------
-- 7. 초등 다유형 어휘 퀴즈(파일럿) - 기존 vocabulary_items(4지선다 단일
--    유형)와 별개로, content 1건에서 여러 유형의 파생 문항을 만들어내는
--    구조. 5개 유형(MEANING_CHOICE/WORD_FROM_DEFINITION/CONTEXT_MEANING/
--    CONTEXT_CLOZE/MATCH_WORD_MEANING)을 한 테이블에 담되, 유형마다
--    필요한 필드가 달라 정답 관련 정보는 answer_payload_json 하나에
--    정규화해서 넣는다(채점 로직이 이 필드 하나만 보면 되게) - 정답
--    노출 방지를 위해 API 응답에서는 이 컬럼을 절대 내려주지 않는다.
--    관리자 전용, R&D 전용 - 기존 vocabulary_quiz_sessions/attempts
--    (단일 유형 MVP)와도 별도 테이블을 쓴다(섞지 않음).
-- ------------------------------------------------------------
CREATE TABLE vocabulary_multiformat_items (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id                  TEXT NOT NULL UNIQUE,
    item_type                TEXT NOT NULL CHECK (item_type IN
                              ('MEANING_CHOICE','WORD_FROM_DEFINITION','CONTEXT_MEANING',
                               'CONTEXT_CLOZE','MATCH_WORD_MEANING','CROSSWORD')),
    source_content_id        TEXT REFERENCES vocabulary_contents(content_id),  -- MATCH_WORD_MEANING/CROSSWORD는 NULL(대신 아래 _ids_json)
    source_content_ids_json  TEXT,   -- MATCH_WORD_MEANING(4개)/CROSSWORD(10개) 전용: content_id 배열
    sense_id                 TEXT,
    sense_ids_json           TEXT,   -- MATCH_WORD_MEANING(4개)/CROSSWORD(10개) 전용: sense_id 배열
    lemma                    TEXT,   -- MATCH_WORD_MEANING/CROSSWORD는 NULL(표제어가 여러 개라 개별 컬럼에 안 맞음)
    pos                      TEXT,
    prompt                   TEXT NOT NULL,
    options_json             TEXT,   -- 선택형 3종만: 보기 4개 배열. CONTEXT_CLOZE/MATCH/CROSSWORD는 NULL
    correct_option           INTEGER CHECK (correct_option IS NULL OR correct_option BETWEEN 1 AND 4),
    public_payload_json      TEXT,   -- 유형별 "공개해도 되는" 표시 정보 정규화(선택형 options / 빈칸 input_hint /
                                     -- 연결형 words·definitions / 십자말 rows·cols·활성칸·entries(정답 제외)) -
                                     -- API가 매 요청마다 answer_payload_json에서 다시 골라내는 대신 이 컬럼을 그대로 반환한다.
    answer_payload_json      TEXT NOT NULL,  -- 유형별 정답 정보 정규화(위 주석 참고) - 채점 전용, 클라이언트에 내려주지 않음
    explanation              TEXT,
    cognitive_level          INTEGER,
    qa_flags_json            TEXT,
    generator_version        TEXT,   -- 생성 파이프라인 식별자(예: 'pilot_multiformat_v1') - 콘텐츠 버전(source_version)과는 별개
    source_version            TEXT NOT NULL,
    is_active                 INTEGER NOT NULL DEFAULT 1,
    created_at                 TEXT DEFAULT (datetime('now')),
    updated_at                 TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_vmi_item_type ON vocabulary_multiformat_items(item_type);
CREATE INDEX idx_vmi_source_content ON vocabulary_multiformat_items(source_content_id);
CREATE INDEX idx_vmi_source_version ON vocabulary_multiformat_items(source_version);

CREATE TABLE vocabulary_multiformat_import_batches (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    version              TEXT NOT NULL,
    source_filename       TEXT,
    source_sha256         TEXT,
    seed                   INTEGER,
    selected_words         INTEGER,
    started_at             TEXT,
    completed_at           TEXT,
    status                 TEXT NOT NULL DEFAULT 'PENDING'
                           CHECK (status IN ('PENDING','DRY_RUN_OK','DRY_RUN_FAILED','COMPLETED','FAILED')),
    item_count             INTEGER,
    item_type_counts_json   TEXT,
    inserted_count          INTEGER,
    updated_count           INTEGER,
    unchanged_count         INTEGER,
    validation_result       TEXT,   -- JSON: {"hard": [...], "soft": [...]}
    notes                   TEXT
);

CREATE INDEX idx_mf_batches_version ON vocabulary_multiformat_import_batches(version);

-- 세션(1회 플레이) + 응답(문항별 1행). 기존 vocabulary_quiz_sessions/attempts
-- (단일 4지선다 MVP)와 구조가 달라(유형 혼합, 유형별 채점 방식이 다름) 별도로 둔다.
CREATE TABLE vocabulary_multiformat_sessions (
    id               TEXT PRIMARY KEY,
    user_id          TEXT NOT NULL,
    source_version   TEXT NOT NULL,
    item_types_json  TEXT,     -- 요청한 유형 필터 JSON 배열, NULL/미지정이면 혼합(전체 유형)
    question_count   INTEGER NOT NULL,
    correct_count    INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed')),
    started_at       TEXT NOT NULL,
    completed_at     TEXT
);

CREATE INDEX idx_mf_sessions_user ON vocabulary_multiformat_sessions(user_id, status);

CREATE TABLE vocabulary_multiformat_responses (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id                 TEXT NOT NULL REFERENCES vocabulary_multiformat_sessions(id) ON DELETE CASCADE,
    item_id                     TEXT NOT NULL REFERENCES vocabulary_multiformat_items(item_id),
    order_index                 INTEGER NOT NULL,
    item_type                   TEXT NOT NULL,
    submitted_payload_json       TEXT,    -- 학생이 제출한 원본 응답(선택형 selected_option / 직접입력 answer_text /
                                          -- 연결형 answers / 십자말 cells)
    is_correct                   INTEGER, -- NULL=미응답. 연결형/십자말은 correct_count==total_count일 때만 1
    correct_count                 INTEGER, -- 연결형(4쌍 중 맞은 개수)/십자말(맞은 칸 수) 전용. 그 외 유형은 NULL
    total_count                   INTEGER, -- 연결형(4)/십자말(활성 칸 수) 전용. 그 외 유형은 NULL
    attempt_count                 INTEGER NOT NULL DEFAULT 0,  -- 문맥빈칸 전용: 제출 시도 횟수(최대 2) - 그 외 유형은 0 또는 1
    hint_used                     INTEGER NOT NULL DEFAULT 0,  -- 문맥빈칸 전용: 초성 힌트를 본 적 있으면 1
    answered_at                   TEXT,
    UNIQUE (session_id, item_id)
);

CREATE INDEX idx_mf_responses_session ON vocabulary_multiformat_responses(session_id, order_index);

CREATE VIEW v_vmf_item_type_counts AS
SELECT source_version, item_type, COUNT(*) AS n
FROM vocabulary_multiformat_items
GROUP BY source_version, item_type;
