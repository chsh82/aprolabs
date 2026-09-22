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
