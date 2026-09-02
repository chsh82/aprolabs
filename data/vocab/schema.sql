-- ============================================================
-- 사자성어 학습 DB v1 (초·중·고 대상)
-- SQLite / aprolabs 연동용
-- ============================================================

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- 1. 표제어
-- ------------------------------------------------------------
CREATE TABLE idiom (
    idiom_id          INTEGER PRIMARY KEY,
    headword          TEXT NOT NULL UNIQUE,   -- 우공이산
    hanja             TEXT NOT NULL,          -- 愚公移山
    literal           TEXT,                   -- 직역: 우공이 산을 옮기다
    meaning           TEXT,                   -- 뜻풀이 (2문장 이내, 중등 기준)
    meaning_easy      TEXT,                   -- 초등용 쉬운 풀이
    origin_source     TEXT,                   -- 출전: 列子 湯問篇  (근거 없으면 NULL)
    origin_story      TEXT,                   -- 유래 요약 (3~5문장, 근거 없으면 NULL)

    -- 난이도 3축 (0.0 ~ 1.0). 단일 등급 대신 축을 분리해 저장한다.
    hanja_score       REAL,                   -- 구성 한자의 급수 기반
    abstraction_score REAL,                   -- 의미의 추상성
    frequency_score   REAL,                   -- 노출 빈도

    level_min         INTEGER,                -- 권장 최소 학년 (1~12), 위 3축에서 산출
    level_note        TEXT,                   -- 등급 산정 근거 메모

    status            TEXT NOT NULL DEFAULT 'draft'
                      CHECK (status IN ('draft','reviewed','published')),
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_idiom_level  ON idiom(level_min);
CREATE INDEX idx_idiom_status ON idiom(status);

-- ------------------------------------------------------------
-- 2. 표제어 선정 근거
--    선정 기준을 필드가 아닌 레코드로 쌓아 우선순위 재조정을 쉽게 한다.
--    source_type: textbook > exam > media > momo (현재 우선순위)
-- ------------------------------------------------------------
CREATE TABLE inclusion_evidence (
    evidence_id  INTEGER PRIMARY KEY,
    idiom_id     INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    source_type  TEXT NOT NULL
                 CHECK (source_type IN ('textbook','exam','media','momo')),
    detail       TEXT,        -- '중2 국어 미래엔 3단원' / '2021 수능 국어 12번'
    grade_band   TEXT,        -- 'elem' | 'mid' | 'high'
    hit_count    INTEGER DEFAULT 1
);

CREATE INDEX idx_evidence_idiom ON inclusion_evidence(idiom_id, source_type);

-- ------------------------------------------------------------
-- 3. 한자
-- ------------------------------------------------------------
CREATE TABLE hanja (
    char       TEXT PRIMARY KEY,    -- 愚
    hun        TEXT,                -- 어리석을
    eum        TEXT,                -- 우
    grade      TEXT,                -- 한자능력검정 급수 (5급, 4II급 ...)
    is_basic900 INTEGER DEFAULT 0   -- 교육용 기초한자 900자 포함 여부
);

CREATE TABLE idiom_hanja (
    idiom_id INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position BETWEEN 1 AND 4),
    char     TEXT NOT NULL REFERENCES hanja(char),
    PRIMARY KEY (idiom_id, position)
);

-- ------------------------------------------------------------
-- 4. 용례
--    성어당 최소 2개. 성격이 다른 문장을 섞는다.
-- ------------------------------------------------------------
CREATE TABLE example (
    example_id   INTEGER PRIMARY KEY,
    idiom_id     INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    sentence     TEXT NOT NULL,
    context_type TEXT CHECK (context_type IN
                 ('situation','essay','literature','media','dialogue')),
    grade_band   TEXT,     -- 'elem' | 'mid' | 'high'
    source       TEXT      -- 인용이면 출처, 창작이면 NULL
);

CREATE INDEX idx_example_idiom ON example(idiom_id);

-- ------------------------------------------------------------
-- 5. 성어 간 관계
--    confusable(혼동쌍)이 수업·출제에서 가장 활용도가 높다.
-- ------------------------------------------------------------
CREATE TABLE relation (
    relation_id INTEGER PRIMARY KEY,
    idiom_a     INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    idiom_b     INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    rel_type    TEXT NOT NULL
                CHECK (rel_type IN ('synonym','antonym','confusable')),
    note        TEXT,       -- 혼동쌍이면 '무엇이 다른가'를 한 문장으로
    UNIQUE (idiom_a, idiom_b, rel_type),
    CHECK (idiom_a <> idiom_b)
);

-- ------------------------------------------------------------
-- 6. 논술 주제 연결 (모모의 책장 커리큘럼 연동)
-- ------------------------------------------------------------
CREATE TABLE topic_link (
    idiom_id INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    topic    TEXT NOT NULL,     -- '정의', '노력과 성취', '권력과 책임'
    PRIMARY KEY (idiom_id, topic)
);

-- ------------------------------------------------------------
-- 7. 교재 등장 기록 (PDF 코퍼스 추출 결과)
--    표제어 선정 기준은 아니지만 frequency_score와 교재 연결에 쓴다.
-- ------------------------------------------------------------
CREATE TABLE corpus_hit (
    hit_id      INTEGER PRIMARY KEY,
    idiom_id    INTEGER NOT NULL REFERENCES idiom(idiom_id) ON DELETE CASCADE,
    doc_id      TEXT,
    material    TEXT,        -- 교재명
    lesson      TEXT,        -- 차시
    snippet     TEXT
);

CREATE INDEX idx_corpus_idiom ON corpus_hit(idiom_id);

-- ============================================================
-- 검수용 뷰
-- ============================================================

-- 필수 필드가 비어 있는 항목
CREATE VIEW v_incomplete AS
SELECT i.idiom_id, i.headword,
       (i.meaning IS NULL)      AS no_meaning,
       (i.level_min IS NULL)    AS no_level,
       (SELECT COUNT(*) FROM example e WHERE e.idiom_id = i.idiom_id) AS n_example
FROM idiom i
WHERE i.meaning IS NULL
   OR i.level_min IS NULL
   OR (SELECT COUNT(*) FROM example e WHERE e.idiom_id = i.idiom_id) < 2;

-- 출전은 있는데 유래가 없는 항목 (또는 그 반대) = 검토 대상
CREATE VIEW v_origin_mismatch AS
SELECT idiom_id, headword, origin_source, origin_story
FROM idiom
WHERE (origin_source IS NULL) <> (origin_story IS NULL);

-- 학년대별 출제 가능 풀
CREATE VIEW v_pool_by_level AS
SELECT level_min, COUNT(*) AS n
FROM idiom
WHERE status = 'published'
GROUP BY level_min
ORDER BY level_min;
