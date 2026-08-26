-- 모모의책장 교재DB — 승인된 스키마 (요청서 v1.1 + 2026-08-26 승인 변경사항)
-- 변경점: discussion_qa.order_label TEXT 추가 (하위 번호 "4-1"/"4-2" 보존용)
-- 변경점(2026-08-26 #2): documents.cover_message, essay_prompt.closing_instruction,
--                        document_image 테이블 추가 (표지 문구/삽화/Step2 안내문 추출)
-- 변경점(2026-08-26 #3): documents.background_text 추가 (중등 1단계 - 어휘/OX 표 없이
--                        배경지식 설명글만 있는 경우 텍스트를 이 필드에 저장)

CREATE TABLE documents (
    doc_id          TEXT PRIMARY KEY,   -- 예: L5-Q4-W10
    curriculum_id   TEXT,               -- momo_bookshelf_weeks.id 참조
    level           TEXT,
    quarter         TEXT,
    week            INTEGER,
    book_title      TEXT NOT NULL,
    book_author     TEXT,
    isbn            TEXT,
    cover_message   TEXT,               -- 표지 하단 대표 문구 - 2026-08-26 추가
    background_text TEXT,               -- 중등 1단계 배경지식 설명글(어휘/OX 표 없을 때) - 2026-08-26 추가
    source_file     TEXT NOT NULL,
    source_format   TEXT,
    source_hash     TEXT,
    version         INTEGER DEFAULT 1,
    parsed_at       TEXT,
    review_status   TEXT DEFAULT 'pending'
);

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
);

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
);

CREATE TABLE discussion_qa (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    order_no        INTEGER NOT NULL,
    order_label     TEXT,               -- 원문 그대로("1","4-1","4-2"...) - 2026-08-26 추가
    reading_type    TEXT,
    excerpt_text    TEXT,
    excerpt_page    INTEGER,
    question_text   TEXT NOT NULL,
    reference_text  TEXT,               -- "보기" 텍스트(질문에 따라붙는 참고자료) - 검수 중 수동 분리, 2026-08-26 추가
    reference_image_path TEXT,          -- "보기" 이미지(있는 경우) - document_image 중에서 선택, 2026-08-26 추가
    ui_type         TEXT,
    ui_config       TEXT,
    model_answer    TEXT,
    source_page     INTEGER,
    raw_text        TEXT,
    extraction_confidence REAL,
    review_status   TEXT DEFAULT 'pending'
);

CREATE TABLE essay_prompt (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    main_topic      TEXT NOT NULL,
    writing_guide   TEXT,               -- 주제와 Step1 사이의 글쓰기 안내/설명 - 2026-08-26 추가
    writing_format  TEXT,
    min_length      INTEGER,
    closing_instruction TEXT,           -- Step2 안내문("앞선 질문들에...") - 2026-08-26 추가
    source_page     INTEGER,
    raw_text        TEXT,
    extraction_confidence REAL,
    review_status   TEXT DEFAULT 'pending'
);

CREATE TABLE essay_outline_question (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    essay_id        INTEGER NOT NULL REFERENCES essay_prompt(id),
    order_no        INTEGER NOT NULL,
    question_text   TEXT NOT NULL,
    role            TEXT
);

CREATE TABLE document_image (          -- 표지/삽화 이미지 - 2026-08-26 추가
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    image_type      TEXT NOT NULL,      -- 'cover' | 'illustration'
    source_page     INTEGER,
    file_path       TEXT NOT NULL,      -- momo_book_db/extracted_images/{doc_id}/... 상대경로
    extraction_confidence REAL
);

CREATE TABLE extraction_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          TEXT REFERENCES documents(doc_id),
    level           TEXT,
    stage           TEXT,
    message         TEXT,
    created_at      TEXT
);
