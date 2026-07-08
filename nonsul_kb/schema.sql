-- momo_kb  :  논술교재 콘텐츠 DB  (a+b 하이브리드)
-- (a) 문항 은행 골격  +  (b) 생성 파이프라인용 JSON 답안
-- SQLite 3.  PRAGMA foreign_keys=ON 을 켜고 쓸 것.

PRAGMA foreign_keys = ON;

-- 교재 1종 = 1행. 학생용/교사용 두 PDF가 한 교재로 묶인다.
CREATE TABLE IF NOT EXISTS materials (
    id                   INTEGER PRIMARY KEY,
    work_title           TEXT NOT NULL,      -- 예: 금오신화
    author               TEXT,               -- 예: 김시습
    grade                TEXT,               -- 예: 중1 / LV7
    quarter              INTEGER,            -- 분기
    week                 INTEGER,            -- 주차
    subject              TEXT,               -- 예: 국어문학
    src_student          TEXT,               -- 학생용 파일명
    src_teacher          TEXT,               -- 교사용 파일명
    created_at           TEXT DEFAULT (datetime('now'))
);

-- 핵심 반복 단위: 발문(문항). 학생용=답 숨김, 교사용=model_answer 표시.
CREATE TABLE IF NOT EXISTS items (
    id                   INTEGER PRIMARY KEY,
    material_id          INTEGER NOT NULL REFERENCES materials(id) ON DELETE CASCADE,
    stage                INTEGER,            -- 1/2/3 단계
    stage_title          TEXT,               -- 단계명("2단계:심화이해" / "2 질문과 토론") ← 교재별 상이
    excluded             INTEGER DEFAULT 0,  -- 교사용 [모모제외] 등 수업 제외 표시
    work                 TEXT,               -- 소속 작품(만복사저포기 등). 단일도서 교재면 NULL
    seq                  INTEGER,            -- 문항 번호(1~10)
    sub_seq              INTEGER,            -- 하위 (1)(2). 없으면 NULL
    prompt               TEXT,               -- 발문 텍스트
    passage_quote        TEXT,               -- 지문 인용
    passage_page         INTEGER,            -- 인용 원전 페이지
    answer_area_type     TEXT,               -- box | table | cloze | writing
    scaffold_labels      TEXT,               -- table일 때 행 라벨 JSON 배열 (a→b 재사용의 핵심)
    model_answer         TEXT,               -- (b) JSON. scaffold 있으면 {라벨:답}, 없으면 {"answer":...}
    answer_confidence    TEXT,               -- high | low  (거짓 고신뢰 검사 결과)
    answer_flags         TEXT,               -- 저신뢰 사유 JSON 배열(유형헤더시작/발문에코 등)
    is_starred           INTEGER DEFAULT 0,  -- ★ 중요 표시
    ord                  INTEGER,            -- 문서 내 순서
    UNIQUE(material_id, stage, seq, sub_seq)
);

-- 독해 유형: 한 문항에 복수 태그(사실적/추론적 등) → 다대다
CREATE TABLE IF NOT EXISTS item_tags (
    item_id              INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    reading_type         TEXT NOT NULL,      -- 사실적|추론적|분석적|비판적|적용|창의적|감상
    PRIMARY KEY (item_id, reading_type)
);

-- 문항에 딸린 참고 박스(<1>~<4> 이기론 등)
CREATE TABLE IF NOT EXISTS ref_notes (
    id                   INTEGER PRIMARY KEY,
    item_id              INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    label                TEXT,               -- <1>, <2> ...
    title                TEXT,               -- 이기론 / 이(理) ...
    body                 TEXT,
    ord                  INTEGER
);

-- 문항이 아닌 블록: 표지/배경지식/EXTRA창작/글쓰기 등 (교재별 형태가 유동적 → JSON)
CREATE TABLE IF NOT EXISTS content_blocks (
    id                   INTEGER PRIMARY KEY,
    material_id          INTEGER NOT NULL REFERENCES materials(id) ON DELETE CASCADE,
    block_type           TEXT,               -- cover | background | extra | writing
    title                TEXT,
    body                 TEXT,               -- JSON
    ord                  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_items_work    ON items(work);
CREATE INDEX IF NOT EXISTS idx_items_mat     ON items(material_id);
CREATE INDEX IF NOT EXISTS idx_tags_type     ON item_tags(reading_type);
