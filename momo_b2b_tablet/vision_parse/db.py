"""방식 B(비전 파싱) 결과 저장소 - momo_book.db와 완전히 분리된 별도 SQLite 파일.

momo_book.db(원본, 읽기 전용)는 여기서 절대 건드리지 않는다(normalize/db.py와
같은 원칙). 되돌릴 수 있게 항상 새 DB에만 쓴다 - 방식 B를 채택 안 하기로 하면
이 파일 하나만 지우면 원상복구된다.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "vision_extract.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS vision_page (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  page_no INTEGER NOT NULL,
  source_pdf_sha256 TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  elapsed_sec REAL NOT NULL,
  input_tokens INTEGER NOT NULL,
  output_tokens INTEGER NOT NULL,
  raw_response_json TEXT NOT NULL,   -- 파싱 전 원본 응답(디버깅/재파싱용)
  parse_error TEXT,                  -- JSON 파싱 실패 시 사유(성공하면 NULL)
  extracted_at TEXT NOT NULL,
  UNIQUE(doc_id, page_no, prompt_version)
);

CREATE TABLE IF NOT EXISTS vision_item (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL REFERENCES vision_page(id) ON DELETE CASCADE,
  doc_id TEXT NOT NULL,
  page_no INTEGER NOT NULL,
  item_index INTEGER NOT NULL,       -- 페이지 안에서 몇 번째 item인지(0부터)
  item_type TEXT,
  reading_type TEXT,
  excerpt_text TEXT,
  question_text TEXT,
  blanks_json TEXT,                  -- JSON 배열 문자열
  choices_json TEXT,
  table_json TEXT,
  page_number TEXT,
  layout_shape TEXT,
  blank_lines INTEGER,
  cell_size_hint TEXT,
  layout_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_vision_page_doc ON vision_page(doc_id);
CREATE INDEX IF NOT EXISTS idx_vision_item_doc ON vision_item(doc_id);

CREATE TABLE IF NOT EXISTS run_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  status TEXT NOT NULL,              -- success|failed|skipped
  pages_done INTEGER,
  total_elapsed_sec REAL,
  total_input_tokens INTEGER,
  total_output_tokens INTEGER,
  error TEXT,
  run_at TEXT NOT NULL
);
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def doc_already_done(doc_id: str, prompt_version: str) -> bool:
    """체크포인트 확인용 - run_log에 이 문서의 성공 기록이 있으면 건너뛴다."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT 1 FROM run_log WHERE doc_id = ? AND status = 'success' "
            "AND doc_id IN (SELECT doc_id FROM vision_page WHERE prompt_version = ?) LIMIT 1",
            (doc_id, prompt_version),
        ).fetchone()
        return row is not None
    finally:
        conn.close()
