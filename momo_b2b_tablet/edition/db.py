"""edition 저장소 스키마 - SPEC §5(원안) + 사용자 지시(2026-09-23) 4단계 확장.

momo_book.db(원본, 읽기 전용)와 완전히 분리된 별도 SQLite 파일이다 - 이 DB는
자유롭게 쓰고 지워도 된다(momo_book.db는 여기서 절대 건드리지 않는다).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "edition_store.db"

# SPEC §5 원안 edition_flag.kind: sup|derived|typo|lowres|split|ocr. 우리 정규화
# Flag taxonomy는 missing도 쓰고, 사용자 지시(4단계 1번)로 widget_unavailable
# (위젯 신호 없음)·placeholder(자리표시자, approve 차단 대상) 2종을 더 추가한다.
EDITION_FLAG_KINDS = {
    "sup", "missing", "derived", "split", "typo", "lowres", "ocr",
    "widget_unavailable", "placeholder",
}
CORRECTION_LOG_KINDS = {"text_correction", "form_change"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS edition (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',      -- draft|review|approved|published
  layout_json TEXT NOT NULL,
  rev INTEGER NOT NULL DEFAULT 1,            -- 낙관적 잠금용(5단계) - version과는 다른 축:
                                              -- version은 "확정 후 다시 고치면 새 버전"이고
                                              -- rev는 "같은 draft를 두 사람이 동시에 고칠 때 충돌 감지"
  band TEXT,
  quarter TEXT,
  created_by TEXT,
  created_at TEXT NOT NULL,
  approved_by TEXT,
  approved_at TEXT,
  source_hash TEXT,
  UNIQUE(doc_id, version)
);

CREATE TABLE IF NOT EXISTS edition_flag (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  edition_id INTEGER NOT NULL REFERENCES edition(id),
  page_idx INTEGER,
  path TEXT,                                 -- JSON pointer(가능한 경우)
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  resolved_by TEXT,
  resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS correction_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  edition_id INTEGER REFERENCES edition(id),
  field_path TEXT NOT NULL,
  before TEXT,
  after TEXT,
  kind TEXT NOT NULL DEFAULT 'text_correction',  -- text_correction|form_change
  reason TEXT,
  editor TEXT,
  created_at TEXT NOT NULL,
  pushed_to_source_at TEXT
);

CREATE TABLE IF NOT EXISTS image_candidate (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  edition_id INTEGER NOT NULL REFERENCES edition(id),
  slot_path TEXT NOT NULL,
  model TEXT,
  prompt TEXT,
  file_path TEXT,
  width INTEGER,
  height INTEGER,
  chosen INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS student_answer (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  student_id TEXT NOT NULL,
  edition_id INTEGER NOT NULL REFERENCES edition(id),
  part_id TEXT NOT NULL,
  ink_json TEXT,
  text TEXT,
  confirmed INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  rev INTEGER NOT NULL DEFAULT 1,
  UNIQUE(student_id, edition_id, part_id)
);

CREATE TABLE IF NOT EXISTS recognition_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  answer_id INTEGER NOT NULL REFERENCES student_answer(id),
  model TEXT,
  prompt_hash TEXT,
  text TEXT,
  unclear_count INTEGER,
  latency_ms INTEGER,
  created_at TEXT NOT NULL
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


def reset_db() -> None:
    """테스트 전용 - edition_store.db를 완전히 비운다."""
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
