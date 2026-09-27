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
  provider TEXT,
  model TEXT,
  prompt_hash TEXT,
  text TEXT,
  unclear_count INTEGER,
  latency_ms INTEGER,
  created_at TEXT NOT NULL
);

-- 2026-09-27 사용자 지시 - 파트너 세션 인증(edition/auth.py). URL만 알면
-- 누구나 학생 런타임에 접근할 수 있던 구멍을 막는다: 파트너 서버가
-- api_key로 launch_token을 발급받아 학생 태블릿에 launch=토큰으로 전달하면,
-- 태블릿이 그 토큰을 한 번만 세션으로 교환한다(session 테이블, httpOnly
-- 쿠키). 학생 식별은 partner_student_id(파트너가 준 가명)만 쓴다.
CREATE TABLE IF NOT EXISTS partner (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  api_key_hash TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  disabled_at TEXT
);

CREATE TABLE IF NOT EXISTS launch_token (
  token TEXT PRIMARY KEY,
  partner_id INTEGER NOT NULL REFERENCES partner(id),
  partner_student_id TEXT NOT NULL,
  edition_id INTEGER NOT NULL REFERENCES edition(id),
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used_at TEXT
);

CREATE TABLE IF NOT EXISTS session (
  id TEXT PRIMARY KEY,
  partner_id INTEGER NOT NULL REFERENCES partner(id),
  partner_student_id TEXT NOT NULL,
  edition_id INTEGER NOT NULL REFERENCES edition(id),
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);
"""

# 2026-09-27 사용자 지시 - 인식 제공자(gemini/anthropic/openai)를 함께 남긴다.
# CREATE TABLE IF NOT EXISTS는 이미 만들어진 기존 edition_store.db엔 새 컬럼을
# 추가해 주지 않으므로 ALTER TABLE로 마이그레이션한다(컬럼이 이미 있으면
# "duplicate column" 예외가 나는데, 그건 이미 마이그레이션된 것이므로 무시).
_MIGRATIONS = [
    "ALTER TABLE recognition_log ADD COLUMN provider TEXT",
]


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e):
                    raise
        conn.commit()
    finally:
        conn.close()


def reset_db() -> None:
    """테스트 전용 - edition_store.db를 완전히 비운다."""
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
