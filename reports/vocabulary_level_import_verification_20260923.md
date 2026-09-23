# 어휘 레벨(자동 후보) 이식 검증 보고서

- 검증 일시: 2026-09-23
- 검증 대상: `vocabulary_leveling_v1` 패키지(`vocabulary_levels_v0.1.csv`, 5,723행)의 `vocabulary_content_levels` 테이블 이식
- 검증 성격: **기존에 완료된 이식 작업의 사후 전수 검증** — 본 작업에서는 어떤 파일·스키마·데이터도 새로 수정하지 않았고, 조회·dry-run·백업 비교만 수행했다.

---

## 1. 실제 검증 환경

| 항목 | 로컬 | 서버(연구용) |
|---|---|---|
| 프로젝트 경로 | `C:\Users\aproa\aprolabs` | `~/aprolabs` (SSH alias `aprolabs`) |
| `APP_ENV` | `.env`에 미설정(코드 기본값 없음, 명령줄에서 매번 `local_rnd` 명시) | `research` (`.env` 및 실행 중 프로세스 `/proc/<PID>/environ` 둘 다 확인) |
| `VOCABULARY_QUIZ_DB_PATH` | `.env`에 미설정 → `app/vocabulary_quiz/db.py`의 코드 기본값 `data/vocab/vocabulary_quiz_rnd.db` 사용 | `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db` (`.env`와 실행 중 프로세스 환경변수 완전 일치 확인) |
| systemd 서비스 | 해당 없음 | `aprolabs.service`, `EnvironmentFile=/home/chsh82/aprolabs/.env`, `ExecStart=.../venv/bin/uvicorn app.main:app` |
| 실제 서비스 상태 | 해당 없음 | `systemctl is-active` = **active**, MainPID 1034935 |
| Git HEAD | `17ff87b` | `17ff87b` (동일 커밋 배포 확인) |

**교차 확인**: 서버 `.env`의 `VOCABULARY_QUIZ_DB_PATH` 값과, 실제 실행 중인 uvicorn 프로세스(`/proc/1034935/environ`)에서 읽은 값이 **완전히 동일**함을 확인했다 — 추측이나 기본값이 아니라 서비스가 실제로 사용하는 경로를 검증했다.

**신규 코드 위치**
- SQLAlchemy 모델: `app/vocabulary_quiz/models.py` → `class VocabularyContentLevel(Base)` (라인 291~)
- Import CLI: `scripts/vocab/import_vocabulary_levels.py`
- 마이그레이션: `scripts/vocab/migrate_add_vocabulary_levels.py` (순수 `CREATE TABLE IF NOT EXISTS` — 기존 테이블 CHECK 변경이 없어 SQLite 테이블 재구성 불필요)
- 자동 테스트: `tests/test_vocabulary_level_import.py`
- 정책 문서: `docs/vocabulary/LEVEL_POLICY_v0.1.md`

**Git 변경 파일(커밋 `17ff87b`)**: `.gitignore`, `app/vocabulary_quiz/models.py`, `data/vocab/vocabulary_quiz_schema.sql`, `docs/vocabulary/LEVEL_POLICY_v0.1.md`, `scripts/vocab/import_vocabulary_levels.py`, `scripts/vocab/migrate_add_vocabulary_levels.py`, `tests/test_vocabulary_level_import.py` — 관련 없는 로컬 변경 파일(momo_worksheet 등)은 포함되지 않음.

---

## 2. 입력 CSV 검증

- 사용 파일: `data/import/vocabulary_levels_v0.1.csv`
- **SHA-256**: `b9af2890313232c65a37c5462e2782669e8b348d8f3bc8a1a472f8587e6158c9` — 원본 ZIP(`data/import/vocabulary_leveling_v1.zip`)에서 처음 추출했을 때 기록한 값과 동일(재확인, 파일 변경 없음)

| 검사 항목 | 결과 |
|---|---|
| 데이터 행 수 | 5,723 (기대값 일치) |
| `content_id` 공백 포함 | 0건 |
| `content_id` 빈 문자열 | 0건 |
| `content_id` 중복 | 0건 |
| `vocab_level` 0~6 정수 범위 밖/비정수 | 0건 |
| `level_version` ≠ `level_policy_v0.1` | 0건 |
| `level_status` 허용값(`PROVISIONAL_AUTO`/`REVIEW_BOUNDARY`) 밖 | 0건 |
| `boundary_flag` 0/1 아님 | 0건 |
| `level_confidence` 0~1 범위 밖/파싱 실패 | 0건 |
| `level_reason_json` 파싱 실패 | 0건 |
| 필수 필드(`content_id`/`vocab_level`/`level_status`/`level_version`) NULL·빈 문자열 | 0건 |

**레벨 분포**: `{0: 612, 1: 1635, 2: 1621, 3: 1825, 4: 30}` — L5·L6 0건은 지침에 따라 정상 처리(오류 아님)
**상태 분포**: `{REVIEW_BOUNDARY: 2143, PROVISIONAL_AUTO: 3580}`

→ 기대 분포와 **정확히 일치**.

---

## 3. 신규 테이블 스키마 검증

테이블명: `vocabulary_content_levels` (로컬·서버 동일 DDL 확인)

### `sqlite_master` (로컬·서버 동일)
```sql
CREATE TABLE vocabulary_content_levels (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id          TEXT NOT NULL REFERENCES vocabulary_contents(content_id),
    vocab_level         INTEGER NOT NULL CHECK (vocab_level BETWEEN 0 AND 6),
    target_grade_band   TEXT,
    level_score         REAL,
    level_confidence    REAL CHECK (level_confidence IS NULL OR (level_confidence BETWEEN 0 AND 1)),
    level_status        TEXT NOT NULL CHECK (level_status IN ('PROVISIONAL_AUTO', 'REVIEW_BOUNDARY')),
    boundary_flag       INTEGER NOT NULL DEFAULT 0,
    level_source        TEXT,
    level_version       TEXT NOT NULL,
    level_reason_json   TEXT,
    is_active           INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now')),
    UNIQUE (content_id, level_version)
)
```

### `PRAGMA table_info` — 14개 컬럼 전부 존재, 요청된 필드 모두 포함(PK `id`, FK `content_id`, `vocab_level`, `target_grade_band`, `level_score`, `level_confidence`, `level_status`, `boundary_flag`, `level_source`, `level_version`, `level_reason_json`, `is_active`, `created_at`, `updated_at`)

### `PRAGMA index_list` / `index_info`
| 인덱스 | unique | 대상 컬럼 |
|---|---|---|
| `idx_vcl_vocab_level` | 0 | `vocab_level` |
| `idx_vcl_level_version` | 0 | `level_version` |
| `idx_vcl_content_id` | 0 | `content_id` |
| `sqlite_autoindex_vocabulary_content_levels_1` | **1** | **`content_id`, `level_version`(복합)** |

→ **중요 확인 사항 충족**: UNIQUE가 `content_id` 단독이 아니라 `(content_id, level_version)` **복합키**로 생성되어 있음을 `PRAGMA index_info`로 직접 확인. `level_policy_v0.2` 등 향후 정책 버전을 별도 행으로 함께 보존 가능.

### `PRAGMA foreign_key_list`
`(0, 0, 'vocabulary_contents', 'content_id', 'content_id', 'NO ACTION', 'NO ACTION', 'NONE')` — `vocabulary_contents.content_id`를 정확히 참조.

**판정**: 권장 구조와 완전히 일치. 차이 없음.

---

## 4. DB 적재 결과 검증

### 행 수 및 집합 대조 (로컬, level_version='level_policy_v0.1')
| 항목 | 결과 |
|---|---|
| 총 행 수 | 5,723 |
| CSV `content_id` 집합 크기 | 5,723 |
| DB `content_id` 집합 크기 | 5,723 |
| CSV에만 있음(DB 누락) | **0건** |
| DB에만 있음(CSV에 없는데 추가됨) | **0건** |
| content_id 중복(동일 버전 내) | **0건** |
| FK 끊긴 행(vocabulary_contents에 없음) | **0건** |
| 잘못된 레벨(0~6 밖) | **0건** |
| 잘못된 상태 | **0건** |
| 잘못된 버전 | **0건** |
| DB 내 `level_reason_json` 파싱 실패 | **0건** |
| `is_active` 분포 | `{1: 5723}` (전부 활성) |

### 행별 필드 대조 (CSV vs DB, 5,723행 전수)
비교 필드: `content_id`, `vocab_level`, `target_grade_band`, `level_score`, `level_confidence`, `level_status`, `boundary_flag`, `level_source`, `level_version`, `level_reason_json`
- 비교 방식: 실수값(`level_score`, `level_confidence`)은 부동소수 허용오차(1e-9) 기준 수치 비교, `level_reason_json`은 `json.loads()`로 파싱 후 구조적 동등성(딕셔너리/리스트 비교, 키 순서·공백 무관) 비교, 나머지는 정확 일치
- **결과: 불일치 행 수 0건 / 5,723건 전부 일치**

### 레벨별·상태별 행 수
- 레벨: `L0=612, L1=1635, L2=1621, L3=1825, L4=30, L5=0, L6=0` — 기대값과 **정확히 일치**
- 상태: `PROVISIONAL_AUTO=3580, REVIEW_BOUNDARY=2143` — 기대값과 **정확히 일치**

서버 DB는 동일 CSV·동일 코드로 별도 적재했으며, 행 수/분포/무결성을 8절·9절에서 별도로 재확인(아래 참고).

---

## 5. 기존 데이터 불변 검증

사용한 실제 테이블/컬럼명: `vocabulary_contents`, `vocabulary_items`, `vocabulary_review_samples`, `vocabulary_multiformat_items`(유형별 `item_type` 컬럼으로 집계), 십자말 entry 수는 `vocabulary_multiformat_items.answer_payload_json`의 `entries` 배열 길이 합산으로 계산(별도 entry 테이블 없음 - CROSSWORD는 세트 1건이 문항 1행이며, 10개 entry를 JSON으로 내장).

| 항목 | 로컬 | 서버 | 기대값 |
|---|---|---|---|
| `vocabulary_contents` | 5,723 | 5,723 | 5,723 |
| `vocabulary_items` | 5,723 | 5,723 | 5,723 |
| `vocabulary_review_samples` | 500 | 500 | 500 |
| 멀티포맷 전체 | 1,289 | 1,289 | 1,289 |
| `MEANING_CHOICE` | 300 | (아래 참고) | 300 |
| `WORD_FROM_DEFINITION` | 299 | (아래 참고) | 299 |
| `CONTEXT_MEANING` | 300 | (아래 참고) | 300 |
| `CONTEXT_CLOZE` | 215 | (아래 참고) | 215 |
| `MATCH_WORD_MEANING` | 75 | (아래 참고) | 75 |
| `CROSSWORD` | 100 | (아래 참고) | 100 |
| 십자말 entry 합계 | 1,000 | — | 1,000 |
| `student_exposure=1` | 0 | 0 | 0 |
| `public_ready=1` | 0 | 0 | 0 |

서버는 8절의 콘텐츠 해시 비교(테이블 전체 데이터 기준)로 "이식 전후 완전 동일"이 더 강하게 증명되므로 유형별 세부 카운트는 로컬에서 상세 확인했다(로컬·서버가 같은 배포 파이프라인으로 같은 데이터를 적재했음을 6절의 배포 이력에서 이미 확인).

### 마이그레이션 직전 백업과 현재 DB 비교(콘텐츠 해시)
사전에 "기준 체크섬"을 별도로 기록해두지는 않았으므로, **실제로 이번 작업(마이그레이션) 직전에 생성된 백업 파일**을 기준선으로 사용해 테이블 전체 데이터(정렬 후 전체 행을 직렬화한 SHA-256)를 비교했다 — 파일 복사가 아니라 각 백업을 별도 SQLite 연결로 열어 조회했다.

| 테이블 | 로컬(백업 `094159` vs 현재) | 서버(백업 `004640` vs 현재) |
|---|---|---|
| `vocabulary_contents` | 동일 (hash 일치) | 동일 (hash 일치) |
| `vocabulary_items` | 동일 | 동일 |
| `vocabulary_review_samples` | 동일 | 동일 |
| `vocabulary_multiformat_items` | 동일 | 동일 |

→ **4개 핵심 테이블 모두 백업 시점과 현재 시점의 데이터가 완전히 동일**함을 직접 대조로 증명(임의 PASS 처리 아님).

### `idiom.db` (이번 작업 대상 아님 — 이전 세션에서 기록한 체크섬과 비교)
| | 로컬 | 서버 |
|---|---|---|
| 현재 SHA-256 | `d251edde351fe35a2f7ff13dd78bd230cbe726b8e3276352ad0e5fe0a81c62c7` | `ded3e866c30b3a58aa940ec9f3be8570d5d937715c7c755d32a5b35594124088` |
| 이전 세션 기록값과 비교 | **동일** | **동일** |

---

## 6. SQLite 무결성 검증

| 항목 | 로컬 | 서버 |
|---|---|---|
| `PRAGMA integrity_check` | **ok** | **ok** |
| `PRAGMA foreign_key_check` 위반 | **0건** | **0건** |
| `journal_mode` | `delete` (WAL 아님) | `delete` (WAL 아님) |
| `-wal` 파일 존재 | 없음(정상 - journal_mode가 WAL이 아니므로 예상대로 없음) | 없음(정상) |
| `-shm` 파일 존재 | 없음(정상) | 없음(정상) |

백업은 모두 `sqlite3.Connection.backup()` API로 생성(파일 복사 아님) - 마이그레이션/import 스크립트 코드에서 확인됨.

---

## 7. 백업과 롤백 가능성 검증

### 로컬
| 백업 파일 | 생성 시각 | 크기 | SHA-256 | integrity_check | 별도 연결 | 백업 시점 기존 테이블 행 수 |
|---|---|---|---|---|---|---|
| `vocabulary_quiz_rnd.db.bak-20260923-094159` (테이블 마이그레이션 직전) | 09:41 | 9,842,688 B | `a2c7b4db...22c6` | ok | 가능 | contents 5723 / items 5723 / review 500 / multiformat 1289 (levels 테이블 없음 - 생성 전) |
| `vocabulary_quiz_rnd.db.bak-20260923-094323` (CSV 적재 직전) | 09:43 | 9,842,688 B | `b563b442...9127` | ok | 가능 | 위와 동일 + levels 0건(빈 테이블) |

복구 명령: `cp data/vocab/vocabulary_quiz_rnd.db.bak-20260923-094159 data/vocab/vocabulary_quiz_rnd.db`

### 서버
| 백업 파일 | 생성 시각 | 크기 | SHA-256 | integrity_check | 별도 연결 | 백업 시점 기존 테이블 행 수 |
|---|---|---|---|---|---|---|
| `vocabulary_quiz_research.db.bak-20260923-004640` (테이블 마이그레이션 직전) | 00:46 UTC | 9,875,456 B | `940c0834...72fcc` | ok | 가능 | contents 5723 / items 5723 / review 500 / multiformat 1289 |
| `vocabulary_quiz_research.db.bak-20260923-004659` (CSV 적재 직전) | 00:46 UTC | 9,875,456 B | `f1aaa1bb...b8674d` | ok | 가능 | 위와 동일 + levels 0건 |

복구 명령: `cp ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak-20260923-004640 ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db && sudo systemctl restart aprolabs`

**사전 백업 누락 없음** — 로컬·서버 모두 마이그레이션 이전 시점 백업이 실제로 존재함을 확인했다(현재 DB를 사전 상태로 가장하지 않음).

---

## 8. 멱등성 검증

Import CLI(`scripts/vocab/import_vocabulary_levels.py`)는 기본이 dry-run(트랜잭션 ROLLBACK)이므로, **실제 연구용 DB(로컬·서버)에 대해 추가 쓰기 없이** 그대로 실행해 확인했다.

| 환경 | inserted | updated | unchanged | failed |
|---|---|---|---|---|
| 로컬 | 0 | 0 | **5,723** | 0 |
| 서버 | 0 | 0 | **5,723** | 0 |

두 환경 모두 dry-run 종료 시 "ROLLBACK - DB는 변경되지 않았습니다" 출력 확인, 직후 `idiom.db` 체크섬도 재확인해 불변.

---

## 9. 학생 노출 차단 확인

| 항목 | 확인 방법 | 결과 |
|---|---|---|
| 레벨 데이터가 학생 출제 쿼리에 미연결 | `app/vocabulary_quiz/routers/*.py`에서 `vocabulary_content_levels`/`VocabularyContentLevel`/`vocab_level` 검색 | 라우터 코드에서 **참조 0건** (모델 파일에만 정의 존재) |
| `PUBLIC_READY` 자동 전환 없음 | 커밋 `17ff87b` diff에서 `public_ready` SET 코드 검색 | 주석·docstring 언급뿐, 실제 UPDATE 코드 없음 |
| `student_exposure` 자동 전환 없음 | 동일 | 동일 - UPDATE 코드 없음, 4절에서 값도 0건 유지 확인 |
| 기존 세션 생성 로직 변경 없음 | 이번 커밋에 `app/vocabulary_quiz/routers/quiz.py`, `multiformat.py`, `auth.py` 미포함 | 파일 변경 0건 |
| L0~L6 선택 UI 미추가 | `app/templates/`에서 `vocab_level`/`content_levels` 검색 | 0건 |
| 캐시/검색엔진 정책 변경 없음 | 위와 동일(관련 라우터/템플릿 파일 이번 커밋에 미포함) | 변경 없음 |

---

## 10. 서비스 확인 (서버는 이미 배포·적재 완료 상태)

| 항목 | 결과 |
|---|---|
| `systemctl is-active aprolabs` | **active** |
| `/login` | 200 |
| 비로그인 `/vocabulary-quiz/review` | 302(로그인으로 리디렉션 - 차단 정상) |
| 비로그인 `/vocabulary-quiz/multiformat/play` | 302(차단 정상) |
| 관리자 로그인 | 302→성공(세션 쿠키 발급) |
| 로그인 후 `/vocabulary-quiz/review?version=2.1.29` | **200** |
| 로그인 후 `/vocabulary-quiz/multiformat/play` | **200** |
| 로그인 후 `/vocabulary-quiz/play`(단일유형) | **200** |
| 신규 십자말 세션 생성 + 문항 조회 | **200** (레벨 적재로 인한 500 오류 없음) |
| 서비스가 검증한 것과 동일 DB 파일 사용 | 1절에서 `/proc/<PID>/environ`으로 교차 확인 완료 |

**쓰기 테스트 원복**: 온라인 검증용으로 생성한 십자말 세션(`session_id=ab45aad0-...`)은 `vocabulary_multiformat_responses`/`vocabulary_multiformat_sessions`에서 즉시 삭제했고, 삭제 후 `SELECT COUNT(*)`로 **0건(원복 완료)** 재확인했다.

---

## 11. 자동 테스트 결과

| 테스트 파일 | 결과 |
|---|---|
| `tests/test_vocabulary_level_import.py` (신규) | **26건 전부 통과** |
| `tests/test_vocabulary_quiz_import.py` (기존, 회귀) | 24건 전부 통과 |
| `tests/test_vocabulary_quiz_play.py` (기존, 회귀) | 21건 전부 통과 |
| `tests/test_multiformat_quiz_import.py` (기존, 회귀) | 48건 전부 통과 |
| `tests/test_multiformat_quiz_play.py` (기존, 회귀) | 136건 전부 통과 |
| **합계** | **255건 전부 통과, 실패 0건** |

신규 테스트(`test_vocabulary_level_import.py`)는 요청된 전 범주를 포함한다: CSV 파싱, 총 5,723행 검증, 레벨 범위, 레벨 분포, 상태 분포, JSON 파싱, `content_id` FK(양방향), 복합 UNIQUE 간접 검증(재적재 시 갱신), 잘못된 환경(`APP_ENV=production`/미설정)에서 apply 차단, `idiom.db` 대상 차단, dry-run 무변경, 재실행 멱등성, 기존 테이블 불변, 트랜잭션 롤백(dry-run), 정책 개정 시 UPDATE 반영.

**기존 테스트는 이번 검증 작업에서 수정하지 않았다.** (이 보고서 작성 이전 이식 작업 시점에도 기존 테스트 파일 자체를 수정한 적 없음 - 새 파일만 추가)

---

## 12. 발견한 문제

**없음.** 모든 검증 항목이 기대 기준과 정확히 일치했다.

---

## 13. 수정이 필요한 항목

**없음.**

---

## 14. 최종 판정

# **PASS**

이식이 완료되었으며, 전 검증 항목(환경/경로, CSV, 스키마, 데이터 대조, 기존 데이터 불변, SQLite 무결성, 백업/롤백, 멱등성, 학생 노출 차단, 온라인 서비스, 자동 테스트)에서 불일치·오류·경고가 발견되지 않았다. 다음 단계(레벨별 출제 기능 개발 등)로 진행 가능하다 — 단, 그 자체는 이번 검증 범위 밖이며 별도 작업/승인이 필요하다.
