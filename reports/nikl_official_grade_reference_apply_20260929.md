# 국립국어원 공식 등급 — 별도 참조 테이블 적재·검증

- 일자: 2026-09-29
- 범위: 연구 DB(`vocabulary_quiz_research.db`)에 신규 테이블
  `vocabulary_official_grade_reference`만 추가·적재. **기존
  `vocabulary_content_levels.vocab_level`, 기존 문항, 파일럿 매니페스트,
  공개 플래그는 전혀 변경하지 않았다.** momolib 이식·학생 공개·현재
  출제 레벨 변경 전혀 없음. 이 세션 전체를 통틀어 처음으로 실제 DB에
  쓰기를 수행한 단계다(그 이전 단계는 전부 읽기 전용 분석).
- 산출 스크립트: `scripts/vocab/migrate_add_official_grade_reference.py`,
  `scripts/vocab/apply_official_grade_reference.py`

## 1. 스키마

```sql
CREATE TABLE vocabulary_official_grade_reference (
    content_id                     TEXT PRIMARY KEY REFERENCES vocabulary_contents(content_id),
    official_grade                 TEXT,      -- '1'..'5' 또는 NULL
    proposed_base_level            TEXT,      -- 'L0'..'L2'/'경계(L3~L4)'/NULL
    proposed_base_level_note       TEXT,
    match_type                     TEXT NOT NULL,  -- 5종 상호 배타(2절)
    standard_homonym_number        INTEGER,
    exception_reason                TEXT,
    exception_reason_secondary      TEXT,
    priority_tier                  INTEGER,   -- 예외 823건에만 1~3(4는 0건)
    review_status                   TEXT NOT NULL,  -- 기계 제안 상태
    human_approval_status           TEXT,      -- 이번 적재는 전부 NULL(예약 컬럼)
    current_vocab_level_snapshot   INTEGER,   -- 적재 시점 스냅샷, 자동 갱신 안 됨
    current_level_status_snapshot  TEXT,
    level_source                    TEXT,
    source_report_seq              INTEGER NOT NULL,  -- 1160
    source_file_sha256              TEXT NOT NULL,     -- 공식 xlsx SHA-256
    source_version_label            TEXT NOT NULL,
    computed_at                     TEXT NOT NULL,
    computed_by_script              TEXT NOT NULL
)
```

기존 `vocabulary_content_levels`에 컬럼을 추가하는 대신(9ca9cc2 보고서
§4에서 이미 권고한 대로) 완전히 별도 테이블로 설계 — 서빙 코드가 읽는
`vocab_level`을 원천적으로 건드릴 수 없는 구조다. 현재 어떤 라우터도
이 테이블을 읽지 않는다(이번 범위 밖).

## 2. 적재 - 5,950건, 5-way 매칭 유형 구분

3개 입력 CSV(`nikl_base_level_policy_20260929.csv`,
`nikl_official_match_full_20260929.csv`,
`nikl_exceptions_priority_tiers_20260929.csv`)를 `content_id`로 조인
(집합 완전 일치 확인 후 진행). 실제 적재 결과(테이블 직접 재조회):

| match_type | 건수 | official_grade/proposed_base_level |
|---|---:|---|
| 단일일치 | 5,825 | 채움 |
| 단일일치_동형이의주의 | 4 | 채움 |
| 다중후보 | 7 | **NULL(추정 안 함)** |
| 매칭없음 | 101 | **NULL(추정 안 함)** |
| 표제어만일치 | 13 | **NULL(추정 안 함)** |
| **합계** | **5,950** | |

121건(다중후보+매칭없음+표제어만일치)에 official_grade/proposed_base_level이
채워진 행 0건을 직접 재확인했다(하드 assert + 사후 쿼리 이중 확인).
`human_approval_status`가 NULL이 아닌 행도 0건 — 이번 적재로 어떤
자동 판정도 "사람 승인"으로 표시되지 않았다.

**한 가지 버그를 잡고 수정**: 최초 조립 시도에서 다중후보/매칭없음
행 일부(`SR_L4CORE_4787` 등)가 `official_grade='다중'`, `'-'` 같은
플레이스홀더 문자열을 갖고 있어 "추정 금지" 가드에 false positive로
걸렸다 — 실제 등급 추정이 아니라 CSV의 "값 없음" 표기 방식 차이였다.
`'1'~'5'`인 경우만 진짜 등급으로 인정하도록 스크립트를 수정해 재검증했다.

## 3. Dry-run → 실제 적용 절차

1. **스크래치 사본에서 전체 플로우 선검증**: 연구 DB를 스크래치 경로로
   복사 → 마이그레이션 → dry-run → 실제 적용(가상) → 재실행(멱등성) 전부
   스크래치 DB에서 먼저 통과 확인 후 삭제.
2. **실제 DB 적용**:
   - GATE 1 `APP_ENV=research` 확인
   - GATE 2 DB 경로 확인 — **1차 시도에서 `VOCABULARY_QUIZ_DB_PATH`를
     빠뜨려 마이그레이션이 기본값 DB(`data/vocab/vocabulary_quiz_rnd.db`,
     연구 DB와 무관한 별개 로컬 DB)에 잘못 실행된 것을 즉시 발견 -
     빈 테이블 생성 + 백업 파일 생성뿐이었으므로 두 산출물 모두 즉시
     제거(`DROP TABLE`, 백업 파일 삭제)하고 원래 상태로 복구한 뒤,
     올바른 연구 DB 경로로 재실행했다.** (부록 참고)
   - GATE 3 적용 전 SHA-256: `788a03cf...`
   - GATE 4 SQLite Backup API 백업: `vocabulary_quiz_research.db.bak_official_grade_ref_20260929-093005`,
     복원 가능성 검증(`integrity_check=ok`, `contents=5950`, `items=1553`) 통과
   - GATE 5 입력 CSV 3종 SHA-256 확인(dry-run 때와 동일 파일임을 재확인)
   - GATE 7 충돌 검사: 기존 행 0건이라 충돌 0건
   - GATE 8 단일 트랜잭션 커밋: 신규 삽입 5,950건 성공
   - GATE 9 `integrity_check=ok`, `foreign_key_check` 위반 0건
   - GATE 10 콘텐츠 5,950→5,950, 문항 1,553→1,553 불변
   - GATE 11 테이블 총수 5,950 확인
3. **재실행(멱등성 테스트)**: 동일 스크립트를 다시 `--apply`로 실행 →
   기존 행 5,950건 전부 "동일값 스킵", 신규 삽입 0건, 충돌 0건 - **재실행
   안전성 확인.**

## 4. 적용 후 불변 조건 검사(전부 재쿼리로 확인, 가정 없음)

| 항목 | 적용 전 | 적용 후 | 결과 |
|---|---:|---:|---|
| `vocabulary_contents`(active) | 5,950 | 5,950 | 불변 |
| `vocabulary_multiformat_items` | 1,553 | 1,553 | 불변 |
| `vocab_level` 값이 바뀐 content_id 수 | - | **0** | 5,950건 전수 대조, 0건 불변 |
| `SR_%`(L4~L6 227건) `REVIEW_BOUNDARY`·`boundary_flag=1` | 227/227 | 227/227 | 불변 |
| 공개 플래그(`student_exposure=1` 또는 `public_ready=1`) | 0 | 0 | 불변 |
| 파일럿 매니페스트 3종 SHA-256 (`pilot_l4l5`/`pilot_l6`/`existing_l0l3`) | - | 로컬 사본과 완전 일치 | 서버 파일 전혀 안 건드림 |

## 5. tier1 빠른 검수 파일(31건만)

`data/import/nikl_tier1_quick_review_20260929.csv` — 컬럼:
`content_id, lemma, pos, 공식_등급, 현재_레벨, 짧은_용례(≤60자), 차이_사유, 판정`.
`판정` 컬럼은 **전부 공란**(사람이 직접 입력) - 허용값 4개(기본 레벨 조정 /
현재 유지 / 뜻 확인 / 보류)는 파일 마지막 `#README` 행에 안내만 남기고
실제 값은 채우지 않았다. tier2(142건)·tier3(650건)은 이번 파일에 포함
하지 않았다(사용자 지시대로 일괄 검수 대상에서 제외).

## 6. 배포 상태 확인(Job 6)

| 확인 | 결과 |
|---|---|
| 로컬 `main` HEAD | `9ca9cc2`(이번 작업 직전 커밋 - 재검증 보고서, 앱 코드 변경 없음) |
| 서버 HEAD | `2411d82`(momo_b2b_tablet 관련, 이번 세션 vocab 작업과 무관) |
| `a8b0584`(L0~L3 관리자 미리보기 기능)가 서버 HEAD의 조상인가 | **예 - 이미 반영됨**(`git merge-base --is-ancestor` 확인) |
| `multiformat.py`에 `existing_l0l3_preview_mode`/`EXISTING_L0L3_SOURCE_VERSION` 실제 존재 여부(서버 파일 직접 grep) | **13건 매치 - 코드가 실제로 서버에 있음** |
| 서버 HEAD가 로컬 `main`의 조상인가(분기 여부) | 예 - 정상적으로 뒤처져 있을 뿐, 분기 없음 |
| 로컬에만 있고 서버엔 없는 커밋 | `9ca9cc2` 단 1개(문서/CSV만, 앱 코드 없음) |

**결론**: 이전 세션에서 "샌드박스 정책으로 서버 `git pull`이 차단됐다"고
보고했던 L0~L3 관리자 미리보기 기능은 **그 사이 사용자가 직접 서버에
반영을 완료한 것으로 확인된다** - 현재 라이브다. 이번 DB 적재 작업은
FastAPI 앱 코드를 전혀 거치지 않는 독립 Python 스크립트(SSH로 서버에서
직접 실행)라서, 서버 코드 배포 상태와 무관하게 안전하게 진행할 수
있었다 - 실제로 배포 상태와 무관하게 아무 문제 없이 완료됐다.

## 부록 — 되돌린 실수 1건(투명성 기록)

첫 마이그레이션 실행 시 `VOCABULARY_QUIZ_DB_PATH`를 명시하지 않아
`app/vocabulary_quiz/db.py`의 기본값(`data/vocab/vocabulary_quiz_rnd.db`,
연구 DB와 무관한 별도 로컬 R&D DB)에 빈 테이블이 생성되고 백업 파일
하나가 만들어졌다. 데이터는 전혀 쓰이지 않은 단계(마이그레이션만
실행, 적재는 안 함)였으므로 영향은 "무관한 DB에 빈 테이블+백업 파일"
뿐이었다 - `DROP TABLE`로 테이블을 제거하고 백업 파일을 삭제해 원래
상태로 완전히 복구한 뒤, 올바른 경로로 재실행했다. 연구 DB에는 이
실수의 흔적이 전혀 없다.

## 결론

- 신규 테이블 `vocabulary_official_grade_reference`가 연구 DB에
  5,950건 전부(다중후보/매칭없음/표제어만일치 121건은 등급 NULL 유지)
  안전하게 적재됐다 - SQLite Backup API 백업·복원성 검증·입력 해시
  확인·단일 트랜잭션·충돌 시 중단 규칙 전부 통과, 재실행 멱등성도
  확인됨.
- 기존 `vocab_level`·문항·파일럿 매니페스트·공개 플래그는 전수 재쿼리로
  0건 변경 확인.
- tier1 31건 빠른 검수 파일을 만들었고 판정 칸은 전부 공란 -
  tier2/tier3는 이번에 올리지 않았다.
- L0~L3 관리자 미리보기는 이미 서버에 라이브로 반영돼 있었고, 이번
  DB 작업과는 독립적이라 아무 영향도 주고받지 않았다.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
