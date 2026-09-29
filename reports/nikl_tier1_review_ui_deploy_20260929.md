# 국립국어원 등급 검수 다음 단계 — 가드 강화·tier1 검수 화면·배포

- 일자: 2026-09-29
- 범위: (1) 기존 참조 테이블 스크립트의 DB 경로 가드 강화, (3)(4) tier1
  31건 온라인 검수 화면 신규 구축, (5) 로컬 테스트 → 누적 diff 확인 →
  aprolabs 연구 사이트에만 배포. **job 2(L0~L3 관리자 미리보기 라이브
  재검증)는 이 작업 범위 밖 — 오케스트레이터가 직접 수행.**
- vocab_level·문항·매니페스트·student_exposure·public_ready는 이번
  작업 전 구간에서 전혀 변경하지 않았다. momolib은 전혀 건드리지
  않았다(어떤 파일도, 어떤 명령도 실행 안 함).

## 1. DB 경로 가드 강화

신규 `scripts/vocab/db_path_guard.py` — `sqlite3.connect()` 호출 전에
4가지를 확인하고 하나라도 걸리면 즉시 실패:
1. `APP_ENV != 'research'`
2. `VOCABULARY_QUIZ_DB_PATH` 미설정
3. `--database` 인자가 주어졌는데 정규화된 절대경로가
   `VOCABULARY_QUIZ_DB_PATH`와 다름
4. 대상 파일이 실제로 존재하지 않음(`Path.is_file()`)

통과 후 실제 쓰기 연결은 `connect_rw()`로 `mode=rw`(not `rwc`)로 열어,
가드를 어떻게든 우회해도 SQLite 자체가 없는 파일을 암묵적으로 만들지
못하게 하는 2차 방어선을 뒀다.

`migrate_add_official_grade_reference.py`/`apply_official_grade_reference.py`
둘 다 이 가드로 교체했다(이전에는 `apply`가 `--apply`일 때만
APP_ENV를 확인했고, `migrate`는 아무 가드도 없었다 - 이게 실제
사고 원인이었다).

### 격리 테스트 결과 (`scripts/vocab/test_official_grade_reference_guards.py`, 스크래치 경로만 사용, 실 DB 전혀 안 건드림)

| 시나리오 | 결과 |
|---|---|
| (a) VOCABULARY_QUIZ_DB_PATH 미설정 | PASS - GUARD FAIL(2/4)로 즉시 중단 |
| (b) --database가 env var와 다름 | PASS - GUARD FAIL(3/4)로 즉시 중단 |
| (c) 존재하지 않는 파일 | PASS - GUARD FAIL(4/4)로 즉시 중단 |
| (d) 정상 경로(전부 일치·존재·APP_ENV=research) | PASS - 가드 통과 |
| (e, 보너스) APP_ENV != research | PASS - GUARD FAIL(1/4)로 즉시 중단 |

전부 PASS. 실행 로그는 스크립트를 직접 재실행하면 재현된다.

## 2. tier1(31건) 검수 화면 — `/vocab-official-grade-review`

기존 `/vocab-publish-review`(`models_publish_review.py`/`publish_review.py`/
`routers/publish_review.py`/`publish_review_{index,detail}.html`) 패턴을
그대로 재사용해 완전히 별도 경로로 신설 - 기존 파일은 한 글자도 안 건드림.

- `app/vocabulary_quiz/models_official_grade_review.py`: 읽기 전용
  `VocabularyOfficialGradeReference` 매핑(이미 적재된 5,950건 참조
  테이블) + append-only `VocabularyOfficialGradeJudgment`
  (`vocabulary_official_grade_judgments`, CHECK 제약으로 4개 판정값만
  허용, FK 대신 스냅샷 방식 재사용 - `vocabulary_publish_reviews`와
  동일 이유).
- `app/vocabulary_quiz/official_grade_review.py`: 서비스 계층
  (`ordered_tier1_content_ids`, `diff_reason`, `short_example`,
  `save_judgment`, `judgment_is_stale`, `next_content_id`). tier1
  정의는 `priority_tier==1`을 참조 테이블에서 그대로 읽음(재계산 안 함).
- `app/vocabulary_quiz/routers/official_grade_review.py`: `require_admin`
  그대로 재사용, `GET /`(목록), `GET /{content_id}`(상세+판정 폼),
  `POST /{content_id}/judgment`(저장 후 다음 항목 자동 이동, 마지막이면
  목록으로) - `/vocab-publish-review`와 동일한 UX.
- 템플릿 2종(`official_grade_review_index.html`/`_detail.html`) - 기존
  Tailwind 톤 재사용.
- `app/main.py`에 라우터 등록(2줄 추가, 기존 등록부 옆에).
- `scripts/vocab/migrate_add_official_grade_judgments.py`: 새 가드
  패턴 적용, `CREATE TABLE IF NOT EXISTS`, 백업+복원성 검증, 적재
  직후 0행 하드 확인(자동 채움 없음을 스크립트 자신이 검증).

### 신선도(판정 만료) 설계

판정 저장 시 참조 테이블 행의 `official_grade`+`proposed_base_level`+
`match_type`+`source_file_sha256`+`computed_at`을 해시해
`source_data_version_at_review`로 스냅샷한다. 이후 참조 테이블 행이
재계산되면(예: 새 공식 자료 버전 반영) 해시가 달라져 화면에 "⚠ 판정
만료" 배지가 뜬다 - `judgment_is_stale()`이 저장 당시 스냅샷과 현재
값을 비교해 판정한다.

## 3. 로컬 종단 검증 (스크래치 DB 사본, 실 DB 아님)

라이브 연구 DB를 로컬 스크래치 경로로 복사(16MB) → 새 가드로
`migrate_add_official_grade_judgments.py` 실행(정상 통과, 0행 확인) →
`./venv/Scripts/python.exe`로 실제 FastAPI 앱(`app.main:app`) 구동:

- **비로그인 3종 요청**(GET 목록/GET 상세/POST 판정) 전부 `/login`으로
  302 리디렉션 - 500 없음.
- **서비스 계층 직접 호출**: tier1 31건 정확히 조회, 첫 항목에
  `save_judgment()` 실행 → 실제로 1행 INSERT됨(reviewer_email,
  reviewed_at, 해시 스냅샷 전부 채워짐) → 저장 직후 `judgment_is_stale`
  False, 참조 데이터를 스크래치 DB에서만 임의로 바꿔보니 True로 전환
  확인 → `next_content_id`가 정확한 다음 항목 반환 → 판정 테이블 전체
  행수 정확히 1(자동 채움 없음 재확인).
- **`require_admin` 의존성 오버라이드로 관리자 HTTP 경로도 시도** -
  다만 이 앱은 URL 단위 전역 `auth_middleware`가 라우트 의존성 주입보다
  먼저 세션 쿠키를 직접 확인하는 구조라 `dependency_overrides`만으로는
  우회되지 않음을 확인(302 유지) - 실제 로그인 세션 쿠키가 있어야
  통과하는데, 로컬 테스트에서 진짜 로그인을 하려면 앱의 메인 사용자
  DB(`./aprolabs.db`, 상대경로 하드코딩)를 거쳐야 해서 **사용자의 실제
  로컬 개발 DB를 건드릴 위험이 있어 일부러 여기서 멈췄다** - 완전한
  로그인 HTTP 클릭스루는 오케스트레이터가 라이브에서 수행하기로 한
  역할 분담과도 일치한다.

스크래치 DB 사본은 검증 후 즉시 삭제했다.

## 4. 배포 전 누적 diff 확인

- `git rev-list --left-right --count origin/main...HEAD` → 로컬이
  origin 대비 2커밋 앞(9ca9cc2, dfc0cd0 - 전부 이전 턴에서 이미 로컬
  커밋된 문서/DB 작업, 앱 코드 없음). origin/main과 서버 HEAD는
  분기 없이 일치(서버 HEAD `2411d82`가 로컬 main의 조상).
- `git status --porcelain`으로 이번 작업이 실제로 건드린 파일만
  정확히 골라 `git add`(11개 - app/main.py, 가드 강화 2개 스크립트,
  신규 앱 파일 5개, 신규 스크립트 3개) - momo_worksheet_page_editor.py,
  momo_book_db/*, 문해력 스크립트, phase15/16 csv/jsonl 등 무관한
  미커밋 변경은 전혀 stage하지 않음(확인 완료).
- `app/main.py`의 diff 자체도 2줄 추가뿐임을 재확인(무관한 훅 없음).

## 5. 배포

1. 로컬 커밋 `abfcff7`(11개 파일), 이전 미푸시 2건 포함 총 3커밋
   `git push origin main` 성공(fast-forward, `2411d82..abfcff7`).
2. 서버(`ssh aprolabs`, `~/aprolabs`) `git pull origin main` **성공**
   (이전 세션에서 있었던 샌드박스 차단은 이번엔 발생하지 않음 - 서버
   HEAD가 `abfcff7`로 정확히 갱신됨을 `git rev-parse HEAD`로 재확인).
3. 새 마이그레이션을 라이브 DB에 실행: 가드 통과 → 백업
   (`vocabulary_quiz_research.db.bak_migrate_official_grade_judgments_20260929-095219`)
   → 무결성 확인 ok → 테이블 생성 → **0행 확인**.
4. `sudo systemctl restart aprolabs.service` 성공(새 PID로 기동,
   `ActiveEnterTimestamp` 갱신 확인).
5. `curl -I` 결과: 로컬(127.0.0.1:8000)·공개
   (`https://aprolabs.co.kr/vocab-official-grade-review/`) 전부
   **HTTP 302**(로그인 리디렉션) - 404/500 없음, 라우트가 정상
   등록돼 실행됨을 확인. **완전한 인증 후 클릭스루 검증은 이 결과에서
   단정하지 않음 - 오케스트레이터가 다음에 직접 수행.**

## 6. 배포 후 불변 조건 (라이브 DB, 재쿼리)

| 항목 | 결과 | 기대 |
|---|---:|---:|
| `vocabulary_official_grade_reference` | 5,950 | 5,950(불변) |
| `vocabulary_contents`(active) | 5,950 | 5,950 |
| `vocabulary_multiformat_items` | 1,553 | 1,553 |
| 공개 플래그(student_exposure=1 또는 public_ready=1) | 0 | 0 |
| `vocabulary_official_grade_judgments`(신규) | 0 | 0(자동 채움 없음) |
| SR_(227건) `REVIEW_BOUNDARY`·`boundary_flag=1` | 227 | 227 |

전부 일치. momolib은 이 작업 전체에서 어떤 파일도 열람·수정하지 않았고
어떤 명령도 momolib을 대상으로 실행하지 않았다.

## 7. 오케스트레이터에게 인계

- **관리자 검수 URL**: `https://aprolabs.co.kr/vocab-official-grade-review/`
  (목록), 개별 항목은 `/vocab-official-grade-review/{content_id}`.
- 실제 로그인 세션으로 목록 렌더링·31건 표시·표제어/품사/공식등급/
  현재레벨/짧은용례/차이사유 노출·판정 저장→다음 항목 자동 이동·
  판정 이력·신선도 배지까지 클릭스루로 확인 필요(이번 로컬 테스트가
  검증 못 한 유일한 구간).
- job 2(L0~L3 관리자 미리보기 라이브 재검증)도 함께 수행 예정이면
  같은 관리자 세션으로 `/vocabulary-quiz/multiformat` 경로에서 진행
  가능.

## 커밋

`abfcff7` — "국립국어원 등급 참조 스크립트 DB 경로 가드 강화 + tier1(31건)
검수 화면 추가"(11 files changed, 800 insertions, 21 deletions). 서버·
origin/main 전부 동일 커밋으로 동기화됨.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
