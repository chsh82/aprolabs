# momolib 어휘 퀴즈 관리자 전용 이식 — 운영 배포 완료

- 일자: 2026-09-28
- **운영 배포 완료**: origin/main = `f453e90` (아바타 커밋 `91de794`는
  포함되지 않음 - 로컬 main과 분리 유지), aprolabs: 이 보고서
- **학생 공개 없음**(전체 227건 `student_exposure=0`/`public_ready=0`,
  학생용 라우트 미연결 - 4절에서 재확인)

## 1. 서버 전용 미커밋 마이그레이션 3개 원본 확인

| 파일 | revision | down_revision | SHA-256 |
|---|---|---|---|
| `e04b5513c0ba_lms_tables.py` | `e04b5513c0ba` | `c3d4e5f6a7b8` | `dd6e4133...5ed5` |
| `d6cf3cef8abd_add_lms_assignment_and_progress_tables.py` | `d6cf3cef8abd` | `e04b5513c0ba` | `4b0514d3...578e` |
| `63723761586a_mileage_and_avatar.py` | `63723761586a`(=운영 실제 head) | `d6cf3cef8abd` | `2de156fa...c2b5` |

내용 검토: 전부 순수 추가형(신규 테이블 4개 + 기존 테이블에 nullable
컬럼 추가) - 파괴적 연산 없음. `curricula`/`packages`/`avatar_items`/
`mileage_logs` 등 이미 존재하는 모델(`app/models/lms.py`,
`app/models/avatar.py`)과 정확히 대응됨을 확인했다.

## 2. Git 이력 복구 (momolib `2b0a0e4`)

3개 파일을 **원본 그대로**(수정 없음, 커밋된 blob의 SHA-256이 서버
원본과 정확히 일치함을 확인) 저장소에 추가하고, 신규 어휘 퀴즈
마이그레이션(`37b334ba3fc8`)의 `down_revision`을 잘못된
`c3d4e5f6a7b8`에서 실제 운영 head `63723761586a`로 수정했다. `flask db
stamp`나 강제 리비전 변경을 전혀 쓰지 않았다 - 정상적인 리비전 체인
연결로만 해결했다. `flask db heads` = 단일 head(`37b334ba3fc8`),
`flask db history` = 초기 모델부터 12단계 완전 선형 체인을 확인했다.

**참고(부작용 없는 발견)**: 완전히 빈 로컬 DB에서 `flask db upgrade`를
처음부터 실행하면 최초 마이그레이션(`4697b50deed2_초기_모델`) 자체의
순환 FK 순서 문제(`branches.owner_id → users`, `users` 테이블이 아직
없음)로 실패한다 - 이건 이 저장소의 원래 초기 마이그레이션에 있던
기존 버그이며, 운영 DB도 이 방식으로 부트스트랩된 적이 없다(운영은
이미 존재하는 DB에 **증분**(63723761586a→37b334ba3fc8) upgrade만
받았고, 이는 완벽하게 성공했다 - 3절 참고).

## 3. 운영 백업의 격리 PostgreSQL 실제 복원·검증

- 운영에서 `sudo -u postgres pg_dump -d momolib -Fc`로 실제 백업 생성
  (`~/momolib_backup_pre_vocabquiz_20260927-161957.dump`, 로컬로 전송해
  SHA-256 일치 확인: `a94e0106...96ec2`).
- **로컬 독립 PostgreSQL(운영과 완전히 분리, portable 16.4)에 이 백업을
  실제로 `pg_restore`** - 47개 테이블, `bank_questions=411`,
  `users=6` 등 **실제 운영 데이터**로 복원됨을 확인(`alembic_version`
  테이블 값도 `63723761586a`로 운영과 정확히 일치).
- 이 복원본에서: 마이그레이션 적용(신규 테이블 5개, 기존 47개 테이블·
  데이터 **완전 불변** - diff 0) → import dry-run/apply(227/227/40+40) →
  관리자 응시·채점 통합테스트 22/22 통과(`bank_questions` 411 불변 재확인)
  → **재실행 시 신규 0건**(멱등성) → **동일 자연키에 내용을 바꾸면
  즉시 FAIL(exit 2), 덮어쓰지 않음**을 실제 운영 데이터 기반으로
  재확인했다.

## 4. 운영 실제 적용

### 4-1. 재확인(값 미노출)
- `systemctl cat momolib`: `Environment="FLASK_ENV=production"` 확인.
- 백업 파일 존재·크기 불변 확인, 서버 코드 여전히 구 버전(`e04834a`)
  확인, 대상 DB `momolib`(PostgreSQL, `SELECT current_database()`로 확인).

### 4-2. 구 코드 상태에서 마이그레이션 파일 전달 + 수동 적용
- 수정된 `37b334ba3fc8` 파일을 scp, SHA-256 일치 확인
  (`0c59085c...302f`).
- **구 코드가 여전히 실행 중인 상태**에서 `flask db upgrade` 실행:
  `63723761586a -> 37b334ba3fc8` 성공.
- 적용 직후: 신규 테이블 5개 존재 확인, **기존 47개 테이블의 전체 행수
  스냅샷이 적용 전/후 완전히 동일**(diff 0)함을 재확인, 앱 재시작
  없이도 정상 서비스 중이었음(`systemctl status active`) 확인.

### 4-3. 미추적 파일 보존 이동 (git pull 충돌 방지)
서버의 미추적 마이그레이션 파일 4개(3개 발굴분 + 내 파일)를 **삭제하지
않고** `~/momolib_untracked_migrations_preserved_20260927-163319/`로
이동, 이동 전 SHA-256을 전부 기록해 두었다(`git clean`/`reset --hard`
사용 안 함).

### 4-4. main 반영 + push + 자동배포
로컬 main에는 여전히 미푸시 아바타 커밋(`91de794`)이 남아 있어, 이를
건드리지 않기 위해 **`git push origin feat/vocab-quiz-admin-migration:main`**
으로 격리 브랜치를 origin/main에 직접 fast-forward push했다(로컬 main
브랜치 자체는 전혀 조작하지 않음 - 아바타 커밋과 절대 섞이지 않음).
GitHub Actions 자동배포가 `git pull`+재시작을 정상 수행함을 확인
(서버 코드 `f453e90`로 갱신, 서비스 재시작 시각 확인).

**발견한 사소한 문제(즉시 수정·재배포)**: `import_vocab_quiz_export.py`가
`create_app('development')`로 하드코딩돼 있던 것을 발견해
`FLASK_ENV`(기본 production)를 따르도록 수정하고 다시 push했다
(`f453e90`).

**발견한 무해한 차이**: git pull로 배포된 `37b334ba3fc8` 파일의
SHA-256(`cb32982a...`)이 scp본(`0c59085c...`)과 다른데, 원인은 git의
CRLF→LF 줄바꿈 정규화뿐이었다(`cat -A`로 실제 내용 확인, 코드 로직은
완전히 동일) - 이미 실행된 마이그레이션 결과에는 영향 없음.

### 4-5. 관리자 전용 import 실행 + 라이브 검증
```
python import_vocab_quiz_export.py            # dry-run: 227/227/40/40 신규
python import_vocab_quiz_export.py --apply     # 실제 적용
```
운영 DB에서 직접 SQL로 확인: `contents=227, levels=227, l4l5=40, l6=40,
exposed(=student_exposure OR public_ready)=0`.

**라이브 HTTP 검증**(임시 테스트 계정 2개 생성 후 실제 사용, 종료 후
전부 삭제):
| 검증 | 결과 |
|---|---|
| 비로그인 `GET /vocab-quiz/` | 302 |
| teacher(비관리자) `GET /vocab-quiz/` | **403** |
| super_admin `GET /vocab-quiz/` | 200 |
| 파일럿 세션 시작 | 200, `question_count=40` |
| 응시 화면 응답 본문 | `answer_payload_json`/`correct_option` **0건** 검출 |
| 정답 제출(POST) | `is_correct:true`, 그 문항만 정답 공개 |
| 학생용 코드 경로(`app/learn`,`app/library`,`app/lms` 등) | `from app.vocab_quiz`/`VocabQuizContent`/`VocabQuizPilot*` 참조 **0건**(기존 `content_type=='vocab_quiz'` 문자열 비교는 완전히 별개의 기존 BankQuestion 기능) |
| 기존 `bank_questions`(411건) 등 46개 테이블 | 테스트 계정 생성으로 인한 `users`(+2)만 차이, **나머지 전부 완전 불변** |

### 4-6. 정리
테스트 계정 2개, 그 계정이 만든 파일럿 세션·응답을 전부 삭제 →
`users` 6으로 복귀, `vocab_quiz_pilot_sessions`/`_attempts` 0건 확인.

## 5. 최종 상태
| 항목 | 값 |
|---|---|
| 운영 DB 신규 테이블 | 5개(비어있지 않음, 227/227/40/40) |
| 학생 노출 | 0건(전부 `student_exposure=0`, `public_ready=0`) |
| 기존 테이블 47개 | 완전 불변(테스트 계정 정리 후 원래 값과 100% 일치) |
| main 병합·push | 완료(`f453e90`), 아바타 커밋과 분리됨 |
| 자동배포 | 2회 성공 확인 |
| 백업 | `~/momolib_backup_pre_vocabquiz_20260927-161957.dump`(서버), 격리 복원본에서 유효성 검증 완료 |

## 6. 하지 않은 것 / 남겨둔 것
- 학생 공개(어떤 라우트도 학생에게 노출하지 않음) - 없음
- 로컬 main 브랜치의 아바타 커밋 처리 - 건드리지 않음(별도 결정 필요)
- 서버의 미추적 파일 보존 디렉터리(`~/momolib_untracked_migrations_preserved_*/`) -
  삭제하지 않고 그대로 둠(감사 기록용)
