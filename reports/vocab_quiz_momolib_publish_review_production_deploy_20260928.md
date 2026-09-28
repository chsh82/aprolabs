# momolib 학생용 어휘 퀴즈 관리자 공개검토 — 운영 배포 완료

- 일자: 2026-09-28
- **운영 배포 완료**: `origin/main` = `81b1945`(로컬 아바타 커밋 `91de794`는
  포함되지 않음 - 로컬 main과 분리 유지)
- **학생 기능 활성화 없음**(`VOCAB_QUIZ_STUDENT_ENABLED` 운영에 설정
  안 함, 기본 OFF), **콘텐츠 공개 전환 없음**(227건 전부 여전히
  `student_exposure=0`/`public_ready=0`/`REVIEW_BOUNDARY`), **승격 실행
  경로 자체가 코드에 없음**(아래 5절)

## 1. 배포 전 확인

| 확인 항목 | 결과 |
|---|---|
| `feat/vocab-quiz-student-design` vs `origin/main` diff | 정확히 2커밋(`4f033a5`, `81b1945`), 24개 파일 **전부 추가만**(삭제 0줄), 다른 작업 커밋 섞임 없음 |
| 신규 마이그레이션 `down_revision` 체인 | `0f604cbf8f2c`(down=`37b334ba3fc8`) → `4d83374ccc5f`(down=`0f604cbf8f2c`), 단일 선형 |
| 운영 실제 Alembic head | `37b334ba3fc8`(재확인, 이번 마이그레이션 체인과 정확히 일치) |
| 신규 테이블 없이 새 코드 기동 | 격리 DB에서 실제 재현: 기존 라우트(관리자 대시보드·기존 vocab-quiz 인덱스·콘텐츠 목록·HQ 대시보드) 전부 200 정상, 신규 공개검토 라우트만 500(테이블 없음)으로 **국소적으로만** 실패 - 전체 앱이 죽지 않음을 확인 |

## 2. 운영 백업 → 격리 DB 복원 검증

- 운영에서 실제 `pg_dump -Fc` 생성:
  `~/momolib_backup_pre_publish_review_20260928-010942.dump`
  (서버 보존, SHA-256 `a60b4d4d...25bdba`)
- 로컬 전송 후 SHA-256 일치 확인, 완전히 독립된 로컬 PostgreSQL(운영과
  무관)에 **실제 복원** - `bank_questions=411`, `users=6`,
  `vocab_quiz_contents=227`, `vocab_quiz_pilot_items=80`,
  `alembic_head=37b334ba3fc8` 전부 운영과 일치함을 확인(실제 운영
  데이터로 복원됐다는 뜻)
- 이 복원본에 신규 마이그레이션 2개 적용: 테이블 52→55(+3, 정확히
  신규 3개), 기존 데이터 전부 불변
- **이 복원본(실제 운영 데이터)을 대상으로** 검증(12/12 PASS):
  - 1순위(파일럿 연결) 콘텐츠가 실제로 **40건**임을 재확인
  - 실제 콘텐츠(`SR_L4CORE_4812` 등) 하나에 판정 저장 → 공개 플래그·
    `REVIEW_BOUNDARY` 불변
  - 그 콘텐츠의 `content_hash`를 바꿔 판정이 stale로 표시됨을 확인 후
    원복
  - 승격 dry-run 로직을 실제 40건 전체에 대해 예외 없이 계산(판정 없는
    상태에서 승격 대상 0건)
  - 검증용 판정·계정 정리 후 `vocab_quiz_contents=227`,
    `vocab_quiz_pilot_items=80`, `bank_questions=411` 최종 불변 재확인

## 3. 운영 실제 적용

### 3-1. feature flag 확인(값 노출 없음)
`systemctl cat momolib`와 `~/momolib/.env`에서
`VOCAB_QUIZ_STUDENT_ENABLED=` 라인 자체가 **0건**(설정 안 됨) - `grep -c`로
존재 여부만 확인했고 값은 어디에도 조회·출력하지 않았다.

### 3-2. push 자동배포 사전 점검
- GitHub Actions 워크플로(`git pull origin main` → `pip install` →
  `systemctl restart`, 마이그레이션 단계 없음)를 재확인.
- 서버 `git status`: `app/static/img/characters/*`, `manifest.json`,
  `sw.js`가 "M"으로 표시됨 - 원인은 배포 스크립트의
  `chmod -R 755 ~/momolib/app/static` 단계가 매 배포마다 실행 파일
  비트를 바꿔 git이 mode 변경으로 인식하는 것으로, **이번 작업과
  무관한 기존 배포 스크립트의 알려진 부작용**(파일 내용 변경 아님).
  이번 커밋은 이 경로들을 전혀 건드리지 않으므로 `git pull` 충돌
  가능성 없음(실제로 무충돌 확인, 아래).
- 서버에 `password_backup_20260927211608.json`,
  `reset_all_passwords.py`, `reset_all_passwords.b64`라는 미추적
  파일이 있음을 관찰했다 - **내용을 읽지 않았고 건드리지 않았다**.
  이번 작업과 무관한, 서버에서 별도로 수행된 것으로 보이는 흔적이며,
  경로 이름이 이번에 배포한 파일들과 겹치지 않아 `git pull`에
  영향을 주지 않는다.

### 3-3. 구 코드 상태에서 마이그레이션 수동 적용
- 수정 파일 없이 그대로 scp, SHA-256 일치 확인.
- **구 코드(`97a13c1`)가 계속 실행 중인 상태**에서 `flask db upgrade`
  실행: 테이블 52→55(+3), 기존 `bank_questions=411`/
  `vocab_quiz_contents=227`/`vocab_quiz_pilot_items=80` 완전 불변,
  서비스 재시작 없이도 정상 서비스 중이었음을 확인.
- 미추적 마이그레이션 파일 2개를 `~/momolib_untracked_migrations_
  preserved_20260928-011500/`로 이동(삭제 아님) 후 push - `git pull`
  무충돌 확인, git으로 들어온 파일과 이동해 둔 원본의 내용이
  `cat -A` 정규화 해시까지 완전히 동일함을 재확인.

### 3-4. main 반영 + push + 자동배포
`git push origin feat/vocab-quiz-student-design:main`으로 격리
브랜치를 직접 fast-forward push(로컬 main의 미푸시 아바타 커밋과
분리 유지). 자동배포 성공(`97a13c1` → `81b1945`, 서비스 01:16:42
재시작), `alembic_version=4d83374ccc5f`, 기존 데이터 전부 불변
재확인.

## 4. 배포 후 라이브 검증(실제 HTTP + 브라우저)

| 대상 | 결과 |
|---|---|
| 비로그인 `GET /vocab-quiz/publish-review/` | 302 |
| teacher/parent/student `GET /vocab-quiz/publish-review/` | 403(전부) |
| super_admin `GET /vocab-quiz/publish-review/` | 200 |
| 임의 역할 `GET /practice/vocab-quiz/`(학생 기능) | **전부 404**(관리자 포함) |
| 목록 화면 | 실제 227건 중 **1순위 40건**이 그대로 표시됨 |
| 상세 화면(`SR_L4CORE_4812` '집단') | 원천/학생용 뜻풀이·예문·레벨 근거(`REVIEW_BOUNDARY`/`boundary_flag=True` 강조)·연결 문항 2건(정답 하이라이트)·온라인 QA 참고자료 전부 정상 렌더. 이전에 수정한 조사 오류 문항(`MF_A_SC_SRL4L5PILOT_20260925_L4_003`)이 "무리'라는"으로 정확히 남아있고 "무리'이라는"은 어디에도 없음을 재확인 |
| 판정 저장(브라우저로 실제 클릭) | "현재 판정" 즉시 반영 확인 |
| 승격 dry-run | "1순위 40건 중 1건 충족" + `student_exposure/public_ready/level_status/boundary_flag` 변경 예정 필드 정확히 계산. 조회 후 실제 DB 값 재확인 결과 **전혀 변경되지 않음** |
| 정리 | 남긴 판정 1건 삭제(`vocab_quiz_admin_reviews=0`), QA 계정 4개 삭제(`users` 10→6 원복) |
| 최종 재확인 | `vocab_quiz_contents=227`, `vocab_quiz_pilot_items=80`, `student_exposure 또는 public_ready=True`인 콘텐츠 0건, `bank_questions=411`, `users=6` |

## 5. 승격 실행 경로가 없음을 코드·HTTP 양쪽에서 확인

- 코드: `app/vocab_quiz/publish_review_routes.py`에 정의된 라우트는
  `publish_review_index`(GET), `publish_review_detail`(GET),
  `publish_review_save_verdict`(POST, 판정 저장 전용),
  `promote_dry_run`(GET 전용) **4개뿐**. "실행"/"승격 적용"에 해당하는
  라우트나 함수 자체가 존재하지 않는다.
- HTTP로 직접 확인: `POST /vocab-quiz/publish-review/promote-dry-run`
  → **405**(Method Not Allowed - 인증 여부와 무관하게 라우팅 단계에서
  거부됨). `/promote`, `/promote-dry-run/execute`,
  `/<content_id>/promote`, `/vocab-quiz/promote` 등 그럴듯한 실행
  경로도 전부 404(존재하지 않음을 재확인, `/publish-review/promote`의
  405는 `<content_id>` 동적 라우트가 "promote"를 콘텐츠 ID로 오인
  매칭한 것일 뿐 실행 기능이 아님을 GET 302 응답으로 재확인).

## 6. 백업/복구

- 정식 백업: `~/momolib_backup_pre_publish_review_20260928-010942.dump`
  (서버 보존, SHA-256 `a60b4d4d...25bdba`, 격리 DB 복원으로 유효성
  검증 완료).
- 복구 절차: 이 파일을 `pg_restore`로 복원하면 이번 마이그레이션
  적용 이전(테이블 52개, 227콘텐츠/80문항/411 bank_questions) 상태로
  정확히 되돌아간다. 이번 마이그레이션은 순수 추가형(신규 테이블
  3개)이라 다운그레이드(`flask db downgrade 37b334ba3fc8`)만으로도
  기존 데이터 손실 없이 되돌릴 수 있다(신규 테이블만 제거).
- 서버 보존 디렉터리 `~/momolib_untracked_migrations_
  preserved_20260928-011500/`는 감사 기록용으로 그대로 둔다(이전 단계와
  동일 관례).

## 7. 관리자 검토 화면 URL

| 화면 | URL |
|---|---|
| 공개검토 목록(1순위 40건 개별 + 2/3순위 대기열) | `https://momolib.com/vocab-quiz/publish-review/` |
| 공개검토 상세(판정 입력 포함) | `https://momolib.com/vocab-quiz/publish-review/<content_id>` |
| 승격 dry-run(읽기 전용) | `https://momolib.com/vocab-quiz/publish-review/promote-dry-run` |

전부 `super_admin`/`hq_manager`만 접근 가능(비로그인 302, 비관리자
403, 학생 기능은 별도로 항상 404).

## 8. 하지 않은 것

- `VOCAB_QUIZ_STUDENT_ENABLED` 활성화 - 하지 않음(운영 미설정 유지)
- 227건 중 어떤 콘텐츠도 `student_exposure`/`public_ready`/
  `level_status`/`boundary_flag` 전환 - 하지 않음
- 승격 실행 기능 구현 - 이번 범위에서 아예 만들지 않음(5절)
- 로컬 main 브랜치의 아바타 커밋 처리 - 이번에도 건드리지 않음
