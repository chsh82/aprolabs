# 어휘 퀴즈 R&D 화면 aprolabs 일원화 — 완료 보고

- 일자: 2026-09-28
- **momolib**: `/vocab-quiz/*` R&D 화면(관리자 파일럿 응시·공개검토·
  승격 dry-run) 전체 폐쇄 완료
- **aprolabs**: `/vocab-publish-review/*` 공개검토 화면 신규 구현·배포 완료
- 양쪽 모두 227/80(momolib), 40/80(aprolabs 1순위) 데이터와 공개
  플래그를 전혀 건드리지 않았다.

## ⚠ 배포 방식에 대해 발견·처리한 위험 (결과: 해결됨)

작업 시작 시점에 로컬 aprolabs 저장소의 `main` 브랜치가 서버(운영)보다
20개 커밋 앞서 있었는데, 그중 **가장 최근 커밋(`f1bc3d1`, "검수 목록
화면 + 문서 간 이동 + 페이지별 이미지 생성")이 이번 세션이 아니라
다른(동시 진행 중인) 세션이 만든 것**이었다(Co-Authored-By는 같지만
이 대화에서 작성·검증한 적이 없는 변경). aprolabs의 배포 스크립트
(`.github/workflows/deploy.yml`)는 `git fetch` + **`git reset --hard
origin/main`**을 쓰기 때문에, 로컬 `main`을 그대로 push하면 그 다른
세션의 미검증 변경까지 통째로 운영에 강제 반영될 위험이 있었다.

**처리 방법**: 먼저 파일 직접 전송(scp)+서버 직접 마이그레이션+수동
재시작으로 안전하게 배포한 뒤(위 8절 절차), `origin/main`(당시
`49a0f13`, 서버 실제 HEAD와 동일)에서 새 브랜치를 만들어 **이번 기능
커밋 하나만 cherry-pick**해 그 브랜치를 `origin/main`으로 push했다
(`49a0f13..736f371`, 다른 세션의 `f1bc3d1`은 어디에도 포함되지 않음).
이 push가 GitHub Actions 자동배포를 정상 트리거해 서버가 `git reset
--hard`로 정확히 736f371로 갱신됐고(이미 scp로 올려둔 파일과 내용이
같아 실질적 변경 없음, 서비스 자동 재시작), 서버의 미추적 파일들
(`.env.bak_*`, `nonsul_kb/*` 등)은 `reset --hard`가 건드리지 않는
untracked라 그대로 보존됐다. **결과: 서버의 git 추적 상태와 실제 실행
코드가 다시 일치하고, 다른 세션의 미검증 커밋은 배포되지 않았다.**

로컬 `main` 브랜치는 여전히 origin/main과 분기된 상태(로컬 22커밋 vs
origin 1커밋)다 - 이는 momolib에서 로컬 main의 미푸시 아바타 커밋을
건드리지 않았던 것과 같은 이유로 의도적으로 그대로 두었다(다른 세션의
로컬 작업 이력을 이 세션이 병합·정리할 권한이 없음).

## A. momolib — R&D 화면 폐쇄

### 1. 사전 확인

- 로컬 `.env` 값 노출 없이 확인: 이번 폐쇄는 코드 변경만으로 처리 -
  마이그레이션·DB 변경 없음(폐쇄 자체가 라우트 등록 제거만으로 가능).
- 조사 결과: R&D 전용 화면은 `app/vocab_quiz` 블루프린트
  하나(관리자 파일럿 응시·채점·미리보기 + 공개검토·승격 dry-run)뿐이고,
  이 블루프린트를 참조하는 곳은 그 자신의 템플릿뿐임을 grep으로
  재확인(다른 화면에서 `url_for('vocab_quiz....')`를 쓰는 곳 없음).

### 2. 폐쇄 방법

`app/__init__.py`에서 `app.register_blueprint(vocab_quiz_bp, ...)` 호출
**한 줄을 제거**(모듈 import는 유지 - 다른 코드의 하위 패키지 임포트
경로 유지용, 라우트 등록만 제거). Flask/Werkzeug 라우팅 자체에 경로가
없어지므로 `/vocab-quiz/*` 전체가 **로그인·역할과 무관하게 항상
404**가 된다(애플리케이션 코드의 `abort(404)`가 아니라 Werkzeug가
"이 경로 자체가 없다"고 판단하는 진짜 404). 블루프린트 코드·템플릿·
마이그레이션은 전혀 삭제하지 않았다.

### 3. 차단된 경로 전체 목록

| 경로 | 기능 |
|---|---|
| `GET /vocab-quiz/` | 관리자 대시보드 |
| `GET /vocab-quiz/contents`, `/vocab-quiz/contents/<id>` | 콘텐츠 목록·미리보기 |
| `GET /vocab-quiz/pilot/<key>`, `POST .../start` | 파일럿 정보·세션 시작 |
| `GET /vocab-quiz/pilot/session/<id>`, `POST .../answer`, `POST .../complete`, `GET .../result` | 파일럿 응시·채점·결과 |
| `GET /vocab-quiz/publish-review/`, `GET .../<content_id>`, `POST .../verdict` | 공개검토 목록·상세·판정저장 |
| `GET /vocab-quiz/publish-review/promote-dry-run` | 승격 dry-run |

메뉴 링크(`app/templates/vocab_quiz/index.html`의 "📋 공개검토" 버튼)는
그 페이지 자체가 이제 어디서도 렌더되지 않으므로 실질적으로 이미
제거됐다(전역 nav에는 애초에 vocab_quiz 링크가 없었음 - grep 재확인).

### 4. 영향받지 않은 것

- `app/vocab_quiz_student`(학생 기능, feature flag 기본 OFF) - 별도
  블루프린트라 전혀 영향 없음, 여전히 404.
- `app/models/content_bank.py`(BankQuestion), `app/models/lms.py`(LMS),
  `app/cms/routes.py`의 `/cms/vocab*`(레거시 CMS 퀴즈) - 전부 별도
  블루프린트, URL 맵에서 정상 유지 확인(로컬 `app.url_map` 직접 출력으로
  검증).
- 마이그레이션·테이블: `vocab_quiz_contents`(227), `vocab_quiz_pilot_
  items`(80), `vocab_quiz_admin_reviews`(판정 이력) 전부 스키마·데이터
  그대로 보존(다운그레이드·삭제 없음, ORM으로 조회 가능함을 테스트로
  확인).

### 5. 테스트 + 배포

`tests/test_vocab_quiz_rd_screens_closed.py`(신규, 격리 로컬
PostgreSQL) - 관리자/비관리자/비로그인 각각 `/vocab-quiz/*` 13개
경로·메서드 조합 전부 404, 학생 기능 여전히 404, 데이터·테이블 보존,
공개 플래그 0 유지 - **32/32 PASS**.

커밋 `16bf9a7`(브랜치 `feat/vocab-quiz-close-rd-screens`) →
`git push origin feat/vocab-quiz-close-rd-screens:main`(momolib은 기존과
동일하게 git push 자동배포 방식 - momolib 배포 스크립트는 `git pull`만
쓰고 `reset --hard`가 아니므로 위험 없음) → 자동배포 확인.

**배포 SHA: `16bf9a7`**

### 6. 라이브 검증 (실제 HTTP, QA 계정으로)

| 대상 | 결과 |
|---|---|
| 비로그인 `/vocab-quiz/publish-review/`, `/promote-dry-run`, `/pilot/l4l5`, `/` | 전부 404 |
| super_admin(QA 계정) 동일 경로 | 전부 404(로그인·관리자 여부 무관) |
| teacher(QA 계정) 동일 경로 | 전부 404 |
| super_admin `POST .../publish-review/ANY/verdict` | 404 |
| `/practice/vocab-quiz/`(학생) | 404(변경 없음) |

정리 후 재확인: `vocab_quiz_contents=227`, `vocab_quiz_pilot_items=80`,
`vocab_quiz_admin_reviews=0`, 공개 플래그 0건, `bank_questions=411`,
`users=6`(QA 계정 정리 완료 후).

## B. aprolabs — 공개검토 화면 신규 구현

### 5. 연구 DB 40콘텐츠·80문항 확인 (읽기 전용)

- `APP_ENV=research`, `VOCABULARY_QUIZ_DB_PATH=/home/chsh82/
  aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`(실행 중인
  프로세스의 `/proc/<pid>/environ`에서 직접 확인).
- 1순위 정의: `vocabulary_multiformat_items`의
  `source_version IN ('schema_reading_l4l5_pilot_dryrun_v1',
  'schema_reading_l6_pilot_dryrun_v1')` 80건이 참조하는 고유
  `source_content_id` **40건**(momolib 쪽 정의와 동일 기준).
- **momolib과의 ID·내용 전수 대조 결과**:
  - content_id 40건: **100% 일치**(존재 여부·모든 필드 값 전수 대조,
    차이 0건)
  - item_id 80건: **100% 일치**(집합 자체는 완전히 같음)
  - 문항 필드(내용) 80건 중 **79건 완전 일치**, **1건만 차이**:
    `MF_A_SC_SRL4L5PILOT_20260925_L4_003`(표제어 "집단")의
    `explanation` - momolib은 조사 오류가 **수정된 상태**("무리'라는"),
    aprolabs 연구 DB는 **원본 그대로**("무리'이라는", momolib에서만
    고쳤던 이력, 2026-09-28 기존 세션에서 이미 확인·기록된 사실).
  - **이 차이는 "예상 밖의 데이터 차이"가 아니라 이미 알고 있던 단일
    건이므로 쓰기를 중단하지 않고 계속 진행했다.** aprolabs 쪽 원본을
    자동으로 고치지는 않았다(범위 밖) - 대신 새 화면의 "주의 사항"에
    이 사실을 그대로 표시한다.

### 6-7. 신규 공개검토 화면

| 항목 | 내용 |
|---|---|
| 메뉴 위치 | 사이드바 "📘 문해력 · 어휘"(펼침 메뉴) 안, "📊 어휘 QA 검수" · "🧩 어휘 퀴즈(다유형 파일럿)" 다음 → **"📋 어휘 공개검토"** |
| 목록 URL | `https://aprolabs.co.kr/vocab-publish-review/` |
| 상세 URL | `https://aprolabs.co.kr/vocab-publish-review/{content_id}`(예: `.../SR_L4CORE_4812`) |
| 판정 저장 | `POST /vocab-publish-review/{content_id}/verdict` |
| 승격 실행 | **없음**(구현하지 않음, 아래 참고) |

- 상세 화면: 원천/학생용 뜻풀이, 예문, 레벨 근거(REVIEW_BOUNDARY/
  boundary_flag 강조), 연결 문항 2건의 선택지·정답(강조)·해설, **주의
  사항**(학년 경계 미확정·HOLD·비활성 자동 감지 + momolib과의 알려진
  차이 안내) 전부 표시.
- 판정: `vocabulary_publish_reviews` 테이블(신규, append-only)에
  승인후보/수정필요/보류 + 근거·판정자(이메일 스냅샷)·시각·콘텐츠/
  문항 내용 해시 스냅샷 기록. 콘텐츠·문항 내용이 바뀌면 해시 불일치로
  이전 판정이 "신선도 만료" 표시됨(로직 구현, 실측은 로컬 테스트에서
  content_hash 변경 시나리오로 검증했던 momolib 패턴과 동일 - aprolabs
  측은 함수 단위로 동일 로직 재사용).
- 기존 `app/vocabulary_quiz/routers/review.py`(QA 표본 검수, `/vocabulary-
  quiz/review`)는 **한 글자도 수정하지 않았고**, URL 경로도 완전히
  분리(`/vocab-publish-review`)돼 있어 충돌하지 않음.
- 비로그인 → 302(로그인 페이지), 비관리자 → 403(기존 `require_admin`
  의존성 그대로 재사용, 로컬에서 비관리자 임시 계정으로 실측).
- 모바일: 기존 사이드바가 이미 오프캔버스 반응형(햄버거 메뉴)이라
  별도 반응형 작업 없이 그대로 모바일에서도 노출됨.

### 8. 배포 절차

1. **백업**: SQLite Backup API로 운영 DB 백업 생성 -
   `vocabulary_quiz_research.db.bak-publish-review-20260928-pre-
   migration.db`(서버 보존, SHA-256 `96015b93...78934`,
   `integrity_check=ok`).
2. 백업을 로컬로 가져와 **격리 사본**에서 마이그레이션(`CREATE TABLE
   IF NOT EXISTS vocabulary_publish_reviews`) 적용 → 실제 연구 데이터
   기준 로컬 FastAPI 서버로 목록(40건)·상세·판정저장·판정이력·접근
   제어 전부 검증 - **14/14 PASS**.
3. 실제 연구 서버: 같은 마이그레이션을 **구 코드가 실행 중인 상태**에서
   적용(테이블 14→15, 기존 `vocabulary_contents=5950`/
   `vocabulary_multiformat_items=1369` 불변, `integrity_check=ok`).
4. 신규/변경 파일 8개를 scp로 서버에 직접 전송, SHA-256 전부 일치
   확인(로컬=서버).
5. `sudo systemctl restart aprolabs` → active 확인.

### 라이브 검증 (실제 브라우저 + HTTP)

- 비로그인 `GET /vocab-publish-review/`, `.../SR_L4CORE_4812` → 302.
- 관리자 로그인 → 사이드바에 "📋 어휘 공개검토" 메뉴 실제 노출 확인 →
  클릭 → 목록에 **40건** 정확히 표시(로컬 검증과 동일 결과) → "집단"
  상세 진입 → 뜻풀이·예문·레벨 근거·연결 문항 2건·주의 사항(momolib
  불일치 안내 포함) 전부 실제 화면에 표시 확인 → "승인후보" 선택 +
  근거 입력 → 저장 → "현재 판정: 승인후보" 즉시 반영 확인 → 목록으로
  돌아가 "집단" 행에 "승인후보" 표시 확인.
- DB 직접 재확인: 판정 저장 전후 `SR_L4CORE_4812`의
  `student_exposure=0`/`public_ready=0`/`level_status=REVIEW_BOUNDARY`/
  `boundary_flag=1` **완전 불변**.
- 테스트 판정 정리: `vocabulary_publish_reviews` 0건으로 삭제 완료.
- 최종 재확인: `vocabulary_contents=5950`, `vocabulary_multiformat_
  items=1369`, `vocabulary_review_samples=500`(기존 QA 표본 검수 데이터
  불변), `integrity_check=ok`.

**배포 상태: 서버 실행 코드 반영 완료(운영 중) + `origin/main` push까지
완료(`49a0f13` → `736f371`, cherry-pick으로 이번 기능 커밋만 반영, 위
"배포 방식" 절 참고) - git 추적 상태와 실제 서버 코드가 일치한다.**

## 5. 승격 버튼/API 부재 확인 (양쪽 모두)

- **momolib**: 블루프린트 자체가 폐쇄돼 `/vocab-quiz/publish-review/
  promote-dry-run`을 포함한 모든 경로가 404 - 애초에 승격을 실행할
  방법이 남아있지 않음.
- **aprolabs**: `app/vocabulary_quiz/routers/publish_review.py`에
  정의된 엔드포인트는 목록(GET)·상세(GET)·판정저장(POST) **3개뿐**.
  "승격"·"공개 전환"에 해당하는 라우트·버튼·API를 아예 구현하지
  않았다(코드에 존재하지 않음 - grep으로 재확인, `promote`라는 문자열
  자체가 이 라우터 파일에 없음).

## 6. 남은 것 / 확인 필요 사항

- aprolabs 연구 DB의 "집단" 문항 조사 오류(원본 미수정 상태)는 이번
  화면의 "주의 사항"에만 표시했고, 실제 수정은 하지 않았다 - 필요하면
  별도로 판단해 처리해야 한다.
- momolib `feat/vocab-quiz-close-rd-screens`/`feat/vocab-quiz-student-
  design` 로컬 브랜치, 로컬 main의 미푸시 아바타 커밋(`91de794`) 처리는
  이번에도 건드리지 않았다.
