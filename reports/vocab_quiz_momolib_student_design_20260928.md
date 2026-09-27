# momolib 학생용 어휘 퀴즈 — 설계·게이트·프로토타입 (운영 미배포)

- 일자: 2026-09-28
- momolib 브랜치: `feat/vocab-quiz-student-design`(격리, main에 미병합·미푸시)
- **운영 DB 변경 없음. 학생 배포 없음. `student_exposure`/`public_ready` 전환 없음.**
- 전 과정을 격리 로컬 PostgreSQL(`momolib_vocab_student_test`, 포트 55433)에서만
  검증했다.

## 1. 기존 조사 + 재사용성 판단

### 1-1. 기존 학생용 퀴즈 파이프라인 조사

momolib에 "vocab_quiz"라는 이름이 붙은 기존 학생용 기능이 **이미 하나
있었다** — 단, 이번에 이식한 `VocabQuizContent`/`VocabQuizPilotItem`과는
**완전히 무관한 레거시 시스템**이다.

| 구분 | 레거시 `vocab_quiz`(content_type) | 이번 이식 `vocab_quiz_*` 테이블 |
|---|---|---|
| 위치 | `app/models/library.py`(`LearningContent.type=='vocab_quiz'`), `app/models/content_bank.py`, `app/models/lms.py` | `app/models/vocab_quiz.py` |
| 데이터 저장 | `QuizQuestion.choices`(JSON, `{word, choices, correct_idx}`) - 관리자가 도서별로 수동 등록 | `vocab_quiz_pilot_items`(문항), `vocab_quiz_contents`(뜻풀이) - aprolabs 연구 사이트에서 이식 |
| 학생 화면 | `library/content_play.html` + `library.content_submit`(한 폼에 문항 전부 제출, 점수는 flash 메시지로만 표시, 해설 없음) | 없음(관리자 파일럿만 존재) |
| 채점 로직 | `_check_answer()` - 선택한 인덱스와 `correct_idx` 문자열 비교 | `app/vocab_quiz/routes.py`의 `pilot_answer` - `correct_option` 정수 비교 |

**결론**: 두 시스템은 테이블도, 라우트도, 정답 비교 방식도 다르다.
레거시 파이프라인의 코드를 재사용하지 않고, 대신 그 **UI 관례**(학생
전용 모바일 템플릿 `student_base.html`, `s-card`/`s-btn-primary` 스타일,
`_student_only()` 접근 패턴)를 새 화면에 그대로 적용했다.

### 1-2. 관리자 파일럿의 재사용 가능 여부

| 요소 | 재사용 가능? | 근거 |
|---|---|---|
| 정답 비노출 패턴(GET 응답에 `correct_option`/`answer_payload_json` 절대 미포함, POST 채점 후 그 문항만 공개) | **그대로 재사용** | `app/vocab_quiz_student/routes.py`의 `take()`/`answer()`가 관리자 `pilot_take`/`pilot_answer`와 동일한 필드 선택·JSON 구조를 씀(코드 복붙이 아니라 같은 패턴을 의도적으로 재적용) |
| 채점 로직(`int(selected_option) == item.correct_option`) | **그대로 재사용** | 동일 |
| 문항 원본 테이블(`vocab_quiz_pilot_items`, `vocab_quiz_contents`) | **읽기 전용으로 공유** | 학생용 라우트는 이 테이블에 절대 쓰지 않음 |
| 세션·응답 기록 테이블 | **재사용하지 않음(신규 테이블)** | 관리자 파일럿 세션(`vocab_quiz_pilot_sessions/_attempts`)은 "관리자가 QA 목적으로 응시한 기록"이라는 감사 의미를 갖는다. 여기에 실제 학생 응답이 섞이면 그 감사 기록의 의미가 오염된다. 그래서 `vocab_quiz_student_sessions`/`vocab_quiz_student_attempts`를 별도로 만들었다(4절) |
| 관리자 인증 데코레이터(`@requires_role('super_admin','hq_manager')`) | **건드리지 않음** | `app/vocab_quiz/routes.py`는 이번 작업에서 단 한 줄도 수정하지 않았다(git diff로 확인 가능) - 학생용은 완전히 새 블루프린트(`app/vocab_quiz_student`)와 새 접근조건(`role == 'student'`)으로 분리했다. 관리자 라우트가 학생용 라우트를 import하거나 그 반대의 경우도 없다 |

**"관리자 인증 경계를 약화하지 않았다"는 것을 코드 구조로 보장**: 학생용
블루프린트는 관리자 블루프린트의 뷰 함수를 호출하지 않고, 관리자
블루프린트도 학생용 블루프린트를 참조하지 않는다. 유일한 공유 지점은
읽기 전용 모델(`VocabQuizContent`/`VocabQuizContentLevel`/
`VocabQuizPilotItem`)과 2절의 게이트 함수뿐이다.

## 2. 학생 공개 자격 단일 게이트

### 2-1. 설계

`app/vocab_quiz/eligibility.py`(신규, 라우트 없음) — 어떤 학생용 화면도
`vocab_quiz_contents`/`vocab_quiz_content_levels`/`vocab_quiz_pilot_items`에
직접 필터 조건을 다시 쓰지 않고 이 모듈의 함수만 거친다.

**제외 규칙**(하나라도 해당하면 제외):
1. `content.is_active = False`
2. `content.student_exposure = False`
3. `content.public_ready = False`
4. `content.hold_reason`이 비어있지 않음(HOLD)
5. 이 콘텐츠에 연결된 레벨이 하나도 없음(안전 기본값)
6. 연결된 레벨 중 하나라도 `level_status == 'REVIEW_BOUNDARY'` 이거나
   `boundary_flag = True`(REVIEW_BOUNDARY)
7. 문항이 참조하는 콘텐츠가 여러 개(`source_content_ids_json`)면 **전부**
   통과해야 함(하나라도 부적격이면 문항 전체 제외)
8. 문항 자체가 `is_active = False`

핵심 함수:
- `eligible_content_ids() -> set[str]`
- `item_is_eligible(item, eligible_cids=None) -> bool`
- `eligible_pilot_items(pilot_key=None) -> list[VocabQuizPilotItem]`

### 2-2. 역할 × 레벨 × 문항 유형 테스트

`tests/test_vocab_quiz_student_gate.py`(격리 DB 전용) - 레벨(4/5/6) ×
문항 유형(MEANING_CHOICE/CONTEXT_MEANING) × 배제 사유 9종 + 기준(공개
가능) 1종 = **60개 픽스처**를 만들어 게이트를 통과한 집합과 대조하고,
배치 API(`eligible_pilot_items`)와 단건 API(`item_is_eligible`)가 항상
같은 결론을 내는지도 교차 확인한다.

```
전체 PASS (123/123)
```
(60건 × 배치 판정 + 60건 × 단건 판정 + 픽스처 생성·정리 확인 3건)

**역할 축**은 게이트 자체와 무관하다(콘텐츠 공개 상태는 "누가 묻는지"와
상관없이 결정되는 값이므로). 대신 라우트 레벨 접근 통제(비로그인/
teacher/parent/admin이 학생 화면에 들어올 수 없음)를 별도로
검증했다(3-3절).

## 3. 프로토타입 구현 + 검증 (격리 DB·가상 데이터)

### 3-1. 신규 파일

| 파일 | 내용 |
|---|---|
| `app/models/vocab_quiz_student.py` | `VocabQuizStudentSession`/`VocabQuizStudentAttempt`(신규 테이블, 관리자 파일럿 테이블과 완전 분리) |
| `app/vocab_quiz/eligibility.py` | 단일 게이트(2절) |
| `app/vocab_quiz_student/__init__.py`, `routes.py` | 학생용 블루프린트, `url_prefix='/practice/vocab-quiz'`, 전 라우트 `@login_required` + `role=='student'`만 허용 |
| `app/templates/vocab_quiz_student/{index,take,result}.html` | `student_base.html` 상속(모바일 우선, 하단 탭바) |
| `migrations/versions/0f604cbf8f2c_add_vocab_quiz_student_tables.py` | 신규 테이블 2개만 생성(현재 운영 head `37b334ba3fc8`을 `down_revision`으로 삼음, `flask db migrate`로 자동생성 후 diff가 정확히 이 2개 테이블뿐임을 확인) |
| `tests/test_vocab_quiz_student_gate.py` | 2-2절 게이트 매트릭스 테스트 |
| `tests/test_vocab_quiz_student_integration.py` | 아래 3-3절 통합 테스트 |
| `tests/seed_vocab_quiz_student_demo.py` | 아래 3-2절 수동 확인용 가상 데이터 시드 |

### 3-2. 가상 공개 문항으로 학생 화면 수동 확인(모바일)

`tests/seed_vocab_quiz_student_demo.py`로 격리 DB에 **가상 공개 콘텐츠
10건**("가상어휘일"~"가상어휘십", L4/L5/L6 각 일부, 문항 2종=20문항) +
**REVIEW_BOUNDARY 비공개 콘텐츠 1건**(화면에 절대 나오면 안 됨)을 심고,
격리 DB만 가리키는 로컬 Flask 서버(포트 5511, 운영과 무관)를 띄워
Chrome에서 실제로 확인했다.

- 390px 모바일 폭: 인덱스("지금 풀 수 있는 문항 20개" - REVIEW_BOUNDARY
  1건이 정확히 제외된 수치) → 연습 시작 → 10문항 응시(라디오 선택 시
  즉시 정답/오답 표시) → 완료(10/10) → 결과 화면(문항별 해설) 전부
  하단 탭바 레이아웃으로 정상 렌더링.
- 제출 전 HTML에 `correct_option`/`answer_payload_json` **0건**(직접
  HTML 스캔으로 확인).
- REVIEW_BOUNDARY 문항("가상비공개어휘")이 결과 화면 텍스트에도
  **한 번도 등장하지 않음**(`document.body.innerText.includes(...)` ===
  false로 확인).
- 데스크톱(1280px)에서도 사이드바 레이아웃으로 정상 렌더링(레이아웃
  깨짐 없음).

### 3-3. 자동 통합 테스트(`tests/test_vocab_quiz_student_integration.py`)

Flask `test_client()`로 실제 HTTP 요청 기반 e2e 테스트. **17/17 PASS**:

```
[PASS] 비로그인 GET /practice/vocab-quiz/ 차단(실제 302)
[PASS] teacher role GET /practice/vocab-quiz/ -> 403(실제 403)
[PASS] student 로그인 성공(실제 302)
[PASS] student GET /practice/vocab-quiz/ -> 200(실제 200)
[PASS] REVIEW_BOUNDARY 문항이 20회 세션 생성 중 단 한 번도 등장하지 않음
[PASS] 세션 시작 200(실제 200)
[PASS] 세션 응시 화면 200
[PASS] 응시 화면에 answer_payload_json 미노출
[PASS] 응시 화면에 correct_option 미노출
[PASS] 전부 정답 응답 시 전부 is_correct=True(실제 10/10)
[PASS] 세션 완료 처리 200
[PASS] 완료 후 correct_count == 문항 수(전부 정답)
[PASS] 결과 화면 200
[PASS] 다른 학생의 세션 조회 차단(실제 403)
[PASS] 학생 응시가 관리자 파일럿 세션 테이블에 전혀 기록되지 않음(완전 분리)
[PASS] 학생 응시가 관리자 파일럿 응답 테이블에 전혀 기록되지 않음(완전 분리)
[PASS] 테스트 계정 정리 후 users 원상복구(4 -> 4)
```

**테스트 작성 중 발견한 테스트 하네스 버그(제품 코드 버그 아님)**: 서로
다른 사용자로 로그인한 두 번째 `test_client()`를 만들 때, 바깥쪽에
`with app.app_context():`를 미리 열어둔 상태로 로그인 요청을 보내면
Flask-Login 세션이 저장되지 않는 현상을 재현했다(`client2.
session_transaction()`이 빈 세션을 반환 - `_user_id` 없음). 원인은 이번
학생용 라우트 코드가 아니라 **테스트 스크립트 구조**였다 - DB 직접
조작에만 짧게 `app_context`를 열고 HTTP 요청 구간은 바깥에 컨텍스트를
미리 열어두지 않도록 고쳐서 해결했다. 실제 라우트의 소유권 검사
(`session.user_id != current_user.user_id: abort(403)`)는 처음부터
정상이었다(고친 뒤 403 정상 확인).

### 3-4. 운영 코드 영향 확인

- `app/vocab_quiz/routes.py`(관리자 파일럿) **0줄 변경**.
- `app/__init__.py`/`app/models/__init__.py`에는 신규 블루프린트·모델
  import 등록만 추가(기존 등록 줄은 그대로).
- 운영 DB(`momolib.com`)는 이번 작업 전체에서 **한 번도 연결하지
  않았다** - SSH·psql 접속 이력 없음.
- 로컬에 이미 남아있던 관리자 파일럿 통합 테스트
  (`test_vocab_quiz_integration.py`)를 이 격리 DB에 재실행하면 세션
  시작에서 500이 나는데, 이는 이 격리 DB에 실제 파일럿 문항 데이터를
  적재(`import_vocab_quiz_export.py --apply`)한 적이 없어서
  매니페스트-DB 정합성 검사가 실패하는 것뿐이다(이번 학생용 작업이
  건드린 적 없는 기존 관리자 코드 경로의 회귀가 아니라, 이 DB 인스턴스에
  그 데이터를 안 넣었을 뿐 - 별도 조치 불필요, 참고로만 기록).

## 4. 아키텍처 다이어그램(텍스트)

```
[momolib.com 운영]
  app/vocab_quiz/            (관리자 전용, super_admin/hq_manager만)
    routes.py  ── 문항 원본 조회(게이트 없음, 관리자는 전부 봄) + pilot 세션/응답
    ↓ 읽기 전용 공유
  app/models/vocab_quiz.py   VocabQuizContent / ContentLevel / PilotItem
    ↑ 읽기 전용 공유 (반드시 eligibility.py를 거쳐서만)
  app/vocab_quiz/eligibility.py   ← 단일 게이트(이번 신규)
    ↑
  app/vocab_quiz_student/    (학생 전용, role=='student'만, 이번 신규)
    routes.py  ── eligible_pilot_items()로만 문항 선택 + 신규 학생 세션/응답
  app/models/vocab_quiz_student.py   VocabQuizStudentSession / Attempt (신규, 분리)
```

## 5. 남은 것 / 하지 않은 것

- 운영 `momolib.com`에는 이번 브랜치를 **병합·푸시·배포하지 않았다**.
- `student_exposure`/`public_ready`/`hold_reason`/`level_status`/
  `boundary_flag` 등 어떤 공개 플래그도 실제 콘텐츠에서 전환하지
  않았다(운영 DB는 여전히 227건 전부 비공개).
- 세션 크기(1회 10문항), pilot_key 구분을 학생에게 노출하지 않는 결정
  등은 이번 프로토타입의 설계 선택이며, 실제 공개 시점에 기획 재검토가
  필요하다.
- 4절 dry-run 공개후보 분류는 별도 보고서
  (`vocab_quiz_momolib_publish_candidates_dryrun_20260928.md`)에서 다룬다.
