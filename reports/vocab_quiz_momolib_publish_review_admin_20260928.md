# momolib 학생용 어휘 퀴즈 — 관리자 공개검토 기능 구현 완료

- 일자: 2026-09-28
- momolib 브랜치: `feat/vocab-quiz-student-design`(격리, 커밋 `81b1945`,
  main 미병합·미푸시)
- **운영 DB 쓰기 없음. 학생용 배포 없음. 승격(공개 플래그 전환) 실행
  없음.** 전 과정을 격리 로컬 PostgreSQL(포트 55433, 운영과 무관)에서만
  검증했다.

## 1. 관리자 공개검토 화면

기존 관리자 블루프린트(`app/vocab_quiz`, `@requires_role('super_admin',
'hq_manager')`)에 새 모듈로만 추가했다 — 기존 `routes.py`(파일럿
응시·채점)는 한 글자도 건드리지 않았다.

| 화면 경로 | 내용 |
|---|---|
| `GET /vocab-quiz/publish-review/` | 1순위(파일럿 연결) 콘텐츠 개별 목록 + 판정 상태 배지, 2순위(독립 출처 근거등급) 대기열, 3순위 건수만 표시(자동 승인 없음) |
| `GET /vocab-quiz/publish-review/<content_id>` | 표제어·레벨 근거·원천/학생용 뜻풀이·예문·연결 문항(정답 하이라이트)·온라인 QA 참고자료·판정 이력·판정 입력 폼 |
| `POST /vocab-quiz/publish-review/<content_id>/verdict` | 판정 저장(승인후보/수정필요/보류 + 근거 필수) |
| `GET /vocab-quiz/publish-review/promote-dry-run` | 읽기 전용 승격 dry-run(2절) |

관리자 대시보드(`/vocab-quiz/`)에 "📋 공개검토" 버튼을 추가해 진입
경로를 만들었다.

### 1순위 40건 실제 검토 흐름 (격리 DB, 가상 데이터로 실연)

1. `demo-admin@momolib.local`(super_admin)로 로그인
2. `/vocab-quiz/publish-review/` 접속 → 1순위 목록에서 콘텐츠 선택
3. 상세 화면에서 원천/학생용 뜻풀이·예문·레벨 근거(REVIEW_BOUNDARY
   여부 강조 표시)·연결 문항 2종(정답 하이라이트·해설)·온라인 QA
   참고자료를 한 번에 확인
4. 라디오로 판정(승인후보/수정필요/보류) 선택 + 근거 필수 입력 후
   "판정 저장" → 같은 화면에 "현재 판정: 승인후보 (데모super_admin,
   2026-09-27 21:36)"으로 즉시 반영됨을 실제 화면에서 확인
5. `/vocab-quiz/publish-review/promote-dry-run`에서 방금 승인후보 처리한
   REVIEW_BOUNDARY 콘텐츠가 "승격 대상 - 변경 예정 필드: 레벨
   L6(demo_v1): level_status REVIEW_BOUNDARY -> PROVISIONAL_AUTO,
   boundary_flag True -> False"로 정확히 계산되는 것을 확인(조회 후
   DB 재확인 결과 실제로는 아무것도 안 바뀜)

227건 중 파일럿(관리자 화면 실사용 온라인 QA까지 통과)에 연결된 정확히
**40건**이 1순위로 분류됨을 격리 DB에서도, 코드 로직에서도 확인했다
(`tier1_content_ids()` = 파일럿이 참조하는 고유 `content_id` 집합).

## 2. 관리자 판정 기록

신규 테이블 `vocab_quiz_admin_reviews`(append-only):

| 필드 | 내용 |
|---|---|
| `verdict` | `APPROVED_CANDIDATE`(승인후보)/`NEEDS_FIX`(수정필요)/`HOLD`(보류) |
| `rationale` | 판정 근거(필수) |
| `reviewer_user_id`, `reviewed_at` | 판정자·시각 |
| `content_hash_at_review`, `item_hashes_at_review_json` | 판정 시점 콘텐츠·연결 문항 버전 스냅샷 |

- **판정 저장은 `vocab_quiz_admin_reviews`에만 쓴다** -
  `student_exposure`/`public_ready`/`level_status`/`boundary_flag`는
  절대 바뀌지 않음을 테스트로 확인(저장 전후 값 비교, HTTP 경로로
  저장한 뒤에도 재확인).
- **신선도(stale) 판정**: 저장된 스냅샷과 현재 `content_hash`/연결
  문항의 `item_hash` 전체를 비교해, 콘텐츠나 문항이 하나라도 바뀌면
  "이 판정 이후 콘텐츠 또는 연결 문항이 수정되어 현재 버전에는 더 이상
  유효하지 않습니다"를 화면에 표시한다. 콘텐츠 해시 변경, 문항 해시
  변경 양쪽 다 테스트로 검증했다.
- 판정은 이력으로 쌓인다(재검토 시 새 행 추가, UPDATE로 덮어쓰지 않음).

## 3. 승격 절차 분리 (dry-run만 구현, 실행 경로 자체가 없음)

`promote-dry-run`은 1순위 콘텐츠마다:
1. 관리자 판정 존재 여부 + `APPROVED_CANDIDATE`인지 + 신선도(stale
   아닌지)
2. `hold_reason`/`is_active`(콘텐츠·문항)
3. 연결 문항의 구조 품질(선택지·정답 존재)
4. eligibility 게이트(레벨 존재 여부, `REVIEW_BOUNDARY`/`boundary_flag`)

를 전부 확인해, 통과한 건은 **정확히 어떤 필드가 어떻게 바뀔
예정인지**(`student_exposure: False -> True`,
`level_status REVIEW_BOUNDARY -> PROVISIONAL_AUTO` 등)를 보여주고,
통과 못한 건은 구체적 차단 사유를 나열한다. **"승격 실행" 버튼이나
API 자체를 이 코드베이스에 구현하지 않았다** - dry-run 조회 자체가
읽기 전용 쿼리만 수행하며, 이번 지시("승격을 실행하지 마")를 코드
구조로 보장했다.

## 4. feature flag + 격리 DB 회귀 테스트

- `config.py`: `VOCAB_QUIZ_STUDENT_ENABLED`(환경변수, 기본 `false`).
- `app/vocab_quiz_student/__init__.py`의 `before_request`가 플래그
  꺼짐 상태에서 로그인·역할과 무관하게 전 라우트를 404로 막는다(운영
  환경변수에는 설정 안 함 - 이 브랜치가 배포되더라도 학생 화면은
  열리지 않음).
- 격리 DB 가상 공개 문항(이전 단계에서 만든 `DEMO_ELIGIBLE_*` 10건
  20문항)으로 회귀 테스트:

| 테스트 파일 | 결과 |
|---|---|
| `tests/test_vocab_quiz_student_gate.py`(게이트 매트릭스) | 123/123 PASS |
| `tests/test_vocab_quiz_student_integration.py`(학생 e2e, 플래그 on) | 18/18 PASS |
| `tests/test_vocab_quiz_publish_review.py`(공개검토+승격dry-run+플래그) | **36/36 PASS** |

`test_vocab_quiz_publish_review.py`가 확인한 것: 플래그 기본 OFF -
비로그인·학생 모두 404 → 명시적으로 켜면 200 → 다시 끄면 즉시 404로
복귀.

### 운영 재확인 (읽기 전용)

```
전체 콘텐츠: 227
student_exposure 또는 public_ready = True: 0
bank_questions: 411 (불변)
users: 6 (불변)
```

이번 기능은 운영에 배포되지 않았으므로 운영에서 `/practice/vocab-quiz/`
라우트 자체가 존재하지 않는다. "학생 접근 시 문항 0건"은 데이터
수준에서 더 강하게 보증된다 - 227건 전부 `student_exposure=0`이므로
설령 배포되더라도 `eligible_pilot_items()`는 반드시 빈 리스트를
반환한다(게이트 로직상 자명하며, 격리 DB 테스트에서 동일 조건으로
직접 검증됨).

## 5. 커밋

| 저장소 | 커밋 | 내용 |
|---|---|---|
| momolib | `81b1945`(격리 브랜치, push 없음) | 공개검토 화면 3종 + 판정 모델/마이그레이션 + 승격 dry-run + feature flag + 테스트 36건 |

같은 작업 디렉터리에 있던 사용자의 다른 미커밋 변경(`.env.example`,
`app/learn/routes.py`, `app/lms/routes.py`, `app/models/lms.py`,
`app/templates/learn/item.html`, `app/templates/lms/curriculum/
detail.html`, `migrations/versions/63723761586a_mileage_and_avatar.py`,
`app/api/`, `app/services/aprolabs_client.py`)는 이번에도 전혀
건드리지 않았고 커밋에도 포함하지 않았다(`app/__init__.py`도 이번
단계에서는 수정할 필요가 없어 손대지 않음 - 기존 blueprint 등록은
직전 단계 커밋에서 이미 완료됨).
