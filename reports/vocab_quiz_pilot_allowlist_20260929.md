# 학생 파일럿 대상 제한(allowlist) 구현·검증·배포

- 일자: 2026-09-29
- 운영 DB의 160개 공개 필드(student_exposure/public_ready/level_status/
  boundary_flag) 변경 없음, allowlist에 실제 학생 없음, feature flag OFF
  — 코드는 실제로 운영에 배포됨(요청대로).

## 1. 기존 접근 제어 확인

`app/vocab_quiz_student/__init__.py`(변경 전)에는 전역 feature flag
(`VOCAB_QUIZ_STUDENT_ENABLED`) 검사 하나뿐이었고, 각 라우트의
`_student_only()`는 `current_user.role == 'student'`만 확인했다.
**특정 학생·반·계정 단위 제한은 전혀 없었다** — feature flag만 켜면
`role='student'`인 모든 계정에 즉시 열리는 구조.

운영 사용자 역할 집계(읽기 전용): `{"parent": 2, "super_admin": 1,
"branch_owner": 1, "student": 2}` — **현재 student 계정 2건(둘 다
active)**, allowlist 없이 flag만 켰다면 이 2건 전부가 노출 대상이었다.

## 2. Allowlist 구현(기본 차단)

- `app/models/vocab_quiz_pilot_allowlist.py`: `vocab_quiz_pilot_allowlist`
  테이블(`user_id` unique + `allowed_levels_json`, `users` FK ON DELETE
  CASCADE). **행이 없으면 그 학생은 아무 것도 못 본다**(빈 집합 반환 →
  자동 차단).
- `app/vocab_quiz/eligibility.py`: `student_allowed_levels(user_id)` /
  `student_is_pilot_allowed(user_id)` 추가 — 콘텐츠 공개 게이트(누구
  에게든 노출 가능한가)와 완전히 독립된 축(이 학생이 참여 가능한가).
- `app/vocab_quiz_student/__init__.py`: `before_request`에
  `_require_pilot_allowlist` 추가 — `role='student'`인 로그인 사용자만
  검사, allowlist에 없으면 403(비로그인/비학생 역할은 기존 302/403
  경로 그대로 유지).
- `/start`: 요청한 `vocab_level`이 그 학생의 허용 레벨 집합에 없으면
  403 `LEVEL_NOT_ALLOWED` — 콘텐츠 게이트 통과 여부와 무관하게 이중으로
  차단.
- 마이그레이션 `c7d2e9f4a1b3`(신규 테이블만, 기존 데이터 영향 없음).

**테스트**: 신규 `tests/test_vocab_quiz_student_pilot_allowlist.py`
**8/8 PASS**(미등록 학생 전면 403, 특정 레벨만 허용된 학생의 다른
레벨 403, 허용된 학생이라도 게이트 미통과 콘텐츠는 여전히 미노출) +
기존 `test_vocab_quiz_student_integration.py`(allowlist 등록 후
**18/18 PASS**) + `test_vocab_quiz_student_level_selection.py`
(**33/33 PASS**) + `test_vocab_quiz_student_gate.py`(무영향,
**123/123 PASS**).

## 3. 격리 사본(운영 백업 실제 복원) 검증

운영 백업 실제 복원(SHA-256 `6bef605c...c0a9b`, `bank_questions=411`
등 운영과 완전 일치) 후, 이전 작업에서 검증된 게이트 통과 40건을 이
격리 사본에만 승격(160건 필드 변경, 187건 불변) 적용해 실제 데이터로
검증:

| 검증 | 결과 |
|---|---|
| 전체 227건 중 정확히 40건만 공개, 187건 비공개 유지 | PASS |
| allowlist 미등록 학생 — index부터 403(공개 콘텐츠가 있어도) | PASS |
| L4·L5만 허용된 학생 — 두 레벨 전량(20건씩) 커버 | PASS |
| 같은 학생이 L6(미허용) 요청 — 403 `LEVEL_NOT_ALLOWED` | PASS |
| 세션 1건 전량 정답 제출 → complete 집계 일치 | PASS |
| 테스트 계정 2개만 추가, 정리 후 users 원상복구 | PASS |

**12/12 PASS.** 검증 후 PostgreSQL 인스턴스·데이터·바이너리 전체 삭제,
로컬 백업 사본 삭제(서버 원본 보존).

## 4. 운영 배포

- 커밋 `5de56c9`(`feat/vocab-quiz-close-rd-screens` 브랜치, 9개 파일).
- 구 코드 상태에서 마이그레이션 2개 선적용(`4d83374ccc5f` →
  `8a1c2f4e6b9d` → `c7d2e9f4a1b3`) — 서비스 재시작 없이 정상 작동,
  `vocab_quiz_contents=227`/`pilot_items=80`/`bank_questions=411`
  완전 불변 확인.
- 서버의 미추적 마이그레이션 파일 2개를
  `~/momolib_untracked_migrations_preserved_20260929-054631/`로 이동
  (삭제 아님, 내용 SHA-256 완전 일치 확인) 후 push — `git pull`
  무충돌.
- `git push origin feat/vocab-quiz-close-rd-screens:main`으로 fast-
  forward(`16bf9a7` → `5de56c9`), 자동배포 성공(서비스 20:46:58
  재시작).
- 배포 후 라이브 확인: `GET /practice/vocab-quiz/` → 404(feature flag
  OFF, 기존과 동일), `GET /vocab-quiz/publish-review/` → 404(관리자
  R&D 화면 여전히 닫힘), `GET /` → 200.
- 최종 재확인(읽기 전용): `vocab_quiz_contents=227`,
  `pilot_items=80`, `bank_questions=411`, `users=6`,
  `admin_reviews=0`, **`vocab_quiz_pilot_allowlist_rows=0`**(실제
  학생 없음), 공개 플래그 True인 콘텐츠 **0건**, `VOCAB_QUIZ_STUDENT_
  ENABLED` **여전히 OFF**.

## 5. 파일럿 대상 설정 방법 + "활성화 시 정확히 누가 무엇을 보는지"

### 설정 방법(현재는 화면 없이 직접 DB에 행 추가하는 방식)

```python
from app.models import db
from app.models.vocab_quiz_pilot_allowlist import VocabQuizPilotAllowlist
import json

db.session.add(VocabQuizPilotAllowlist(
    user_id="<대상 학생의 users.user_id>",
    allowed_levels_json=json.dumps([4, 5]),   # 예: L4·L5만 허용
    added_by_user_id="<등록한 관리자 user_id>",
    note="2026-09-29 파일럿 대상 등록",
))
db.session.commit()
```
(전용 관리자 화면은 이번 범위에 없음 — 필요하면 별도 구현.)

### 활성화(`VOCAB_QUIZ_STUDENT_ENABLED=true`) 시 실제로 벌어지는 일

**3중 독립 게이트**가 전부 통과해야 학생이 문항을 볼 수 있다:

1. **feature flag** — 꺼져 있으면(현재 상태) 로그인·역할·allowlist와
   무관하게 전부 404.
2. **allowlist** — flag가 켜져 있어도, `vocab_quiz_pilot_allowlist`에
   행이 없는 학생(현재 0명)은 index부터 403. **행이 있는 학생만**,
   `allowed_levels_json`에 지정된 레벨만.
3. **콘텐츠 공개 게이트**(`student_exposure`/`public_ready`/
   `REVIEW_BOUNDARY` 등, 현재 227건 전부 비공개) — allowlist로 허용된
   학생이라도, 그 레벨에 **실제로 공개된 콘텐츠가 없으면** 0문항.

**정리**: 지금 이 순간 `VOCAB_QUIZ_STUDENT_ENABLED=true`로 바꿔도
① allowlist가 비어 있어 **어떤 계정도 접근 불가**, ② 설사 누군가를
allowlist에 넣더라도 227건 전부 `student_exposure=False`라
**어느 레벨이든 0문항**만 보인다. 세 게이트를 순서대로 전부 통과시켜야
(flag ON + allowlist 등록 + 해당 콘텐츠 공개) 비로소 "누가 무엇을
보는지"가 실제로 결정된다 — 예를 들어 학생 X를 `allowed_levels=[4]`로
등록하고 게이트 통과 40건 중 L4 10건(20문항)을 승격하면, **학생 X만**
로그인해 L4를 선택했을 때 **그 20문항 중 무작위 10문항**을 풀 수 있고,
다른 학생·다른 레벨·비공개 187건은 어떤 경로로도 노출되지 않는다.
