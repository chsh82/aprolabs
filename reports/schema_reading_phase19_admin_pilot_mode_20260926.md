# 스키마리딩x어휘 DB 통합 — 19단계: 관리자 화면 "L4·L5 파일럿" 모드 추가(phase18 40건 정식 세션화)

- 작성일: 2026-09-26 (`date` 명령으로 시스템 현재 날짜 직접 확인)
- 전제(재조사하지 않고 그대로 인용): `reports/schema_reading_phase18_quiz_pilot_apply_20260926.md`
  — `vocabulary_multiformat_items`에 40건(`source_version='schema_reading_l4l5_pilot_dryrun_v1'`,
  MEANING_CHOICE 20 + CONTEXT_MEANING 20)이 이미 실제 적재돼 있고, `SOURCE_VERSION="2.1.29"`
  하드코딩 때문에 정상 세션 생성 API로는 구조적으로 선택되지 않음(의도적 격리).
  `reports/schema_reading_phase13_l4_core50_apply_20260925.md`,
  `reports/schema_reading_phase14_l5_core_apply_20260925.md` — 이 40건이 연결된 20개
  content_id(`SR_L4CORE_*`/`SR_L5CORE_*`)는 `level_status='REVIEW_BOUNDARY'`,
  `student_exposure=0`, `public_ready=0`.
- 목표: 관리자 전용 "L4·L5 파일럿" 모드를 추가해, 이 40건만 정식 세션 생성 API(우회
  없음)로 조회·풀이·제출·채점할 수 있게 한다. 기존 일반 출제(`SOURCE_VERSION="2.1.29"`)
  경로는 전혀 수정하지 않는다.
- git commit/push 없음, 서버 배포 없음(로컬 검증까지만) — 이번 세션의 절대 제약대로.

---

## 1. 코드 변경 요약

### 1-1. `app/vocabulary_quiz/routers/multiformat.py`

기존 일반 출제 함수(`_select_question_items`, `_select_level_candidates`,
`_level_availability`, `create_session`의 레벨모드/혼합모드/CROSSWORD 분기,
`SOURCE_VERSION = "2.1.29"`)는 **한 글자도 수정하지 않았다.** 추가만 했다:

- `import logging` + `logger = logging.getLogger(__name__)`, `VocabularyContent` import 추가.
- 상수 `PILOT_SOURCE_VERSION = "schema_reading_l4l5_pilot_dryrun_v1"` 신규 추가(주석으로
  phase18 보고서 인용).
- `CreateSessionBody`에 `pilot_mode: bool = False` 필드 추가 — 이 필드를 안 보내는
  기존 모든 요청은 기본값 `False`라 동작이 100% 그대로다.
- 신규 함수 `_select_pilot_item_ids(db, item_types)` — 3중 검증(2절).
- 신규 함수 `_pilot_availability(db, item_types)` — 표시 전용 가용량 집계.
- 신규 엔드포인트 `GET /api/vocabulary-quiz/pilot-availability` — 기존
  `GET /availability`는 그대로 두고 완전히 별도로 추가.
- 신규 함수 `_create_pilot_session(body, db, admin)` — 파일럿 전용 세션 생성 로직
  (문항 선택 → 세션/응답 행 INSERT → 커밋), 기존 `create_session()` 본문 로직을
  복제하되 문항 소스만 `_select_pilot_item_ids()`로 교체.
- `create_session()`에는 딱 한 줄의 분기만 추가:
  `if body.pilot_mode: return _create_pilot_session(body, db, admin)` — 이 반환 이후
  기존 코드(레벨모드/혼합모드/CROSSWORD)는 실행되지 않는다. 기존 코드 라인은
  그대로 아래에 이어진다(삭제/수정 없음).
- `session_result()`(`GET /sessions/{id}/result`)에 `pilot_info` 응답 필드 추가 —
  `metadata.get("pilot_mode")`가 참이면 `level_info`는 `None`으로 두고 별도
  `pilot_info`(candidate_count 등)를 채운다. 기존 레벨모드/전체모드 세션의
  `level_info` 구성 로직과 반환값은 그대로다(추가 키만 붙음).
- `play_page()`(`GET /vocabulary-quiz/multiformat/play`)에 `pilot_available`
  컨텍스트 변수 추가(`_pilot_availability(db, None)["available_items"] > 0`) —
  파일럿 후보가 0건이면 화면에서 체크박스를 비활성화하기 위함. 기존
  `level_disabled` 계산 로직은 그대로다.

`next_question`/`submit_answer`/`request_hint` 등 문제 조회·채점 엔드포인트는
**전혀 수정하지 않았다** — 세션 ID 기반으로 동작하는 공용 로직이라 파일럿
세션도 자동으로 같은 경로(정답 비노출 GET, 서버 채점 POST)를 탄다.

### 1-2. `app/templates/vocabulary_quiz/multiformat_play.html`

- 설정 화면 최상단에 "L4·L5 파일럿 문항만 출제" 체크박스(`#pilot-mode-checkbox`)
  추가(파일럿 후보 0건이면 서버 렌더링 시점에 `disabled`).
- 기존 "출제 범위(레벨 선택)" 블록을 `<div id="level-scope-fields">`로 감싸
  파일럿 모드 선택 시 JS로 숨길 수 있게 함(HTML 구조만 감쌌을 뿐 내부 내용은 무수정).
- JS: `isPilotMode()` 헬퍼, `refreshAvailability()`에 파일럿 분기(별도로
  `/pilot-availability` 호출, 기존 `/availability` 호출 경로는 그대로 아래에
  남아 있음), `pilotModeCheckbox`의 `change` 리스너(레벨/신뢰도 필드 숨김,
  CROSSWORD 비활성화), `start-btn` 클릭 핸들러에 `pilot_mode: true` 분기 추가
  (레벨모드 파라미터는 보내지 않음), 결과 화면에 `pilot_info` 표시 분기 추가.
- 기존 레벨모드 관련 JS 로직(레벨 select/신뢰도 라디오/CROSSWORD 단독모드 처리)은
  삭제·수정 없이 그대로 유지.

---

## 2. 3중 검증 로직 (`_select_pilot_item_ids`)

매 요청마다 DB에서 새로 조회한다(하드코딩된 40개 item_id 목록을 코드에 넣지 않음):

1. **배치 소속**: `vocabulary_multiformat_items.source_version ==
   'schema_reading_l4l5_pilot_dryrun_v1'` AND `is_active=1` (SQL 필터).
2. **content_id 존재/활성**: 문항의 `source_content_id`가
   `vocabulary_contents`에 실제로 존재하고 `is_active=1`인지 ORM으로 재조회.
3. **레벨 상태**: 그 content_id의 `vocabulary_content_levels`
   (`level_version='level_policy_v0.1'`, `is_active=1`) 행의 `level_status`가
   정확히 `'REVIEW_BOUNDARY'`인지.

2)/3) 중 하나라도 예상과 다르면 `logger.warning(...)`으로 item_id/content_id와
사유를 남기고 그 문항 하나만 조용히 제외한다(전체 실패 아님).

**실제 동작 확인(2절 테스트 결과, research DB 사본):**
- 정상 상태에서 3중 검증을 통과한 후보는 정확히 40건, DB의 marker 40건과
  1:1 일치, 경고 로그 0건.
- 한 content_id를 `is_active=0`으로 강제로 바꾸자 그 content_id에 연결된
  문항 2건(MEANING_CHOICE 1 + CONTEXT_MEANING 1)만 후보에서 빠지고(38/40),
  `is_active` 관련 경고 로그가 실제로 남는 것을 확인. 원복 후 다시 40건.
- 같은 content_id의 `level_status`를 `PROVISIONAL_AUTO`로 바꾸자 역시 해당
  2건만 제외되고(38/40), `level_status` 관련 경고 로그가 남는 것을 확인.
  원복 후 다시 40건, marker 집합과 재일치.
- **REVIEW_BOUNDARY/student_exposure/public_ready는 테스트 종료 후 원래
  값으로 완전히 복원했다** — 이번 기능이 콘텐츠 상태를 바꾸지 않는다는
  제약을 실제로 지켰음을 최종 재조회로 확인(4절 표).

---

## 3. 테스트 결과

### 3-0. 테스트 환경에 대한 설명

로컬 `data/vocab/vocabulary_quiz_rnd.db`에는 phase18 데이터가 없다(로컬은
phase13/14 이전 상태 — `vocabulary_contents`/`vocabulary_content_levels` 각
5,723건, `vocabulary_multiformat_items` 1,289건, 파일럿 marker 0건). 파일럿
선택이 실제로 뭔가를 반환하는지 검증하려면 phase18이 적용된 DB가 필요해
research 서버의 `vocabulary_quiz_research.db`를 **읽기 전용으로 scp 다운로드한
사본**(로컬 스크래치 디렉터리)에 대해 새 테스트를 실행했다 — 서버에는 어떤
것도 쓰지 않았다(배포 없음, SSH는 다운로드에만 사용). 사본 확인 결과 phase18
보고서와 정확히 일치(`vocabulary_multiformat_items`=1,329, `vocabulary_contents`/
`vocabulary_content_levels`=5,820, 파일럿 marker=40). 테스트 전후 사본의 핵심
컬럼(활성 상태·레벨 상태·source_version) 테이블 해시가 완전히 일치함을 확인했고,
테스트 종료 후 이 사본 파일은 로컬에서 삭제했다(산출물 아님, 순수 검증용
fixture).

일반 출제 회귀(`tests/test_admin_level_quiz.py`)는 **로컬 rnd.db**(이 파일이
원래 기대하는 데이터셋, 하드코딩된 기대 카운트가 이 DB 기준)에 대해 그대로
실행했다.

### 3-1. 테스트 1 — 관리자 로그인 → 파일럿 선택 → 정상 세션 생성(우회 없음) →
문항 조회 → 제출 → 채점

신규 `tests/test_phase19_admin_pilot_mode.py`(research DB 사본 대상, `[PASS]/[FAIL]`
스타일, FastAPI TestClient) 결과: **51건 중 실패 0건.**

- `POST /api/vocabulary-quiz/sessions {"pilot_mode": true, "question_count": 40}`
  (정식 API, 세션 직접 삽입 등 우회 전혀 없음) → 40문항 세션 생성 성공.
  응답의 `source_version`이 파일럿 marker, `level_info=None`,
  `pilot_info.candidate_count=40`. DB에 저장된 세션 행의 `source_version`/
  `metadata_json`도 정확히 파일럿 marker/`{audience: ADMIN_ONLY, pilot_mode: true, ...}`.
- 40문항 전부 `GET .../next` → `POST .../answer` 반복 — 짝수 인덱스는 정답,
  홀수 인덱스는 의도적 오답을 제출해 채점 로직이 무조건 참을 반환하지 않고
  실제로 오답도 정확히 `is_correct=False`로 판정함을 확인(20문항 정답,
  20문항 오답 → `result.correct=20/40`, `wrong_items` 20건).
- 출제된 40개 item_id가 phase18 marker 40건 집합과 정확히 1:1 일치(중복/누락 0건).
- `GET .../result`의 `pilot_info.pilot_source_version`이 정확히 파일럿 marker.

### 3-2. 테스트 2 — 비로그인 접근 차단

`GET /multiformat/play`(302→/login), `GET /pilot-availability`(302),
`POST /sessions {pilot_mode: true}`(302) — 전부 통과.

### 3-3. 테스트 3 — 비관리자 접근 차단

임시 비관리자 계정으로 위 3개 요청 전부 403 확인 후 계정 삭제. `require_admin`
(기존 `app/vocabulary_quiz/auth.py`)을 그대로 재사용했다(새 인증 로직 없음).

### 3-4. 테스트 4 — 제출 전 정답 비노출

40문항 전부 `GET .../next` 응답에서 `{correct_option, answer_text,
accepted_answers, answers}` 필드가 하나도 없음을 매 문항 실측 확인(기존
`_public_item_payload`를 그대로 재사용하므로 당연한 결과지만 실제로 확인함).

### 3-5. 테스트 5 — 일반 출제에 파일럿 40건이 섞이지 않는지(회귀)

- 일반 혼합모드 세션(`POST /sessions {"question_count": 10}`, `pilot_mode` 미지정)을
  20회 생성해 매 세션 최대 10문항씩 조회 — 총 71개 문항 관찰, 파일럿 marker
  item_id와 교집합 0건.
- 레벨모드 L4/L5 후보(`_select_level_candidates`)도 직접 호출해 파일럿
  item_id가 전혀 섞이지 않음을 재확인(phase18 보고서의 격리 설계가 이번
  변경 이후에도 그대로 유효함을 재검증).
- 기존 `SOURCE_VERSION="2.1.29"` 필터는 전혀 수정하지 않았으므로 이 결과는
  예상대로였다.

### 3-6. 테스트 6 — `tests/test_admin_level_quiz.py` 회귀

로컬 rnd.db 대상 재실행 결과: **71건 중 실패 0건**(변경 전과 동일 — 이번
변경이 기존 레벨별 출제 v1 기능에 회귀를 일으키지 않음을 확인).

---

## 4. 적재/DB 상태 재확인

이번 세션은 DB에 **아무 것도 쓰지 않았다**(코드/템플릿/테스트 파일만 추가·수정,
데이터 변경 없음). 이번 phase19 검증용으로 내려받은 research DB 사본에 대해서만
테스트가 임시로 쓰기(세션 생성, is_active/level_status 일시 변경 후 원복)를
했고, 종료 시 전부 원복·삭제했다. 최종 확인:

| 항목 | 결과 |
|---|---|
| research DB 사본 `vocabulary_multiformat_items` | 1,329건, 불변 |
| research DB 사본 `vocabulary_contents` | 5,820건, 불변 |
| research DB 사본 `vocabulary_content_levels` | 5,820건, 불변 |
| `student_exposure`/`public_ready` 합계 | 0/0 (불변) |
| 파일럿 20개 content_id의 `level_status` | 전부 `REVIEW_BOUNDARY` (불변, 원복 확인) |
| `vocabulary_contents`/`vocabulary_content_levels`/`vocabulary_multiformat_items`
  핵심 컬럼(활성상태/레벨상태/source_version) 테이블 해시 | 테스트 전후 완전 일치 |
| 테스트가 만든 세션/응답/계정 | 전부 삭제 확인(사전 존재하던 세션 8건/응답 62건은
  손대지 않고 그대로 남음 — 테스트가 만든 것이 아니라 원래 있던 실사용 이력) |
| 로컬 `data/vocab/vocabulary_quiz_rnd.db` | 이번 세션 내내 미접촉, 행 수 전부 불변
  (`vocabulary_contents`/`vocabulary_items`/`vocabulary_content_levels` 각 5,723,
  `vocabulary_review_samples` 500, `vocabulary_multiformat_items` 1,289) |
| research DB 사본 파일 | 검증 종료 후 로컬에서 삭제(산출물 아님) |
| git 작업 | 없음(commit/push 없음), 서버 배포 없음 |

---

## 5. 산출물

- 코드: `app/vocabulary_quiz/routers/multiformat.py`(파일럿 모드 함수/엔드포인트/
  분기 추가), `app/templates/vocabulary_quiz/multiformat_play.html`(파일럿 모드
  UI·JS 추가)
- 테스트: `tests/test_phase19_admin_pilot_mode.py`(신규, research DB 사본 대상
  51건 검증, 재실행 가능 — 사전에 `VOCABULARY_QUIZ_DB_PATH` 환경변수 또는
  스크립트 내 `DEFAULT_COPY_DB_PATH`에 phase18 데이터가 있는 DB 사본 경로를
  준비해야 함)
- 본 보고서: `reports/schema_reading_phase19_admin_pilot_mode_20260926.md`

---

## 6. 리스크 (우선순위 순)

1. **파일럿 40건은 여전히 관리자 검토용 초안**(전문가 최종 검수 미실시,
   phase16/17/18 리스크 그대로 유지) — 이번 세션은 "관리자가 정식 API로
   실제로 풀어볼 수 있게" 만든 것일 뿐, 콘텐츠 품질 검수는 다음 단계다.
2. **`tests/test_phase19_admin_pilot_mode.py`는 로컬 DB만으로는 실행할 수
   없다** — phase18 데이터가 적재된 DB 사본이 필요하다(3-0절). 사본을 준비하지
   않고 실행하면 스크립트가 사전조건 체크에서 즉시 실패하도록 만들어 뒀다
   (조용히 통과하지 않음).
3. **파일럿 모드는 `item_types`로 CROSSWORD를 요청하면 422로 거부**하지만,
   애초에 파일럿 배치에 CROSSWORD 문항이 없으므로(MEANING_CHOICE/CONTEXT_MEANING만)
   이 제약이 실질적으로 문제될 일은 없다 — 다만 향후 파일럿 배치에 다른
   유형이 추가되면 이 하드가드를 재검토해야 한다.
4. `session_result()`에 `pilot_info` 필드를 추가하면서 기존 응답 스키마에
   새 키가 하나 늘었다(`level_info`는 그대로, `pilot_info`가 그 외 세션에서는
   항상 `None`) — 기존 클라이언트가 응답 객체를 엄격한 스키마로 파싱하지
   않는 한(현재 템플릿 JS는 그렇지 않음) 영향 없음을 확인했다.
