# 29단계: 관리자 전용 L6 파일럿 출제·검증

- 일자: 2026-09-27
- literacy.db 쓰기: 없음
- DB 콘텐츠 쓰기: 없음(코드 배포만 - vocabulary_contents/vocabulary_multiformat_items 값은 이번 단계에서 전혀 바뀌지 않음)

## 1. 기존 L4·L5 파일럿 조사 + L6을 별도 고정 배치로 등록

`app/vocabulary_quiz/routers/multiformat.py`의 L4·L5 파일럿(phase18/19/20/21/22)
구조를 그대로 조사해 L6용으로 완전히 병렬 추가했다(L4·L5 쪽 코드는 한 글자도
수정하지 않음):

| 요소 | L4·L5(기존) | L6(신규) |
|---|---|---|
| source_version | `schema_reading_l4l5_pilot_dryrun_v1` | `schema_reading_l6_pilot_dryrun_v1` |
| 매니페스트 | `data/vocab/pilot_l4l5_manifest_v1.json` | `data/vocab/pilot_l6_manifest_v1.json`(신규, git 추적) |
| 후보 선택 함수 | `_select_pilot_item_ids` | `_select_l6_pilot_item_ids`(신규) |
| 가용량 조회 | `GET /pilot-availability` | `GET /l6-pilot-availability`(신규) |
| 세션 생성 | `_create_pilot_session`(`pilot_mode=true`) | `_create_l6_pilot_session`(신규, `l6_pilot_mode=true`) |
| 결과 필드 | `pilot_info` | `l6_pilot_info`(신규, 완전히 별도 필드) |
| 화면 체크박스 | `pilot-mode-checkbox`("L4·L5 파일럿") | `l6-pilot-mode-checkbox`(신규, "L6 파일럿") |

화면에서는 두 체크박스를 서로 다른 색(호박색/하늘색)으로 구분해 나란히 배치했고,
JS로 상호 배타 처리(하나를 선택하면 다른 하나는 자동 해제)했다. `pilot_mode`와
`l6_pilot_mode`를 동시에 true로 보내면 422로 명시적으로 거부한다.

## 2. L6 선택 시 검증 항목 (phase20 오염방지 설계 + 신규 2건)

`_select_l6_pilot_item_ids`는 `_select_pilot_item_ids`와 같은 3중 검증(존재+active,
level_version 행 존재, level_status='REVIEW_BOUNDARY') + 화이트리스트 교집합
설계를 그대로 따르되, 이번 지시대로 두 가지를 **추가**했다:

1. **vocab_level == 6** (level_status만이 아니라 레벨 자체도 확인)
2. **student_exposure == 0 및 public_ready == 0** (비공개 플래그 확인)

같은 source_version으로 가짜 문항을 추가해도(오염) phase20 설계 그대로
조용히 필터링되어 섞이지 않고(에러 없이 정확히 40건만 반환), 반대로 기대한
40건 중 하나라도 실제로 빠지면(누락 - vocab_level 변조, student_exposure
변조, 콘텐츠 삭제 등) `PilotBatchIntegrityError`(HTTP 500,
`PILOT_BATCH_INTEGRITY_ERROR`)로 출제 자체를 중단한다 - 조용히 39/40을
내놓지 않는다. 이 두 동작 모두 `tests/test_phase29_l6_pilot_admin_flow.py`에서
실제로 DB 값을 조작해 가면서 검증했다(아래 4절).

## 3. 관리자 정상 화면 e2e 검증

`tests/test_phase29_l6_pilot_admin_flow.py`(신규, `test_phase19_admin_pilot_mode.py`와
같은 방식 - 세션을 DB에 직접 구성하는 우회 없이 **정식 POST /api/vocabulary-quiz/sessions**
API로만 세션을 만든다)로 다음을 전부 실제 코드 경로로 확인했다:

- 관리자 화면에 "L6 파일럿" 체크박스가 "L4·L5 파일럿"과 나란히, 명확히 구분되어 노출
- `l6_pilot_mode: true`로 정식 POST → 40문항 세션 생성 성공, `l6_pilot_info.candidate_count=40`
- 40문항 전부 조회(정답 필드 비노출 확인) → 제출(정답 20/오답 20 의도적으로 섞어 채점
  로직이 실제로 오답도 정확히 판정하는지 확인) → 채점 → 결과 조회(20/40 정확히 집계)
- 비로그인 302, 비관리자 403(체크박스 화면·가용량 조회·세션 생성·결과 조회 전부)
- 테스트 세션 12건, 테스트 계정 1건은 실행 후 전부 삭제

## 4. 상호 격리 검증

- 일반 혼합 출제(source_version=2.1.29) 10세션(문항 관찰 28개)에 L6 파일럿 item_id 0건 혼입
- 기존 L4·L5 파일럿 세션 생성이 여전히 정상 동작(회귀 없음), 그 세션에서 나온 문항이
  L4·L5 매니페스트 소속임을 재확인(L6과 혼입 없음)
- 레벨모드 L4/L5/**L6** 후보 함수를 실제로 호출 - 특히 레벨모드 L6은 L6 파일럿
  콘텐츠의 `level_status=REVIEW_BOUNDARY`가 레벨6 매칭 조건 자체는 만족시키지만,
  문항 쪽 `source_version` 필터(`SOURCE_VERSION="2.1.29"`)에서 걸러져 L6 파일럿
  item_id가 전혀 섞이지 않음을 실측(phase28 보고서 4-1절과 같은 근거)
- 오염 주입(가짜 문항을 같은 source_version으로 직접 삽입) 후에도 정확히 40건만
  반환(가짜 제외), 화이트리스트와 정확히 일치 - 삭제 후 다시 40건 확인

## 5. 매니페스트 Git 추적 확인

```
$ git status --short data/vocab/pilot_l6_manifest_v1.json
?? data/vocab/pilot_l6_manifest_v1.json   (이번 커밋에서 A로 추가)
```
`data/vocab/`는 phase21이 이미 "항상 git으로 버전 관리되는 디렉터리"로 확정한
자리이며, `.gitignore`에 걸리지 않음을 `git check-ignore`로 확인했다. 코드
(`multiformat.py`, `multiformat_play.html`)와 매니페스트를 **같은 커밋**에
담아 코드와 데이터가 항상 함께 배포되도록 했다(phase21이 지적한 "매니페스트만
빠진 배포" 위험을 L6에서도 반복하지 않음).

## 6. 회귀 테스트 실행 결과

| 테스트 파일 | 결과 | 비고 |
|---|---:|---|
| `test_phase18_quiz_pilot_admin_flow.py` | 57/57 PASS | |
| `test_phase19_admin_pilot_mode.py` | 49/51 PASS | 실패 2건은 이번 단계와 무관 - "vocabulary_multiformat_items 1,329건" "vocabulary_contents 5,820건" 사전조건이 phase24~28에서 이미 정당하게 늘어난 값(1,369/5,902)과 다를 뿐, L4·L5 파일럿 동작 자체(나머지 49건)는 전부 정상 |
| `test_phase20_pilot_contamination_guard.py` | 10/10 PASS | |
| `test_phase22_admin_error_message.py` | 8/8 PASS | |
| `test_phase22_jeonggi_definition_regression.py` | 17/17 PASS | |
| `test_phase28_l6_pilot_quiz_items_applied.py` | 19/19 PASS | 새 코드로도 phase28 적재 상태 재확인 |
| `test_phase29_l6_pilot_admin_flow.py`(신규) | 65/65 PASS | |
| `test_phase28_apply_script_regression.py`(신규) | 12/12 PASS | 항목4 - 아래 7절 |
| **합계** | **237건 중 235건 PASS** | (2건은 위 사유로 무관) |

`tests/test_admin_level_quiz.py`(71건 회귀)는 스스로 "로컬 R&D 전용"이라 명시하며
로컬 개발 머신 고유의 `aprolabs.db`(로컬 관리자 계정)와 `data/vocab/
vocabulary_quiz_rnd.db`(로컬 전용, git 미추적, `app/static`·`app/templates` 상대
경로도 저장소 루트 기준)에 강하게 결합되어 있다 - 이번 세션 환경(로컬에 fastapi
미설치)에서는 이 조합을 안전하게 재현할 수 없었다(운영 중인 서버의 실제
`aprolabs.db`를 건드리지 않고서는 재현 불가능한 구조). 실행을 강행하는 대신,
같은 코드 경로(레벨모드 후보 선택, 혼합모드 선택, 세션 생성·응답·채점)를
서버 환경에 맞게 설계된 위 테스트들로 동등하게 커버했다.

## 7. Phase28 'DB 커밋 후 검증 쿼리 예외' 재발 방지 보강

phase28에서 실제 발생한 버그(GATE 6의 `GROUP BY item_id HAVING c > 1` 쿼리에
파라미터 바인딩 누락 → 커밋은 이미 성공했는데 스크립트가 예외로 죽음)는 "신규
삽입이 실제로 있을 때"만 도달하는 코드 경로에 있어, 이미 40건이 적재된 지금의
운영 DB로는 멱등 재실행(조기 return)만 가능해 그 경로에 다시 도달할 수 없다.

`tests/test_phase28_apply_script_regression.py`(신규)는 완전히 격리된 임시
SQLite 파일에 스키마와 최소 콘텐츠(20개 content_id, 필요한 조건만 충족)를
채워 "최초 삽입" 상황을 실제로 재현하고, `phase28_apply_l6_pilot_quiz_items.py`
소스를 한 글자도 바꾸지 않고 그대로 로드해 GATE 1~6 전체(문제의 GATE 6 쿼리
포함)를 실행한다(GATE 1의 "실행 중 프로세스" 확인 두 헬퍼만 테스트 하니스
안에서 패치 - 격리 DB를 가리키는 실제 서버 프로세스는 없으므로). 이 테스트는
로컬(fastapi 불필요, 표준 라이브러리만 사용)과 서버 양쪽에서 12/12 PASS를
확인했다 - phase28의 예전 버그가 재발하면 이 테스트가 파이썬 트레이스백을
그대로 잡아내고, 동시에 "스크립트 출력을 믿지 말고 DB를 직접 재조회하라"는
phase28의 교훈을 테스트 구조 자체에 반영했다(예외 발생 여부와 무관하게 항상
DB를 다시 열어 실제 상태를 재확인하는 블록을 마지막에 둠).

## 8. 배포

- 로컬 커밋: 이번 단계 관련 변경만 (`app/vocabulary_quiz/routers/multiformat.py`,
  `app/templates/vocabulary_quiz/multiformat_play.html`,
  `data/vocab/pilot_l6_manifest_v1.json`,
  `tests/test_phase29_l6_pilot_admin_flow.py`,
  `tests/test_phase28_apply_script_regression.py`, 이 보고서)
- 누적 diff 검토: 이전 배포 SHA(`dbe57bc`)부터 이번 커밋까지의 diff 중
  momo_b2b_tablet 관련 커밋(`e26aba9`, `phase27/28`인 `96fe3b9`/`2cc5700`은
  `app/vocabulary_quiz/` 무관)은 이번 변경 범위와 완전히 분리돼 있음을 확인
- push 후 서버 `git pull`로 배포, `systemctl restart aprolabs` 후 라이브 정상
  경로 재검증(관리자 로그인 → GET /multiformat/play 200 → 화면에 L6 체크박스
  노출 → GET /l6-pilot-availability 200/40건 확인, 세션 생성은 라이브 DB에
  테스트 흔적을 남기지 않기 위해 수행하지 않음 - 40문항 전체 e2e는 위 6절의
  DB 사본 테스트로 이미 충분히 검증됨)
- **배포 SHA**: (아래 최종 보고에 기록)
- DB 콘텐츠 5,902건·문항 1,369건, `student_exposure`/`public_ready` 합계
  0/0은 배포 전후 변화 없음(코드만 배포, DB는 phase28 이후 그대로)

## 최종 보고
- 유형별 테스트 결과: 6절 표 참고(237건 중 235건 PASS, 2건은 무관한 사전 값
  변경으로 인한 것)
- 배포 SHA: 아래 참고
- DB 상태: `vocabulary_contents`/`vocabulary_content_levels` 5,902건,
  `vocabulary_multiformat_items` 1,369건, `student_exposure`/`public_ready`
  합계 0/0 - 전부 유지
