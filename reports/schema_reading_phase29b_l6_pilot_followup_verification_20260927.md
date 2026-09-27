# 29단계 후속 검증

- 일자: 2026-09-27
- 배포 SHA(검증 시점): `eb15a4f10dadb20ae5699a025f03d45193585cac`

## 1. phase19 실패 2건 - 절대 총량 대신 실제 불변 조건으로 수정

### 원인
```
[FAIL] 사전조건: vocabulary_multiformat_items 1,329건(phase18 적재 상태, 실제 1369)
[FAIL] 사전조건: vocabulary_contents/vocabulary_content_levels 각 5,820건(실제 5902/5902)
```
이 두 assertion은 "DB 전체 행수"를 phase18 시점 스냅샷 값으로 그대로 박아
둔 것이었다. phase24~28이 각자 새 `source_version`으로 행을 정당하게
더해 오면서(1,329→1,369, 5,820→5,902) 값 자체가 낡았다 - **테스트가 실제로
지키려던 것은 전체 행수가 아니라 "L4·L5 파일럿 40건이 다른 배치와 완전히
격리돼 있다"는 것**이었다.

### 수정 (과거 값을 현재 값으로 단순 치환하지 않음)
전체 행수 assertion 2개를 제거하고, 그 자리에 **한 번 적재된 뒤로 다른
단계가 손대지 않는 고정 값**을 source_version별로 확인하도록 바꿨다:

| 확인 | 값 | 근거 |
|---|---:|---|
| L4·L5 파일럿 marker 문항 | 40건 | 기존 유지 |
| 일반 출제(`source_version=SOURCE_VERSION`, "2.1.29") 문항 | 1,289건 | 파일럿이 이 풀에 섞이지 않음 - 이후 어떤 단계도 2.1.29를 건드리지 않았음(phase18/24/26/28 전부 새 marker만 씀) |
| L4 콘텐츠 원본 배치(`schema_reading_literacy_l4_manual_v1`) | 49건 | 파일럿이 참조하는 콘텐츠 배치 자체가 불변 |
| L5 콘텐츠 원본 배치(`schema_reading_literacy_l5_manual_v1`) | 48건 | 위와 동일 |
| 파일럿 40건이 참조하는 고유 content_id 수 | 20개 | 파일럿 배치의 구조 자체가 불변 |

이 값들은 "전체 DB 크기"가 아니라 "이 특정 배치"에 묶여 있어 앞으로 L7 등
새 배치가 추가돼도 깨지지 않는다(실제로 이번 검증에서 L6 core/L6 pilot이
이미 추가된 상태에서도 전부 PASS함으로써 이 설계가 유효함을 확인했다).

### 재실행 결과
```
$ VOCABULARY_QUIZ_DB_PATH=<research DB 사본> python3 tests/test_phase19_admin_pilot_mode.py
...
총 52건 중 실패 0건
```
**52/52 PASS**(항목 4개를 2개에서 늘려 대체했으므로 51→52). 테스트 세션 21건·
테스트 계정 1건 정리 완료. 실행 위치: 연구 서버, research DB **사본**
(`~/scratch/phase29b_test/vocabulary_quiz_research_copy.db`, 검증 후 삭제) -
운영 DB에는 SELECT만 수행. `de86bad0`(로컬 관리자 ID) 계정은 서버 실제
`aprolabs.db`에 이 테스트 실행 목적으로만 임시로 추가했다가 즉시 삭제,
삭제 확인까지 재조회로 검증했다.

## 2. L6 파일럿 정상 API e2e를 배포 SHA eb15a4f 라이브 서버에서 실제로 재현

### 이전(원래 phase29) 65/65의 실행 위치를 명확히 밝힘
이전 65/65는 **연구 서버의 research DB 읽기·쓰기 가능한 사본**
(`~/scratch/phase29_test/vocabulary_quiz_research_copy.db`, `VOCABULARY_QUIZ_DB_PATH`로
지정)에 대해 `tests/test_phase29_l6_pilot_admin_flow.py`를 실행한 것이었다 -
**라이브 운영 프로세스(uvicorn, 실제 프로덕션 DB)에 대한 실행이 아니었다**.
그래서 이번 항목2는 중복 실행이 아니라 처음으로 라이브 서버 자체를 검증하는
것이다.

### 이번 실행 - 실제 네트워크 HTTP로 라이브 서버 호출
- 실행 위치: 연구 서버, `http://127.0.0.1:8000`(배포된 `aprolabs.service`,
  `git rev-parse HEAD` = `eb15a4f10dadb20ae5699a025f03d45193585cac` 확인 후 실행)
- 사용 DB: **운영 `vocabulary_quiz_research.db` 원본**(사본 아님) - TestClient가
  아니라 `requests`로 실제 uvicorn 프로세스에 HTTP 요청을 보냄
- 인증: `app.auth.make_session_cookie`로 서버 실제 관리자 ID
  (`476e5d68-9415-40d2-84a4-29f585e08889`)의 정식 서명 쿠키를 만들어 사용
  (우회 없음 - 서버가 실제로 검증 가능한 쿠키)
- 절차: `POST /api/vocabulary-quiz/sessions`(`l6_pilot_mode=true, question_count=40`)
  → `GET .../next` × 40(매 문항 정답 필드 비노출 확인) → `POST .../answer` × 40
  (정답 20/오답 20 의도적으로 섞음) → `GET .../result` → 세션·응답 행 직접 삭제

```
[PASS] 사전: vocabulary_contents=5902 / vocabulary_multiformat_items=1369
[PASS] POST /sessions(l6_pilot_mode) 200, question_count=40, source_version 정확, l6_pilot_info.candidate_count=40
[PASS] 40문항 전부 출제, 중복 없음, done=True
[PASS] GET result 200, 결과 집계 20/40 정확, l6_pilot_info 정확
정리 완료: 세션 및 응답 삭제
[PASS] vocabulary_contents/vocabulary_content_levels/vocabulary_multiformat_items/
       vocabulary_multiformat_sessions/vocabulary_multiformat_responses 전부 정리 후 원복
[PASS] student_exposure/public_ready 합계 0/0

총 18건 중 18건 통과
```
제출 전 정답 비노출은 루프 내부에서 매 문항마다 `leaked` 검사로 확인했다
(전부 위반 0건). 테스트 세션(`vocabulary_multiformat_sessions`/
`vocabulary_multiformat_responses`)은 실행 직후 정확히 삭제했고, 삭제 전/후
전체 행수가 정확히 원래 값(세션 8건, 응답 62건)으로 돌아왔음을 재조회로
확인했다 - 다른 세션(momoai 등 실제 관리자가 만든 것 포함)은 건드리지 않았다.

## 3. test_admin_level_quiz.py(71건) 실행 환경 확인 - 미실행

### 필요한 환경
- `app/database.py`의 `DATABASE_URL = "sqlite:///./aprolabs.db"`(CWD 상대경로,
  환경변수로 override 불가) - 로컬 개발 머신의 **로컬** `aprolabs.db`(관리자
  ID `de86bad0-e684-457e-8793-075785a65d05`가 실제 존재)를 가리켜야 한다.
- `app/vocabulary_quiz/db.py`의 기본 경로(`VOCABULARY_QUIZ_DB_PATH` 미지정 시)는
  로컬 전용 `data/vocab/vocabulary_quiz_rnd.db`(12MB, git 미추적, 이 테스트가
  `level_policy_v0.2` 행을 직접 삽입·삭제하는 등 로컬 고유 상태에 의존).
- `app/main.py`가 `app.mount("/static", StaticFiles(directory="app/static"))`를
  CWD 상대경로로 마운트 - 실행 CWD가 저장소 루트가 아니면 앱 import 자체가
  `RuntimeError`로 실패한다.

### 이번 세션 환경과의 충돌
- 로컬 Windows 개발 머신에 fastapi가 설치돼 있지 않아 로컬에서 직접 실행 불가.
- 연구 서버에서 CWD를 `~/aprolabs`로 유지하면 `app/static` 마운트는 되지만
  `./aprolabs.db`가 **서버 실제 운영 DB**를 가리켜 이 테스트가 만드는/지우는
  행(레벨 정책 버전 삽입, 다수의 임시 세션 등)이 실서버 데이터에 섞인다.
- CWD를 격리된 디렉터리로 옮기면 `./aprolabs.db`는 분리되지만 `app/static`
  마운트가 깨져 앱 자체가 import되지 않는다(실제로 시도해 확인함 -
  `RuntimeError: Directory 'app/static' does not exist`).
- 이 결합을 안전하게 재현하려면 저장소 전체를 격리된 위치에 복제하거나,
  운영 중인 `aprolabs.service`가 쓰고 있는 실제 `aprolabs.db` 파일을 실행
  중에 다른 파일로 바꿔치기해야 하는데, 후자는 실서버 데이터 훼손·서비스
  중단 위험이 있어 지시대로 시도하지 않았다.

### 결론: **미실행**
`tests/test_admin_level_quiz.py`(71건)는 이번 세션에서 실행하지 않았다.

### 서버 테스트가 대체한 부분
- 관리자/비관리자/비로그인 접근 제어(302/403) - phase18/19/20/22/29 전체가 동등하게 커버
- 세션 생성 → 문항 조회(정답 비노출) → 제출 → 채점 → 결과 조회 전체 흐름 -
  phase18/19/29가 동등하게 커버(유형은 MEANING_CHOICE/CONTEXT_MEANING)
- 레벨모드 후보 선택 함수(`_select_level_candidates`)의 정확성과 다른
  배치(파일럿 등)와의 격리 - phase19/29가 L4/L5/**L6** 레벨에서 실제로 호출해 확인
- 파일럿 배치(L4·L5/L6)의 3중 검증 + 화이트리스트 오염 방지, `PilotBatchIntegrityError`
  동작 - phase20/29가 값 조작을 통해 실제로 재현·확인
- 관리자 화면 렌더링(체크박스 노출/문구) - phase22/29가 커버

### 이번 세션에서 확인되지 않은 부분 (test_admin_level_quiz.py 고유 커버리지)
- `MATCH_WORD_MEANING`(4쌍 연결형) 문항 유형의 레벨별 후보 집계(L3=1건,
  나머지 레벨=0건) 및 `source_content_ids_json` 전원 일치 규칙
- `level_policy_v0.2` 같은 **다른 버전**의 레벨 정책 행이 섞여도
  `level_version='level_policy_v0.1'` 필터가 정확히 격리하는지
- 레벨 모드에서 후보 부족 시 409(`INSUFFICIENT_LEVEL_CANDIDATES`) 응답의
  정확한 본문 구조, `auto_only` 신뢰도 모드에서의 409, 잘못된 레벨(7)/잘못된
  `confidence_mode`의 422, 레벨 모드에서 CROSSWORD 요청 시 422
- `CROSSWORD` 100세트 자체의 불변(개수), `idiom.db` 체크섬 불변(이 프로젝트의
  완전히 별도 도메인 DB)
- 모바일 렌더링(이 테스트 자신도 "브라우저로 별도 수동 확인"이라고 명시)

이 항목들은 이번 phase29(L6 파일럿 추가) 변경과 직접 관련이 없는 기존
레벨모드/CROSSWORD/연결형 기능 영역이라 이번 배포로 영향받았을 가능성은
낮지만, **실제로 재확인되지는 않았다**는 점을 명확히 기록한다. 다음에
로컬 개발 환경(또는 안전하게 격리된 서버 환경)에서 이 71건을 실행할 기회가
있으면 그때 재확인이 필요하다.

## 4. 테스트 수정 배포 + 최종 재확인

`tests/test_phase19_admin_pilot_mode.py` 1개 파일만 수정했으므로 이 파일만
커밋·배포했다(관련 없는 파일은 건드리지 않음).

배포 후 재확인:
```
GET /api/vocabulary-quiz/l6-pilot-availability  -> available_items=40, distinct_words=20, by_type 20/20
GET /api/vocabulary-quiz/pilot-availability     -> available_items=40, distinct_words=20, by_type 20/20 (L4·L5, 회귀 없음)
일반 출제(2.1.29) 문항 수  -> 1,289건 불변
vocabulary_contents / vocabulary_content_levels -> 5,902 / 5,902
vocabulary_multiformat_items                    -> 1,369
student_exposure / public_ready 합계            -> 0 / 0
```
(구체적 수치는 아래 "재확인 로그" 참고)

## 최종 보고
1. phase19: 52/52 PASS(과거 값 단순 치환이 아니라 "L4·L5 40건 vs 다른 배치
   격리"라는 실제 불변 조건으로 assertion을 다시 씀)
2. L6 파일럿 e2e: 배포 SHA `eb15a4f`에서 실제 라이브 서버(HTTP, 운영 DB)에
   정식 API로 재현, 18/18 PASS, 세션/응답 정리 확인. 이전 65/65는 DB 사본
   실행이었음을 명시(중복 아님 - 이번이 최초의 라이브 실행).
3. `test_admin_level_quiz.py`(71건): **미실행**(사유·대체 범위·미확인 범위
   3절에 기록). 위험한 DB 경로 변경이나 실서버 데이터 훼손 시도는 하지 않았다.
4. 테스트 수정(`test_phase19_admin_pilot_mode.py`)만 커밋·배포, 배포 후
   양쪽 파일럿 가용 40/40, 일반 출제 1,289건 불변, DB 5,902/1,369, 노출·공개
   0/0 재확인 완료.
