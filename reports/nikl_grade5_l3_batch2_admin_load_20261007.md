# L3 중등 보강 2차(38콘텐츠·76문항) - 관리자 검수용 비공개 적재 + 배포 완료

- 일자: 2026-10-07
- 범위: (1) 산출물·대상 ID·해시 고정 + 적용 직전 재검증, (2) 별도
  source_version + git 추적 매니페스트로 연구 DB 비공개 적재(GATE 1~11),
  (3) 레벨 판정과 검수 상태 분리, (4) 기존 L3 검수·응시 기능을 배치
  단위로 일반화(라우터·테이블 복제 없음)해 2차 추가, (5) 실제 API
  경로로 전수 검증(관리자 서명 쿠키, 격리된 DB 사본), (6) 1차 승인본·
  기존 데이터 불변 확인 + 배포.

## 1. 산출물·대상 ID·해시 고정 + 적용 직전 재검증

완료된 후보 선정 조사(이전 턴의 잔여 40건 재구성·38건 콘텐츠 작성)는
반복하지 않고, 그 결과물만 최종 고정했다:

- `data/vocab/nikl_grade5_l3_batch2_manifest_v1.json`(76문항, DRAFT 접미사
  제거)
- `data/import/nikl_grade5_l3_batch2_final_content_20261007.csv`(38콘텐츠,
  `content_id`=`candidate_id` 그대로 재사용 - 1차와 동일 원칙)
- `data/import/nikl_grade5_l3_batch2_version_manifest_20261007.json`
  (SHA-256: content=`a9ceefdd...`, manifest=`2d3af9a8...`)

**적용 직전 재확인**(연구 DB 라이브, 읽기 전용):

| 확인 항목 | 결과 |
|---|---|
| 38건 전부 최신 사람 판정 여전히 L3인가 | **38/38 PASS**(판정 없음 0, L3 아님 0) |
| 기존 DB 전체(`vocabulary_contents`)와 표제어 중복 | **0건** |
| candidate_id가 이미 콘텐츠로 존재(재적재 위험) | **0건** |

## 2. 연구 DB 비공개 적재 (GATE 1~11)

`scripts/vocab/apply_grade5_l3_batch2.py` - 1차(`apply_grade5_l3_batch1.py`)와
동일한 GATE 패턴(환경 가드 → SQLite Backup API 백업 → dry-run → 단일
트랜잭션 적재 → 멱등성 확인)에 **GATE 10에 1차 배치 콘텐츠·문항 수
불변 확인을 추가**했다.

| 단계 | 결과 |
|---|---|
| GATE 1~2(환경·경로 가드) | PASS |
| GATE 4(백업) | `vocabulary_quiz_research.db.bak_grade5_l3_batch2_20261006-162047`, integrity=ok |
| GATE 5(입력 해시) | content/manifest 둘 다 로컬 계산값과 바이트 단위 일치 |
| GATE 7(신규/스킵/충돌) | 콘텐츠 신규 38·스킵 0·충돌 0, 문항 신규 76·스킵 0·충돌 0 |
| GATE 8(단일 트랜잭션 커밋) | PASS |
| GATE 9(integrity/FK) | integrity_check=ok, FK 위반 0건 |
| GATE 10(무관 데이터+1차 배치 불변) | RULE_A/B 1408→1408, content_levels 5950→5950, **1차 콘텐츠 30→30, 1차 문항 118→118** |
| GATE 11(비공개) | 적재 38건 전부 student_exposure=0/public_ready=0 |
| 멱등성(재실행) | 신규 0·스킵 38/76·충돌 0 |

**독립 재확인**(적재 스크립트 자체의 보고와 별도로 다시 조회):

| 항목 | 값 |
|---|---:|
| `vocabulary_contents` 총행수 | **6,018**(기존 5,980 + 신규 38) |
| `vocabulary_multiformat_items` 총행수 | **1,747**(기존 1,671 + 신규 76) |
| `vocabulary_multiformat_items` 활성(is_active=1) | **1,689**(기존 1,613 + 신규 76) |
| 2차 콘텐츠 | 38건, 전부 student_exposure=0/public_ready=0, 표제어 38개 고유 |
| 1차 콘텐츠·문항(완전 불변 재확인) | 30건 / 118행(활성 60) - 적재 전후 바이트 단위 동일 |
| `vocabulary_publish_reviews`(append-only) | 107행(1차 전체 재승인분 포함, 이번 적재로 변경 없음) |
| integrity_check | ok |

## 3. 레벨 판정과 검수 상태 분리, 신규 검수 판정 공란

1차와 동일한 설계: 이 38건은 레벨 정책 파이프라인(RULE_A/B·공식등급
매칭)을 거친 적 없는 신규 어휘라 `vocabulary_content_levels`에 행을
쓰지 않는다(레벨 미확정을 가짜 행으로 감추지 않음). 콘텐츠·문항 검수
판정(`vocabulary_publish_reviews`)은 **이 화면을 통해 사람이 실제로
버튼을 눌러야만** 생성되며, 적재 스크립트는 이 테이블에 아무것도
쓰지 않는다(2절 GATE 로그에 이 테이블 관련 INSERT가 없음 - 검수 전
공란 그대로).

## 4. 기존 L3 검수·응시 기능을 배치 단위로 일반화(복제 없음)

**검수 화면**(`app/vocabulary_quiz/grade5_l3_batch1_review.py` +
`routers/grade5_l3_batch1_review.py`): 배치별 차이(source_version·보류
content_id·보류 사유)만 `BATCHES` 레지스트리에 등록, 나머지 로직(해시
비교·신선도 판정·검수 저장·다음 미검수 이동)은 전부 공유. URL은 1차가
쓰던 그대로(`/vocab-grade5-l3-batch1-review/`) - 목록은 `?batch=batch2`
쿼리로 전환, 상세/판정 저장은 content_id가 전역 유일이므로 경로 변경
없이 콘텐츠의 source_version에서 배치를 내부적으로 판별한다.
`vocabulary_publish_reviews` 테이블도 그대로 공유(배치별 테이블 분리
없음).

**응시(퀴즈 풀이) 기능**(`app/vocabulary_quiz/routers/multiformat.py`):
기존에는 매 파일럿/배치마다 매니페스트 로딩·화이트리스트 검증·가용량
조회·세션 생성 함수 4벌을 거의 그대로 복제해 왔던 패턴을, 이번부터는
`L3_BATCH_CONFIGS` 레지스트리 + `batch_id`를 받는 공용 함수
(`_load_l3_batch_manifest_rows`/`_select_l3_batch_item_ids`/
`_l3_batch_availability`/`_create_l3_batch_session`)로 바꿨다. 기존
`grade5_l3_batch1_*` 이름의 함수·엔드포인트·요청 필드는 전부 이 공용
함수를 `batch_id="batch1"`로 호출하는 얇은 래퍼로 남겨 **하위 호환
100% 유지**(기존 코드/프런트엔드 변경 없이 그대로 동작).
`/grade5-l3-batch1-availability`는 `?batch=` 쿼리로, 세션 생성은 새
`grade5_l3_batch2_mode` 요청 필드로 배치를 선택한다. 결과 조회 응답에
`grade5_l3_batch2_info` 필드를 추가(기존 `grade5_l3_batch1_info` 등
다른 필드는 그대로).

**검수 화면에서만 관리자용 근거 노출**: `qa_flags_json`의
`wrong_option_reasons`/`key_clue`는 검수 상세 화면 템플릿에서만
렌더링한다(1차와 동일 - 응시 화면 서빙 코드는 `_public_item_payload()`류
기존 mode-agnostic 코드를 그대로 재사용하며 이번에 손대지 않음).

## 5. 실제 API 경로 전수 검증(관리자 서명 쿠키, 격리된 DB 사본)

**방법**: 연구 DB를 `/tmp`에 **복사**하고(`VOCABULARY_QUIZ_DB_PATH`로
지정) 그 사본에 대해 FastAPI `TestClient` + 실제 관리자 계정
(`admin@aprolabs.co.kr`)의 **진짜 서명된 세션 쿠키**(`app.auth.
make_session_cookie()`로 생성 - 비밀번호 입력 단계만 건너뛸 뿐
`require_admin`의 `is_admin` DB 확인은 그대로 통과해야 함, 1차 때와
동일한 방식)로 35개 항목을 실제 HTTP 요청으로 검증했다. **302 확인만으로
끝내지 않고**, 로그인 이후의 실제 기능(세션 생성·문항 조회·채점·결과·
검수 저장)까지 전부 확인했다.

| 분류 | 검증 항목 | 결과 |
|---|---|---|
| 접근제어 | 비로그인 시 play_page/availability/검수목록 → 302 | PASS |
| 배치 격리 | availability(batch2)=76문항/38어휘, availability(batch1, 기본값)=58문항/29어휘(1차 불변) | PASS |
| 입력 검증 | 알 수 없는 batch → 404, 1차+2차 동시선택 → 422, batch2+CROSSWORD → 422 | PASS |
| 세션 생성 | batch2 세션 200, source_version 일치 | PASS |
| 정답 비노출 | `/next` 응답 전체에 `correct_option`/`wrong_option_reasons`/`key_clue`/`answer_payload` 문자열 **0건**(5문항 전수 검사) | PASS |
| 응답·채점 | 5문항 전부 정상 채점 | PASS |
| 결과 조회 | `grade5_l3_batch2_info`에만 값, `grade5_l3_batch1_info`/다른 파일럿 필드 전부 None(교차오염 없음) | PASS |
| 1차 회귀 확인 | batch1 세션 생성·조회 정상(2차 추가가 1차에 영향 없음) | PASS |
| 검수 목록 | batch2(40건=38+보류2)·batch1(기본값, 회귀) 둘 다 200 | PASS |
| 검수 상세 | 일반 콘텐츠 상세에 관리자 근거(key_clue) 노출 | PASS |
| 검수 판정 저장 | POST → 303, 다음 미검수 항목(batch2 내)으로 리다이렉트, DB에 판정 실제 저장 확인 | PASS |

**총 35/35 PASS**(최초 실행에서 3건 FAIL 발견 → 전부 실제 수정 후 재검증
PASS, 아래 6절).

**테스트 기록 정리**: 전부 `/tmp`의 **격리된 DB 사본**에서만 수행했고
(라이브 DB에는 연결조차 하지 않음), 테스트 종료 후 사본을 삭제했다.
라이브 연구 DB를 독립적으로 재조회해 테스트 판정(`rationale LIKE
'%LIVE_API_TEST%'`) **0건**, 테스트 세션(batch2 source_version)
**0건**임을 재확인했다(정리할 테스트 기록 자체가 없음).

**배포 후 라이브(프로덕션) 확인**: 라이브 DB에는 테스트 세션/판정을
남기지 않는다는 원칙에 따라 익명 상태로만 확인했다 - play_page/
availability(batch2)/검수목록(batch2·batch1) 전부 **302**(로그인
리디렉션, 500/404 없음).

## 6. 실제 테스트로 발견·수정한 결함 2종(투명성 기록)

1차 테스트 실행에서 FAIL 3건이 나왔고, 그중 1건은 테스트 코드 자체의
기대값 오류(1차 가용 문항수를 구 버전 60으로 잘못 기대 - 실제로는 v2
개정 이후 58이 맞음), 나머지 2건은 **실제 코드 결함**이었다:

- **아멘(G5-b5364a7010af6ca0) 검수 상세 페이지 404**: 1차 '벨기에'는
  이미 DB에 콘텐츠가 있던 상태에서 나중에 오답 개정만 보류했지만, 2차의
  아멘·파키스탄은 **콘텐츠 자체를 처음부터 작성하지 않아** DB에 행이
  없다. 그래서 목록에 안 보이고 상세 페이지는 404였다 - "보류 사유를
  보존한다"는 요구와 달리 실제 검수 화면에서는 그 사유를 볼 방법이
  없었다.
- **수정**: `BATCHES` 레지스트리에 `unloaded_held_lemmas`(content_id→
  lemma)를 추가, 목록 라우트가 DB 행이 없어도 이 정보로 "보류 - 콘텐츠
  미작성" 행과 보류 사유를 함께 보여주도록 수정(상세 페이지 링크는
  없음 - 검토할 콘텐츠 자체가 없으므로). 재배포 후 재테스트 35/35 PASS.

## 7. 배포

| 커밋 | 내용 |
|---|---|
| `26c8f6b` | 2차 산출물 고정 + 적재 스크립트 + 검수·응시 기능 배치 일반화 |
| `769207d` | 실제 API 테스트로 발견한 보류 항목 404 수정 |
| `dc0aaf1` | 사이드바 메뉴 라벨을 1차·2차 공용으로 갱신(링크 동일) |

GitHub Actions 3회 모두 성공(`git reset --hard origin/main` + 서비스
재시작), 서비스 `active` 상태 확인. 연구 DB 적재는 배포 완료 후
서버에서 직접 `apply_grade5_l3_batch2.py --apply` 실행(로컬에서 만든
파일을 그대로 올려치지 않음 - 1차와 동일 원칙).

## 8. 실제 검수 URL · 메뉴 위치

- **콘텐츠·문항 검수**: `https://aprolabs.co.kr/vocab-grade5-l3-batch1-review/?batch=batch2`
  (1차는 `?batch=batch2` 없이 기본값, 화면 상단 "1차/2차" 탭으로도 전환 가능)
- **관리자 응시(다유형 퀴즈)**: `https://aprolabs.co.kr/vocabulary-quiz/multiformat/play`
  → "L3 중등 보강 2차 문항만 출제" 체크박스(1차 체크박스는 그대로 유지)
- **메뉴 위치**: 좌측 사이드바 "📘 문해력 · 어휘" → "🌱 L3 중등 보강
  검수(1차·2차)"(기존 메뉴 항목 1개 그대로, 신규 메뉴 추가 없음 - 지시대로
  배치마다 메뉴를 새로 만들지 않음)

## 9. 변경받지 않은 것(재확인)

- 1차 승인본 29어휘·58문항(및 벨기에 보류): 콘텐츠·문항·검수 이력
  바이트 단위로 불변(2·5절 GATE 10 + 독립 재확인 + 라이브 회귀 테스트
  3중 확인)
- 기존 레벨 테이블(5,950건)·RULE_A/B(1,408건): 불변
- `student_exposure`/`public_ready`: 신규 38건 전부 0, 기존 데이터 전부
  불변
- RULE_A/B 판정 로직·momolib 코드: 전혀 열어보지 않음
- 일반 출제(`source_version=2.1.29`)·L4/L5/L6 파일럿·L0~L3 확장
  미리보기: 코드 경로 변경 없음(공용 함수로 일반화한 것은 L3 배치
  섹션뿐)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
