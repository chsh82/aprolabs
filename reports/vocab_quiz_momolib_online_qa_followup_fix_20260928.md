# momolib 어휘 퀴즈 온라인 QA 후속 수정 완료

- 일자: 2026-09-28
- momolib: `origin/main` = `97a13c1` (자동배포 확인)
- aprolabs: `e665434`

## 1. 조사 오류 추적 및 수정

### 원인 추적 결과

| 위치 | 확인 결과 |
|---|---|
| 생성 스크립트 `scripts/vocab/phase16_build_quiz_pilot_dryrun.py:129` | **원인.** MEANING_CHOICE `explanation`을 만들 때 `eun_neun()`(은/는)은 받침 기반으로 올바르게 처리했지만, 뒤따르는 "이라는/라는"은 헬퍼 없이 `'이라는 뜻입니다'`로 하드코딩됨. 같은 파일 안 CONTEXT_MEANING(`eul_reul` 사용)이나 이후 작성된 L6용 `phase27_generate_l6_pilot_items.py`(자체 `ira_neun()` 헬퍼 보유)는 이 문제가 없었음. |
| `data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.jsonl`/`.csv` | phase16 실행 결과 - 동일 오류 포함(날짜가 박힌 감사 이력 파일이라 **수정하지 않음**, 아래 "손대지 않은 이유" 참고) |
| `data/import/schema_reading_phase18_quiz_pilot_rows_20260926.json` | phase18이 `explanation`을 그대로 복사(`r["explanation"]`) - 동일 오류. 마찬가지로 날짜가 박힌 감사 이력 파일이라 **수정하지 않음** |
| `data/vocab/pilot_l4l5_manifest_v1.json`(aprolabs 연구 원본) | 동일 오류 확인 → **수정함** |
| `momolib/data/vocab_quiz/pilot_l4l5_manifest_v1.json`(momolib export 패키지) | 동일 오류 확인 → **수정함** |
| momolib 운영 DB `vocab_quiz_pilot_items.explanation` | 동일 오류 확인 → **수정함** |
| 연구 SQLite DB(`data/vocab/vocabulary_quiz_rnd.db`, `vocabulary_multiformat_items`) | 이 파일럿 배치(`source_version=schema_reading_l4l5_pilot_dryrun_v1`)는 phase18의 설계상 의도(`SOURCE_VERSION 하드코드`로 일반 퀴즈 선택에서 구조적으로 배제)에 따라 **애초에 이 DB에 적재된 적이 없음**(조회 결과 0건) - 해당 없음 |

### 수정 방식 — 전부 "예상 기존값 정확 일치" 확인 후 1줄만 교체

세 위치(aprolabs 매니페스트, momolib 매니페스트, momolib DB) 모두 수정 전에
`explanation == "'집단'은 '여러 사람이 모여 이룬 무리'이라는 뜻입니다."`
와 **정확히 일치**하는지 코드로 assert한 뒤에만
`"'집단'은 '여러 사람이 모여 이룬 무리'라는 뜻입니다."`로 교체했다.
다른 필드(뜻풀이·정답·공개 상태 등)는 건드리지 않았고, momolib DB
수정 후 `correct_option`/`options_json`/`public_payload_json`/
`answer_payload_json`/`source_version`/`is_active`가 수정 전과 동일한지
코드로 재확인했다.

- aprolabs `data/vocab/pilot_l4l5_manifest_v1.json`: 1줄 diff
- momolib `data/vocab_quiz/pilot_l4l5_manifest_v1.json`: 1줄 diff
- momolib 운영 DB: `explanation` 1개 필드만 UPDATE, 나머지 필드 불변 확인

### 손대지 않은 이유 (phase16/18 산출물 원본 파일)

`data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.{jsonl,csv}`,
`data/import/schema_reading_phase18_quiz_pilot_rows_20260926.json`은
날짜가 파일명에 박힌 감사 이력(그 날 실제로 무엇이 생성됐는지의
기록)이며, `qa_flags_json`의 `phase16_report`/`phase17_report` 경로가
이 파일들을 근거로 참조한다. 현재 어떤 라이브 파이프라인도 이 파일을
다시 읽어 momolib로 내보내지 않으므로(연구 DB에도 적재된 적 없음),
지금 시점의 값을 바꾸면 "그날 실제로 생성된 값"이라는 감사 기록의
사실성이 깨진다고 판단해 수정하지 않았다. 대신 생성 스크립트 자체를
고쳐 재발을 막는 쪽을 택했다(아래 2절).

## 2. 생성·이식 절차 보완 + 80건 전수 회귀 테스트

- `phase16_build_quiz_pilot_dryrun.py`에 `ira_neun(word)` 헬퍼(받침
  있으면 "이라는", 없으면 "라는")를 추가하고 MEANING_CHOICE
  explanation 생성부에 적용 - 앞으로 이 스크립트를 다시 실행해도
  동일 오류가 재발하지 않음.
- 받침 유무 규칙으로 80건(L4·L5 40 + L6 40) explanation의 은/는·
  이라는/라는·을/를을 **독립 로직**으로 전수 재검사하는 회귀 테스트
  추가:
  - aprolabs: `tests/test_particle_agreement_pilot_items.py`
  - momolib: `tests/test_vocab_quiz_pilot_explanation_particles.py`
  - 실행 결과 둘 다 **8/8 PASS**, 조사 오류 0건(수정 대상 1건 외
    추가 발견 없음), 뜻풀이·정답·공개 상태는 검사만 하고 변경하지
    않음.

## 3. 모바일 390px 가로 스크롤 수정

### 원인

`app/templates/base.html`의 상단 네비게이션(`<div class="flex
justify-between h-16">`)이 좁은 화면에서 줄어들 수 없는 여러 `<a>`
링크 그룹(좌측 4종 역할별 메뉴)과 우측 사용자 정보 그룹(이름·역할
배지·알림벨·로그아웃)을 그대로 유지해, 링크 텍스트가 세로로 끊겨
표시되고 문서 전체 `scrollWidth`가 `clientWidth`를 넘는 가로 스크롤이
발생했다(390px 기준 최초 관측: 531 vs 485).

### 1차 시도와 보완

좌측 링크 그룹에만 `overflow-x-auto`를 걸었더니 우측 사용자 정보
그룹이 여전히 축소 불가능해 오히려 전체 `scrollWidth`가 932까지
늘어나는 부작용이 발견되어(1차 커밋 `2c84a04`), 좌우 모두를 포함하는
상위 flex 행 자체(`<div class="flex justify-between h-16">`)에
`overflow-x-auto`를 걸고, 좌우 각 그룹은 `flex-shrink-0
whitespace-nowrap`만 갖도록 보완했다(2차 커밋 `97a13c1`, 최종
반영본).

### 결과 (390px 실측)

| 화면 | 수정 전 | 수정 후 |
|---|---|---|
| `/hq/`(관리자 대시보드) | `scrollWidth=531`(문서 전체 가로 스크롤) | `scrollWidth=485=clientWidth`(문서 가로 스크롤 없음, 네비 바 내부만 좌우 스크롤 가능) |
| `/vocab-quiz/`(어휘 퀴즈 관리자 화면) | 문서 가로 스크롤 발생 | `scrollWidth=485=clientWidth` |

네비게이션 내부를 우측 끝까지 스크롤하면 배지·알림벨·로그아웃까지
전부 정상 노출됨을 실제로 확인했다(어떤 링크도 숨기거나 제거하지
않음).

### 데스크톱 회귀 확인

1280px 폭에서 `/hq/`를 재확인 - 레이아웃이 수정 전과 픽셀 단위로
동일(네비게이션 한 줄 유지, 스크롤 없음, `justify-between` 배치
그대로).

### 범위 밖으로 판단해 손대지 않은 부분

학생 화면(`/essays/dashboard` 등)은 `base.html`의 이 상단 `<nav>`가
아니라 별도의 모바일 앱형 레이아웃(하단 탭바)을 쓰고 있어 이번 문제와
무관함을 확인했고, 손대지 않았다.

## 4. 백업·데이터 불변·접근제어 검증

- **변경 전 백업**: aprolabs/momolib 매니페스트 파일 원본 각 1부,
  momolib DB 행(수정 전 전체 컬럼) JSON 스냅샷 1부를 로컬 스크래치
  디렉터리에 저장한 뒤 수정 진행.
- **수정 후 export 패키지 vs DB 전수 재대조**: 227콘텐츠·227레벨·
  80파일럿문항 불일치 **0건**(배포 후 서버에서 직접 재실행).
- **기존 데이터 불변**: `bank_questions=411`, `users=6`,
  `vocab_quiz_contents=227` - 작업 전후 동일.
- **접근제어 재확인**(실제 라이브 요청): 비로그인 302, teacher/
  parent/student 403, 관리자 200 - 전부 기존과 동일.
- QA 임시 계정 4개는 이번 라운드에도 생성 후 전부 삭제(`users` 6으로
  원복, 세션/응답 0건).

## 5. 커밋 요약

| 저장소 | 커밋 | 내용 |
|---|---|---|
| momolib | `2c84a04` | 조사 오류 수정 + 모바일 네비 1차 수정(부작용 있었음) |
| momolib | `97a13c1`(배포 완료) | 모바일 네비 수정 보완(우측 그룹 포함) |
| aprolabs | `e665434` | 연구 원본 매니페스트 수정 + phase16 생성 스크립트 보완 + 회귀 테스트 |

학생 공개는 이번 작업에서도 발생하지 않았다(전 227건
`student_exposure=0`/`public_ready=0` 유지, 재확인 완료).
