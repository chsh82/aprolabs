# L0~L3 확장 184문항 — 관리자 전용 미리보기 구현·검증

- 일자: 2026-09-29
- 범위: 기존에 연구 DB에 비공개로 적재된 184건(`schema_reading_existing_l0l3_dryrun_v1`)을
  관리자가 레벨별로 실제 출제·응답·채점해 볼 수 있는 화면 추가. 학생 공개·momolib 이식·
  일반 출제(`SOURCE_VERSION=2.1.29`) 경로 변경은 전혀 없음.

## 1. 184문항 고정 매니페스트 (`data/vocab/existing_l0l3_manifest_v1.json`)

현재 연구 DB(적용 완료 상태)에서 `source_version=schema_reading_existing_l0l3_dryrun_v1`
184건을 직접 조회해 그대로 매니페스트로 고정했다. **매니페스트를 DB에서 만들었기
때문에**, "유리병" 항목처럼 생성 파일과 DB 현재값이 의도적으로 다른 2문항도 자동으로
DB값이 기준이 되어, 앞으로 이 스크립트를 재실행해도 그 2건을 드리프트로 오판하지 않는다.

| 레벨 | 문항 수 | 고유 어휘(content_id) |
|---|---|---|
| L0 | 50 | 25 |
| L1 | 50 | 25 |
| L2 | 34 | 17 |
| L3 | 50 | 25 |
| 합계 | 184 | 92 |

검증한 항목: content/level 연결 존재, `is_active=1`, `student_exposure=0`,
`public_ready=0`, `hold_reason` 없음, `level_status=PROVISIONAL_AUTO` ·
`boundary_flag=0` — **이슈 0건**.

## 2. 관리자 전용 미리보기 화면 구현

기존 L4·L5 파일럿·L6 파일럿과 완전히 같은 패턴(관리자 인증 `require_admin` +
매니페스트 화이트리스트 3중 검증 + `PilotBatchIntegrityError`→HTTP 500)을 재사용했다.

- `app/vocabulary_quiz/routers/multiformat.py`: `EXISTING_L0L3_*` 상수, 매니페스트
  로더/캐시, `_select_existing_l0l3_item_ids`(레벨 파라미터화 + `PROVISIONAL_AUTO`·
  `boundary_flag=0` 확인), `/existing-l0l3-preview-availability` 엔드포인트,
  `_create_existing_l0l3_preview_session`, `create_session`/`session_result`/
  `play_page` 분기 추가. 기존 일반/레벨/두 파일럿 분기는 한 글자도 수정하지 않음.
- `app/templates/vocabulary_quiz/multiformat_play.html`: 세 번째 체크박스(세 미리보기
  모드 상호 배타적), 레벨(L0~L3) 선택 UI는 유지하되 신뢰도 필드는 숨김, 결과 화면에
  "레벨 미확정(PROVISIONAL_AUTO)" 배지 표시.
- Python `ast.parse`, JS `Function()` 파싱, Jinja2 템플릿 파싱 전부 사전 통과.

## 3. 실제 API 경로 검증 (격리된 스크래치 환경)

연구 서버에 `git worktree`로 별도 체크아웃(`~/aprolabs_test_l0l3`, 코드만 가벼운
worktree, venv는 심볼릭 링크)을 만들고, 연구 DB를 별도 파일로 복사해 완전히 분리된
uvicorn 프로세스(포트 8901, 스크래치 DB)를 띄워 검증했다. **운영 포트(8000)·운영
DB는 전혀 건드리지 않음.**

| 게이트 | 결과 |
|---|---|
| 가용성 API(L0~L3) 개수 | 50 / 50 / 34 / 50 — 매니페스트와 정확히 일치 |
| 세션 생성(4개 레벨) | 전부 성공, `candidate_count`가 가용성 API와 일치 |
| 레벨 간 문항 혼입 | **0건**(4개 세션 전체 응답 행을 매니페스트 화이트리스트와 대조) |
| 제출 전 정답 비노출 | `/next` 응답에 `correct_option`/`correct_text`/`explanation` 없음 확인 |
| 응답→채점→결과 전체 사이클 | L1 세션 10문항 전부 응답 → `/result`에 `existing_l0l3_preview_info.level_status_note="레벨 미확정(PROVISIONAL_AUTO)"` 정상 표시 |
| 매니페스트 삭제 시 | `/existing-l0l3-preview-availability` 즉시 HTTP 500, `PILOT_BATCH_INTEGRITY_ERROR` |
| 매니페스트 복구 후 | 재시작 시 정상 200 복구 확인 |
| 미로그인 | `/play` 302(로그인 리디렉션), API 302(미들웨어 차단) |
| 비관리자(로그인O·관리자X) | `/play` 403, API 403 |
| L4·L5 파일럿 회귀 | `/pilot-availability`·세션 생성 정상(40문항 가용) |
| L6 파일럿 회귀 | `/l6-pilot-availability`·세션 생성 정상(40문항 가용) |
| 모드 상호 배타 검증 | `pilot_mode`+`existing_l0l3_preview_mode` 동시 지정 → 422 |
| 레벨 필수 검증 | `existing_l0l3_preview_mode`만 지정(레벨 없음) → 422, 레벨 4 → 422 |

**검증 후 정리**: 스크래치 uvicorn 프로세스 종료, `git worktree remove`, 스크래치
DB 파일·테스트 계정용 사용자 DB 사본·테스트 세션 쿠키 파일 전부 삭제. 운영 DB·운영
사용자 DB에는 테스트 데이터가 전혀 생성되지 않았다(격리된 사본에서만 세션 생성).

## 4. 배포 — 코드 커밋·푸시는 완료, 서버 반영은 사용자 확인 대기

### 분리성 확인
- 로컬 `main`과 `origin/main`은 검증 시작 시점 `d73c030`, 검증 도중 사용자 쪽
  작업(`momo_b2b_tablet` 관련 무관 커밋)으로 `d2d4a4b`까지 한 번 더 동기화됐고,
  연구 서버도 같은 시점 `d2d4a4b`로 동일 — **로컬/원격/서버 세 지점이 완전히 일치**한
  상태에서 이번 커밋을 얹었다.
- `git diff`로 `multiformat.py`·`multiformat_play.html` 두 파일을 스캔해 이번 기능과
  무관한 훅(hunk)이 섞여 있지 않음을 확인했다(작업 트리에 남아 있던 다른 무관한
  미커밋 변경 - `momo_worksheet_page_editor.py`, `momo_book_db/*`, 문해력 스크립트 등
  - 은 이번 커밋에 전혀 포함하지 않음).
- 커밋(`a8b0584`)은 정확히 3개 경로만 포함: `app/vocabulary_quiz/routers/multiformat.py`,
  `app/templates/vocabulary_quiz/multiformat_play.html`,
  `data/vocab/existing_l0l3_manifest_v1.json`.
- `git push origin main` 성공(`d2d4a4b..a8b0584 main -> main`, fast-forward).

### 서버 반영은 미완료 — 사용자 확인 필요
연구 서버(`ssh aprolabs`)에서 `git pull`을 실행하려 했으나, **이 세션의 샌드박스
자동승인 분류기가 서버 저장소에 대한 `git pull`을 차단**했다(운영 서버에 대한
직접 변경으로 판단해 거부 - 사유 상세 미제공). 따라서:

- **GitHub(`origin/main`)에는 이미 반영되어 있다.**
- **연구 서버(`aprolabs.service`, 포트 8000)는 아직 이전 커밋(`d2d4a4b`)으로 실행
  중이며, 이번 L0~L3 미리보기 기능은 아직 라이브로 배포되지 않았다.**
- 사용자가 직접 `ssh aprolabs`로 접속해 `cd ~/aprolabs && git pull origin main &&
  sudo systemctl restart aprolabs.service`(또는 기존에 쓰던 배포 절차)를 실행해야
  실제로 반영된다. 이 명령 자체는 이번 스크래치 검증과 완전히 같은 코드이므로
  추가 위험 없이 그대로 적용 가능하다.
- **라이브 반영 후 재검증은 사용자가 배포를 완료한 다음 요청하면 즉시 수행 가능**
  (같은 절차로 `/existing-l0l3-preview-availability?level=0..3` 라이브 호출 및
  `/play` 화면 렌더링 확인).

## 5. DB 불변 조건 확인 (라이브 DB, 읽기 전용)

| 확인 | 결과 |
|---|---|
| 328건 감사 중 `auto_verdict=HOLD` 44건 | 정확히 44건 확인, **184건 매니페스트와 겹치는 item_id 0건** — 별도 보완 대기열로 완전히 분리됨 |
| `vocabulary_multiformat_items` 총수 | **1,553** (이전 턴 적용 후 값과 동일, 배포 전 상태) |
| 공개 플래그(`student_exposure=1` 또는 `public_ready=1`)인 콘텐츠 | **0건** |
| L2 콘텐츠 17건의 `level_status`/`boundary_flag` | 전부 `PROVISIONAL_AUTO` / `0` — 배포 전 스냅샷 확보 완료(배포 후 사용자가 반영을 마치면 동일 쿼리로 재대조 권장, 배포는 코드만 바꾸므로 값 변화가 있으면 그 자체가 이상 신호) |

## 결론

1~3단계(매니페스트 고정, 관리자 화면 구현, 실제 API 경로 검증)는 전부 완료하고
전부 통과했다. 4단계는 "안전하게 분리 가능한 경우에만 배포" 조건은 충족했고 코드는
GitHub에 반영했으나, **서버에 대한 `git pull`/서비스 재시작 자체가 이 세션의 승인
정책상 차단되어 실제 라이브 반영은 사용자가 직접 수행해야 한다.** 5단계 DB 불변
조건은 현재(배포 전) 시점 기준 전부 정상이며, 배포는 코드 전용 변경이라 DB 값에
영향을 줄 수 없다.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
