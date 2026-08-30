# Zoom 수업 요약 → 학부모 리포트 파이프라인

## 프로젝트 목적

모모의 책장 온라인 수업(Zoom)의 AI Companion 회의 요약을 대표 계정에서 중앙 수집하여,
학생별 학부모 리포트 초안으로 변환하고, 담당 강사 검토·승인을 거쳐 PDF로 발행한다.

## 운영 환경 전제

- Zoom 유료 계정 1개(대표 계정) 아래 강사 9명이 관리 사용자로 소속 (전원 Licensed)
- 수업은 **각 강사가 본인 계정으로 개설**한다 (요약 소유권이 호스트를 따라가므로)
- 수업 형태: 1:4 소그룹 토론(하크네스) 및 1:1
- 기존 자산: Flask CMS, SQLite 파이프라인, Python/WeasyPrint A4 리포트 생성기

---

## 확정된 기술 결정

### 1. 수집은 API, 이메일 경로는 폐기

이메일 자동 수신 방식을 검토했으나 폐기했다.
- Zoom은 요약 메일의 임의 수신자 지정을 지원하지 않음 (호스트 계정 메일로 고정)
- 메일 본문 포함 옵션은 보안상 꺼둔 상태 → 링크만 오므로 파싱 불가
- HTML 템플릿 변경 시 파서가 조용히 깨짐

### 2. 표준 엔드포인트를 쓴다 (중요)

```
GET https://api.zoom.us/v2/meetings/meeting_summaries          # 목록
GET https://api.zoom.us/v2/meetings/{meetingUUID}/meeting_summary  # 본문
```

**`/v2/accounts/{accountId}/...` 형태의 계정 프리픽스 경로를 절대 쓰지 말 것.**
그 경로는 ISV 파트너용 마스터 API이며 `:master` 스코프를 요구한다.
일반 Business 계정은 마스터 계정이 아니라서 해당 스코프를 받을 수 없다.
표준 경로 + admin 스코프로 계정 내 전체 사용자 데이터가 조회된다.

### 3. 인증

Server-to-Server OAuth (app type: Server-to-Server OAuth)

- 스코프: `meeting:read:list_summaries:admin`, `meeting:read:summary:admin`, `user:read:admin`
- 토큰: `POST https://zoom.us/oauth/token?grant_type=account_credentials&account_id={id}`
  - `Authorization: Basic base64(client_id:client_secret)`
  - TTL 1시간, 리프레시 토큰 없음 → 만료 시각 캐싱 후 재발급
- 자격증명은 `.env`에서 로드: `ZOOM_ACCOUNT_ID`, `ZOOM_CLIENT_ID`, `ZOOM_CLIENT_SECRET`
- `.env`는 반드시 `.gitignore`에 포함

### 4. 폴링 방식

웹훅(`meeting.summary_completed`)이 아닌 **야간 배치 폴링**을 주 경로로 한다.
- 상시 엔드포인트 불필요, 백필이 날짜 범위 재실행으로 끝남
- 조회 범위는 **최근 3일**. 요약 생성 지연분이 있어 당일치만 긁으면 누락됨
- 중복은 `meeting_uuid` PK로 제거됨

### 5. 회의명 네이밍 컨벤션

```
[H-YG-SAT1500] 07차시_카프카_변신          # 정규
[H-YG-SAT1500][보강] 07차시_카프카_변신     # 보강
 └반코드        └표식  └차시  └텍스트
```

보강 표식은 **반 코드와 별개의 대괄호**에 둔다. `class_code` 자체에 하이픈이
포함되므로 접미사(`-R`) 방식은 코드와 표식의 경계가 모호해져 쓸 수 없다.

`host_email`(결정론적)로 강사를, `topic` 파싱(휴리스틱)으로 반·차시를 얻고
**두 축을 교차 검증**한다. 불일치는 대타 수업 또는 개설 실수이므로 자동 발행을 막는다.

### 6. 1차 적용 범위 — 하크니스 그룹수업 5개 반

현재 활성 그룹수업은 하크니스 5개 반이며 전원 윤영기 담당이다.
1:1 수업(정규반/프리미엄/시그니처/특강)은 이번 범위에서 제외한다.
범위 밖 회의는 **수집은 하되** `session.status = 'out_of_scope'`로 두고
리포트를 생성하지 않는다. 삭제하거나 건너뛰지 않는다.

### 7. class_code 발급 규칙

키는 **(강사, 요일, 시작시각)** 조합이다. 코드 형식 예: `H-YG-SAT1500`
(유형-강사이니셜-요일시각). 파서가 `[A-Za-z0-9-]`만 인식하므로 이 문자셋을 지킨다.

**처음 보는 키 조합이 나와도 class를 자동 생성하지 않는다.**
`pending_class_key`에 쌓고 운영자 확인을 받는다. 확인 후 둘 중 하나를 한다.
- 기존 반의 시간표 변경 → 해당 `class_id`에 `class_key`를 하나 더 추가
- 실제 신규 반 → 새 `class` 발급

이 안전장치가 없으면 시간표 조정·강사 교체 시 같은 반이 조용히 둘로 갈라져
리포트가 엉뚱한 학생에게 간다. 규칙은 자동, 예외는 수동이다.

### 8. momoai_web 연동

`momoai_web`의 `course_code`(예: `중1하260228`)는 시작일이 박혀 있어
학기마다 바뀐다. **영속 식별자로 쓰지 않는다.**
`User.zoom_id`는 이메일이 아니라 개인 회의실 번호이므로 연결키로 쓸 수 없다.
강사 연결은 `instructor.momoai_user_id`에 9명분을 **한 번 수동으로** 채운다.
이름 문자열 매칭을 코드로 자동화하지 않는다 (동명이인·개명 시 조용히 틀린다).

---

## API 구현 시 주의사항

- `meetingUUID`에 `/` 또는 `+`가 포함되면 **더블 URL 인코딩** 필요. 안 하면 3001 에러
- 429 응답에 지수 백오프 적용
- 목록 조회는 `next_page_token`으로 페이지네이션
- 사용자 `type`이 1(Basic)이면 AI Companion 미적용 → 요약이 생성되지 않음

---

## 데이터 모델

`zoom_pipeline_core.py`에 스키마 DDL과 파서가 이미 구현되어 있다. 재작성하지 말고 임포트할 것.

| 테이블 | 역할 |
|---|---|
| `zoom_summary_raw` | Zoom 응답 원문. **가공 금지** |
| `instructor` | 강사. `zoom_email`이 `host_email` 조인 키 |
| `class` | 반(영속 단위). `instructor_id`가 배정표 역할 |
| `class_key` | (강사,요일,시각) → 반. 시간표 변경 시 키를 추가 |
| `pending_class_key` | 미확인 키 조합. 운영자 확인 대기 |
| `student` | 학생. 반에 소속 |
| `session` | 수업 회차. status: `mapped` / `unmapped` / `mismatch` / `out_of_scope`, session_type: `regular` / `makeup` |
| `report` | 학생별 리포트. status: `draft` / `review` / `published` |

핵심 원칙:
- **raw는 절대 가공하지 않는다.** 프롬프트·파싱 규칙 변경 시 재처리로 해결한다.
  수집 단계에서 가공하면 Zoom 보관 기간 경과 후 복구 불가
- `report`에 `UNIQUE (session_id, student_id)` → 1:4 수업에서 학생당 정확히 1건

구현된 함수:
- `init_db(path)` — 스키마 생성
- `parse_topic(topic) -> ParsedTopic` — 예외를 던지지 않고 `errors`에 누적
- `resolve_session_status(conn, host_email, parsed)` — 교차 검증, 상태 판정

---

## 개인정보 요구사항 (타협 불가)

미성년 학생의 수업 발언을 다루므로 다음은 설계 제약이다.

1. **1:4 수업 정보 분리** — 대상 학생 외 다른 학생의 이름과 발언은
   **프롬프트에 넣기 전에 코드로 마스킹**한다. 모델에게 "언급하지 말라"고
   지시하는 방식은 사용하지 않는다.
2. **소유자 스코핑** — 강사는 자기 담당 수업만 조회 가능.
   조회 쿼리에 소유자 조건을 강제한다.
3. **승인 게이트** — `published` 전환은 담당 강사의 명시적 승인으로만 발생한다.
   승인 없이 학부모에게 나가는 경로가 존재해서는 안 된다.
4. **원문 즉시 적재** — Zoom 요약 보관 기간이 제한적이므로 조회 즉시 자체 DB에 저장

---

## 진행 상황

**완료**
- 아키텍처 결정 (수집 방식, 엔드포인트, 스키마)
- `zoom_pipeline_core.py` — 스키마 DDL, 회의명 파서, 교차 검증 로직 (테스트 통과)
- Zoom 관리자 설정: 요약 켜고 잠금, 공유 대상 호스트 단독, 외부 공유 차단,
  이메일 본문 포함 해제, 자동 시작 활성화
- Server-to-Server OAuth 앱 생성, 토큰 발급 검증 완료
- `GET /v2/users` 대조 완료 — 강사 9명 전원 `type=2`(Licensed),
  `instructor.zoom_email`과 API 반환 이메일 완전 일치
- `instructor` 테이블 입력 완료 (`seed_master_data.py`)

- 운영 규칙 확정 (적용 범위, class_code 발급, 보강 처리)
- 파서 확장 — 보강 표식 인식, `session_type` 판정 (테스트 통과)
- `derive_class_codes.py` — momoai_web 활성 하크니스 5개 반의 course_name을
  파싱해 `H-{강사이니셜}-{요일3자}{HHMM}` class_code 후보 생성(출력만, 미입력)
- `load_class_keys.py` — 위 5건을 `class`/`class_key`에 입력 완료
  (재실행해도 중복 안 생김 - class_code UNIQUE, class_key는
  (instructor_id,weekday,start_time) UNIQUE로 UPSERT)
- `student` 마스터 데이터 입력 완료 — momoai_web `course_enrollments`에서
  이 5개 반의 실제 재원생 17명을 조회해 `seed/students.csv`로 옮기고
  `load_seed_csv.py`로 적재(반별 5/4/4/2/2명, momoai_web 등록 인원과 일치 확인).
  `seed/`의 예시 데이터(`MB-3A`/`학생1~5`)는 실제 데이터로 교체됨

- `collector.py` — 배치 폴링 수집기. `/v2/meetings/meeting_summaries`(목록,
  페이지네이션) → 신규 건만 `/v2/meetings/{uuid}/meeting_summary`(detail,
  더블 URL 인코딩) → `zoom_summary_raw` 즉시 저장. `--days`/`--from --to` 백필
  지원. 실제 실행: 최근 3일 73건 전부 성공, 재실행 시 전부 스킵(idempotent)
- **중요 발견**: 수집된 73건 전부 `meeting_topic`이 "OO의 개인 회의실"
  (Zoom 기본값) — `[H-YG-SAT1500] 07차시_...` 회의명 규칙이 실전에서
  아직 하나도 적용되지 않음. 회의명 파싱만으로는 매핑이 불가능한 상태였음
- `map_sessions.py` — 회의명 규칙 미적용 문제를 우회하는 **시간표 기반
  매핑**으로 방향 전환(2026-08-31 사용자 지시). `raw.meeting_start_time`
  (UTC)→KST 요일/시각 변환 → `host_email`로 instructor_id 확정 →
  `class_key(instructor_id, weekday)` 후보 중 ±30분 이내 매칭 →
  정확히 1건이면 mapped, 0건이면 unmapped+`pending_class_key` 기록,
  2건 이상이면 mismatch. `parse_topic()`도 병행 시도해 lesson_no/
  text_label/session_type을 채우되(지금은 전부 비어있음), 매핑 성사 여부는
  시간표 기준으로만 정한다. 회의명 규칙이 나중에 지켜지면 자동으로
  더 풍부한 정보가 채워지는 구조.
  실행 결과: mapped=23(5개 반에 5/4/5/4/5건), unmapped=50, mismatch=0.
  버그 발견·수정: `status` 필드가 항상 'unmapped'로 저장되던 문제(class_id는
  맞게 들어갔는데 status만 갱신 안 됨) - 재실행 검증 중 발견, 수정 완료
- **확인 필요 - pending_class_key 42건**: unmapped 50건이 서로 다른
  시각이라 42개의 개별 키로 쌓임. 이 중 상당수는 진짜 새 수업 시간표가
  아니라 임의 시각의 1:1 상담·잡담 통화로 추정됨. pending_class_key를
  "새 수업 시간대 후보"로만 쓸지, 이런 노이즈까지 그대로 쌓아도 되는지
  운영 정책 정리 필요
- **원인 정정 - "반복 mapped"는 재접속이 아니라 실제 다른 주차였음**:
  FRI1940 반 5건이 재접속으로 쪼개진 게 아니라 **7/31, 8/7, 8/14, 8/21,
  8/28 — 5주치의 서로 다른 정규 수업**이었다(session.started_at으로 직접
  확인).
- **확정 - `/v2/meetings/meeting_summaries`의 `from`/`to`는 이 계정에서
  전혀 필터링하지 않는다(2026-08-31 재현 확인, 문서 대조 완료)**.
  실제 요청 URL을 로그로 찍어 확인:
  `https://api.zoom.us/v2/meetings/meeting_summaries?from=2026-08-28&to=2026-08-31&page_size=100`
  - 파라미터명(`from`/`to`)과 값 형식(`YYYY-MM-DD`)은 Zoom 문서 규격과 일치.
    응답도 요청값을 그대로 echo함
  - 그런데 **`from=2020-01-01&to=2020-01-31`(데이터가 있을 수 없는 범위)로
    보내도 73건이 그대로 반환됨** - 서버가 값을 받기는 하지만 실제
    필터링에 반영하지 않는 것으로 실측 확인됨
  - Zoom이 2026-01-12 changelog에서 이 엔드포인트에 `time_filter_field`
    파라미터(어떤 시각 필드 기준으로 from/to를 적용할지 지정)를 추가했다고
    공지한 걸 찾아 `meeting_start_time`/`summary_created_time`/
    `summary_last_modified_time` 세 값 모두로 테스트했지만 **역시 동일하게
    73건 그대로** - 이 파라미터도 효과 없음
  - **결론**: 날짜 범위 제한은 서버가 아니라 **클라이언트 쪽에서** 해야
    한다. `collector.py`의 `--days`/`--from --to`는 지금 목록을 좁히는
    용도가 아니라 "이후 detail을 가져올지 판단할 기준 날짜"로만 의미가
    있고, 목록 조회 자체는 매 실행마다 계정의 전체 보관 기간 데이터를 다
    받아온다(피해는 없음 - 중복은 meeting_uuid로 걸러지고 idempotent).
  - **2026-08-31 해결**: `collector.py`에 `filter_by_kst_date()` 추가 -
    `map_sessions.to_kst()`를 재사용해 목록 응답의 `meeting_start_time`을
    KST 날짜로 바꾼 뒤 `--days`/`--from`/`--to`로 직접 걸러낸다. 실측:
    `--days 3` 요청 시 서버가 준 73건 중 클라이언트 필터 후 10건만 남음
    (경계 포함 확인: `--from 2026-08-30 --to 2026-08-30` 단일일 조회도
    정상 동작). 이제 "N일치만 가져온다"는 게 실제로 맞다.
  - 로그도 정리: 기본 실행은 한 줄 요약만(`목록 N건, 신규 N건, 실패 N건,
    소요 X.X초`), `--verbose`/`-v`를 줘야 요청 URL·응답 상세·필터 제외
    내역이 찍힌다. Authorization 헤더/토큰은 verbose에서도 절대 출력 안
    함(직접 grep으로 확인 완료) - `get_access_token()`이 토큰 문자열
    자체를 로그 경로에 절대 넘기지 않는 기존 설계를 그대로 따름.
  - 부수 수정: zoom_reports 스크립트 여럿이 win32에서 stdout을 utf-8로
    래핑하는데, 한 스크립트가 다른 스크립트를 import하면(예: collector.py
    가 map_sessions.py를 import) 무조건 재래핑하다가 먼저 만든 래퍼가
    GC되며 하부 버퍼를 닫아 "I/O operation on closed file"이 나던 문제를
    전체 스크립트에 이미 utf-8이면 건너뛰는 가드로 고침.
- **`class_meeting` 도입 완료** (2026-08-31, 사용자 설계):
  `zoom_summary_raw`(UUID)/`session`(UUID, 그대로 유지) 위에 `class_meeting`
  (class_id+날짜 단위, 회차) 레이어를 추가. `report`는 이제 `session_id`가
  아니라 `class_meeting_id` 기준(`UNIQUE(class_meeting_id, student_id)`).
  `migrate_class_meeting.py`로 기존 momo_zoom.db에 무손실 마이그레이션
  (class_meeting 테이블 생성, session에 컬럼 ALTER 추가, report는 비어있어
  안전하게 재생성). mapped 23건 → class_meeting 23건(전부 날짜가 달라
  묶일 게 없었음 - 위 "5주치" 건으로 확인됨). 재실행해도 안전(idempotent).
- **검토 큐 화면 완료** (2026-08-31): `review_app.py` — FastAPI 독립 앱
  (`zoom_reports/` 안, `momo_zoom.db` 직접 오픈, 인증 없음, 메인
  aprolabs 앱(8000)과 무관, 포트 8801). 탭 3개:
  1) 미확인 키(`pending_class_key`, resolved=0) — 반복 횟수 내림차순.
     반복 횟수는 컬럼에 저장돼 있지 않아 unmapped session을 (instructor_id,
     weekday, 정확히 같은 KST 분)으로 직접 집계해서 구함
  2) 미매핑 세션(`session.status='unmapped'`) — KST 시각 내림차순
  3) 매핑됨(`session.status='mapped'`) — 반별 그룹, 그룹 내 날짜순
  읽기 전용 + "확인함"(resolved=1) 버튼만. 시각은 `map_sessions.to_kst()`
  재사용. 페이지네이션 없음(건수 적음).
  - **학생 이름 노출 문제 발견·해결**: 처음엔 탭 2에 요약 본문 200자
    미리보기 + `student` 테이블 이름 문자열 마스킹을 넣었는데, 실사용
    테스트 중 실명이 그대로 노출됨을 확인(예: "이호준", "심지후"). 원인 -
    탭 2는 정의상 아직 반이 안 정해진 세션인데, 실제로 하크니스 5개 반
    학생이 아닌 1:1 개인 수업(등록 안 된 학생)이었어서 알고 있는 학생
    명단 기반 마스킹이 통하지 않음. `summary_title`도 고정 문구라 대안이
    안 됨. **사용자 결정(2026-08-31): 요약 본문 미리보기를 완전히 빼고
    `meeting_topic`만 표시** — 문자열 마스킹은 등록 안 된 학생까지
    가릴 수 없어 신뢰할 수 없다고 판단.
  - 서버 기동해 3개 탭 모두 200 확인, "확인함" 버튼이 실제로 resolved를
    바꾸고 탭 1에서 행이 사라지는 것 확인(테스트 후 원복), 탭 3 그룹/날짜
    정렬 눈으로 확인(5개 반 전부 class_meeting 연결됨).
  - **2026-08-31 추가**: 탭 2에 회의 길이(분)와 요약 본문 글자 수 컬럼
    추가(본문 내용 자체는 여전히 노출 안 함). 길이는 `zoom_summary_raw`를
    `meeting_uuid`로 조인해 payload의 `meeting_start_time`/
    `meeting_end_time`(둘 다 UTC) 차이를 분으로 계산, 글자 수는
    `len(summary_overview)`만 읽고 텍스트 자체는 템플릿에 넘기지 않음.

**진행 중**
- `instructor.momoai_user_id` 컬럼 추가 및 9명분 수동 입력
  (momoai_web 강사 12명 중 이 9명과 이름 매칭 확인됨 - `User.zoom_id`는
  이메일이 아니라 개인 회의실 번호라 조인키로 못 씀)
- 강사 대상 회의명 규칙 공지 (더 이상 매핑의 필수 조건은 아니지만, 지켜지면
  lesson_no/text_label/보강 여부가 자동으로 채워지므로 여전히 유용)

**완료 (추가)**
- **`generate_reports.py` — 변환 배치 완료** (2026-08-31): class_meeting
  id를 입력받아 학생별 학부모 리포트 초안(`report.body_md`, status='draft')
  생성. 대상 학생 1명당 1회 LLM(Claude, `claude-sonnet-4-6`) 호출.
  - **마스킹은 프롬프트가 아니라 코드로 강제**(CLAUDE.md 개인정보 요구사항
    1번). 대상 학생을 뺀 같은 반 나머지 학생 이름을 API 호출 *전에*
    "다른 학생 A/B/..." 익명 라벨로 치환(삭제가 아니라 라벨 - 토론 흐름
    유지). 강사 이름은 치환하지 않음.
  - 안전장치 2개, 둘 다 실측 확인됨: (1) 다른 학생 이름이 대상 학생
    이름의 부분 문자열이면(예: 대상 "김민수", 다른 학생 "민수") 치환이
    대상 이름 자체를 깨뜨릴 수 있어 치환 전에 감지해 실패 처리.
    (2) 치환 후에도 원본 이름이 하나라도 남아 있으면 API를 호출하지
    않고 실패 처리. 두 경우 모두 유닛 테스트로 확인.
  - `(class_meeting_id, student_id)` UNIQUE 활용 - 이미 있는 report는
    API를 다시 부르지 않고 건너뜀(재실행 안전 + 비용 절약, 실측 확인).
    학생 1명 처리할 때마다 즉시 커밋 - 배치 중간에 실패해도 이미 만든
    초안은 남음.
  - 실측(class_meeting_id=1, 학생 2명): 생성된 두 리포트 모두 대상 학생
    본인 얘기만 담겼고(이 회차는 다른 학생 언급 자체가 없어 라벨도 안
    등장), 상대 학생 실명은 전혀 안 나옴 - 문자열 포함 검사로 확인.
  - **API 키 발견**: `aprolabs/.env`의 `ANTHROPIC_API_KEY`가 만료 상태였음
    (401 invalid, 원시 SDK 호출로도 재현). zoom_reports는 독립 앱이라
    자체 `.env`를 쓰므로 새로 발급받은 키를 `zoom_reports/.env`에
    별도로 넣음(2026-08-31) - aprolabs 쪽 키와 별개로 관리됨.
  - **2026-08-31 전체 백필**: 존재하는 class_meeting 23건 전부에 대해
    실행. 신규 생성 76건 + 이미 있던 2건(위 첫 테스트) = `report` 78건,
    전부 `status='draft'`, 실패 0건. 반별 재원생 수와 리포트 수가 정확히
    일치하는지 DB에서 직접 대조해 불일치 0건 확인.
- **검토·승인 UI 완료** (2026-08-31): `review_app.py`(검토 큐, 파이프라인
  상태 확인용)와는 별개로, 업무용 조회 화면인 aprolabs
  `app/routers/zoom_summaries.py`에 이어붙임. 회차 상세 페이지에 그 회차의
  학생별 리포트 목록(상태별 건수 포함)을 추가하고, 각 리포트를 열면
  `report_detail.html`에서 마크다운 미리보기(marked.js, 클라이언트
  렌더링) + 편집 가능한 textarea를 같이 보여줌.
  - draft만 편집 가능 - 저장(`/save`)은 body_md만 갱신, status는 draft
    유지. review/published는 textarea가 `readonly`로 잠기고 저장 폼
    자체가 안 보임.
  - 승인(`/approve`)은 draft -> review만 허용, `approved_by`(로그인한
    aprolabs 사용자 id)와 `approved_at`(UTC) 기록. review ->
    published(학부모 발행)는 이번 범위 밖 - 별도 단계.
  - 승인 취소(`/unapprove`)는 review -> draft로 되돌리고 approved_by/
    approved_at을 NULL로 지움.
  - 상태 전환 가드 전부 실측 확인: draft가 아닐 때 save/approve 시도 →
    400, review가 아닐 때 unapprove 시도 → 400.
  - **쓰기 커넥션 분리**: 조회(GET)는 기존 `get_zoom_db()`(SQLite
    `mode=ro`, 파일 자체가 쓰기 거부)를 그대로 쓰고, save/approve/
    unapprove 3개 POST 라우트만 새로 만든 `get_zoom_db_rw()`(일반
    커넥션)를 쓰도록 분리. mode=ro 커넥션으로 직접 UPDATE 시도하면
    SQLite가 막는 것도 재확인.
  - **주의(임시 상태)**: 지금은 로그인한 aprolabs 운영자 전원이 소유자
    스코핑 없이 전체 반을 봄. `approved_by`에는 aprolabs `User.id`(UUID
    문자열)를 그대로 저장하는데, `report.approved_by`는 원래 zoom_reports
    쪽 `instructor.id`(정수)를 참조하도록 설계된 필드라 값 공간이 다름 -
    나중에 강사별 소유자 스코핑을 붙일 때 이 필드의 의미를 다시 정리해야
    함(사용자도 인지하고 있는 임시 상태).
  - base.html의 Tailwind CDN에 `?plugins=typography` 추가(마크다운
    미리보기의 `prose` 클래스가 실제로 스타일을 받게 하려고) - 다른
    화면에는 영향 없음(다른 템플릿은 `prose` 클래스를 안 씀).
- **`correct_reports.py` — 고유명사 교정 배치 완료** (2026-08-31):
  `generate_reports.py`로 만든 draft를 원본 수업 요약(마스킹된 상태)과
  대조해서 인명·지명·책 제목·역사적 사건/인물명 등 고유명사만 고치고
  나머지 문장·구조·어조는 바꾸지 말라고 지시하는 2차 LLM 패스.
  `class_meeting_id` 입력, 대상은 `report.status='draft'`만(review/
  published는 검토 화면의 "편집 불가"와 같은 원칙으로 건너뜀).
  - 마스킹 로직은 새로 안 만들고 `generate_reports.py`의
    `mask_other_students`/`MaskingError`/`load_meeting_segments`/
    `build_raw_text`/`load_class_students`를 그대로 import해서 씀 -
    같은 보안 로직이 두 파일에 따로 존재하면서 나중에 어긋나는 걸 방지.
  - **안전장치 추가(입력+출력 양쪽)**: (1) 교정 API를 부르기 *전에*
    현재 draft 본문에 이미 다른 학생 실명이 들어있는지 검사(검토
    화면에서 운영자가 편집해뒀을 수 있으므로) - 있으면 API 호출 안 하고
    실패 처리. (2) 교정 API *응답*에도 다른 학생 실명이 새로 나타나는지
    저장 전에 재검사(모델이 익명 라벨을 실명으로 "복원"할 가능성 대비) -
    있으면 저장하지 않고 실패 처리. 두 안전장치 모두 실제로 다른 학생
    실명을 주입한 뒤 API 호출이 차단되는 것을 실측 확인.
  - **`generate_reports.py`에도 같은 출력 검사를 retrofit**: 원래는
    입력(마스킹)만 검사했는데, LLM 출력에 다른 학생 실명이 나타나는지도
    저장 전에 검사하도록 보강("코드로 강제"는 입력뿐 아니라 출력에도
    적용돼야 한다는 원칙에 맞춤).
  - 실측(class_meeting_id=1,2): 교정 정상 동작 확인, status는 계속
    draft로 유지됨. review 상태인 리포트는 건너뛰는 것도 실측 확인
    (건너뛴 건수를 "승인돼서 건너뜀"으로 별도 집계).
  - **발견 - FK 강제 여부가 커넥션마다 다름**: `zoom_pipeline_core.py`의
    스키마 스크립트 맨 앞에 `PRAGMA foreign_keys = ON;`이 있어서
    `init_db()`로 얻은 커넥션(이 배치들, collector.py, map_sessions.py
    등 zoom_reports 쪽 스크립트 전부)은 FK 강제가 걸려 있음. 반면
    aprolabs `app/routers/zoom_summaries.py`의 `get_zoom_db_rw()`는
    `init_db()`를 안 쓰고 그냥 `sqlite3.connect()`라 FK 강제가 꺼져
    있음 - 그래서 `report.approved_by`에 zoom_reports `instructor.id`
    형식이 아닌 aprolabs `User.id`(UUID 문자열)를 넣어도 지금은 에러
    없이 들어감. 나중에 누가 zoom_reports 쪽 스크립트(FK 강제 커넥션)로
    `approved_by`를 만지면 기존 UUID 값 때문에 FK 위반이 날 수 있음 -
    소유자 스코핑 정리할 때 같이 처리해야 함.

**범위 결정**
- **PDF 발행은 이 파이프라인 범위에 넣지 않음** (사용자 결정, 2026-08-31).
  `review` 상태(교사 승인 완료)가 이 시스템의 최종 상태 - `published`
  전환 및 학부모 발행 경로는 만들지 않는다. 승인된 리포트를 실제로
  학부모에게 전달하는 방법은 이 시스템 밖의 별도 절차.

**다음 작업 (우선순위 순)**
- 운영(백필, 재시도, 알림) - 필요해지면

---

## 코딩 규약

- Python 3.12+, 표준 라이브러리 우선, SQLite
- 시크릿은 `.env`에서만 로드. 코드·로그·커밋에 절대 노출 금지
- 파싱 실패는 예외가 아니라 상태값으로 처리하고 검토 큐에 남긴다.
  **조용히 버리지 않는다**
- 외부 API 호출에는 타임아웃과 재시도를 반드시 붙인다
