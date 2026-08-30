# Zoom 수업 요약 → 학부모 리포트 파이프라인

## 프로젝트 목적

모모의 책장 온라인 수업(Zoom)의 AI Companion 회의 요약을 대표 계정에서 중앙 수집하여,
학생별 학부모 리포트 초안으로 변환하고, 담당 강사 검토·승인을 거쳐 PDF로 발행한다.

## 운영 환경 전제

- Zoom 유료 계정 1개(대표 계정) 아래 강사 10명이 관리 사용자로 소속
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
[MB-3A] 07차시_카프카_변신
 └반ID   └차시   └텍스트
```

`host_email`(결정론적)로 강사를, `topic` 파싱(휴리스틱)으로 반·차시를 얻고
**두 축을 교차 검증**한다. 불일치는 대타 수업 또는 개설 실수이므로 자동 발행을 막는다.

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
| `class` | 반. `instructor_id`가 배정표 역할 |
| `student` | 학생. 반에 소속 |
| `session` | 수업 회차. status: `mapped` / `unmapped` / `mismatch` |
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

**진행 중**
- Marketplace에서 Server-to-Server OAuth 앱 생성

**다음 작업 (우선순위 순)**
1. 토큰 발급 + `GET /v2/users` 검증 스크립트 → 강사 10명 조회 확인
2. 마스터 데이터 입력 (instructor / class / student)
3. 수집기 구현 — 배치 폴링, raw 적재
4. 매핑 배치 — `parse_topic` + `resolve_session_status` → `session` 적재
5. 검토 큐 확인용 관리자 화면
6. 변환 (마스킹 → LLM 초안 → 고유명사 교정)
7. Flask CMS에 검토·승인 UI
8. PDF 발행 (기존 A4 조판 재사용) + 운영 (백필, 재시도, 알림)

**미결정**
- PDF 출력 방식: WeasyPrint 유지 vs 브라우저 인쇄(CSS Paged Media)로 통일.
  교재 생성 쪽이 후자로 전환했으므로 통일 여부를 정해야 함.
  두 갈래를 유지하면 스타일 수정 비용이 두 배가 됨

---

## 코딩 규약

- Python 3.12+, 표준 라이브러리 우선, SQLite
- 시크릿은 `.env`에서만 로드. 코드·로그·커밋에 절대 노출 금지
- 파싱 실패는 예외가 아니라 상태값으로 처리하고 검토 큐에 남긴다.
  **조용히 버리지 않는다**
- 외부 API 호출에는 타임아웃과 재시도를 반드시 붙인다
