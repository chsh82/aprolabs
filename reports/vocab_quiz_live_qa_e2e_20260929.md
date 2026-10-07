# 어휘 퀴즈 학생 파일럿 — momolib 운영 서버 실제 E2E(QA 전용 계정)

- 일자: 2026-09-29
- **사용자 승인 하에 운영 DB를 실제로 일시 변경**했다가 전부 원복함.
- 실제 학생 계정 2건은 처음부터 끝까지 **로그인·데이터 변경 없음**,
  allowlist 0건 유지. 작업 종료 시점 `VOCAB_QUIZ_STUDENT_ENABLED`
  **OFF**, 공개 플래그 **0건**, allowlist **0건** — 학생 기능이 켜진
  채로 끝나지 않았음.

## 1. 사전 게이트 + 백업/복원 검증

| 확인 | 결과 |
|---|---|
| aprolabs 유효 판정 | **40/40**, 차단 0건 |
| momolib 콘텐츠·문항 대조 | **콘텐츠 40/40, 문항 80/80** 필드 불일치 0건 |
| 승격 dry-run 게이트 | **PASS** |
| 운영 사전 상태 | `vocab_quiz_contents=227`, `pilot_items=80`, `bank_questions=411`, `users=6`, `admin_reviews=0`, `allowlist=0`, 공개 플래그 True 0건, feature flag OFF, alembic head `c7d2e9f4a1b3` |
| `VOCAB_QUIZ_STUDENT_ENABLED` env 존재 여부 | 사전 0줄(미설정, 값 미노출) |
| **실제 백업** | `pg_dump -Fc`(앱 config URI만 내부에서 사용, 자격증명 비노출) — `momolib_backup_LIVE_qa_e2e_pre_20260929-060048.dump`(서버 보존, SHA-256 `960c0336...c81dfa9`) |
| **실제 복원 검증** | 완전히 별도의 로컬 PostgreSQL에 실제 복원 — `bank_questions=411`/`vocab_quiz_contents=227`/`pilot_items=80`/`users=6`/`allowlist=0`/alembic head `c7d2e9f4a1b3` 전부 운영과 일치 확인 후 검증용 인스턴스 폐기(서버 원본 백업은 보존) |

**차이 없음 → 쓰기 진행.**

## 2. QA 전용 계정

- 식별자(고유): `qa-vocab-pilot-b4236e2cb4fe@momolib-internal.test`
  (도메인 자체가 `-internal.test`로 실존 인물이 아님을 명확히 표시)
- 이름 필드: `QA전용계정-어휘퀴즈파일럿-b4236e2cb4fe`
- 비밀번호: 무작위 생성(32바이트) 후 즉시 해시만 저장 — **평문은 어떤
  로그·커밋·보고서·채팅에도 출력하지 않음**(스크립트 프로세스 메모리
  안에서만 존재, 프로세스 종료와 함께 소멸)
- allowlist: 이 계정에만 `[4, 5, 6]` 등록(1건)
- **기존 실제 학생 2건은 생성 전·생성 후 모두 allowlist 0건**(스크립트가
  직접 재확인)

## 3. 40콘텐츠 승격 + feature flag ON(운영 DB 실제 적용)

계획 검증(콘텐츠 40건/필드 160건 정확히 일치) 통과 후에만 커밋하도록
작성한 스크립트로 실제 운영 DB에 적용:

| 단계 | 기대값 | 실제값 |
|---|---|---|
| 적용 전 eligible | 0 | 0 |
| 40건 제외 187건 사전 상태 | 전부 `(False, False)` | 확인됨 |
| 계획된 필드 변경 | 콘텐츠 40건 / 160건 | 콘텐츠 40건 / **160건** |
| 커밋 후 eligible 콘텐츠 | 40 | **40** |
| 커밋 후 eligible 문항 | 80 | **80** |
| 187건 완전 불변 | True | **True** |
| `bank_questions` | 411 | **411** |

→ **SUCCESS.** 이어서 `.env`에 `VOCAB_QUIZ_STUDENT_ENABLED=true` 한
줄만 추가(값은 화면에 출력하지 않음, 존재 여부만 확인) → 서비스
재시작 → `app.config` boolean으로 `True` 확인 → 비로그인
`GET /practice/vocab-quiz/` → **302**(이전엔 404) 실제 확인.

## 4. QA 계정 실제 HTTP E2E(https://momolib.com, 진짜 외부 요청)

비밀번호를 전혀 쓰지 않고, `login_user()`를 서버 프로세스 안에서
직접 호출해 얻은 유효한 세션 쿠키만으로 **진짜 `https://momolib.com`에
대한 외부 HTTPS 요청**(nginx/gunicorn/TLS 실제로 거침)을 수행했다
(운영 사이트 로그인 화면에 비밀번호를 직접 입력하지는 않음 — 자격증명
입력 금지 정책 준수, 서명 메커니즘만 재사용).

| 검증 항목 | 결과 |
|---|---|
| 비로그인 GET → 302 | PASS |
| QA 계정 GET index → 200 | PASS |
| `vocab_level` 잘못된 값(3/7/'abc'/누락) → 400 | 4/4 PASS |
| **L4** 출제→응답→채점→결과 | 10문항(정답 5·오답 5 섞어 제출), 채점 전부 정확, complete 집계 일치, result 200 |
| **L5** 출제→응답→채점→결과 | 10문항, 동일하게 전부 정확 |
| **L6** 출제→응답→채점→결과 | 10문항, 동일하게 전부 정확 |
| 제출 전 정답 비노출(`correct_option`/`answer_payload`) | 3개 레벨 전부 미노출 확인 |
| 비공개 187건 미출제 | 3개 레벨 세션에 나온 문항 전부 게이트 통과 80건 안에 있음(위반 0건) |
| 실제 노출된 문항 수 | 30건(80건 중 일부 — 전량 커버는 이전 격리 리허설에서 이미 검증, 이번엔 운영 실사용 흔적을 최소화하기 위해 레벨당 1세션만 실행) |

**57/57 PASS.**

브라우저 시각 확인(비로그인 페이지)은 도구 응답 지연으로 스크린샷은
생략했으나, 기능 검증은 실제 외부 HTTPS 요청으로 위와 같이 완료됨.

### 비허용 계정·비공개 콘텐츠 관련 — 실제 학생 계정 미노출 근거

- 실제 학생 계정 2건은 **이번 작업 전체 기간 동안 단 한 번도 로그인
  시도·조회·수정하지 않음**(요청대로).
- 두 계정의 `vocab_quiz_pilot_allowlist` 행 수는 **작업 시작 전 0건 →
  작업 중 0건(재확인) → 작업 종료 후 0건**으로 동일 — 즉 이번
  feature flag ON 구간 동안 실제로 접근을 시도했더라도 코드상
  `_require_pilot_allowlist`에서 즉시 403 처리됐을 것(이 로직 자체는
  이전 격리 리허설·유닛테스트에서 이미 반복 검증됨).
- 187개 비공개 콘텐츠는 애초에 momolib `vocab_quiz_pilot_items`에
  연결된 문항 자체가 없어(전체 80문항 = 이번 게이트 통과 40건에만
  연결) 구조적으로 노출 불가능하며, 이번 E2E에서 실제로 나온 30문항도
  전부 게이트 통과 80건 안에서만 나왔음을 직접 확인.

## 5. 안전 종료(요청 순서대로)

| 순서 | 조치 | 확인 |
|---|---|---|
| 1 | `VOCAB_QUIZ_STUDENT_ENABLED` OFF | `.env` 줄 제거 → 재시작 → `app.config` boolean `False` 확인 → 라이브 `GET /practice/vocab-quiz/` **404** 재확인 |
| 2 | QA allowlist 제거 | (3단계에서 계정 정리 시 함께 삭제) |
| 3 | 160개 필드 원복 | 40건 전부 `student_exposure/public_ready→False`, 레벨 `level_status→REVIEW_BOUNDARY`/`boundary_flag→True` — **160건 커밋**, eligible 콘텐츠/문항 **0/0**, 공개 플래그 True인 콘텐츠 **0건** |
| 4 | QA 계정·시도 기록 정리 | FK 순서 확인 후 응답(30건)→세션(3건)→allowlist(1건)→계정 순으로 삭제. 삭제 직전 이메일이 `momolib-internal.test` 패턴과 일치하는지 재확인한 뒤에만 삭제(안전장치) |

## 6. 최종 재확인(읽기 전용, 작업 완전 종료 후)

| 항목 | 값 |
|---|---|
| `vocab_quiz_contents` | 227(불변) |
| `vocab_quiz_pilot_items` | 80(불변) |
| `bank_questions` | 411(불변) |
| `users` | 6(QA 계정 완전 제거, 실제 계정만 남음) |
| `student` 역할 계정 | 2(실제 계정 그대로) |
| `vocab_quiz_admin_reviews` | 0(불변) |
| **`vocab_quiz_pilot_allowlist` 전체 행 수** | **0** |
| 공개 플래그 True인 콘텐츠 | **0** |
| `eligible_content_ids()` | **0** |
| `eligible_pilot_item_count()` | **0** |
| `VOCAB_QUIZ_STUDENT_ENABLED` | **OFF** |

## 7. 백업/복구 상태

- 운영 백업 파일: `~/momolib_backup_LIVE_qa_e2e_pre_20260929-060048.dump`
  (서버 보존, SHA-256 `960c0336...c81dfa9`, 실제 별도 인스턴스 복원으로
  유효성 검증 완료)
- 이번 변경은 신규 마이그레이션 없이 전부 4단계에서 원복까지
  완료됐으므로 이 백업은 "만약을 위한" 여분 안전장치이며, 실제로는
  이미 원상태로 되돌아가 있음(6절 재확인).

## 8. 실패 시 대응(참고)

이번 실행은 전 단계 성공(SUCCESS)이었으나, 각 스크립트는 "계획 건수
≠ 실제 건수"나 "사후검증 실패" 시 `MISMATCH`를 출력하고 비정상
종료(exit 2)하도록 작성했다 — 그 경우에도 4·5단계(flag OFF → allowlist
제거 → 필드 원복 → QA 계정 정리)를 그대로 이어서 실행해 동일한 종료
절차를 완료할 계획이었다(이번엔 필요 없었음).
