# 어휘 퀴즈 1순위 40건 학생용 파일럿 배포 리허설

- 일자: 2026-09-28
- 범위: aprolabs(1차 검수 완료 40건 export 고정) → momolib 운영 DB 읽기
  전용 대조 → 운영 백업의 격리 PostgreSQL 복원 → 그 격리 사본에서만
  레벨 확정·공개 플래그 dry-run 적용 → 학생 기능도 그 격리 사본에서만
  켜고 로그인·출제·채점·결과·세션 격리 E2E 검증 → 롤백 시험 → 격리
  사본 완전 폐기.
- **운영 momolib DB 쓰기 없음. feature flag 운영 활성화 없음. 학생용
  실제 배포 없음. aprolabs 공개 플래그 변경 없음.**(전부 아래에서
  read-only 재확인)

## 1. aprolabs — 승인후보 40건 고정 export

- 1순위 tier1 `content_id` 40건 전부 최신 판정 `APPROVED_CANDIDATE`,
  **stale 0건**(테스트/만료 판정 섞임 없음 → 중단 조건 미발생)
- 콘텐츠 40건 + 연결 문항 80건(콘텐츠당 정확히 2건) 필드값·해시를
  JSON으로 고정(로컬 `aprolabs_export_20260928.json`)
- 판정자 `admin@aprolabs.co.kr`, 저장 시각 `2026-09-28 06:22:13~06:44:52`
  (직전 1차 검수 세션과 일치)

## 2. momolib 운영 DB 읽기 전용 대조

앱 설정(`create_app`)만 이용해 지정된 40 content_id/80 item_id의 현재
필드값을 조회하는 전용 스크립트를 서버에서 1회 실행 후 즉시 삭제(SELECT만,
커밋/DML 없음).

| 대상 | 결과 |
|---|---|
| momolib에 없는 content_id | 0건 |
| 콘텐츠 필드값 차이 | **0건**(40/40 완전 일치) |
| momolib에 없는 item_id | 0건 |
| 문항 필드값 차이 | **1건** — `MF_A_SC_SRL4L5PILOT_20260925_L4_003`(`SR_L4CORE_4812`) `explanation`: aprolabs="...이라는 뜻입니다" / momolib="...라는 뜻입니다" |
| 레벨 근거(vocab_level/level_status/boundary_flag/target_grade_band) 차이 | 0건 |
| momolib 40건 현재 공개 플래그 | `student_exposure=False, public_ready=False, hold_reason 없음` 40건 전부 동일 |

문항 1건 차이는 **기존에 이미 알려진 조사(은/는·이라는/라는) 표현
차이**(momolib 2026-09-28 온라인 QA에서 수정 완료, aprolabs 연구 DB는
여전히 수정 전 표현)로, aprolabs `content_cautions()`에도 이미 "주의
사항"으로 표시되어 있는 항목이다. **자동 승인하지 않고 그대로
보고한다** — momolib 쪽이 이미 올바른 최신본을 갖고 있어 momolib
승격에는 지장 없으나, aprolabs 연구 DB 동기화는 별도 결정 사항.

## 3. 운영 백업 → 격리 PostgreSQL 복원 → dry-run 적용(격리 사본에만)

### 3-1. 백업
- 서버에서 앱의 `SQLALCHEMY_DATABASE_URI`를 스크립트 내부에서만 사용해
  `pg_dump -Fc` 실행(연결 문자열·비밀번호를 화면에 출력하지 않음)
- `~/momolib_backup_pilot_rehearsal_20260928-224944.dump`(서버 보존,
  329,502 bytes, SHA-256 `ade952f3...59eae`)

### 3-2. 격리 복원
- 운영과 완전히 분리된 로컬 포터블 PostgreSQL(포트 `55432`, DB
  `momolib_rehearsal`)에 실제 복원
- 복원본 행 수: `bank_questions=411`, `users=6`,
  `vocab_quiz_contents=227`, `vocab_quiz_pilot_items=80`,
  `vocab_quiz_admin_reviews=0`, `alembic_version=4d83374ccc5f` — 운영과
  전부 일치(실제 운영 데이터로 복원됨 확인)

### 3-3. eligibility 코드 기반 변경 도출·적용(격리 사본에만)

`app/vocab_quiz/eligibility.py`의 `content_is_eligible()` 조건과
`app/vocab_quiz/publish_review_routes.py`의 `promote_dry_run()` 필드
계산 로직을 그대로 재사용(판정=승인후보 조건만, momolib
`vocab_quiz_admin_reviews`가 R&D 화면 폐쇄로 비어 있으므로 1·2단계에서
대조 완료한 aprolabs 40건 승인후보 목록으로 대체).

| 필드 | 변경 전 → 후 | 행 수 |
|---|---|---|
| `vocab_quiz_contents.student_exposure` | False → True | 40 |
| `vocab_quiz_contents.public_ready` | False → True | 40 |
| `vocab_quiz_content_levels.level_status` | (전부 `REVIEW_BOUNDARY`) → `PROVISIONAL_AUTO` | 40 |
| `vocab_quiz_content_levels.boundary_flag` | (전부 `True`) → `False` | 40 |
| **합계** | | **160건, 콘텐츠 40건 전체 대상, 차단 0건** |

**참고 사실**: aprolabs 원본 데이터를 보니 1순위 40건 전부 `boundary_flag=1`
또는 `level_status=REVIEW_BOUNDARY` 상태였다(정기·조치 등 화면에서
이미 관찰된 것과 동일) — momolib도 동일하게 40건 전부 레벨 확정이
필요한 상태임을 이번에 재확인.

적용 후(격리 DB에서만): `eligible_content_ids()`가 정확히 이 40건과
일치(`True`), `eligible_pilot_item_count()`=80. **나머지 187건은
`student_exposure`/`public_ready` 완전 불변**(사전·사후 스냅샷 비교
`True`).

## 4. 학생 기능 E2E(격리 사본에서만 `VOCAB_QUIZ_STUDENT_ENABLED=true`)

로컬 momolib 앱을 격리 DB(`localhost:55432/momolib_rehearsal`)에만
연결하도록 강제하는 가드(`DATABASE_URL` 불일치 시 즉시 중단)를 스크립트
서두에 걸고 실행. 테스트 계정 3개(studentA/B, admin, `@example.test`)만
신규 생성.

- 80개 eligible 문항을 10개씩 8세션으로 **결정론적으로 분할**(무작위
  샘플링에 맡기면 반복해도 전량 커버를 보장 못 하므로, 이번 리허설은
  전수 검증을 위해 세션을 직접 구성 — 실제 학생 흐름은 `/start`의
  무작위 10문항 샘플링을 그대로 사용)
- 세션별로 절반은 정답, 절반은 오답을 제출해 채점 로직 양방향 검증

| 검증 항목 | 결과 |
|---|---|
| 로그인 | studentA/B/admin 전부 정상 |
| 제출 전 정답 비노출 | 8세션 전부 `correct_option`/`answer_payload` 응답에 없음 |
| 80문항 채점 정확성(정답·오답 양쪽) | 80/80 전부 기대값과 일치 |
| 세션별 `complete` 정답 수 집계 | 8/8 일치 |
| 결과 화면 렌더 | 8/8 정상(200) |
| 80문항 전체가 최소 1회 이상 풀이됨 | 확인(누락 0건) |
| 다른 학생 세션 조회 차단 | studentB → studentA 세션 GET 403 |
| 다른 학생 세션에 답안 제출 차단 | studentB → studentA 세션 POST 403 |
| 관리자 R&D 화면(`/vocab-quiz/publish-review/`) | 학생 플래그 ON과 무관하게 **여전히 404**(블루프린트 미등록은 별개 스위치) |
| 기존 HQ 대시보드/`library` 블루프린트 | 정상 응답(500 없음) |
| 관리자 파일럿 응시 테이블(`vocab_quiz_pilot_sessions/_attempts`) | 학생 응시로 **전혀 증가하지 않음**(완전히 분리된 테이블 재확인) |
| 신규 사용자 | 정확히 3건만 추가 |

**138/138 PASS.**

### 4-1. 롤백 시험(격리 사본 폐기 전)

3단계에서 적용한 160건을 정확히 원래 값(student_exposure/public_ready
`False`, level_status `REVIEW_BOUNDARY`, boundary_flag `True`)으로
되돌리는 UPDATE를 실행:

- 롤백 후 `eligible_content_ids()`=0, `eligible_pilot_item_count()`=0 —
  기대값과 일치
- 학생 세션 8건/응답 80건(응시 기록)은 롤백 후에도 **그대로 보존**
  (콘텐츠 재비공개가 과거 응시 기록을 지우지 않음을 확인)
- 이번 작업은 신규 마이그레이션 없이 기존 컬럼 값만 바꿨으므로, 실제
  운영에 적용했다가 되돌릴 경우도 **스키마 다운그레이드 불필요** —
  동일한 역방향 UPDATE(또는 `pg_restore`로 백업 시점 전체 복원)만으로
  충분함을 실증.

### 4-2. 격리 사본 폐기

PostgreSQL 인스턴스 정지 → 데이터 디렉터리·포터블 바이너리 전체
삭제(`C:\Users\aproa\pgportable` 통째로 제거) → 로컬에 남은 백업 덤프
사본도 삭제(서버 쪽 원본은 보존, 아래 6절).

## 5. 최종 재확인(운영 momolib, 읽기 전용)

| 항목 | 값 |
|---|---|
| `vocab_quiz_contents` | 227(불변) |
| `vocab_quiz_pilot_items` | 80(불변) |
| `bank_questions` | 411(불변) |
| `users` | 6(불변 — 리허설 테스트 계정 유입 없음) |
| `vocab_quiz_admin_reviews` | 0(불변) |
| `student_exposure=True 또는 public_ready=True`인 콘텐츠 | **0건** |
| `VOCAB_QUIZ_STUDENT_ENABLED` | **여전히 OFF** |

## 6. 운영에 실제 적용한다면 — 명령과 복구 절차(실행 안 함, 참고용)

**적용 순서(실제 승격 시)**:
1. `pg_dump -Fc`로 배포 직전 백업(이번과 동일한 앱-내부 URI 사용
   방식, 자격증명 비노출)
2. 40건 콘텐츠·레벨에 대해 3절의 160건 UPDATE를 트랜잭션으로 적용
   (`student_exposure`/`public_ready` → `True`, 대상 레벨
   `level_status`→`PROVISIONAL_AUTO`/`boundary_flag`→`False`)
3. `VOCAB_QUIZ_STUDENT_ENABLED=true`를 운영 환경변수에 설정하고
   서비스 재시작(별도 배포 절차, 이번 리허설 범위 아님)

**복구(롤백) 절차**:
- 경미한 되돌림: 2번 UPDATE의 역방향(정확히 4-1절에서 실증한 스크립트)
  만 실행 — 신규 마이그레이션이 없으므로 스키마 다운그레이드 불필요
- 전체 복구: 1번 백업 파일을 `pg_restore`로 복원

## 7. 남은 위험 / 확인 필요 사항

1. **레벨 확정은 기계적 파생일 뿐, 별도 언어학적 재검토 아님** —
   `REVIEW_BOUNDARY`→`PROVISIONAL_AUTO`, `boundary_flag`→`False` 변경은
   eligibility 게이트를 통과시키기 위해 코드 조건에서 그대로 도출한
   것으로, 실제 학년 경계 판단이 이번에 새로 이루어진 것은 아니다.
   진짜 운영 승격 전에는 이 레벨 확정 자체에 대한 관리자 승인 절차가
   별도로 필요하다.
2. **판정 신뢰 경로가 수작업 대조** — momolib `vocab_quiz_admin_reviews`가
   비어 있어(R&D 화면 폐쇄로 판정은 aprolabs로 일원화됨), "판정=승인후보"
   확인을 이번엔 1·2단계의 수동 field-diff로 대체했다. 반복적으로
   승격할 계획이라면 aprolabs 판정 → momolib 배포 절차를 연결하는
   공식적인 방법(예: 판정 export 서명/체크섬)이 필요하다.
3. **"레벨별 출제" API 자체는 없음** — `eligible_pilot_items(pilot_key=...)`
   함수는 레벨(L4L5/L6)별 필터를 지원하지만, 학생용 `/start` 엔드포인트는
   레벨 선택 없이 전체 80문항 pool에서 무작위 10문항만 뽑는다. 이번
   리허설의 "80문항 전량 검증"은 세션을 직접 8개로 나눠 강제로 전량을
   커버한 것으로, **실제 학생이 한 번에 전체를 보장받아 풀 수 있는
   기능은 아직 없다.** 필요하면 별도 기능 추가가 필요하다.
4. **이미 알려진 문항 1건 표현 차이**(`SR_L4CORE_4812`,
   `MF_A_SC_SRL4L5PILOT_20260925_L4_003`)는 momolib은 문제 없으나
   aprolabs 연구 DB 동기화 여부는 이번 범위 밖의 별도 결정 사항.
5. 이번 리허설 자체가 매번 새 격리 사본을 만들고 폐기하는 방식이라,
   반복 리허설이 필요하면 동일한 절차(다운로드·복원·적용·검증·폐기)를
   다시 거쳐야 한다(재사용 가능한 상태로 남겨두지 않음 — 의도적으로
   완전 폐기).

## 8. 하지 않은 것(요청대로)

- 운영 momolib DB 쓰기 없음(전부 격리 사본에서만 적용)
- `VOCAB_QUIZ_STUDENT_ENABLED` 운영 활성화 없음
- 학생용 실제 배포 없음
- aprolabs `vocabulary_publish_reviews`/콘텐츠 공개 플래그 변경 없음
