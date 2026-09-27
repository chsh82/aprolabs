# 어휘 퀴즈 실제 사이트(momolib) 이식 설계 — 읽기 전용 조사, 코드/DB 변경 없음

- 일자: 2026-09-29
- 범위: aprolabs 연구 사이트(`app/vocabulary_quiz/`, `vocabulary_quiz_research.db`)의
  어휘 퀴즈를 실제 서비스 momolib(`C:\Users\aproa\momolib`, 배포 서버
  `34.158.201.115`)로 이식하기 위한 **설계 문서만** 작성한다. 이번 단계에서
  momolib·aprolabs 어느 쪽도 코드나 DB를 수정·배포하지 않았다.
- 조사 방법: aprolabs 쪽은 직접 코드(`app/vocabulary_quiz/*.py`)와 연구 서버
  SQLite를 읽었고, momolib 쪽은 fork 에이전트가 실제 코드(모델·라우트·설정·
  배포 스크립트)를 직접 열어 확인했다(문서만 보고 추측하지 않음 - 대상이
  최초 momoai_web으로 잘못 지정됐다가 조사 중 momolib으로 정정됨, momoai_web
  조사분은 이 보고서에 포함하지 않음).

---

## 1. 연구 DB 기준선 재확인

### 1-1. 전체 콘텐츠 (5,950건 = 5,902 + phase35 신규 48건)
| source_version | 건수 | 성격 |
|---|---:|---|
| `2.1.29` | 5,723 | 일반 어휘 퀴즈 풀(기존, schema_reading과 무관) |
| `schema_reading_literacy_l4_manual_v1` | 49 | L4 코어(phase13) |
| `schema_reading_literacy_l5_manual_v1` | 48 | L5 코어(phase14) |
| `schema_reading_literacy_l6_manual_v1` | 50 | L6 코어 1차(phase24) |
| `schema_reading_literacy_l6_manual_v2` | 32 | L6 코어 2차(phase26) |
| `schema_reading_l6_evidence_grounded_v1` | **48** | **이번에 이식 검토 대상 - phase32~35 근거 기반 집필** |

**L4~L6 신규 콘텐츠 집합**(2.1.29와 분리): 위 5개 `schema_reading_*` 배치
합계 **227건**(batch_id `SCHEMA_READING_PHASE13/14/24/26/35_*`로 서로 구분됨).

### 1-2. 레벨·보류·노출 플래그
- `vocabulary_content_levels.level_status`: `REVIEW_BOUNDARY`가 vocab_level=5
  48건, level=6 130건(=50+32+48, L6 코어 3배치 합과 정확히 일치) - **전부
  "사람이 직접 대조했으나 자동 채점·전문가 검수 완료는 아님"** 상태.
  level=0~4는 `PROVISIONAL_AUTO`/`REVIEW_BOUNDARY`가 혼재하며 2.1.29 풀도
  일부 REVIEW_BOUNDARY를 갖고 있어 **L4~L6 배치와 1:1 대응하지 않는다**(주의).
- `student_exposure`/`public_ready`: **5,950건 전부 0/0**(예외 없음, `hold_reason`도
  전부 NULL) - 연구 DB 자체가 통째로 비공개다.
- 이식 검토 대상 48건은 `level_status='REVIEW_BOUNDARY'`,
  `expert_review_status='DRAFT_NOT_REVIEWED'`(`level_reason_json` 안,
  phase35 커밋 참고) - **전문가 검수 전** 상태임을 이식 설계 전체에서
  전제해야 한다.

### 1-3. 관리자 파일럿 2개 (별도 집합, `vocabulary_multiformat_items`)
| source_version | 건수 | 성격 |
|---|---:|---|
| `schema_reading_l4l5_pilot_dryrun_v1` | 40 | L4·L5 파일럿(phase18/19), manifest 파일로 정합성 하드가드(`multiformat.py:140-206`) |
| `schema_reading_l6_pilot_dryrun_v1` | 40 | L6 파일럿(phase27~29b), 동일 구조(`multiformat.py:153-257`) |
| `2.1.29` | 1,289 | 일반 출제 문항(파일럿과 완전히 분리, phase29b가 재검증) |

**중요 발견(이번 조사에서 확인)**: `app/vocabulary_quiz/routers/{quiz,review,multiformat}.py`의
**모든 라우트가 예외 없이 `require_admin`(`auth.py:18-29`)**을 요구한다 -
일반 출제(`/play`)조차 학생용 경로가 아니라 관리자 전용이다. 즉 **aprolabs
쪽에는 아직 "학생용 기능"이 전혀 존재하지 않는다** - 이번 이식은 "관리자
기능은 그대로, 학생 기능만 momolib 인증에 새로 연결"이 아니라 **학생
노출 경로 자체를 momolib에서 처음 설계**해야 하는 일이다. 이는 지시사항
4번(1차 범위=관리자 전용)과 정확히 맞아떨어진다 - 1차 이식은 **원본 그대로
관리자 전용으로만** 옮기면 되고, 학생 노출은 아예 만들지 않는다.

---

## 2. momolib에 필요한 테이블·라우터·화면·인증 매핑

### 2-1. momolib 현황 (실제 코드 확인, 문서 추측 아님)
- Flask 3.1.2 앱 팩토리(`app/__init__.py:14`), blueprint 10개, Flask-Login +
  Flask-Migrate(Alembic, `migrations/`) + PostgreSQL 운영(`config.py:16`,
  개발은 SQLite `instance/momolib_dev.db`로 override).
- **이미 존재하는 유사 파이프라인**: `BankQuestion`(`app/models/content_bank.py`,
  `type='vocab_quiz'`, JSON payload) → CMS 등록(`app/cms/routes.py:138-1059`,
  엑셀 대량 업로드 포함) → `LearningContent`/`QuizQuestion`(`library.py`) →
  LMS 코스 편성 → 학생 풀이 시 마일리지 지급(`app/learn/routes.py:198,246-247`).
- 인증: `User.role`(문자열) + `role_level`(정수, 낮을수록 상위),
  `@requires_role(*roles)`/`@requires_permission_level(level)`
  (`app/utils/decorators.py`, momoai_web과 이름은 같으나 별개 구현), 전역
  `public_paths` 화이트리스트 개념 없음(`/`, `/sw.js`만 예외, 나머지는
  라우트별 `@login_required`).
- 학생 식별: `Student` 모델이 로그인 계정(`User`)과 별도로 존재하는지는
  momolib 쪽에서 이번 조사로 직접 확인하지 못했다(momoai_web에서 확인한
  `teacher_id`/`user_id` 분리 패턴을 momolib에 그대로 가정하면 안 됨 -
  **구현 착수 전 momolib의 `Student`/`User` 관계를 반드시 재확인**).

### 2-2. 왜 기존 `BankQuestion` 파이프라인에 바로 얹으면 안 되는가
`BankQuestion`/`QuizQuestion`에는 aprolabs의 `student_exposure`/
`public_ready`/`level_status`/`hold_reason` 같은 **비공개 게이트 컬럼이
전혀 없다** - 이미 실제 학생에게 서비스되는 LMS 경로(`app/learn/routes.py`)와
연결돼 있으므로, phase35의 REVIEW_BOUNDARY·DRAFT_NOT_REVIEWED 48건을 이
테이블에 그대로 넣으면 **momolib의 기존 CMS/LMS 화면을 통해 즉시 학생에게
노출될 위험**이 있다(momolib은 별도 승인 없이 push 즉시 배포되는 구조라
더 위험 - 5절 참고). 지시사항 4번("REVIEW_BOUNDARY·HOLD·student_exposure=0·
public_ready=0을 학생에게 노출하는 경로는 만들지 마라")을 지키려면 **기존
파이프라인과 완전히 분리된 새 테이블 세트**가 필요하다.

### 2-3. 제안 테이블(신규, `app/vocab_quiz/models.py` 예정 - 이름은 momolib의
기존 blueprint 명명 규칙 확인 후 확정)
aprolabs 스키마의 검수·공개 게이트 컬럼을 **그대로 보존**해서 옮긴다(새로
설계하지 않음 - 지시사항 "추측으로 스키마를 만들지 마라"에 따라 원본 컬럼을
1:1 복제):
- `vocab_quiz_contents`: `content_id`(원본 그대로, UNIQUE), `lemma`,
  `canonical_definition`, `student_definition`, `example_sentence`,
  `example_target_form`, `source_version`, `student_exposure`(기본 0),
  `public_ready`(기본 0), `hold_reason`, `is_active` - aprolabs
  `vocabulary_contents`와 동일 컬럼 부분집합.
- `vocab_quiz_content_levels`: `content_id`(FK), `vocab_level`,
  `level_status`(`REVIEW_BOUNDARY`/`PROVISIONAL_AUTO`), `level_reason_json` -
  aprolabs `vocabulary_content_levels`와 동일.
- `vocab_quiz_pilot_sessions`/`vocab_quiz_pilot_attempts`: aprolabs의
  `vocabulary_multiformat_sessions`/`_responses`를 관리자 파일럿 전용으로
  축소 이식(1차 범위가 파일럿까지이므로 일반 학생 세션 테이블은 만들지 않음).

### 2-4. 라우터·화면·인증 매핑
| aprolabs | momolib(신규) | 인증 |
|---|---|---|
| `app/vocabulary_quiz/routers/review.py` (`/review`, `/review/card`, `/review/save`) | 신규 blueprint `app/vocab_quiz/routes.py`의 관리자 미리보기 화면 | momolib `@requires_role('super_admin','hq_manager')`(momolib 기존 최상위 역할 이름으로 교체 - `admin`이라는 momoai_web식 이름이 아니라 momolib 실제 role 문자열 사용) |
| `app/vocabulary_quiz/routers/multiformat.py`의 `_create_pilot_session`/`_create_l6_pilot_session` | 파일럿 세션 생성·응시 화면(관리자 본인이 미리 풀어보는 용도) | 동일 역할 가드 |
| `app/vocabulary_quiz/auth.py:require_admin` | momolib `app/utils/decorators.py`의 기존 `@requires_role`/`@requires_permission_level` 재사용(새 인증 계층 만들지 않음) | - |
| aprolabs `NOINDEX_HEADERS`(검색엔진 색인 방지) | 동일하게 응답 헤더에 적용 | - |

**경계**: 학생용 화면·라우트는 **1차 이식에 포함하지 않는다**(4절 참고). 위
표의 모든 화면은 momolib 관리자만 접근하며, 학생이 접근 가능한 어떤 화면·
API에도 이 신규 테이블을 연결하지 않는다.

---

## 3. Export/Import 설계

### 3-1. Export (aprolabs → 파일)
- 형식: JSON(행별), `content_id`를 자연키로 유지(momolib 쪽 PK는 새로 발급하되
  `content_id`는 원본 문자열 그대로 UNIQUE 컬럼에 보존 - **PK 재사용은
  하지 않는다**, PostgreSQL의 자동 증가 PK와 SQLite `id`를 혼동하지 않기
  위함).
- 대상: `source_version='schema_reading_l6_evidence_grounded_v1'` 48건 +
  대응 `vocabulary_content_levels` 48건(파일럿 세션은 momolib 쪽에서 새로
  생성하는 것이므로 세션/응답 데이터는 이식하지 않음 - 파일럿은 콘텐츠만
  이식하고 momolib에서 새로 응시).
- 파일: `data/export/vocab_quiz_l6_evidence_grounded_v1_export_<날짜>.json`
  (aprolabs 저장소에 산출, git 커밋 대상).

### 3-2. Dry-run Import (momolib)
phase35 적재 스크립트(`scripts/vocab/phase35_apply_l6_evidence_grounded.py`)의
게이트 구조를 그대로 재사용한다(이미 검증된 패턴):
1. **환경 게이트**: momolib은 PostgreSQL이라 `.env`의 `FLASK_ENV`/`DATABASE_URL`
   호스트가 개발/스테이징인지 확인(운영 DB에 dry-run조차 직접 붙지 않는 것을
   권장 - 별도 스테이징 DB 또는 운영 리드전용 복제본에서 먼저 실행).
2. **중복 방지/멱등성**: `content_id`에 `UNIQUE` 제약을 걸고
   `INSERT ... ON CONFLICT (content_id) DO NOTHING`(PostgreSQL UPSERT
   구문, SQLite의 `INSERT OR IGNORE`와 동등) - 재실행해도 중복 삽입 없음.
   phase35처럼 "이미 존재=스킵" 사전 점검도 병행.
3. **백업**: PostgreSQL이므로 SQLite Backup API 대신 `pg_dump --format=custom`
   으로 대상 테이블(또는 스키마 전체) 백업 + 복원 테스트(`pg_restore
   --list`로 백업 파일 자체 유효성 확인 - phase35의 "백업 파일
   integrity_check" 검증과 동등한 절차)를 임포트 직전에 반드시 수행.
4. **단일 트랜잭션**: 신규 테이블 2개(`vocab_quiz_contents`,
   `vocab_quiz_content_levels`) INSERT를 하나의 트랜잭션으로 커밋, 실패 시
   전체 ROLLBACK.
5. **사후 검증**: 신규 48건 전부 `student_exposure=0 AND public_ready=0`,
   기존 momolib 테이블(`bank_questions` 등) 행수·체크섬 불변, FK 위반 0건.

### 3-3. 재실행 멱등성
`content_id`가 이미 momolib에 있으면 스킵(업데이트도 하지 않음 - 내용을
바꾸려면 별도 "갱신" 절차를 명시적으로 설계해야 함, 이번 1차 이식 스크립트는
"최초 삽입"만 다룬다). 이는 phase26/35에서 이미 검증된 원칙과 동일하다.

### 3-4. 백업·복구 범위
- 임포트 직전 `pg_dump` 1개(대상 스키마) - 복구 시 이 파일로 되돌리면 신규
  48건이 없는 상태로 정확히 복원됨.
- aprolabs 쪽은 이번 이식으로 아무것도 바뀌지 않으므로(읽기 전용 export)
  별도 백업 불필요.

---

## 4. 1차 이식 범위 (관리자 전용 미리보기 + 파일럿까지)

**포함**: 2-3/2-4절의 신규 테이블 + 관리자 전용 라우트·화면만. `content_id`
48건을 momolib에 비공개로 들여와 **momolib 관리자가 브라우저로 미리 보고,
파일럿 세션을 momolib 안에서 직접 풀어볼 수 있게** 하는 것까지가 1차 목표.

**절대 하지 않는 것(구조적으로 보장)**:
- `student_exposure`/`public_ready`를 0이 아닌 값으로 바꾸는 코드 경로를
  만들지 않는다(그런 UI 버튼·API 자체를 1차 범위에서 구현하지 않음).
- 기존 `BankQuestion`/`LearningContent`/LMS 코스 편성에 이 신규 콘텐츠를
  연결하지 않는다(2-2절 이유).
- 학생이 로그인한 상태에서 도달 가능한 어떤 URL에도 신규 테이블을 노출하지
  않는다.
- HOLD 상태 콘텐츠(적재 안 된 22건)는 이식 대상 자체가 아니다(48건만).

---

## 5. 구현 순서·필요 파일·자동 테스트·예상 위험

### 5-1. 구현 순서
1. momolib의 `Student`/`User` 관계, 정확한 role 문자열 목록(super_admin 등
   정확한 이름) 재확인 - 이번 조사가 못 확인한 부분(2-1절 마지막).
2. `data/export/vocab_quiz_l6_evidence_grounded_v1_export_<날짜>.json` 생성
   (aprolabs, 읽기 전용 스크립트).
3. momolib에 Alembic 마이그레이션 1개 추가(`vocab_quiz_contents`,
   `vocab_quiz_content_levels`, `vocab_quiz_pilot_sessions`,
   `vocab_quiz_pilot_attempts` 4테이블 생성 - **아직 적용하지 않음, 이번
   단계는 설계까지만**).
4. dry-run import 스크립트 작성(3-2절 게이트 구조).
5. 관리자 전용 blueprint(`app/vocab_quiz/`) + 미리보기·파일럿 응시 화면.
6. 자동 테스트 작성(momolib에 테스트가 전혀 없으므로 이 기능만이라도 최소
   커버리지 확보 - 5-3절).
7. 스테이징/개발 DB에서 dry-run → 검토 → 별도 PR/커밋으로 병합(불필요한
   기존 미푸시 커밋과 섞이지 않게, 6절 참고) → 실제 적용은 **사용자 승인
   후 별도 단계**.

### 5-2. 필요한 파일(신규)
- `aprolabs/scripts/vocab/export_l6_evidence_grounded_for_momolib.py`
- `momolib/migrations/versions/<revision>_add_vocab_quiz_tables.py`
- `momolib/app/vocab_quiz/__init__.py`, `models.py`, `routes.py`
- `momolib/scripts/import_vocab_quiz_dryrun.py`(3-2절 게이트 구현)
- `momolib/tests/test_vocab_quiz_import.py`, `test_vocab_quiz_admin_routes.py`

### 5-3. 자동 테스트(momolib에 최초로 추가하는 셋)
- 임포트 스크립트: 멱등성(재실행 시 삽입 0건), 백업 파일 유효성, FK 무결성,
  `student_exposure`/`public_ready` 전부 0 검증(phase35 테스트 패턴 재사용).
- 라우트: 비로그인/학생 role로 접근 시 403·401, 관리자 role로만 200 -
  "학생 노출 경로 없음"을 코드로 고정.
- 회귀: 기존 `bank_questions`/`quiz_questions`/LMS 관련 테이블 행수가
  임포트 전후로 변하지 않는지(별도 파이프라인이라는 것의 자동 확인).

### 5-4. 예상 위험
| 위험 | 내용 | 대응 |
|---|---|---|
| **무검증 자동배포** | momolib은 `main` push 즉시 GitHub Actions가 배포(`deploy.yml`), 테스트 게이트 없음 | 이 기능은 별도 브랜치에서 충분히 검토 후 병합, 병합 직후 자동배포된다는 것을 인지하고 스케줄링(예: 트래픽 낮은 시간) |
| **기존 미푸시 커밋 혼입** | 현재 momolib `main`이 origin보다 1개 커밋 앞섬(아바타 캐릭터 7종 추가, 무관한 변경) | 이 커밋을 먼저 별도로 push하거나, 최소한 이식 PR 리뷰 시 diff에 포함되지 않는지 확인 |
| **기존 파이프라인과의 스키마 충돌 오인** | `BankQuestion.type='vocab_quiz'`가 이미 있어 혼동 가능 | 완전히 별도 테이블(2-3절)로 분리, 이름도 `vocab_quiz_*`로 명확히 구분 |
| **momolib DB가 운영 PostgreSQL** | 개발 SQLite와 동작이 다를 수 있음(UPSERT 문법, 트랜잭션 격리) | dry-run을 반드시 PostgreSQL(스테이징)에서 실행, SQLite 개발 DB만으로 검증하지 않음 |
| **Student/User 관계 미확인** | 관리자 프리뷰 시 "임시 학생 계정" 필요할 수 있는데 momolib 구조를 아직 못 확인 | 구현 착수 전 1순위로 확인(5-1절 1번) |
| **REVIEW_BOUNDARY 의미 손실** | 새 팀원이 momolib 쪽 컬럼만 보고 "검수 완료"로 오인할 위험 | aprolabs와 동일하게 `expert_review_status`를 `level_reason_json`(또는 별도 컬럼)에 명시적으로 유지 |

---

## 6. 다른 프로젝트의 미완성 변경 확인

- **momolib**: `git status`상 작업트리는 깨끗하나, `main`이 origin보다
  **1개 커밋 앞서 있음**(아바타 캐릭터 7종 추가 및 독서취향 유형명 수정,
  11개 파일 - 이미지 8개 + 코드 2줄 + 모델 1줄, 어휘 퀴즈와 무관). 이
  커밋이 push되지 않은 상태로 남아 있으므로, **이식 작업을 위해 momolib에
  커밋을 추가하고 push하면 이 무관한 커밋도 함께 배포된다** - 이식 착수
  전에 사용자가 이 커밋을 검토·별도 push할지 결정해야 한다.
- **aprolabs**: main이 origin보다 6개 커밋 앞섬(전부 phase30~35 어휘 근거
  집필/적재 작업, 서로 관련됨) - 이번 조사에서 새로 추가한 파일은 없다
  (이 보고서 자체 1건만 신규).

## 7. 변경 없음 확인
이번 단계는 momolib·aprolabs 어느 쪽에도 코드·DB·설정을 수정하지 않았다.
연구 DB는 SELECT/PRAGMA만 실행했고, momolib은 조사 에이전트가 파일을
읽기만 했다(Read/Grep, Write/Edit 없음). git commit도 이 보고서 파일
1건에 대해서만 수행한다(아래 커밋 참고).
