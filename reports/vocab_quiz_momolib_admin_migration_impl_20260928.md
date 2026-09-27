# 어휘 퀴즈 momolib 1차 이식(관리자 전용) 구현 완료 보고

- 일자: 2026-09-28
- 위치: `C:\Users\aproa\momolib`, 브랜치 `feat/vocab-quiz-admin-migration`
  (origin/main 기준으로 분기 - 미푸시 아바타 커밋 `91de794` 미포함)
- push·main 병합·운영 PostgreSQL 접속: **없음**. 로컬 브랜치에만 커밋
  (`77dd11e`).

## 1. 격리 브랜치

```
git checkout -b feat/vocab-quiz-admin-migration origin/main
```
origin/main(`e04834a`) 기준으로 분기했다 - 로컬 main이 갖고 있던 미푸시
아바타 커밋(`91de794`, 아바타 캐릭터 7종 + 이미지 8개, 어휘 퀴즈와 무관)은
이 브랜치에 전혀 포함되지 않는다. 이번 작업을 나중에 push하더라도 그
커밋과는 완전히 별개다(그 커밋을 별도로 push할지는 사용자가 결정).

## 2. 신규 PostgreSQL 테이블(기존 파이프라인과 완전 분리)

`migrations/versions/37b334ba3fc8_add_vocab_quiz_tables_isolated_admin_.py` -
`flask db migrate` 자동 diff가 **신규 테이블 5개뿐**임을 확인(기존
`bank_questions`/`quiz_questions`/`curriculum*` 등 어떤 테이블도 변경 없음):

| 테이블 | 역할 | aprolabs 원본과의 관계 |
|---|---|---|
| `vocab_quiz_contents` | 어휘 콘텐츠(표제어·정의·예문) | `vocabulary_contents` 컬럼 그대로(content_id/source_version/student_exposure/public_ready/hold_reason 등) |
| `vocab_quiz_content_levels` | 레벨 상태 | `vocabulary_content_levels` 그대로(level_status CHECK 'PROVISIONAL_AUTO'/'REVIEW_BOUNDARY' 포함) |
| `vocab_quiz_pilot_items` | 파일럿 문항(40+40) | `vocabulary_multiformat_items` 중 파일럿 부분집합 - public_payload_json/answer_payload_json 분리 그대로 |
| `vocab_quiz_pilot_sessions` | 관리자 파일럿 응시 세션 | `vocabulary_multiformat_sessions`를 파일럿 전용으로 축소 |
| `vocab_quiz_pilot_attempts` | 문항별 응답·채점 | `vocabulary_multiformat_responses`를 파일럿 전용으로 축소 |

기본값: `student_exposure=False`, `public_ready=False`(모든 신규 행) - 이
값을 바꾸는 라우트·스크립트를 이번 범위에 만들지 않았다.

## 3. Export/Import (재현 가능, 건수·해시 고정)

- aprolabs: `scripts/vocab/export_l6_content_for_momolib.py` - 연구 DB
  **전체 기준선**(vocabulary_contents=5,950, vocabulary_multiformat_items=
  1,369, 파일럿 40+40)을 먼저 검증(하나라도 다르면 즉시 실패)한 뒤, 실제
  이식 대상인 **L4~L6 신규 227건**(5개 source_version)만 분리해서 내보냄.
  재실행해도 동일 sha256(contents/levels 각각)이 나옴을 확인(재현성).
- momolib: `import_vocab_quiz_export.py` - 기본 dry-run(SAVEPOINT 후 항상
  ROLLBACK), `--apply`만 실제 COMMIT. 자연키(content_id/item_id) 규칙:
  DB 없음→삽입, 있고 내용 동일→SKIP, 있고 내용 다름→**FAIL**(그 자리에서
  즉시 중단, 조용히 덮어쓰지 않음).
  - **버그 발견·수정**: 처음에는 DB에 저장된 해시 캐시 컬럼끼리 비교했는데,
    누가 UPDATE로 값을 직접 바꿔도 캐시가 그대로면 충돌을 놓치는 것을
    테스트 중 실제로 발견해, 매번 **현재 DB 값으로 해시를 다시 계산**하는
    방식으로 고쳤다.
  - **버그 발견·수정**: SQLite export(0/1 정수)와 Postgres(True/False)의
    불리언 표현 차이로 거짓 충돌이 나는 것도 발견해, 해시 계산 전에
    불리언 필드를 정규화하도록 고쳤다.

### 실제 적재 예상치(--apply 1회 실행 시)
| 테이블 | 예상 삽입 건수 |
|---|---:|
| vocab_quiz_contents | 227 |
| vocab_quiz_content_levels | 227 |
| vocab_quiz_pilot_items(l4l5) | 40 |
| vocab_quiz_pilot_items(l6) | 40 |
| **합계** | **534행** |

## 4. 관리자 화면 (`app/vocab_quiz/`, prefix `/vocab-quiz`)

전 라우트 `@login_required` + `@requires_role('super_admin', 'hq_manager')`
(momolib 기존 데코레이터 그대로 재사용, 새 인증 계층 없음).

| 화면 | 라우트 |
|---|---|
| 대시보드(source_version별 건수, 파일럿 상태) | `GET /vocab-quiz/` |
| 콘텐츠 목록/미리보기 | `GET /vocab-quiz/contents`, `/contents/<content_id>` |
| 파일럿 정보·시작 | `GET /vocab-quiz/pilot/<l4l5\|l6>`, `POST .../start` |
| 파일럿 응시(문항 표시, 정답 미포함) | `GET /vocab-quiz/pilot/session/<id>` |
| 문항 채점(제출 후에만 정답 공개) | `POST /vocab-quiz/pilot/session/<id>/answer` |
| 세션 완료/결과 | `POST .../complete`, `GET .../result` |

파일럿 세션 시작 시 DB의 item_id 40개 집합을 `data/vocab_quiz/pilot_*.json`
매니페스트와 다시 대조하고, 불일치하면 `PILOT_BATCH_INTEGRITY_ERROR`(500)로
거부한다(aprolabs `multiformat.py`의 방어 로직을 그대로 재구현). 기존
CMS `templates/cms/vocab/*`(BankQuestion 기반)나 LMS 학생 화면과는 라우트·
템플릿·모델 어디에서도 연결하지 않았다.

## 5. 통합 테스트 (독립 로컬 PostgreSQL, 38건 전부 통과)

**환경**: 이 머신에 PostgreSQL이 전혀 없어(선택에 따라) Chocolatey 설치를
시도했으나 관리자 권한 문제로 실패, EDB의 portable 16.4 바이너리(zip)를
사용자 공간에 내려받아 initdb+pg_ctl로 별도 포트(55432)에 띄웠다(관리자
권한 불필요, 운영 서버와 무관한 완전히 독립된 인스턴스, 테스트 종료 후
정상 종료함).

- `tests/test_vocab_quiz_import.py`(16건): dry-run 롤백 확인, apply 삽입
  227/227/40/40, 재실행 멱등성(신규 0건), 4건 삭제 후 부분적재 복구(정확히
  4건만 채움), **내용 충돌 시 종료 코드 2로 중단 + 변조값 보존 확인**,
  원복 후 재성공.
- `tests/test_vocab_quiz_integration.py`(22건): 마이그레이션
  upgrade→downgrade(테이블 5개 완전 제거 확인)→재upgrade, 비로그인
  302 차단, 비관리자(teacher) 403 차단, 관리자 200, 콘텐츠 미리보기,
  파일럿 응시 화면에 `answer_payload_json`/`correct_option` **미노출**
  확인, 40문항 전부 응답·채점(전부 정답 응답 시 전부 True), 세션 완료·
  결과 화면, **매니페스트 불일치 시뮬레이션(문항 1건 삭제) → 500 +
  `PILOT_BATCH_INTEGRITY_ERROR` 확인 → 복구 후 정상화**, 기존
  `bank_questions` 행수 불변, 테스트가 만든 계정 2명만 `users` 증가.

```
총 16건 중 16건 통과 (test_vocab_quiz_import.py)
총 22건 중 22건 통과 (test_vocab_quiz_integration.py)
```

## 6. 복구 절차

- **스키마 롤백**: `flask db downgrade`(신규 테이블 5개만 제거, 기존
  테이블 무관 - 테스트로 실제 확인함) → 필요 시 `flask db upgrade`로
  재적용.
- **데이터 롤백**: 신규 테이블 5개를 지우면(`DROP TABLE` 또는 downgrade)
  이식된 데이터가 전부 사라진다 - 기존 테이블은 애초에 건드리지 않으므로
  별도 복구 불필요.
- **재적재**: `import_vocab_quiz_export.py --apply`를 몇 번 다시 실행해도
  멱등적으로 같은 결과(534행)가 된다.
- **완전 되돌리기**: 이 브랜치 자체를 삭제하거나 main에 병합하지 않으면
  origin/main·운영 서버는 전혀 영향받지 않는다(push하지 않았으므로).

## 7. 변경 파일 목록 (momolib, 22개, 10,864줄 추가/삭제 0줄)

```
M  app/__init__.py                              (blueprint 등록 2줄)
M  app/models/__init__.py                       (model import 3줄)
A  app/models/vocab_quiz.py
A  app/vocab_quiz/__init__.py, manifests.py, routes.py
A  app/templates/vocab_quiz/*.html (7개)
A  data/vocab_quiz/pilot_l4l5_manifest_v1.json, pilot_l6_manifest_v1.json
A  data/vocab_quiz_import/vocab_quiz_momolib_export_v1/*.json (3개)
A  import_vocab_quiz_export.py
A  migrations/versions/37b334ba3fc8_add_vocab_quiz_tables_isolated_admin_.py
A  tests/test_vocab_quiz_import.py, test_vocab_quiz_integration.py
```
aprolabs 쪽: `scripts/vocab/export_l6_content_for_momolib.py` +
`data/export/vocab_quiz_momolib_export_v1/*`(이미 커밋 `a33bf8e`).

## 8. 명시적으로 하지 않은 것
- 운영 PostgreSQL 접속·변경 (전부 로컬 portable 인스턴스에서만 검증)
- `main` 병합, push, 서버 배포
- 학생 노출(`student_exposure`/`public_ready`를 바꾸는 코드)
- 퀴즈 문항 생성/기존 CMS·LMS 라우트·모델과의 연결
- literacy.db·aprolabs 연구 DB에 대한 쓰기(이번 단계는 export 스크립트로
  읽기만 함)
