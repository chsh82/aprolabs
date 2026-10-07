# 스키마리딩 3단계: 두 DB의 연결 dry-run 준비

근거: 사용자가 전달한 `reports/schema_reading_phase2_readonly_audit_20260924.md`
요약. 보고서 원문은 현재 작업 공간에 없으므로 아래 지시문을 실행하는
Claude Code가 반드시 원문과 실제 모델을 대조해야 한다.

## v3 파일의 위치

`schema_reading_integration_revised_after_server_audit_v3.md`는
**`schema_reading_phase1_baseline_v1.zip`의 최상위**에 있으며,
`internal_vocab_upper_restore_v1.zip`에는 없다. 두 ZIP은 역할이 다르다.
원본 감사 ZIP과 스키마리딩 참고자료 ZIP의 체크섬 접두부는 전달받은
보고서와 일치한다: `internal_vocab_upper_restore_v1.zip` = `ed83a8f9…`,
`스키마리딩 참고자료.zip` = `616abed0…`. 따라서 2단계 읽기 전용 조사는
다시 할 필요가 없고, 대체 근거로 진행한 결과를 출발점으로 삼는다.

## 업데이트된 판단

- 이미 존재하는 `app/literacy`의 `terms/examples/collection_runs`를
  재사용한다. 원본 어휘 XLSX가 실제 raw와 바이트 동일하므로 원천 전체를
  새로 이식하는 작업은 이번 dry-run의 목적이 아니다.
- 148건의 표제어 완전 일치는 **뜻 연결 검토 대상의 최대 초기 집합**이다.
  148건을 전부 링크하거나 새 콘텐츠로 승격한다고 가정하지 않는다.
- 1:N 가능성이 낮다는 집계만으로 nullable `literacy_term_id` 컬럼을
  확정하지 않는다. 하나의 콘텐츠에 V와 S 출처가 동시에 연결되는지,
  동일 표제어의 서로 다른 뜻이 있는지, 변경될 수 있는 ID인지 실데이터와
  코드에서 확인한다. SQLite의 두 별도 DB 간 일반 FK는 제공되지 않는다.
- L5=고1, L6=고2~3이 확정 정책이다. 이전 기준으로 Gemini가 저장한
  `app/literacy`의 62건은 자동 확정 배정에서 제외하고 근거를 모아
  검토 대상으로 둔다. `vocabulary_content_levels`의 L5/L6은 0건으로
  보고되었으나 최종 스크립트 실행 전 다시 확인한다.
- 보고서에서 언급한 **615건의 정확한 정의와 62건과의 포함관계**는
  보고서 원문에서 확인한다. 이유 코드와 함께 dry-run 제외 목록으로
  출력하고, 숫자만 보고 같은 집합으로 간주하지 않는다.
- 옛 `어휘퀴즈DB.xlsx` raw와 사용자 ZIP은 해시가 다르다.
  두 버전을 구분하고 옛 퀴즈 338행은 이번 연결 dry-run 대상에서 제외한다.
- 원천 정의의 AI 자동 보강 출처가 재현되지 않으므로 보강 정의를
  사람이 승인한 canonical 정의로 취급하지 않는다. 일치한 문자열이
  같은 의미라는 근거로 충분하지 않을 수 있다.

## Claude Code 지시문

```text
스키마리딩 통합 3단계의 읽기 전용 dry-run을 준비해줘.
지난 보고서 reports/schema_reading_phase2_readonly_audit_20260924.md를
원문까지 먼저 읽고, 현재 research 서버/로컬의 실제 SQLite 스키마를
대조해줘. 참고 문서는 schema_reading_integration_revised_after_server_audit_v3.md와
schema_reading_phase3_dryrun_handoff_v4.md다. v3은
schema_reading_phase1_baseline_v1.zip 최상위 또는 별도 전달된 MD에 있고
internal_vocab_upper_restore_v1.zip 내부에는 없다. 2단계 조사는 반복하지 마.

1. literacy.db(terms/examples/collection_runs)와
   vocabulary_quiz_research.db(vocabulary_contents, vocabulary_content_levels)의
   실제 PK/자연키/뜻풀이/품사/source/level_status 컬럼을 확인해라.
   DB 파일은 SQLite mode=ro로만 열고 APP_ENV=research와 경로를 확인해라.
2. 기존 보고의 완전일치 148건을 기준 집합으로 재현하고 각각
   vocabulary content ID, literacy term ID, 양쪽 표제어·품사·뜻,
   V/S source, 현재 레벨, 연결 기수성, 제외 이유를 출력하는 분석기를
   만들어라. 이 단계는 임포터의 예측 분석이므로 DB 쓰기 없음.
   표제어만 같거나 AI 자동 보강 정의만 같으면 자동 승인하지 마.
3. 연결 관계가 0:1, 1:1, 1:N, N:M 중 무엇인지 실제 행으로 검사해
   nullable literacy_term_id 단일 컬럼으로 출처·다의어·V/S 중첩을
   보존할 수 있는지 결정하고 반례를 함께 보고해라. 두 SQLite 파일 간
   FK를 걸 수 없으므로 교차 DB ID의 유효성 확인 방법도 설계해라.
4. app/literacy의 옛 레벨 정책으로 Gemini가 확정한 62건과 보고된
   615건을 원문 기준으로 각각 정의하고, 두 집합의 교집합과 합집합,
   제외 원인을 보고해라. L5/L6 학년 자동 치환/기존 62건 자동 재분류
   금지. 현행 퀴즈 DB L5/L6 0건을 직접 SQL로 재확인해라.
5. 두 원본 ZIP과 literacy.db raw에 대한 기존 SHA-256 대조 결과를
   유지하고, raw/의 어휘퀴즈DB.xlsx와 참고자료 ZIP의 서로 다른 버전을
   식별해라. 이번 dry-run에서 338개 옛 문항의 적재·평가를 제외해라.
6. dry-run 산출물을 CSV 또는 JSONL의 행별 판정표와 Markdown 요약으로
   저장해라. APPROVABLE_CANDIDATE, AMBIGUOUS_SENSE,
   MULTIPLE_LINKS, OLD_LEVEL_REVIEW, UNVERIFIED_DEFINITION,
   NO_MATCH 등 근거가 보이는 상태를 사용하고, 콘텐츠별 실제 승인 건수를
   섣불리 주장하지 마. 최초/재실행 예측이 동일한지 검증해라.
7. 연구용 DB 백업과 기존 테이블 체크섬, student_exposure/public_ready
   무변경, integrity_check/FK 점검의 실제 적용 절차를 제안해라.
   이번 요청에서는 백필, 컬럼 추가, 레벨·라벨 변경, Git 배포를
   실행하지 마. 148건을 실제로 연결하는 SQL도 실행하지 마.

결과 보고: 148건의 의미 검증 상태별 수, 양쪽 DB의 연결 기수성 증거,
62건·615건의 정의와 중첩, L5/L6 정책 영향, 단일 컬럼/관계 테이블
선택의 반례 및 다음 마이그레이션 게이트. 현재 DB 읽기 전용 유지.
```
