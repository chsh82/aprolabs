# 스키마리딩x어휘 DB 통합 — 22단계: "정기"(SR_L4CORE_4786) student_definition 수정 적용 + 재판정

- 작성일: 2026-09-26 (`date` 명령으로 시스템 현재 날짜 확인)
- 범위: (작업1) phase21이 낸 "정기" 수정안을 실제 적용 전 전면 재대조·확정,
  (작업2) `vocabulary_quiz_research.db`(서버, research)에 게이트 기반 단일 행
  UPDATE 실제 적용(phase13/14/21 패턴 재사용), (작업3) phase16 스크립트
  재확인 + 회귀 테스트 신설 + "정기" 2문항 독립 재판정 및 40건 판정표 갱신.
- 재조사하지 않고 그대로 인용한 사실(지시받은 그대로): phase21 보고서
  (`reports/schema_reading_phase21_literacy_10fix_and_jeonggi_root_cause_20260926.md`)의
  원인 규명, 원천/문항/콘텐츠 3자 텍스트, dry-run 제안
  (`data/import/schema_reading_phase21_jeonggi_student_definition_fix_dryrun_20260926.json`),
  phase16 스크립트 31~32행, phase20 40건 판정표의 HOLD 2건.
- **절대 제약 준수**: `literacy.db`는 이번 세션 내내 `mode=ro`로만 재조회했다(로컬·서버
  둘 다). `vocabulary_quiz_research.db` 쓰기는 작업2의 단일 트랜잭션 UPDATE 1회뿐이다
  (그 전후의 모든 조회는 `mode=ro` 또는 읽기 전용 SELECT). `student_exposure`/
  `public_ready`는 어디서도 변경하지 않았다(전체 합계 0 유지, 아래 검증 참고).
  40건 판정표 중 대상 2건 외 38건은 프로그램적으로 스킵되도록 코드에서 강제했다
  (`TARGET_IDS` 집합 밖 행은 절대 수정 안 됨). git commit/push는 하지 않았다.

---

## 0. 요약 (호출한 에이전트용)

1. **작업1 확정**: 재대조 결과 원천(literacy.db id=4786, 로컬·서버 byte-identical)·
   문항 2건(`vocabulary_multiformat_items`)·현재 콘텐츠(`vocabulary_contents`) 값을
   모두 재조회했고, phase21이 제안한 새 값(`"기한이나 기간이 일정하게 정해져 있는
   것"`)은 품사(명사)·레벨(4)·예문(`"우리 반은 매달 첫째 주에 정기 모임을
   엽니다."`)과 의미 충돌이 없음을 확인했다. **적용 대상으로 확정.**
2. **작업2 적용 완료, 전 게이트 PASS**: 백업(`vocabulary_quiz_research.db.bak-phase22-jeonggi-fix-pre-20260926-142247`,
   SHA-256 `a20afa8f80273bc67a8a1f7b65203833cb1e4cd0519b6ae746d7d14f45cbb07b`) →
   하드가드(현재값이 정확히 구버전 문구와 일치, PASS) → 단일 트랜잭션 UPDATE
   1건 → 나머지 5,819건 체크섬 불변/`vocabulary_content_levels`(5,820행)·
   `vocabulary_multiformat_items`(1,329행) 불변/`integrity_check` ok/
   `foreign_key_check` 위반 0/`student_exposure`+`public_ready` 합계 여전히 0/
   재실행 멱등성(2회차 실행 시 대상 0건, "이미 새 값"으로 스킵) 전부 확인.
   변경된 값: `SR_L4CORE_4786.student_definition`
   `"일정한 기간마다 되풀이하도록 정한 것"` → `"기한이나 기간이 일정하게
   정해져 있는 것"` (다른 컬럼은 전부 무변경).
3. **작업3**: phase16 스크립트 31~32행은 이미 새 값(재확인 완료). `scripts/`,
   `app/`, `tests/` 전체를 스캔해 구버전 문구가 (의도적 하드가드 상수 2개
   파일을 제외하고) 어디에도 남아있지 않음을 확인. 신규 회귀 테스트
   `tests/test_phase22_jeonggi_definition_regression.py` 작성, **17/17 PASS**.
   "정기" 2문항 재판정: 적용 후 문항 텍스트와 `vocabulary_contents.student_definition`이
   완전히 동일한 문자열로 일치하고 원천과도 핵심 의미가 일치함을 재확인해
   **HOLD → PASS로 전환**. 40건 판정표(CSV/JSONL) 갱신, 나머지 38건은
   프로그램적으로 손대지 않았음(전체 40건 중 PASS 40/HOLD 0으로 재확인).
4. **가장 중요한 리스크**: (a) 이 수정은 서버 `vocabulary_quiz_research.db`에만
   적용됐다 — 로컬에는 이 DB 자체가 없어(research DB는 서버 전용) 동기화 이슈가
   구조적으로 없다(phase21의 literacy.db 동기화 이슈와는 다른 상황). (b) 판정표
   갱신은 문서(CSV/JSONL) 수준일 뿐, 이 40문항 자체가 아직 정식 `vocabulary_items`/
   운영 편입 절차를 거친 것은 아니다(phase18은 이미 `vocabulary_multiformat_items`에
   `student_exposure=0/public_ready=0`으로 비공개 적재만 완료한 상태 — phase20/21
   문서가 이미 확인한 사실을 재확인, 이번 세션이 새로 바꾼 것 없음).

---

## 1. 작업1 — 재대조 + 확정

### 1-1. 원천(literacy.db id=4786) 재조회 (mode=ro, 로컬+서버)

```
LOCAL  literacy.db id=4786: (4786, '정기', '기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 기간.', '명사', 4, 'schemareading-tooldict', 'L4-129')
SERVER literacy.db id=4786: (4786, '정기', '기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 기간.', '명사', 4, 'schemareading-tooldict', 'L4-129')
```

로컬-서버 byte-identical(phase21이 이미 확인한 대로, 10건 사자성어 UPDATE와
무관한 행이라 불변). **원천 정의는 "기한이나 기간이 일정하게 정하여져 있는
것. 또는 그 기한이나 기간."**

### 1-2. 파일럿 문항 2건 재조회 (서버, `vocabulary_multiformat_items`, mode=ro)

| item_id | item_type | prompt(요약) | 정답 옵션(4번) | explanation |
|---|---|---|---|---|
| `MF_A_SC_SRL4L5PILOT_20260925_L4_008` | MEANING_CHOICE | '정기'의 뜻으로 가장 알맞은 것은? | `기한이나 기간이 일정하게 정해져 있는 것` | `'정기'는 '기한이나 기간이 일정하게 정해져 있는 것'이라는 뜻입니다.` |
| `MF_C_SC_SRL4L5PILOT_20260925_L4_008` | CONTEXT_MEANING | 우리 반은 매달 첫째 주에 【정기】 모임을 엽니다. | `기한이나 기간이 일정하게 정해져 있는 것` | `문장 속 '정기'는 '기한이나 기간이 일정하게 정해져 있는 것'을 뜻합니다.` |

두 문항 모두 `correct_option=4`, `answer_payload_json={"correct_option": 4}`,
`source_content_id='SR_L4CORE_4786'`. 오답 3개(공간/집단/하위 정의)는 '정기'와
의미 영역이 명확히 다름 — phase20이 이미 확인한 정답 유일성/오답 배제력에는
변화 없음(이번 세션도 재확인).

### 1-3. 현재 `vocabulary_contents` 재조회 (서버, UPDATE 전, mode=ro)

```
('SR_L4CORE_4786', '정기', '명사', '기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 기간.',
 '일정한 기간마다 되풀이하도록 정한 것',   <- student_definition (구버전)
 '우리 반은 매달 첫째 주에 정기 모임을 엽니다.', '정기', 0, 0, 1)
```

`canonical_definition`은 원천(literacy.db)과 정확히 동일 — 이는 처음부터
정확했고 이번에 건드리지 않는 컬럼이다. 문제는 `student_definition`
(구버전, `"일정한 기간마다 되풀이하도록 정한 것"`)에만 있었다 — phase21의
분석과 정확히 일치.

### 1-4. 다른 필드와의 의미 충돌 여부 확인

- **품사(pos='명사')**: 새 정의 `"기한이나 기간이 일정하게 정해져 있는
  것"`도 명사구이며 품사 불일치 없음.
- **레벨(vocab_level=4)**: 정의 문구 교체가 난이도/학년 매핑에 영향을 주는
  요소(어휘 자체, 예문, 출처)를 전혀 건드리지 않으므로 레벨 재산정 불필요.
- **예문**(`"우리 반은 매달 첫째 주에 정기 모임을 엽니다."`, `example_target_form='정기'`):
  "매달 첫째 주"라는 반복 주기를 언급하는 예문이지만, 이는 "정기 모임"이라는
  실제 쓰임(고정된 주기로 열리는 모임)을 보여주는 것이지 정의 문구 자체에
  "되풀이"를 명시할 필요를 만들지 않는다 — 새 정의("기한/기간이 고정되어
  있다는 것")로도 이 예문은 자연스럽게 설명된다("정기 모임"="시기가 고정된
  모임"). 예문을 바꿀 필요 없음, 충돌 없음.
- **문항 텍스트**(1-2절): 이미 새 값과 완전히 동일한 문자열을 쓰고 있어,
  콘텐츠 쪽만 새 값으로 맞추면 3자(원천/문항/콘텐츠)가 정합해진다.

**결론: 충돌 없음. phase21이 제안한 새 정의를 적용 대상으로 확정한다.**

---

## 2. 작업2 — 게이트 기반 단일 행 적용

### 2-1. GATE 0 — APP_ENV=research 재확인 (.env + 실행 중 프로세스, 재확인)

```
서버 .env: APP_ENV=research, VOCABULARY_QUIZ_DB_PATH=/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
실행 중 프로세스(PID 1321697, uvicorn app.main:app): /proc/<pid>/environ
  APP_ENV=research, VOCABULARY_QUIZ_DB_PATH=/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
```
`.env`와 실행 중 프로세스 environ이 byte-for-byte 일치 → **PASS**.

### 2-2. 백업 (`vq_research_backup.py` 재사용, 서버에서 실행)

```
GATE 1(APP_ENV=research) PASS / GATE 2(db-path basename) PASS
백업 경로: /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase22-jeonggi-fix-pre-20260926-142247
원본 행수: {'vocabulary_contents': 5820, 'vocabulary_content_levels': 5820}
백업 행수: {'vocabulary_contents': 5820, 'vocabulary_content_levels': 5820}  -> 일치
백업 PRAGMA integrity_check: ok
백업 PRAGMA foreign_key_check 위반: 0건
백업 SHA-256: a20afa8f80273bc67a8a1f7b65203833cb1e4cd0519b6ae746d7d14f45cbb07b
백업 크기: 12,541,952 bytes
최종: PASS
```

### 2-3. 신규 적용 스크립트 및 게이트 설계

`scripts/vocab/phase22_apply_jeonggi_student_definition_fix.py`(신규, phase13/14
패턴 재사용)를 작성해 서버 `/tmp/`로 scp 후 실행했다. 게이트 구성:

1. GATE 1: `.env` + 실행 중 프로세스 environ 재확인(APP_ENV=research 일치)
2. GATE 2: db-path basename 하드가드(`vocabulary_quiz_research.db`)
3. **GATE 3 (핵심 하드 가드)**: `content_id='SR_L4CORE_4786'`의 현재
   `student_definition`이 정확히 기대 구버전 문자열
   (`"일정한 기간마다 되풀이하도록 정한 것"`)과 일치하는지 확인.
   - 이미 새 값이면 → 멱등 재실행으로 판단, 대상 0건, 트랜잭션 없이 종료.
   - 구버전도 새 값도 아닌 제3의 값이면 → **즉시 중단**, 아무것도 쓰지 않음
     (지시받은 안전장치, 이번 실행에서는 발동하지 않았다 — 실제 값이
     정확히 기대 구버전과 일치했다).
4. GATE 4: 단일 트랜잭션 UPDATE(`student_definition` 컬럼만,
   `WHERE content_id=? AND student_definition=?`로 이중 조건, `rowcount==1` 검증
   실패 시 롤백)
5. GATE 5: 커밋 후 검증(아래 2-5절)

### 2-4. 적용 실행 결과 (1회차, `--apply` 개념 없이 게이트 자체가 실행 = 적용)

```
GATE 1 PASS (.env와 실행 중 프로세스 environ byte-for-byte 일치)
GATE 2 PASS
UPDATE 전 스냅샷: vocabulary_contents(제외 1건)=5819행 sha256=34da919b...d122958
UPDATE 전 스냅샷: vocabulary_content_levels=5820행 sha256=b97b1b4b...c5a8cb
UPDATE 전 스냅샷: vocabulary_multiformat_items=1329행 sha256=fb07e6df...52a6ad7a14
GATE 3: 현재 student_definition='일정한 기간마다 되풀이하도록 정한 것'
GATE 3 PASS: 현재 값이 정확히 기대 구버전 문자열과 일치
GATE 4 PASS: 단일 트랜잭션 커밋 완료, UPDATE 1건(SR_L4CORE_4786)
```

**정확히 변경된 값(전/후)**:

| 컬럼 | 이전 값 | 이후 값 |
|---|---|---|
| `student_definition` | `일정한 기간마다 되풀이하도록 정한 것` | `기한이나 기간이 일정하게 정해져 있는 것` |
| (그 외 모든 컬럼) | 무변경 | 무변경 |

적용 후 행 전체(재조회):
```
('SR_L4CORE_4786', '정기', '명사', '기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 기간.',
 '기한이나 기간이 일정하게 정해져 있는 것', '우리 반은 매달 첫째 주에 정기 모임을 엽니다.', '정기', 0, 0, 1)
```

### 2-5. 커밋 후 검증 게이트 결과

| 게이트 | 결과 |
|---|---|
| 대상 1행 새 값 확인(`VALUE_OK`) | **PASS** |
| 나머지 5,819건(모든 다른 콘텐츠) 체크섬 불변(sha256 pre==post) | **PASS** |
| `vocabulary_content_levels`(5,820행) 행수·전체 스냅샷 불변 | **PASS** |
| `vocabulary_multiformat_items`(1,329행) 행수·전체 스냅샷 불변 | **PASS** |
| `PRAGMA integrity_check` | **PASS** (ok) |
| `PRAGMA foreign_key_check` | **PASS** (위반 0) |
| `student_exposure`+`public_ready` 전체 합계 여전히 0 | **PASS** (0, 0) |
| 재실행 멱등성 사전 확인(구버전 값 남은 행수=0) | **PASS** |

**전 게이트 PASS — 롤백 불필요.**

### 2-6. 멱등성 실측 (2회차 재실행)

동일 스크립트를 그대로 다시 실행:
```
GATE 3: 현재 student_definition='기한이나 기간이 일정하게 정해져 있는 것'
GATE 3: 이미 새 값 - 멱등 재실행으로 판단, 대상 0건, 트랜잭션 없이 종료
UPDATED=0
IDEMPOTENT_SKIP=True
```
**멱등성 실측 PASS** — 재실행해도 추가 UPDATE가 발생하지 않는다.

### 2-7. 파일 수준 증거

```
적용 후 DB 파일: /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
크기: 12,541,952 bytes
SHA-256: 4c838cb3862406acdad90f2c83b59aac36a5f8fc99f6cd649dfbf62b2dc769ea
(백업 SHA-256 a20afa8f...와 다름 - 실제로 값이 바뀌었다는 파일 수준 증거)
```

### 2-8. 백업 경로 · 복구(롤백) 명령 (문서화만, 미실행 — 게이트 실패 없었음)

```bash
# 서버(aprolabs SSH)에서 실행
# (필요 시) 복구 전 현재 상태도 먼저 백업
cp ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
   ~/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.rollback-before-restore-$(date +%Y%m%d-%H%M%S)

cp ~/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase22-jeonggi-fix-pre-20260926-142247 \
   ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db

python3 -c "import sqlite3; c=sqlite3.connect('file:/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db?mode=ro', uri=True); print(c.execute('PRAGMA integrity_check').fetchone())"
```
(서버에도 `sqlite3` CLI 설치돼 있음: `sqlite3 ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db "PRAGMA integrity_check;"`로 대체 가능)

---

## 3. 작업3 — phase16 스크립트 검증 + 회귀 테스트 + "정기" 재판정

### 3-1. phase16 스크립트 하드코딩 값 재확인

`scripts/vocab/phase16_build_quiz_pilot_dryrun.py` 31~32행을 다시 열어 확인:
```python
("정기", "SR_L4CORE_4786", "4786", "명사", "기한이나 기간이 일정하게 정해져 있는 것",
 "우리 반은 매달 첫째 주에 정기 모임을 엽니다.", "정기", "4"),
```
**이미 새 값**(phase17이 고친 그대로) — 재확인 완료, 되돌아간 흔적 없음.

### 3-2. 구버전 문구 잔존 여부 저장소 전수 스캔

`scripts/`, `app/` 전체(.py) 및 `tests/` 전체를 스캔해 구버전 문구
(`"일정한 기간마다 되풀이하도록 정한 것"`)가 남아있는지 확인했다. 결과:
- **의도적으로 하드가드 상수로 이 문구를 담고 있는 파일 2개만 존재**:
  `scripts/vocab/phase22_apply_jeonggi_student_definition_fix.py`(GATE 3
  하드가드 상수), `tests/test_phase22_jeonggi_definition_regression.py`
  (테스트 자체의 OLD_VALUE 상수) — 둘 다 "회귀"가 아니라 이번 수정/검증
  작업의 필수 요소.
- 그 외 `scripts/`, `app/` 어디에도 이 문구는 남아있지 않다(0건).
- (참고, 스캔 대상 아님) `reports/`, `data/import/`의 과거 보고서·초안
  CSV(`schema_reading_phase12_*.md`, `phase17_*.md`, `phase20_*.md`,
  `phase21_*.md`, `literacy_l4_batch1_50_dryrun_20260924.csv`)에는 역사적
  기록으로 이 문구가 그대로 남아있다 — 이는 의도된 것(과거 사실을 보존해야
  하는 보고서/드래프트이지 실행되는 코드가 아님)이므로 수정하지 않았다.

**결론: "다시 구버전으로 되돌아갈 위험이 있는 부분"은 발견되지 않았다.**
스크립트 소스 자체에 하드코딩된 값이 유일한 진입점이고, 그 값은 이미
올바르다.

### 3-3. 신규 회귀 테스트

`tests/test_phase22_jeonggi_definition_regression.py`(신규, pytest 없이
`[PASS]/[FAIL]` 관례). 검증 내용:
1. phase16 스크립트 소스에 구버전 문구 없음 / 새 값 존재
2. `L4` 하드코딩 목록에서 `SR_L4CORE_4786` 항목의 `definition` 필드가 새 값과
   정확히 일치
3. **스크립트를 실제로 재실행**(`build_items()`를 직접 호출, `main()`의 파일
   출력은 건드리지 않음)해서 "정기" 관련 문항 2건을 재생성하고, `explanation`/
   `options_json`에 새 값이 포함되고 구버전 문구는 전혀 없음을 확인
4. 재생성된 두 문항이 스크립트 자체의 `validate()` 함수로도 PASS 판정되는지
5. `scripts/`, `app/`, `tests/` 전체에서 구버전 문구가 "의도치 않게" 남아있지
   않은지(3-2절의 예외 2개 파일만 허용)

실행 결과:
```
총 17건 중 실패 0건
```
**17/17 PASS.**

### 3-4. "정기" 2문항 독립 재판정

작업2에서 `vocabulary_contents.student_definition`이 실제로 새 값으로
바뀐 뒤(2-4절), 3자(원천/문항/콘텐츠)를 다시 대조했다:

| 대상 | 텍스트 |
|---|---|
| 원천(literacy.db id=4786) | `기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 기간.` |
| 문항(`MF_A/MF_C_..._L4_008`) 정답/explanation | `기한이나 기간이 일정하게 정해져 있는 것` |
| 콘텐츠(`vocabulary_contents.student_definition`, 적용 후) | `기한이나 기간이 일정하게 정해져 있는 것` |

**문항 ↔ 콘텐츠: 완전히 동일한 문자열로 일치. 문항/콘텐츠 ↔ 원천: 핵심 의미
(고정된 기한/기간) 일치**(원천의 부연 어구 "또는 그 기한이나 기간"을 생략한
축약형이지만 phase17/21이 이미 "원천에 더 가깝다"고 확정한 판단 그대로).
정답 유일성/오답 배제력(공간/집단/하위와 명확히 구분)도 불변.

**판정: HOLD 사유("문항-콘텐츠 텍스트 불일치")가 완전히 해소됨 → PASS로
전환.** 통과하지 못할 이유(품사/레벨/예문과의 충돌, 정답 모호성 등)는
발견되지 않았다.

### 3-5. 40건 판정표 갱신

`scripts/vocab/phase22_update_verdicts.py`(신규, 1회성 유틸리티, 대상
item_id 2건만 프로그램적으로 필터링해 수정 — 그 외 38건은 로직 구조상
건드릴 수 없음)로 다음 2개 파일을 갱신했다:

- `data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.csv`
- `data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.jsonl`

갱신 내용: `MF_A_SC_SRL4L5PILOT_20260925_L4_008`,
`MF_C_SC_SRL4L5PILOT_20260925_L4_008` 두 행의 `verdict`를 `HOLD` → `PASS`로,
`reason`을 3-4절 판정 근거로 갱신. 갱신 전 `assert r["verdict"] == "HOLD"`로
사전 상태를 강제 확인했고(불일치 시 예외로 중단), 나머지 38개 `item_id`는
필터링 조건(`item_id in TARGET_IDS`) 밖이라 코드 구조상 전혀 접근되지 않았다.

**갱신 후 검증**(`scripts/vocab/phase22_verify_verdicts.py`로 재확인):
```
csv total rows: 40 / csv PASS count: 40 / HOLD count: 0
MF_A_SC_SRL4L5PILOT_20260925_L4_008 PASS
MF_C_SC_SRL4L5PILOT_20260925_L4_008 PASS
jsonl total: 40 / PASS: 40 / HOLD: 0
MF_A_SC_SRL4L5PILOT_20260925_L4_008 PASS
MF_C_SC_SRL4L5PILOT_20260925_L4_008 PASS
```
40건 전부 PASS(기존 38건 PASS + 이번 2건 PASS 전환) — HOLD 0건.

---

## 4. 산출물

### 신규 코드 (로컬, git 미커밋)

- `scripts/vocab/phase22_apply_jeonggi_student_definition_fix.py` — 작업2
  게이트 기반 단일 행 UPDATE 적용 스크립트(서버에서 실행, `/tmp/`에 사본
  전달).
- `tests/test_phase22_jeonggi_definition_regression.py` — 작업3 신규 회귀
  테스트(17건, pytest 없이 `[PASS]/[FAIL]`).
- `scripts/vocab/phase22_update_verdicts.py` — 40건 판정표 CSV/JSONL 중
  "정기" 2건만 HOLD→PASS로 갱신하는 1회성 유틸리티(대상 외 38건은 코드
  구조상 수정 불가).
- `scripts/vocab/phase22_verify_verdicts.py` — 갱신 후 판정표 재검증용
  보조 스크립트.

### 수정된 데이터 파일 (로컬, git 미커밋)

- `data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.csv` —
  2행(verdict/reason)만 갱신, 나머지 38행 무변경.
- `data/import/schema_reading_phase20_pilot_40_human_verdicts_20260926.jsonl` —
  동일.

### 본 보고서

- `reports/schema_reading_phase22_jeonggi_fix_and_rejudge_20260926.md`

### 실제 변경된 유일한 DB

- 서버 `vocabulary_quiz_research.db` — **오직 `content_id='SR_L4CORE_4786'`
  행의 `student_definition` 컬럼만** 변경(2-4절). `literacy.db`(로컬·서버 모두)는
  이번 세션에서 읽기만 했고 전혀 변경하지 않았다.

---

## 5. 최종 요약 (호출한 에이전트용)

1. **작업1**: 확정함. 원천/문항 2건/콘텐츠 현재값을 전부 재조회했고, 새
   정의가 품사·레벨·예문과 충돌 없음을 확인해 phase21 제안값
   (`"기한이나 기간이 일정하게 정해져 있는 것"`)을 적용 대상으로 확정했다.
2. **작업2**: 전 게이트 PASS. 백업
   `vocabulary_quiz_research.db.bak-phase22-jeonggi-fix-pre-20260926-142247`
   (SHA-256 `a20afa8f80273bc67a8a1f7b65203833cb1e4cd0519b6ae746d7d14f45cbb07b`).
   정확히 변경된 값: `SR_L4CORE_4786.student_definition`
   `"일정한 기간마다 되풀이하도록 정한 것"` → `"기한이나 기간이 일정하게
   정해져 있는 것"` (다른 컬럼/행 전부 무변경, 5,819건 체크섬 불변,
   `integrity_check`/`foreign_key_check` 이상 없음, exposure/public_ready
   합계 0 유지, 멱등성 실측 확인). 복구 명령은 2-8절.
3. **작업3**: phase16 스크립트는 이미 새 값이었고(재확인), 저장소 전체에
   구버전 문구가 의도치 않게 남은 곳은 없었다. 신규 회귀 테스트 17/17
   PASS. "정기" 2문항은 재판정 결과 HOLD → **PASS로 전환**(문항-콘텐츠
   텍스트가 완전히 일치하게 됐고 원천과의 의미 충돌도 없음). 판정표는
   40건 전부 PASS(HOLD 0)로 갱신됨, 갱신 경로는 위 4절.
4. **산출물**: 코드 4개, 데이터 파일 2개(갱신), 본 보고서 1개(4절 참고).
5. **가장 중요한 리스크**:
   - (a) 이번 판정표 갱신은 "정기" 2문항이 phase20과 동급의 사람 수준
     재검토를 통과했다는 문서적 기록일 뿐, 이 40문항 자체는 여전히
     `vocabulary_multiformat_items`에 `student_exposure=0/public_ready=0`
     비공개 상태로만 존재한다 — 실제 운영 노출/공개 전환은 이번 세션
     범위 밖이며 별도 승인·절차가 필요하다.
   - (b) `vq_research_backup.py`/`phase22_apply_*` 스크립트는 서버에서만
     실행 가능하다(research DB가 로컬에 없음) — 향후 유사 1건 수정 시
     이 패턴(하드가드 → 백업 → 단일 트랜잭션 → 검증 → 멱등성)을 그대로
     재사용할 것을 권장한다.
   - (c) 이번 세션이 만든 `phase22_apply_jeonggi_student_definition_fix.py`와
     회귀 테스트 파일 안에는 구버전 문구가 상수로 남아있다(의도적, 3-2절
     설명) — 향후 저장소 전체를 "구버전 문구 잔존 여부"로 스캔할 때 이
     두 파일은 정상적인 예외로 처리해야 하며, 실수로 삭제/수정하면 안 된다
     (테스트의 GATE 3/회귀 검증 로직이 깨짐).
