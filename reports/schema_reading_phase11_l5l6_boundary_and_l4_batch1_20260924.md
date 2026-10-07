# 스키마리딩x어휘 — 11단계: L5/L6 학년 경계 정책 갱신 + L4 첫 배치(50건) 콘텐츠 작성안

- 작성일: 2026-09-24 (`date` 명령으로 시스템 현재 날짜 확인)
- 선행 문서: `reports/schema_reading_phase10_73drafts_crosscheck_20260924.md`(10단계)가
  확립한 사실(73건 전부 literacy.db `schemareading-tooldict`(V) 소스, L4=39/L5=34,
  FULL_MATCH 71건, HEADWORD_ONLY_MATCH 2건("부여"·"수집"), GRADE_LABELS 등
  수정 대상 코드 3곳)을 재조사하지 않고 그대로 인용·재사용했다.
- **이번 세션은 코드/테스트/문서 수정과 dry-run 산출물만 만들었다.**
  `data/literacy.db`·`vocabulary_quiz_research.db`(서버) 어디에도 쓰지 않았다.
  기존 AI 판정 값과 저장된 `level`은 재분류하지 않았다. git commit/push,
  서버 배포·재시작 전부 하지 않았다(호출한 세션이 diff 검토 후 직접 진행).

---

## 0. 핵심 요약

1. **L5=고1/L6=고2~3 기준으로 코드 3곳 중 수정 가능한 2곳을 고쳤다**
   (`scripts/literacy/auto_review_level.py`의 `LEVEL_TABLE`+Gemini 프롬프트,
   `app/vocabulary_quiz/routers/multiformat.py`의 `GRADE_LABELS`). 세 번째
   (`app/literacy/migrations/002_add_level.py`)는 지시대로 손대지 않고, 대신
   과거 마이그레이션과 새 정책 사이의 불일치를 별도 문서
   `docs/literacy/07-학년경계정책-L5L6.md`로 기록했다.
2. **근거 부족 AI 판정이 곧바로 "검수완료"로 넘어가지 않도록
   `auto_review_level.py`에 `grounded` 신호와 보류(`review_status='보류'`)
   분기를 추가했다** — literacy.db가 이미 쓰던 `'보류'` 상태값을 재사용했다
   (새 상태값을 만들지 않음). 과거 2,944건(기존 완전자동 판정)은 소급
   재분류하지 않는다.
3. **새 테스트 27건 전부 통과, 기존 관리자 레벨 퀴즈 회귀 71건도 그대로
   통과했다**(재실행 확인). krdict 폴백 보류 회귀 테스트(29건)도 영향
   없음을 재확인했다.
4. **L4 첫 배치 50건을 완성했다**: 73건 초안 중 L4 FULL_MATCH **39건**(V)을
   그대로 활용하고, literacy.db V(L4) 미사용분에서 **11건**을 새로 작성해
   50건을 채웠다. S(교과개념어)는 이번 배치에 포함하지 않았다(V만으로
   50건 충족, 사유는 4절 참고).
5. **중요 발견(신규)**: 73건 초안의 "L4"는 원본 패키지 자체 분류
   (`original_level_pkg`)일 뿐, **literacy.db에 실제 저장된 `level` 값과는
   다른 경우가 39건 중 29건(74%)**이다 — 나선형 반복(같은 헤드워드가 여러
   레벨에 반복 등장할 때 최저 레벨로 병합) 때문에 literacy.db는 이 29건을
   실제로 `level=1`(3건) 또는 `level=2`(26건)로 저장하고 있다. 이번 배치는
   지시대로 원본 패키지 분류 기준 "L4 FULL_MATCH"를 그대로 썼지만, **향후
   실제 DB 적재 시에는 이 29건을 literacy.db 기준 실제 레벨(L1/L2)로 넣을지,
   패키지 기준 L4로 넣을지 정책 결정이 필요하다**(5절 리스크 1번 참고,
   `data/import/literacy_l4_batch1_50_dryrun_20260924.csv`의
   `level_mismatch_flag` 컬럼에 전부 표시해 뒀다).

---

## 1. 작업 1 — 학년 경계 코드 수정

### 1-1. 수정한 파일과 diff 요약

| 파일 | 변경 |
|---|---|
| `scripts/literacy/auto_review_level.py` | `LEVEL_TABLE`: `5: 고1~2 → 고1`, `6: 고3 → 고2~3`. 이 표는 `build_prompt()`가 그대로 Gemini 프롬프트에 넣으므로, 앞으로 이 스크립트를 실행하면 새 경계로 판정된다(테스트로 직접 확인, 2절). |
| `app/vocabulary_quiz/routers/multiformat.py` | `GRADE_LABELS`: `5: "고등 1~2학년" → "고등 1학년"`, `6: "고등 3학년" → "고등 2~3학년"`. 템플릿(`multiformat_play.html`)은 이 딕셔너리를 순회만 하므로 별도 수정 불필요(확인 완료). |
| `app/literacy/migrations/002_add_level.py` | **의도적으로 미수정.** 이미 적용된 과거 마이그레이션 — 코드를 고치면 코드와 이미 저장된 DB 상태(momo-textbook 821건 백필값)가 어긋난다. |
| `docs/vocabulary/LEVEL_POLICY_v0.1.md` | 공식 학년 체계표의 L5/L6 라벨(`고등학교 1학년`/`고등학교 2~3학년`)과 코드(`H1`/`H2_3`) 갱신. 이 문자열은 코드에서 참조되지 않음을 grep으로 확인(문서 전용). |
| `docs/literacy/04-스키마리딩어휘.md` | 학년 대응표를 새 기준으로 고치고, 과거 백필 데이터(momo-textbook)와의 불일치 가능성을 명시하는 주의문 추가. |
| `docs/literacy/04-스키마리딩어휘적재.md` | 같은 표에 각주로 "정책 갱신 전 원안"을 남김(이 문서는 Phase 3-2 작업 지시서 기록이라 표 전체를 새로 쓰지 않고 각주만 추가). |
| `docs/literacy/07-학년경계정책-L5L6.md` (신규) | 이번 정책 갱신 전체를 기록하는 정책 문서 — 확정 기준, 고친 곳, 002_add_level.py를 고치지 않은 이유, 소급 재분류하지 않는 범위, grounded/보류 설계를 전부 담았다. |

### 1-2. 판정 흐름 확인 결과

`auto_review_level.py`의 판정 흐름: `select_targets()`(속담·관용구·사자성어 중
`level IS NULL`이고 정의가 있는 행 조회) → `build_prompt()`(LEVEL_TABLE을
그대로 프롬프트에 주입) → Gemini 호출 → `save_result()`(DB에 `level`,
`review_status`, `note` 갱신). **LEVEL_TABLE 수정이 실제로 프롬프트 텍스트에
반영되는지, 그리고 이 스크립트 외에 다른 곳에서 같은 경계 문자열을 다시
정의하고 있지 않은지**를 grep으로 전수 확인했다 — `app/literacy/`,
`scripts/vocab/`에는 매칭 없음(2026-09-24 기준 재확인, phase10 3절과 동일
결론).

---

## 2. 작업 2 — 저장 상태·검증 조건 확인 + 테스트

### 2-1. 기존 저장 로직 확인 결과

`save_result()`는 원래 `level`이 있으면(`None`이 아니면) **무조건**
`review_status='검수완료'`로 즉시 확정했다(2026-09-02 사용자 결정 - 완전자동,
사람 확인 없음). 실제 DB를 읽기 전용으로 집계한 결과, 이 설계 그대로
지금까지 `grade_source='auto'`인 속담/관용구/사자성어 2,944건이 전부
`review_status='검수완료'`로 저장돼 있었다(제외 6건, 그 밖에 다른 경로로
생긴 `보류` 159건은 이 스크립트가 만든 게 아님 - `grade_source='auto'`이지만
`save_result()` 호출 흔적과 무관, 별도 스크립트 기원으로 판단해 이번 조사
범위 밖으로 남겼다).

즉 **"학년 근거가 부족해도 곧바로 검수완료로 넘어간다"는 우려가 실제
사실이었다.** 이번 수정으로 이를 고쳤다(1-1절 diff 참고):

- Gemini 프롬프트에 `grounded`(학년 근거가 실제로 있는지) 필드를 추가로
  요구한다.
- `save_result(conn, term_id, level, reason, now, grounded=True)` — 기본값은
  `True`(레거시 호출 회귀 없음). `grounded=False`면 `level`은 저장하되
  `review_status='보류'`로 남긴다.
- 메인 루프에서 AI 응답에 `grounded` 필드가 아예 없으면(구버전 캐시·응답
  누락) 안전 쪽으로 `False`(보류)를 기본값으로 쓴다.
- **과거 2,944건은 소급 재분류하지 않는다** — 이번 변경은 앞으로 이
  스크립트를 실행할 때만 적용된다.

### 2-2. 신규 테스트

`tests/test_auto_review_level_grade_boundary.py`(신규, pytest 없이
`[PASS]/[FAIL]` 스타일, `tests/test_krdict_fallback_hold_fix.py` 패턴 따름).
**읽기 전용/격리 전용** — `data/literacy.db`를 전혀 열지 않고, `save_result()`
검증은 `tempfile`로 만든 임시 sqlite DB(테스트 종료 시 삭제)에서만 수행한다.

검증 항목:
1. `LEVEL_TABLE`에 새 경계(`고1`, `고2~3`)가 있고 옛 경계(`고1~2`, `고3`)가
   없는지
2. `build_prompt()`가 실제로 만드는 프롬프트 문자열에 새 경계와 `grounded`
   지시가 포함되는지
3. 경계값 매핑(중3→L4, 고1→L5, 고2~3→L6)이 `LEVEL_TABLE` 파싱 결과와
   정확히 일치하는지
4. `save_result()`가 `grounded=False`일 때 `review_status='보류'`로,
   `True`일 때 `'검수완료'`로 저장하는지(4가지 경계 케이스: grounded=True,
   grounded=False, 인자 생략 시 기본값, level=None)
5. 메인 루프의 `grounded` 파싱 로직(필드 없으면 안전하게 보류로 기본값)

**실행 결과**: `venv/Scripts/python.exe tests/test_auto_review_level_grade_boundary.py`
→ **27/27 passed**.

### 2-3. 기존 회귀 테스트 재실행

- `tests/test_admin_level_quiz.py` (관리자 전용 레벨별 어휘 퀴즈 회귀,
  로컬 R&D 전용 `data/vocab/vocabulary_quiz_rnd.db` 사용 — 이번 절대 제약이
  가리키는 서버 `vocabulary_quiz_research.db`와는 다른 파일, 테스트 자체가
  변경분을 원복하는 자기완결형 테스트): **수정 전 71/71 passed(베이스라인
  확인) → `GRADE_LABELS` 수정 후 재실행 71/71 passed**(회귀 없음 확인 —
  L1 라벨만 assert하는 기존 케이스라 L5/L6 라벨 변경 영향 없음, 현재
  L5/L6 후보가 0건이라 API 실질 동작도 그대로).
- `tests/test_krdict_fallback_hold_fix.py`: 29/29 passed(이번 수정과 무관한
  모듈이지만 같은 `scripts/literacy/` 경로 의존성 확인 차원에서 재실행,
  영향 없음 확인).

---

## 3. 작업 3 — 배포 전 확인 사항 (실제 배포는 하지 않음)

### 3-1. 이번 수정이 건드리는 파일 전체 목록

**코드(배포 시 실질 동작에 영향)**:
- `scripts/literacy/auto_review_level.py` — 관리자가 수동으로 실행하는
  일회성 스크립트. 서버에 cron/타이머/실행 중 프로세스가 없음을 이미
  확인함(사용자 제공 정보). **배포되면 다음 수동 실행부터 즉시 새 경계와
  보류 로직이 적용된다** — 실행 전에 `--dry-run`으로 먼저 확인 권장.
- `app/vocabulary_quiz/routers/multiformat.py` — FastAPI 라우터, 서버
  재시작이 있어야 반영됨. 영향받는 화면/엔드포인트:
  - `GET /vocabulary-quiz/multiformat/play`(관리자 전용 화면) — 레벨
    선택기에 L5="고등 1학년", L6="고등 2~3학년"로 표시됨(`multiformat_play.html`
    L15).
  - `GET /api/vocabulary-quiz/.../availability`(`_level_availability`) —
    응답의 `grade_label` 필드.
  - `POST /api/vocabulary-quiz/.../sessions`(`create_session`) — 결과의
    `level_info.grade_label`.
  - **현재 L5/L6 후보가 0건이라(literacy.db→vocabulary_quiz 이관이 아직
    없음) 라벨 문자열 외에는 기능 변화가 없다** — 출제 로직·문항 수·API
    상태 코드는 전혀 바뀌지 않는다.

**문서(배포와 무관, 참고용)**:
- `docs/vocabulary/LEVEL_POLICY_v0.1.md`, `docs/literacy/04-스키마리딩어휘.md`,
  `docs/literacy/04-스키마리딩어휘적재.md`, `docs/literacy/07-학년경계정책-L5L6.md`(신규)

**테스트(배포와 무관)**:
- `tests/test_auto_review_level_grade_boundary.py`(신규)

**dry-run 산출물(배포와 무관, DB 미적재)**:
- `data/import/literacy_l4_batch1_50_dryrun_20260924.csv`,
  `data/import/literacy_l4_batch1_50_dryrun_20260924.jsonl`

### 3-2. 배포 판단에 참고할 점

- `multiformat.py` 변경은 **서버 재시작이 필요**하다(FastAPI 앱 코드).
- `auto_review_level.py` 변경은 **재시작 불필요**(다음 수동 실행부터 적용,
  cron 없음 확인됨).
- 문서·테스트·dry-run 산출물은 배포 자체와 무관(정적 파일).
- 기능적 영향(사용자가 실제로 다른 걸 보게 되는지)은 관리자 화면의 텍스트
  라벨뿐이고, 학생 노출 화면에는 영향이 없다(L5/L6 관련 콘텐츠 자체가
  아직 학생에게 공개되지 않음, `student_exposure`/`public_ready` 전부 0).

---

## 4. 작업 4 — L4 첫 배치(최대 50건) 콘텐츠 작성 대기열

### 4-1. 구성 요약

| 구분 | 건수 | 소스 |
|---|---:|---|
| 73건 초안 중 L4 FULL_MATCH 활용분 | **39** | V(`schemareading-tooldict`), 기존 `student_definition_draft`/`example_sentence_draft` 그대로 사용 |
| 보충분(신규 작성) | **11** | V(`schemareading-tooldict`), literacy.db L4 미사용분에서 선정해 학생용 정의·예문 신규 작성 |
| **합계** | **50** | **V 50 / S 0** |

**S(교과개념어)는 이번 배치에 포함하지 않았다** — V만으로 50건이 이미
채워졌고(V L4 미사용 후보가 39건 제외 후에도 75건 남아 충분), S는 정의
채움률이 낮은 시트가 있어(사회 73.4%, 과학 57.9%) 첫 배치는 검증된 V
자료만으로 구성하는 편이 안전하다고 판단했다. S 223건(전부 정의 있음,
AI 미생성 확인됨)은 다음 배치를 위한 풀로 그대로 남아 있다.

### 4-2. 제외 항목과 사유

| 항목 | 사유 |
|---|---|
| "부여"(`INTERNAL_ACADEMIC:Level5:210`), "수집"(`INTERNAL_ACADEMIC:Level5:302`) | HEADWORD_ONLY_MATCH — **애초에 L5 소속**이라 이번 L4 배치와 무관(참고용으로만 명시) |
| "삶"(literacy_db id 4756, V L4 후보) | literacy.db `note`에 "원본 데이터 오류로 제외된 뜻풀이 1건 있음"(phase3-2 당시 발견된 원본 스프레드시트 오류 이력) — 이미 정제됐지만 첫 배치는 이력 없는 항목 우선으로 신중하게 선정하기 위해 보충 11건에서 제외 |
| "예7", "유리02", "유형02", "유형07" 등 번호가 붙은 V L4 후보 | 동형이의어 번호 표기(같은 헤드워드의 다른 의미가 별도 행으로 존재할 가능성) — 콘텐츠 작성 단계에서 어느 의미인지 혼동 위험이 있어 첫 배치에서는 제외, 번호 없는 명확한 표제어만 선정 |
| S(schema) level=6 576건 | **이번 배치와 무관을 명시적으로 확인함** — level=4가 아니라 level=6이고, phase9/phase10이 이미 확인한 대로 94.7%가 `[AI 자동 생성 뜻풀이]` 태그를 가진 미검증 자료라 애초에 이 L4 배치의 대상이 될 수 없다 |
| S(schema) level=4~5 전체(730건) | 이번 배치는 아니지만 제외가 아니라 "다음 배치 후보"로 남김(정의 있음, AI 미생성 확인됨) |

### 4-3. 보충 11건 상세 (신규 작성)

문맥 확인을 위해 literacy.db의 원 정의(사전적 정의)와 새로 쓴 학생용 정의·
예문을 함께 남긴다. 전부 `source='schemareading-tooldict'`, `level=4`,
`review_status='검수전'`(literacy.db 원본 상태, 이번 세션이 바꾸지 않음).

| 헤드워드 | literacy.db 원 정의 | 학생용 정의(신규) | 예문(신규) | 비고 |
|---|---|---|---|---|
| 간과 | 대충 보아 넘김. 깊이 유의하지 않고 예사로 내버려둠. | 중요한 것을 대충 보고 넘겨서 미처 신경 쓰지 않는 것 | 우리는 안전 규칙을 간과해서 작은 사고가 났습니다. | |
| 공간 | 아무 것도 없는 빈 곳. 또는 물질, 물체가 존재할 수 있거나 어떤 일이 일어날 수 있는 자리. | 무엇이 있거나 어떤 일이 일어날 수 있는 자리나 빈 곳 | 교실 뒤쪽에 책상을 놓을 공간이 남아 있습니다. | |
| 기억 | 지난 일을 잊지 아니함. 또는 그 내용. | 지나간 일을 잊지 않고 마음속에 간직하고 있는 것 | 할머니 댁에서 보낸 여름은 좋은 기억으로 남아 있습니다. | |
| 만족 | 마음에 흡족함. 또는 흡족하게 생각함. | 바라던 대로 이루어져 마음이 흡족한 것 | 발표를 무사히 마치고 나니 만족스러운 기분이 들었습니다. | |
| 문자 | 말의 음과 뜻을 나타내는 시각적 기호, 글자. | 말소리와 뜻을 눈으로 볼 수 있게 나타낸 기호, 즉 글자 | 한글은 소리를 정확하게 나타낼 수 있는 문자입니다. | |
| 보수 | 낡은 것을 보충해서 수선함. | 낡거나 고장 난 것을 손보아 고치는 것 | 장마가 끝난 뒤 학교는 새는 지붕을 보수했습니다. | **동형이의어 주의**: '보수'는 '정치적 보수', '급여(보수를 받다)'라는 다른 뜻도 있다. literacy.db 정의(수선)에 한정된 뜻만 채택 — 출제 시 문맥으로 뜻을 명확히 구분해야 함 |
| 불가피하다 | 피할 수가 없다. | 어쩔 수 없어서 피할 수가 없다 | 갑자기 폭우가 쏟아져 일정 변경이 불가피했습니다. | |
| 완료 | 완전히 끝마침. | 하던 일을 완전히 끝내는 것 | 모둠은 예정보다 일찍 과제를 완료했습니다. | |
| 요건 | 긴요한 일이나 안건. 필요한 조건. | 어떤 일을 하기 위해 반드시 갖추어야 하는 조건 | 이 대회에 나가려면 몇 가지 요건을 먼저 갖추어야 합니다. | |
| 원천 | 물이 흘러나오는 근원. 사물의 근원. | 어떤 것이 처음 생겨나거나 흘러나오는 근본이 되는 곳 | 독서는 새로운 생각의 원천이 됩니다. | |
| 유동 | 액체 상태의 물질이나 전류 따위가 흘러 움직임. 이리저리 자주 옮겨 다님. | 액체나 전류처럼 흘러 움직이거나, 한곳에 머물지 않고 자주 옮겨 다니는 것 | 명절에는 고속도로에 차량 유동이 많아집니다. | |

vocabulary_quiz 로컬 R&D DB(`data/vocab/vocabulary_quiz_rnd.db`,
`vocabulary_contents.lemma`)와 이 11개 헤드워드를 직접 대조해 겹치는 것이
0건임을 확인했다(73건과 마찬가지로 기존 5,723개 콘텐츠와 무관).

### 4-4. 중요 발견 — 73건 중 29건의 "L4"는 literacy.db 저장값과 다르다

73건 패키지의 `original_level_pkg`(원본 학습도구어 사전 엑셀에서 그 단어가
있던 시트 번호)는 L4/L5였지만, literacy.db는 **같은 소스 안에서 같은
헤드워드가 여러 레벨에 반복 등장하면 최저 레벨로 병합**한다(나선형 반복
규칙, `docs/literacy/04-스키마리딩어휘.md` "source 간 병합 금지 규칙" 절
참고 — 이건 그 반대인 "같은 source 내" 병합 규칙). 그 결과 **39건의 L4
FULL_MATCH 중 실제로 literacy.db에 `level=4`로 저장된 것은 10건뿐이고,
나머지 29건은 `level=2`(26건) 또는 `level=1`(3건)로 저장돼 있다**
(원칙, 한계, 원인, 인식, 준수 등 — 전체 목록은
`data/import/literacy_l4_batch1_50_dryrun_20260924.csv`의
`level_mismatch_flag='MISMATCH'` 행 참고).

이번 배치는 사용자 지시("73개 초안 중 L4 FULL_MATCH 항목을 우선 활용")를
문자 그대로 따라 **패키지 자체의 L4 분류**를 기준으로 39건을 그대로
포함시켰다. 이 결정과 발견 사실은 5절 리스크 1번에도 남긴다 — **다음
단계(실제 콘텐츠 DB 적재)로 넘어가기 전에 반드시 사람이 결정해야 한다**:
이 29건을 (a) 패키지 분류대로 L4 콘텐츠로 쓸지, 아니면 (b) literacy.db의
실제 저장 레벨(L1/L2)을 따를지.

### 4-5. dry-run 산출물

- `data/import/literacy_l4_batch1_50_dryrun_20260924.csv`
- `data/import/literacy_l4_batch1_50_dryrun_20260924.jsonl`

컬럼: `batch_seq, origin(73draft_L4_FULL_MATCH/supplement_literacy_db_V_L4),
lemma, pos, source, literacy_db_term_id, literacy_db_level_actual,
original_level_pkg, level_mismatch_flag, literacy_db_definition,
student_definition_draft, example_sentence_draft, caution,
review_status_literacy_db`. **DB에는 전혀 쓰지 않았다** —
`current_content_id`/`vocabulary_content_levels` 등 실제 적재에 필요한
어떤 테이블도 건드리지 않았고, 학생 공개(`student_exposure`/`public_ready`)와
무관한 순수 파일 산출물이다.

---

## 5. 리스크 (우선순위 순)

1. **73건 중 29건의 "L4" 분류가 literacy.db 실제 저장 레벨(L1/L2)과 다르다**
   (4-4절) — 다음 단계로 넘어가기 전 반드시 사람이 정책을 결정해야 한다.
   결정하지 않고 그대로 L4 콘텐츠로 적재하면, 같은 헤드워드가 literacy.db
   에서는 더 낮은 레벨로 분류돼 있다는 사실과 충돌한다.
2. **`auto_review_level.py`의 `grounded` 필드는 Gemini의 자기 신고에
   의존한다** — AI가 실제로는 근거가 부족한데도 `grounded=true`로 잘못
   응답할 가능성은 이 설계로 완전히 막을 수 없다(자기 신고의 구조적 한계).
   다만 필드가 아예 없거나 파싱 실패 시에는 안전 쪽(보류)으로 기본값을
   두었으므로, 최소한 "침묵 실패"로 검수완료가 나오는 경로는 막았다.
3. **과거 2,944건(옛 경계+무조건 검수완료)은 이번 수정으로 하나도
   재분류되지 않는다** — 사용자 지시에 따른 의도된 설계이지만, 이 2,944건
   안에 실제로 근거 부족 판정이나 옛 경계로 잘못 매겨진 값이 섞여 있을
   가능성은 여전히 남아 있다(phase2 리포트가 이미 예시를 확인함). 별도
   소급 재검토 작업이 필요하면 이번 결과물과 분리해서 진행해야 한다.
4. **momo-textbook 821건의 `level` 백필값도 옛 경계 기준**(`002_add_level.py`
   의 `_GRADE_TO_LEVEL`)이다 — `grade_level=11`(고2)인 행이 `level=5`로
   저장돼 있어 새 정책(L6=고2~3)과 어긋난다. 마이그레이션 코드와 저장값은
   의도적으로 그대로 두었으므로(1-1절), 이 불일치는 `docs/literacy/07-학년경계정책-L5L6.md`
   에 명시적으로 기록만 해 두었다 — 언젠가 고치기로 하면 별도의 명시적
   소급 재분류 작업(백업 필수, 영향 행수 먼저 집계)으로 다뤄야 한다.
5. **보충 11건의 "보수"는 동형이의어 주의가 필요하다**(4-3절 비고) —
   literacy.db 정의(수선)에 한정했지만 실제 콘텐츠·문항 제작 시 다른 뜻과
   섞이지 않도록 주의가 필요하다.

---

## 6. 산출물 목록

- 본 보고서: `reports/schema_reading_phase11_l5l6_boundary_and_l4_batch1_20260924.md`
- 코드: `scripts/literacy/auto_review_level.py`, `app/vocabulary_quiz/routers/multiformat.py`
- 문서: `docs/vocabulary/LEVEL_POLICY_v0.1.md`, `docs/literacy/04-스키마리딩어휘.md`,
  `docs/literacy/04-스키마리딩어휘적재.md`, `docs/literacy/07-학년경계정책-L5L6.md`(신규)
- 테스트: `tests/test_auto_review_level_grade_boundary.py`(신규, 27/27 passed)
- dry-run 데이터: `data/import/literacy_l4_batch1_50_dryrun_20260924.csv`,
  `data/import/literacy_l4_batch1_50_dryrun_20260924.jsonl`
- git commit/push, 서버 배포·재시작: **하지 않음**(호출한 세션이 직접 진행)
