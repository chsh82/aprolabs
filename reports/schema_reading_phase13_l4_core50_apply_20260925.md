# 스키마리딩x어휘 — 13단계: L4 신규 핵심 50건(B10+C11+D29) 3중 연결 + REUSE 판정 + 단일 트랜잭션 적재

- 작성일: 2026-09-25 (`date` 명령으로 시스템 현재 날짜 직접 확인)
- 범위: 12단계(`reports/schema_reading_phase12_l4_l5_spiral_review_20260924.md`)가 확정한
  `group=B_10_real_L4_verified`(10) + `C_11_supplement_carryover`(11) + `D_29_new_V_S_candidates`(29)
  = `classification=NEW_CORE` 50건(V 36 / S 14)을 이번 세션의 실제 작업 대상으로 삼았다.
  `group=A_29_mismatch_reclass`(SPIRAL_REVIEW, L4)와 `group=E_34_L5_spiral_check`(L5,
  L5_CANDIDATE 6 + L5_SPIRAL_REVIEW 28) **합계 63건은 이번 세션에서 전혀 건드리지 않았다** —
  읽지도, 쓰지도 않았다. 별도 목록으로 그대로 보존된다(12단계 결과표에 그대로 남아 있음).
- literacy.db는 이번 세션 내내 `mode=ro`+`PRAGMA query_only=ON`으로만 열었다(SELECT만
  실행, INSERT/UPDATE/DELETE/DDL 없음, 파일 mtime도 세션 시작 전인 2026-09-24 08:37로
  변화 없음을 재확인했다). research DB 쓰기는 4번 작업의 단일 트랜잭션 한 번만 수행했다.
  git commit/push 없음.

---

## 0. 핵심 요약

1. **50건(B10+C11+D29) 전부를 원천 literacy.db 값과 이번 세션에 직접 재조회한 값으로
   재대조했다** — term_id/표제어/저장 레벨(전부 4) 불일치 0건. phase11/phase12 시점 값을
   그대로 믿지 않고 다시 확인하라는 지시를 지켰다.
2. **REUSE 판정: 0건.** research 서버 `vocabulary_quiz_research.db`의 `vocabulary_contents`
   5,723건 전체(lemma 기준) 대 50건 lemma를 직접 대조한 결과 **정확히 일치하는 lemma가
   하나도 없었다**(부분 문자열 근접 매칭도 별도로 확인했으나 전부 형태소가 다른 별개
   단어였다 — 예: "인지"↔"가스오븐레인지"는 우연의 글자 겹침일 뿐). 따라서 50건 전부
   REUSE 후보가 아니다(기존 콘텐츠와 뜻까지 같은 중복이 원천적으로 없음).
3. **동형이의어 검사**: "정상"(산 정상=꼭대기와 다른 뜻)은 phase12가 이미 발견한 위험을
   재확인했고, literacy.db에는 "제대로인 상태" 뜻 1행만 존재해 배치에서 제외하지 않았다
   (캐주션만 유지). "지향/지양"류 스캔은 50건 내부 및 vocabulary_contents 5,723건 전체와
   대조해(글자 1개 차이·같은 길이 기준 전수 스캔, 189쌍 원시 후보) 수작업으로 재검토한
   결과, **진짜 "형태 비슷·뜻 반대/혼동 위험" 파생어 쌍은 새로 발견하지 못했다** —
   원시 후보 대부분은 우연한 음절 일치였다(예: "정상"↔"정박", "고원"↔"고삐"). 다만
   **"유추"/"유사"**(둘 다 50건 안에 있음, 어근 '유' 공유, 둘 다 "비슷함" 개념과
   관련되나 뜻이 다름: 유사=비슷함 자체, 유추=비슷함에 근거한 추론)는 완전한
   지향/지양형 반의어는 아니지만 학생이 혼동할 수 있어 주의 표시했고, **"사법권"**
   (권한)과 기존 `vocabulary_contents`에 있는 **"사법부"**(기관, 우리 50건과 무관한
   기존 콘텐츠)는 권한/기관을 혼동할 수 있어 향후 문항 제작 시 구분이 필요하다고
   기록했다(4-3절).
4. **S(교과개념어) 14건 적합성 대조: 13건 통과, 1건 개별 HOLD.** 14건 전부 literacy.db
   `note` 필드의 교과·주차·소분류가 phase12 CSV의 `subject_category`/`note_week`와
   완전히 일치했고, 정의도 학생용 정의와 의미가 일치했다. **"가변성"만 개별 HOLD** —
   literacy.db 원문이 "자원의 가치는 기술 수준, 경제 수준, 사회 문화적 배경에 따라
   변함"처럼 현상을 서술하는 문장이지, "가변성"이라는 표제어를 사전적으로 정의하는
   문장이 아니어서(성질/속성이라는 지정을 해석으로 보충해야 함) 독립 근거가 나머지
   13건보다 약하다고 판단했다. **이 1건만 HOLD 처리했고 작업 전체를 멈추지 않았다.**
5. **`level_status`/`review_status` 새 열거값을 발명하지 않았다** — `vocabulary_content_levels.level_status`는
   기존에 쓰이는 두 값(`PROVISIONAL_AUTO`, `REVIEW_BOUNDARY`) 중 **`REVIEW_BOUNDARY`**를
   택했다(근거: 3절). `vocabulary_contents.generation_method`/`qa_method`도 기존 값
   (`MANUAL_STRUCTURED_AUTHORING`/`DETERMINISTIC_PLUS_HEURISTIC`)을 그대로 재사용했다.
   `generation_status`는 의도적으로 **NULL**로 두었다(3-3절, 화이트리스트 회피 방어선).
6. **적재 전 게이트 7개 전부 PASS, 단일 트랜잭션으로 49건(HOLD 1건 제외) 삽입, 멱등성
   재실행 확인(2회차 삽입 0건, 스킵 49건), 적재 후 검증 전부 PASS.** 기존 5,723행
   체크섬은 마이그레이션 전후 완전 동일, 신규 49행 전부 `student_exposure=0`/
   `public_ready=0`, `vocabulary_items`/`vocabulary_multiformat_items` 어느 쪽도
   새 콘텐츠를 참조하지 않아(각각 0건) 구조적으로 출제 불가능함을 확인했다.
7. **최종 분류: REUSE 0 / NEW_PRIVATE_LOADABLE 49(V 36 + S 13) / HOLD 1(S, 가변성).**

---

## 1. 작업 1 — 50건 3중 연결 + 재사용(REUSE) 판별

### 1-1. 원천-literacy.db 재대조 (라이브 재검증)

`data/import/schema_reading_phase12_l4_l5_spiral_review_20260924.csv`에서
`group IN (B_10_real_L4_verified, C_11_supplement_carryover, D_29_new_V_S_candidates)`
50행을 추출한 뒤, 각 행의 `literacy_db_term_id`로 **이번 세션에 직접**
`data/literacy.db`(mode=ro)를 재조회했다(`PRAGMA table_info(terms)`로 스키마도
재확인: `id, category, headword, origin, definition, pos, sense_category,
subject_category, grade_level, grade_source, source, license, external_id,
collected_at, updated_at, review_status, note, level, reviewed_at`).

결과: **50건 전부 term_id/headword/level 일치, 불일치 0건.** phase11/phase12
시점 이후 다른 세션이 literacy.db를 바꾸지 않았음을 다시 한 번 확인했다(phase12가
이미 이 사실을 확인했지만, 이번 세션도 독립적으로 재확인했다). `review_status`는
50건 전부 `검수전`, `source`는 V(schemareading-tooldict) 36건 / S(schemareading-schema)
14건.

### 1-2. REUSE 판별 — research 서버 `vocabulary_contents` 대조

서버 `vocabulary_quiz_research.db`(mode=ro, `PRAGMA query_only=ON`)에서
`vocabulary_contents` 5,723건 전체의 `lemma`를 직접 조회해(`scripts/vocab/phase13_reuse_check.py`)
50건 lemma와 정확 일치(exact match)를 대조했다. **정확 일치 0건.** 이 스크립트가
5,723건의 `(lemma, pos)` 전체를 함께 가져와, 부분 문자열 포함 관계도 로컬에서
추가로 스캔했으나(`기억`↔`기억력`, `문자`↔`대문자`, `유동`↔`포유동물`,
`인지`↔`레인지` 등) 전부 다른 단어의 부분 문자열일 뿐 같은 표제어가 아니었다
(뜻 대조 이전에 표제어부터 다름 → REUSE 검토 대상조차 아님).

**결론: 50건 전부 REUSE=0, 기존 콘텐츠와 중복되는 항목이 없다.** (뜻까지 비교할
필요조차 없었던 이유: 표제어가 애초에 vocabulary_contents 5,723건 안에 하나도
없었기 때문.)

### 1-3. 3중 연결 표

전체 3중 연결 표(원천-literacy.db-vocabulary_quiz)는 아래 두 파일에 있다:

- `data/import/schema_reading_phase13_l4_core50_final_20260925.csv`
- `data/import/schema_reading_phase13_l4_core50_final_20260925.jsonl`

컬럼: `group, v_s, lemma, pos, literacy_source, literacy_term_id,
literacy_definition, l4_basis, student_definition, example_sentence,
example_target_form, subject_category, note_week, reuse_check,
final_classification, classification_reason, caution, content_id, vocab_level,
level_status`

각 행이: (1) 원천(phase12 group, V/S 구분), (2) literacy.db term_id/정의/L4 근거,
(3) research DB 신규 content_id(적재된 경우)까지 한 줄로 연결돼 있다.

---

## 2. 작업 2 — 학생용 뜻풀이·예문 + 자동 검사

### 2-1. 뜻풀이·예문 출처

- **B10(10건)**: phase12가 이미 검수 통과(10/10 PASS)시킨 학생용 정의·예문을
  재검토했다 — literacy.db 정의와 이번 세션 재조회 결과가 완전히 동일함을 재확인,
  정의·예문 문구도 원 뜻을 왜곡하지 않았음을 직접 읽고 재확인(보완 불필요).
- **C11(11건)**: phase11이 작성한 정의·예문을 재검토했다 — literacy.db 정의
  재조회 결과 phase11/phase12 시점과 동일, 문구 보완 불필요.
- **D29(29건, V15+S14)**: 이번 세션 신규 작성(phase12가 이미 초안을 만들어
  두었으므로 그 초안을 이번 세션이 literacy.db 정의와 하나씩 직접 대조하며
  검토·확정했다 — 39절 아래 표는 재확인 결과).

### 2-2. 자동 검사 — 뜻 일치

50건 전부에 대해 literacy.db `definition`과 학생용 정의를 나란히 놓고 핵심
의미소가 보존되는지 직접 대조했다(예: "저하" DB="정도, 수준, 능률 따위가 떨어져
낮아짐." / 학생용="정도나 수준, 능률이 떨어져서 낮아지는 것" — 완전 일치).
**49건(HOLD 1건 제외) 전부 PASS.** "가변성"은 2-3절/3절에서 별도로 다룬다.

### 2-3. 자동 검사 — 예문의 목표어 실제 용법

각 예문에서 목표어(또는 그 활용형)가 정의된 뜻으로 자연스럽게 쓰였는지 직접
읽고 확인했다(예: "불가피하다" — "갑자기 폭우가 쏟아져 일정 변경이
불가피했습니다" → "피할 수 없다"는 뜻으로 정확히 쓰임). `example_target_form`
컬럼에 예문 안에서 실제로 쓰인 표면형을 별도로 기록했다(활용형이 다른 경우
포함). **49건 전부 PASS.**

### 2-4. 동형이의어 검사

- **"정상" 명시적 검사**: literacy.db 저장 뜻("특별한 변동이나 탈이 없이
  제대로인 상태")과 일상어 "산 정상"(꼭대기, 완전히 다른 뜻)을 별도로
  대조했다. literacy.db에는 "제대로인 상태" 뜻 1행만 존재(동음이의 분리 행
  없음)하므로 이 데이터셋 안에서는 뜻이 섞일 위험이 없지만, **향후 문항
  제작 시 "정상"이라는 낱말만 보고 학생이 "산꼭대기"로 오해할 수 있어
  캐주션을 결과표에 유지했다**(phase12 2절 판정 재확인, PASS(주의) 유지).
- **"지향/지양"류 파생어 쌍 스캔**: 50건 lemma 쌍(같은 길이, 글자 1개
  차이) 전수 대조 + 50건 대 vocabulary_contents 5,723건 lemma 전수 대조를
  했다(원시 후보 189쌍, `scripts/vocab/`의 임시 스캔 스크립트로 계산,
  결과는 스크래치 파일에만 남기고 리포지토리에는 커밋하지 않음 — 지시상
  git 작업 자체가 없으므로 무관). **원시 후보를 전부 수작업으로 다시
  읽었고, 실제로 "형태 비슷·뜻이 반대 또는 심하게 혼동되는" 진짜 위험
  쌍은 새로 발견하지 못했다** — 대부분 우연한 음절 겹침(예: "종속"↔"종료",
  "완료"↔"동료", "고원"↔"고삐")으로 어근도 뜻도 무관했다. 예외적으로
  낮은 수준의 주의가 필요하다고 판단한 것은 다음 두 쌍이다:
  - **"유추"/"유사"**(둘 다 50건 안, V): 어근 '유(類)'를 공유하고 둘 다
    "비슷함" 개념과 관련되지만, "유사"=비슷함 그 자체, "유추"=비슷함에
    근거해 다른 것을 미루어 추측하는 사고 과정으로 뜻이 다르다. 지향/지양
    처럼 정반대는 아니지만 초등 학습자가 혼동할 수 있어 결과표에 캐주션을
    남겼다.
  - **"사법권"(50건 안, S) / "사법부"**(50건과 무관한 기존
    `vocabulary_contents` 콘텐츠, REUSE 대상 아님): 권한(사법권) vs 그
    권한을 행사하는 기관(사법부)을 혼동할 수 있어, 향후 이 두 콘텐츠를
    같은 세트로 출제할 경우 구분을 명시할 것을 권장 기록했다.
  - "지향"(50건에 포함) 자체는 phase12가 이미 "지양"(literacy.db V L4에
    존재하나 반대 뜻)과의 혼동을 우려해 "지양"을 배치에서 제외한 결정을
    유지했다 — 이번 세션 재확인 결과 "지양"은 vocabulary_contents
    5,723건에도 존재하지 않아(REUSE 무관) 이번 적재와는 접점이 없다.

---

## 3. 작업 3 — S(교과개념어) 14건 적합성 대조 + 개별 HOLD

### 3-1. 교과·주차·주제 메타데이터 대조

literacy.db `terms.note` 필드(예: "소분류: 전류가 만드는 자기장 / 주차: 1주차")와
`terms.subject_category`(예: "물리")를 이번 세션에 직접 재조회해 phase12 CSV의
`subject_category`/`note_week` 컬럼과 한 글자도 틀리지 않고 일치함을 확인했다.

| 표제어 | 교과 | 주차/소분류 | 정의-학생용 정의 일치 | 판정 |
|---|---|---|---|---|
| 자기장 | 물리 | 1주차 / 전류가 만드는 자기장 | 일치 | PASS |
| 비열 | 물리 | 3주차 / 열을 받으면 커지는 것들이 있다고? | 일치 | PASS |
| 이동거리 | 물리 | 4주차 / 물체가 어떻게 운동을 할 수 있을까? | 일치 | PASS |
| 해류 | 지구과학 | 7주차 / 바닷물의 움직임 | 일치 | PASS |
| 기권의 층상구조 | 지구과학 | 8주차 / 뜨거워지는 지구 | 일치 | PASS |
| 세포분열 | 생명과학 | 14주차 / 우리 몸의 성장은 어떻게 이뤄지는가? | 일치 | PASS |
| 순물질 | 화학 | 17주차 / 라면 끓일 때... | 일치 | PASS |
| 사회변동 | 일반사회 | 1주차 / 사회 변화의 물결 | 일치 | PASS |
| 고원 | 지리 | 4주차 / 산지지형 | 일치 | PASS |
| 곶 | 사회 | 5주차 / 해안 지형 | 일치 | PASS |
| 가변성 | 사회 | 6주차 / 자원을 두고 경쟁하는 지구촌 | **원문이 서술문, 표제어 자체 정의 아님** | **HOLD** |
| 대통령 | 사회 | 14주차 / 행정부 | 일치 | PASS |
| 사법권 | 사회 | 15주차 / 사법부 | 일치(단, 기존 "사법부" 콘텐츠와 권한/기관 구분 캐주션, 2-4절) | PASS |
| 균형 가격 | 사회 | 18주차 / 시장 경제의 보이지 않는 손 | 일치 | PASS |

과학 7(물리 3/지구과학 2/생명과학 1/화학 1) : 사회 7(일반사회 1/지리 1/사회 5)
로 phase12가 확정한 분포와 정확히 일치함을 재확인했다.

### 3-2. 개별 HOLD — "가변성"

- literacy.db 정의(원문 그대로): "자원의 가치는 기술 수준, 경제 수준, 사회
  문화적 배경에 따라 변함."
- 문제: 이 문장은 "가변성"이라는 **표제어를 사전적으로 정의하는 문장이
  아니라, 자원의 가치가 상황에 따라 달라진다는 사실을 서술하는 문장**이다.
  "성질"이라는 지정(가변성=~하는 성질)은 원문에 명시돼 있지 않고, 학생용
  정의 작성 과정에서 해석으로 보충한 것이다. 나머지 13건은 모두 표제어를
  직접 정의하는 문장(예: "해류"="~ 흐름", "고원"="~ 곳")이라 이 1건만
  독립 근거가 상대적으로 약하다.
- **처리**: "전문가 검수가 없다"는 이유로 작업 전체를 멈추지 않고, 이
  1건만 개별 HOLD 처리했다. 나머지 49건(S 13건 포함)은 계속 진행해
  적재까지 완료했다.
- **"부여"·"수집"(phase12 기록 이중위험) 관련**: 이 두 단어는 이번 50건
  (B10+C11+D29)에 포함되지 않는다 — `group=E_34_L5_spiral_check`(L5,
  이번 세션이 손대지 않은 목록)에만 속해 있다. 따라서 이번 세션에서
  HOLD 처리할 대상이 아니며, 향후 L5 배치 작업 시 그대로 이중위험으로
  다뤄야 한다(재확인만 하고 조치는 하지 않음 — 지시된 범위 밖).

---

## 4. 작업 4 — dry-run 표 + 게이트 기반 단일 트랜잭션 적재

### 4-1. 최종 분류 (dry-run 표)

| 분류 | 건수 | V | S |
|---|---:|---:|---:|
| REUSE | 0 | 0 | 0 |
| NEW_PRIVATE_LOADABLE | 49 | 36 | 13 |
| HOLD | 1 | 0 | 1 |
| **합계** | **50** | **36** | **14** |

행별 근거는 `data/import/schema_reading_phase13_l4_core50_final_20260925.csv`의
`final_classification`/`classification_reason` 컬럼 참고.

### 4-2. 스키마 재확인 (라이브)

서버 `vocabulary_quiz_research.db`에서 `PRAGMA table_info`로 스키마를 이번
세션에 직접 재확인했다(과거 기록을 맹신하지 않음):

- `vocabulary_contents`(23컬럼): `content_id`(UNIQUE), `lemma`, `pos`,
  `canonical_definition`, `student_definition`, `example_sentence`,
  `example_target_form`, `generation_method`, `qa_method`,
  `generation_status`(nullable), `student_exposure`(기본 0),
  `public_ready`(기본 0), `hold_reason`, `merge_source`, `source_version`,
  `is_active` 등.
- `vocabulary_content_levels`(14컬럼): `content_id`, `vocab_level`,
  `target_grade_band`, `level_score`, `level_confidence`, `level_status`
  (NOT NULL), `boundary_flag`, `level_source`, `level_version`,
  `level_reason_json`, `is_active`.
- 마이그레이션 전 행수: 양쪽 다 5,723건(phase4/phase7과 동일 — 그 사이
  변경 없음을 재확인).
- **`level_status` 기존 사용 값은 `PROVISIONAL_AUTO`(3,580건)/
  `REVIEW_BOUNDARY`(2,143건) 딱 두 가지뿐이며, 둘 중 어느 쪽도 문자
  그대로 "사람이 직접 작성·검토함"을 의미하지 않는다**(라우터 코드
  `app/vocabulary_quiz/routers/multiformat.py` 기준: `PROVISIONAL_AUTO`=
  자동 채점 신뢰도가 높아 `auto_only` 모드에서도 노출, `REVIEW_BOUNDARY`=
  낮은 신뢰도라 `all_candidates` 모드에서만 노출). **새 값을 발명하지
  않고 `REVIEW_BOUNDARY`를 선택했다** — 이번 49건은 자동 채점 알고리즘을
  거치지 않았고(그래서 `PROVISIONAL_AUTO`는 부정확한 라벨), 사람이
  literacy.db 정의와 대조해 처음 작성했을 뿐 별도 QA 배치나 교과 전문가
  검수(3절 리스크)를 거치지 않아 "추가 검토가 필요하다"는
  `REVIEW_BOUNDARY`의 의미론에 더 가깝다고 판단했다. `boundary_flag=1`도
  함께 세워 이 판단을 구조적으로도 남겼다.
- `generation_method`는 기존 값 `MANUAL_STRUCTURED_AUTHORING`(4,893/5,723,
  최다 사용값)을 재사용했다 — 사람이 literacy.db 정의를 읽고 학생용
  정의·예문을 직접 작성했다는 사실과 정확히 일치. `qa_method`도 기존 값
  `DETERMINISTIC_PLUS_HEURISTIC`(4,893건)을 재사용했다 — 결정론적 대조
  (정의 문자열/레벨 재조회)와 휴리스틱 검토(동형이의어 스캔, 예문 자연스러움
  직접 판독)를 둘 다 수행했으므로.
- **`generation_status`는 의도적으로 NULL로 남겼다.** 코드 확인 결과
  (`app/vocabulary_quiz/routers/quiz.py` 1~5행, 49~51행) 구식 라우터
  `quiz.py`는 `vocabulary_items`와 `vocabulary_contents`를 조인해
  `generation_status IN (PRIVATE_SERVER_READY, PRIVATE_SERVER_READY_CANDIDATE)`
  인 것만 화이트리스트로 출제한다. 이번 49건에 대응하는 `vocabulary_items`
  행을 아예 만들지 않았으므로(JOIN 자체가 성립하지 않음) 구조적으로 이미
  출제 불가능하지만, `generation_status`를 NULL로 둬 이 화이트리스트에도
  애초에 들지 못하게 하는 **방어선을 하나 더** 추가했다.
- `source_version`은 기존 5,723건 전부가 `'2.1.29'` 단일 값인데,
  이번 49건은 의도적으로 **다른 값(`schema_reading_literacy_l4_manual_v1`)**
  을 부여했다 — `quiz.py`/`multiformat.py` 양쪽 다 `source_version=2.1.29`를
  요구하므로, 혹시 미래에 어떤 코드가 실수로 `vocabulary_items`를
  만들더라도 `source_version` 불일치로 한 번 더 걸러지도록 하는 방어선.

### 4-3. 게이트별 pass/fail

| 게이트 | 내용 | 결과 |
|---|---|---|
| 1 | `APP_ENV=research` 재확인(`.env` + 실행 중 프로세스 `/proc/<PID>/environ`, PID 1162248 재확인 — phase4의 PID 1100600에서 재기동돼 PID는 바뀌었으나 값은 byte-for-byte 동일) | **PASS** |
| 2 | SQLite Backup API 백업 생성(`scripts/vocab/vq_research_backup.py` 그대로 재사용) | **PASS** |
| 3 | 백업 무결성(`integrity_check`=ok, FK 위반 0)·행수 일치(원본=백업, 5,723=5,723) | **PASS** |
| 4 | 마이그레이션 전 체크섬 스냅샷(`vocabulary_contents`/`vocabulary_content_levels`) | **PASS** — 저장한 체크섬이 phase4/phase7의 마지막 확인값과 완전히 동일(`117ad373...4cc926b`, `6a0c97cc...b0fc6f30`) → 그 사이 아무도 건드리지 않았음을 재확인 |
| 5 | `NEW_PRIVATE_LOADABLE` 49건만 단일 트랜잭션으로 `vocabulary_contents`+`vocabulary_content_levels` 동시 삽입(하나만 생기는 상태 방지) | **PASS** — 49건 모두 커밋, 예외 0건 |
| 6-a | 적재 후 기존 5,723행 체크섬 불변(신규 49건 제외하고 재계산) | **PASS** — `117ad373...4cc926b`/`6a0c97cc...b0fc6f30` 그대로 |
| 6-b | 신규 49건 `student_exposure`/`public_ready` 전부 0 | **PASS**(49/49) |
| 6-c | `PRAGMA integrity_check` / `foreign_key_check` (적재 후) | **PASS**(ok / 위반 0) |
| 6-d | `vocabulary_items`/`vocabulary_multiformat_items` 신규 콘텐츠 참조 여부 | **PASS** — 둘 다 0건 참조(퀴즈 문항 생성 안 함, `vocabulary_multiformat_items` 행수 1,289건 불변) |
| 7 | 멱등성 — 동일 스크립트 재실행 | **PASS** — 2회차 신규 삽입 0건, 스킵(이미 존재) 49건 |

**하나도 fail하지 않아 롤백을 실행하지 않았다.** 백업은 만약을 위해 그대로
보존돼 있다(복구 절차는 5절).

### 4-4. 적재 스크립트/증적

- `scripts/vocab/vq_research_backup.py` (백업, 서버 실행, phase4와 동일 스크립트 재사용)
- `scripts/vocab/vq_checksum_core_tables.py` (체크섬, 서버 실행, phase4/phase7과 동일 스크립트 재사용)
- `scripts/vocab/phase13_reuse_check.py` (REUSE 대조, 서버 실행, 읽기 전용)
- `scripts/vocab/phase13_apply_l4_core.py` (게이트 1·2·3·4·5 + 단일 트랜잭션 적재 + 멱등성, 서버 실행)
- 서버 산출물: `~/scratch/phase13_l4_core/`(lemmas.json, reuse_candidates.json,
  phase13_final_rows.json, checksum_before.json, checksum_after_full.json)

---

## 5. 백업 · 롤백 절차

- 백업 파일: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase13-l4-core50-pre-migration-20260924-154138`
- 백업 SHA-256: `48c1872cea9d28866b42826d1d3382d96ddcd2d5c5dc93e1692a2d619d4b2aeb`
- 백업 크기: 12,189,696 bytes
- 백업 검증: 행수 일치(5,723=5,723), `integrity_check`=ok, `foreign_key_check` 위반 0

게이트가 전부 PASS했으므로 롤백을 실행하지 않았다. 문제가 발견되면(참고용,
실행 안 함):

```bash
ssh aprolabs
cp /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.rollback-before-restore-$(date +%Y%m%d-%H%M%S)
cp /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase13-l4-core50-pre-migration-20260924-154138 \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
sqlite3 /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db "PRAGMA integrity_check;"
```

이 백업 하나로 이번 49건 삽입(과 그 외 아무 변화도 없음)을 완전히 되돌릴 수
있다.

---

## 6. 작업 5 — 적재 후 최종 검증

| 항목 | 결과 |
|---|---|
| 기존 콘텐츠(5,723건) 체크섬 불변 | **PASS** — 신규 content_id(`SR_L4CORE_%`)를 제외한 5,723건만 재계산해도 마이그레이션 전과 완전 동일 |
| `vocabulary_content_levels` 기존 5,723건 체크섬 불변 | **PASS** |
| 신규 49건 `student_exposure`=0 전부 | **PASS**(49/49) |
| 신규 49건 `public_ready`=0 전부 | **PASS**(49/49) |
| `vocabulary_items`(구식 퀴즈 문항 테이블) 행수 | 5,723건, 마이그레이션 전후 불변 — 신규 content_id를 참조하는 행 0건 |
| `vocabulary_multiformat_items`(신식 퀴즈 문항 테이블) 행수 | 1,289건, 마이그레이션 전후 불변 — 신규 content_id를 참조하는 행 0건 |
| `vocabulary_content_literacy_links`(phase4가 만든 링크 테이블) 행수 | 142건, 불변(이번 세션은 이 테이블을 전혀 건드리지 않음) |
| `PRAGMA integrity_check` | ok |
| `PRAGMA foreign_key_check` | 위반 0건 |
| 코드 변경 | 없음(앱 코드 파일 수정 0건, 읽기만 함) |
| git 작업 | 없음(commit/push 없음) |
| literacy.db 변경 여부 | 없음(mode=ro 유지, 파일 mtime 세션 시작 전 그대로) |
| A_29(SPIRAL_REVIEW, L4)·E_34(L5_CANDIDATE 6 + L5_SPIRAL_REVIEW 28) 보존 여부 | **그대로 보존** — 이번 세션에서 조회·수정 모두 하지 않음, 12단계 결과표에 그대로 남아 있음 |

**학생 공개·운영 배포를 하지 않았음을 구조적으로 확인**: 신규 49건을 참조하는
`vocabulary_items`/`vocabulary_multiformat_items` 행이 0건이라, `quiz.py`·
`multiformat.py` 어느 라우터의 "출제 가능" 쿼리도 이 콘텐츠를 절대 반환할 수
없다(코드 직접 확인, 4-2절). 즉 이번 적재는 데이터만 존재할 뿐 어떤 학생
노출 경로에도 연결돼 있지 않다.

---

## 7. 리스크 (우선순위 순)

1. **S(교과개념어) 13건(가변성 제외)은 교과 전문가(과학/사회) 검수를 아직
   거치지 않았다** — phase12가 이미 지적한 리스크를 이번 세션도 해소하지
   못했다. `level_status=REVIEW_BOUNDARY`/`boundary_flag=1`로 "추가 검토
   필요"를 구조적으로 표시해 뒀지만, 실제 문항화(=`vocabulary_items`/
   `vocabulary_multiformat_items` 생성) 전에는 사람이 한 번 더 봐야 한다.
2. **"가변성" 1건은 HOLD 상태로 미적재** — 향후 이 단어를 다시 검토하려면
   literacy.db 원문("자원의 가치는 ~ 따라 변함")을 표제어 정의문으로
   재해석할 근거를 보강하거나, 다른 정의 출처를 찾아야 한다.
3. **"유추"/"유사"(둘 다 이번에 적재됨), "사법권"(이번 적재)/"사법부"
   (기존 콘텐츠, 무관)**는 지향/지양급은 아니지만 낮은 수준의 혼동
   위험이 있다 — 향후 이 콘텐츠들로 실제 문항을 만들 때는 두 뜻을 함께
   보여주지 않거나 구분 문구를 넣는 것을 권장한다.
4. **"부여"·"수집"(phase12 이중위험)은 이번 50건과 무관**(L5,
   `group=E_34_L5_spiral_check`) — 향후 L5 작업 시 별도로 다뤄야 한다.
5. **`source_version`/`generation_status`를 의도적으로 표준값과 다르게
   설정**했다(4-2절 방어선) — 이는 "출제 절대 불가"를 이중·삼중으로
   보장하려는 선택이지만, 만약 향후 누군가 이 49건을 정식 파이프라인에
   편입시키려면 `source_version`을 `2.1.29`로, `generation_status`를
   `PRIVATE_SERVER_READY_CANDIDATE`로 **의도적으로 변경하는 별도
   마이그레이션이 필요하다**는 점을 다음 세션이 알아야 한다(자동으로
   편입되지 않음).

---

## 8. 산출물 목록

### 로컬(`C:\Users\aproa\aprolabs`, 커밋하지 않음)

- 본 보고서: `reports/schema_reading_phase13_l4_core50_apply_20260925.md`
- 결과표: `data/import/schema_reading_phase13_l4_core50_final_20260925.csv`,
  `data/import/schema_reading_phase13_l4_core50_final_20260925.jsonl`
- 스크립트: `scripts/vocab/phase13_reuse_check.py`,
  `scripts/vocab/phase13_apply_l4_core.py`
  (`scripts/vocab/vq_research_backup.py`, `scripts/vocab/vq_checksum_core_tables.py`는
  phase4/phase7 기존 스크립트를 그대로 재사용, 신규 파일 아님)

### 서버(`aprolabs`, research)

- 백업: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase13-l4-core50-pre-migration-20260924-154138`
- 작업 스크립트/체크섬/중간 산출물: `~/scratch/phase13_l4_core/`
- 실제 변경: `vocabulary_quiz_research.db`의 `vocabulary_contents`+
  `vocabulary_content_levels`에 각각 49행 신규 삽입(content_id `SR_L4CORE_<literacy_term_id>`)
  — 그 외 기존 테이블/행 변경 없음(6절 체크섬으로 증명)
