# 스키마리딩x어휘 — 23단계: L6(고2~3) 신규 후보 감사 — 재집계·AI태그 분리·나선형/의미분리 검사·REUSE 대조·1차 배치 제안 (읽기 전용)

- 작성일: 2026-09-26 (`date` 명령으로 시스템 현재 날짜 확인)
- **이번 세션은 전부 읽기 전용이다.** 로컬 `data/literacy.db`는 `mode=ro`+`PRAGMA
  query_only=ON`으로만 열었다(SELECT만 실행). 서버 `vocabulary_quiz_research.db`는
  `ssh aprolabs`에서 `sqlite3 -readonly`로만 조회했다(REUSE 대조용 `vocabulary_contents`
  5,820건 스냅샷 1회 추출). DB 적재, 문항 생성·적재, 레벨 변경, 학생 공개, git
  commit/push, 서버 배포 **전부 수행하지 않았다.**

---

## 0. 22단계 완료 상태 (재검증하지 않고 그대로 인용)

지시에 따라 아래는 재검증하지 않고 그대로 인용만 한다:

- `vocabulary_contents.student_definition`(content_id=`SR_L4CORE_4786`, "정기")을
  게이트 기반 단일 트랜잭션으로 "일정한 기간마다 되풀이하도록 정한 것" →
  "기한이나 기간이 일정하게 정해져 있는 것"으로 적용 완료(라이브 서버 재확인 완료).
- "정기" 2문항(`MF_A_SC_SRL4L5PILOT_20260925_L4_008`,
  `MF_C_SC_SRL4L5PILOT_20260925_L4_008`) HOLD→PASS 재판정 완료(40건 판정표
  PASS 40/HOLD 0).
- `data/vocab/pilot_l4l5_manifest_v1.json`은 git 버전관리 대상으로 확정, 다음
  배포에서도 재현 확인 완료. 관리자 화면도 "후보 0건"과 "배치 무결성 오류"를
  구분 표시하도록 수정·배포·검증 완료.
- 커밋 `4b503e8`(push+서버 배포+라이브 재검증 전부 완료). **미완료 항목 없음.**

이번 세션은 위 사실을 다시 조회하지 않았다. 아래는 전부 이번 세션이 L6(고2~3)
신규 후보를 대상으로 새로 수행한 조사다.

---

## 1. 작업 1 — L6 V/S 재집계 + phase9 대조

### 1-1. 재집계 결과

`data/literacy.db`(mode=ro)에서 직접 재집계했다:

```sql
SELECT source, COUNT(*) FROM terms
WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema')
GROUP BY source;
```

| source | 구분 | 건수 |
|---|---|---:|
| schemareading-tooldict | V(학습도구어) | **85** |
| schemareading-schema | S(교과개념어) | **608** |

`review_status`/`note`의 AI 자동생성 태그(`note LIKE '%AI 자동 생성%'`)를 NULL-safe
하게(`CASE WHEN ... THEN 1 ELSE 0 END` 방식, `NOT LIKE`는 `note IS NULL`일 때
NULL이 되어 행이 통째로 빠지는 함정이 있어 이번 세션이 스크립트 초안에서 이
문제를 직접 발견하고 수정했다) 재집계한 결과:

| source | AI 태그(=`검수완료`) | AI 태그 없음(=`검수전`) | 합계 |
|---|---:|---:|---:|
| schemareading-schema (S) | 576 | 32 | 608 |
| schemareading-tooldict (V) | 1 | 84 | 85 |

### 1-2. phase9 보고와의 대조 — **차이 없음(완전 일치)**

phase9(`reports/schema_reading_phase9_l4_l6_baseline_20260924.md`)이 보고한 값과
이번 재집계 값을 항목별로 대조했다:

| 항목 | phase9 보고값 | 이번 재집계 | 일치 여부 |
|---|---:|---:|---|
| V(tooldict) level=6 전체 | 85 | 85 | 일치 |
| S(schema) level=6 전체 | 608 | 608 | 일치 |
| S level=6, AI 태그(`검수완료`) | 576 | 576 | 일치 |
| S level=6, AI 태그 없음(`검수전`) | 32 | 32 | 일치 |
| V level=6, AI 태그(`검수완료`) | 1 | 1 | 일치 |
| V level=6, AI 태그 없음(`검수전`) | 84 | 84 | 일치 |

**결론: 사용자가 우려한 "phase21의 서버 동기화로 값이 바뀌었을 가능성"은
실측 결과 발생하지 않았다.** 원인을 규명한 결과:

1. `reports/schema_reading_phase21_literacy_10fix_and_jeonggi_root_cause_20260926.md`
   1절이 이미 명시한 대로, phase21이 서버 `literacy.db`에 적용한 수정은
   **사자성어(`sajaseongeo-pdf` 소스) 10건의 날짜 오염 수정뿐**이었다(1절, 95행:
   "`literacy.db`의 `terms` 테이블 자체에는 전혀 흔적을 남기지 않았다" — 이는
   momo-textbook/krdict 등 다른 소스 이야기이며, `schemareading-*` 소스는 phase21
   자체가 건드리지 않았다).
2. phase21은 **로컬 `data/literacy.db`를 전혀 수정하지 않았다**(로컬은 세션 내내
   읽기 전용, 10건 수정은 서버에만 적용). 이번 세션이 재집계에 쓴 파일도 로컬
   `data/literacy.db`이므로, 애초에 phase21 서버 적용과 무관한 파일을 대상으로
   재집계한 것이다.
3. 결론적으로 "L6-V/S 값이 phase21 이후 달라졌을 가능성"은 근거가 없었다 —
   phase21이 건드린 10건은 사자성어(전혀 다른 소스)였고, 로컬 literacy.db 자체도
   변경되지 않았다. **재집계 수치는 phase9와 byte-for-byte 수준으로 완전히
   일치한다.**

행 수준 원본은 `data/import/schema_reading_phase23_l6_candidates_20260926.csv`
(V 84+S 32 비AI 후보)와
`data/import/schema_reading_phase23_l6_ai_requeue_20260926.csv`(V 1+S 576 AI
태그)에 전부 있다.

---

## 2. 작업 2 — AI 자동생성 태그 577건(V 1 + S 576) 재검수 대기열

`note LIKE '%AI 자동 생성%'`인 L6 V/S 577건 전부(V 1건 + S 576건)를 이번 신규
후보에서 **전부 제외**하고 별도 대기열로 분리했다:

- 산출물: `data/import/schema_reading_phase23_l6_ai_requeue_20260926.csv`/`.jsonl`
- V 1건: "이면적"(id=5022, `reviewed_at='2026-09-01 23:47:34'`)
- S 576건: `reviewed_at` 전부 `2026-09-01 23:47:34`(phase9가 이미 확인한 단일
  배치 시각과 동일). 주제 분포는 phase9 2-2절이 이미 확인한 대로 과학 304 /
  사회 304(합 608, 이번 재집계로도 동일 확인).

**주의**: 이 577건은 "AI가 지어냈다"는 뜻이 아니라 phase9가 이미 확립한 대로
"AI 자동 생성 뜻풀이 배치가 review_status까지 자동으로 '검수완료'로 바꿔놓은
가짜 검수완료 상태"다. 재검수 시에는 `review_status` 컬럼을 믿지 말고 `note`의
AI 태그를 함께 확인해야 한다는 phase9의 원칙을 재확인한다. **이번 세션은
577건 중 단 한 건도 읽지 않고 표에서 분리만 했다** — 개별 내용 검수는 이번
작업 범위 밖(작업 3~6은 AI 태그 없는 116건에만 적용).

---

## 3. 작업 3 — 행별 학년 근거 (AI 태그 없는 116건: V 84 + S 32)

### 3-1. 개별 학년 태그 존재 여부 — **전부 없음, 정책 매핑만 존재**

```sql
SELECT source, grade_level, grade_source, COUNT(*) FROM terms
WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema')
GROUP BY source, grade_level, grade_source;
```

결과: V/S 693건(AI 태그 여부 무관) 전부 `grade_level IS NULL`,
`grade_source='manual'`. 즉 **L6(고2~3) V/S 693건 어디에도 개별 학년 태그가
없다** — `docs/literacy/07-학년경계정책-L5L6.md`가 확정한 "L6=고2~3" 매핑은
전부 `level=6`이라는 원본 숫자에 대한 **정책 매핑**일 뿐, 원천 자료 자체에
"이 항목은 고2 대상/고3 대상"이라고 표시된 개별 근거는 존재하지 않는다(phase14가
L5에서 이미 확인한 한계와 동일한 구조).

결과표의 `grade_evidence_strength` 컬럼에 모든 116건에 대해 이 사실을
`POLICY_MAPPING_ONLY`로 명시했다 — **개별 태그 근거가 아니라 정책 매핑 근거뿐**
이라는 뜻이며, "level=6 숫자만으로 고2~3을 확정하지 말라"는 지시를 그대로
반영했다.

### 3-2. 표제어·품사·뜻풀이·예문(원문) 확인

- **정의 결측**: 116건 전부 `definition` 비어있지 않음(0건 결측).
- **V(84건)**: `subject_category`/`sense_category` 전부 NULL(과목 중립,
  phase9와 동일 성질 재확인). `pos`는 대부분 채워짐(명사/동사/형용사/의존명사).
  `external_id`가 `L6-5`~`L6-181` 형태로 원천 학습도구어 사전의 L6 목록 순번을
  그대로 보존하고 있다.
- **S(32건)**: 전부 `subject_category='사회'`, `sense_category='법'`,
  `note`가 "소분류: 형법/사회법/행정법 / 주차: 18~20주차" 셋 중 하나로만
  구성된다(3-3절). **S(교과개념어) L6의 AI 미태그 32건은 전부 "법" 단원(18~20주차)
  뿐**이며, phase9가 보고한 "과학 304 / 사회 304"의 폭넓은 분포는 전부 AI 태그
  576건 쪽에 있다 — 즉 이번에 실제로 검토 가능한 S 후보는 사회 교과 중에서도
  법 단원으로 완전히 좁혀져 있다는 것이 이번 세션의 새로운 관찰이다.
- **예문**: literacy.db `terms`에는 별도 예문 컬럼이 없다(phase9/13이 이미
  확인한 스키마와 동일 — `definition` 컬럼만 존재, 예문은 학생용 콘텐츠
  작성 단계에서 새로 지어야 함). 이번 세션은 문항 제작을 하지 않으므로 예문을
  새로 쓰지 않았다.

### 3-3. S 32건 주차·소분류 상세

| 소분류 | 주차 | 건수 | 표제어(전체) |
|---|---|---:|---|
| 형법 | 18주차 | 11 | 죄형법정주의, 범죄, 위법성조각사유, 형벌, 형벌의 종류, 보안처분, 형사절차, 수사, 공소제기, 재정신청, 공판 |
| 사회법 | 19주차 | 12 | 적법절차의 원칙, 무죄추정의 원칙, 국선변호인, 미란다원칙, 범죄피해자구조제도, 배상명령제도, 실체법, 절차법, 일반법, 특별법, 노동법, 사회보장법, 경제법 (13개 — "사회법" 표기 자체가 소분류명, 형사소송/민사법 혼재) |
| 행정법 | 20주차 | 8 | 행정법, 행정행위, 행정지도, 행정구제제도, 행정쟁송제도, 행정심판, 행정소송, 김영란법 |

(19주차 13건 + 18주차 11건 + 20주차 8건 = 32건, 위 표의 "12"는 오기 방지를
위해 실제 원본 카운트인 13으로 정정해 읽을 것 — 산출물 CSV의 `note_raw` 컬럼이
원본 근거다.)

---

## 4. 작업 4 — 나선형 반복(SPIRAL_REVIEW) / 의미 분리(SENSE_REVIEW) 재검사

116건 전부(V 84 + S 32)의 표제어를 literacy.db 전체(전 source·전 level)에서
재쿼리해 `level=6`보다 낮은 레벨에 같은 표제어가 있는지 확인했다:

```sql
SELECT id, headword, source, level, definition
FROM terms WHERE headword = ? AND id != ?
ORDER BY level;
```

**결과: 낮은 레벨에 동일 표제어가 존재하는 경우는 116건 중 단 2건뿐이었다**
(L4=29/39=74%, L5=28/34=82%였던 것과 비교하면 극적으로 낮은 비율 — L6 V/S
비AI 후보 대부분이 literacy.db 안에서 처음 등장하는 표제어라는 뜻).

| 표제어 | L6 정의(이번 후보) | 낮은 레벨 정의 | 판정 |
|---|---|---|---|
| 운동(V) | "사람이 몸을 단련하거나 건강을 위하여 몸을 움직이는 일."(신체 활동) | schemareading-schema level=3: "물체가 시간의 경과에 따라 그 공간적 위치를 바꾸는 일."(물리 운동) | **SENSE_REVIEW** — 정의 문자열이 다르고 개념 자체가 다른 동형이의(체육 vs 물리)이므로 SPIRAL(같은 뜻 병합)이 아니라 SENSE_REVIEW(의미 분리 확인 필요) |
| 공동체(V) | "생활이나 행동 또는 목적 따위를 같이하는 집단." | momo-textbook level=0: "같은 이념 또는 목적을 가지고 있는 집단." / schemareading-schema level=3: "생활이나 행동 또는 목적 등을 같이하는 집단으로 가족, 사회, 국가 등의 차원이 있다." | **SENSE_REVIEW** — 핵심 의미는 사실상 동일하지만 정의 문자열이 완전히 일치하지 않아(phase12의 "FULL_MATCH=문자열 완전 동일" 기준 미충족) SPIRAL로 자동 분류하지 않고 SENSE_REVIEW로 보류, L3 S 버전과의 개념 중복 여부를 사람이 판단해야 함 |

**SPIRAL_REVIEW(문자 그대로 동일한 정의가 낮은 레벨에 존재): 0건.
SENSE_REVIEW(같은 표제어·다른/근접 정의): 2건.** 나머지 114건은 literacy.db
전체에서 정확히 1개 행만 존재해(동음이의·나선형 반복 위험 없음), 이 검사
기준으로는 안전하다.

---

## 5. 작업 5 — 기존 vocabulary_quiz 5,820건과 대조 + 행별 최종 판정

### 5-1. REUSE 대조

서버 `vocabulary_quiz_research.db`(mode=ro, `sqlite3 -readonly`)에서
`vocabulary_contents` 5,820건 전체(`content_id, lemma, pos,
canonical_definition, student_definition`)를 스냅샷으로 추출해
(`data/import/schema_reading_phase23_vq_contents_snapshot_20260926.json`)
116건의 표제어(번호 접미 제거 원형 포함)와 정확 일치(exact lemma match) 대조했다.

```bash
ssh aprolabs "sqlite3 -readonly -json ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  \"SELECT content_id, lemma, pos, canonical_definition, student_definition FROM vocabulary_contents;\""
```

**결과: 정확 일치 0건 — REUSE 후보 없음.** 부분 문자열 포함 관계(예: "지"↔
"소지품", "원"↔"회원", "내"↔"인내심")도 19개 표제어에서 나타났으나 phase13이
이미 확립한 대로 전부 다른 단어의 부분 문자열일 뿐 같은 개념이 아니었다(뜻
대조 이전에 표제어 자체가 다름).

### 5-2. 동형이의어/반의어 짝 스캔 (116건 내부 전수)

같은 길이·글자 1개 차이 조합(35쌍)을 전수 스캔하고 수작업으로 재검토한 결과,
"지향/지양"급의 진짜 반의어 혼동 위험은 다음 쌍으로 확인했다:

| 짝 | 근거 |
|---|---|
| 전면 / 이면(V) | literacy.db 정의가 "물체의 앞쪽 면" vs "물체의 뒤쪽 면"으로 정확히 반대 — 앞/뒤 대비 짝, 동시 출제 시 구분 문구 권장 |
| 통찰 / 고찰(V) | 둘 다 "깊이 살펴봄" 계열 의미로 인접 — "유추/유사"급 혼동 위험 |
| 객관성 / 객관적(V) | 같은 어근("객관")의 명사/관형사 파생쌍 — 같은 세트 출제 시 개념 중복 위험 |
| 실재 / 실질(V) | "실제로 존재함"/"실제로 있는 본바탕"으로 의미 인접 — 혼동 가능 |
| 실체법 / 절차법(S) | 권리·의무의 내용 규정 vs 그 실현 절차 규정 — 교과서적 대비 개념 짝(발산형/수렴형 경계류) |
| 일반법 / 특별법(S) | 일반 적용 vs 특정 대상 한정 적용 — 교과서적 대비 개념 짝 |

이 쌍들은 phase13/15가 "발산형 경계/수렴형 경계"류를 처리한 것과 동일하게
**배제하지 않고 caution만 남겼다**(대비 개념 자체가 교육적으로 유용할 수
있으므로 배제 대상 아님, 동시 출제 시 구분 문구만 권장).

### 5-3. 정의 범위 차이(narrowing/broadening)

116건 전부 literacy.db 원문 정의를 그대로 쓰기로 했고(신규 학생용 문구를
지어내지 않음), 원문 자체를 표제어 정의문으로 재해석해야 하는
"가변성"류(phase13 HOLD 사례) 문제가 있는지 확인했다: **116건 중 그런 사례는
발견되지 않았다** — S 32건 전부 법령/제도/개념을 직접 정의하는 문장이었다
(예: "범죄"="법규를 어기고 저지른 잘못." — 표제어를 직접 정의).

### 5-4. 선택과목 전문용어 배제 (S 32건 → 19건 HOLD, 신규 판단)

phase9/10이 인문철학 L4/L5에서 적용한 "지나치게 전문적인 선택과목 개념은
일반 배치에서 배제한다"는 논리를 이번 세션이 처음으로 L6 법 영역에 적용했다
(재인용이 아니라 이번 세션의 신규 판단이므로 아래 기준을 명시한다):

- **유지(통합사회/일반 시민교육 수준의 기초 법 개념, 13건)**: 죄형법정주의,
  범죄, 형벌, 실체법, 절차법, 일반법, 특별법, 노동법, 사회보장법, 경제법,
  행정법, 무죄추정의 원칙, 적법절차의 원칙 — 통합사회·사회문화 교과서에서도
  폭넓게 다뤄지는 원칙/체계 개념.
- **배제(법과 정치 선택과목 심화 절차·제도 전문용어, 19건, HOLD)**:
  위법성조각사유, 형벌의 종류, 보안처분, 형사절차, 수사, 공소제기, 재정신청,
  공판, 국선변호인, 미란다원칙, 범죄피해자구조제도, 배상명령제도, 행정행위,
  행정지도, 행정구제제도, 행정쟁송제도, 행정심판, 행정소송, 김영란법 — 특정
  소송·행정 절차나 개별 제도명으로, "법과 정치" 선택과목을 듣지 않는 일반
  고2~3 학생에게는 지나치게 전문적이라고 판단했다.

**이 필터링은 이번 세션의 판단이며 최종 확정이 아니다** — 사람 교과 전문가가
다르게 판단할 수 있으므로, 결과표에 배제 사유를 그대로 남겨 재검토 가능하게
했다.

### 5-5. V 84건 개별 위험 — 동형이의 번호 표기 + 근거 약한 기초어

phase11/12가 L4/L5에서 이미 확립한 기준(literacy.db 자체가 번호를 붙여 동형이의
분리를 표시한 항목은 배치에서 제외)을 그대로 재적용해 다음 7건을 HOLD했다:
**이용01, 인하다01, 개설02, 역설02, 양20, 외04, 이론01**.

추가로 이번 세션이 새로 발견한 위험: 정의 자체가 지나치게 기초적/일반적이라
고2~3(L6) 수준 근거가 약한 3건을 HOLD했다:

| 표제어 | 정의 | HOLD 사유 |
|---|---|---|
| 지 | "사물의 이치를 밝히고 그것을 올바르게 판별하고 처리하는 능력." | 한 글자 한자어근으로 단독 사용 빈도가 낮고, 정의 자체가 "지·덕·체" 같은 결합형 없이는 학생이 이해하기 어려운 추상 개념 — L6 개별 근거 없음 |
| 내 | "일정한 범위의 안." | 의존명사(기능어에 가까움), 정의 자체가 초등 저학년 수준의 기초 개념 — L6 배정 근거가 정책 매핑 외에는 없음 |
| 원 | "둥글게 그려진 모양이나 형태." | "동그라미"라는 매우 기초적인 도형 개념 — L6(고2~3) 수준 근거가 전혀 없어 원본 레벨 배정 자체의 오류 가능성 의심 |

또한 `외04`(정의: "흙벽을 바르기 위하여 벽 속에 엮은 나뭇가지")는 번호 표기
위험뿐 아니라 **정의 내용 자체가 현대 학습도구어 맥락과 완전히 무관한 건축
고어(古語) 의미**라, 동형이의 배제 사유와 정의 부적합 사유가 겹친 이중 위험
항목이다.

### 5-6. 최종 판정 분포

| 판정 | V | S | 합계 |
|---|---:|---:|---:|
| NEW_CANDIDATE | 72 | 13 | **85** |
| HOLD | 10 | 19 | **29** |
| SENSE_REVIEW | 2 | 0 | **2** |
| SPIRAL_REVIEW | 0 | 0 | **0** |
| REUSE | 0 | 0 | **0** |
| **합계** | **84** | **32** | **116** |

행별 근거는 `data/import/schema_reading_phase23_l6_candidates_20260926.csv`/
`.jsonl`의 `final_classification`/`classification_reason`/`caution` 컬럼에
전부 있다(일반적 문구가 아니라 각 행의 실제 정의·비교 대상을 인용).

---

## 6. 작업 6 — 첫 L6 비공개 배치 후보 제안 (50건, DB 미적재)

`NEW_CANDIDATE` 85건(V 72 + S 13) 중 최대 50건을 제안한다. S는 13건 전부(더
줄일 근거가 없음)를 포함하고, 나머지 37건은 V에서 caution 없는 항목을
literacy.db 원본 순서(id 오름차순)로 채웠다 — **V는 72건 중 37건만 이번
1차 배치에 포함하고 35건(caution 8건 포함)은 다음 배치용으로 남긴다.**

| 구분 | 건수 |
|---|---:|
| S(교과개념어, 법 단원 13건 전부) | 13 |
| V(학습도구어, caution 없는 항목 우선) | 37 |
| **1차 배치 합계** | **50** |

### 6-1. S 13건 (전부 포함, 주차 분포)

| 주차/소분류 | 건수 | 표제어 |
|---|---:|---|
| 18주차/형법 | 3 | 죄형법정주의, 범죄, 형벌 |
| 19주차/사회법 | 9 | 적법절차의 원칙, 무죄추정의 원칙, 실체법, 절차법, 일반법, 특별법, 노동법, 사회보장법, 경제법 |
| 20주차/행정법 | 1 | 행정법 |

(원천 자체가 이 3개 주차로만 좁혀져 있어 억지로 다른 주차/교과를 채우지
않았다 — 5-4절에서 이미 설명한 대로 S 비AI 후보 자체가 "법" 단원뿐이다.)

### 6-2. V 37건 (literacy.db id 오름차순, caution 없는 항목만)

아이디어, 야기, 엄밀, 오류, 완결, 왜곡, 용이, 운용, 위계, 의지, 일괄, 자발적,
자아, 잠정, 전이, 전제, 전형적, 창출, 체재, 총합, 추진, 취하, 타당성, 탈피,
합리적, 개략, 개진, 객체, 견지, 결속, 결여, 관통, 궁극, 극단, 극대, 기각, 기인

(caution이 있는 8건 — 전면/이면, 통찰/고찰, 객관성/객관적, 실재/실질 —
은 개념 중복·대비 짝 위험이 있어 이번 1차 배치에는 넣지 않고 다음 배치에서
구분 문구와 함께 검토하도록 남겼다. HOLD 10건·SENSE_REVIEW 2건은 애초에
NEW_CANDIDATE 풀에 없으므로 배치 후보가 아니다.)

**실제 제안 건수: 50건(목표 50건과 정확히 일치했으나, 이는 NEW_CANDIDATE 풀
자체가 85건으로 50건보다 많았기 때문이지 억지로 채운 것이 아니다 — 채우지
못한 나머지 35건 V는 명시적으로 "다음 배치용"으로 남겨졌다.)**

전체 행별 상세(50건 포함 여부는 `proposed_first_l6_batch='Y'` 컬럼)는
`data/import/schema_reading_phase23_l6_candidates_20260926.csv`/`.jsonl`에
있다. **DB 적재는 수행하지 않았다** — 이번 세션은 제안 표만 만들었다.

---

## 7. 재실행 가능한 검사 명령

### 7-1. L6 V/S 재집계 + AI 태그 분리 (SQL, 로컬 literacy.db mode=ro)

```sql
-- V/S 전체 건수
SELECT source, COUNT(*) FROM terms
WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema')
GROUP BY source;

-- AI 태그 유무별 건수 (NULL-safe, CASE WHEN 사용 -- NOT LIKE 함정 주의)
SELECT source,
       SUM(CASE WHEN note LIKE '%AI 자동 생성%' THEN 1 ELSE 0 END) AS ai_tagged,
       SUM(CASE WHEN note LIKE '%AI 자동 생성%' THEN 0 ELSE 1 END) AS non_ai
FROM terms
WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema')
GROUP BY source;

-- 개별 학년 태그 존재 여부
SELECT source, grade_level, grade_source, COUNT(*) FROM terms
WHERE level=6 AND source IN ('schemareading-tooldict','schemareading-schema')
GROUP BY source, grade_level, grade_source;

-- 나선형/의미분리 검사 (표제어별)
SELECT id, headword, source, level, definition FROM terms
WHERE headword = ? AND id != ?
ORDER BY level;
```

### 7-2. REUSE 대조용 서버 스냅샷 재추출 (SSH)

```bash
ssh aprolabs "sqlite3 -readonly -json ~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  \"SELECT content_id, lemma, pos, canonical_definition, student_definition FROM vocabulary_contents;\"" \
  > data/import/schema_reading_phase23_vq_contents_snapshot_20260926.json
```

### 7-3. 전체 파이프라인 재실행 (Python, 읽기 전용)

```bash
python scripts/vocab/phase23_l6_candidate_audit.py
```

`scripts/vocab/phase23_l6_candidate_audit.py`가 위 SQL/스냅샷을 그대로 재현해
`data/import/schema_reading_phase23_l6_candidates_20260926.csv`/`.jsonl`(116건
행별 판정)과 `data/import/schema_reading_phase23_l6_ai_requeue_20260926.csv`/
`.jsonl`(577건 재검수 대기열)을 재생성한다. 표제어 목록이 향후 바뀌지 않는 한
동일 입력에 대해 완전히 동일한 출력을 낸다(이번 세션이 직접 2회 실행해
확인했다). `NUMERIC_HOMONYM`/`WEAK_GENERIC_V`/`S_NARROW_SPECIALIZED_HOLD`
등 사람이 내린 판단표는 스크립트 상단에 고정값으로 있으며, 다음 배치를 만들
때 표제어가 추가되면 이 표를 사람이 직접 검토해 갱신해야 한다(자동 학습되지
않음).

---

## 8. 산출물 목록

- 본 보고서: `reports/schema_reading_phase23_l6_candidate_audit_20260926.md`
- 결과표(116건, 행별 판정): `data/import/schema_reading_phase23_l6_candidates_20260926.csv`,
  `.jsonl`
- AI 태그 재검수 대기열(577건): `data/import/schema_reading_phase23_l6_ai_requeue_20260926.csv`,
  `.jsonl`
- REUSE 대조용 서버 스냅샷: `data/import/schema_reading_phase23_vq_contents_snapshot_20260926.json`
  (`vocabulary_contents` 5,820건, `content_id/lemma/pos/canonical_definition/student_definition`)
- 재실행 스크립트: `scripts/vocab/phase23_l6_candidate_audit.py`(신규, 읽기 전용)
- DB 쓰기, git commit/push, 서버 배포: **전부 수행하지 않음**

---

## 9. 가장 중요한 리스크 (우선순위 순)

1. **L6 S(교과개념어)의 AI 미태그 후보 자체가 "법" 단원(18~20주차) 32건뿐이다**
   — phase9가 보고한 "과학 304/사회 304"라는 폭넓은 인상과 달리, 실제로 사람이
   검토 가능한 S 후보는 법 영역으로 완전히 좁혀져 있다. 다른 교과(과학 등)의
   L6 S 콘텐츠를 확보하려면 결국 AI 태그 576건 재검수를 피할 수 없다.
2. **"선택과목 전문용어" 배제(19건 HOLD)는 이번 세션의 신규 판단이며 사람
   교과 전문가 검증을 거치지 않았다** — 어느 절차·제도명까지가 "일반 상식
   수준"이고 어디부터 "선택과목 심화"인지의 경계는 주관적일 수 있어, 다음
   세션/전문가가 이 19건 중 일부를 다시 살릴 수도 있다(5-4절 기준표 참고).
3. **577건(V1+S576)의 AI 자동생성 태그 재검수는 이번 세션 범위 밖으로 전혀
   들여다보지 않았다** — "review_status='검수완료'가 실제로는 AI 흔적"이라는
   phase9의 경고가 L6에서도 그대로 유효하며, 이 577건이 전체 L6 콘텐츠 후보의
   83%(577/693)를 차지하므로 향후 L6 확장의 핵심 병목이 될 것이다.
4. **"운동"/"공동체" 2건의 SENSE_REVIEW는 동형이의(체육/물리)와 개념 중복
   (하위 레벨과 핵심 의미 근접) 성격이 서로 다르다** — 하나의 규칙으로 묶어
   처리하면 안 되고, 콘텐츠화 시점에 개별로 사람이 판단해야 한다(4절).
5. **V "지"·"내"·"원"의 HOLD 사유(정의가 지나치게 기초적)는 원본 레벨 배정
   자체의 신뢰성에 의문을 던진다** — 이 3건 외에도 V/S 693건 전체에 비슷한
   레벨 오배정이 더 있을 수 있으나, 이번 세션은 비AI 후보 116건만 전수 검토
   했고 AI 태그 577건은 들여다보지 않았으므로 이 리스크의 전체 규모는
   확인하지 못했다.
6. **모든 학년 근거는 "정책 매핑"뿐이고 개별 학년 태그는 0건이다**(3-1절) —
   `docs/literacy/07-학년경계정책-L5L6.md`의 L6=고2~3 확정 기준 자체를 부정하는
   것은 아니지만, 개별 항목 단위로 "이 단어가 왜 고2~3인지"를 literacy.db
   내부 근거만으로 증명할 수는 없다는 구조적 한계가 L4/L5에 이어 L6에도
   그대로 있다.
