# 스키마리딩x어휘 DB 통합 — 9단계: L4~L6 vocabulary_quiz 현황·literacy.db V/S 분리·142건/73건 연결성 기준선 조사 (읽기 전용)

- 작성일: 2026-09-24 (`date` 명령으로 시스템 현재 날짜 확인)
- 범위: phase4(`schema_reading_phase4_literacy_link_apply_20260924.md`), phase6
  (`schema_reading_phase6_ai_level_full_audit_20260924.md`), phase7
  (`schema_reading_phase7_literacy_repr_error_remediation_20260924.md`)이 확립한
  사실(APP_ENV=research 대상 DB 경로, literacy.db 비분리, 142건 링크 구성, AI 레벨
  3,103건 4분류, GROUNDED=0 등)은 재조사하지 않고 그대로 인용·재사용했다.
- **이번 세션의 DB 접근은 전부 읽기 전용이다.** 로컬 `data/literacy.db`는
  `mode=ro`(`PRAGMA query_only=ON`)로만 열었다. 서버
  `vocabulary_quiz_research.db`도 SSH에서 동일하게 `mode=ro`+`PRAGMA query_only=ON`으로만
  열었다. 두 DB 모두 쓰기 시도(INSERT/UPDATE/DELETE/DDL)를 코드에 전혀 포함하지
  않았다. 레벨 재분류·학생 공개·git commit/push·서버 배포 전부 수행하지 않았다.
  **동시에 다른 세션이 literacy.db를 mode=ro로 읽고 있다는 사전 안내를 확인했고,
  이번 세션도 오직 mode=ro만 사용했으므로 락 충돌·이상 징후는 전혀 관찰되지
  않았다(다중 리더는 SQLite에서 안전).**
- **AI_ONLY 2,872건(및 POLICY_CONFLICT 62건)은 검증 완료로 계산하지 않았다** — 아래
  모든 집계에서 "미검증"으로 명시한다. 원본 레벨·단계 값은 전부 원본 그대로 보고하고,
  현재 공식 서비스 레벨(L0=초1~2···L6=고2~3)과의 자동 대응은 어디에서도 수행하지
  않았다 — 아래 "L4/L5/L6" 표기는 전부 **각 소스/모듈 자체의 원본 레벨 숫자(4/5/6)**를
  가리키며, 공식 L4~L6과의 관계는 별도로 "대응 안 됨/수동 재검수 필요"로 표시한다.

---

## 0. 핵심 요약 (먼저 읽는 사람용)

1. **vocabulary_quiz 모듈의 `vocab_level`(0~6, `level_policy_v0.1`) 체계에서
   vocab_level=5,6은 문자 그대로 0건**이다(`vocabulary_content_levels` 5,723행
   전체를 스캔해도 5,6은 단 한 행도 없다) — `play_page`가 "L5/L6은 데이터 준비 중"
   이라며 선택기를 비활성화하는 이유가 바로 이 사실과 정확히 일치한다(코드 주석과
   실측 데이터가 일치함을 이번 세션이 직접 확인). vocab_level=4는 콘텐츠 30건,
   출제 가능 문항은 all_candidates 기준 8건뿐(auto_only 기준 0건)이다.
2. **literacy.db의 "학습도구어(V)"/"스키마 개념어(S)"는 `source` 컬럼의
   `schemareading-tooldict`(V, 1,321건, `subject_category` 전량 NULL)와
   `schemareading-schema`(S, 2,061건, `subject_category` 전량 비NULL: 과학/사회/인문
   철학)로 **100% 깨끗하게 갈린다**(직접 SQL로 확인, 예외 0건). 뜻풀이 결측은 V/S
   전체 3,382건 중 **0건**이다.
3. **그러나 V/S 3,382건 중 사람이 실제로 검수완료 처리한 행은 0건이다** — 이번
   세션의 가장 중요한 새 발견이다(3-3절). `review_status='검수완료'`인 행
   (schemareading-schema 697건, schemareading-tooldict 13건, 합 710건)은
   **전부 예외 없이** `note`에 `[AI 자동 생성 뜻풀이]` 태그와 `reviewed_at='2026-09-01
   23:47:34'`가 찍혀 있다 — 즉 "검수완료"라는 라벨이 사람 검수가 아니라 AI 배치가
   지나가면서 자동으로 남긴 흔적이다. L4/L5는 이 배치가 아예 지나가지 않아
   100% `검수전`(정직하게 미검수 상태)인 반면, L6-schema는 94.7%(576/608)가 이
   "가짜 검수완료" 상태라 오히려 더 위험하다(3-4절, 4절 참고).
4. **기존 142건 CANDIDATE 링크는 L4~L6과 완전히 무관하다** — vocabulary_quiz의
   `vocab_level` 기준으로도(0건), literacy.db `terms.level` 기준으로도(0건, phase4
   재인용) 양쪽 다 0건이 확인됐다. 142건은 momo-textbook(138)+sajaseongeo-pdf(4)
   소스로만 구성돼 V/S(schemareading-schema/tooldict)와 원천적으로 겹치지 않는다.
5. **"학생용 뜻풀이·예문 초안 73건"에 해당하는 항목을 literacy.db 안에서 찾지
   못했다** — 저장소 전체(`reports/`, `docs/`, 스크립트) 어디에도 "73건"이라는
   숫자가 등장하지 않고, literacy.db의 여러 후보 집계(review_status·examples 소스별
   건수·quiz_items 건수 등)에도 정확히 73이 나오는 조건을 찾지 못했다(가장 가까운
   값은 momo-textbook 821건 중 examples가 전혀 없는 63건이었으나, 63≠73이라 이것을
   73으로 간주하지 않았다). 5절에 탐색 과정을 전부 기록한다 — **억지로 끼워맞추지
   않았다.**

---

## 1. 작업 1 — vocabulary_quiz_research.db L4/L5/L6 현황

### 1-1. 코드 확인 — 출제 가능 상태가 실제로 요구하는 조건

`app/vocabulary_quiz/routers/multiformat.py`를 직접 읽어 "출제 가능"이 정확히
무엇을 요구하는지 확인했다(추측 없이 코드 그대로):

- `_matching_level_content_ids()`(L192-200): `vocabulary_content_levels`에서
  `level_version='level_policy_v0.1'`, `is_active=1`, `vocab_level=선택레벨`,
  `level_status IN (confidence_mode에 따라 PROVISIONAL_AUTO만, 또는
  PROVISIONAL_AUTO+REVIEW_BOUNDARY)`를 만족하는 `content_id` 집합.
- `_select_level_candidates()`(L203-225): 단일 어휘형(`MEANING_CHOICE` 등)은
  `vocabulary_multiformat_items.source_content_id`가 위 집합에 속해야 하고,
  복합형(`MATCH_WORD_MEANING`)은 `source_content_ids_json`의 **전부**가 집합에
  속해야 한다(평균/대표 레벨을 만들지 않음, 코드 주석과 일치). `CROSSWORD`는
  레벨 모드에서 완전히 제외(선택 시 422).
- 두 조건 모두 `vocabulary_multiformat_items.source_version='2.1.29'`,
  `is_active=1`을 추가로 요구한다(전역 조건, `_select_level_candidates` 밖에서
  적용).
- **`student_exposure`/`public_ready` 컬럼은 이 라우터 어디에서도 참조되지
  않는다** — 애초에 `vocabulary_multiformat_items`/`vocabulary_content_levels`
  테이블 자체에 그 컬럼이 없다(모델 정의 확인, `app/vocabulary_quiz/models.py`
  L188-323). 이 모듈은 파일 최상단 docstring에 "관리자 전용, 학생 비공개,
  R&D 전용"이라고 명시돼 있고, `create_session`의 `metadata["audience"]`도
  `"ADMIN_ONLY"`로 하드코딩돼 있다(L461) — 즉 "출제 가능"은 곧 "관리자가
  `/vocabulary-quiz/multiformat/play`에서 시도할 수 있는 상태"를 뜻하며, 학생
  노출 여부와는 이 모듈 자체가 무관하다(phase7이 literacy.db 쪽에서 별도로 확인한
  "학생 노출 경로 없음"과 같은 결론이 vocabulary_quiz에도 구조적으로 적용된다).
- `play_page()`(L742-754)는 `_matching_level_content_ids(level, "all_candidates")`가
  빈 집합이면 그 레벨을 `level_disabled`에 넣어 화면에서 비활성화한다 —
  "현재는 L5/L6, 데이터가 채워지면 자동으로 활성화된다"는 주석이 있다.

### 1-2. 실측 — `vocabulary_content_levels`(레벨 후보) 분포

서버 `vocabulary_quiz_research.db`(mode=ro)에서 직접 집계했다(`vocab_level`은
0~6, `level_policy_v0.1` 단일 버전만 존재, 전체 5,723행 전수):

| vocab_level | grade_label(모듈 자체 상수) | PROVISIONAL_AUTO | REVIEW_BOUNDARY | 합계 |
|---:|---|---:|---:|---:|
| 0 | 초등 1~2학년 | 586 | 26 | 612 |
| 1 | 초등 3~4학년 | 1,285 | 350 | 1,635 |
| 2 | 초등 5~6학년 | 21 | 1,600 | 1,621 |
| 3 | 중등 1~2학년 | 1,682 | 143 | 1,825 |
| **4** | **중등 3학년** | **6** | **24** | **30** |
| **5** | **고등 1~2학년** | **0** | **0** | **0** |
| **6** | **고등 3학년** | **0** | **0** | **0** |
| 합계 | | | | 5,723 |

**중요한 경고**: 이 모듈이 자체적으로 쓰는 `GRADE_LABELS` 상수(코드 L105-108)는
`5:"고등 1~2학년", 6:"고등 3학년"`으로 라벨링돼 있다 — 이는 사용자가 이번 작업에서
확정한 공식 서비스 기준(L5=고1 단독, L6=고2~3)과 **경계가 다르다**(옛
LEVEL_TABLE "고1~2/고3" 구간 vs 새 기준 "고1/고2~3" 구간, phase6이 이미
POLICY_CONFLICT로 확정한 것과 동일한 경계 불일치 패턴). 이번 세션은 이 라벨을
그대로 원본으로만 보고하며, 공식 L5/L6과 자동으로 대응시키지 않았다 — 설령
vocab_level=5,6에 데이터가 있었더라도 이 라벨 불일치 때문에 기계적 재사용은
불가능했을 것이다(현재는 데이터 자체가 0건이라 이 문제가 가려져 있을 뿐).

### 1-3. 실측 — 출제 가능 문항 수 (라우터 로직 그대로 재현)

`_level_availability()`/`_select_level_candidates()` 로직을 그대로 재현하는
스크립트로 레벨×신뢰도 모드별 실제 출제 가능 문항 수를 집계했다(`CROSSWORD`는
레벨 모드에서 항상 제외).

| vocab_level | 모드 | 매칭 콘텐츠 수 | 출제 가능 문항 수 | 낱말(distinct) | 유형별(MEANING_CHOICE/WORD_FROM_DEFINITION/CONTEXT_MEANING/CONTEXT_CLOZE/MATCH_WORD_MEANING) |
|---:|---|---:|---:|---:|---|
| 0 | all_candidates | 612 | 121 | 32 | 32/32/32/25/0 |
| 0 | auto_only | 586 | 113 | 30 | 30/30/30/23/0 |
| 1 | all_candidates | 1,635 | 316 | 87 | 87/87/87/55/0 |
| 1 | auto_only | 1,285 | 244 | 67 | 67/67/67/43/0 |
| 2 | all_candidates | 1,621 | 332 | 88 | 88/88/88/68/0 |
| 2 | auto_only | 21 | 4 | 1 | 1/1/1/1/0 |
| 3 | all_candidates | 1,825 | 338 | 91 | 91/90/91/65/1 |
| 3 | auto_only | 1,682 | 316 | 85 | 85/84/85/62/0 |
| **4** | **all_candidates** | **30** | **8** | **2** | **2/2/2/2/0** |
| **4** | **auto_only** | **6** | **0** | **0** | **0/0/0/0/0** |
| **5** | **all_candidates / auto_only** | **0** | **0** | **0** | **0/0/0/0/0** |
| **6** | **all_candidates / auto_only** | **0** | **0** | **0** | **0/0/0/0/0** |

**결론**: vocab_level=5,6은 콘텐츠·문항·출제가능 전부 완전한 빈 구간이다.
vocab_level=4는 콘텐츠 30건이 있지만 실제 출제 가능한 문항은 8건(all_candidates,
신뢰도 낮은 REVIEW_BOUNDARY 후보 포함)뿐이고, `auto_only`(신뢰도 높은
PROVISIONAL_AUTO만)로 좁히면 0건이다 — 사실상 지금 당장 관리자가 레벨=4로
플레이할 수 있는 문항이 없다.

이 vocabulary_contents 5,723건은 `merge_source`가 전부 vocabulary_quiz 자체
파이프라인 배치 라벨(`QUALITY_PATCH_V2_0_*`, `REWRITE_BATCH_*`,
`AUTO_HOLD_RECOVERY_*`, `POLICY_REVIEW_*` 등)이며 literacy.db의
`schemareading-*` 소스명과 겹치지 않는다 — 3절에서 확인하듯 142건 링크 외에는
vocabulary_quiz 콘텐츠와 literacy.db term을 잇는 공식 연결이 없다(phase4
4-1절 재확인 사실).

---

## 2. 작업 2 — literacy.db V/S 분리 기준과 집계

### 2-1. V/S가 실제로 어떤 `source` 값에 대응하는지 — 직접 데이터로 확인

`terms.source`의 전체 분포(7,312건):

| source | category | 건수 |
|---|---|---:|
| krdict | 관용구 | 2,227 |
| krdict | 속담 | 657 |
| momo-textbook | 관용구 | 1 |
| momo-textbook | 어휘 | 820 |
| sajaseongeo-pdf | 사자성어 | 225 |
| **schemareading-schema** | 어휘 | **2,061** |
| **schemareading-tooldict** | 어휘 | **1,321** |

`schemareading-schema`/`schemareading-tooldict` 두 소스만 표본 추출해 직접
읽은 결과(각 12건 무작위 표본):

- `schemareading-tooldict` 표본: 크기·척도·듣다·표면·사정·불규칙·빨리·수용·모습·
  부정적·개설·경향 — **과목에 무관하게 쓰이는 범용 학습 도구 어휘**이고
  `subject_category`/`sense_category`가 전부 NULL이었다.
- `schemareading-schema` 표본: 재정정책·주민·원자번호·제거 소화법·바람·발광
  다이오드·기선·논농사·약염기·통장·예방접종·연주 운동 — **특정 교과 개념어**이고
  `subject_category`(과학/사회/인문 철학)·`sense_category`(경제/화학/지구과학 등
  14종)가 채워져 있었다.

전수로 재확인한 결과 **예외 0건으로 100% 깨끗하게 갈린다**:

```
schemareading-tooldict: 전체 1,321건 중 subject_category IS NULL 1,321건(100%), NOT NULL 0건
schemareading-schema:   전체 2,061건 중 subject_category IS NULL 0건, NOT NULL 2,061건(100%)
```

→ **분류 근거**: `source='schemareading-tooldict'`(어원상 "학습 도구 사전" —
과목 중립적 범용 어휘) = **V(학습도구어)**, `source='schemareading-schema'`
(과목별 배경지식 스키마 개념어, `subject_category`가 항상 채워짐) =
**S(스키마 개념어)**. 두 소스 이름 자체가 이 구분과 정확히 일치하고,
`subject_category` 유무라는 구조적 신호로도 100% 확증됐다 — 짐작이 아니라
데이터로 확인한 결과다. krdict(관용구/속담)·momo-textbook(교재 어휘)·
sajaseongeo-pdf(사자성어)는 V/S 어느 쪽도 아닌 별도 콘텐츠 유형이다(성격이
전혀 다름 — 관용구/속담/사자성어/교재 개별 단어).

### 2-2. 레벨(원본 그대로)·교과·주차·주제별 집계

`level` 컬럼(원본 그대로, 공식 L0~L6과 자동 대응 안 함) × V/S:

| level(원본) | V(tooldict) | S(schema) |
|---:|---:|---:|
| 1 | 366 | 261 |
| 2 | 597 | 235 |
| 3 | 80 | 227 |
| **4** | **85** | **223** |
| **5** | **108** | **507** |
| **6** | **85** | **608** |
| 합계 | 1,321 | 2,061 |

(level=0은 V/S 어느 쪽에도 없음 — schemareading 소스는 level 1부터 시작.)

**뜻풀이(definition) 결측**: V/S 3,382건 전체에서 **0건**(`definition IS NULL
OR TRIM(definition)=''` 조건으로 레벨별 전수 확인, 전부 0/총건수).

**교과(subject_category, S만 해당 — V는 과목 중립이라 값이 없음)**, level=4/5/6만:

| level | 과학 | 사회 | 인문 철학 |
|---:|---:|---:|---:|
| 4 | 102 | 121 | — |
| 5 | 263 | 216 | 28 |
| 6 | 304 | 304 | — |

**주차(`note`의 "주차: N주차" 패턴에서 정규식 추출, S만 100% 존재 — V는 이
패턴이 0건)**: S는 전 레벨에서 1주차~20주차까지 고르게 분포한다(예: level=6은
1주차 34건~16주차 49건 범위로 20개 주차 전부 존재). level=4/5/6 각각 40~44개
**소분류(주제, "소분류: X" 패턴)**가 있으며, 상위 빈도 주제는 "개인과 국가"(35),
"민법"(33), "정당과 선거제도"(27), "민주정치와 법치주의"(22), "반도체와
신소재"(21) 등이다(학교 교육과정 단원 구조로 보인다). V(tooldict)는 `note`에
주차/소분류 패턴이 **0건**(대신 `L1-NNN`/`L2-NNN` 형태 `external_id`와,
613/1,321건에 "나선형 반복: L5/L6" 같은 재출현 표시만 있음 — 교과/주차 개념이
아예 다른 방식으로 설계돼 있다).

전체 상세 행(3,382행, level/subject_category/sense_category/week/topic/
definition_missing 포함)은
`data/import/schema_reading_phase9_vs_terms_detail_20260924.csv`에 저장했다.

### 2-3. 새로 발견한 리스크 — "검수완료"가 실제로는 검수완료가 아니다

`review_status`를 `note`의 `[AI 자동 생성 뜻풀이]` 태그와 대조한 결과, **V/S
전체(3,382건)에서 `review_status='검수완료'`인 행(710건: schema 697+tooldict
13)이 전부 예외 없이 이 AI 태그를 갖고 있고, `reviewed_at`도 전부 동일 시각
`2026-09-01 23:47:34`였다**:

```
schemareading-schema:   level1 검수완료 121/121(=AI태그 121) | level6 검수완료 576/576(=AI태그 576) | 그 외 레벨 전부 검수전, AI태그 0
schemareading-tooldict: level1 검수완료 6/6(=AI태그 6) | level2 검수완료 5/5(=AI태그 5) | level6 검수완료 1/1(=AI태그 1) | 그 외 레벨 전부 검수전, AI태그 0
```

즉 **`review_status='검수완료'`는 사람이 검수를 마쳤다는 뜻이 아니라, 이
AI 자동 생성 뜻풀이 배치가 지나가면서 자동으로 남긴 흔적일 뿐이다.** phase4가
이미 이 배치(709건 = schema 697+tooldict 12)의 존재를 문서화했지만, 이번
세션에서 새로 확인한 것은 **이 배치가 `review_status` 컬럼까지 "검수완료"로
자동 전환시켰다는 점**이다(phase4는 142건과의 겹침 0건만 확인했지, 이 배치
자체의 review_status 오염 여부는 다루지 않았다). L4/L5는 이 배치가 지나가지
않아 100% `검수전`(정직한 미검수)이고, **L6-schema는 608건 중 576건(94.7%)이
이 "가짜 검수완료" 상태**다 — `review_status`만 보고 "L6-schema는 거의 다
검수됐다"고 판단하면 완전히 틀린 결론이 된다.

AI 태그가 붙은 L6-schema 정의 6건을 무작위 표본 확인했다(민족·민주정치의
이념·라니냐·실시간 중합효소연쇄반응·손해배상·건조단열감률) — 표면적으로는
전부 그럴듯한 문장이었으나, phase6이 이미 확립한 원칙(GROUNDED=0, DB 내부에
독립 검증 근거가 없음)이 여기도 동일하게 적용된다 — **이 6건을 포함한 576건
전부, "말이 되는 것처럼 보인다"는 것과 "국어 교과서/전공 수준에서 실제로
정확하다"는 것은 다른 문제이며, 이번 세션은 후자를 검증할 수단이 없었다**(사람
전문가 검수 또는 외부 자료 대조가 필요).

---

## 3. 작업 3 — 142건 링크·"73건"과 L4~L6 연결 가능성

### 3-1. 142건 링크의 L4~L6 겹침 — 양쪽 DB 모두에서 재확인

`vocabulary_content_literacy_links`(142행)의 `content_id`를
`vocabulary_content_levels`(level_version='level_policy_v0.1')와 조인해
vocab_level 분포를 직접 재조회했다:

| vocab_level | 142건 중 건수 |
|---:|---:|
| 0 | 9 |
| 1 | 61 |
| 2 | 36 |
| 3 | 36 |
| **4** | **0** |
| **5** | **0** |
| **6** | **0** |

(142개 content_id 전부 `vocabulary_content_levels`에서 매칭됨, 미매칭 0건.)

literacy.db `terms.level` 쪽은 phase4 2-3절이 이미 직접 재조회로 확인한 분포를
재인용한다: `{0:61, 1:48, 2:33}`, 합 142 — **레벨 3·4·5·6 어디에도 없음**(0건).

→ **결론: 142건은 vocabulary_quiz `vocab_level` 기준으로도, literacy.db
`terms.level` 기준으로도 L4~L6과 완전히 무관하다(양쪽 다 0건).** 142건이
momo-textbook(138)+sajaseongeo-pdf(4) 소스로만 구성돼 V/S
(schemareading-schema/tooldict)와 전혀 겹치지 않는다는 phase4의 기존 결론과도
정합적이다. 산출물:
`data/import/schema_reading_phase9_142links_l4l6_crosscheck_20260924.json`.

### 3-2. "학생용 뜻풀이·예문 초안 73건" — 탐색 결과: 못 찾음

지시대로 억지로 끼워맞추지 않고, 시도한 탐색과 결과를 전부 기록한다.

- 저장소 전체(`reports/`, `docs/`, 모든 스크립트)에서 정확한 문자열 `73건`을
  검색 → **0건 매칭**.
- literacy.db `terms.review_status` 분포: 검수완료 3,668 / 검수전 3,431 /
  보류 54 / 제외 159 — 73 없음.
- `quiz_items.review_status` 분포: 검수완료 229 / 제외 23(합 252) — 73 없음.
  `quiz_type`은 `뜻풀이선택` 단일값, `generated_by`는 `api` 단일값이라
  "학생용 뜻풀이 초안"을 가리킬 만한 별도 상태 구분이 없었다.
- `examples` 테이블(5,191행)을 `source` 컬럼별로 집계 → 22개 서로 다른
  `source` 값 어디에도 73이 없음(가장 가까운 건 `tooldict-sense-3` 227건,
  `krdict-sense-3` 27건 등, 73과 무관).
- `terms.note`에서 "학생용"/"초안"/"student"/"draft"/"예문" 키워드 검색 →
  전부 0건 매칭(`docs/literacy/*.md`에는 "학생용"/"초안"이라는 단어 자체는
  나오지만, 특정 73건 배치를 가리키는 문맥이 아니라 일반 설명문이었다 — 예:
  "학생용 라우터", "AI 초안으로 채워져서" 등).
- momo-textbook(821건) 중 `examples` 테이블에 연결된 행이 전혀 없는 건수를
  세어봤다 → **63건**(758건은 krdict 정의를 예문으로 대체 보유). 73과
  가장 가까운 값이었지만 **63≠73**이므로 이것을 73건으로 간주하지 않았다.
- vocabulary_quiz_research.db의 `REWRITE_BATCH_*` 계열 merge_source 19개
  배치 건수(47/50×10/39/40/44/40/37/29/34/27/9)에서도 73과 일치하거나 합쳐서
  정확히 73이 되는 자연스러운 조합을 찾지 못했다.

**결론: "73건"에 해당하는 걸 못 찾았다.** 사용자가 이전에 다른 대화/문서에서
언급했을 가능성이 있으나 이번 세션이 접근 가능한 범위(저장소 파일 전체 +
literacy.db + vocabulary_quiz_research.db)에서는 근거를 확인할 수 없었다 —
73이라는 숫자가 필요하다면 그 출처(어느 보고서/대화/스프레드시트인지)를
사용자에게 확인받는 것을 제안한다.

---

## 4. 작업 4 — 레벨(원본 4/5/6) × 구분(V/S) 3분류

**주의**: 아래 "L4/L5/L6"은 각 시스템의 원본 레벨 숫자를 병기한 것이지, 공식
서비스 레벨과 자동으로 같다는 뜻이 아니다(1-2절 경고 참고 — 특히 vocab_level의
5/6 라벨은 공식 L5/L6과 경계 자체가 다르다).

### 빈 구간 (콘텐츠/문항이 거의 없거나 없음)

| 구분 | 내용 |
|---|---|
| vocabulary_quiz vocab_level=5,6 | 콘텐츠 0건, 문항 0건 — 완전한 빈 구간(1-2·1-3절) |
| vocabulary_quiz vocab_level=4 (auto_only 기준) | 콘텐츠 30건은 있으나 신뢰도 높은 후보로는 출제 가능 문항 0건 |
| literacy.db V/S level=0 | V/S 소스 자체에 level=0 행이 아예 없음(1부터 시작) |

### 바로 작업 가능한 후보 (뜻풀이 있고 레벨 근거가 비교적 명확한 것)

| 구분 | 내용 | 근거 |
|---|---|---|
| literacy.db S(schema) level=4 | 223건, 뜻풀이 100% 존재, `subject_category`(과학/사회) 채워짐, 20주차 커리큘럼 구조 명확, **AI 자동생성 태그 0건**(정의가 AI로 지어진 게 아님이 데이터로 확인됨) | 다만 `review_status`는 100% 검수전 — "내용이 원본"이라는 것과 "최종 검수를 마쳤다"는 것은 별개 |
| literacy.db S(schema) level=5 | 507건, 위와 동일(뜻풀이 100%, 주제/주차 구조 명확, AI 태그 0건) | 위와 동일 |
| literacy.db V(tooldict) level=4,5 | 85건, 108건, 뜻풀이 100%, AI 태그 0건 | 과목 중립 범용 도구어라 S보다 링크 대상 폭이 넓을 수 있음 |
| literacy.db S(schema) level=6, `검수전` 32건만 | AI 배치가 지나가지 않은 "진짜" 미검수 원본 32건 — 나머지 576건과 분리해서 우선 검토 가능 | 3-3절에서 밝힌 대로 이 32건만 AI 오염과 무관 |

이 4개 그룹의 공통 전제: **level 값 자체는 `grade_source='manual'`로 커리큘럼
설계자가 수동 배정한 값**(AI 레벨 판정 3,103건의 GROUNDED/AI_ONLY 프레임과는
별개 계통)이라 krdict/사자성어처럼 "AI가 레벨을 추측했다"는 위험은 없다. 다만
"검수전" 상태가 뜻하는 바는 "정식 QA를 아직 거치지 않았다"는 것이므로, 학생
노출 전 최소 표본 검수는 필요하다.

### 보류 후보 (의미·학년 근거가 부족한 것)

| 구분 | 내용 | 이유 |
|---|---|---|
| literacy.db S(schema) level=6, `검수완료` 576건 | 94.7%가 AI 자동 생성 뜻풀이이며 `review_status`만으로는 이 사실이 드러나지 않음 — **가장 위험한 그룹**(가짜 신뢰 신호) | 3-3절 |
| literacy.db V(tooldict) level=6, 84건(`검수전`) + 1건(AI `검수완료`) | 대부분 미검수, 표본 극소(85건) | |
| AI 레벨 판정 krdict/sajaseongeo-pdf(POLICY_CONFLICT 62건, AI_ONLY 2,872건) | phase6 재확인: 이 3,103건은 애초에 V/S(schemareading)와 표제어 교집합 0건, L4~L6 어느 쪽과도 구조적으로 무관 — 별도 트랙으로 남겨둠 | phase6 재인용 |
| 콘텐츠 정책 10건(성차별/구시대 가치관) | phase6·phase7이 이미 정리 — 원천 기록 보존 확인, 상태 변경 없음, 사용자 정책 결정 대기 | 이번 세션은 상태를 다시 조회만 하고 변경하지 않았다(review_status 그대로: EXCLUDED_NEEDS_REVIEW 159건 내 유지) |
| 142건 CANDIDATE 링크 | L4~L6과 무관(3-1절) — 이 작업 범위에서는 참고 자료 아님 |

### 다음 작성 배치 우선순위 제안 (제안만, 실행 안 함)

1. **literacy.db S(schema) level=4·5(총 730건, AI 태그 0건)의 표본 검수부터
   시작** — 뜻풀이가 원본이고 커리큘럼 구조(주차/소분류)가 이미 정리돼 있어
   "바로 작업 가능" 조건에 가장 잘 맞는다. 표본을 사람이 검수해 review_status를
   정식으로 전환하는 절차부터 설계할 것을 제안한다.
2. **S(schema) level=6의 "진짜 검수전" 32건**을 먼저 사람이 보고, 그 다음에야
   576건(AI 생성) 처리 방침(전수 재검수 vs 표본 검수 후 승인)을 결정할 것을
   제안한다 — 576건을 "이미 검수됐다"고 오인해 그대로 쓰지 않도록 review_status
   컬럼에만 의존하지 말고 `note`의 AI 태그를 항상 같이 확인해야 한다.
3. **vocabulary_quiz vocab_level=4~6에 문항을 채우려면**, literacy.db
   S/V level=4~6 콘텐츠를 vocabulary_quiz `vocabulary_contents`로 새로 옮기고
   `vocabulary_content_levels`에 `vocab_level=4/5/6` 행을 만드는 별도
   마이그레이션이 필요하다(현재 두 DB 사이에 이 레벨대의 공식 연결이 전혀 없음,
   142건도 무관함을 3-1절에서 확인). 다만 vocab_level의 5/6 라벨이 공식 L5/L6과
   경계가 다르다는 1-2절 경고를 반드시 먼저 해소(라벨 재정의 또는 신규
   level_version 도입)한 뒤 진행해야 한다 — 그렇지 않으면 phase6이 지적한
   POLICY_CONFLICT와 같은 유형의 혼선이 vocabulary_quiz 쪽에도 새로 생긴다.
4. "73건" 정체를 사용자에게 직접 확인 — 이번 세션은 근거를 못 찾았다(3-2절).

---

## 5. 산출물

- 본 보고서: `reports/schema_reading_phase9_l4_l6_baseline_20260924.md`
- `data/import/schema_reading_phase9_vq_level_availability_20260924.json` —
  vocabulary_quiz vocab_level(0~6)×confidence_mode(all_candidates/auto_only)별
  매칭 콘텐츠 수·출제 가능 문항 수·유형별 세부(라우터 로직 재현, 서버 mode=ro)
- `data/import/schema_reading_phase9_142links_l4l6_crosscheck_20260924.json` —
  142건 링크의 vocab_level/literacy terms.level 교차 확인
- `data/import/schema_reading_phase9_vs_terms_detail_20260924.csv` — V/S
  3,382건 전체 상세 행(level/subject_category/sense_category/week/topic/
  definition_missing/review_status)
- `data/import/schema_reading_phase9_vs_summary_20260924.json` — 위 CSV의 집계
  요약(레벨×V/S, 레벨×교과, 뜻풀이 결측, 주차/주제 distinct 개수)
- 재현에 쓴 스크립트(로컬 임시 스크래치패드, 저장소에는 남기지 않음 — 필요시
  본문에 기술된 쿼리 그대로 재현 가능): `vq_level_availability.py`(서버 실행,
  읽기 전용), `literacy_vs_breakdown.py`(로컬 실행, 읽기 전용)

## 6. 가장 중요한 리스크 (우선순위 순)

1. **literacy.db S(schema) level=6의 `review_status='검수완료'` 576건(전체의
   94.7%)이 실제로는 사람이 아니라 AI 배치가 자동으로 붙인 라벨이다** — 이번
   세션의 가장 중요한 신규 발견. `review_status` 컬럼만 믿고 "검수 끝난 콘텐츠"로
   분류하면 안 된다. V/S 전체(3,382건)를 통틀어 사람이 실제로 검수완료 처리한
   행은 0건이다.
2. **vocabulary_quiz `vocab_level`의 5·6 라벨("고1~2"/"고3")이 사용자가 확정한
   공식 L5/L6("고1"/"고2~3") 경계와 다르다** — 지금은 vocab_level=5,6에 데이터가
   0건이라 문제가 드러나지 않지만, 향후 이 레벨대를 채우는 순간 phase6의
   POLICY_CONFLICT와 동일한 유형의 혼선이 재발할 구조적 소지가 있다.
3. **vocabulary_quiz vocab_level=4는 사실상 비어 있다**(콘텐츠 30건, 신뢰도 높은
   출제 가능 문항 0건) — "L4는 어느 정도 있다"고 오인하기 쉬우나 실측하면 L5/L6과
   본질적으로 같은 "빈 구간"에 가깝다.
4. **142건 링크와 literacy.db S/V(schemareading) 사이에는 아무 연결이 없다** —
   앞으로 L4~L6 콘텐츠를 vocabulary_quiz에 채우려면 142건 마이그레이션 패턴을
   그대로 재사용할 수 없고 별도의 신규 매핑 작업이 필요하다.
5. **"73건"의 정체가 불명확한 채로 남아 있다** — 다음 지시를 내리기 전에 그 숫자의
   출처를 사용자에게 확인받아야 잘못된 전제로 작업이 진행되는 것을 막을 수 있다.
