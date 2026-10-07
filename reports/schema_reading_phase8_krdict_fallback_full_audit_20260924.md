# 스키마리딩x어휘 DB 통합 — 8단계: momo-textbook 821건 krdict `definitions[0]`
# 고정 채택 전수 재감사 + 재발 방지 코드/테스트 + 콘텐츠정책 10건·언어사실 2건
# 재확인 (읽기 전용)

- 작성일: 2026-09-24 (`date` 명령으로 시스템 현재 날짜 확인)
- 범위: phase6(`schema_reading_phase6_ai_level_full_audit_20260924.md`)과
  phase7(`schema_reading_phase7_literacy_repr_error_remediation_20260924.md`)이
  확립한 사실(`유용하다` id 정정=2891, krdict 동음이의 오매칭 4건, 26건 판정표
  REPLACE_CANDIDATE 6/HOLD 1/NO_ISSUE 19, `/literacy/terms`만 노출 경로, 성차별
  10건·서양속담 2건 분리)은 재조사하지 않고 그대로 인용·재사용했다. 이번 세션은
  기존 26건이 "momo-textbook 56개 다중레벨 중복 그룹" 안에서만 찾은 것이었다는
  점을 명시적으로 인지하고, **momo-textbook 821건 전체**(중복 그룹 여부 무관)를
  대상으로 krdict `definitions[0]` 고정 채택 패턴을 다시 찾았다.
- **이번 세션의 DB 접근은 전부 읽기 전용이다.** `data/literacy.db`는 모든 연결에서
  `mode=ro` URI 또는 `PRAGMA query_only=ON`만 썼다. `momo_book.db`도 `mode=ro`만
  썼다. definition/level/vocabulary_content_literacy_links 컬럼에 어떤 UPDATE도
  실행하지 않았다(세션 종료 시점에 `data/literacy.db` 파일 mtime이 phase7이 마지막
  UPDATE를 실행한 시각(`2026-09-24 08:37:40`)과 정확히 일치함을 확인해 이번 세션이
  파일을 전혀 건드리지 않았음을 재확인했다 - `PRAGMA integrity_check`도 `ok`).
- git commit/push 없음, 운영 배포 없음.

---

## 작업 1 — momo-textbook 821건 전체 재검사

### 1-1. 방법

`import_textbook_vocab.py`의 실제 적재 로직(L225, `definition = rep.definition or
entry.definitions[0]`)을 그대로 재현하는 신규 스크립트
`scripts/literacy/scan_momo_textbook_krdict_fallback_821_full.py`를 작성했다(읽기
전용, 새 방법을 발명한 것이 아니라 기존 스크립트들의 재현 방식을 그대로 따름):

1. `literacy.db`에서 `source='momo-textbook'` 821건 전체(`terms.id/headword/
   definition/external_id/note/review_status`)를 가져온다.
2. 각 행의 `external_id`로 `momo_book.db`의 원본 `vocabulary.definition`을 찾아
   원본이 falsy(NULL/빈 문자열)였는지 확인한다.
3. 원본이 falsy였고 krdict 매칭(`pick_krdict_match`, 기존 함수 그대로 재사용)이
   있었던 행 = **krdict `definitions[0]` 고정 채택 후보(fallback)**.
4. fallback 행 중 "위험 신호"(동음이의 `homonym_count>1` 또는 매칭된 entry 자체가
   다의어 `len(entry.definitions)>1`)가 있는 행만 추가 검증 대상으로 삼는다(위험
   신호가 없으면 krdict 자체에 뜻이 하나뿐이라 "잘못된 인덱스를 고를 가능성"이
   구조적으로 없다).
5. 위험 신호가 있는 행은 `momo_book.db` 전체 939행(56개 다중레벨 그룹 제한을
   풀고, 단일 레벨 중복 포함 전체)에서 **같은 정제 표제어의 다른 행**을 찾아,
   그 행에 정의가 있으면 krdict 전체 entry(모든 동음이의)·전체 sense와 문자
   2-gram 유사도로 대조한다. 일치하는 sense가 index 0이면 NO_ISSUE, 0이 아니면
   REPLACE_CANDIDATE, 대조할 다른 행 자체가 없으면(정의 있는 중복이 momo_book.db에
   아예 없음) **HOLD**로 분류한다 — 지시대로 독립 근거가 없으면 자동으로
   교체 대상으로 올리지 않았다.

### 1-2. 전수 분류 결과

| 구분 | 건수 |
|---|---:|
| momo-textbook 총 행수 | **821** |
| krdict `definitions[0]` fallback 후보 | **317** |
| fallback 아님(원본 정의 있음 또는 krdict 매칭 없음) | 504 |
| fallback 중 literacy.db 현재값 != entry.definitions[0] | **0**(수입 코드 재현이 정확함을 확인) |
| fallback 중 위험 신호(동음이의 또는 다의어) 있음 | **123** |
| fallback 중 위험 신호 없음(krdict 뜻 1개뿐 - 구조적으로 안전) | 194 |

### 1-3. 기존 26건(6 REPLACE_CANDIDATE / 1 HOLD / 19 NO_ISSUE) 재현 확인

기존 26건 중 krdict `definitions[0]` fallback 패턴과 직접 관련된 것은
**NULL_DEF 유형 21건**뿐이다(SHORT_DEF 3건·PUNCT_DAMAGE 2건은 momo_book.db 원본
정의 자체가 존재했으나 짧거나 구두점이 손상된 별개 버그이지 krdict fallback이
아니다 - 1-5절에서 별도로 재확인).

NULL_DEF 21건 전부가 이번 821 전수 스캔의 fallback pool(317건) 안에 있음을
확인했다(재현 성공, 누락 0건). 그중 12건은 위험 신호가 있어 추가 판정 로직을
탔고, 9건(기리다·비아냥거리다·문명·으름장·안달하다·미심쩍다·애지중지·거드름·
으스대다)은 krdict 뜻이 하나뿐이라 위험 신호 없이 구조적으로 안전한 것으로
재확인됐다(=NO_ISSUE와 결과적으로 동일).

| 기존 판정 | 건수 | 새 방법(821 전수 스캔)으로 자동 재현 | 비고 |
|---|---:|---:|---|
| REPLACE_CANDIDATE | 4 | **3/4** 자동 재현(모락모락·선구자·유용하다) | 관대하다는 아래 참고 |
| NO_ISSUE | 17 | **10/17** 자동 재현(9건 위험신호 없음 + 1건 문자열 일치) | 나머지 7건은 아래 참고 |

**관대하다(id=3168)는 자동 유사도 계산으로는 재현되지 않았다** — momo_book.db의
다른 후보 정의(`마음이 너그럽고 크다.`)와 krdict 형용사 sense(`마음이 넓고
이해심이 많다.`)가 **의미는 같지만 문자열이 많이 달라** 2-gram 유사도가 임계값
아래로 떨어졌다(단순 문자 중첩 휴리스틱의 한계 - 의미 대조가 아니라 표층 문자
중첩만 본다). 다만 phase7이 이미 이 건을 `literacy.db`의 `note` 컬럼 자체(`krdict
동음이의 2건 중 1번 채택`)와 krdict raw XML 재조회로 독립 검증했으므로(강한 근거),
이번 세션은 **phase7의 기존 판정(REPLACE_CANDIDATE)을 재조사 없이 그대로 유지**
했다 — 자동 유사도 휴리스틱이 놓쳤다고 해서 이미 확립된 사실을 뒤집지 않았다.

나머지 7건(으레·읊다·바래다·모질다·감싸다·고즈넉하다·**기색**)도 자동 유사도
계산으로는 재현되지 않았다(같은 이유 - 표층 문자 중첩 부족). 이 중 6건(관대하다
제외 나머지)은 기존 build_repr_errors_26_verdict_table.py의 사람 대조 근거가
이미 확실해(동일/거의 동일한 개념) 기존 NO_ISSUE 판정을 그대로 유지했다.

**단 하나 주의가 필요한 예외 — 기색(id=3278)**: 자동 유사도 계산이 momo_book.db
다른 후보 정의(`마음의 작용으로 얼굴에 드러나는 빛. 어떠한 행동이나 현상 따위가
일어나는 것을 짐작할 수 있게 하여 주는 눈치나 낌새.`)를 krdict 0번 뜻보다 **1번
뜻**(`어떤 행동이나 현상 등이 일어나는 것을 짐작할 수 있게 해 주는 눈치나
분위기.`)에 더 가깝다고 계산했다(유사도 0.36, 0번 뜻과는 이보다 낮음). 그런데 이
momo_book.db 텍스트는 실제로는 "얼굴에 드러나는 빛"(0번 뜻과 유사)과 "눈치나
낌새"(1번 뜻과 유사) **두 표현을 한 문장에 모두 담고 있어** 어느 한쪽으로
단정하기 어렵다 — phase7의 기존 판정(NO_ISSUE, "같은 개념")도 틀렸다고 확정할
근거가 없고, 자동 계산도 확정적 증거가 아니다. **지시대로 근거 불충분 상태이므로
이번 세션은 기색을 REPLACE_CANDIDATE로 승격하지 않고 기존 NO_ISSUE 판정을
유지하되, "재검토 여지가 있는 경계 사례"로 별도 플래그만 남긴다**(dry-run 표에는
포함하지 않음 - 작업 2 참고).

SHORT_DEF/PUNCT_DAMAGE 5건(궁색하다=HOLD, 허사·자초지종=NO_ISSUE, 혼비백산·
독불장군=REPLACE_CANDIDATE)은 krdict fallback pool에 애초에 속하지 않는다(원본
momo_book.db 정의가 NULL이 아니라 짧거나 구두점이 손상됐을 뿐 - momo_book.db
자체 텍스트 문제). 이번 세션은 이 5건의 `literacy.db` 현재값이 phase7 보고서와
**완전히 동일함**을 직접 SELECT로 재확인했다(값 변경 없음, 재현 성공).

### 1-4. 새로 발견되는 오류 건수 — **0건 확정, 111건 신규 HOLD**

317건 fallback 중 위험 신호가 있는 123건에서 기존 26건에 속하는 12건을 뺀
**신규 111건**을 같은 방법(momo_book.db 내 다른 정의 있는 행과 대조)으로 전부
검증했다. 결과:

| 신규 111건 세부 | 건수 |
|---|---:|
| momo_book.db에 이 표제어가 이 행 하나뿐(대조 대상 자체가 없음) | 106 |
| momo_book.db에 중복은 있으나 전부 정의 NULL(대조 대상 없음) | 5 |
| momo_book.db에 정의 있는 다른 후보가 있어 실제 대조 가능 | **0** |

즉 **111건 전부 momo-textbook 내부에서 독립적으로 대조할 근거 자체가 없다**
(기존 26건이 발견될 수 있었던 것은 momo_book.db 939행 중 해당 표제어가 우연히
여러 레벨에 중복 등장했기 때문인데, 그 조건을 충족하는 행이 신규 111건 중
하나도 없었다). 지시("근거 부족하면 HOLD, 자동으로 수정 대상 제시 금지")에 따라
**111건 전부 HOLD로 분류했다** — REPLACE_CANDIDATE로 새로 확정한 건은 **0건**
이다.

나머지 fallback 185건(123-111이 아니라 317-123=194에서 이미 확인된 9건을 뺀
185건)은 krdict 뜻이 하나뿐인 구조적 안전 케이스라 애초에 "잘못된 인덱스" 버그가
성립할 수 없다.

### 1-5. momo-textbook 외 다른 원천의 영향 범위

저장소 전체에서 `definitions[0]` 패턴(및 동일 구조의 `.definitions[0]`)을
검색한 결과, momo-textbook 외에 **4곳**이 같은 "krdict/사자성어 sense 목록에서
인덱스 0을 무조건 채택" 로직을 쓴다:

| 파일:줄 | 소스 | 코드 | 영향 범위 실측 |
|---|---|---|---|
| `scripts/literacy/collect_proverbs.py:80` | `krdict`(속담·관용구, 2,884건) | `first_definition = entry.definitions[0] if entry.definitions else ""` | krdict 속담/관용구 XML 전체에서 다의어(sense>1)인 entry가 **281/2,884건(9.7%)** — 이번 세션이 직접 카운트, momo-textbook과 별개의 실측 위험 규모 |
| `scripts/literacy/import_sajaseongeo.py:85` | `sajaseongeo-pdf`(사자성어, 225건) | `definition = e.definitions[0] if e.definitions else None` | 병합된 사자성어 225건 중 다의어(sense>1) **3건(강구연월·무위자연·비일비재)** — 이번 세션이 직접 카운트 |
| `scripts/literacy/import_schemareading_vocab.py:175` | `schemareading-schema`(tooldict, 615건) | `definition = rep.definitions[0] if rep.definitions else None` | 코드 패턴은 동일 확인. 원본 raw 데이터가 krdict XML이 아니라 별도 tooldict 포맷이라 이번 세션에서 다의어 건수를 실측하지 않았다(범위 밖 - 위치만 특정) |
| `scripts/literacy/apply_sajaseongeo_datefix_10.py:106` | (phase7이 이미 적용·완료한 10건 전용 스크립트) | `new_defs[tid] = e.definitions[0]` | 이미 실행 완료된 1회성 스크립트(사자성어 날짜 정규식 재계산 전용) - 재사용 위험은 낮지만 같은 패턴이라 기록만 |

`import_textbook_vocab.py:225`(이번 세션이 수정한 곳)까지 포함하면 이 저장소
안에서 "krdict/사전형 sense 리스트에서 인덱스 0을 무조건 채택"하는 지점은
**총 5곳**이다. momo-textbook 외 소스는 이번 세션 범위(momo-textbook 821건) 밖
이라 전수 재검증하지 않았으나, krdict 속담/관용구 281건·사자성어 3건은 **같은
버그가 발생할 수 있는 구조**임을 실측으로 확인했다(수정은 하지 않음 - 다음 단계
제안 사안).

---

## 작업 2 — 6건(+신규 확정분) dry-run 표

작업 1 결과, **신규로 확정된 REPLACE_CANDIDATE는 0건**이라 dry-run 대상은 기존
6건 그대로다(궁색하다 HOLD 유지, 나머지 19건 NO_ISSUE 제외, 기색은 근거 불충분
으로 HOLD 유지 - 위 1-3절).

파일: `reports/literacy_krdict_fallback_dryrun_6_20260924.csv`,
`reports/literacy_krdict_fallback_dryrun_6_20260924.md` (이번 세션 신규 생성,
`reports/literacy_repr_errors_26_verdict_20260924.csv`에서 REPLACE_CANDIDATE
6건만 추출해 재구성 - 근거 텍스트는 기존 판정표를 그대로 가져왔다, 재조사 없음).

| id | headword | 수정 전 | 수정 후(제안, 미적용) |
|---:|---|---|---|
| 2891 | 유용하다 | 남의 것이나 이미 용도가 정해져 있는 것을 다른 데에 쓰다. | 쓸모가 있다. |
| 3168 | 관대하다 | 친절하고 정성스럽게 대하다. | 마음이 넓고 이해심이 많다. |
| 3085 | 모락모락 | 작은 것이 순조롭게 잘 자라는 모양. | 연기나 냄새, 김 따위가 계속 조금씩 피어오르는 모양. |
| 3180 | 선구자 | 행렬에서 맨 앞에 가는 사람. | 사회적으로 중요한 일이나 사상에서 다른 사람보다 앞선 사람. |
| 3015 | 혼비백산 | 몹시 놀라 넋을 잃음을 이르는. 말 | 혼백이 어지러이 흩어진다는 뜻으로, 몹시 놀라 넋을 잃음을 이르는 말. |
| 2924 | 독불장군 | 무슨 일이든 자기 생각대로 혼자서 처리하는. 사람 | 무슨 일이든 자기 생각대로 혼자서 처리하는 사람. |

실제 `data/literacy.db`는 건드리지 않았다(dry-run 표 생성 과정도 파일 쓰기만
했을 뿐 DB 연결 자체가 없었다).

---

## 작업 3 — 재발 방지 코드 수정안 + 테스트

### 3-1. 수정 코드

`scripts/literacy/import_textbook_vocab.py`에 `resolve_definition()` 함수를
신규 추가하고, `run()`의 적재 루프(구 L220-226)가 이 함수를 쓰도록 교체했다
(momo-textbook 소스에 한정 - 3-3절 참고).

설계 원칙(지시받은 그대로):
- 원본(momo_book.db) 정의가 있으면 그대로 쓴다(변경 없음).
- krdict 매칭이 아예 없으면 기존과 동일(변경 없음).
- **동음이의(krdict에 같은 표제어의 LexicalEntry가 2개 이상)면 무조건 보류**
  (`definition=None`, `review_status='보류'`) — 매칭된 entry 자체의 sense가
  1개뿐이어도 보류한다(어느 동음이의가 맞는지는 원본 정의 없이 판단 불가).
- **매칭된 단일 entry가 다의어(sense 2개 이상)면 보류** — 어느 뜻인지 판단 불가.
- 둘 다 아니면(krdict 뜻이 애초에 하나뿐) 그 뜻을 채택한다(구조적으로 안전한
  경우만 자동 채움).
- 보류된 행은 `note`에 사유를 남기고(예: `krdict 동음이의 2건 - 어느 동음이의
  항목의 뜻인지... 자동 선택 보류`), `unmatched.csv`에도
  `krdict동음이의보류`/`krdict다의어보류` 사유로 기록해 검수자가 놓치지 않게
  했다. 보류 시에는 `examples`에 krdict 후보 뜻도 넣지 않는다(특정 sense를
  슬쩍 끼워 넣으면 "이미 확인된 뜻"으로 오인될 위험이 있어서).

**이번 세션은 실제 `literacy.db`에 이 수정을 적용하지 않았다** — 코드만
수정했고, `import_textbook_vocab.py`를 재실행(재적재)하지 않았다.

### 3-2. 테스트

`tests/test_krdict_fallback_hold_fix.py`(신규, pytest 없이 저장소 관례대로
`[PASS]/[FAIL]` 출력 - `tests/test_sajaseongeo_date_lead_fix.py` 스타일을 그대로
따름). 두 절로 구성:

1. **합성(fabricated) Entry로 경계 조건 검증**(8개 체크): 원본 정의 우선,
   krdict 매칭 없음(회귀 없음), 동음이의만 있어도 보류, 다의어만 있어도 보류,
   둘 다 없으면 정상 채택(회귀 없음), `definitions`가 빈 리스트인 방어적 케이스.
2. **실제 krdict 원본 덤프로 재현**(21개 체크): 기존 버그 사례
   유용하다·관대하다·모락모락·선구자가 실제로 보류되는지, 우연히 맞았던
   바래다(동음이의 3건, 기존 NO_ISSUE)도 새 설계상 안전 우선으로 여전히
   보류되는지, 문제 없던 기리다는 회귀 없이 정상적으로 값이 채워지는지.

**실행 결과: 29/29 통과** (`python tests/test_krdict_fallback_hold_fix.py`).
기존 `tests/test_sajaseongeo_date_lead_fix.py`도 재실행해 **34/34 통과**(회귀
없음)를 재확인했다 - 이번 세션이 건드리지 않은 파일이라 당연한 결과지만 명시적
으로 재확인했다.

### 3-3. 수정 범위에 대한 스코프 결정

이번 세션 지시("`definitions[0]`을 무조건 선택하는 코드 경로를 찾아 수정안을
작성")를 momo-textbook 소스(`import_textbook_vocab.py`)에만 적용했다 - 이 세션
전체가 momo-textbook 821건을 대상으로 하고, 실제로 확인된 4건의 오매칭도 전부
이 경로에서 나왔기 때문이다. 1-5절에서 찾은 나머지 3곳
(`collect_proverbs.py`/`import_sajaseongeo.py`/`import_schemareading_vocab.py`)
은 **같은 패턴이지만 이번 세션에서 수정하지 않았다** — 다른 소스는 데이터 구조
(사자성어는 `merge_sources()`가 만든 별도 `Entry`형, tooldict는 완전히 다른
원본 포맷)가 달라 `resolve_definition()`을 그대로 재사용할 수 없고, 각 소스별로
"보류 조건"을 독립적으로 설계·검증해야 해서 이번 세션 범위를 벗어난다고 판단
했다. 다음 단계 제안 사항으로 남긴다.

---

## 작업 4 — 성차별/구시대 가치관 10건 + 언어사실 검증 2건 상태 재확인

### 4-1. 성차별/구시대 가치관 10건 — 삭제 아님, 제외 상태 유지 확인

`data/literacy.db`(mode=ro)에서 phase6이 특정한 10개 id를 직접 SELECT로
재조회했다. **10건 전부 행이 그대로 존재**하며(삭제되지 않음), 전부
`review_status='제외'`, `level=NULL`, `reviewed_at='2026-09-01 23:46:45'`로
동일했다(phase6/phase7 이후 값 변경 없음). 예:

| id | headword | review_status | level | note(요약) |
|---:|---|---|---|---|
| 18 | 치마 밑에 키운 자식 | 제외 | NULL | 시대착오적/편견 유발 - 교육적으로 부적절 |
| 110 | 팔자(를) 고치다 | 제외 | NULL | 성차별적/구시대적 정의 |
| 116 | 평생을 맡기다 | 제외 | NULL | 성차별적/시대착오적 정의 |
| 679 | 귀머거리 삼 년이요 벙어리 삼 년(이라) | 제외 | NULL | 장애인 비하 + 구시대 시집살이 관습 |
| 887 | 남편 복 없는 여자는[년은] 자식 복도 없다 | 제외 | NULL | 비속어 + 성차별적 요소 |
| 2179 | 솥뚜껑 운전수 | 제외 | NULL | 가정주부 비하/성차별적 속어 |
| 2321 | 암탉이 운다 | 제외 | NULL | 성차별적 의미 |
| 2322 | 암탉이 울면 집안이 망한다 | 제외 | NULL | 성차별적 관념 |
| 2437 | 여자 셋이 모이면 접시가 깨진다 | 제외 | NULL | 성차별적 편견 |
| 2438 | 여자가 한을 품으면 오뉴월에도 서리가 내린다 | 제외 | NULL | 성차별적 고정관념 |

**결론**: 10건은 원천에서 삭제되지 않았고, "학생용 후보 제외" 상태(`review_status=
'제외'`)를 그대로 유지 중이다(phase7이 확인한 대로 이 필드가 `/literacy/terms`
화면의 필터로 쓰이진 않지만 - 4-3절 - 데이터베이스 상 "제외 표시" 자체는 유지).

### 4-2. 언어사실 검증 필요 2건 — 독립 목록(콘텐츠 정책 10건과 절대 혼합 안 함)

| id | headword | review_status | note(요약) |
|---:|---|---|---|
| 2139 | 손이 차가운 사람은 심장이 뜨겁다 | 제외 | 서양 속담(Cold hands, warm heart)의 번역 - 한국 표준 속담 아님 |
| 2582 | 인간은 만물의 척도 | 제외 | 서양 철학자(프로타고라스) 명언 - 속담/관용구 아님 |

두 목록의 판단 축이 다름을 재확인: **10건**은 "실제 존재하는 한국 속담/관용구
이지만 내용이 부적절하다"는 **콘텐츠 정책 판단**이고, **2건**은 "이게 애초에
한국 전통 속담이 맞는가"라는 **언어적 사실 판단**이다(2건 모두 AI reason이
구체적 출처를 명시했고 실제로 한국 속담이 아님이 맞다). 둘 다 현재
`review_status='제외'`로 동일하게 처리돼 있지만, 판단 성격은 서로 다르므로
이번 보고서에서도 표를 분리해 유지했다.

### 4-3. 학생용 API 미반환 재확인

`scripts/literacy/verify_student_exposure_repro.py`를 재실행했다(venv 활성화,
TestClient 기반, 실제 크리덴셜 미사용, DB 쓰기 없음 - GET 요청만). phase7의
확인을 그대로 재현했다:

```
① 미인증: GET /literacy/terms -> 302 /login, GET /api/literacy/health -> 302 /login
② 로그인(스텁): GET /literacy/terms?q=유용하다 -> 200, 정의 텍스트 노출(관리자 화면)
③ GET /literacy/review/definition?id=2891 -> 200, 정의 텍스트는 노출 안 됨(빈 작성 화면)
④ GET /api/literacy/health -> 200 {"status":"ok"}
⑤ /literacy/*, /api/literacy/* 라우트 전체 목록: /api/literacy/health 단 1개만
   /api/literacy 하위에 존재 - 데이터 반환 엔드포인트는 여전히 없음
```

**재확인 결론**: `/api/literacy/*`(학생용으로 예정된 prefix)는 여전히 헬스체크
하나뿐이고, 실제 데이터를 반환하는 학생용 엔드포인트는 코드로 존재하지 않는다.
phase7의 결론이 이번 세션에서도 그대로 재현됐다(변경 없음).

---

## 다음 단계 적용 시 필요한 백업·롤백 조건 제안 (이번 세션 아님)

phase4(`schema_reading_phase4_literacy_link_apply_20260924.md`)·phase7의 게이트
패턴(정확한 대상 건수 하드가드, dry-run 기본값, 백업 후 적용, 체크섬 대조,
멱등성, 롤백 스크립트)을 그대로 따르되, 이번에 확정한 6건에 맞춰 구체화한다.

1. **백업 먼저**: `data/literacy.db`를 타임스탬프 붙은 파일로 SQLite Backup
   API로 복사(phase7의 `.bak-*` 패턴 재사용) + 복사본 `PRAGMA integrity_check`/
   `foreign_key_check` 확인.
2. **대상 건수 하드 가드**: 재적재/UPDATE 스크립트는 대상이 **정확히 6개
   id**(2891, 3168, 3085, 3180, 3015, 2924)와 정확히 일치하지 않으면 즉시 중단
   (초과·미달 모두 거부). `reports/literacy_krdict_fallback_dryrun_6_20260924.csv`
   를 소스 오브 트루스로 스크립트에서 직접 읽어 대조하는 것을 권장(하드코딩
   중복 방지).
3. **UPDATE 범위 제한**: 이 6개 `id`에 대해서만 `definition`(및 `note`에 정정
   근거 추가)을 UPDATE하고, `level`/`review_status`/`reviewed_at`/`pos`/
   `sense_category` 등 다른 컬럼은 건드리지 않는다.
4. **사전/사후 체크섬**: UPDATE 전, `momo-textbook` 소스 전체(821건)의
   `(id, headword, level, review_status)` 체크섬을 찍어두고, UPDATE 후 이 6건을
   제외한 815건의 체크섬이 완전히 동일한지 확인.
5. **dry-run 기본값**: `--dry-run`이 기본, 실제 UPDATE는 `--apply` 명시 플래그로만.
   `reports/literacy_krdict_fallback_dryrun_6_20260924.md`의 전/후 값을 그대로
   사람이 최종 승인.
6. **멱등성**: 이미 수정된 DB에 재실행해도(현재값이 이미 제안값과 같으므로)
   대상 0건으로 아무 것도 하지 않아야 한다.
7. **롤백**: 백업 파일로부터 6개 `id`만 복원하는 전용 스크립트(또는 백업 전체
   복원 경로) 준비.
8. **111건 HOLD 및 다른 소스 3곳(collect_proverbs.py 281건·import_sajaseongeo.py
   3건·import_schemareading_vocab.py 미상)은 이번 6건과 별도로, 각 소스별
   `resolve_definition`류 함수를 새로 설계·검증한 뒤에만 다뤄야 한다** - 이번
   세션의 수정 코드는 momo-textbook 경로에만 적용했으므로, 이 소스들을 향후
   재적재하면 여전히 옛 버그(definitions[0] 무조건 채택)가 그대로 발생한다.

---

## 산출물

### 신규/수정 코드 (로컬, git 미커밋)

- `scripts/literacy/import_textbook_vocab.py` — **수정**: `resolve_definition()`
  신규 함수 + `run()`의 적재 루프가 이를 쓰도록 교체(동음이의/다의어 시 보류).
  실제 DB 재적재는 하지 않았다.
- `scripts/literacy/scan_momo_textbook_krdict_fallback_821_full.py` — **신규**.
  821건 전수 스캔 스크립트(읽기 전용).
- `tests/test_krdict_fallback_hold_fix.py` — **신규**. 29/29 통과.

### 신규 데이터/보고서 (로컬, git 미커밋)

- `data/import/krdict_fallback_821_full_audit_20260924.json` — 821 전수 스캔
  원본 결과(fallback/위험신호/판정 전체)
- `reports/literacy_krdict_fallback_dryrun_6_20260924.csv`,
  `reports/literacy_krdict_fallback_dryrun_6_20260924.md` — 6건 dry-run 표
- 본 보고서: `reports/schema_reading_phase8_krdict_fallback_full_audit_20260924.md`

### 실제 변경된 DB

- **없음.** `data/literacy.db`는 이번 세션 내내 읽기 전용으로만 접근했다(mtime
  불변, integrity_check `ok`로 확인).

---

## 최종 요약 (호출한 에이전트용)

1. **821건 분류**: fallback 후보 317건(위험신호 123·안전 194). 기존 26건 중
   krdict fallback 관련 NULL_DEF 21건은 전부 fallback pool 안에서 재확인됨.
   자동 유사도 휴리스틱으로는 REPLACE_CANDIDATE 3/4, NO_ISSUE 10/17만 재현됐고
   (문자 중첩 기반 휴리스틱의 한계), 나머지(관대하다 포함)는 phase7의 기존 확립
   근거를 재조사 없이 그대로 유지했다. **신규 발견 REPLACE_CANDIDATE: 0건**
   (신규 위험신호 111건은 momo_book.db 내 대조 근거 자체가 없어 전부 HOLD).
   SHORT_DEF/PUNCT_DAMAGE 5건은 별도 버그(krdict fallback 아님)로 값 불변 재확인.
2. **dry-run 최종 대상**: **6건**(유용하다·관대하다·모락모락·선구자·혼비백산·
   독불장군), 신규 추가 없음. 궁색하다(HOLD)·기색(신규 경계사례, HOLD 유지)
   모두 dry-run 표에서 제외. 파일: `reports/literacy_krdict_fallback_dryrun_6_
   20260924.csv`/`.md`.
3. **다른 원천 영향 범위**: `collect_proverbs.py`(krdict 속담/관용구, 다의어
   281/2,884건=9.7%), `import_sajaseongeo.py`(사자성어, 다의어 3/225건),
   `import_schemareading_vocab.py`(tooldict, 패턴 동일·건수 미실측),
   `apply_sajaseongeo_datefix_10.py`(이미 실행 완료된 1회성 스크립트) - 총 4곳,
   momo-textbook 포함 5곳이 같은 패턴.
4. **테스트 결과**: `tests/test_krdict_fallback_hold_fix.py` **29/29 통과**,
   기존 `tests/test_sajaseongeo_date_lead_fix.py` **34/34 통과**(회귀 없음
   재확인). 실제 literacy.db에는 수정 미적용.
5. **성차별 10건**: 전부 행 존재, `review_status='제외'` 유지 확인(삭제 아님).
   **언어사실 2건**(id=2139, 2582)은 독립 목록으로 분리 유지, 콘텐츠 정책
   판단과 혼합하지 않음.
6. **학생용 API**: `verify_student_exposure_repro.py` 재실행 결과 `/api/literacy/*`
   는 여전히 `/health` 하나뿐, 데이터 반환 엔드포인트 없음 - phase7 결론 재현.
7. **가장 중요한 리스크**:
   - (a) momo-textbook 6건 수정은 아직 코드에만 있고 DB에는 미적용 - 다음
     세션에서 위 백업/게이트 절차로 적용해야 한다.
   - (b) 이번에 고친 `resolve_definition()`은 momo-textbook 경로 하나뿐이다 -
     krdict 속담/관용구(281건)·사자성어(3건) 소스는 여전히 옛 버그가 살아있고,
     이 소스들을 다음에 재적재하면 같은 유형의 오매칭이 새로 생길 수 있다.
   - (c) 111건의 신규 HOLD는 momo-textbook 내부 대조 수단이 구조적으로 없다 -
     krdict 원본이 다의어/동음이의를 갖고 있다는 사실 자체는 확인됐지만, "교재가
     의도한 뜻이 무엇인지"는 momo_book.db 안에 그 증거가 아예 없는 상태라
     외부 자료(교재 원문 재대조 등) 없이는 더 이상 자동으로 검증할 수 없다.
   - (d) 기색(id=3278)은 기존 NO_ISSUE 판정을 유지했지만 자동 계산이 이견을
     보인 경계 사례로, 사람이 momo_book.db 원문 맥락(교재 예문 등)을 직접 봐야
     확정할 수 있다.
