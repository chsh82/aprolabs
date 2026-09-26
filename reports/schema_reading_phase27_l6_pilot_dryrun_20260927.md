# 27단계: L6 관리자용 퀴즈 파일럿 — 파일 기반 dry-run

- 일자: 2026-09-27
- DB 쓰기: **없음** (전 과정 SELECT만 수행, 생성·검증 스크립트 전부 읽기 전용)
- 세션 생성 코드 변경: 없음 (`app/vocabulary_quiz/` 미수정)
- 학생 공개·push·배포: 없음

## 1. 콘텐츠 선정 (69건 중 20건)

- L6 비공개 V 콘텐츠 = phase24 배치1의 V 37건 + phase26 배치2의 V 32건 = **69건**
  (매커니즘·이성·신장 HOLD 3건은 애초에 DB에 적재된 적이 없어 이 풀에 나타나지
  않음, S 13건은 `level_reason_json.is_S_subject_concept`로 걸러 제외 - 스크립트가
  69/13 두 숫자를 assert로 재확인)
- 선정 방식: `literacy_term_id` 오름차순 상위 20건(결정론적, 재현 가능) -
  `scripts/vocab/phase27_select_l6_pilot_content.py`
- 선정 결과: `data/import/schema_reading_phase27_l6_pilot_selected_20260927.json`
  (아이디어·야기·엄밀·오류·완결·왜곡·용이·운용·위계·의지·일괄·자발적·자아·잠정·
  전면·전이·전제·전형적·창출·체재)
- caution 확인: 20건 중 5건(완결/의지/자아/전면/체재)에 기존 caution이 있음 -
  그 상대 표제어(완료/견지/이면 등)는 전부 이번 20건 **밖**에 있어, 뒤 단계의
  오답 후보 풀(20건 자체)에는 애초에 들어올 수 없다는 것을 확인했다(2절 참고).

## 2. 문항 생성 (최대 40문항)

- `scripts/vocab/phase27_generate_l6_pilot_items.py` - 실행할 때마다 DB의
  `vocabulary_contents.student_definition`/`example_sentence`/
  `example_target_form`을 다시 읽는다(정의 문자열을 스크립트에 하드코딩하지
  않음 - `git grep`으로 스크립트 소스에 완성된 한국어 정의 문장이 없음을 확인
  가능, 있는 것은 조사·템플릿 문구뿐).
- 유형: 뜻 고르기(MEANING_CHOICE) 1문항 + 문맥형(CONTEXT_MEANING) 1문항 ×
  20건 = **40문항**
- 오답(distractor) 3개는 같은 20건 안에서만 뽑는다(외부 콘텐츠 의존 없음,
  파일 하나로 완결). 대상의 caution에 등장하는 표제어가 이번 20건 안에 있으면
  그 표제어를 오답 후보에서 제외하는 로직을 항상 적용 - 이번 선정에서는 실제
  제외된 사례가 0건이었지만(caution 상대가 전부 20건 밖에 있어서), 로직 자체는
  코드에 존재하며 다음에 다른 20건을 선정하면 바로 작동한다.
- 문맥형 문항은 이미 phase24/26에서 사람이 직접 작성해 둔 `example_sentence`/
  `example_target_form`을 그대로 재사용해 `【표제어 활용형】`으로 표시 -
  새로 문장을 짓지 않아 정의·예문 불일치 위험 자체를 구조적으로 없앴다.
- **문법 버그 발견·수정**: 1차 생성 결과를 직접 읽다가 받침 유무에 따른 조사
  이형태(은/는, 이/가, 을/를, 이라는/라는, 와/과)가 전부 한쪽으로 고정돼
  "완결는", "체재'이라는" 같은 비문이 20건 중 9~10건에서 나타난 것을 발견했다.
  받침 판정 함수(`has_batchim`)를 추가해 전면 수정하고, 40건 전체를 다시
  생성해 조사 오류가 0건임을 별도 자동 검사(정규식 대조)로 재확인했다
  (`scripts/vocab/phase27_verify_l6_pilot_items.py`의 `check_particles`).

## 3. 문항별 기록 항목

각 문항(`data/import/schema_reading_phase27_l6_pilot_items_20260927.json`)에
다음을 기록했다:
- `item_id`(안정적 패턴 `MF_{A|C}_SC_SRL6PILOT_20260927_L6_{seq:03d}`), `source_content_id`
- `qa_flags_json.literacy_term_id`, `qa_flags_json.content_source_version`
  (원천 term_id·콘텐츠 source_version)
- `explanation`(정답 근거), `qa_flags_json.wrong_option_reasons`(오답 3개
  각각이 틀린 이유 - "이 설명은 X가 아니라 Y의 뜻이다" + Y의 실제 정의)
- `qa_flags_json.caution`(콘텐츠 자체의 caution 그대로 승계)
- `qa_flags_json.l6_grade_caveat`: "L6=고2~3 분류는 개별 grade_level 태그가
  아니라 정책 매핑에 따른 잠정 근거"라는 설명을 40건 전부에 고정 기록

## 4. 별도 검증 단계 (생성 로직과 분리)

### 4-1. 기계 검증 (`auto_validation_status`) - `phase27_verify_l6_pilot_items.py`
DB를 다시 읽어 재확인: 정답 유일성, 보기 4개 완전중복 없음, 정의-예문 일치
(explanation ↔ 지금 DB student_definition), 문맥형 표시 형식·문장 일치,
기존 문항과 item_id/`(content_id, item_type)` 중복 없음, 조사 이형태 정확성.

**결과: 40/40 AUTO_PASS** (`data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json`)

### 4-2. 의미 판정 (`semantic_verdict`) - 사람이 직접 40건 전부를 읽고 판정
`phase27_finalize_l6_pilot_results.py`가 병합하되, 판정 값 자체는 스크립트가
계산하지 않고 호출 세션이 직접 기록한 것만 반영한다(기록되지 않은 item_id는
자동으로 `SEMANTIC_HOLD`가 되어 목표 수량을 채우려고 판정을 건너뛸 수 없게
설계함).

40건 전부를 직접 읽고 다음을 확인했다:
- 오답 3개의 의미 영역이 정답과 겹치지 않음(caution 근접 관계가 이번 20건
  안에 우연히도 하나도 없어 뜻 근접으로 인한 혼동 사례 자체가 없었음)
- 문맥형 20건은 예문의 문맥 단서가 나머지 3개 오답과 결합되지 않아 정답을
  하나로 좁힘(예: "그 드라마는 10부작으로 【완결되었다】"는 '용이/잠정/
  아이디어'와 결합 불가능)
- 해설이 지금 DB 정의와 정확히 일치

**결과: 40/40 SEMANTIC_PASS**

### 4-3. 최종 판정(`final_status`)
`auto_validation_status`와 `semantic_verdict`가 **둘 다** PASS일 때만 PASS,
하나라도 아니면 HOLD.

**최종: 40/40 PASS, HOLD 0건**

이번 20건 선정이 caution으로 연결된 두 표제어를 동시에 포함하지 않아(예:
완결↔완료, 의지↔견지, 전면↔이면 짝의 상대가 전부 20건 밖) 실제로 보류된
문항이 없었다 - 목표 수량(40건)을 채우려고 문제를 발견하고도 넘어간 것이
아니라, 애초에 이번 선정 구성에서 그런 충돌이 생기지 않았다. 다음에 다른
20건(예: 완결과 완료가 모두 들어갈 수 있는 미래의 확장 선정)을 고르면 이
로직이 실제로 문항을 HOLD시킬 것이다.

## 5. 독립 재검증 (`tests/test_phase27_l6_pilot_items.py`)

생성/검증 스크립트와는 별개로 다시 짠 테스트로 16개 항목을 재확인(전부 PASS):
item_id 40개 고유성, 40개 문항이 기존 `vocabulary_multiformat_items`와
전혀 겹치지 않음(적재된 적 없음 재확인), `vocabulary_multiformat_items`
행수가 1,329건 그대로(이번 단계 어떤 스크립트도 DB에 쓰지 않았음을 재확인),
선정 20건 전부 V(S 유출 없음)·HOLD 3건 표제어 없음, explanation이 지금
DB `student_definition`과 일치, 보기 중복 없음, 정답 유일성, 문맥형 표시
형식 일치, 결과표(CSV) PASS/HOLD 집계와 문항 JSON 집계 일치.

실행 결과: 16/16 PASS.

## 6. 재실행 명령

```
# 1) 콘텐츠 선정
python3 scripts/vocab/phase27_select_l6_pilot_content.py \
  --db-path <research db 사본, 읽기 전용> \
  --out data/import/schema_reading_phase27_l6_pilot_selected_20260927.json

# 2) 문항 생성(초안)
python3 scripts/vocab/phase27_generate_l6_pilot_items.py \
  --db-path <research db 사본> \
  --selected-json data/import/schema_reading_phase27_l6_pilot_selected_20260927.json \
  --out /tmp/phase27_draft.json

# 3) 기계 검증
python3 scripts/vocab/phase27_verify_l6_pilot_items.py \
  --db-path <research db 사본> \
  --items-json /tmp/phase27_draft.json \
  --out data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json

# 4) 의미 판정 병합(최종 산출물)
python3 scripts/vocab/phase27_finalize_l6_pilot_results.py \
  --draft-items /tmp/phase27_draft.json \
  --auto-verify data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json \
  --out-items data/import/schema_reading_phase27_l6_pilot_items_20260927.json \
  --out-result data/import/schema_reading_phase27_l6_pilot_result_20260927.csv

# 5) 독립 재검증
VOCABULARY_QUIZ_DB_PATH=<research db 사본> python tests/test_phase27_l6_pilot_items.py
```
(모두 읽기 전용 - `--db-path`에 실서버 원본을 직접 지정해도 SELECT만 하므로
안전하지만, 사본을 쓰는 것을 권장)

## 7. 최종 보고

| 항목 | 값 |
|---|---|
| 선정 콘텐츠 수 | 20건 (전체 L6 비공개 V 69건 중, S 13건·HOLD 3건 제외) |
| 생성 문항 수 | 40건 (MEANING_CHOICE 20 + CONTEXT_MEANING 20) |
| 기계 검증(AUTO_VALIDATION) PASS | 40/40 (MEANING_CHOICE 20/20, CONTEXT_MEANING 20/20) |
| 의미 판정(SEMANTIC_VERDICT) PASS | 40/40 (MEANING_CHOICE 20/20, CONTEXT_MEANING 20/20) |
| 최종 PASS | 40/40 |
| 최종 HOLD | 0건 (사유: 이번 20건 선정 구성에 caution 연결 쌍이 우연히 없었음 - 3절 참고, 목표 수량을 채우려 대체하지 않았음) |
| DB 적재 | **없음** (파일 산출물만) |
| 학생 공개·push·배포 | 없음 |

## 산출물
- `scripts/vocab/phase27_select_l6_pilot_content.py`
- `scripts/vocab/phase27_generate_l6_pilot_items.py`
- `scripts/vocab/phase27_verify_l6_pilot_items.py`
- `scripts/vocab/phase27_finalize_l6_pilot_results.py`
- `tests/test_phase27_l6_pilot_items.py`
- `data/import/schema_reading_phase27_l6_pilot_selected_20260927.json`
- `data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json`
- `data/import/schema_reading_phase27_l6_pilot_items_20260927.json`(최종 40문항)
- `data/import/schema_reading_phase27_l6_pilot_result_20260927.csv`(결과표)

## 다음 단계로 넘길 사항
- 이 40문항을 실제로 관리자 화면에서 재생하려면(phase19가 만든 "L4·L5 파일럿"
  모드와 같은 방식) 별도 단계에서 `app/vocabulary_quiz/routers/multiformat.py`에
  L6 파일럿 화이트리스트·매니페스트를 추가하는 코드 변경이 필요하다 - 이번
  단계에서는 하지 않았다(지시 준수).
- 실제 DB 적재를 원하면 phase18/24/26과 같은 게이트(백업·단일 트랜잭션·
  체크섬·멱등성) 적용 스크립트를 새로 작성해야 한다.
- caution으로 연결된 쌍(완결↔완료 등)이 실제로 같은 20건 안에 들어가는
  경우의 HOLD 동작은 아직 실제 사례로 관찰되지 않았다 - 필요하면 그런 쌍이
  포함되도록 선정 기준을 바꿔 한 번 더 dry-run 해 보는 것을 다음 단계로
  제안한다.
