# 비전 결과 → 조판 레코드 매핑 + 3자 비교 + 검수 부담 재산정 (2026-09-24)

`vision_parse/layout_map.py`에 매핑 함수를 구현했다. `momo_book.db`·기존
`normalize`/`layout` 파이프라인은 전혀 건드리지 않았다 - 이 매핑은 별도
함수로, vision_item 1건을 입력받아 `q`에 병합할 form 필드 dict를 낸다.

## 1. 매핑 설계

| layout_hint.shape | 매핑 form | 비고 |
|---|---|---|
| `table_answer` | `table` (기본) | 행 라벨이 전부 "1." "2." "3."류 순번이면 `list`로 승격(아래 "한계" 참고) |
| `compare_two_col` | `compare` | table_json 행 또는 blanks를 카드로 |
| `numbered_list` | `list` | blanks 또는 table 행을 items로 |
| `choice_options` | `choice`(2개) / `choiceList`(3개 이상) | 기존 choice_ab/choice_multi 관례와 동일 |
| `boxed_form` | `pledge` | `n`은 blank_lines 또는 표 행 수 |
| `speech_bubble` | `speech` | `starter`는 vision의 excerpt_text(말풍선 안 시작 문구) |
| `ruled_lines` | `single` | blank_lines≤1이면 kind=short, 아니면 long |
| `reference_table` | `reference`(신규, SPEC 8종에 없음) | 학생 답란이 없는 참고표 - 아래 "한계" 참고 |
| `unclear`/그 외 | `single` + flag | 사용자 지시대로 |

### 한계 (억지로 규칙을 늘리지 않고 그대로 보고)

- **compare vs table 구분 신호 없음**: 야옹아 1번(목표 compare)과 열하일기
  7번(목표 table)이 vision에서 **똑같이 `table_answer`, 2행, 순번 없음**으로
  나왔다 - 구조가 동일해서 vision_item 데이터만으로는 구분이 안 된다(2절
  결과 참고). 카드가 좌우로 나란한지 표 형태로 위아래인지는 지금 프롬프트가
  안 물어본다 - 다음 반복에서 `layout_hint`에 "orientation: horizontal/
  vertical" 같은 필드를 추가하면 해결될 수 있다(선잇기 프롬프트 보강과 함께
  검토 권장).
- **list 판정은 순번 패턴에만 의존**: "1." "2." "3."로 시작하는 행이 전부일
  때만 list로 승격한다. 반례(순번 있는데 list가 아닌 경우)는 이번 검증
  범위에서 못 찾았지만, 305건 전체를 보증하지는 않는다.
- **reference_table은 SPEC 8종 form에 없다**: 학생이 채울 답란이 없는
  참고용 표(예: 열하일기 한자 참고표)를 `table`로 강제 매핑하면 빈
  정답칸이 있는 것처럼 보인다 - `reference`라는 새 태그로 구분만 해 뒀다.
  실제 페이지에 어떻게 배치할지(질문과 나란히? 배경지식으로?)는 조판
  단계 설계가 더 필요하다.

## 2. 기준 3종 검증 — 지정하신 6개 항목

| 문서 | 위치 | 목표 | vision layout_shape | 매핑 결과 | 판정 |
|---|---|---|---|---|---|
| 야옹아(L2-Q2-W08) | 1번 | compare | table_answer(2행, 순번无) | **table** | ✗ 불일치 (위 "한계" 참고 - compare/table 구분 신호 없음) |
| 야옹아 | 2-1번 | list | table_answer(3행, "1." "2." "3.") | **list** | ✓ 일치 |
| 야옹아 | 2-2번 | speech | speech_bubble | **speech** | ✓ 일치 |
| 야옹아 | 3번 | pledge | boxed_form | **pledge** | ✓ 일치 |
| 열하일기(L9-Q3-W07) | 5-1(한자 참고표) | table | reference_table | **reference**(신규 태그) | △ 설계상 의도적 - "정답 없는 표"를 답란 있는 table로 강제하지 않음(1절 참고) |
| 열하일기 | 7번(답안표) | table | table_answer(2행, 순번无) | **table** | ✓ 일치 |

**요약**: 6건 중 4건 정확히 일치, 1건은 설계 판단(reference 분리, "틀렸다"기
보다 SPEC에 없는 케이스를 억지로 끼워 맞추지 않은 것), 1건은 vision
데이터만으로 구분 불가능함을 확인한 진짜 한계(다음 프롬프트 반복 대상).

원래 계획한 "(가) 비전 기반 새 초안 / (나) 기존 초안 / (다) 시안
samples/*.layout.json" 3자 비교 중, **(나) 기존 초안은 이 6개 항목에서
전부 `form=None, kind=long`(위젯 미지정)** 이었다 - momo_book.db의
discussion_qa.ui_type이 이 6개 항목 전부 `text_long`으로만 기록돼 있어
(원본 파서가 이 구조들을 애초에 구분 못 함), 기존 규칙 엔진은 6개 항목
모두 그냥 "단순 답란"으로 냈다. **즉 6개 중 5개(compare 제외)는 방식 B
없이는 애초에 자동으로 못 만들던 위젯**이었다 - 이번 작업의 존재 이유를
정량적으로 보여주는 결과다.

## 3. 305건 전체 위젯 부착 통계 (신규 Vision 호출 없음 - 매핑 함수만 적용)

전체 vision_item 5,476건에 매핑 함수를 적용:

| form | 건수 | 비율 |
|---|---:|---:|
| single | 2,268 | 41.4% |
| table | 1,352 | 24.7% |
| choice | 642 | 11.7% |
| pledge | 328 | 6.0% |
| list | 290 | 5.3% |
| reference | 279 | 5.1% |
| compare | 220 | 4.0% |
| speech | 87 | 1.6% |
| choiceList | 10 | 0.2% |

**전체 위젯(single이 아님) 부착 비율: 3,208/5,476 = 58.6%**

### discussion_qa 유형만 비교 (기존 `widget_unavailable` 플래그와 같은 기준)

| | 기존 파이프라인(momo_book.db 기반) | 방식 B(vision 매핑) |
|---|---:|---:|
| 대상 항목 수 | 2,211(discussion_qa) | 2,833(discussion_qa 유형) |
| 위젯 미지정(placeholder류) | **1,322건 (widget_unavailable)** | **339건**(unclear 또는 table_answer인데 표 데이터 없음) |
| 비율 | 약 59.8% | 약 12.0% |

`item_type='discussion_qa'`로 범위를 맞춰도 항목 수 자체가 다르다(2,211 vs
2,833) - 이유는 `NORMALIZE_QUALITY_ANALYSIS.md`/`VISION_PARSE_FINAL_REPORT_305.md`
에서 이미 확인한 집계 차이(어휘 표를 항목 1개로 묶는 방식 등)와 같다.
비율로 보면 **위젯 미지정 비율이 59.8% → 12.0%로 줄었다** - 정성적으로는
"검수에서 위젯을 새로 골라야 하는 문항이 5개 중 3개꼴에서 8개 중 1개꼴로
줄었다"는 뜻이다.

## 4. 305건 전체 예상 검수 부담 재산정

```
기존(momo_book.db 기반 layout/generate.py) 305건 전체 재실행 결과:
  총 플래그 9,492건
    split               2,507
    derived             1,910
    placeholder         1,908
    widget_unavailable  1,322   <- 이번에 감소 확인 대상
    missing             1,009
    sup                   386
    typo                  259
    lowres                136
    structure              55
```

(이 9,492건은 layout 단계까지 포함한 전체 집계라, 이전에 보고했던 "2,912건"
(정규화 단계만)·"2,798건"(정규화 단계, 수정 후)과는 **집계 범위가 다르다** -
"2,912건"은 이번 `widget_unavailable`을 포함하지 않는다. 같은 잣대로
비교하려면 이번 9,492건을 기준으로 삼는 게 맞다.)

vision 매핑 기준 "위젯 미지정류"(unclear + table_answer 데이터 없음, 2절
방식) 339건과 비교하면, **discussion_qa 대상 위젯 미지정이 1,322 → 339건
으로 줄어드는 것으로 추정**된다(약 74% 감소). 다만 이건 "레이아웃 JSON을
끝까지 생성해서 다시 플래그를 센 것"이 아니라 **매핑 함수 결과만으로 계산한
추정치**다 - 실제로 vision 데이터를 조판 파이프라인에 완전히 통합해
`generate_layout()`과 동등한 산출물을 만들면(이번 범위 밖 - 사용자 지시
[1]은 "매핑"까지) 이미지 슬롯·STEP 그룹핑 등에서 새로운 종류의 flag가
추가로 나올 수 있다.

## 5. 별도 과제 — 선잇기(매칭형) 문제 집계

"낱말의 뜻을 찾아 선으로 이어보세요" 문구가 있는 항목: **77건**(전체 305건
기준). 그중 **정의 목록(표)이 아예 안 잡힌 것: 15건**(15개 문서:
`L1-Q1-W03`, `L1-Q2-W05`, `L1-Q3-W03`, `L1-Q3-W04`, `L1-Q3-W06`,
`L1-Q3-W09`, `L1-Q4-W09`, `L1-Q4-W12`, `L2-Q1-W03`, `L2-Q1-W04`,
`L2-Q2-W11`, `L2-Q4-W01`, `L2-Q4-W03`, `L2-Q4-W06`, `L2-Q4-W12`).
나머지 62건은 표 형태로 어떻게든 잡혔다(정확도까지는 이번에 검증 안 함).
**15건이 확인된 "완전 누락" 규모**이고, 62건은 "표는 있지만 정확한지 확인
필요"로 별도 확인이 필요하다 - 이번에는 집계만 하고 프롬프트 수정은 보류.
