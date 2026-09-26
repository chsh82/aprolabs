# 스키마리딩×어휘 DB 통합 — 16단계: L4·L5 관리자용 문항 파일럿 (dry-run)

- 작성일: 2026-09-26 (작업 자체는 2026-09-25에 수행됨)
- **세션 경위 참고**: 이번 16단계는 위임한 에이전트가 실질 작업(작업 A~D)을
  전부 마친 뒤 본 보고서를 작성하려던 순간 스트림이 멈춰(무응답 600초) 강제
  종료됐다. 남겨진 산출물(스크립트 diff, 테스트 파일, 결과표)을 호출 세션이
  전부 다시 열어 독립적으로 재검증했고, 재작업 없이 그 검증 결과를 이 보고서에
  담는다. DB 쓰기는 애초에 이번 단계 범위 밖이었고, 서버·로컬 DB 모두 세션
  종료 전후로 변경되지 않았음을 재확인했다(6절).

---

## 1. 작업 A — caution 필드 보충 + 스크립트 리팩터 + 테스트

### 1-1. 배경

phase15 감사(`scripts/vocab/audit_l4_l5_batch_quality.py`)는 "유추"·"유사"·
"사법권" 3건의 혼동 위험을 phase13 보고서 **본문에만** 서술된 내용에서
가져와 스크립트 내부 하드코딩 딕셔너리(`SUPPLEMENTARY_MEANING_REVIEW`)로
보충 판정했다. 기계가 읽는 필드(결과표 `caution` 컬럼)와 사람이 읽는 문서
(보고서 본문)가 분리되는 문서화 간극이었다.

### 1-2. 조치

- `data/import/schema_reading_phase13_l4_core50_final_20260925.csv`의
  "유추"·"유사"·"사법권" 행 `caution` 컬럼에 해당 캐주션 텍스트를 직접 기록.
- `scripts/vocab/audit_l4_l5_batch_quality.py`에서 `SUPPLEMENTARY_MEANING_REVIEW`
  딕셔너리를 **완전히 제거**하고, MEANING_REVIEW 판정이 오직 결과표 `caution`
  컬럼 + literacy.db 재조회로 얻는 구조적 신호(동형이의 복수 행, 사실 서술형
  정의문 패턴)만으로 이뤄지도록 리팩터. 모듈 docstring에 "새 배치도 caution을
  반드시 결과표 컬럼에 직접 기록해야 하며, 하드코딩 딕셔너리로 되돌리지
  않는다"고 명시.

### 1-3. 검증 (호출 세션이 재실행)

`tests/test_audit_l4_l5_caution_column.py` (신규) 재실행 결과:

```
[PASS] caution 컬럼이 비어있으면 AUTO_PASS
[PASS] caution 컬럼에 값이 있으면 MEANING_REVIEW
[PASS] MEANING_REVIEW 사유에 caution 텍스트가 그대로 인용됨
[PASS] '유추'류 패턴(가짜유추어)도 caution 컬럼만으로 MEANING_REVIEW
[PASS] 모듈에 SUPPLEMENTARY_MEANING_REVIEW 속성이 더 이상 존재하지 않음
[PASS] 모듈에 SUPPLEMENTARY로 시작하는 어떤 전역 딕셔너리도 남아있지 않음
[PASS] audit_row 함수 소스에 '유추'/'유사'/'사법권' 리터럴이 없음(캐주션 컬럼만 읽음)
[PASS] HOLD 제외 97건 적재(L4 49 + L5 48)
[PASS] L4: AUTO_PASS 43 / MEANING_REVIEW 6 (phase15 보고와 동일)
[PASS] L5: AUTO_PASS 31 / MEANING_REVIEW 17 (phase15 보고와 동일)
[PASS] SOURCE_EVIDENCE_WEAK 0건 (phase15 보고와 동일)
[PASS] '유추': caution 컬럼만으로(하드코딩 없이) 여전히 MEANING_REVIEW
[PASS] '유사': caution 컬럼만으로(하드코딩 없이) 여전히 MEANING_REVIEW
[PASS] '사법권': caution 컬럼만으로(하드코딩 없이) 여전히 MEANING_REVIEW

15/15 passed
```

리팩터 전후 97건 판정 분포가 **완전히 동일**(L4 43/6, L5 31/17)함을 확인 —
하드코딩 제거가 기존 판정을 바꾸지 않았다.

---

## 2. 작업 B — 파일럿 후보 선정

- 수정된 판정 기준(작업 A 반영 후)의 AUTO_PASS 74건(L4 43 + L5 31)에서
  선정.
- **V 항목만으로 목표(L4·L5 각 최대 10건)를 채울 수 있어 S 항목은 포함하지
  않았다** — "적합한 후보가 부족하면 20건을 채우지 않는다"는 지시에 따라
  V로 충분한 이상 S를 억지로 섞지 않음.
- 최종 선정: **L4 10건**(착수·조치·집단·종속·합성·하위·중복·정기·간과·공간),
  **L5 10건**(대등·본론·논술·부가·가치관·간략·감안·계승·국면·급진).

---

## 3. 작업 C·D — 문항 초안 + 메타데이터 + 자동검증

- 스크립트: `scripts/vocab/phase16_build_quiz_pilot_dryrun.py`(원래
  `scratch_pilot/build_pilot_items.py`에 있던 것을 호출 세션이 정식 위치로
  이동, 하드코딩된 절대경로를 `Path(__file__)` 기반으로 수정 후 재실행해
  **바이트 단위로 동일한 출력**이 나옴을 확인).
- 형식: 기존 `vocabulary_multiformat_items` 포맷의 `MEANING_CHOICE`·
  `CONTEXT_MEANING` 두 유형을 그대로 따름. 20개 표제어 × 2유형 = **40건**.
- 오답 선택지: 무작위 생성이 아니라 **같은 배치 안 다른 표제어의 실제
  정의**를 그대로 가져와 배치(`DISTRACTOR_OFFSETS`로 순환 선택). 각 오답마다
  "이 설명은 '{lemma}'가 아니라 '{다른표제어}'의 뜻이다" 형태의 근거를
  `wrong_option_reasons_json`에 기록.
- 메타데이터: `content_id_for_draft`, `source_content_id`(phase13/14
  content_id), `literacy_term_id`, `vocab_level`, `explanation`(정답 근거),
  `wrong_option_reasons_json`(오답 근거), `expert_review_status`("관리자
  검토용 초안 - 전문가 검수 완료 아님"), `l5_grade_caveat`(L5 항목에만:
  "L5 고1 근거는 개별 학년 태그가 아니라 정책 매핑임" — phase14의 한계를
  문항 메타데이터에도 유지).
- **짝 개념/동형이의 문제**: 이번 20개 표제어(착수·조치·집단·종속·합성·
  하위·중복·정기·간과·공간·대등·본론·논술·부가·가치관·간략·감안·계승·
  국면·급진) 중에는 phase13/14/15가 찾은 기존 짝개념 쌍(지향/지양,
  개관/개괄, 구축/구현, 해령/해구, 발산형/수렴형경계, 쪼개짐/깨짐,
  인간중심주의/생태중심주의자연관, 가변/불변, 거시/미시, 사후/향후,
  성향/지향)에 해당하는 항목이 없다 — 애초에 MEANING_REVIEW로 분류된
  항목들이라 AUTO_PASS 파일럿 풀에 들어오지 않았기 때문. 이번 20개 안에서
  새로운 짝개념 위험도 스캔했으나 추가로 발견된 것은 없다.

### 자동검증 (호출 세션이 재실행해 재확인)

```
총 문항 40건 (MEANING_CHOICE 20 + CONTEXT_MEANING 20)
자동검증 PASS 40 / HOLD 0
```

검사 항목: 선택지 중복 없음, 정답 유일성(정답 텍스트가 옵션 중 정확히
1번만 등장), 정답 옵션이 explanation에 인용된 정의와 일치, 옵션 4개 존재,
CONTEXT_MEANING 문항은 프롬프트에 목표어가 【 】로 실제 표시됐는지.
**40/40 전부 PASS, 보류 0건.**

### 기존 문항 중복 확인

`data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.csv`의
`source_content_id`(전부 `SR_L4CORE_*`/`SR_L5CORE_*`, phase13/14 신규
비공개 콘텐츠)는 서버 `vocabulary_multiformat_items`의 기존 문항이 참조하는
`source_content_ids_json`에 애초에 존재할 수 없는 content_id다(phase13/14
적재 시점에 이미 "신규 97건을 참조하는 문항 0건"을 확인했고, 16단계 동안
문항을 만들지 않았으므로 그 상태가 그대로 유지됨). 즉 구조적으로 기존
문항과 중복될 수 없다.

---

## 4. 산출물

- `scripts/vocab/audit_l4_l5_batch_quality.py` (수정 - 하드코딩 딕셔너리 제거)
- `tests/test_audit_l4_l5_caution_column.py` (신규, 15/15 통과)
- `data/import/schema_reading_phase13_l4_core50_final_20260925.csv`/`.jsonl`
  (수정 - "유추"·"유사"·"사법권" caution 컬럼 보충)
- `scripts/vocab/phase16_build_quiz_pilot_dryrun.py` (신규 - 파일럿 문항
  생성기, 재실행 가능)
- `data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.csv`/`.jsonl`
  (40건 문항 초안)
- 본 보고서

---

## 5. 이번 단계에서 하지 않은 것 (지시대로)

- DB 적재: 하지 않음.
- 학생 노출·공개 상태 변경: 하지 않음.
- S 항목: 이번 파일럿에 포함하지 않음(V만으로 충분).
- 전문가 검수 완료 표시: 어디에도 하지 않음 — 모든 문항에
  "관리자 검토용 초안" 표시 유지.

---

## 6. 현재 상태 재확인 (호출 세션이 직접 재확인)

| 항목 | 확인 결과 |
|---|---|
| `literacy.db` mtime | 2026-09-24 08:37:40(phase7 마지막 UPDATE 이후 불변) |
| 서버 `vocabulary_contents` 총 행수 | 5,820 (5,723 + L4 49 + L5 48, 16단계로 변화 없음) |
| `SUM(student_exposure)`/`SUM(public_ready)` | 0 / 0 |
| `PRAGMA integrity_check` | ok |
| git commit/push | 없음(본 보고서 작성 시점까지 미커밋) |

---

## 7. 가장 중요한 리스크

1. 위임 에이전트가 보고서 작성 직전 무응답으로 중단된 이력이 있음 — 실제
   산출물은 전부 호출 세션이 재실행·재검증했으므로 신뢰할 수 있으나, 향후
   비슷한 대규모 세션은 중간 체크포인트(예: 결과표를 먼저 저장한 뒤 보고서
   작성)를 권장.
2. 이번 파일럿 20개 표제어는 전부 V(학습도구어)였다 — S(교과개념어) 항목의
   실제 문항화 검증은 아직 이뤄지지 않았다(phase15가 이미 지적한 "S 37건
   전문가 검수 미실시"와 별개로, "S를 실제 문항으로 만들 때 자동검증이
   똑같이 통과하는지"는 다음 파일럿에서 확인 필요).
3. 오답 선택지가 "같은 배치 안 다른 표제어의 정의"에서만 뽑혀서, 배치가
   작을 때(이번 20개) 오답 후보 다양성이 제한적이다 — 실제 서비스 규모로
   확장하면 오답 선택 전략을 다시 검토할 필요가 있다.
4. 40건 전부 dry-run 파일 상태이며, 실제 퀴즈 문항 테이블에 적재되지
   않았다 — 다음 단계(적재 여부)는 사용자 판단 필요.
