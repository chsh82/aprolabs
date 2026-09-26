# 25단계: L6 신규 50건 전수 의미 감사 + 잔여 V 35건 dry-run + 롤백 명령 검증

- 일자: 2026-09-27
- DB 쓰기: 없음(literacy.db·research DB 전부 읽기 전용 SELECT만 수행, 롤백 테스트는 서버 DB 사본에만 적용)
- push/배포: 없음

## 1. 적재된 L6 50건 전수 재대조

### 1-1. 방법
- `data/import/schema_reading_phase24_l6_core_final_20260927.csv`(적재된 50건)과
  local literacy.db(server literacy.db와 이 50건의 `id` 범위에서 content-hash
  일치 재확인 완료, 아래 4절 참고)를 행별로 대조.
- 기계적 검증은 `scripts/vocab/phase25_l6_semantic_audit.py`로 재현 가능:
  1) `canonical_definition`이 literacy.db 원문 `definition`과 정확히 일치하는지(자구
     변경으로 인한 의미 확대/축소 여부의 1차 신호)
  2) 기존 vocabulary_contents 5,820건 중 `lemma`가 hamming distance 1(같은 길이,
     한 글자 차이) 또는 포함 관계(길이 차 1, 부분 문자열)인 "근접 후보"를 전수
     추출 - 사람이 놓칠 수 있는 유의어/반의어 쌍을 찾는 실마리.
- 근접 후보로 나온 177개 쌍을 전부 직접 읽고, 의미가 실제로 근접한 경우만 아래
  하이라이트에 남겼다(대부분은 '위계/위생'처럼 글자만 겹치고 뜻은 무관한
  false positive였음 - 50건 중 38건이 근접 후보를 가졌으나 실제 혼동 위험이
  있는 쌍은 아래 6개뿐).

### 1-2. 결과: 50/50 PASS (HOLD 0건)
- `canonical_definition` vs literacy.db 원문 불일치: **0건** (전부 자구 그대로)
- `student_definition`/`example_sentence`가 원천 의미를 넓히거나 좁힌 사례: **0건**
  (개별 근거는 `data/import/schema_reading_phase25_l6_50_semantic_audit_20260927.csv`
  50행에 전부 기록)
- S 13건의 `expert_review_required` 플래그는 그대로 유지(변경 없음, DB 미기록)

### 1-3. 지정 확인 항목(사용자가 명시한 3쌍)
| 쌍 | literacy.db 원문 | 판정 |
|---|---|---|
| 완결(SR_L6CORE_4946) / 완료(SR_L4CORE_4759, 기존) | 완결="완전하게 끝을 맺음.” / 완료="완전히 끝마침." | 사전적 정의는 거의 동일하나 용법이 다름(완결=저작물·연재물이 매듭지어짐 / 완료=일반 과업 종료). phase24가 이미 구분 문구 caution을 남겼고, 예문("드라마 10부작 완결")도 완결 고유 용법으로만 작성돼 원천을 벗어나지 않음. **PASS**(caution 유지) |
| 결여(SR_L6CORE_4981) / 결점(기존 SC_V1920_B020_032) | 결여="마땅히 있어야 할 것이 빠져서 없어나 모자람." / 결점="잘못되거나 부족하여 완전하지 못한 점." | 결여=있어야 할 것이 없는 '상태', 결점=구체적인 '흠' 자체 - 개념적으로 구분됨. phase24가 원문 오탈자("없어나"→"없어지거나") 정정 사실과 결점과의 관계를 caution에 이미 기록. **PASS**(caution 유지) |
| 타당성(SR_L6CORE_4969) / 부당성(기존 SC_V1964_B064_010) | 타당성="사물의 이치에 맞는 옳은 성질." / 부당성="이치에 맞지 않는 성질." | 구조적 반의어 쌍(지향/지양급). phase24가 강한 구분 문구 caution을 이미 남김. **PASS**(caution 유지) |

### 1-4. 이번 감사에서 새로 찾은 근접 유의어(phase24 caution 누락분) - 3건
phase24의 caution 필드에는 기록되지 않았지만, 근접 후보 스캔으로 제가 직접 발견해
`data/import/schema_reading_phase25_l6_50_semantic_audit_20260927.csv`의
`additional_finding_phase25` 컬럼에 남긴 것:

- **개략(SR_L6CORE_4973)** vs 기존 **개괄**(SR_L5CORE_4839)·**개관**(SR_L5CORE_4838):
  세 단어 모두 "대강 요약/살펴봄" 계열 근접 유의어 3종 클러스터. 완결/완료급 혼동
  위험이 있음에도 phase24 caution에 없었음.
- **총합(SR_L6CORE_4966)** vs 기존 **총량**(SC_V19118_B118_002): 둘 다 "전체를
  합한 값" 계열. 용법(점수 합계 vs 물리량 총계)은 다르나 뜻풀이가 근접.
- **합리적(SR_L6CORE_4972)** vs 기존 **논리적**(SR_L5CORE_4858): 실제 국어 교육
  현장에서도 자주 혼동/구분 지도되는 근접 유의어 쌍.

이 3건은 정의 자체가 틀리거나 원천을 벗어난 것이 아니므로 **HOLD로 재분류하지
않았다**(등재된 50건은 여전히 50/50 PASS) - 다만 향후 문항 제작 시 완결/완료·
결여/결점·타당성/부당성과 동일한 수준의 구분 문구가 필요하다는 점을 결과표에
남겼다. DB의 `level_reason_json`/caution 텍스트 자체를 이번 단계에서 갱신하지는
않았음(DB 쓰기 금지 지시 준수) - 필요하면 phase22가 "정기" 건에 했던 것과 같은
방식의 별도 캡션 업데이트 단계를 다음에 열 수 있다.

## 2. 잔여 V NEW_CANDIDATE 35건 dry-run

### 2-1. 중복 재확인
- phase23의 116건 결과표에서 `final_classification=NEW_CANDIDATE`·`v_s=V`·
  `proposed_first_l6_batch != Y`인 행을 다시 필터링 → **정확히 35건**(72-37=35,
  기대값과 일치)
- `literacy_term_id` 기준 적재된 50건과 중복: **0건**
- `lemma` 기준 적재된 50건 및 기존 5,820건과 중복: **0건**
- 전부 `reuse_check=NO_MATCH(exact lemma vs vocabulary_contents 5,820)` 유지 확인

### 2-2. 배치 후보 평가 (dry-run만, 적재/문항 생성 없음)
- 35건 전부 `dryrun_status=READY_FOR_NEXT_BATCH` - 다음 비공개 배치로 고려 가능한
  상태(전부 literacy.db 원문 정의를 그대로 학생용으로 옮길 수 있는 수준의 단일
  의미, 나선형 반복·동형이의 충돌 없음).
- phase23이 이미 남긴 근접 유의어/반의어 caution 4건: 전면/이면(반의어), 통찰/고찰,
  실재/실질, 객관성/객관적(파생쌍) - 다음 배치 작성 시 그대로 유지 권장.
- 이번 단계에서 제가 추가로 남긴 참고 사항 2건(DB에 반영하지 않음, 결과표
  `additional_note_phase25` 컬럼):
  - **매커니즘**(id=4995): literacy.db 표제어 표기 자체가 '매커니즘'(표준
    외래어 표기는 '메커니즘'). 다음 배치 작성 시 원문 그대로 쓸지 표준 표기로
    교정할지 결정 필요 - 이번엔 dry-run이라 결정하지 않음.
  - **이성**(id=5024): literacy.db 안에는 이 표제어가 하나뿐이라 DB 내부
    동형이의 충돌은 없으나, 일상어 '이성(異性)'과 외부적으로 혼동될 수 있어
    실제 작성 시 예문에서 사유 능력 맥락임을 분명히 할 필요.
- **50건에 채우지 않고 그대로 dry-run 평가만 함** - DB 적재도, 문항 생성도
  하지 않았다(지시 그대로 준수).

결과표: `data/import/schema_reading_phase25_l6_v35_dryrun_20260927.csv`

## 3. 롤백 명령 실제 적합성 검증 (서버 DB 사본에만 적용, 실서버 미실행)

### 3-1. 왜 확인이 필요했나
phase24 보고서가 안내한 복구 명령은 `scripts/vocab/rollback_literacy_link_apply.py`
인데, 이 스크립트의 이름과 모듈 docstring은 "phase4(literacy 링크 적용)를
되돌리는" 용도로 좁게 서술되어 있다. 이름만 보고 L6 배치(phase24) 복구에도
그대로 쓸 수 있다고 가정하면 안 되므로, 코드를 직접 읽고 서버 DB 사본으로
실제 동작을 확인했다.

### 3-2. 코드 분석
- Gate 1(`.env`의 `APP_ENV=research`), Gate 2(`--db-path` basename이 정확히
  `vocabulary_quiz_research.db`)는 어떤 백업에도 적용되는 범용 가드 - phase4
  전용 하드코딩이 없음.
- Gate 3은 `--backup-path` 파일명에 `.bak-`가 포함되어 있고 그 파일 자체의
  `PRAGMA integrity_check`가 `ok`인지만 확인한다 - 백업 라벨이 `phase4-...`인지
  `phase24-l6-core-...`인지는 전혀 구분하지 않는다.
- 실제 복원 동작은 **DB 파일 전체를 통째로 `shutil.copy2`로 교체**하는
  방식(테이블 단위 복원이 아님) - 어떤 시점의 백업이든 그 시점의 전체 DB
  상태로 완전히 되돌린다. 따라서 이름과 달리 phase24 백업에도 기계적으로
  올바르게 작동한다.
- **주의(구조적 한계, 이번에 코드로 확인)**: 통째 파일 교체 방식이므로, 이
  백업 시점(2026-09-26 15:19:39) **이후에 다른 쓰기가 있었다면 그것도 함께
  사라진다**. 현재는 phase24가 research DB에 대한 마지막 쓰기이므로 문제
  없지만, 앞으로 phase26 이상에서 추가 쓰기가 발생한 뒤에는 이 백업으로
  복원하면 그 사이 쓰기도 함께 유실된다 - "L6 50건만 되돌리는" 명령이 아니라
  "그 시점 전체로 되돌리는" 명령임을 다음 단계 담당자가 알아야 한다.

### 3-3. 서버 DB 사본으로 실제 실행 검증
실서버 DB(`~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`)와
백업 파일을 서버의 임시 격리 디렉터리(`~/scratch/phase25_rollback_test/`)에
복사해, 그 사본에만 대해 스크립트를 실행했다(실서버 원본은 전혀 건드리지
않음 - 검증 후 서버 원본 행수 5,870 재확인, 임시 디렉터리는 검증 직후 삭제).

- dry-run 실행: GATE 1/2/3 전부 PASS, "현재 DB 5,870건 vs 백업 DB 5,820건" 미리보기 정상 출력
- `--confirm` 실행(사본 대상): 성공. 실행 로그:
  ```
  1) 복구 전 안전 백업: vocabulary_quiz_research.db.rollback-before-restore-20260926-154443
  2) 백업으로 복원 완료
  3) 복원 후 PRAGMA integrity_check: ok
  복원 후 DB 상태: {'vocabulary_contents': 5820, 'vocabulary_content_levels': 5820,
                    'vocabulary_content_literacy_links': 142}
  INTEGRITY_OK=True
  ```
- 복원된 사본에서 재확인: `SR_L6CORE_*` 행 **0건**(완전히 제거됨),
  `vocabulary_multiformat_items` **1,329건**(문항 영향 없음), integrity_check=ok
- 검증 후 실서버 원본 재확인: `vocabulary_contents` **5,870건**(변화 없음, 사본만
  대상이었음을 재확인) - 임시 디렉터리 삭제 완료

### 3-4. 결론
`rollback_literacy_link_apply.py`는 이름과 달리 **phase24 L6 50건 백업 복구
명령으로도 기계적으로 적합**함을 코드 검토 + 서버 DB 사본 실행으로 확인했다.
실제 복구가 필요할 경우 명령은 다음과 같다(사용 전 3-2절의 "그 시점 전체로
되돌아간다"는 한계를 재확인할 것):

```
python3 scripts/vocab/rollback_literacy_link_apply.py \
  --env-file /home/chsh82/aprolabs/.env \
  --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  --backup-path /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase24-l6-core-pre-migration-20260926-151939
(--confirm 추가 시 실제 복원)
```

## 4. 불변 사항 재확인 (기존 5,820건 / L6 50건 비공개 / 문항 참조 0건)

| 확인 항목 | 값 |
|---|---|
| 기존 5,820건(source_version != l6_manual_v1) content-hash | `b80ba0bfaa75fe3e6a51f731c9aef41e1640b1c234bb388da35744357b303dff` (phase24 적용 전과 완전 동일) |
| 전체 vocabulary_contents 행수 | 5,870 (5,723+49+48+50) |
| L6 50건 student_exposure/public_ready 합계 | 0 / 0 |
| L6 50건을 참조하는 vocabulary_multiformat_items | 0건 |
| vocabulary_multiformat_items 총 행수 | 1,329 (불변) |
| `PRAGMA integrity_check` | ok |
| `PRAGMA foreign_key_check` 위반 | 0건 |
| literacy.db mtime(로컬) | 2026-09-24 08:37:40 (phase24 이후 불변) |
| literacy.db mtime(서버) | 2026-09-26 09:32:29 UTC (이번 단계에서 SELECT만 수행, 쓰기 없었음) |
| local literacy.db ↔ server literacy.db, L6 50건 term_id 대상 content-hash | 두 쪽 다 `aee05a835a9450869c5c3d387ca75ca3143e939876f0e5d56cbf2ceb33db89c7` 로 완전 일치(로컬 사본으로 대조해도 안전함을 재확인) |

DB 쓰기·push·배포는 이번 단계에서 전혀 하지 않았다(연구 서버 SELECT, literacy.db
SELECT, 롤백 스크립트는 서버 임시 사본에만 `--confirm` 실행 후 즉시 삭제).

## 재실행 명령

```
python3 scripts/vocab/phase25_l6_semantic_audit.py \
  --literacy-db data/literacy.db \
  --research-db-copy <research DB 사본 경로>
```
(1-1/1-4/2-1절의 기계적 재검증을 재현. PASS/HOLD의 사람 판단 근거 자체는
`schema_reading_phase25_l6_50_semantic_audit_20260927.csv`/
`schema_reading_phase25_l6_v35_dryrun_20260927.csv`에 기록되어 있으므로 이
스크립트가 재현하지 않는다.)

## 다음 단계로 넘길 사항
- 개략/개괄/개관, 총합/총량, 합리적/논리적 3쌍에 대한 caution 텍스트 보강(DB
  `level_reason_json` 갱신은 phase22의 "정기" 사례처럼 별도 게이트 적용 단계 필요)
- 잔여 V 35건의 다음 비공개 배치 실제 작성·적재 여부는 사용자 결정 대기
- S 13건 전문가 검수는 여전히 대기 상태(변경 없음)
