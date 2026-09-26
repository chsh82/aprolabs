# 26단계: L6 잔여 V 후보 비공개 배치(32건) 적재 + phase25 caution 3건 반영

- 일자: 2026-09-27
- literacy.db 쓰기: 없음(읽기 전용 유지, 로컬/서버 mtime 불변 재확인)
- 학생 공개·push·배포: 없음

## 1. phase25 caution 3건을 실제 DB caution 필드에 기록

### 1-1. 대상 컬럼 확인
"caution"은 별도 컬럼이 아니라 `vocabulary_content_levels.level_reason_json`
(JSON) 안의 `"caution"` 키에 저장된다(phase24_apply_l6_core.py에서 확인).
phase25가 발견한 3건(개략/총합/합리적)은 이 키가 빈 문자열(`""`)이었다.

### 1-2. 적용
- 백업: SQLite Backup API, 라벨 `phase26-caution-update-pre-migration`
  - 경로: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase26-caution-update-pre-migration-20260926-155858`
  - SHA-256: `bec0f5844080105e0ece0f4e170c87fd33f8a77e965d2e300eba91f3a3d213c0`
  - 행수 일치(5,870=5,870), integrity_check=ok, FK 위반 0건
- 적용 스크립트: `scripts/vocab/phase26_apply_l6_caution_updates.py`
  - GATE 3 하드가드: 3건(`SR_L6CORE_4973`/`4966`/`4972`) 전부 현재 `caution==""` 확인 후에만 진행(부분 일치·혼재 상태면 전체 중단)
  - GATE 4: 단일 트랜잭션으로 `level_reason_json`의 `caution` 키만 교체(다른 키 순서·값 전부 유지)
  - GATE 5: 3건 새 caution 확인, 대상 3건을 제외한 나머지 vocabulary_content_levels 체크섬 불변, vocabulary_contents 전체 불변(5,870=5,870), vocabulary_multiformat_items 불변(1,329=1,329), integrity_check=ok, FK 위반 0건, exposure/public 합계 여전히 0
  - 결과: `UPDATED=3`, 전 게이트 PASS
- **재실행 검사로 동일하게 읽히는지 테스트**: 스크립트를 그대로 다시 실행 →
  `GATE 3: 이미 새 값=3건 ... 멱등 재실행으로 판단, 대상 0건` - DB에 실제로 반영된
  caution 텍스트를 재조회해 다시 비교하는 방식이므로, 보고서 본문에만 적힌 문구가
  아니라 **DB에 저장된 값 자체**로 검사가 통과함을 확인했다.
- 기존 L6 50건의 PASS 판정·비공개 상태: student_definition/canonical_definition/
  lemma/level_status/vocab_level/exposure/public 등 caution 외 어떤 필드도
  건드리지 않았으므로 그대로 유지(GATE 5의 "다른 키 불변" 확인으로 재검증).

## 2. 잔여 V 35건 작성 + 전수 대조

### 2-1. HOLD 3건 (요청대로 유지)
| 표제어 | id | HOLD 사유 |
|---|---|---|
| 매커니즘 | 4995 | literacy.db 원문 표기 '매커니즘' vs 표준 외래어 표기 '메커니즘' - 처리 방침 미확정 |
| 이성 | 5024 | 동형이의(理性 사유능력 / 異性 남녀관계) 목표 뜻 미확정 |
| 신장 | 5002 | **이번 단계에서 추가 지정.** 동형이의(伸張/伸長 세력 확장[literacy.db 정의] / 腎臟 콩팥 / 身長 키) 목표 뜻 미확정 - '이성'과 동일한 외부 동형이의 위험 수준이라 같은 기준 적용 |

나머지 32건은 literacy.db 정의가 단일하고 명확해 추가 HOLD 사유를 찾지 못했다
(전수 검토 결과: 뜻 범위가 불명확한 사례 없음).

### 2-2. 32건 작성 및 원천 대조
- `data/import/schema_reading_phase26_l6_v35_final_20260927.csv`에 표제어·품사·
  원천 정의(canonical_definition)·학생용 정의·예문·example_target_form(예문 내
  표제어 활용형, phase24 방식 재사용)·caution·판정을 행별로 기록.
- 32건 전부 `canonical_definition`이 literacy.db 원문과 정확히 일치(자동
  재확인, 불일치 0건).
- 원천 정의가 좁은 의미(물리적/문자적)인 경우 예문도 그 범위를 벗어나지 않게
  작성(예: '연쇄'=문자 그대로 이어진 사슬, '연쇄 반응'류 비유적 확장 금지 -
  phase24의 '추진'/'탈피' 처리 방식과 동일한 기준).
- 언어학 전문 개념('분절성')은 원천 정의의 예시(무지개 색깔)를 예문에 그대로
  반영해 의미 범위를 벗어나지 않게 함.
- 근접 유의어/파생/대비 짝 caution 6건 기록: 전면/이면(대비), 통찰/고찰(근접),
  객관성/객관적(파생), 실재/실질(근접, phase23에서 이미 확인), 그리고 이번에
  새로 확인한 **논증/논지/논하다 + 기존 논거·논술의 '논-' 계열 세트**(개념은
  다르나 동시 학습·출제 시 구분 문구 권장).

### 2-3. 기존 5,870건과 뜻 단위 재중복 확인
- 정확한 lemma 중복(35건 전체 대상): **0건**
- 근접 후보(hamming distance 1 또는 부분 포함, 5,870건 전체 대상) 스캔: 30/35건이
  근접 후보를 가졌으나 직접 읽고 확인한 결과 실제 혼동 위험은 위 6개 세트뿐,
  나머지는 글자만 겹치는 false positive(예: '신장'↔'찬장'류, '기호성'↔'기동성'류)
- **35건을 채우려 하지 않았다** - HOLD 3건은 그대로 두고 32건만 결과표에 담음

## 3. dry-run → 실제 적재

### 3-1. dry-run(적재 전 구조 검증)
- 로컬 sandbox DB 사본(서버에서 받은 최신 5,870건 사본)에 동일한 INSERT
  로직을 먼저 실행해 구조적 오류(컬럼 수 불일치, FK 위반 등)가 없는지 확인 -
  32건 추가, 5,870→5,902행, integrity_check=ok, FK 위반 0건. 사본은 검증 후 삭제.
- 32건 각각 `content_id=SR_L6COREV2_<literacy_term_id>` 형식이 기대값과
  정확히 일치하는지, `example_target_form`이 `example_sentence`의 부분
  문자열인지 사전 assert로 확인(전부 통과).

### 3-2. 실제 적재 (research DB)
- 백업: SQLite Backup API, 라벨 `phase26-l6-v35-batch2-pre-migration`
  - 경로: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase26-l6-v35-batch2-pre-migration-20260926-160125`
  - SHA-256: `7d7f5dae34176d07269239941419338191f12fc91a38c556fb518338a8fae0fc`
  - 행수 일치(5,870=5,870, caution 갱신 반영 후 상태), integrity_check=ok, FK 위반 0건
- 적용 스크립트: `scripts/vocab/phase26_apply_l6_v35_batch2.py`
  - `source_version='schema_reading_literacy_l6_manual_v2'`(phase24의 v1과
    구분되는 두 번째 L6 배치 고유 마커), `content_id` 접두사 `SR_L6COREV2_`,
    `vocab_level=6`, `level_status='REVIEW_BOUNDARY'`, `student_exposure=0`,
    `public_ready=0`
  - GATE 3: 신규 삽입 대상 32건(기존 존재 0건)
  - GATE 4: 단일 트랜잭션 커밋, 삽입 32건(vocabulary_contents + vocabulary_content_levels 각각)
  - GATE 5: integrity_check=ok, FK 위반 0건, 신규 32건 exposure/public 전부 0,
    vocabulary_multiformat_items 불변(1,329=1,329, 퀴즈 문항 생성 없음),
    **기존 5,870건(신규 배치 접두어 제외) 체크섬 불변**
  - 결과: `INSERTED=32 / SKIPPED=0 / HELD=3 / INTEGRITY_OK=True / FK_OK=True / EXPOSURE_ALL_PRIVATE=True / EXISTING_ROWS_UNCHANGED=True`
- **멱등성 재실행**: 스크립트를 그대로 다시 실행 → `이미 존재(스킵 대상) 32건,
  신규 삽입 대상 0건` → `INSERTED=0 / SKIPPED=32`

### 3-3. 적재 후 라이브 SQL 재확인 (호출 세션이 직접 재조회)
```
vocabulary_contents 총 행수        : 5,902 (5,723+49+48+50+32)
source_version 분포                : 2.1.29|5723, l4_manual_v1|49, l5_manual_v1|48,
                                      l6_manual_v1|50, l6_manual_v2|32
전체 student_exposure/public_ready : 0 / 0
batch2(v2) student_exposure/public_ready : 0 / 0
vocab_level=6 & level_status=REVIEW_BOUNDARY 행수 : 82 (50+32)
batch2 32건을 참조하는 vocabulary_multiformat_items : 0건
vocabulary_multiformat_items 총 행수 : 1,329 (불변)
PRAGMA integrity_check             : ok
PRAGMA foreign_key_check 위반      : 0건
```
- literacy.db mtime: 로컬 2026-09-24 08:37:40(불변), 서버 2026-09-26 09:32:29 UTC(불변) - 이번 단계도 SELECT만 수행

## 4. 백업 체인과 복구 안내 (항목 5 - 중요)

이번 단계에서 서버 research DB에 대한 쓰기가 **두 번** 있었고, 그 사이에 각각
전체 파일 백업을 만들었다. 백업은 전부 `rollback_literacy_link_apply.py`
(phase25가 검증한 대로, 이름과 달리 어떤 `.bak-*` 백업에도 기계적으로
동작하는 파일 전체 교체 방식)로 복원할 수 있지만, **어느 백업을 쓰느냐에 따라
되돌아가는 지점이 다르다**:

| 백업 라벨 | 시점 | 이 백업으로 복원하면 |
|---|---|---|
| `phase24-l6-core-pre-migration-20260926-151939` (phase24) | L6 1차 50건 적재 전 | 1차 50건 + caution 갱신 + 2차 32건 **전부** 사라짐(5,820건으로 복귀) |
| `phase26-caution-update-pre-migration-20260926-155858` (이번 단계) | caution 3건 갱신 전, 1차 50건은 이미 적재된 상태 | caution 갱신 + 2차 32건이 사라짐(1차 50건은 유지, 5,870건이지만 caution 3건은 빈 문자열로 되돌아감) |
| `phase26-l6-v35-batch2-pre-migration-20260926-160125` (이번 단계) | 2차 32건 적재 전, caution 갱신은 이미 반영된 상태 | 2차 32건만 사라짐(1차 50건 + caution 갱신은 유지, 5,870건) |

**즉 "가장 최근 백업"을 골라도 그 시점 이후의 모든 변경이 통째로 사라진다** -
L6 32건만, 또는 caution 갱신만 선택적으로 되돌리는 명령이 아니다(phase25가
확인한 구조적 한계와 동일). 복구가 필요하면 반드시 되돌리려는 지점보다
먼저 만들어진 백업 중 **가장 가까운** 것을 골라야 하며, 그보다 늦게 이뤄진
다른 정상 변경까지 함께 사라진다는 점을 반드시 확인한 뒤 실행할 것.

```
python3 scripts/vocab/rollback_literacy_link_apply.py \
  --env-file /home/chsh82/aprolabs/.env \
  --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  --backup-path <위 표에서 고른 백업 경로>
(--confirm 추가 시 실제 복원. 기본은 dry-run)
```

## 5. 최종 보고

- **실제 적재 건수**: 32건(`SR_L6COREV2_*`, source_version=`schema_reading_literacy_l6_manual_v2`)
- **HOLD 3건과 사유**:
  - 매커니즘(4995): 표기 정책(원문 '매커니즘' vs 표준 '메커니즘') 미확정
  - 이성(5024): 동형이의(理性/異性) 목표 뜻 미확정
  - 신장(5002): 동형이의(伸張/腎臟/身長) 목표 뜻 미확정 - 이번 단계에서 이성과 동일 기준으로 추가 지정
- **caution 갱신 3건**: 개략(SR_L6CORE_4973)/총합(SR_L6CORE_4966)/합리적(SR_L6CORE_4972) -
  `level_reason_json.caution`에 근접 유의어 안내 반영, DB 값으로 재검사 통과 확인
- **최종 DB 행 수**: `vocabulary_contents`/`vocabulary_content_levels` 각 **5,902건**
  (5,723 기존 + 49 L4 + 48 L5 + 50 L6-1차 + 32 L6-2차), `vocabulary_multiformat_items`
  **1,329건**(불변, 신규 문항 없음)
- 신규 32건 전부 `student_exposure=0`/`public_ready=0`, 참조 퀴즈 문항 0건
- literacy.db 읽기 전용 유지, 학생 공개·push·배포 없음

## 재실행 명령
```
# caution 갱신 재검사(멱등)
python3 scripts/vocab/phase26_apply_l6_caution_updates.py \
  --env-file /home/chsh82/aprolabs/.env \
  --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db

# batch2 적재 재검사(멱등)
python3 scripts/vocab/phase26_apply_l6_v35_batch2.py \
  --env-file /home/chsh82/aprolabs/.env \
  --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  --rows-csv data/import/schema_reading_phase26_l6_v35_final_20260927.csv
```

## 다음 단계로 넘길 사항
- 매커니즘/이성/신장 3건: 표기·동형이의 목표 뜻 확정 후 재평가 필요
- '논-' 계열 세트(논증/논거/논지/논술/논하다) caution은 이번에 신규 32건 쪽에
  기록했으나, 기존 '논거'·'논술'(SR_L5CORE) 쪽 level_reason_json에는 아직
  반영하지 않음 - 필요하면 phase26의 caution 갱신과 같은 방식으로 추가 가능
- S 13건 전문가 검수는 여전히 대기 상태(변경 없음)
