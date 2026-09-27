# 28단계: L6 관리자 파일럿 40문항 비공개 적재

- 일자: 2026-09-27
- literacy.db 쓰기: 없음(읽기 전용 유지, 로컬/서버 mtime 불변 재확인)
- `app/vocabulary_quiz/` 라우터 코드 변경: **없음**
- 학생 공개·push·배포: 없음

## 1. phase27 산출물 고정 + 서버 콘텐츠 재대조

`scripts/vocab/phase28_freeze_check.py`가 phase27 산출물 8개 파일의 SHA-256을
고정값으로 박아 두고(커밋 `96fe3b9` 시점 값), item_id 40개 고정 목록과 함께
다시 계산해 전부 일치함을 확인했다. 이어서 서버 DB를 다시 조회해:

- 선정 20개 content_id 전부 존재, `is_active=1`, `student_exposure=0`,
  `public_ready=0`, `level_status='REVIEW_BOUNDARY'` - 위반 0건
- 40개 문항의 explanation/보기에 인용된 정의가 **지금** DB의
  `student_definition`과 정확히 일치(드리프트 0건) - phase27 생성 이후
  콘텐츠가 바뀐 적이 없음을 재확인

**결과: 전 항목 PASS, 적재 진행 가능.**

## 2. dry-run 검증 (적재 전)

`scripts/vocab/phase28_build_rows_from_phase27.py`가 phase27 최종 문항
JSON을 DB 삽입용 행(rows-json)으로 변환하면서 다음을 전부 통과해야만 파일을
만들도록 설계했다(하나라도 실패하면 파일 자체를 만들지 않음):

- 입력 정확히 40건, 유형별 정확히 20/20
- item_id 40개 상호 중복 없음 + 기존 `vocabulary_multiformat_items`와 충돌 0건
- 문항 내용(유형+프롬프트+보기 집합) 완전 중복 0건
- 40건 전부 실제 `vocabulary_contents`를 가리키는 유효한 content_id 연결
- `public_payload_json`에는 보기 목록만(정답 인덱스 없음), `answer_payload_json`에는
  정답 인덱스만 - 채점 정보와 공개 정보를 파일 단계에서부터 분리

**결과: 6개 항목 전부 PASS, rows-json 40건 생성.**

## 3. 실제 적재

- 백업: SQLite Backup API, 라벨 `phase28-l6-pilot-quiz-items-pre-migration`
  - 경로: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase28-l6-pilot-quiz-items-pre-migration-20260927-001152`
  - SHA-256: `71be6bc4f51ddb030722ed469533fcb767bccf46509ab03c5782ee65051db9df`
  - 적용 전 행수: `vocabulary_contents`/`vocabulary_content_levels` 각 5,902건, integrity_check=ok, FK 위반 0건
- 적용 스크립트: `scripts/vocab/phase28_apply_l6_pilot_quiz_items.py`
  (phase18_apply_quiz_pilot.py 패턴 재사용 + level_status 재확인 + 기존 행
  체크섬 불변 확인 추가)
  - `source_version='schema_reading_l6_pilot_dryrun_v1'` - 기존 일반 출제
    (`2.1.29`, 1,289건)와도, 기존 L4·L5 파일럿(`schema_reading_l4l5_pilot_dryrun_v1`,
    40건)과도 다른 새 고유 마커
  - GATE 3: content_id 20개 전부 존재·is_active=1·exposure=0·public=0·
    REVIEW_BOUNDARY 확인 + 적용 전 행수(5,902/1,329) 확인
  - GATE 4: 신규 삽입 대상 40건(기존 존재 0건)
  - GATE 5: 단일 트랜잭션 커밋, 삽입 40건
  - GATE 6: integrity_check=ok, FK 위반 0건, 신규 유형별 20/20, 연결 40/40,
    중복 0, 전체 문항 수 1,369, 기존 콘텐츠 5,902건 체크섬 불변, 기존 문항
    1,329건 체크섬 불변

### 3-1. 적용 중 발견·수정한 스크립트 버그
최초 실행 시 GATE 5(삽입) 커밋까지는 정상 완료됐으나, 커밋 **이후** 실행되는
읽기 전용 검증 쿼리 하나(`GROUP BY item_id HAVING c > 1`)에 파라미터 바인딩을
빠뜨린 코드 버그가 있어 스크립트가 예외로 중단됐다(`Incorrect number of
bindings supplied`). 이 시점에 **데이터 자체는 이미 정상적으로 40건 커밋된
상태**였다 - 별도의 독립 점검 스크립트(`phase28_postcheck.py`, 커밋 대상
아님)로 직접 재계산해 전체 문항 1,369건, 유형별 20/20, 연결 40/40, 중복 0건,
기존 콘텐츠·문항 체크섬이 적용 전 스냅샷과 정확히 일치함을 확인한 뒤에야
스크립트의 버그를 고쳤다(파라미터 추가) - 데이터를 다시 만지지 않고 코드만
수정. 이후 같은 스크립트를 재실행해 멱등성(`INSERTED=0/SKIPPED=40`)을
정상적으로 확인했다(중간에 GATE 3의 "적용 전 행수" 하드가드가 1,329만
허용하던 것도 "1,329 또는 1,369(이미 적용된 경우)"를 모두 허용하도록
고쳐 재실행 자체가 막히지 않게 했다).

## 4. 적재 후 확인 (독립 재계산 + 실제 라우터 함수 호출)

| 확인 항목 | 값 |
|---|---|
| 전체 문항 수 | 1,369 (기존 1,329 + 신규 40) |
| 신규 배치 유형별 | MEANING_CHOICE 20 / CONTEXT_MEANING 20 |
| 신규 40건 content_id 연결 | 40/40 |
| item_id 중복 | 0건 |
| `PRAGMA integrity_check` | ok |
| `PRAGMA foreign_key_check` 위반 | 0건 |
| 기존 vocabulary_contents(5,902건) | 체크섬 불변 |
| 기존 vocabulary_multiformat_items(1,329건, 신규 제외) | 체크섬 불변 |
| student_exposure/public_ready 합계 | 0 / 0 |
| 멱등 재실행 | `INSERTED=0 / SKIPPED=40` |

### 4-1. 일반 출제에서 신규 40건이 선택되지 않는 근거 (라우터 미수정, 실제 코드 경로로 검증)
`app/vocabulary_quiz/routers/multiformat.py`를 전혀 수정하지 않고, 그 모듈의
실제 상수·함수를 그대로 import해 호출한 결과(`tests/test_phase28_l6_pilot_quiz_items_applied.py`):

- `SOURCE_VERSION`(`"2.1.29"`, 일반/레벨 모드)과 `PILOT_SOURCE_VERSION`
  (`"schema_reading_l4l5_pilot_dryrun_v1"`, 기존 L4·L5 파일럿) 중 어느 값으로
  필터링해도 신규 40건은 **0건** 노출됨을 SQL로 재확인
- 실제 함수 `_select_question_items`(혼합 모드 전체 후보), `_select_level_candidates`
  (레벨 6, `confidence_mode='all_candidates'` - 신규 L6 콘텐츠의
  `level_status='REVIEW_BOUNDARY'`가 이 모드의 매칭 조건 자체는 만족시키지만,
  문항 쪽 `source_version` 필터에서 걸러짐을 실측), `_select_pilot_item_ids`
  (기존 L4·L5 파일럿 - 오염 시 `PilotBatchIntegrityError`를 던지도록 설계된
  화이트리스트 교집합 검사를 통과해 여전히 정확히 40건만 반환, 신규 배치
  미포함, 예외 없음)를 **직접 호출**해 반환된 id 목록에 신규 40건이 전혀
  없음을 확인했다 - SQL을 손으로 흉내 낸 것이 아니라 앱의 실제 선택 로직을
  그대로 실행한 결과다.
- 기존 일반 출제 풀(1,289건)과 기존 L4·L5 파일럿 풀(40건) 건수도 그대로임을
  재확인(신규 배치가 우연히도 다른 풀에 섞여 들어가지 않았음).

## 5. 복구 안내

이번 백업(`phase28-l6-pilot-quiz-items-pre-migration-20260927-001152`)은
`vocabulary_multiformat_items`에 40건을 추가하기 **직전** 시점 전체를
담고 있다. `rollback_literacy_link_apply.py`로 복원하면(phase25/26이 검증한
대로 통째 파일 교체 방식) 이 신규 40건뿐 아니라 **이 백업 시점 이후의 모든
DB 변경이 함께 사라진다** - phase25/26이 확인한 것과 같은 구조적 한계다.
현재는 이 백업이 최신 쓰기 시점이므로 지금 복원하면 정확히 "신규 40건만"
사라지지만, 앞으로 phase29 이상에서 추가 쓰기가 생긴 뒤에는 이 백업으로
복원 시 그 사이 쓰기도 함께 유실된다.

```
python3 scripts/vocab/rollback_literacy_link_apply.py \
  --env-file /home/chsh82/aprolabs/.env \
  --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  --backup-path /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase28-l6-pilot-quiz-items-pre-migration-20260927-001152
(--confirm 추가 시 실제 복원. 기본은 dry-run)
```

## 6. 최종 보고

- **실제 적재 결과**: 신규 40건 삽입(MEANING_CHOICE 20 + CONTEXT_MEANING 20),
  스킵 0건, `source_version='schema_reading_l6_pilot_dryrun_v1'`
- **백업 경로**: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase28-l6-pilot-quiz-items-pre-migration-20260927-001152`
  (SHA-256 `71be6bc4f51ddb030722ed469533fcb767bccf46509ab03c5782ee65051db9df`)
- **복구 경로**: 위 5절 명령(이 백업 이후의 모든 변경이 함께 사라진다는 한계 포함)
- **최종 DB 상태**: `vocabulary_contents`/`vocabulary_content_levels` 5,902건
  (불변), `vocabulary_multiformat_items` **1,369건**(기존 1,329 + 신규 40),
  `student_exposure`/`public_ready` 합계 0/0
- **일반 출제 미노출 근거**: 4-1절 - 실제 라우터 함수 3종을 직접 호출해 검증
  (라우터 코드는 전혀 수정하지 않음)
- 학생 공개·push·배포: 없음

## 재실행 명령
```
# 1) 고정 체크섬·item_id·콘텐츠 재대조
python3 scripts/vocab/phase28_freeze_check.py --db-path <research db>

# 2) dry-run 검증 + rows-json 생성
python3 scripts/vocab/phase28_build_rows_from_phase27.py \
  --db-path <research db> \
  --items-json data/import/schema_reading_phase27_l6_pilot_items_20260927.json \
  --out /tmp/phase28_rows.json

# 3) 실제 적재(멱등)
python3 scripts/vocab/phase28_apply_l6_pilot_quiz_items.py \
  --env-file /home/chsh82/aprolabs/.env \
  --db-path /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
  --rows-json /tmp/phase28_rows.json

# 4) 독립 재검증(실제 라우터 함수 호출 포함)
PYTHONPATH=<repo root> VOCABULARY_QUIZ_DB_PATH=<research db> \
  python3 tests/test_phase28_l6_pilot_quiz_items_applied.py
```

## 다음 단계로 넘길 사항
- 이 40문항을 관리자 화면에서 실제로 재생하려면 phase19가 만든 "L4·L5
  파일럿" 모드와 같은 방식으로 `app/vocabulary_quiz/routers/multiformat.py`에
  L6 파일럿 화이트리스트·매니페스트·전용 엔드포인트를 추가하는 별도 단계가
  필요하다(이번 단계에서는 하지 않음, 지시 준수).
- S 13건 전문가 검수, 잔여 caution 보강(개략/총합/합리적은 phase26에서 이미
  반영), 잔여 콘텐츠 확장 배치는 여전히 대기 상태.
