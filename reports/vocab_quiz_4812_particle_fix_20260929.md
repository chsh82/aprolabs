# '집단'(SR_L4CORE_4812) 조사 불일치 수정 + 재검토 준비

- 일자: 2026-09-29
- 운영 DB 쓰기: aprolabs 연구 DB에 **정확히 1개 필드, 1행**만 수정(아래
  1절). momolib 운영 DB는 전혀 건드리지 않음(읽기 전용 확인만).
- aprolabs 공개 플래그(`student_exposure`/`public_ready`)는 전 구간
  0건 유지, 사용자 대신 승인후보 판정을 저장하지 않음.

## 1. 문항 수정

**대상**: `SR_L4CORE_4812` 연결 문항 `MF_A_SC_SRL4L5PILOT_20260925_L4_003`의
`explanation` 필드, 정확히 "무리'이라는" → "무리'라는" 한 곳만.

- **원천 생성 스크립트 확인**: `scripts/vocab/phase16_build_quiz_pilot_dryrun.py`의
  `ira_neun()`(받침 유무 기반 이라는/라는 선택 헬퍼)을 직접 추적한
  결과, "무리"(받침 없음)에 대해 이미 올바르게 "라는"을 계산하도록
  구현돼 있음을 확인 — **생성 스크립트 로직 자체는 이미 정상**.
- **매니페스트 재생성 규칙 확인**: 정적 스냅샷
  `data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.{csv,jsonl}`
  (2026-09-26 생성, 즉 `ira_neun()` 수정 이전 스냅샷)에는 옛 오류
  텍스트가 그대로 남아 있었음. 적용 스크립트
  `scripts/vocab/phase18_apply_quiz_pilot.py`의 **GATE 4(멱등성
  사전 점검)** 코드를 직접 확인한 결과 **이미 존재하는 item_id는
  무조건 스킵하는 순수 INSERT-only**(UPDATE/UPSERT 경로 자체가 코드에
  없음) — 즉 **이 매니페스트로 재실행해도 지금 적용할 수정을 덮어쓸
  위험이 없음**을 코드로 확인했다. 다만 소스 일관성을 위해 같은 1곳만
  매니페스트 CSV/JSONL에도 동일하게 반영(바이너리 안전 치환으로 개행
  문자·인코딩 변경 없이 딱 1줄만 diff되는 것을 확인).
- **SQLite Backup API 백업**: `sqlite3.Connection.backup()`으로 실제
  백업 파일 생성 — `~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak_particle_fix_20260928-163805`(서버 보존).
- **기존값 일치 검사**: UPDATE 전 `explanation`이 기대한 정확한 원문
  ("...무리'이라는 뜻입니다.")과 **완전히 일치할 때만** 진행하도록
  가드, `WHERE item_id=? AND explanation=?`로 UPDATE해 rowcount=1
  아니면 자동 중단하도록 작성.
- **적용 결과**: `UPDATE_OK: rowcount=1`. 적용 전후 대조 —
  `correct_option`(3)·`options_json`·`prompt` 전부 불변, 같은
  콘텐츠의 다른 연결 문항(`MF_C_SC_SRL4L5PILOT_20260925_L4_003`) 불변,
  `vocabulary_contents`/`vocabulary_multiformat_items` 전체 행 수
  불변(각 5950건/1369건, 수정 전후 동일), `SR_L4CORE_4812`의
  `student_exposure`/`public_ready` 여전히 `(0, 0)`.

## 2. 수정 후 재대조

| 값 | aprolabs(수정 후) | momolib(현재) |
|---|---|---|
| explanation | `...무리'라는 뜻입니다.` | `...무리'라는 뜻입니다.` |
| prompt/options_json/correct_option | 동일 | 동일 |
| **정규화 필드셋 해시**(prompt+options_json+correct_option+explanation, SHA-256) | `0db6ec37...4c15fdc1` | `0db6ec37...4c15fdc1` |

**완전히 일치.** 게이트 스크립트(`vocab_quiz_promote_gate.py`)로도
재확인 — 나머지 39건 필드 불일치 0건 유지.

**판정 이력(append-only, 둘 다 보존됨)**:
```
id=48 verdict=NEEDS_FIX          reviewed_at=2026-09-28 15:16:33  stale=True
id=14 verdict=APPROVED_CANDIDATE reviewed_at=2026-09-28 06:39:18  stale=True
```
문항 내용이 바뀌었으므로 **기존 두 판정 모두 자동으로 stale로
표시**된다(기존 `review_is_stale()` 메커니즘 그대로 재사용, 새 코드
없음) — 상세 화면에 "이 판정 이후 콘텐츠 또는 연결 문항이 수정되어
현재 버전에는 더 이상 유효하지 않습니다. 다시 검토해 주세요."가 뜬다.
**이것이 곧 "새 문항 버전은 아직 승인되지 않은 상태"의 표시**이며,
승인후보 판정은 내가 대신 저장하지 않았다.

게이트 재실행 결과(실제 운영 데이터):
```
aprolabs tier1 전체: 40건, 유효 판정: 39건, 차단 1건
  BLOCKED SR_L4CORE_4812: 판정이 승인후보 아님(NEEDS_FIX)
필드 불일치 콘텐츠: 0건 / 필드 불일치 문항: 0건
=== 승격 dry-run 게이트: FAIL (39/40 유효, 불일치 0건) ===
```
데이터는 이제 momolib과 완전히 일치하지만, **판정 자체가 아직
승인후보가 아니므로(NEEDS_FIX + stale) 게이트는 여전히 FAIL** —
데이터 정합성과 판정 승인은 서로 독립적으로 작동함을 재확인.

## 3. 재검토 화면 URL

`SR_L4CORE_4812`('집단') 1건을 다시 확인·재판정할 수 있는 정확한 URL:

**https://aprolabs.co.kr/vocab-publish-review/SR_L4CORE_4812**

(super_admin 로그인 필요. 목록 화면은
`https://aprolabs.co.kr/vocab-publish-review/`) 이 화면에서 stale
경고와 현재(수정된) 문항 텍스트, 판정 이력 2건을 모두 확인할 수 있다.
**이번 작업에서 승인후보 판정을 대신 저장하지 않았으므로, 실제
재승인 여부는 관리자가 직접 이 화면에서 결정해야 한다.**

## 4. 최종 재확인

| 항목 | 값 |
|---|---|
| aprolabs 39건 판정(SR_L4CORE_4812 제외) | distinct content_id 39건, 전부 불변 |
| aprolabs `student_exposure`/`public_ready=True`인 콘텐츠 | 0건(전체 5950건 기준) |
| momolib `vocab_quiz_contents` | 227(불변) |
| momolib `vocab_quiz_pilot_items` | 80(불변) |
| momolib `bank_questions` | 411(불변) |
| momolib `users` | 6(불변) |
| momolib `vocab_quiz_admin_reviews` | 0(불변) |
| momolib 공개 플래그 True인 콘텐츠 | 0건 |
| momolib `VOCAB_QUIZ_STUDENT_ENABLED` | OFF |
| momolib 레벨 선택 코드 | `feat/vocab-quiz-close-rd-screens` 브랜치 커밋 `3416d9a` — **로컬에만 존재**(`origin/main`=`16bf9a7`과 비교해 재확인, push 안 함), 학생 기능 활성화 안 함 |
