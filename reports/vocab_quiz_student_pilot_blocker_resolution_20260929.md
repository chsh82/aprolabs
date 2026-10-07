# 어휘 퀴즈 학생 파일럿 공개 전 남은 차단 조건 해결

- 일자: 2026-09-29
- 운영 DB·공개 플래그 변경 없음(요청대로) — 전부 읽기 전용 조사, 실제
  기록이 필요한 부분은 aprolabs 판정 화면 기능(save_review) 자체를
  정상적으로 1건만 사용, momolib/aprolabs 격리 사본 리허설, 로컬 커밋만
  수행.

## 1. SR_L4CORE_4812 불일치 필드 단위 조사

대상: 콘텐츠 `SR_L4CORE_4812`('집단') 연결 문항
`MF_A_SC_SRL4L5PILOT_20260925_L4_003`의 `explanation` 필드.

| 출처 | 값 | item_hash |
|---|---|---|
| aprolabs 현재 DB | `...무리'이라는 뜻입니다.` | `fe62a8a2...1b767` |
| 승인 당시(2026-09-28 06:39:18) 저장 해시 | 동일 | `fe62a8a2...1b767` |
| export 파일(2026-09-28 고정) | `...무리'이라는 뜻입니다.` | `fe62a8a2...1b767` |
| momolib 현재 DB | `...무리'라는 뜻입니다.` | (momolib은 별도 컬럼 `item_hash` 사용, 텍스트 자체가 다름) |

**aprolabs 현재 DB = 승인 당시 해시 = export 파일 — 셋 다 완전히 같다
(미수정 원문).** aprolabs 자체 신선도(stale) 검사는 "판정 이후 콘텐츠가
바뀌었는가"만 보므로 이 값들이 일치하는 한 통과한다 — 이번 케이스가
바로 **그 검사로는 못 잡는 유형**이다. 오직 momolib만 다르다(수정본).

**조사 수정 이력**: momolib 커밋 `2c84a044`(2026-09-28 05:38:15, "어휘
퀴즈 파일럿 조사 오류 수정")에서 이 문항의 `explanation`을 "무리'이라는"
→ "무리'라는"으로 수정(받침 없는 명사 뒤 조사 규칙, 뜻풀이·정답·공개
상태는 불변)하고 80문항 전수 조사 규칙 회귀 테스트를 추가했다.
**aprolabs의 `SR_L4CORE_4812` 승인(06:39:18)은 momolib이 이미 고친
뒤(05:38:15)에 이루어졌지만, aprolabs 쪽 원문은 수정되지 않은 채로
그대로 승인되었다** — 즉 검수 화면이 보여준 텍스트와 실제 학생에게
나갈 momolib의 텍스트가 서로 다른 상태에서 승인된 것.

**조치**: 이 콘텐츠 1건만 aprolabs의 실제 판정 저장 기능
(`app/vocabulary_quiz/publish_review.py`의 `save_review()`, 검수
화면이 쓰는 것과 동일한 코드 경로)으로 `NEEDS_FIX`(수정필요)를
새로 기록했다(id=48, `reviewer_user_id=system:vocab_quiz_promote_gate_20260929`
— 사람 관리자를 사칭하지 않도록 구분되는 식별자 사용, rationale에
위 근거 전문 기재). **append-only라 기존 `APPROVED_CANDIDATE`(id=14)
판정도 이력에 그대로 남는다.** 나머지 39건의 최신 판정은 전혀
건드리지 않음(재확인 완료).

## 2. `/start` vocab_level(4/5/6) 명시 선택 구현(momolib)

- `app/vocab_quiz/eligibility.py`: `eligible_content_ids_by_level()`/
  `item_is_eligible_at_level()`/`eligible_pilot_items_by_level()` 추가
  — 기존 단일 게이트(`eligible_content_ids`) 결과를 레벨로 한 번 더
  좁히기만 한다(게이트 자체를 대체하지 않음 → 레벨 값을 조작해도
  게이트 미통과 콘텐츠는 교집합 밖).
- `/practice/vocab-quiz/start` 동작 정의:

| 입력 | 응답 |
|---|---|
| `vocab_level` 자체 없음 | 400 `VOCAB_LEVEL_REQUIRED` |
| `vocab_level`이 4/5/6이 아님(문자열·음수·범위밖·None) | 400 `INVALID_VOCAB_LEVEL` |
| 유효한 레벨이지만 해당 레벨 공개 문항 0건 | 409 `NO_ELIGIBLE_ITEMS` |
| 유효한 레벨 + 문항 있음(10개 미만 포함) | 200, `min(10, 가능문항수)`개로 세션 생성, `vocab_level` 컬럼에 저장 |

- 화면 라벨: L4=중3, L5=고1, L6=고2~3(레벨 선택 버튼 UI 추가, 레벨별
  공개 문항 수 표시).
- 마이그레이션 `8a1c2f4e6b9d`: `vocab_quiz_student_sessions.vocab_level`
  컬럼 추가(nullable, 기존 세션 영향 없음).
- 테스트: 신규 `tests/test_vocab_quiz_student_level_selection.py`
  **33/33 PASS**(누락/범위밖값 7종/문항부족/레벨별 미혼입/비공개 콘텐츠
  절대 미노출 포함) + 기존 `tests/test_vocab_quiz_student_gate.py`
  **123/123 PASS**(회귀 없음) + `tests/test_vocab_quiz_student_integration.py`
  API 계약 변경분 반영 후 **18/18 PASS**.

## 3. aprolabs 40건 판정 ↔ momolib 자동 대조 게이트

`aprolabs/scripts/vocab/vocab_quiz_promote_gate.py`(신규, 로컬 커밋만) —
3모드 전부 읽기 전용:

- `--export-aprolabs`: aprolabs에서 유효 판정(승인후보 + non-stale)만
  골라 고정 export. 유효하지 않은 항목은 사유와 함께 `blocked`에 담아
  **절대 자동 무시하지 않는다.**
- `--export-momolib --ids`: momolib에서 지정 ID의 현재 필드값만 조회
  (admin_reviews 테이블에 쓰지 않고, 닫힌 공개검토 블루프린트도
  건드리지 않음).
- `--compare`: 두 export를 필드 단위로 대조해 게이트 PASS/FAIL 산출.

**실행 결과(실제 운영 데이터, 2026-09-29)**:
```
aprolabs tier1 전체: 40건, 유효 판정: 39건, 차단 1건
  BLOCKED SR_L4CORE_4812(집단): 판정이 승인후보 아님(NEEDS_FIX)
momolib에 없는 콘텐츠: []  /  momolib에 없는 문항: []
필드 불일치 콘텐츠: 0건  /  필드 불일치 문항: 0건
=== 승격 dry-run 게이트: FAIL (39/40 유효, 불일치 0건) ===
```
**1건 불일치(정확히는 "재검토 대기")가 해결되기 전에는 PASS를 내지
않음을 실제로 확인**(exit code 1). SR_L4CORE_4812가 재검토 후
다시 승인후보로 판정되고 stale이 아니면, 이 게이트를 다시 돌리는
것만으로 자동으로 40/40 PASS가 된다(코드 수정 불필요).

## 4. 격리 사본 최종 재검증

운영 백업을 새로 떠서(SHA-256 `52c5e0d4...c28bfe`, 서버 보존) 별도
로컬 PostgreSQL에 실제 복원(`bank_questions=411`,
`vocab_quiz_contents=227`, `vocab_quiz_pilot_items=80`, `users=6` 전부
운영과 일치) 후, 새 마이그레이션(`8a1c2f4e6b9d`)을 구 코드 기준
운영과 동일한 head(`4d83374ccc5f`)에서 적용 — 정상 적용 확인.

- **게이트 통과 39건만 승격**: eligibility 조건 기반 156건 필드 변경을
  게이트가 승인한 39건에만 적용(공개 플래그 78건 + 레벨 확정 78건).
  적용 후 `eligible_content_ids()`=39, `eligible_pilot_item_count()`=78,
  **SR_L4CORE_4812는 eligible 집합에 없음**(게이트가 차단했으므로
  momolib 쪽 데이터 자체는 이미 정상임에도 승격 대상에서 제외됨 —
  판정 기반 게이트가 데이터 정합성과 독립적으로 작동함을 실증) —
  이 콘텐츠를 포함한 **188건(187 + SR_L4CORE_4812) 완전 불변** 재확인.
- **레벨별 출제·채점 E2E(실제 데이터)**: L4/L5/L6 각각 40회씩 반복
  요청 → 각 레벨의 eligible 문항(20/20/38건) **전량 커버**, 다른
  레벨·차단된 콘텐츠(SR_L4CORE_4812의 문항 2건)는 **120회 중 단 한
  번도 등장하지 않음**. 세션 1건을 실제로 끝까지 풀어 채점·결과
  화면까지 확인. **132/132 PASS.**
- **롤백 재시험**: 156건을 정확히 원래 값으로 되돌리는 UPDATE 실행 →
  `eligible_content_ids()`=0으로 복귀, 학생 세션/응답 기록은 보존.
  **PASS.**
- **폐기**: PostgreSQL 인스턴스 정지 + 데이터 디렉터리·바이너리 전체
  삭제, 로컬 백업 사본 삭제(서버 원본은 보존).

## 5. 최종 운영 재확인(읽기 전용)

| 항목 | 값 |
|---|---|
| `vocab_quiz_contents` | 227(불변) |
| `vocab_quiz_pilot_items` | 80(불변) |
| `bank_questions` | 411(불변) |
| `users` | 6(불변) |
| `vocab_quiz_admin_reviews` | 0(불변 — momolib 판정 테이블 안 건드림) |
| 공개 플래그 True인 콘텐츠 | 0건 |
| `VOCAB_QUIZ_STUDENT_ENABLED` | OFF |
| aprolabs `vocabulary_publish_reviews` | SR_L4CORE_4812에 NEEDS_FIX 1건 append(요청대로 이 콘텐츠만), 나머지 39건 불변 |

## 6. 코드 상태(로컬 커밋만, main 병합·push 없음)

- momolib `feat/vocab-quiz-close-rd-screens` 브랜치, 커밋 `3416d9a`
  (7개 파일 — eligibility.py/routes.py/모델/템플릿/마이그레이션/
  테스트 2개). 이 브랜치는 R&D 화면이 이미 닫힌 상태라 운영 배포는
  없음(요청대로 활성화 안 함).
- aprolabs `main` 브랜치(로컬), 커밋 `64219d9`
  (`scripts/vocab/vocab_quiz_promote_gate.py` 1개 파일). push 안 함.

## 7. 남은 사항

- SR_L4CORE_4812는 재검토 대상으로 표시만 됐을 뿐, 실제로 momolib
  최신본에 맞춰 aprolabs 원문을 수정할지, 혹은 momolib 수정 내용을
  검토해 그대로 승인할지는 **사람 관리자의 판단이 필요**하다(이
  스크립트는 그 판단을 대신하지 않았다).
- 재승인 후에는 `vocab_quiz_promote_gate.py --compare`를 다시 돌리는
  것만으로 40/40 PASS 여부를 즉시 재확인할 수 있다.
- momolib `/start`는 이제 `vocab_level` 파라미터가 **필수**로
  바뀌었다(이전 파라미터 없는 호출은 더 이상 지원 안 함) — 아직
  운영 미배포이므로 하위 호환 영향 없음.
