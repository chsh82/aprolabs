# 어휘 퀴즈 학생 파일럿 준비 + DB 확장 기준선 조사

- 일자: 2026-09-29
- **실제 학생 allowlist 등록·40건 공개 승격·feature flag ON은 이번에
  전혀 실행하지 않음**(요청대로) — 대상 학생·레벨이 정해진 뒤 한 번의
  최종 결정으로 진행.

---

## A. 학생 파일럿 준비

### A1. 진입 흐름 — 메뉴 → 레벨선택 → 10문항 → 결과

**문제 발견**: 기존 학생 하단 탭(`student_base.html`)에 어휘 퀴즈로
가는 링크가 **전혀 없었다** — `VOCAB_QUIZ_STUDENT_ENABLED`와
allowlist가 둘 다 통과돼도 `/practice/vocab-quiz/`를 직접 입력해야만
접근 가능한 상태였음.

**조치**: `app/__init__.py`에 `context_processor`를 추가해
`vocab_quiz_pilot_allowed`(feature flag ON + 그 학생이 allowlist에
있을 때만 True)를 전역 템플릿 변수로 주입하고, `student_base.html`
하단 탭에 "📖 어휘" 항목을 **조건부로** 추가했다. 허용 안 된
학생·비로그인·flag OFF에는 탭 자체가 렌더되지 않는다(라우트 쪽
404/403 게이트와 별개로 메뉴도 숨김 — 이중 방어).

**흐름 검증**(격리 테스트, 14/14 PASS): 허용 학생 화면에 탭 링크 실제
렌더 확인 → `index`(레벨별 남은 문항 수 표시, 허용된 레벨만 노출) →
`start`(레벨 선택) → `take`(10문항, 또는 있는 만큼) → `answer` ×N →
`complete` → `result` — 전부 같은 `student_base.html`을 확장하므로
페이지 간 이동이 자연스럽게 이어짐.

### A2. 엣지 케이스 확인 결과

| 케이스 | 동작(확인됨) |
|---|---|
| 지정 레벨 외 문항 요청 | `student_allowed_levels()`에 없는 레벨 → 403 `LEVEL_NOT_ALLOWED`(기존 검증 유지) |
| **이미 푼 문항의 반복** | **의도된 동작으로 확인**: `/start`는 세션 간 중복 제거를 하지 않는다. 문항 풀이 `SESSION_ITEM_COUNT(10)`보다 작으면(현재 L4/L5=20개, L6=40개) 몇 세션만 반복해도 같은 문항이 다시 나온다. 격리 테스트로 문항 2개짜리 레벨에서 3회 연속 완전히 동일한 2문항이 나옴을 재현 확인. |
| **중도 이탈 → 재접속** | **정상 동작 확인**: 세션은 `in_progress` 상태로 DB에 남고, 같은 `session_id`로 다시 `GET`하면 이미 답한 문항은 `answered=True`/`selected_option`/`is_correct`가 그대로 복원되고 나머지만 이어서 풀 수 있다. 이미 답한 문항을 다시 제출해도 새 행이 아니라 기존 응답만 갱신(중복 없음). |
| **문항 부족**(전체 10개 미만) | 있는 만큼만(`min(10, 가능 문항수)`) 세션 생성 — 3개짜리 레벨에서 실제로 3문항 세션이 만들어짐을 확인. 0개면 409 `NO_ELIGIBLE_ITEMS`(기존 검증 유지). |

### 운영 중 집계 항목(정답 노출 없음)

기존 DB 테이블에 이미 있는 것과, 이번에 추가한 로그를 합쳐 아래를
그대로 운영 집계에 쓸 수 있다:

| 항목 | 소스 | 비고 |
|---|---|---|
| 세션 시작/완료 시각·개수 | `vocab_quiz_student_sessions`(started_at/completed_at/status) | 이미 저장됨 |
| 레벨별 응시 수 | `vocab_quiz_student_sessions.vocab_level` | 이미 저장됨 |
| 정오답·문항 ID | `vocab_quiz_student_attempts`(item_id/selected_option/is_correct) | 이미 저장됨, 정답은 제출 후에만 기록 |
| 시작/완료 이벤트 로그 | `app.logger.info('[vocab_quiz_student] event=session_start ...')` / `session_complete` | **이번에 신규 추가**(user_id·session_id·vocab_level·item_count만, 정답 없음) |
| 오류(레벨 없음/범위밖/미허용/문항부족) | `event=start_error error=...` | **신규 추가** |
| allowlist 차단 | `event=access_denied reason=NOT_IN_ALLOWLIST` | **신규 추가** |

문항별 정오답은 로그에 중복 기록하지 않고 DB 테이블만 참조하도록
설계(로그는 세션 단위 이벤트 + 오류만).

### A3. 운영 상태 유지 + 배포

- 누적 diff 검토: `origin/main`(`5de56c9`) 대비 5개 파일, **마이그레이션
  없음**(코드만) — 컨텍스트 프로세서 1개, 템플릿 조건부 탭 1개, 로그
  호출 5곳, 신규 테스트 1개.
- 격리 테스트 전부 통과 후(기존 4개 스위트 182/182 + 신규 14/14 =
  **196/196**) 배포 진행: 커밋 `db34921`, `feat/vocab-quiz-close-rd-
  screens:main`으로 push → 자동배포 성공(서비스 21:49:21 재시작).
- **배포 후 재확인**: `GET /practice/vocab-quiz/` → **404**(변화 없음,
  flag 여전히 OFF), `GET /` → 200. 읽기 전용 재확인: 공개 플래그 True
  콘텐츠 **0건**, `vocab_quiz_pilot_allowlist` **0건**, feature flag
  **OFF**.

---

## B. DB 확장 기준선(aprolabs 연구 DB, 읽기 전용)

### B4. L0~L6 현황(기존 5,723건 vs 신규 L4~L6 227건 구분)

두 그룹은 `source_version`으로 명확히 구분됨: 기존
5,723건=`2.1.29`(구 파이프라인), 신규 227건=`schema_reading_
literacy_l4/l5/l6_manual_v1/v2` + `schema_reading_l6_evidence_
grounded_v1` 5개 배치. 합계 5,950건 = 5,723 + 227 정확히 일치.

| 그룹 | 레벨 | 콘텐츠 수 | 연결 문항 수 | 유형별 | 검수 상태 | 문항 없는 콘텐츠 |
|---|---|---|---|---|---|---|
| 기존 | L0 | 612 | 121 | MC 32·WFD 32·CM 32·CC 25 | READY 586 / CANDIDATE 26 | **580건(94.8%)** |
| 기존 | L1 | 1,635 | 316 | MC 87·WFD 87·CM 87·CC 55 | READY 437 / CANDIDATE 1,198 | **1,548건(94.7%)** |
| 기존 | L2 | 1,621 | 332 | MC 88·WFD 88·CM 88·CC 68 | READY 421 / CANDIDATE 1,200 | **1,533건(94.6%)** |
| 기존 | L3 | 1,825 | 337 | MC 91·WFD 90·CM 91·CC 65 | READY 421 / CANDIDATE 1,404 | **1,734건(95.0%)** |
| 기존 | L4 | 30 | 8 | MC 2·WFD 2·CM 2·CC 2 | READY 2 / CANDIDATE 28 | **28건(93.3%)** |
| 기존 | L5 | **0** | - | - | - | - |
| 기존 | L6 | **0** | - | - | - | - |
| 신규 | L4 | 49 | 20 | MC 10·CM 10 | (신규 배치, 별도 필드 없음) | **39건(79.6%)** |
| 신규 | L5 | 48 | 20 | MC 10·CM 10 | 〃 | **38건(79.2%)** |
| 신규 | L6 | 130 | 40 | MC 20·CM 20 | 〃 | **110건(84.6%)** |

(MC=MEANING_CHOICE, WFD=WORD_FROM_DEFINITION, CM=CONTEXT_MEANING,
CC=CONTEXT_CLOZE. hold_reason 설정된 콘텐츠는 전체 0건.)

**핵심 관찰**:
1. **기존 5,723건 코퍼스는 L5·L6이 전혀 없다.** L4도 30건뿐. 즉 L5·L6
   확장은 기존 DB를 아무리 뒤져도 나오지 않고 **신규 생성만 가능**.
2. 모든 레벨에서 "콘텐츠는 있지만 출제할 문항이 없는" 비율이
   80~95%로 압도적으로 높다 — 콘텐츠(뜻풀이) 자체는 풍부하지만
   문항 제작이 병목.
3. 신규 227건은 `generation_status` 필드 자체를 안 쓰고(전부
   "없음"), 레벨은 전부 `REVIEW_BOUNDARY`/`boundary_flag=1` 상태 —
   40건 파일럿 승격 때 이미 확인된 사실과 일치(다른 187건도 전부
   레벨 재확인이 필요한 상태라는 뜻).

### B5. 다음 비공개 이식 후보(뜻 단위, momolib 227건·승인 40건 제외)

기존 5,723건 중 **L4에 해당하는 30건**을 뜻(lemma) 단위로 신규 L4
배치 49건과 전수 대조 — **중복 0건**(같은 lemma 없음, 안전).

이 30건 중 **문항까지 이미 존재하는 것은 2건뿐**: `물리학`
(SC_V1955_B055_030), `은유법`(SC_V1998_B098_049). 각각 4가지 유형
(MEANING_CHOICE/WORD_FROM_DEFINITION/CONTEXT_MEANING/CONTEXT_CLOZE)의
문항이 있으나, **레벨 표기만으로 판단하지 않고 실제 확인한 결과**:

- MEANING_CHOICE + CONTEXT_MEANING 2종은 현재 momolib 학생 엔진의
  객관식(`selected_option` 1~4) 채점 방식과 그대로 호환 — **즉시
  재사용 가능**(단, 레벨 확정·관리자 재검토는 필요).
- WORD_FROM_DEFINITION(단어 직접 입력)·CONTEXT_CLOZE(빈칸 직접
  입력) 2종은 **momolib 학생 라우트가 아직 지원하지 않는 입력
  방식**(현재 `/answer`는 정수 선택지만 받음) — 콘텐츠·문항 텍스트는
  있어도 **바로 출제 불가**, 별도 UI/채점 로직 개발이 선행돼야 함.

나머지 28건(전부 문항 없음)은 뜻풀이 품질도 들쭉날쭉했다 —
`보조선`·`보험증`·`생산비`·`송전선` 등은 **student_definition이
canonical_definition을 거의 그대로 복사**(학생용 순화가 안 됨), 한자
병기가 그대로 남은 것도 있음(`암각화(칠하기...)`, `단애(斷崖)`) →
**"레벨 표기만으로 출제·공개 가능이라 판정하지 말라"는 지시대로,
문항 제작 이전에 뜻풀이 자체의 재작성이 먼저 필요한 그룹으로 별도
분류**(B6).

### B6. 파일럿 이후 확장 우선순위(목표 건수 임의 설정 없음)

**반복 가능성 위험도(현재 승인 40건 기준)**: L4=콘텐츠 10·문항
20건, L5=콘텐츠 10·문항 20건, L6=콘텐츠 20·문항 40건. 한 세션 10문항
고정이라 **L4·L5는 2세션이면 전체 문항을 다 보고, 3세션째부터 반드시
반복**된다. L6은 4세션 정도의 여유가 있지만 그래도 금방 소진.
**→ 확장 시급도: L4 = L5 > L6.**

| 구분 | 대상 | 근거 |
|---|---|---|
| **① 바로 재사용 가능**(레벨 확정·관리자 재검토만 필요, 콘텐츠·문항 텍스트는 이미 있음) | 기존 L4 `물리학`·`은유법` 2건(MEANING_CHOICE+CONTEXT_MEANING만) | 문항 텍스트 존재, 현재 엔진과 형식 호환, lemma 중복 없음 확인 |
| **② 문항 제작 필요**(뜻풀이·레벨은 이미 쓸만하지만 문항이 없음) | 신규 배치 중 문항 없는 187건(L4 39·L5 38·L6 110) 우선, 그중에서도 **L4·L5가 최우선**(반복 위험 큼) | `generation_status` 없음이지만 뜻풀이 자체는 신규 배치 품질 기준으로 만들어져 있음(파일럿 40건과 동일 배치) |
| **③ 뜻·레벨 검토 필요**(문항 제작 이전에 사람 검토가 먼저) | 기존 L4의 나머지 28건 중 student_definition이 canonical을 그대로 복사했거나 한자 병기가 남은 항목들, 그리고 신규·기존 불문 전체 `REVIEW_BOUNDARY` 콘텐츠(레벨 확정 자체가 안 됨) | 자동 생성 품질이 고르지 않음을 실제로 확인(예시 위 B5) — 자동 지표만으로 공개 가능 여부를 판정하지 말라는 지시와 일치 |
| **④ 구조적으로 이번 경로에서 확장 불가** | 기존 코퍼스의 L5·L6(0건) | 기존 DB에 아예 없음 — 신규 생성 파이프라인(phase16~28 계열)을 다시 돌리는 수밖에 없음 |
| **⑤ 별도 과제**(문항 제작과 무관) | WORD_FROM_DEFINITION/CONTEXT_CLOZE 두 유형을 momolib 학생 엔진이 받을 수 있게 만드는 작업 | 이걸 안 하면 ①의 물리학·은유법도 반쪽짜리(2/4유형)만 재사용 가능 |

---

## C. 최종 산출

### C1. 학생 파일럿 운영 절차(요약)

1. **[대상 입력 자리]** 대상 학생 user_id 목록과 학생별 허용 레벨을
   결정한다 — 예: `{"user_id": "...", "levels": [4, 5]}` 형태로
   `vocab_quiz_pilot_allowlist`에 학생당 1행씩 등록
   (`allowed_levels_json`).
2. **[대상 입력 자리]** 공개할 콘텐츠 범위 결정 — 현재 게이트 통과
   40건(콘텐츠)/80건(문항) 기준, 필요시 `vocab_quiz_promote_gate.py
   --compare`로 재검증 후 승격(160개 필드).
3. 위 두 가지가 확정된 뒤, **한 번의 결정**으로 40건 승격 →
   allowlist 등록 → `VOCAB_QUIZ_STUDENT_ENABLED=true` → 서비스
   재시작 순서로 적용(이번 세션에서 이미 리허설 2회 실증).

### C2. 모니터링 방법

- 실시간: 서버 로그에서 `grep '\[vocab_quiz_student\]'` — 이벤트별
  카운트(`event=session_start`, `session_complete`, `start_error`,
  `access_denied`)를 시간대별로 집계.
- 집계: `vocab_quiz_student_sessions`/`_attempts` 테이블을 직접
  조회 — 레벨별 응시 수, 정답률, 완료율(`status='completed'` 비율),
  세션당 소요 시간(`completed_at - started_at`).
- 이상 감지: `event=start_error error=NO_ELIGIBLE_ITEMS`가 자주
  찍히면 그 레벨 문항 풀이 부족하다는 신호(B6의 확장 우선순위와
  직결).

### C3. 중단·복구 절차(이번 세션에서 2회 실증됨)

1. `.env`에서 `VOCAB_QUIZ_STUDENT_ENABLED` 줄 제거 → 서비스 재시작 →
   `/practice/vocab-quiz/` 404 확인.
2. 40건 승격 때 적용한 160개 필드를 정확히 원래 값
   (`student_exposure/public_ready=False`,
   `level_status=REVIEW_BOUNDARY`, `boundary_flag=True`)으로 원복 →
   `eligible_content_ids()=0` 확인.
3. allowlist 테이블에서 파일럿 대상 행 삭제(학생 세션/응답 기록은
   FK 순서대로 정리하거나, 기록을 남기고 싶으면 계정만
   `is_active=False`로 비활성화하는 대안도 가능).
4. 최종 재확인: 공개 플래그 0건, `eligible` 0/0, allowlist 0건, flag
   OFF, 운영 핵심 수치(`vocab_quiz_contents=227`,
   `pilot_items=80`, `bank_questions=411`) 불변.

### C4. L0~L6 DB 현황표

B4 표 그대로(위 참고) — 요약: 기존 코퍼스는 L0~L4에 집중(L4는
희소), 신규 배치가 L4~L6 전담. 전 레벨에서 "문항 없는 콘텐츠"가
80~95%.

### C5. 다음 배치 제안(우선순위 순, 목표 건수 임의 설정 안 함)

1. **①(즉시)**: 기존 L4 `물리학`·`은유법`의 MEANING_CHOICE/
   CONTEXT_MEANING 재검토·레벨 확정 — 가장 빠른 win, 단 2건뿐이라
   근본적 해결은 아님.
2. **②(핵심)**: 신규 배치 중 문항 없는 L4 39건·L5 38건을 **최우선**
   문항 제작 대상으로(반복 위험이 가장 큼), L6 110건은 그다음.
3. **③(선행 작업)**: 기존 L4 나머지 28건은 문항 제작 전에 뜻풀이
   재검토(학생용 순화·한자 제거) 먼저.
4. **④(별도 트랙)**: L5·L6 확장을 기존 DB에서 더 찾을 수 없으므로,
   필요하면 phase16~28류 신규 생성 파이프라인을 다시 돌려야 함(이번
   조사 범위 밖).
5. **⑤(플랫폼 과제)**: WORD_FROM_DEFINITION/CONTEXT_CLOZE 입력형
   문항을 momolib 학생 엔진이 받을 수 있도록 확장하면, 기존 코퍼스의
   문항 자산을 더 넓게 재사용할 길이 열림(당장 필요는 아님).
