# L0~L3 신규 초안 92어휘·184문항 R&D 검증

- 일자: 2026-09-29
- 범위: **aprolabs 연구 DB만**. momolib DB 적재·실제 학생 노출·공개
  플래그 변경은 전혀 하지 않음(요청대로). 관리자 화면 검증은 모두
  admin 전용 경로에서만 수행.

## 1. 기존 보고서 용어 정정

`vocab_quiz_existing_l0l3_expansion_dryrun_20260929.md`를 수정 —
"즉시 출제 가능 596→780" 표현을 제거하고 **유형호환/재사용가능/
레벨보류/내용보류/신규초안** 5범주로 재정리, 상단에 "이 수치는 연구
DB 내부 분류이며 학생 공개 가능 수가 아니다"라는 경고를 명시했다.
합산 표("적용 시 합계")도 제거하고 범주별 개별 수치만 남겼다.

## 2. 신규 184문항 독립 의미 검사

생성 스크립트(`expand_existing_l0l3_quiz_dryrun.py`)를 **import하지
않는 완전히 별도 스크립트**로, DB에서 콘텐츠를 다시 조회해 재검증했다.

검사 항목: 정답 유일성(완전일치+포함관계 양쪽), 오답의 정답 텍스트
포함 관계(다른 뜻 가능성), 선택지 길이 이상치(정답이 눈에 띄게
길거나 짧은 경우), 조사(은/는·이라는/라는·을/를) 재계산(받침 규칙을
새로 구현해 대조), 문맥형 문항의 문장 완결성·괄호 표기, 학생용
뜻풀이 길이(레벨별 임계값: L0 20자/L1 26자/L2 34자/L3 40자, 독립적
설정)와 한자 포함 여부(연령 적합성).

| 레벨 | PASS | HOLD |
|---|---|---|
| L0 | 34 | 16 |
| L1 | 48 | 2 |
| L2 | 10 | 24 |
| L3 | 50 | 0 |
| 합계 | **142** | **42** |

- 콘텐츠 단위로는 71건 PASS(142문항), 21건 HOLD(42문항) — **한
  콘텐츠 안에서 PASS/HOLD가 갈린 경우는 0건**(레벨·정의 관련 이슈는
  두 문항 유형에 동일하게 적용됨을 확인).
- 주요 HOLD 사유: 학생용 뜻풀이 길이 초과(연령 적합성 재검토, 34건),
  정답 선택지 길이 이상치(눈대중 추측 위험, 3건), 한자 포함(2건).
  생성 스크립트 자체 검사(정답 유일성·오답 중복·문맥 표시)는 모두
  통과했던 항목들이라, **독립 검사가 생성 스크립트가 놓친 실제
  결함을 새로 찾아냈다.**

**L2 17어휘 레벨 근거**(전수 재확인): 전부 `level_status=
PROVISIONAL_AUTO`, `boundary_flag=0`, `target_grade_band=E5_6`,
`level_source=NIKL_GRADE_PLUS_DETERMINISTIC_RULES`,
`level_version=level_policy_v0.1`, `level_confidence=0.664~0.752`
— 레벨 근거 일관되고 불확실한 것 없음(HOLD된 것은 레벨이 아니라
뜻풀이 길이·한자 때문).

## 3. 중복 검사(328건 재사용 후보 vs 신규 184건)

| 대조 | content_id 중복 | item_id 중복 | lemma 중복 |
|---|---|---|---|
| 재사용가능 328건 | 0 | 0 | 0 |
| 레벨보류 207건 | 0 | 0 | - |
| 내용보류 61건 | 0 | 0 | - |

**전부 0건.** 신규초안은 "문항이 아예 없던 콘텐츠"에서만 뽑았으므로
구조적으로 겹치지 않으며, 이번 조사로 실제 확인했다. 레벨보류 207건과
내용보류 61건은 이번 적재 배치에 전혀 섞지 않았다(애초에 별도
소스이므로 섞일 수도 없음).

## 4. 검증 통과분만 비공개 적재(aprolabs 연구 DB)

**대상**: 독립 검사 PASS 142건(콘텐츠 71건) — HOLD 42건은 수량을
채우지 않고 그대로 제외.

| 게이트 | 결과 |
|---|---|
| APP_ENV | `research` 확인 |
| SQLite Backup API 백업 | `vocabulary_quiz_research.db.bak_l0l3_dryrun_apply_20260928-235610`(서버 보존) |
| 대상 ID 고정 | 입력 142건, source_version 전부 일치, item_id 중복 0 |
| 콘텐츠 사전 확인 | 71건 전부 존재·`is_active=1`·`student_exposure=0`·`public_ready=0`·HOLD 없음 |
| 멱등성 | 기존 존재 0건, 신규 삽입 142건 |
| 단일 트랜잭션 | 커밋 성공(예외 시 자동 롤백하도록 작성) |
| 무결성 | `integrity_check=ok`, `foreign_key_check` 위반 0건 |
| 기존 데이터 불변 | `vocabulary_contents` 5,950건 그대로, `vocabulary_multiformat_items` 1,369→**1,511**(+142, 기대치 정확히 일치) |
| 공개 상태 | 관련 콘텐츠 71건 `student_exposure`/`public_ready` **여전히 0** |

새 `source_version="schema_reading_existing_l0l3_dryrun_v1"`으로
MEANING_CHOICE 71 + CONTEXT_MEANING 71 = 142건 적재. `qa_flags_json`에
독립 검증 통과 근거와 오답 사유를 함께 기록해 추적 가능하게 했다.

## 5. 관리자 화면(레벨별 출제) 실제 검증

momolib 이식·학생 노출·공개 플래그 변경 없이, **아프로랩스 관리자
다유형 문항 화면이 실제로 쓰는 프로덕션 함수**(`_level_availability`/
`_select_level_candidates`/`_public_item_payload`/`_grade`/
`_correct_answer_payload`, `app/vocabulary_quiz/routers/multiformat.py`)를
그대로 불러와 이번 배치만 가리키도록 한 별도 스크립트로 검증했다
(운영 서버 프로세스는 전혀 건드리지 않음 - 이 스크립트 자신의 import
안에서만 유효).

| 레벨 | 가용 문항(단어 수) | 세션 생성 | 출제·채점 |
|---|---|---|---|
| L0(초1~2) | 34(17단어) | question_count=10 | 10/10 정확 채점 |
| L1(초3~4) | 48(24단어) | question_count=10 | 10/10 정확 채점 |
| L2(초5~6) | 10(5단어) | question_count=10 | 10/10 정확 채점 |
| L3(중1~2) | 50(25단어) | question_count=10 | 10/10 정확 채점 |

- 레벨별 후보가 **전부 이번 신규 배치 소스**임을 재확인(다른
  source_version 문항 섞임 0건 — 배치 격리 확인).
- 제출 전 payload에 `correct_option`/`answer_payload`/`explanation`
  **미노출**, 제출 후에만 정답 정보 제공 확인.
- 정답·오답을 절반씩 섞어 제출해 채점 로직 양방향 확인, 세션 완료
  처리(`correct_count`) 집계 일치.
- 실제 `vocabulary_multiformat_sessions`/`_responses`에 admin 계정
  기준 QA 세션 8건(4레벨×2회 실행)이 남아 기존 파일럿 온라인 QA와
  동일한 방식의 감사 기록이 됨.

**172/172 PASS.**

### 최종 상태(읽기 전용 재확인)
`vocabulary_contents=5,950`(불변), `vocabulary_multiformat_items=1,511`
(142 증가), 공개 플래그 True인 콘텐츠 **0건**, momolib은 이번 작업에서
전혀 접속하지 않음.

## 결론

신규초안 184건 중 독립 검증을 통과한 **142건(71어휘)만** 비공개로
적재했고, 관리자 화면 기준 실제 레벨별 출제·채점까지 정상 동작함을
확인했다. HOLD된 42건(21어휘)은 연령 적합성(뜻풀이 길이·한자) 재작성
후 별도 배치로 재시도해야 한다. 실제 momolib 이식·학생 공개는 이번
범위 밖이며 사람 검토 후 별도로 결정한다.
