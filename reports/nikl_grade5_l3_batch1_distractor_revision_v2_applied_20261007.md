# L3 중등 보강 1차 오답 개정 v2 - 관리자 재검수용 실제 반영 완료

- 일자: 2026-10-07
- 범위: 개정된 29어휘·58문항을 연구 DB에 실제로 반영하고, 관리자 검수
  화면에서 개정 여부·재검수 필요·보류를 구분해 보여주도록 했다. 벨기에
  2문항은 이번에도 제외·보류(완전 불변). RULE_A/B·학생 공개·momolib
  무관. 완료된 조사·분류기 실험은 반복하지 않음(API 호출 0건).

## 1. 적용 대상 고정 — 29어휘·58문항, 벨기에 제외

`scripts/vocab/finalize_grade5_l3_batch1_revision_v2.py`로 적용 대상을
고정했다: 신규 활성 문항 58건(29단어 × 2유형), 비활성화 대상(기존
문항) 58건, 변경 없음(벨기에) 2건. 기존 승인 이력(`vocabulary_
publish_reviews` 77행)과 원본 문항(비활성화된 58건)은 **삭제하지 않고
그대로 감사 자료로 DB에 보존**했다.

## 2. 과거 응시 영향 — 확인 후 새 버전 ID로 등록

**구조 확인(코드 직접 읽음)**: `VocabularyMultiformatResponse.item_id`는
`vocabulary_multiformat_items.item_id`를 **FK로 참조**하고,
`/sessions/{id}/result`는 세션에 저장된 스냅샷이 아니라 **그 FK로
문항을 매번 다시 읽어** 프롬프트·정답·해설을 보여준다(코드에 그런
스냅샷 컬럼 자체가 없음). 즉 기존 item_id를 그대로 고쳐 쓰면 이미 끝난
과거 응시의 결과 화면이 조용히 달라질 수 있는 구조였다.

**라이브 재확인**: 이 배치(`source_version=nikl_grade5_l3_batch1_v1`)로
생성된 세션·응답은 적용 시점 기준 **0건**(2026-10-07). 0건이라는 이유로
안전장치를 생략하지 않고, 구조상 필요한 설계를 그대로 적용했다:

- 변경 대상 58문항 → **새 item_id**(기존 id + `_V2`)로 INSERT.
- 기존 58문항 → **삭제 없이 `is_active=0`으로만 UPDATE**(FK 무결성상
  삭제는 애초에 불가능하고, 감사 자료로도 남는다).
- 벨기에 2문항 → item_id·내용·is_active **전부 그대로**.

## 3. 연구 DB 반영 (GATE 1~13, 백업+예상값 일치 확인 후에만 적용)

적용 전 라이브 DB 해시(`2f26b6a20a33023c89cd4ffe20e98aa78c4cade52cff95
6826e29ee165a29eba`)가 로컬 검증 때와 정확히 같음을 확인한 뒤 SQLite
Backup API로 백업(`...db.bak_g5l3b1_distractor_v2_20261002-131600`,
integrity_check=ok)하고, **비활성화 대상 58건 전부의 현재 DB 값
(prompt/options_json/correct_option/explanation/source_content_id/
source_version)이 원본 매니페스트 스냅샷과 정확히 일치함을 확인한
뒤에만** 반영했다(다르면 그 즉시 전체 중단하도록 설계 - 이번엔 58/58
전부 일치해 충돌 0건).

**변경 범위 제한 확인**: 바뀐 컬럼은 `options_json`·`correct_option`·
`public_payload_json`·`answer_payload_json`·`qa_flags_json`(관리자용
오답 근거)뿐이고, `prompt`·`lemma`·`pos`·`source_content_id`·
`source_version`은 신규 행도 기존 행과 동일하게 유지했다(교차 확인:
`correct_option`이 가리키는 위치의 텍스트가 실제 정답 뜻풀이와 일치,
`answer_payload_json`의 `correct_option`과 컬럼 값 일치 - 전부 GATE
6/8에서 기계적으로 검증).

**매니페스트 갱신**: 서버가 실제로 읽는
`data/vocab/nikl_grade5_l3_batch1_manifest_v1.json`을 58건(신규
item_id)으로 교체했다(원본 60건은 `..._ORIGINAL_60_20261007.json`으로
별도 보존). `multiformat.py`의 `EXPECTED_GRADE5_L3_BATCH1_ITEM_COUNT`도
60→58로 함께 갱신해 매니페스트와 코드가 어긋나지 않게 했다.

## 4. 기존 승인 보존 + 신선도 만료 확인 (집계 단위 구분)

이 적용 스크립트는 `vocabulary_publish_reviews`를 **전혀 import하지도
않는다** - 적용 전후 그 테이블 행수가 **77→77**로 불변임을 GATE 10에서
확인했다(기존 30건 승인을 덮어쓰거나 새 버전 승인으로 복사하지 않았다는
직접적 증거).

**기존 `review_is_stale()`을 새 로직 없이 그대로 재사용**해(서비스
함수를 직접 호출) 확인한 결과:

| 집계 단위 | 값 |
|---|---:|
| 이번에 반영한 **문항** 수 | **58건**(29단어 × 2유형) |
| 승인이 "재검수 필요"로 전환된 **콘텐츠** 수 | **29건** |
| 승인이 그대로 "유효"로 남은 **콘텐츠** 수 | **1건**(벨기에) |

**58과 29는 다른 집계 단위다** - 58은 문항(item) 수, 29는 그 문항들이
속한 콘텐츠(content, 사람 승인의 단위)의 수다(단어당 문항 2개이므로
29×2=58). 보고서·화면 어디에서도 이 둘을 같은 숫자로 섞어 쓰지 않았다.

## 5. 검수 화면 — 개정/재검수필요/보류 구분 + before/after + 관리자 근거

`/vocab-grade5-l3-batch1-review/`에 다음을 추가했다:

- **목록**: 각 항목에 "오답 개정됨(v2)" 또는 "보류(v1 유지)" 배지, 그리고
  판정 상태를 "재검수 필요(개정으로 만료)" / "승인 유지" / "보류" / "검수
  전"으로 구분 표시. 상단에 배치 전체 집계(개정 29 / 재검수필요 29 /
  보류 1)도 추가.
- **상세**: "현재 문항(v2, 활성)"과 "이전 문항(v1, 비활성 - 감사 자료)"을
  나란히 보여준다(보류 항목은 "이전 문항" 섹션 자체가 없음 - 바뀐 게
  없으므로). 각 현재 문항 아래에 **관리자용 근거**(오답별 혼동 지점·
  이유, 정답 핵심 단서)를 파란 박스로 표시.
- **노출 시점**: `qa_flags_json`(관리자용 근거가 들어있는 필드)은
  `_public_item_payload()`/`_grade()`/`/next` 응답 어디에서도 읽지
  않는다(기존 mode-agnostic 서빙 코드, 이번에도 손대지 않음) - 로컬
  검증에서 `/next` 응답 전체를 문자열로 검사해 `correct_option`·
  `wrong_option_reasons`·`key_clue`가 **전혀 없음**을 확인했다.
- **관리자 기본 응시**: `/grade5-l3-batch1-availability`가 **58문항/29
  고유어휘**로 집계됨을 라이브 적용 전 로컬 검증으로 확인(벨기에 2문항은
  매니페스트에서 빠져 자동으로 제외 - 별도 필터 옵션을 추가하지 않고
  매니페스트 자체를 바꿔서 "기본"이 곧 58문항이 되게 했다).

## 6. 검증 요약 (로컬 DB 사본 → 라이브 순서로 동일하게 재현)

| 항목 | 결과 |
|---|---|
| 적용 전 예상값 일치 확인 | 58/58건 일치, 충돌 0건(다르면 전체 중단하도록 설계) |
| 백업 + 복원성 | SQLite Backup API, `integrity_check=ok` |
| 비대상 불변 | RULE_A/B 1,408·`vocabulary_content_levels` 5,950·`vocabulary_publish_reviews` 77·이 배치 콘텐츠 30건 - 전부 적용 전후 동일 |
| 벨기에 2문항 불변 | item_id·내용·is_active 바이트 단위로 동일(GATE 11) |
| 과거 응시 보존 | 이 배치 과거 응답 0건(해당 없음) - 설계상 0건이어도 새 id 적용은 그대로 수행 |
| 멱등성 | 재실행 시 "적용 대상 0건, 이미 적용됨 58건" - 실제 UPDATE/INSERT 0건(로컬·라이브 둘 다 재현) |
| 배치 격리 | `/next` 응답에 `correct_option`/`wrong_option_reasons`/`key_clue` 전혀 없음(문자열 검사) |
| 접근제어 | 비로그인 시 availability API·검수 화면 모두 **302** |
| 신선도 전환 | `review_is_stale()` 재현 결과 29건 재검수 필요, 1건 유효 |
| 라이브 정상 경로 확인 | 홈페이지·검수화면·availability API 모두 **302**(500/404 없음) |

**테스트 기록 정리**: 모든 TestClient 검증은 로컬 DB 사본
(`%TEMP%\dbtest5\...`, 종료 후 삭제)에서만 수행했다 - 라이브 DB에는
이 배치의 세션이 적용 전후 모두 **0건**임을 재확인했다(정리할 테스트
기록 자체가 없음).

### 재검수 URL · 적용 건수

- **재검수 화면**: `https://aprolabs.co.kr/vocab-grade5-l3-batch1-review/`
  (좌측 사이드바 "📘 문해력 · 어휘" → "🌱 L3 중등 보강 1차 검수")
- **관리자 응시(기본 58문항)**: `https://aprolabs.co.kr/vocabulary-quiz/multiformat/play`
  → "L3 중등 보강 1차 문항만 출제" 체크박스
- **적용 건수**: 신규 활성화 58문항(콘텐츠 29개), 비활성화 보존 58문항,
  변경 없음 2문항(벨기에) - **실제 DB 값 변경은 콘텐츠 29개·문항 58건
  범위로 정확히 한정됐다.**

## 7. 산출 파일

| 파일 | 내용 |
|---|---|
| `scripts/vocab/finalize_grade5_l3_batch1_revision_v2.py` | 적용 대상 고정(58 신규/58 비활성화/2 보류) + 매니페스트 교체 |
| `scripts/vocab/apply_grade5_l3_batch1_distractor_revision_v2.py` | GATE 1~13 적용 스크립트 |
| `data/vocab/nikl_grade5_l3_batch1_manifest_v1.json` | 활성 매니페스트(58건으로 교체) |
| `data/vocab/nikl_grade5_l3_batch1_manifest_v1_ORIGINAL_60_20261007.json` | 원본 60건 감사 스냅샷 |
| `data/import/nikl_grade5_l3_batch1_revision_v2_apply_plan_20261007.json` | 적용 계획(신규/비활성화/보류 목록) |
| `app/vocabulary_quiz/grade5_l3_batch1_review.py`/`routers/...` | 개정/보류/재검수필요 구분, before/after, 관리자 근거 표시 |

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
