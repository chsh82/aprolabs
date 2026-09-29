# L0~L3 문항 정합성 — 38건 정정 + 42건 비공개 적재(최종)

- 일자: 2026-09-29
- 실제로 연구 DB에 쓰기를 수행한 첫 턴(이전 조사 턴들은 전부 읽기
  전용/파일 dry-run). momolib 이식·학생 공개·feature flag 변경·
  push·배포는 전혀 하지 않음(요청대로).

## 1. "38건 오답 텍스트 이슈" vs "40건 정정 후보" — item_id 단위 해명

기존 적재 142건과 재생성 184건 파일을 item_id 단위로 대조한 결과,
**정확히 40건(20어휘)의 텍스트가 달라졌고, 그중 38건(19어휘)만
"교정이 필요한" 경우, 2건(1어휘)은 "이미 유효한데 우연히 다른
유효한 오답으로 바뀐" 경우**임을 확인했다 — **설명되지 않는 차이는
0건**이었으므로 적용을 진행했다.

| 구분 | 문항 수 | 어휘 수 | 판정 |
|---|---|---|---|
| DB 현재값이 강화된 4-선택지 기준으로 실제 실패 | **38** | 19 | 교정 필요 → 적용 |
| 재생성으로 값은 바뀌었으나 DB 현재값 자체는 이미 기준 통과 | **2** | 1 | 교정 불필요 → **적용 안 함**(DB 값 그대로 유지) |
| 합계(재생성으로 값이 달라진 전체) | 40 | 20 | — |

2건 사례(어휘 `유리병`, `MF_A/C_SC_EXCORE_L0_SC_V191_B001_031`): DB
현재 오답 중 하나가 `'일이 매우 중요하고 몹시 급한 상태'`(긴급의
정의, 13자)였는데 재생성 결과는 `'봄철에 피는 꽃'`(봄꽃의 정의,
8자)로 바뀜 — **둘 다 개별적으로 연령 적합성·의미 중복 기준을
전부 통과**하는 유효한 오답이라, 새 값으로 바꿀 실익이 없어 **DB
값을 그대로 두기로 결정**했다(불필요한 데이터 변경 최소화).

38건 전체 행별 표(문항 수 기준, 어휘 수와 혼용하지 않음):
`data/import/existing_l0l3_update38_20260929.json`에 `expected_old_*`
(적용 전 실제 DB값)/`new_*`(적용 후 값)/사유를 전부 기록.

## 2. 184건 전 필드 대조 → 적용 목록 확정

| 분류 | 문항 수 | 처리 |
|---|---|---|
| 기존 142건 중 unchanged(재생성과 완전 동일, 또는 2건처럼 이미 유효) | 104 | 미적용(그대로) |
| 기존 142건 중 수정 필요 | **38** | UPDATE |
| 미적재 42건(21어휘) — 신규 | **42** | INSERT |
| 미적재 42건 중 충돌(이미 DB에 있는 item_id) | 0 | 해당 없음 |
| 328건 중 HOLD 44건 | - | **이번 적용 대상 아님**(별도 집합, 손대지 않음) |

**생성 결과 PASS(184/184)는 "DB 반영 완료"로 집계하지 않았다** —
위 표의 "UPDATE 38 + INSERT 42 = 40건 문항 수 변경(문항 기준)"만이
실제 적용 대상이었다.

## 3. 게이트 적용(실제 DB 쓰기)

| 게이트 | 결과 |
|---|---|
| APP_ENV | `research` |
| 실제 DB 경로 | `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db` |
| 적용 전 DB 파일 SHA-256 | `bdb2517c...f0ac41` |
| SQLite Backup API 백업 | `vocabulary_quiz_research.db.bak_l0l3_fix38_ins42_20260929-025208`(서버 보존, SHA-256 `a0347284...e94a5659`) |
| 백업 복원 가능성 검증 | 별도 커넥션으로 즉시 재조회 - `integrity_check=ok`, `contents=5950`, `items=1511`(적용 전 값과 일치) |
| UPDATE 38건 사전 재확인 | **현재 DB 값이 기록해 둔 예상값과 정확히 일치하는 행만** `WHERE item_id=? AND options_json=? AND correct_option=? AND explanation=?`로 조건부 UPDATE(rowcount≠1이면 즉시 예외) |
| INSERT 42건 사전 확인 | item_id 전역 중복 0건, 연결 콘텐츠 21건 전부 `is_active=1`·`student_exposure=0`·`public_ready=0`·HOLD 없음 |
| 328건 HOLD 44건 제외 | 이번 UPDATE(38)·INSERT(42) 대상 목록 어디에도 44건 중 하나도 포함되지 않음(집합 자체가 다름 - 44건은 애초에 다른 328건 배치 소속) |
| 트랜잭션 | 단일 트랜잭션(BEGIN ~ COMMIT), 실패 시 자동 롤백(이번엔 전부 성공) |

**예상 밖 변경 0건 → 전체 적용 성공(롤백 없음).**

## 4. 적용 후 검증(읽기 전용)

| 확인 | 결과 |
|---|---|
| 184건 전 필드(item_type/content_id/lemma/pos/prompt/options_json/correct_option/answer_payload_json/explanation/source_version) 대조 | **182/184 완전 일치**, 나머지 2건은 1절에서 설명한 "의도적으로 미적용"한 그 2건(DB 값이 이미 유효해 그대로 둠 - 결함 아님) |
| `vocabulary_contents` | 5,950(불변) |
| `vocabulary_multiformat_items` | **1,553**(1,511+42, 정확히 예상값 일치) |
| `source_version='2.1.29'`(기존 문항) 총수 | 1,289(불변 - 전혀 건드리지 않음) |
| 공개 플래그 True인 콘텐츠 | **0건** |
| `PRAGMA integrity_check` | `ok` |
| `PRAGMA foreign_key_check` | 위반 0건 |
| 멱등성 | 184건 item_id 전부 현재 DB에 존재 확인 - 스크립트를 다시 실행하면 GATE 7(중복 검사)에서 즉시 중단되어 재적용되지 않음 |
| **L2 17어휘** | 전부 `level_status=PROVISIONAL_AUTO`, `boundary_flag=0` **그대로 유지**(레벨 확정 절차나 값 변경 전혀 없음). **학생 공개 대상 수로 계산하지 않음**(공개 플래그는 여전히 0) |

## 5. 관리자 일반 출제 격리 — 현황과 향후 방안

**재확인(코드 변경 없음)**: `_select_question_items()`(일반 혼합
모드)·`_select_level_candidates()`(레벨별 모드) 둘 다
`source_version == SOURCE_VERSION`("2.1.29" 하드코딩) 필터라, 이번
배치(`schema_reading_existing_l0l3_dryrun_v1`, 184건)는 **일반 출제
5회 샘플 테스트에서 0건 섞임**을 실제로 재확인했다. **DB의
source_version 값을 바꿔 섞는 조치는 하지 않았다.**

**현재 관리자가 이 배치를 확인할 수 없는 상태**: 정상 관리자 UI
(`/vocabulary-quiz/multiformat`)에는 이 배치를 선택할 수 있는
버튼·URL 파라미터가 없다. `PILOT_SOURCE_VERSION`/
`L6_PILOT_SOURCE_VERSION`처럼 `pilot_mode`/`l6_pilot_mode` 같은 전용
토글도 아직 없다. 현재 유일한 확인 방법은 운영 프로세스를 건드리지
않는 별도 스크립트(이 스크립트 자신의 import 안에서만
`SOURCE_VERSION`을 교체)로 실제 프로덕션 함수를 호출하는 것 뿐이다
(이번에도 그 방식으로 184건이 정상 조회됨을 재확인).

**향후 관리자 전용 미리보기 구현 방안(제안, 이번 범위 아님)**:
1. `multiformat.py`에 `EXISTING_L0L3_DRYRUN_SOURCE_VERSION` 상수와
   `preview_mode: bool` 필드를 `CreateSessionBody`에 추가 —
   `PILOT_SOURCE_VERSION` 처리와 동일한 패턴(`_create_pilot_session`
   상당의 `_create_existing_l0l3_preview_session` 함수)으로 완전히
   격리된 분기를 만든다(일반/레벨/파일럿 분기는 한 글자도 안 건드림).
2. 관리자 화면에 "L0~L3 확장 미리보기(비공개)" 배지가 붙은 별도
   버튼만 추가 — 일반 사용자·학생 화면에는 노출하지 않는다
   (`require_admin` 그대로 재사용).
3. 배치가 여러 개로 늘어날 상황을 고려하면, `pilot_mode`/
   `l6_pilot_mode`처럼 매번 새 불리언 필드를 추가하는 대신
   `preview_source_version: str | None` 하나로 일반화해 "관리자가
   지정한 어떤 비공개 배치든" 미리보기할 수 있게 하는 편이 더
   확장성 있다(이번 결정은 제안일 뿐, 실제 설계는 별도 검토 필요).

## 결론

- 문항 총수 1,511 → **1,553**(+42), UPDATE 38건 + INSERT 42건, 전부
  게이트 통과 후 단일 트랜잭션으로 적용. 예상 밖 변경 0건.
- 328건 중 HOLD 44건은 이번에 손대지 않음(향후 별도 배치 후보로
  남음, `existing_l0l3_next_step_20260929.md` 참고).
- 여전히 전부 비공개(`student_exposure=0`/`public_ready=0`), 레벨
  확정 절차 없음, 관리자 일반 경로에는 섞이지 않음, momolib·학생·
  push/배포 무관.
