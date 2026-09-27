# momolib 어휘 퀴즈 227건 — 공개 후보 자동 분류 (dry-run, 플래그 미변경)

- 일자: 2026-09-28
- **이 보고서는 어떤 데이터도 바꾸지 않는다.** `student_exposure`/
  `public_ready`/`hold_reason`/`level_status`/`boundary_flag` 등 어떤
  플래그도 이 작업으로 전환되지 않았다(운영 DB 227건 전부 여전히
  `student_exposure=0`/`public_ready=0`/`REVIEW_BOUNDARY`).
- 목적은 "지금 당장 공개해도 되는 건수"를 추정하는 것이 **아니라**,
  "향후 전문가 검수를 배정할 때 어디서부터 보는 게 합리적인지"를 지금까지
  쌓인 자동검사·실사용 검증 이력을 근거로 우선순위화하는 것이다.

## 0. 핵심 구분 — 자동검사 통과 ≠ 최종 공개 승인

| | 자동검사 통과 | 최종 공개 승인 |
|---|---|---|
| 주체 | 스크립트(literacy.db 대조, 정의 일치, 동형이의 스캔, 구조 검증, 의미 검증, 독립 출처 근거 등급, 실제 관리자 화면 온라인 QA) | 사람 전문가(교사) |
| 현재 227건 중 통과 건수 | 아래 1절 표 참고(전건 어떤 형태로든 최소 1개 이상의 자동검사는 통과) | **0건** |
| 근거 | `expert_review_status` 값 | 227건 전부 `DRAFT_NOT_REVIEWED`/`expert_review_required=true`(phase35 보고서 재확인), momolib DB `boundary_flag=True` 227/227 |

자동검사가 아무리 여러 겹으로 통과해도, 이 227건 중 사람이 최종
승인한 것은 아직 하나도 없다. 이 보고서의 "후보" 순위는 **자동검사
이력의 두께**를 기준으로 한 것이지, 공개 가능 여부의 최종 판정이
아니다.

## 1. 227건 전체 — source_version별 자동검사 이력

momolib 운영 DB(읽기 전용 조회, 값 변경 없음) 기준:

| source_version | 건수 | 자동검사 이력 | 근거 보고서 |
|---|---:|---|---|
| `schema_reading_l6_evidence_grounded_v1` | 48 | 독립 출처(표준국어대사전/공공기관/학술) 근거 등급 MATCH/PARTIAL_MATCH만 통과(CONFLICT·INSUFFICIENT_SOURCE는 이 배치에 없음, `known_definition_errors` 체커로 뜻 충돌 확정 2건 별도 제외) | phase33~35 |
| `schema_reading_literacy_l4_manual_v1` | 49 | literacy.db 3중 연결·REUSE 판정, 뜻풀이/예문 자동 일치 검사, 동형이의어 검사, S(교과개념어) 적합성 대조(HOLD 1건 "가변성"은 애초에 미적재이므로 이 49건에 없음) | phase13 |
| `schema_reading_literacy_l5_manual_v1` | 48 | 동일 방식 + "고1 학년 근거" 재검증(literacy.db, L5_CANDIDATE 6건), 동형이의어 검사(HOLD 2건 "검정"·"내면"은 미적재) | phase14 |
| `schema_reading_literacy_l6_manual_v1` | 50 | 정의 대조·REUSE 판정 + 동형이의/반의어 스캔(50건 내부 + 전체 5,820건 대조), HOLD 0건 | phase23/24 |
| `schema_reading_literacy_l6_manual_v2` | 32 | 동일 방식(전체 35건 후보 중 "매커니즘"·"이성"·"신장" 3건은 동형이의 목표 뜻 미확정으로 HOLD, 32건만 로드) | phase26 |
| **합계** | **227** | | |

**공통 사실**: 5개 배치 전부 사람 전문가 최종 검수 없이 자동검사만으로
로드됐다(0절 참고). 다만 검사 **깊이**는 배치마다 다르다 - 특히
`l4/l5/l6_manual_v1/v2`(179건)는 literacy.db라는 기존 검수된 사전을
근거로 삼는 반면(간접 근거), `l6_evidence_grounded_v1`(48건)은 매
항목마다 독립 출처를 직접 재검색해 확인했다(직접 근거).

## 2. 80개 파일럿 문항(40개 고유 표제어)이 얹은 추가 검증

momolib DB 조회 결과, 파일럿(`vocab_quiz_pilot_items`)이 참조하는
고유 콘텐츠는 정확히 **40건**이며, source_version 분포는:

| source_version | 파일럿에 포함된 건수 |
|---|---:|
| `schema_reading_literacy_l4_manual_v1` | 10 |
| `schema_reading_literacy_l5_manual_v1` | 10 |
| `schema_reading_literacy_l6_manual_v1` | 19 |
| `schema_reading_literacy_l6_manual_v2` | 1 |
| `schema_reading_l6_evidence_grounded_v1` | **0**(파일럿에 포함되지 않음) |

이 40건은 1절의 배치별 자동검사에 더해 **세 겹이 추가로 쌓여 있다**:
1. phase16/27 구조 자동검증(선택지 중복 없음/정답 유일성/설명-정답
   일치/문항수 4개) - 40/40 PASS
2. phase17 의미 검증(SEMANTIC_PASS) - 40/40 PASS
3. **2026-09-28 오늘, momolib 운영 관리자 화면에서 실제로 응시한
   온라인 QA** - 80문항(40×2형식) 전부 실제로 풀어 40/40, 40/40 정답
   처리 확인, 조사 오류 1건 발견·수정 완료(별도 보고서
   `vocab_quiz_momolib_online_qa_20260928.md`,
   `vocab_quiz_momolib_online_qa_followup_fix_20260928.md`)

## 3. 우선순위 티어 (플래그 미변경, 참고용 순위일 뿐)

| 티어 | 조건 | 건수 | 사람 검수 완료 |
|---|---|---:|---|
| **1순위** | 배치 자동검사 + phase16/17 구조·의미 검증 + 오늘 실사용 온라인 QA까지 전부 통과 | **40** | 0 |
| **2순위** | 독립 출처 근거 등급(직접 근거) 통과, 실사용 온라인 QA는 없음 | **48**(`l6_evidence_grounded_v1` 전체) | 0 |
| **3순위** | literacy.db 기반(간접 근거) 자동검사만 통과, 파일럿·온라인 QA 없음 | **139**(179 - 40) | 0 |
| 합계 | | **227** | **0** |

**"공개 가능 추정 수"를 하나의 숫자로 단정하지 않는다.** 실제로 몇 건이
공개되는지는 전문가 검수 인력·시간 배정, 그리고 검수 과정에서 나올
개별 HOLD(1절의 "가변성"·"검정"·"내면"·"매커니즘"·"이성"·"신장" 같은
사례가 이 227건에서도 나올 수 있음) 결과에 달려 있다. 이 표는 **검수
착수 순서를 정하는 데만** 쓰라는 의미다 - 1순위가 가장 많은 자동+실사용
검증을 거쳤으니 가장 먼저 사람 검수를 배정하는 게 합리적이라는
제안이다.

## 4. 남은 위험 요소(검수 시 참고)

- 2순위(evidence_grounded_v1) 48건은 **파일럿 형태의 실제 문항으로
  가공된 적이 없다** - 콘텐츠(뜻풀이·예문)는 근거 검증을 거쳤지만,
  이 시스템의 실제 4지선다 문항(오답 생성·해설 문구)으로 변환됐을 때
  1·2절에서 발견한 것과 같은 조사 오류·오답 매력도 문제가 생길 수
  있다 - 아직 아무도 만들지도, 검사하지도 않았다.
- 3순위 139건은 literacy.db 근거만 있고, 그 literacy.db 자체가 완전히
  오류 없다는 보장은 없다(phase13/14가 "가변성"·"검정"·"내면"에서
  literacy.db 저장값과 실제 쓰임의 괴리를 실제로 찾아냈다).
- `boundary_flag`(REVIEW_BOUNDARY)는 "학년 경계"에 대한 것이지 뜻풀이
  정확성에 대한 것이 아니다 - 학년 재분류가 끝나도 뜻풀이·해설 자체의
  전문가 검수는 별도로 필요하다.

## 5. 하지 않은 것

- `student_exposure`/`public_ready`/`hold_reason`/`level_status`/
  `boundary_flag` 중 어떤 것도 바꾸지 않았다.
- "공개 가능 227건 중 N건" 같은 단정적 숫자를 제시하지 않았다 - 3절의
  티어는 검수 우선순위 제안일 뿐이다.
- 새 콘텐츠·문항을 생성하지 않았다(2순위 48건의 실제 문항화는 아직
  시작하지 않음).
