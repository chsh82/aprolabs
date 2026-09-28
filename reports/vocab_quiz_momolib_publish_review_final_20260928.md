# momolib 학생용 어휘 퀴즈 관리자 공개검토 — 현재 배포본 최종본 확정

- 일자: 2026-09-28
- **방향 확정**: 이미 배포된 momolib 관리자 공개검토 기능(배포 SHA
  `81b1945`)을 **현재 버전의 완성본으로 확정**한다. 이 문서 이후로
  추가 마이그레이션·배포·학생 공개 작업을 시작하지 않는다.
- **이번 작업은 전부 읽기 전용이다.** 운영 DB에 어떤 판정도 저장하지
  않았고, 어떤 공개 플래그도 바꾸지 않았다. aprolabs에는 동일 화면을
  재구현하지 않았다(이 보고서만 작성).

## 1. 운영 상태 최종 기록 (읽기 전용, 2026-09-28 재확인)

| 항목 | 값 |
|---|---|
| 배포 커밋 | `81b1945` |
| `vocab_quiz_contents` | **227**건 |
| `vocab_quiz_pilot_items` | **80**건 |
| `vocab_quiz_admin_reviews`(판정 잔여) | **0**건 |
| `student_exposure` 또는 `public_ready` = True인 콘텐츠 | **0**건 |
| `GET /practice/vocab-quiz/`(학생 기능) | **404**(모든 역할, feature flag 미설정) |
| `GET /vocab-quiz/publish-review/`(비로그인) | 302(로그인 필요) |
| `bank_questions` | 411건(불변) |
| `users` | 6명(불변) |

이 상태가 이번 "최종본 확정" 시점의 기준선이다.

## 2. 1순위 40건 — 관리자 화면에서 보기 좋은 검토 순서

이미 배포된 `app.vocab_quiz.publish_review` 모듈의 함수(`tier1_content_
ids`, `linked_items`, `online_qa_note` 등 - 새 로직 추가 없이 기존 함수
그대로 재사용)로 운영 DB를 SELECT만 해서 40건을 수집했다. 관리자
화면(`/vocab-quiz/publish-review/`) 자체의 정렬 순서는 이번에 변경하지
않았다(신규 배포 없음) - 아래는 **검토 작업 시 참고할 권장 열람
순서**다: 학년(L4 → L5 → L6) 그룹 안에서 표제어 가나다순.

| # | 레벨 | 표제어 | content_id | 근거 배치 | 자동 판정 제안 |
|---:|---|---|---|---|---|
| 1 | L4 | 간과 | SR_L4CORE_4749 | l4_manual_v1 | 승인후보 제안(자동) |
| 2 | L4 | 공간 | SR_L4CORE_4750 | l4_manual_v1 | 승인후보 제안(자동) |
| 3 | L4 | 정기 | SR_L4CORE_4786 | l4_manual_v1 | 승인후보 제안(자동) |
| 4 | L4 | 조치 | SR_L4CORE_4794 | l4_manual_v1 | 승인후보 제안(자동) |
| 5 | L4 | 종속 | SR_L4CORE_4796 | l4_manual_v1 | 승인후보 제안(자동) |
| 6 | L4 | 중복 | SR_L4CORE_4801 | l4_manual_v1 | 승인후보 제안(자동) |
| 7 | L4 | **집단** | SR_L4CORE_4812 | l4_manual_v1 | 승인후보 제안(자동) — ⚠ 아래 참고 |
| 8 | L4 | 착수 | SR_L4CORE_4813 | l4_manual_v1 | 승인후보 제안(자동) |
| 9 | L4 | 하위 | SR_L4CORE_4825 | l4_manual_v1 | 승인후보 제안(자동) |
| 10 | L4 | 합성 | SR_L4CORE_4827 | l4_manual_v1 | 승인후보 제안(자동) |
| 11 | L5 | 가치관 | SR_L5CORE_4835 | l5_manual_v1 | 승인후보 제안(자동) |
| 12 | L5 | 간략 | SR_L5CORE_4836 | l5_manual_v1 | 승인후보 제안(자동) |
| 13 | L5 | 감안 | SR_L5CORE_4837 | l5_manual_v1 | 승인후보 제안(자동) |
| 14 | L5 | 계승 | SR_L5CORE_4843 | l5_manual_v1 | 승인후보 제안(자동) |
| 15 | L5 | 국면 | SR_L5CORE_4848 | l5_manual_v1 | 승인후보 제안(자동) |
| 16 | L5 | 급진 | SR_L5CORE_4849 | l5_manual_v1 | 승인후보 제안(자동) |
| 17 | L5 | 논술 | SR_L5CORE_4859 | l5_manual_v1 | 승인후보 제안(자동) |
| 18 | L5 | 대등 | SR_L5CORE_4868 | l5_manual_v1 | 승인후보 제안(자동) |
| 19 | L5 | 본론 | SR_L5CORE_4904 | l5_manual_v1 | 승인후보 제안(자동) |
| 20 | L5 | 부가 | SR_L5CORE_4906 | l5_manual_v1 | 승인후보 제안(자동) |
| 21 | L6 | 아이디어 | SR_L6CORE_4942 | l6_manual_v1 | 승인후보 제안(자동) |
| 22 | L6 | 야기 | SR_L6CORE_4943 | l6_manual_v1 | 승인후보 제안(자동) |
| 23 | L6 | 엄밀 | SR_L6CORE_4944 | l6_manual_v1 | 승인후보 제안(자동) |
| 24 | L6 | 오류 | SR_L6CORE_4945 | l6_manual_v1 | 승인후보 제안(자동) |
| 25 | L6 | 완결 | SR_L6CORE_4946 | l6_manual_v1 | 승인후보 제안(자동) |
| 26 | L6 | 왜곡 | SR_L6CORE_4947 | l6_manual_v1 | 승인후보 제안(자동) |
| 27 | L6 | 용이 | SR_L6CORE_4948 | l6_manual_v1 | 승인후보 제안(자동) |
| 28 | L6 | 운용 | SR_L6CORE_4950 | l6_manual_v1 | 승인후보 제안(자동) |
| 29 | L6 | 위계 | SR_L6CORE_4951 | l6_manual_v1 | 승인후보 제안(자동) |
| 30 | L6 | 의지 | SR_L6CORE_4953 | l6_manual_v1 | 승인후보 제안(자동) |
| 31 | L6 | 일괄 | SR_L6CORE_4955 | l6_manual_v1 | 승인후보 제안(자동) |
| 32 | L6 | 자발적 | SR_L6CORE_4956 | l6_manual_v1 | 승인후보 제안(자동) |
| 33 | L6 | 자아 | SR_L6CORE_4957 | l6_manual_v1 | 승인후보 제안(자동) |
| 34 | L6 | 잠정 | SR_L6CORE_4958 | l6_manual_v1 | 승인후보 제안(자동) |
| 35 | L6 | 전면 | SR_L6COREV2_4959 | l6_manual_v2 | 승인후보 제안(자동) |
| 36 | L6 | 전이 | SR_L6CORE_4960 | l6_manual_v1 | 승인후보 제안(자동) |
| 37 | L6 | 전제 | SR_L6CORE_4961 | l6_manual_v1 | 승인후보 제안(자동) |
| 38 | L6 | 전형적 | SR_L6CORE_4962 | l6_manual_v1 | 승인후보 제안(자동) |
| 39 | L6 | 창출 | SR_L6CORE_4964 | l6_manual_v1 | 승인후보 제안(자동) |
| 40 | L6 | 체재 | SR_L6CORE_4965 | l6_manual_v1 | 승인후보 제안(자동) |

**레벨 분포**: L4 10건 · L5 10건 · L6 20건(l6_manual_v1 19 + l6_manual_v2 1)

### 자동검사 근거 (40건 전체 공통, 이번에 다시 SELECT만으로 재확인)

- 40건 전부: 콘텐츠 `is_active=True`, `hold_reason` 없음, 레벨 정보
  존재 → **구조적으로 완전함**
- 40건 전부: `MEANING_CHOICE`·`CONTEXT_MEANING` 두 유형 모두 연결됨,
  각 문항 `is_active=True`·선택지·정답 존재 → **연결 문항 구조 완전함**
- 40건 전부: 2026-09-28 momolib 운영 관리자 화면에서 **실제 응시 확인**
  (80문항 전부 정답 처리·해설 표시 정상, 별도 보고서
  `vocab_quiz_momolib_online_qa_20260928.md`)
- 40건 전부: 여전히 `level_status=REVIEW_BOUNDARY`/`boundary_flag=True`
  (학년 경계 미확정 - 사람 전문가 검수 대상, 변경하지 않음)
- **관리자 판정 잔여 0건** - 40건 전부 아직 사람이 판정하지 않은
  상태(현재 배포본은 판정 저장 기능만 제공하고, 자동으로 판정을 채워
  넣지 않는다)

### ⚠ 7번(집단, SR_L4CORE_4812) 참고 사항

이 콘텐츠에 연결된 문항 `MF_A_SC_SRL4L5PILOT_20260925_L4_003`는
2026-09-28 온라인 QA 최초 응시 시 조사 오류("무리'이라는" → "무리'라는")
가 발견되어 **이미 수정 완료**된 이력이 있다(momolib 커밋 `2c84a04`).
현재 값은 정정된 상태이지만, 이런 수정 이력이 있었다는 사실 자체는
검토자가 참고할 만해 별도 표시했다 - 판정에 영향을 주는 조치는 하지
않았다.

### "자동 판정"의 의미와 한계 (중요)

위 표의 "자동 판정 제안"은 **구조·문항 완전성 자동검사 + 이미 완료된
온라인 QA 결과를 종합한 제안일 뿐**이다:

- **저장되지 않았다** - `vocab_quiz_admin_reviews`에 어떤 행도
  추가하지 않았다(1절 재확인대로 여전히 0건).
- **공개 플래그를 바꾸지 않았다** - `student_exposure`/`public_ready`/
  `level_status`/`boundary_flag` 전부 불변.
- **사람 전문가 검수를 대신하지 않는다** - 뜻풀이의 교육적 적절성,
  학년 적합성, 서술 품질은 자동검사가 확인하지 못하는 영역이다(이전
  dry-run 보고서 0절 표 참고). "승인후보 제안"은 "검토를 우선
  배정할 만하다"는 뜻이지 "공개해도 된다"는 뜻이 아니다.

## 3. 관리자 검토 URL

| 화면 | URL |
|---|---|
| 공개검토 목록(1순위 40건 + 2/3순위 대기열) | `https://momolib.com/vocab-quiz/publish-review/` |
| 공개검토 상세(콘텐츠별 판정 입력) | `https://momolib.com/vocab-quiz/publish-review/<content_id>`(예: `.../SR_L4CORE_4812`) |
| 승격 dry-run(읽기 전용, 실행 버튼 없음) | `https://momolib.com/vocab-quiz/publish-review/promote-dry-run` |

전부 `super_admin`/`hq_manager`만 접근 가능.

## 4. 실제 판정 방법 (관리자가 할 일)

1. `super_admin` 또는 `hq_manager` 계정으로 momolib에 로그인
2. `/vocab-quiz/publish-review/` 접속 → 2절 표의 순서(레벨별 → 가나다순)로
   위에서부터 하나씩 클릭
3. 상세 화면에서 확인: 원천/학생용 뜻풀이가 서로 자연스럽게 이어지는지,
   예문에 목표어가 실제로 들어있는지, 레벨 근거(`REVIEW_BOUNDARY` 상태
   확인용, 실제 학년 판단은 별도), 연결 문항의 선택지·정답·해설이
   국어 어법상 정확한지, "온라인 QA" 참고문 확인
4. 화면 하단에서 판정 선택:
   - **승인후보**: 뜻풀이·예문·문항에 문제 없음 → 다음 단계(승격)
     대상 후보로 표시됨
   - **수정필요**: 뜻풀이·예문·문항 중 고칠 부분이 있음 → 근거란에
     구체적으로 무엇을 고쳐야 하는지 기록
   - **보류**: 판단이 더 필요함(예: 학년 재검토 필요) → 근거란에 사유
     기록
5. 근거(필수) 입력 후 "판정 저장" - 이 순간 `vocab_quiz_admin_reviews`에
   판정자·시각·근거·버전 스냅샷이 기록되며, **공개 플래그는 이 시점에
   전혀 바뀌지 않는다**
6. 콘텐츠나 문항이 나중에 수정되면 이전 판정에 "신선도 만료" 표시가
   뜨므로 재검토
7. 40건(또는 그 이상) 검토가 쌓인 뒤 `/vocab-quiz/publish-review/
   promote-dry-run`에서 승인후보로 확정된 항목들의 "변경 예정 필드"를
   확인 - **이 화면은 읽기 전용이며 실행 버튼이 없다.** 실제 공개
   전환(승격 실행) 기능은 이번 범위에서 아예 구현하지 않았으므로,
   공개하려면 별도의 명시적인 후속 작업이 필요하다.

## 5. 이번에 하지 않은 것

- momolib 코드 변경·마이그레이션·배포 - 없음(배포본 `81b1945` 그대로)
- aprolabs에 동일 공개검토 화면 재구현 - 하지 않음(이 보고서만 작성)
- `vocab_quiz_admin_reviews`에 판정 저장 - 없음(0건 유지)
- `student_exposure`/`public_ready`/`level_status`/`boundary_flag`
  전환 - 없음
- 학생 기능(`VOCAB_QUIZ_STUDENT_ENABLED`) 활성화 - 없음
