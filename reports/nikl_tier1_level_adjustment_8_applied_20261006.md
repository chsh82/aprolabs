# tier1 "기본 레벨 조정" 8건 실제 적용 완료

- 일자: 2026-10-06
- 범위: 이전 턴에서 준비한 tier1 "기본 레벨 조정" 8건 개별 적용안을 **실제로
  연구 DB에 적용**했다. RULE_A/B 1,408건, tier1 "현재 유지" 23건, 문항·
  매니페스트·공개 플래그, momolib은 전혀 변경하지 않았다. 완료된 조사·화면
  배포는 반복하지 않았다(새 화면 없음, 기존 분석 재사용).
- 적용 스크립트: `scripts/vocab/apply_tier1_level_adjustment_8.py`
  (GATE 1~12, 기본은 dry-run, `--apply`로만 실제 적용 — 이번에 처음으로
  이 프로젝트에서 `vocabulary_content_levels.vocab_level`을 실제로 UPDATE한
  스크립트다).

## 목표 레벨 확인 방법 (추정 아님)

대상·목표 레벨은 정적 CSV를 믿지 않고 **적용 시점에 DB에서 다시 조회**했다:
`vocabulary_official_grade_judgments`에서 content_id별 **최신** 판정이
정확히 "기본 레벨 조정"인 행만, 그리고 `vocabulary_official_grade_reference.
proposed_base_level`이 `L0`~`L6` 형식으로 **명시적으로** 채워진 행만
대상으로 삼았다 - 하나라도 불명확하면 추정하지 않고 전체 중단하도록
짜여 있다(이번 실행에서는 8건 전부 명시적이라 중단 없이 통과).

그 목표 레벨 자체는 tier1 검수 화면(`/vocab-official-grade-review/`)이
판정 당시 보여준 `diff_reason` 문장("공식 N등급(→LX) vs 현재 LY")에 있던
숫자와 동일하다(`app/vocabulary_quiz/official_grade_review.py`의
`GRADE_TO_LEVEL_LABEL` 매핑, 공식 3등급→L1, 4등급→L2) - 사람이 "기본 레벨
조정"을 누를 때 실제로 화면에서 본 바로 그 숫자다.

**신선도(stale) 확인**: 판정 당시 스냅샷 해시를 서비스 코드
(`official_grade_review.py`의 `_reference_version_snapshot`)와 완전히
동일한 방식으로 재계산해 8건 전부 비만료임을 확인했다(참조 테이블이
2026-09-29 최초 적재 이후 재계산된 적이 없어, 구조적으로 만료될 수
없었다 - 직전 보고서에서 이미 확인한 사실을 적용 직전에 다시 확인).

## 실행 절차 (GATE 1~12, 전부 PASS)

1. **연구 환경·DB 경로 확인(GATE 1·2)**: `APP_ENV=research` +
   `VOCABULARY_QUIZ_DB_PATH=/home/chsh82/aprolabs_data/vocabulary_quiz/
   vocabulary_quiz_research.db`(연구 서버, SSH `aprolabs` 알리아스).
2. **적용 전 해시(GATE 3)**: `f75f5fadafe85f1c8a2fa35299979da66a257ad95616b82ba64836171ea463ec`
3. **SQLite Backup API 백업(GATE 4)**: 적용 직전 전체 DB를
   `vocabulary_quiz_research.db.bak_tier1_level_adj8_20261002-084836`로
   백업(16,404,480바이트), `PRAGMA integrity_check=ok`·
   `vocabulary_contents` 행수(5,950)까지 복원 가능성 검증 완료.
4. **예상 현재값 일치 검사(GATE 7)**: 8건 전부 "현재값=기대한 이전 스냅샷"
   으로 일치(충돌 0건) - 적용 대상 8건 확정.
5. **단일 트랜잭션 적용(GATE 8)**: `UPDATE vocabulary_content_levels SET
   vocab_level = ? WHERE content_id = ? AND level_version =
   'level_policy_v0.1'` 8건을 한 트랜잭션으로 커밋(그 외 어떤 컬럼도, 어떤
   다른 테이블도 쓰지 않음).
6. **변경 전후·비대상 불변 확인(GATE 9·10·11·12)**:
   - `integrity_check=ok`, `foreign_key_check` 위반 0건.
   - 비대상 **5,942건**이 스냅샷과 바이트 단위로 완전히 동일함을 확인
     (모든 컬럼 비교, 단 1건도 변경 없음).
   - 대상 8건도 `vocab_level` 외 다른 컬럼(`level_status`/`boundary_flag`/
     `level_source`/`level_reason_json` 등)은 전혀 바뀌지 않았음을 확인.
   - 대상 8건 `public_ready`/`student_exposure` **전부 0 그대로**(공개
     승인과 혼동되지 않았음을 재확인).
   - RULE_A/B(`review_status='RULE_PROPOSED_PENDING_APPROVAL'`) 건수
     **1,408 → 1,408**(불변), `vocabulary_official_grade_judgments` 전체
     행수 **32 → 32**(불변 - 이 스크립트가 판정 테이블에 아무것도 쓰지
     않았다는 증거), `vocabulary_contents` **5,950 → 5,950**,
     `vocabulary_multiformat_items` **1,553 → 1,553**.
7. **멱등성 검증**: 적용 직후 같은 명령(`--apply`)을 **다시 실행**한 결과,
   8건 전부 "이미 적용됨(멱등 스킵)"으로 분류돼 **UPDATE 0건**, 새 백업도
   만들지 않고 정상 종료함을 라이브에서 직접 재현해 확인했다(로컬 DB
   사본으로도 사전에 동일하게 재현).
8. **적용 후 독립 재질의**: 스크립트 자신의 GATE 로그만 믿지 않고, 별도
   SQL로 8건의 최종 `vocab_level`/`level_status`/`boundary_flag`/
   `public_ready`/`student_exposure`를 다시 조회해 아래 표와 전부 일치함을
   재확인했다.

## 실제 변경 결과

| 표제어 | content_id | 이전 레벨 | 새 레벨 | 적용 후 level_status/boundary_flag |
|---|---|---|---|---|
| 잇몸 | SC_V19102_B102_042 | L2 | **L1** | REVIEW_BOUNDARY/1 (불변) |
| 정다각형 | SC_V19107_B107_041 | L3 | **L2** | PROVISIONAL_AUTO/0 (불변) |
| 효과음 | SC_V19135_B135_034 | L3 | **L2** | PROVISIONAL_AUTO/0 (불변) |
| 공배수 | SC_V1923_B023_045 | L3 | **L2** | PROVISIONAL_AUTO/0 (불변) |
| 단옷날 | SC_V1939_B039_009 | L3 | **L2** | PROVISIONAL_AUTO/0 (불변) |
| 아이디 | SC_V1985_B085_015 | L2 | **L1** | REVIEW_BOUNDARY/1 (불변) |
| 약분하다 | SC_V1987_B087_032 | L3 | **L2** | PROVISIONAL_AUTO/0 (불변) |
| 예금되다 | SC_V1992_B092_009 | L3 | **L2** | REVIEW_BOUNDARY/1 (불변) |

**적용 건수: 8건 전부 성공**(충돌·실패 0건). `level_status`/`boundary_flag`는
이번 승인 범위에 포함되지 않아 손대지 않았고(이전 값 그대로), 그래서 일부는
여전히 REVIEW_BOUNDARY로 남아 있다(3절 참고 — "경계" 표시와 레벨 숫자는
별개 개념).

## 보존·불변 확인 요약

- **사용자 판정·변경 이력 보존**: `vocabulary_official_grade_judgments`는
  이 적용 과정에서 전혀 쓰지 않았다(행수 32 → 32 불변) - 8건의 "기본 레벨
  조정" 판정과 그 이전 이력 전부 그대로 남아 있다.
- **"현재 유지" 23건·RULE_A/B 1,408건 불변**: 전부 라이브 재질의로 확인
  (review_status 분포 재확인 포함).
- **문항·매니페스트·공개 플래그·momolib**: 손댄 코드 경로가 없다(스크립트가
  `vocabulary_content_levels` 외 어떤 테이블도 쓰지 않음, momolib 관련
  코드는 이번에 열어보지도 않았다).
- **백업**: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak_tier1_level_adj8_20261002-084836`
  (연구 서버에 보관, 필요 시 이 파일로 즉시 복원 가능 - integrity_check 검증
  완료).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
