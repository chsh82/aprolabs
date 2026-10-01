# 최소 정보 기반 Gemini 레벨 분류기 비교 실험 + 2차 배치 검수 배포

- 일자: 2026-10-03
- 범위: 1차 배치 규칙 분류기(58.3%) 산식 감사, Gemini 분류기 설계·평가(1차=개발
  자료), 2차 배치 100건 고정 예측 생성, 2차 배치를 기존 검수 화면에 추가 배포.
  **DB 쓰기는 신규 테이블·신규 후보 100건·모델 예측 100건에만 한정** -
  `vocab_level`·문항·매니페스트·공개 플래그 변경 없음, momolib 무관, 새 뜻풀이·
  예문·문항 생성 없음, 외부 자료 조사 없음. **전체 17,000건 자동 분류는 실행하지
  않았다**(1차 100건 평가 + 2차 100건 예측뿐).
- 산출 스크립트: `scripts/vocab/nikl_grade5_gemini_classifier.py`,
  `scripts/vocab/migrate_add_grade5_model_predictions.py`,
  `scripts/vocab/apply_grade5_candidate_batch2.py`,
  `scripts/vocab/load_grade5_model_predictions.py`

## 1. 규칙 분류기 58.3% 산식 재감사

`data/import/nikl_grade5_classifier_validation_20261002.csv`(100행) 원본에서
직접 재계산(혼동행렬):

| 실제\예측 | L3 | L4 | 경계 유지 |
|---|---:|---:|---:|
| L3(60) | 48 | 10 | 2 |
| L4(40) | 30 | 8 | 2 |

- 검증 대상 100건, forced(L3/L4만) n=96, 정답 56건 → **58.3%**(기존 보고서
  수치 확인됨)
- 경계 유지 4건을 "오답"으로 포함하면 100건 기준 정확도는 **56.0%**(이전
  보고서는 이 프레이밍을 보여주지 않았음 - 이번에 명시)
- 근거 없는 "신뢰도 %" 수치(`confidence`/`threshold_used` 컬럼)는 규칙
  임계값과의 거리일 뿐 실제 보정된 확률이 아니므로, 이번 보고서 어디에도
  "신뢰도 X%"로 재인용하지 않았다(원본 CSV는 감사용으로 그대로 보존).
- 산출: `data/import/nikl_grade5_rule_classifier_audit_20261003.csv`

## 2~3. Gemini 분류기 설계

`scripts/vocab/nikl_grade5_gemini_classifier.py` - 기존 `scripts/literacy/
gemini_client.py`(model=`gemini-3.6-flash`, JSON 모드) 그대로 재사용. 입력은
표제어·품사·공식등급('5' 상수)·원문 짧은 뜻풀이·전문어 분야 5개로 제한
(DB vocab_level·사람 판정·다의어/고유명사 위험 플래그는 입력에서 제외).

판단 기준은 프롬프트에 "그 뜻의 한국어 모국어 화자 기준 기본 습득 시기"로
명시 고정했고, 뜻풀이 길이·전문어 여부·교재 등장 학년만으로 기계적으로
결정하지 말라고 직접 지시했다(규칙 분류기가 뜻풀이 길이에 과적합한 실패를
교정하기 위함). `grounded`(학년 근거가 실제로 있는지)를 모델이 스스로
표시하게 하고, `grounded=false`면 L3/L4를 강제하지 않고 경계(`borderline`
신호)와 근거부족을 구분해 "경계 유지"/"검토 필요"로 내리게 했다
(`scripts/literacy/auto_review_level.py`의 grounded 패턴 재사용).

**실측 중 발견한 버그와 수정**: 배치 크기 20건·`max_output_tokens=4096`
설정에서 응답 JSON이 중간에 잘리는(Unterminated string) 사례가 100건 중
37건 발생했다(배치 전체가 실패로 처리됨 - 정상 판정으로 치환하지 않고
전부 `api_error=True`로 남김, 요청한 "오류를 정상 판정으로 처리하지 않는다"
원칙대로). 배치 크기를 10, `max_output_tokens`를 8192로 올려 재시도한 결과
오류 0건으로 해소됐다 - 이 수정이 스크립트에 반영된 최종 설정이다.

## 4. 1차 배치(100건) 평가 — **개발 자료 평가, 독립적 최종 성능 아님**

사람 판정(L3=60/L4=40)과 비교:

| 지표 | 값 |
|---|---:|
| forced 정확도(L3/L4만, n=91) | **69.2%**(63/91) |
| 보류율(경계 유지/검토 필요) | 9/100 |
| L3 재현율 | 90.0%(54/60) |
| L4 재현율 | **22.5%**(9/40) |

규칙 분류기(58.3%)·다수결(60.0%)보다는 forced 정확도가 높지만, **L4
재현율이 매우 낮다** - 여전히 다수 클래스(L3)로 쏠리는 경향이 남아 있다.
오분류(forced, 전부 "실제 L4인데 L3로 예측") 27건 중 일부: 개(인사/대명사),
자부심, 보리수나무, 서넛, 소맥분, 상서롭다, 적시타, 혈세, 동판화, 마지않다,
연방, 체 등. **이 100건은 규칙 분류기 설계에 이미 쓰인 개발 자료이므로,
이 수치를 독립적인 최종 성능으로 인용하지 않는다.**

산출: `data/import/nikl_grade5_gemini_batch1_eval_20261003.csv`(actual_
judgment/correct 컬럼 포함)

## 5. 2차 배치(100건) 고정 예측 + 캐싱 증거

고정 설정: `model=gemini-3.6-flash`, `prompt_template_hash=305d00a3d7b055f6`,
`generation_config={'max_output_tokens': 8192}`(배치 크기 10).

**버그 발견·수정(투명성 기록)**: 2차 배치 적재 중 1차 배치와 candidate_id가
충돌하는 3건을 발견했다(서넛/수사, 마지않다/보조동사, 유효하다/형용사 -
compare-and-swap GATE 7이 정확히 설계대로 전체 트랜잭션을 중단시켜 잡아냄).
원인은 이전 턴의 "1차와 겹치지 않게 제외" 로직이 실제 1차 100건 집합이
아닌 중간 분석 산출물(`JOINED_JSON`)로 제외 집합을 만들어 3건이 누락된
것으로 추정된다. 1차 배치 100건 실제 candidate_id 집합을 기준으로 이
3건을 제거하고, 같은 seed 계열(20261003)로 신규 후보 풀에서 3건을 보충해
`data/import/nikl_grade5_batch2_candidates_20261002.csv`를 교체했다 -
1차와의 겹침 0건, 배치 내부 중복 0건을 재확인했다.

| 지표 | 값 |
|---|---:|
| 오류(api_error) | **0/100** |
| 보류(경계 유지/검토 필요) | 12/100 |
| 확정(L3/L4) | 88/100 |

**캐싱 증명**: 같은 입력으로 재실행 시 "캐시 적중 100건, 신규 호출 필요
0건"으로 API 재호출 0건을 실측 확인(`data/import/nikl_grade5_gemini_
cache_20261003.json`).

산출: `data/import/nikl_grade5_gemini_batch2_predictions_20261003.csv`

## 6. 배포 — 2차 배치를 기존 검수 화면에 추가

- 신규 테이블 `vocabulary_grade5_candidate_model_predictions`(모델 예측
  전용, 사람 판정 테이블과 완전 분리) 추가(`migrate_add_grade5_model_
  predictions.py`).
- 2차 배치 100건을 기존 `vocabulary_grade5_candidate_batch`에 `batch_no=2`로
  적재(`apply_grade5_candidate_batch2.py` - 1차 배치 스크립트와 동일한
  candidate_id 파생·compare-and-swap·백업 패턴 재사용, 공유 함수
  `apply_to_db()`에 `expected_table_total` 파라미터를 추가해 누적 배치
  총수(200)를 정확히 검증하도록 보강했다 - 기존 1차 배치 스크립트의 기본
  동작은 그대로 유지).
- 모델 예측 100건을 신규 테이블에 적재(`load_grade5_model_predictions.py`,
  사람 판정 테이블은 절대 건드리지 않음 - 매 실행 시 그 테이블 행 수를
  재확인해 불변임을 로그로 남긴다).
- **배치별 진행률/집계 분리**: 기존 서비스 계층(`grade5_candidate_review.py`)
  함수들은 이미 처음부터 `batch_no` 파라미터를 받고 있었다 - 라우터
  `index()`만 `batch=1`로 고정돼 있던 것을 `?batch=N` 쿼리로 노출하고,
  목록 화면에 배치 선택기를 추가했다. 상세·판정 저장 로직은 수정하지
  않았다(이미 `row.batch_no` 기반으로 완전히 범용).
- **모델 예측 비노출 검증**: 상세 페이지 렌더링 HTML에서
  `predicted_judgment`/`predicted_reason`/"자동 제안" 등 문자열 매치
  0건을 실측 확인(아래 7절).

## 7. 격리 환경 실제 HTTP 검증 (생성 로직 재호출 아님)

연구 DB 사본 + `aprolabs.db` 사본 + 별도 git worktree + 스크래치 포트(8903)에서
전부 실제 HTTP 요청으로 검증:

| 검증 항목 | 결과 |
|---|---|
| 2차 배치 적재 멱등성(2회 연속 `--apply`) | 1회차 100건 삽입, 2회차 "신규 0, 스킵 100" - PASS |
| 모델 예측 적재 멱등성 | 1회차 100건 삽입, 2회차 "신규 0, 스킵 100" - PASS |
| 접근제어(비로그인) | 목록/상세/판정POST 전부 **302** |
| 배치별 진행률 분리 | batch=1 "100/100", batch=2 "0/100" 동시 확인 |
| 모델 예측 비노출 | 상세 페이지 HTML에서 관련 문자열 **0건** |
| 판정 저장 + 다음 미검수 이동 | 실제 POST → 303, 다른 candidate_id로 리디렉션 |
| 중복 제출(동일 토큰 2회 POST) | 둘 다 303 응답이나 **DB 행 1개만** 생성 |
| 재판정(새 페이지 로드 = 새 토큰) | 서로 다른 토큰으로 2회 제출 → **이력 2행 보존**(L4→L3), 목록은 최신(L3)만 집계 |

스크래치 worktree·DB·`aprolabs.db` 사본·프로세스 전부 테스트 종료 후 삭제
확인(연구 DB·실사용자 DB는 전혀 건드리지 않음).

## 8. 실제 배포

- 로컬 변경 사항 중 이번 기능과 무관한 기존 미커밋 변경(momo_worksheet_
  page_editor.py, momo_book_db/* 등)은 전혀 포함하지 않고, 이번 기능
  파일만 커밋했다.
- `git push origin main` → 연구 서버 `git pull` → 실제 연구 DB에
  `migrate_add_grade5_model_predictions.py` 실행(APP_ENV=research,
  SQLite Backup API 백업+복원성 검증) → `apply_grade5_candidate_batch2.py
  --apply` 실행(GATE 1~11 전부 PASS, 콘텐츠/문항/공식등급참조 테이블
  불변 확인) → `load_grade5_model_predictions.py --apply` 실행(사람 판정
  테이블 불변 확인) → `aprolabs.service` 재시작.
- 배포 후 읽기 전용 확인: `vocabulary_contents` 5,950(불변),
  `vocabulary_multiformat_items` 1,553(불변), `vocabulary_official_grade_
  reference` 5,950(불변), `vocabulary_grade5_candidate_batch` 200건
  (1차 100+2차 100), `vocabulary_grade5_candidate_model_predictions`
  100건(전부 2차), `vocabulary_grade5_candidate_judgments` 100건(1차
  그대로, 신규 0건 - 사람 판정은 전혀 안 건드림).
- **라이브 인증 클릭 검증**: 이번 실행 단위는 브라우저 세션에 접근할 수
  없어 수행하지 못했다(tier1·1차 배치 배포 턴과 동일한 한계) - 다음
  인터랙티브 턴에서 실제 로그인 브라우저로 이어서 확인이 필요하다.

## 9. 결론

- **2차 배치(100건) 검수 URL**: `https://aprolabs.co.kr/vocab-grade5-candidate-review/?batch=2`
  (관리자 메뉴: 사이드바 "📘 문해력 · 어휘" → "📗 중등 어휘 레벨 검수",
  화면 진입 후 "2차 배치" 선택기 클릭)
- Gemini 분류기는 규칙 분류기보다 forced 정확도가 높지만(69.2% vs 58.3%)
  L4 재현율이 22.5%로 낮아 여전히 **자동 확정에는 부적합** - 2차 배치
  화면에는 사람에게 예측을 보여주지 않았고(비노출 실측 확인), 모델
  제안은 별도 테이블에만 보존했다.
- 2차 사람 검수가 끝나면 고정된 모델 예측(`vocabulary_grade5_candidate_
  model_predictions`)과 비교해 모델 개선 또는 자동 분류 범위 확대 여부를
  판단할 수 있다 - 이번 단계에서는 그 비교 자체를 수행하지 않았다(아직
  2차 사람 검수가 없으므로).
- 전체 17,000건 자동 분류는 지시대로 실행하지 않았다.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
