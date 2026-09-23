# 원천 데이터 결손 — 별도 과제 (정규화 범위 밖)

`NORMALIZE_QUALITY_ANALYSIS.md` 4절에서 분리. 두 항목 모두 **정규화 규칙이나
LLM으로 만들어낼 수 없는, 원본 학생용 PDF 자체에 구조적으로 없는 값**이라
momo_b2b_tablet(정규화·조판·검수) 프로젝트 범위 밖이다. 여기서는 목록만 정리한다 -
구현은 착수하지 않았다.

## 1. O·X 정답 누락 — 824건

**현황**: `momo_book.db.ox_quiz.answer`가 305건 중 전부(또는 거의 전부) NULL.
원본 학생용 PDF의 raw_text를 보면 `"...138페이지) | ○ | X"`처럼 **선택지만** 있고
정답 표시(동그라미/체크 등)는 없다 — 학생이 직접 골라야 하는 워크북 구조이기
때문에 원본 자체에 정답이 없다. 파싱 오류가 아니다.

**필요한 것**: 같은 교재의 **교사용(정답 포함) PDF**. `momo_book_db/parser.py`
파이프라인은 지금까지 학생용 PDF만 입력받는다.

**제안 범위**(구현 전 확인 필요):
1. 교사용 PDF 원본 확보 경로 확인(현재 momo_book_db가 참조하는 원본 파일들과
   동일한 소스에서 나오는지, 별도 수급이 필요한지).
2. 교사용 PDF에서 O·X 정답만 추출하는 별도 파서(기존 `parser.py`의 학생용 파서와
   합치지 말고, `documents.doc_id` + `ox_quiz.order_no` 매칭 기준으로 `answer`
   컬럼만 채우는 얇은 보강 스크립트를 제안 - 학생용 파서의 다른 필드는 건드리지
   않음).
3. 매칭 실패(교사용에서 해당 문항을 못 찾음) 시 기존처럼 `missing` 플래그 유지.

## 2. 어휘 뜻풀이 누락 — 386건

**현황**: `vocabulary.definition`이 386건 NULL. 원본 확인 결과(예: L2-Q2-W08
"심통"/"부스스"/"으름장" 등) `raw_text`가 표제어 단어 하나뿐 — 학생이 직접
뜻을 채우는 워크북 빈칸이라 원본 자체에 뜻풀이가 없다. 파싱 오류가 아니다.

**필요한 것**: 사용자 지시대로 **초등 어휘 DB**에서 조회 — aprolabs
`app/vocabulary_quiz` 모듈의 `vocabulary_contents`(5,723건, `lemma`+
`canonical_definition`/`student_definition` 보유)를 1차 조회 대상으로 제안한다
(같은 저장소 내 기존 자산 재사용, 신규 사전 구축 불필요).

**제안 범위**(구현 전 확인 필요):
1. `vocabulary.word` ↔ `vocabulary_contents.lemma` 표제어 매칭 규칙 확정(완전
   일치만 쓸지, 활용형 정규화까지 할지).
2. 매칭되면 `definition`을 그대로 채우지 말고(momo_book.db는 원본 보존 원칙 -
   `NORMALIZE_QUALITY_ANALYSIS.md`와 같은 설계 원칙), **정규화 단계 output(`sup`
   플래그 자리)에서만 참고 뜻풀이로 붙이는 방식**을 제안 - `normalize/run.py`의
   `_normalize_vocab()`이 이미 `sup` 플래그를 만드는 지점이라 자연스럽게 확장
   가능.
3. 매칭 실패 시 기존처럼 `sup` 플래그(LLM 보충 필요) 유지.
4. 두 DB가 물리적으로 분리된 SQLite 파일이라는 제약은 스키마리딩 통합 1단계
   조사(`aprolabs/reports/schema_reading_phase1_baseline_20260923.md`)에서 이미
   확인한 것과 같은 제약이다 - 애플리케이션 레벨 조회만 가능하고 FK는 불가.
