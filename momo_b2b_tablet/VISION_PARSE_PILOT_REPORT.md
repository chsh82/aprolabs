# 방식 B(비전 파싱) 전환 — 준비 작업 + 20건 표본 파일럿 (2026-09-24)

`PARSING_METHOD_COMPARISON.md`에서 방식 B 채택이 결정된 뒤, 전량 실행 전 준비
작업과 20건 표본 파일럿을 진행했다. 코드는 `momo_b2b_tablet/vision_parse/`
(신규 모듈, momo_book.db·edition_store.db와 완전히 분리된 별도 DB)에 있다.

## 1. 원본 PDF 로컬 복사 (외장 드라이브 의존 제거)

`momo_book_db/vision_source_pdfs/{doc_id}.pdf`로 305건 전부 복사, 원본 경로와
`documents.source_hash` 대조 후 복사, 복사본도 재해시해서 검증했다(불일치 0건).
`_manifest.json`에 문서별 원본 경로·해시·복사 시각·크기를 기록했다(총 237MB).
git에는 안 올린다(`.gitignore` 추가) - 저작권 있는 교재 원본이라 서버에는 필요
시 수동 동기화.

## 2. layout_hint 프롬프트 다듬기

사용자 지시대로 8종 + unclear로 세분화했다(`vision_parse/extract.py`의
`_LAYOUT_SHAPES`): `table_answer`/`numbered_list`/`compare_two_col`/
`choice_options`/`boxed_form`/`speech_bubble`/`ruled_lines`/`reference_table`/
`unclear`. `blank_lines`(답란 줄 수)·`cell_size_hint`(칸 크기: small/medium/
large)도 함께 받는다. `note`는 `unclear`일 때만 채우도록 제한(비용 절감).

**검증**: 지목하신 예시가 있는 페이지 6장(야옹아 서약서·작전목록·말대꾸 비교,
열하일기 한자표·비교표 등)으로 두 버전을 반복 실험했다:
- v1(첫 실험): 답란 있는 항목 대부분이 `"표"` 하나로 뭉뚱그려짐(`
  PARSING_METHOD_COMPARISON.md` 4절에서 이미 지적한 문제).
- v2(9종 세분화, note 항상 채움): 서약서(`boxed_form`), 말풍선(`speech_bubble`),
  참고용 연표(`reference_table`), 번호 목록형 표(`table_answer`+세부 note)가
  각각 올바르게 구분됨. 다만 note가 매번 상세히 달려 출력 토큰이 v1 대비
  약 2.2배(657→1,439 토큰/페이지)로 늘었다.
- v3(최종, note는 unclear일 때만): note 절감 지시가 실제로 적용되는지 같은
  6장으로 재확인 - `unclear`가 아닌 항목은 `note: null`로 정확히 비워졌다
  (예: L5-Q3-W10 페이지의 정상 항목은 note 없음, 질문이 다음 페이지로 넘어갈
  가능성이 있는 항목만 `"unclear"` + "질문 없이 제시문만 제공됨, 페이지 하단에서
  끝나 다음 페이지로 이어질 가능성 있음"으로 정확히 잡힘). 이 v3를 최종 프롬프트로
  확정했다(`extract.py`의 `PROMPT_VERSION = "v3"`).

## 3. 결과 스키마 고정 — `vision_parse/db.py`

`vision_extract.db`(momo_book.db와 별개 SQLite 파일, git 비대상):
- `vision_page`: 문서·쪽 단위 메타(모델·프롬프트버전·소스 PDF 해시·토큰·소요시간·
  원본 응답·파싱오류 여부·추출시각). `(doc_id, page_no, prompt_version)` UNIQUE.
- `vision_item`: 페이지 안의 항목별 정규화 레코드(제시문/질문/빈칸/선택지/표/
  쪽수/layout_hint 4필드).
- `run_log`: 문서 단위 실행 이력(성공/실패, 쪽수, 토큰, 소요시간, 에러) - 5절
  체크포인트·재개의 기반.

momo_book.db는 어디서도 쓰지 않는다(정규화 모듈과 같은 읽기 전용 원칙).

## 4. 20건 표본 파일럿 결과

무작위 시드 고정(`--sample 20 --seed 7`) + 이전 스모크테스트 1건(L1-Q1-W01) =
**21건**, 188쪽 전부 성공(파싱 오류 0건 - 아래 "신뢰성 문제 1건" 참고).

| 항목 | 방식 A(momo_book.db 정규화) | 방식 B(비전) |
|---|---:|---:|
| 총 항목 수 | 149(discussion_qa만) | 348(vocab·ox·essay·discussion_qa 전부 포함 - **집계 범위가 달라 직접 비교 부적절**, 5절 참고) |
| 제시문·질문 미분리(추정)* | 18 | 4 |
| 어순 뒤섞임 감지 | 0 | 0 |
| 표/빈칸 구조 포착 | - | 155건 |
| 열린 플래그(A만) | 224 | - |

\* 느슨한 휴리스틱(`compare.py`의 `_unsplit_score` - excerpt 없이 question이
300자 초과 또는 인용부호로 시작하면 "미분리"로 추정)이라 실제 349건 플래그
로직과 다르다 - **상대 비교용 참고치**로만 쓴다.

**"어순 뒤섞임 0/0"은 이 표본이 우연히 그 문제가 있는 문서를 안 뽑았기 때문**
이다(전체 305건 중 111건 관련 - 이 표본 21건에는 없었음). 어순 뒤섞임 해결은
이미 `PARSING_METHOD_COMPARISON.md` 2절에서 긴긴밤 문서로 직접 확인했다 -
이번 파일럿은 그 결론을 뒤집지 않는다(추가 반증도 없었다는 뜻).

### 가장 중요한 발견 — L5-Q2-W12: 방식 A가 완전히 실패했던 문서를 방식 B가 복구

`L5-Q2-W12`는 momo_book.db에서 **모든 테이블이 0건**이다(discussion_qa/
vocabulary/ox_quiz/essay_prompt 전부 0건, background_text도 빈 문자열) - 원본
파서가 이 문서를 통째로 놓친 것으로 보인다. 방식 B는 같은 문서에서 **9쪽 전부
처리해 어휘 4개·OX 문항 등 20개 항목을 정상 추출**했다. 이건 "349건 문제"보다
심각한, 방식 A의 완전 실패 사례를 방식 B가 구제한 실측 증거다.

### layout_hint.shape 분포 (188쪽, 348항목 기준)

```
table_answer     89   (25.6%)
ruled_lines      82   (23.6%)
unclear          51   (14.7%)
choice_options   34    (9.8%)
numbered_list    33    (9.5%)
boxed_form       29    (8.3%)
compare_two_col  16    (4.6%)
reference_table   9    (2.6%)
speech_bubble     5    (1.4%)
```

`unclear` 비율 14.7%는 "억지로 찍지 말 것" 지시가 실제로 지켜지고 있다는
신호로 해석한다(v1에서 대부분 "표"로 몰렸던 것과 대조) - 다만 이 비율이
검수 부담을 얼마나 줄이는지는 조판 단계와 연결해 봐야 실제로 판단 가능하다.

### 신뢰성 문제 1건 발견·수정 — max_tokens truncation

1차 실행에서 `L5-Q2-W12` 9쪽 중 1쪽이 JSON 파싱 실패(내용이 빽빽한 페이지).
원인 확인: `output_tokens`가 `max_tokens=4096`에 정확히 도달 - 응답이 중간에
잘려 JSON이 깨진 것이었다. **수정**: `max_tokens`를 8192로 올림
(`vision_parse/extract.py`). 재실행 결과 같은 페이지 정상 처리, 21건 전체
파싱 오류 0건 확인. 페이지 단위 자동 재시도(같은 이미지로 1회)도 `run.py`에
추가해 두었다(이번엔 재시도 없이도 통과했지만 안전장치로 유지).

## 5. 비용·시간 재추정 (188쪽 실측 기준 - 이전 11쪽 추정보다 신뢰도 높음)

```
21건 문서, 188쪽 전부 처리
총 소요 시간: 2,117초(35.3분), 쪽당 평균 11.3초
쪽당 평균 토큰: in 4,535 / out 973
쪽당 평균 비용($3/M in, $15/M out 가정): 약 $0.028
```

**305건 전체(2,712쪽) 추정**: **비용 약 $76, 순차 처리 약 8.5시간**
(이전 11쪽 표본 추정이었던 $57~104보다 표본이 10배 넓어져 신뢰도가 높다 -
이 수치를 최종 추정치로 삼는 걸 권장한다).

## 6. 방식 A와 방식 B "항목 수" 비교가 부적절했던 것 정정

`compare.py` 첫 버전은 방식 A의 `discussion_qa`만 세고 방식 B는 vocab/ox/
essay/discussion_qa를 전부 세서, "B가 2.3배 더 많이 뽑는다"처럼 보이는 착시가
있었다 - 실제로는 집계 범위가 다른 것뿐이다(방식 A도 vocab 4건·ox 5건 등을
별도 테이블에 갖고 있다). 이 보고서에서는 이 열을 참고용으로만 남기고
"미분리 추정"·"표/빈칸 포착"처럼 같은 기준으로 잴 수 있는 지표를 핵심 비교로
삼았다.

## 7. 전량 실행 준비 상태

- **체크포인트**: `run_log`에 문서별 성공 기록, 재실행 시 완료된 문서는
  자동 건너뜀(`--force`로 강제 재실행 가능).
- **부분 실패 처리**: 문서 단위 실패해도 `run_log`에 사유 기록하고 다음 문서로
  계속 진행(`run.py`의 예외 처리).
- **페이지 단위 재시도**: JSON 파싱 실패 시 같은 이미지로 1회 자동 재시도.
- **실행 명령**: `python -m vision_parse.run --all` (스크린샷: `--sample N`으로
  더 큰 표본도 먼저 가능).

## 8. 해결 안 되는 것은 그대로 둠 (사용자 지시대로 착수하지 않음)

- 이미지 자산 추출 - 기존 방식(A) 병행 필요, `PARSING_METHOD_COMPARISON.md` 4절.
- 페이지 경계를 넘는 문항 연결 - 미해결(이번 표본에서도 L5-Q3-W10 사례로
  재확인, 4절 "unclear" 사례).
- O·X 정답·어휘 뜻 결손 - `SOURCE_DATA_GAPS.md`, 별도 과제 유지.

## 9. 다음 단계 확인 요청

20건 표본 결과(파싱 오류 0건, L5-Q2-W12 완전 복구, layout_hint 세분화 확인)를
검토하시고 305건 전체 실행 여부를 확정해 주시면, `python -m vision_parse.run
--all`을 백그라운드로 실행하고 완료 후 실측 비용·시간·전체 layout_hint 분포로
최종 보고하겠습니다.
