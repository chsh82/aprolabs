# momo_b2b_tablet 검수 가이드

## 1. 서버 띄우기

```bash
cd momo_b2b_tablet
uvicorn edition.api:app --port 8000
```

`ANTHROPIC_API_KEY` 환경변수가 있어야 학생 화면의 "글자로 확인"(손글씨 인식)이 실제로 동작합니다. 없어도 나머지(검수·학생 화면 조작·인쇄)는 전부 됩니다.

## 2. 화면별 URL

| 화면 | URL 형식 | 비고 |
|---|---|---|
| 검수·편집 | `http://127.0.0.1:8000/review/index.html?edition=<edition_id>` | 3단 화면(플래그 큐/페이지 목록 · 미리보기 · 인스펙터) |
| 학생 화면 | `http://127.0.0.1:8000/renderer/viewer.html?doc=<doc_id>&mode=student&adapter=api&edition=<edition_id>` | included=false 페이지는 안 보임 |
| 인쇄 PDF(빈 답란) | `http://127.0.0.1:8000/api/editions/<edition_id>/print.pdf` | A4 2-up, 100% 크기 |
| 인쇄 PDF(필기 포함) | `http://127.0.0.1:8000/api/editions/<edition_id>/print.pdf?ink=true&student_id=dev-anonymous` | 학생 화면에서 실제로 답을 쓴 뒤에 확인 |

학생 화면에서 쓴 필기는 `student_id`가 없으면 전부 `dev-anonymous` 한 명으로 저장됩니다(아직 로그인이 없어서 - 5단계 노트 참고). 여러 학년 손글씨를 비교하고 싶으면 `&student_id=아무이름`을 붙여 구분하세요. 손글씨 인식 모델을 바꿔 보고 싶으면 학생 화면 URL에 `&model=claude-opus-5` 처럼 붙이면 됩니다(기본은 `claude-sonnet-5`).

## 3. edition_store.db 초기화

```bash
cd momo_b2b_tablet
rm -f edition/edition_store.db
python -c "from edition import db; db.reset_db()"
```

momo_book.db(원본)는 이 DB와 완전히 분리돼 있어 절대 건드리지 않습니다.

## 4. 검토용 초안 5종 (2026-09-23 생성)

| edition_id | doc_id | 비고 |
|---|---|---|
| **1** | L2-Q2-W08 | 기준 3종 - 야옹아(저학년) |
| **2** | L5-Q3-W10 | 기준 3종 - 긴긴밤(고학년) |
| **3** | L9-Q3-W07 | 기준 3종 - 열하일기(중등, 연표형 배경지식 bgline) |
| **4** | L5-Q4-W02 | choice_ab 4건 - 두근두근 한국사 1권(고학년) |
| **5** | L7-Q1-W08 | 프로즈형 배경지식 - 젊은 예술가의 초상(중등, bgtext 2쪽 분할) |

검수 화면 예: `http://127.0.0.1:8000/review/index.html?edition=3`

## 5. 초안 품질 요약 (검수 손이 얼마나 갈지 미리 보기)

| edition_id | doc_id | 총 페이지 | included | 단순 답란(위젯 없음) | placeholder | widget_unavailable | derived | split | typo | missing | sup | lowres | 플래그 총합 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | L2-Q2-W08 | 14 | 14 | 8 | 3 | 8 | 0 | 20 | 0 | 0 | 5 | 0 | **36** |
| 2 | L5-Q3-W10 | 17 | 17 | 10 | 14 | 10 | 2 | 17 | 15 | 5 | 0 | 1 | **64** |
| 3 | L9-Q3-W07 | 13 | 13 | 4 | 9 | 4 | 6 | 16 | 0 | 2 | 0 | 1 | **38** |
| 4 | L5-Q4-W02 | 12 | 12 | 2 | 4 | 2 | 7 | 0 | 1 | 5 | 0 | 2 | **21** |
| 5 | L7-Q1-W08 | 5 | 5 | 0 | 1 | 0 | 1 | 1 | 1 | 2 | 0 | 0 | **6** |

**읽는 법**
- **단순 답란**: `text_long`/`text_short`가 규칙상 그냥 단답형으로 매핑된 문항 수 - `widget_unavailable`과 거의 1:1로 대응합니다(비교형/목록형/서약형 등으로 바꿀지는 DB에 신호가 없어 전부 검수 판단).
- **placeholder**: 승인을 막는 플래그(자리표시자 - 생성 이미지 지시문, 빈 배경지식 등). **승인 전에 반드시 확인해야 할 항목**입니다.
- **split**: 정규화 단계 구조 재조립(컬럼 밀림·문장 흐트러짐 의심 등) - 원문 대조로 확인하면 됨.
- L5-Q3-W10(긴긴밤)이 플래그 총합 64건으로 가장 많이 걸립니다(placeholder 14건 포함) - 손이 제일 많이 갈 것으로 보입니다. L7-Q1-W08(bgtext 확인용)은 6건으로 가장 가볍습니다.
