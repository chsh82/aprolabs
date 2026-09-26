# 모모의 책장 B2B 태블릿 교재 — aprolabs 이식 명세서

작성: 2026-09-22 · 기준 시안: 야옹아(L2-Q2-W08, 저학년) · 긴긴밤(L5-Q3-W10, 고학년) · 열하일기(L9-Q3-W07, 중학생)

## 0. 패키지 구성

| 경로 | 내용 |
|---|---|
| `SPEC_aprolabs_이식명세.md` | 이 문서 |
| `samples/*.layout.json` | 시안 3종의 조판 결과(layout JSON). 렌더러의 입력 계약의 실례 |
| `assets/` | 시안에 쓴 이미지(캐릭터, 표지, 원본 교재 삽화). layout JSON의 이미지 키와 파일명이 같다 |
| `prototype/template.html` | 검증된 렌더러(조판·필기·인식·인쇄 전부 포함한 단일 파일) |
| `prototype/data_*.js`, `build.py` | 시안 빌드 스크립트. `build.py`가 template에 데이터·이미지를 끼워 넣는다 |

---

## 1. 목표와 흐름

```
교재 DB(원본, 읽기 전용)
   │ ① 정규화     컬럼 밀림·문장부호·unknown 행 정리, 제시문/질문 분리
   ▼
정규화 레코드
   │ ② 조판 초안  규칙 엔진 + LLM 보조 → layout JSON (검수 플래그 포함)
   ▼
초안 edition
   │ ③ 검수·편집  미리보기 + 필드 편집 + 이미지 후보 선택/재생성 + 승인
   ▼
확정 edition (버전 고정, 저장)
   ├─ ④ B2B 탑재   학생 런타임이 layout JSON을 받아 렌더 · 필기 · 인식 · 칸별 답안 저장
   └─ ⑤ 인쇄      같은 렌더러로 A4 2-up PDF (서버 headless Chromium)
```

**설계 원칙**

1. **layout JSON이 유일한 계약이다.** 검수 페이지, 학생 런타임, 인쇄가 모두 같은 JSON과 같은 렌더러(JS)를 쓴다. 서버는 JSON을 만들고 저장할 뿐 HTML을 조판하지 않는다.
2. **원본 DB는 수정하지 않는다.** 검수에서 고친 내용은 `correction_log`에 남기고, 원본 반영은 별도 승인 단계로 한다.
3. **자동화는 초안까지.** LLM이 만든 값(하위 질문, 보충 뜻, 이미지 지시문, 제시문 분리)은 반드시 검수 플래그를 달고 사람이 확정한다.

---

## 2. layout JSON 규격 (`momo-edition/1`)

```jsonc
{
  "schema": "momo-edition/1",
  "doc_id": "L9-Q3-W07",
  "book":   { "id", "title", "subtitle?", "author", "byline?", "level": "LV 9", "week": "7주차", "quote", "spineTitle?" },
  "tone":   { "band": "lower|elem-upper|mid", "grade": "중학교 3학년", "line": 8, "lines?": { "<kind>": [min, max] } },
  "quarter": "winter|spring|summer|autumn",
  "pages":  [ Page, ... ]
}
```

### 2.1 페이지 유형

| type | 용도 | 주요 필드 |
|---|---|---|
| `cover` | 표지 | (book에서 가져옴) |
| `vocab` | 낱말 익히기 | `vocab[{w, p, d, sup?}]`, `inst?` — 4개 이하 2열, 5개 이상 3열 |
| `oxp` | O·X 내용 확인 | `ox[{s, p}]`, `slot?` |
| `draw` | 그림 칸(생각상자, 활동형 배경지식) | `id`, `inst`, `chip?` — 글자 인식 대상 아님 |
| `bgline` | 배경지식 연표형 | `inst`, `rows[[{e, y?, nt?, hl?, end?}]]`, `image{key, caption}`, `term{title, text}` |
| `excerpt` | 제시문 전용(긴 제시문 분리 시 앞쪽 페이지) | `excerpt{p?, text[]}`, `continues?`(다음 쪽에 이어짐) |
| `qa` | 좌 제시문 / 우 (이미지 슬롯 → 문항 → 답란) | `ratio`, `excerpt{p, text[], continued?}`, `q`, `slot?` |
| `qaband` | 제시문 위 띠 + 아래 2단 | `excerpt{p, text[], continued?}`, `q`, `slot?`, `ratio?`, `wide?`(문항·답란을 전체 폭으로) |
| `qaref` | 띠 제시문 + 좌 참고 표 / 우 문항 | `excerpt{p, text[], continued?}`, `ref{title, img?, rows[{n, v} | {gap:true}], note?}`, `q` |
| `solo` | 제시문 없는 문항 (좌 이미지 / 우 문항) | `q`, `slot?` |
| `essay` | STEP 3 주제 페이지(주제는 단 구분 없이 중앙 상단, 그 아래 좌 도입·인용/우 이미지) | `topic`, `lead?`, `dialog[]`, `closing`, `slot` |
| `memos` | STEP 3 메모 1·2·3 | `topic`, `closing`, `qs[{id, no, t, kind:"memo"}]` |

공통: `step`("STEP 1~3"), `title`, `guide{rt, nm?, img?}`(독해유형 라벨·캐릭터), `level?`·`spineTitle?`(책등 덮어쓰기).

### 2.2 문항(q)과 답란 형식(form)

| form | 모양 | 필드 | part id |
|---|---|---|---|
| `single` (기본) | 괘선 답란 1개 | `kind`(long/short), `starter?` | `{id}` |
| `blanks` | 라벨 달린 빈칸 여러 개 | `blanks[{label, prompt}]` | `{id}#1, #2…` |
| `table` | 좌 머리칸 / 우 답칸 표 | `rows[{label, hint?, prompt}]` | `{id}#1…` |
| `list` | 번호 목록 (쪽수 힌트) | `items[{hint?, prompt}]`, `rowKind?` | `{id}#1…` |
| `compare` | 비교 카드 (각 카드 단일/번호형) | `cards[{title, prompt, n?, img?}]`, `img?`(카드 전체 공유 이미지 - 카드별 `img`가 있으면 그게 우선) | `{id}#1` 또는 `{id}#1-1…` |
| `pledge` | 서약서 양식 | `n` | `{id}#name`, `{id}#1…` |
| `speech` | 말풍선 시작말 + 답란 | `starter` | `{id}` |
| `memo` | STEP 3 메모 | — | `{id}` |

- **part가 채점·저장의 최소 단위다.** 모든 답란은 part 하나이며 `prompt`(그 칸이 답하는 하위 질문)를 가진다.
- 복합 질문(물음이 둘 이상이거나 비교 대상이 둘 이상)은 part로 나누고 하위 질문을 파생한다. 원문에 없던 하위 질문은 `derived: true` 플래그.

### 2.3 이미지 슬롯(slot)

```jsonc
{ "img": "ill8", "src": "원본 8쪽" }                 // 원본 교재 이미지가 있으면 그대로
{ "scene": "…", "avoid": "…" }                       // 없으면 생성 지시문
```

---

## 3. 조판 규칙 (시안에서 확정된 것)

### 3.1 판형과 공통 틀
- 페이지 **A5 가로 210 × 148.5mm**. 흰 여백 6.5mm → 좌측 **책등 띠 11mm**(레벨·분기·책 제목 세로쓰기·쪽번호) → 본문(sub 바탕).
- 인쇄: A4 세로 한 장에 두 페이지 위아래(2-up), 100% 크기. 총 페이지 수는 짝수가 되게.
- 괘선과 답란 테두리는 **같은 색(#C9C0AB), 같은 두께**. CSS 배경이 아니라 캔버스에 정수 픽셀로 그린다(배율에 따라 굵기가 달라지는 문제 방지).
- 낱말 답란에 안내 문구(placeholder)를 넣지 않는다.

### 3.2 학년대 (tone)

| band | 레벨 | 괘선 | 제시문 | 질문 | 도장 | 캐릭터 |
|---|---|---|---|---|---|---|
| `lower` | LV1~2 | 11mm | 16.5px | 15px | −3° | 크게 |
| `elem-upper` | LV3~6 | 9.5mm | 14.5px | 13.5px | −2° | 보통 |
| `mid` | LV7~9 | 8mm | 14px | 13px | 0° | 없음(라벨만) |

답란 줄 수 [최소, 최대] 기본값: long 2~4, short 2~3, blank 1~2, blankTall 2~4, cell 2~4, row 1~2, cardInk 2~4, memo 3~3, vocab 2~3.
저학년 덮어쓰기: vocab 2~2, row 1~1, memo 3~3. (책·문항별로 `tone.lines`, `rowKind`로 조정)

### 3.3 분기 색 (quarter)

| DB 분기 | key | 이름 | main | sub | point |
|---|---|---|---|---|---|
| 1분기(고전) | winter | 겨울 고전 | #1C2A39 | #F5F2E7 | #5A3E36 |
| 2분기(탐구) | spring | 봄 탐구 | #3F7A46 | #FFF8E7 | #E2703A |
| 3분기(문학) | summer | 여름 문학 | #0A6E92 | #E8F8FF | #B8830F |
| 4분기(인문예술) | autumn | 가을 인문예술 | #836B5D | #E8DCC3 | #A8761F |

파생색: `main-deep = main을 검정에 25% 섞기(겨울은 12%)`, `main-soft = main을 흰색에 80% 섞기`, `point-text = point를 paper 대비 4.5:1이 될 때까지 8%씩 어둡게`(작은 글자 전용). 본문 글자·종이·괘선·보조 글자 색은 분기와 무관하게 고정.

### 3.4 페이지 구성

- **STEP 1**
  - 저학년: 낱말 익히기 → 생각상자(그림 칸) → O·X 퀴즈
  - 고학년: 낱말 익히기(1쪽) → 내용 확인 O·X(1쪽, 남는 자리 이미지)
  - 중학생: 배경지식. 형태는 책마다 다름(연표·설명형 `bgline`, 활동형 `draw`). **다른 책의 배경지식을 섞지 않는다.**
- **STEP 2**
  - 한 페이지에 **소문항 하나**. 제시문은 소문항마다 반복해서 함께 싣는다.
  - 제시문 120자 미만 → `qaband`(위 띠). 긴 제시문(250자 이상) → `qa` 3:2.
  - **질문 자체가 80자 이상이면(2026-09-26) 제시문 길이와 무관하게 `qaband`
    + `wide`로 강제한다** - 위에서 아래로 제시문(띠) → 질문(전체 폭) → 답란
    (전체 폭)인 3층 구조. 질문+답란을 반쪽 칸에 넣으면 답답해 보인다는
    지적(젊은 예술가의 초상 8·19쪽)에 따른 규칙 - `layout/rules.py`의
    `LONG_QUESTION_MIN`.
  - 제시문 상자에 여백이 있으면 **세로 가운데 정렬**, 쪽수는 오른쪽 아래 고정.
  - 공간 배분(2026-09-26 정정): **원본 교재 이미지**(`slot.img`)가 있으면
    그 페이지 내용의 일부이므로 최소 높이(25mm)를 먼저 확보하고, 남는
    공간을 답란이 최소 줄 수부터 채운다(야옹아 12쪽 동물등록증처럼 답란이
    자리를 다 차지해 원본 이미지가 숨겨지던 문제 수정 - 5종 실측 결과
    원본 이미지 16곳 중 9곳이 이렇게 숨겨져 있었다). 그래도 답란 최소
    줄 수가 안 나오면 칸이 넘치며, 인쇄 미리보기에서 빨간 점선 테두리로
    표시된다(`renderer.js`의 `allocate()`, `.alloc-overflow` - 자동
    페이지 분할까지는 하지 않으니 검수자가 확인해야 한다). **생성 이미지**
    (원본이 없어 지시문만 있는 자리)는 기존 그대로 "답란이 최대 줄 수까지
    먼저 늘고, 그래도 30mm 이상 남으면 문항 위쪽에 슬롯이 열린다"이다 -
    답란을 줄여 생성 이미지를 넣는 일은 없다.
  - 번호 목록형 칸은 **같이** 늘어난다(모든 칸에 한 줄씩 줄 공간이 있을 때만). 표는 마지막 괘선에서 끝난다.
  - 비교형 활동은 양쪽 형식을 대칭으로(예: 찬성·반대 모두 "3가지 이유").
- **STEP 3**: 주제 페이지(**주제는 단 구분 없이 페이지 중앙 상단에 큰 폰트로**,
  그 아래를 좌 도입·인용 / 우 이미지 2단으로 - 2026-09-26 규칙 변경, 예전엔
  주제가 좌측 단 안에 있었다) → 메모 페이지(사각 번호 1·2·3, 질문 | 답란
  가로 행, 안내 문구는 주제 옆 한 줄).

#### 3.4.1 긴 제시문 분리 (2026-09-26)

제시문이 한 페이지에 다 들어가지 않으면 페이지를 나눈다(경험적 기준:
500자 초과 - `layout/step2.py`의 `_EXCERPT_SPLIT_THRESHOLD`). 나뉜 조각
하나(전용 페이지 포함)의 상한은 `_EXCERPT_PAGE_BUDGET`(800자, 2026-09-26
조정 - 처음엔 500으로 뒀다가 823자짜리가 전용 2쪽+결합 1쪽 총 3쪽으로
너무 잘게 갈려 "두 쪽을 합쳐도 될 듯"하다는 지적을 받고 올림). 문단
(줄바꿈) 경계로만 나누고 문장 중간을 자르지 않는다.

- **앞쪽 페이지(들)**: `excerpt` 타입 - 제시문만 전체 폭, 세로 여유 있게.
  제시문이 더 길면 여러 장으로 계속 나눈다. 다음 쪽에 더 이어지면
  `continues: true`(우측 하단에 "다음 쪽에 이어집니다 ▶" 표시).
- **마지막 페이지**: 기존 `qa`/`qaband`/`qaref` 타입 그대로 - 남은 제시문 +
  문항 + 답란. 앞쪽에서 이어진 것이면 `excerpt.continued: true`(제시문
  상자 위에 "◀ 앞쪽에서 이어짐" 표시).
- 자동 분리는 글자수 규칙일 뿐 실제 인쇄 폰트·여백 기준 줄바꿈이 아니므로,
  분량이 실제로 맞는지는 인쇄 미리보기에서 검수자가 최종 확인한다(`split`
  플래그로 안내).

#### 3.4.2 좌우 배치 (2026-09-26, A안 도입 후 5종 검수로 철회)

한때 "왼쪽 = 이미지 자리 + 문항 + 답란, 오른쪽 = 제시문"(A안)으로
바꿨다가, 5종(야옹아·긴긴밤·열하일기·두근두근 한국사·젊은 예술가의
초상) 전부에서 같은 지적(오른손으로 답란에 쓸 때 손이 제시문을 가림)을
받고 원래대로 되돌렸다: **왼쪽 = 제시문, 오른쪽 = 이미지 자리 + 문항 +
답란**(`qa` 기준 - `qaband`/`qaref`/`solo`는 애초에 이 축의 변경 대상이
아니었다).

### 3.5 이미지

1. **원본 교재 이미지 우선.** DB `document_image`에 그 페이지 이미지가 있으면
   그대로 쓴다(표지·배경·삽화 - `image_type`이 `illustration`뿐 아니라
   `reference`/`excerpt`인 것도 포함, 2026-09-26 정정: 두근두근 한국사
   조선총독부·삼전도비 사진이 `reference`로 들어 있어 기존엔 통째로 빠졌었다).
   `compare`처럼 카드가 여럿인 문항에서 같은 쪽에 카드 수만큼(또는 그 이상)
   이미지가 있으면 카드마다 하나씩 배정한다(qaref가 이미 쓴 이미지는 같은
   쪽의 다른 문항에 재사용하지 않음).
1-1. **같은 쪽에 원본 이미지가 여러 장이면 문항마다 한 장씩 순서대로 배분**
   (2026-09-27, 이미지 재추출 v2로 한 쪽에 여러 장이 나오는 경우가 크게
   늘어난 뒤 사용자 지시). 갤러리처럼 한 페이지에 여러 장을 나란히 작게
   넣지 않는다 - A5 가로 슬롯 폭이 60~80mm뿐이라 나누면 각 장이 30~40mm로
   더 작아지고, 재추출 이미지의 상당수가 이미 150dpi 미만이라 더 나빠진다.
   그 쪽 원본 이미지가 2장 이상이면(`layout/step2.py`의
   `_page_original_counts` 기준) 문항마다 다른 장을 배정하고 쓴 이미지는
   claim 처리한다. 문항 수보다 이미지가 많으면 남는 건 미배치(검수 화면
   "원본 이미지로 바꾸기"에서 수동 배정 대상), 적으면 뒤 문항은 생성
   자리표시자로 남는다. 이미지가 정확히 1장뿐이면 기존 동작(qaband끼리
   공유)을 그대로 유지한다. STEP1 배경지식(`bgtext`, 중등 산문형)이 여러
   쪽으로 나뉠 때도 같은 원칙으로 페이지마다 다른 배경 이미지를 순서대로
   배정한다(`layout/step1.py`의 `_background_images_ordered`). STEP3도
   같은 쪽 essay 이미지가 2장이면 1장은 주제 페이지, 2장째는 "생각 모으기"
   (memos) 페이지의 보조 슬롯에 배정한다. 표지(`cover`)는 문서당 1장이면
   충분하므로 이 분배 대상에서 제외한다.
1-2. **미사용 이미지 목록.** 위 규칙으로도 못 쓰는 이미지(페이지보다 이미지가
   많은 경우, 또는 비-중등 밴드처럼 애초에 소비할 화면이 없는 배경지식
   이미지)는 버리지 않고 검수 화면의 "원본 이미지로 바꾸기" 목록에 그대로
   남는다(`edition/store.available_images` - 재추출 v2 결과까지 병합,
   현재 layout에서 이미 쓰인 이미지는 `used=true`로 표시해 미사용부터
   보여준다). 검수자가 원하는 자리에 수동으로 붙일 수 있다.
2. 없으면 생성. 지시문 규칙:
   - 같은 페이지 제시문에 **명시된 장면만**.
   - **답 유출 금지**: 질문이 추론하라고 요구하는 내용(감정, 이유, 결과)을 그리지 않는다. 지시문에 질문 원문과 금지 요소(`avoid`)를 함께 넣는다.
   - 이미지 안에 글자 없음, 실존 인물 없음, 원작 삽화 화풍·인물 표현 모방 금지.
   - 분기 하우스 스타일(3색 이내, 잉크 선 평면 일러스트, 흑백 복사 시 식별 가능).
   - 어두운 소재(죽음, 폭력)는 암시로만.
3. 생성은 **여러 모델로 후보**를 만들고 검수자가 선택·재생성. 그림체 최종 판단은 검수 단계에서.
4. 한 페이지에 이미지 하나. 비율은 슬롯 실측에 가장 가까운 1:1, 4:3, 3:4, 3:2, 2:1 중 하나로 생성. 인쇄 기준 300dpi.

### 3.6 캐릭터 (저·고학년)

사실적 → 셜록 홈즈, 분석적 → 아로낙스 박사, 추론적 → 지킬 박사, 적용적 → 필리어스 포그, 글쓰기·생각상자 → 앤, 저학년 낱말 → 도로시. 한 페이지 하나, 머리 라벨 옆 고정, 답란 안에는 두지 않는다.
미하엘 엔데 작품 캐릭터(모모, 베포, 호라, 카시오페이아, 회색신사)는 **권리 확인 전까지 사용 보류**.

---

## 4. ① 정규화 — DB에서 확인된 문제

| 문제 | 예 | 처리 |
|---|---|---|
| `discussion_qa`, `essay_prompt`, `documents` 컬럼 밀림 | 질문이 `excerpt_image_path`/`ui_config`, ui 타입이 `question_text`, 표지 문구가 `parsed_at`, 배경지식이 `review_status`에 | 샘플 추출 시 `INSERT … SELECT *` 위치 복사로 추정. **원본 DB를 이름 기준으로 조회해 먼저 확인**, 정규화는 이름 기준 매핑으로 |
| 제시문 문장부호 밀림 | "돌이켜 보고는 해그러면." | 규칙 기반 복원 + LLM 교정, 원문과 diff 표시 |
| 줄바꿈 하이픈 띄어쓰기 | "공부하 면서", "동물 은" | 동일 |
| `unknown` 행 | 저학년: 독해유형+제시문 머리 행 / 중등: 표 행 분해, 한자 뜻 목록 / 파놉티콘: 실행원칙 표 11행 | 같은 source_page·order로 묶어 머리 행은 제시문·라벨로, 표 행은 `table`/`ref`로 재조립 |
| 중등 제시문이 질문 안에 섞임 | `[분석적 / 추론적 독해] "…" (174) …질문` | 독해유형 라벨, 인용(쪽수), 맥락 문장, 질문으로 분리(LLM) |
| O·X 누락 | 저학년 OX가 `ox_quiz`에 없고 배경 텍스트에 | 배경 텍스트에서 추출 |
| 어휘 뜻 누락 | 야옹아 5개 전부 | 초등 어휘 DB에서 조회, 없으면 LLM 보충 + `sup` 플래그 |
| O·X 정답 null | 긴긴밤 | 교사용 교재에서 보충 |
| 이미지 저해상도 | 열하일기 표지 204×299 | 검수 경고 |
| 원문 오타 | "감으로"→"감으라고", "코끼리는 마주한"→"코끼리를", "인생이 유사점"→"인생의" | 검수에서 수정, `correction_log` 기록 |

**LLM 보조 작업** (모두 결과에 `confidence`와 검수 플래그): 제시문·질문 분리, 문장부호 복원, 하위 질문 파생, 보충 뜻, 이미지 장면·금지 요소 지시문, 독해유형 → 캐릭터 매핑.

---

## 5. 저장 구조 (aprolabs, SQLite 기준 초안)

```sql
edition(id, doc_id, version, status /*draft|review|approved|published*/, layout_json, band, quarter,
        created_by, created_at, approved_by, approved_at, source_hash)
edition_flag(id, edition_id, page_idx, path /*JSON pointer*/, kind /*sup|derived|typo|lowres|split|ocr*/, message, resolved_by, resolved_at)
image_candidate(id, edition_id, slot_path, model, prompt, file_path, width, height, chosen, created_at)
correction_log(id, doc_id, field_path, before, after, reason, editor, created_at, pushed_to_source_at)
student_answer(id, student_id, edition_id, part_id, ink_json, text, confirmed, updated_at, rev)
recognition_log(id, answer_id, model, prompt_hash, text, unclear_count, latency_ms, created_at)
```

- 확정된 edition의 `layout_json`은 **수정 불가**. 고치면 새 version.
- 학생 답안은 **part 단위 한 행**. 필기 원본(`ink_json`, 좌표는 레이아웃 px, 획별 필압 포함)과 인식 글자를 함께 둔다.

---

## 6. API (FastAPI, router / service / model 계층)

| 메서드 | 경로 | 설명 |
|---|---|---|
| POST | `/api/editions/draft` | `{doc_id}` → 정규화 + 조판 초안 생성 |
| GET/PATCH | `/api/editions/{id}` | layout JSON 조회 / 부분 수정(JSON Patch). 수정 시 correction_log 기록 |
| GET | `/api/editions/{id}/flags` | 검수 플래그 목록, PATCH로 해결 처리 |
| POST | `/api/editions/{id}/slots/{path}/candidates` | 여러 모델로 이미지 후보 생성 |
| POST | `/api/editions/{id}/slots/{path}/choose` | 후보 선택 → layout JSON의 slot에 `img` 기록 |
| POST | `/api/editions/{id}/approve` · `/publish` | 확정 · B2B 공개 |
| GET | `/api/editions/{id}/print.pdf` | headless Chromium으로 A4 2-up PDF |
| GET | `/api/runtime/{edition_id}` | 학생용 layout JSON + 이미지 URL |
| PUT | `/api/runtime/{edition_id}/answers/{part_id}` | part 단위 저장(필기·글자) |
| POST | `/api/runtime/recognize` | 필기 PNG + part 정보 → 인식 글자(서버에서 Claude API 비전 호출) |
| GET | `/api/runtime/{edition_id}/export` | 평가용 JSON(문항 → parts → 답) |

---

## 7. ③ 검수·편집 페이지 요구사항

- **3단 화면**: 좌 페이지 목록(썸네일, 플래그 개수) / 가운데 실제 렌더러 미리보기(태블릿 비율) / 우 인스펙터(선택한 요소의 필드 편집).
- **플래그 큐**: 보충 뜻, 파생 하위 질문, 오타 후보, 저해상도, 분리 결과 등을 모아 하나씩 확인·해결.
- **원문 대조**: 모든 텍스트 필드 옆에 DB 원문과 diff. 수정하면 correction_log 자동 기록.
- **구조 편집**: 소문항 나누기/합치기, form 바꾸기(single ↔ blanks ↔ table), 페이지 순서 이동, 답란 줄 수 조정.
- **이미지 슬롯**: 지시문 편집 → 여러 모델 후보 생성 → 선택/재생성. 원본 이미지로 교체 가능.
- **분기 색 미리보기**, **인쇄 미리보기**(A4 2-up), 페이지별 승인 → 전체 승인.

---

## 8. ④ 학생 런타임 — 시안에서 바꿀 부분

| 시안(claude.ai 아티팩트) | 이식 후 |
|---|---|
| `window.claude.use("sample")`로 인식 | `POST /api/runtime/recognize` (서버가 Claude API 호출) |
| `localStorage`에 필기·글자 저장 | part 단위 `PUT answers` + 오프라인 대기열(연결 복구 시 재전송) |
| 데이터를 HTML에 내장 | `GET /api/runtime/{edition_id}`로 JSON 로드, 이미지는 URL |
| "그림 자리 설명" 토글 | 학생 화면에서는 제거(검수 페이지 전용) |

유지할 동작: 펜만 필기(손가락은 페이지 넘김, 펜 사용 직후 0.9초 손바닥 무시), 펜/지우개/되돌리기, 칸별 "글자로 확인" → 필기와 인식 글자 나란히 → 학생 수정 후 저장, 필기 수정 시 "다시 확인" 표시, 탭 선택형 O·X, 인쇄 시 학생 필기 포함.

**인식 프롬프트(현행)**: "이미지는 {학년} 학생이 태블릿에 펜으로 쓴 한국어 손글씨 답안입니다. 전체 질문: … / 이 답란이 답하는 부분: … 이미지에 적힌 글자를 보이는 그대로 옮겨 적으세요. 맞춤법·띄어쓰기를 고치지 말고 내용을 보태지 마세요. 줄바꿈 유지. 알아볼 수 없는 글자는 [?]. JSON {"text", "unclear"}로만."

**개인정보**: 미성년자 필기 데이터. 보관 기간, 외부 모델 전송 범위, 삭제 요청 처리를 B2B 계약 조항과 맞출 것.

---

## 9. 구현 순서와 완료 기준

| 단계 | 작업 | 완료 기준 |
|---|---|---|
| 1 | 원본 DB 컬럼 확인, 정규화 모듈 | 샘플 8건 전부 이름 기준으로 올바르게 매핑, unknown 행 0 |
| 2 | 렌더러 모듈화(template.html → `renderer.js` + `renderer.css`, JSON 입력) | `samples/*.layout.json` 3종이 시안과 동일하게 렌더 |
| 3 | 조판 규칙 엔진(정규화 레코드 → layout JSON) | 긴긴밤·야옹아·열하일기를 자동 생성했을 때 시안과 페이지 구성 일치, 플래그 생성 |
| 4 | edition 저장·API | 초안 생성 → 수정 → 승인 → 공개가 버전 단위로 동작 |
| 5 | 검수·편집 페이지 | 플래그 해결, 원문 diff, correction_log, 이미지 후보 선택까지 |
| 6 | 인쇄 PDF | A4 2-up, 필기 포함/미포함 선택 |
| 7 | 학생 런타임 + 답안·인식 API | part 단위 저장, 오프라인 재전송, 평가 export |
| 8 | 나머지 샘플(돈키호테, 통계, 페테르부르크, 파놉티콘)로 규칙 검증 | 새 form·페이지 유형 필요 여부 목록화 |
