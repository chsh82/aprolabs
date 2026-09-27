# 검수 화면 프롬프트 편집 기능

2026-09-27 사용자 지시. 검수자가 텍스트 필드를 하나하나 고치는 대신, 자주
쓰는 레이아웃 교정을 버튼 한 번으로 적용하게 한다. 두 단계로 나눈다.

## 1단계 - 프리셋 버튼 (구현 완료)

**원칙**: LLM을 거치지 않는 고정 패치. 빠르고 정확한 것이 우선이라, 실제
문구를 새로 짓지 않고 구조(레이아웃 플래그, 자리표시자 텍스트)만 바꾼다.

**버튼(페이지 인스펙터 - 해당 페이지 타입에 맞는 것만 표시)**:

| 버튼 | 프리셋 키 | 적용 대상 | 동작 |
|---|---|---|---|
| 좌우 바꾸기 | `mirror` | qa/qaband/qaref/solo | `page.mirror` 토글, 2단 레이아웃 좌우 반전(비율도 함께 뒤집음) |
| 제목 중앙 상단 | `title_top` | qa/qaband/qaref/solo | `page.titleTop` 토글, 문항 제목을 STEP3 essay와 같은 중앙 상단 배너로 |
| 3층 구조 | `three_tier` | qa/qaband/qaref | `page.wide` 토글, 제시문/문항/답란을 전체 폭 3단 스택으로 |
| 답란 늘리기/줄이기 | `lines_more`/`lines_less` | 전체 | `page.linesBoost`(±3~+4) - 그 페이지의 모든 답란 min/max에 균일하게 가감 |
| 이미지 자리 추가 | `add_image_slot` | slot 없는 페이지 | 빈 `page.slot`(생성 자리표시자) 추가 |
| 표 머리칸 넣기 | `table_header` | qaref(ref.title)/compare(카드 제목)/table(행 라벨) | 빈 머리칸에 "참고"/"구분 N"/"항목 N" 자리표시자 채움 |
| 페이지 합치기 | `merge_pages` | 인접한 excerpt 전용 페이지, 또는 자동 분리된 이어짐 쌍 | 문단 배열 합치고 페이지 하나 제거 |
| 페이지 나누기 | `split_page` | 문단 2개 이상인 excerpt | 문단 절반 지점에서 두 페이지로(`continues`/`continued`) |

계산 로직: `edition/presets.py`(`PRESETS` 딕셔너리, 각 프리셋이
`PresetResult(ops, summary)` 또는 `PresetNotApplicable(이유)`를 반환).

**API**:
- `POST /api/editions/{id}/preset-preview` `{preset, page_idx}` → 저장하지
  않고 `{summary, layout}`(패치 적용된 전체 layout)만 계산해 돌려준다.
- `POST /api/editions/{id}/preset-apply` `{preset, page_idx, editor?, expected_rev?}`
  → 지금 상태 기준으로 다시 계산해서 실제 저장. `edition/store.patch_edition`에
  `kind="prompt_edit"`로 넘겨 correction_log에 프리셋 이름+요약 문장 한 줄로
  기록한다(여러 op를 한 덩어리로 - 프리셋 하나가 여러 필드를 건드릴 수 있어서
  op 단위로 남기면 무슨 버튼을 눌렀는지 알아보기 어렵다).

**흐름(review.js)**: 버튼 클릭 → `preset-preview` 호출 → 결과 layout을
`sessionStorage`에 넣고 미리보기 iframe을 `?previewKey=...`로 다시 로드해
즉시 보여줌(가운데 미리보기 바에 요약 문장 + 토글/적용/취소) → "원래 화면
보기"로 저장 전 화면과 토글 비교 → "적용"이면 `preset-apply`로 실제 저장,
"취소"면 `sessionStorage`만 지우고 아무것도 저장하지 않음.

**렌더러(renderer.js/renderer.css) 변경**: `page.mirror`/`titleTop`/`wide`/
`linesBoost` 플래그를 실제로 그려야 프리셋이 의미가 있어서 같이 손봤다.
- `mirror`: `mirrorCols()` 헬퍼로 qa/qaband(비-wide)/qaref/solo의 2단을
  반전(비율 문자열도 좌우 교환).
- `titleTop`: `qHeadTop()`으로 중앙 상단 배너를 그리고, 안에 있던 원래
  `.qhead`는 CSS(`.page.title-top .qhead{display:none}`)로 숨긴다(QTEXT
  등록 부수효과는 그대로 유지 - 중복 등록은 멱등이라 문제 없음).
- `wide`: 기존엔 `qaband`만 지원했다 - `qa`/`qaref`에도 지원을 추가했다
  (자동 생성 규칙도 이 두 타입에 `wide=true`를 줄 수 있어서 원래도 잠재
  버그였음, 이번에 같이 고침).
- `linesBoost`: `inkBox()`가 페이지 스코프의 `LINE_BOOST` 변수를 읽어
  min/max에 가감 - 위젯 종류(single/multi/table/compare/...)를 가리지
  않는 범용 방식.
- STEP3 memos 페이지도 보조 이미지 슬롯(`m-fig`)을 지원하도록 이미 확장돼
  있었다(이미지 재추출 v2 배분 작업, 2026-09-27 앞선 커밋).

**완료 기준 확인**: 102~106(야옹아/긴긴밤/열하일기/두근두근 한국사/젊은
예술가의 초상) 각 문서에서 해당 페이지 타입에 맞는 버튼을 실제로 눌러
미리보기가 바뀌는지, 적용 후 렌더가 깨지지 않는지, 취소 시 아무것도
저장되지 않는지 확인.

## 1.5단계 - 프리셋 추가 + 인스펙터 직접 편집 (구현 완료, 2026-09-27)

1단계를 써 본 사용자 지시: "2단계(자유 입력)보다 아래 두 가지가 먼저입니다."

**[A] 프리셋 추가 - STEP 단계 바꾸기**
값이 셋뿐이라 버튼 그리드 대신 드롭다운(`<select>`)으로 제공. 프리셋 키
`set_step`, `params: {step: "STEP 1"|"STEP 2"|"STEP 3"}`. `page.step`이 있는
모든 페이지 타입에 적용(cover만 없음).

**[B] 인스펙터 직접 편집** - "명세서에서 텍스트는 LLM이 못 고치게 막았지만,
사람이 직접 고치는 경로는 원래 허용". 텍스트 교정 자체는 기존 `patch()` +
`patch_edition`이 이미 `kind="text_correction"`으로 correction_log에 남기므로
(요청 3번 "연결만 하면 됩니다") 추가 백엔드 작업 없이 review.js에 입력창만
붙였다.

1. **제시문 편집**(`renderExcerptInspector`, `page.excerpt`가 있는 모든
   페이지 - qa/qaband/qaref/excerpt 전용 페이지 전부) - 전문을 담은 큰
   textarea(직접 수정, blur 시 저장) + 문단별 "◀ 앞 페이지로 / 뒤 페이지로
   ▶" 버튼. `excerpt.text`는 이 코드베이스 전체에서 항상 원소 1개짜리
   리스트("\n"으로 문단을 이어붙임 - `edition/presets.py`의 split/merge와
   같은 데이터 모양)라 문단 경계는 그 "\n"으로 판단한다. 이동 버튼은 인접
   페이지(`idx-1`/`idx+1`)에 `excerpt`가 있을 때만 나타나고, 누르면 그
   페이지와 대상 페이지 양쪽의 `excerpt.text`를 한 번의 `patch()`로 같이
   바꾼다(젊은 예술가의 초상 18·19쪽처럼 자동 반반 분할 대신 사람이 경계를
   직접 조정하는 사례).
2. **질문·라벨·안내 문구 편집** - `q.t`/`blanks[].label`/`rows[].label,hint`/
   `cards[].title`는 이미 편집 가능했다(이번 확인). 빠져 있던 것: 각 항목의
   `prompt`(학생이 그 답란을 탭했을 때 보이는 안내 문구) 입력칸을
   blanks/table/list/compare 네 가지 위젯 전부에 추가. 페이지 단위
   `topic`/`inst`/`closing`(essay/memos/draw/bgline 등)도 키 존재 여부만
   보고 공통으로 편집창을 붙인다(`renderFreeTextFields`).
3. correction_log 기록 - 위 전부 기존 `patch()`를 그대로 타므로 자동으로
   `kind="text_correction"`, `reason`은 필드별로 구체적으로 남는다. 새 코드
   없음.

**완료 기준 확인**: 젊은 예술가의 초상(edition 106)에서 실제로 문단을
옆 페이지로 옮겨 양쪽 `excerpt.text`가 함께 바뀌는 것, 표 행의 `prompt`
입력이 저장되는 것, STEP 드롭다운 미리보기/취소가 정상 동작하는 것을 확인.

## 2단계 - 자유 입력 (보류)

사용자 지시: "세 가지 요청 모두 말로 시키는 것보다 직접 고치는 편이 빠르고
정확합니다." B가 들어간 뒤 다시 써 보고, 그래도 말로 하는 게 나은 작업이
남으면 그때 진행하기로 함. 아직 설계하지 않았다.
