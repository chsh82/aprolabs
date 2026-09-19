# PROGRESS — 모모의 책장 DB → 학습지 자동 생성기 1차 안정화 (2026-09-18)

상태: **구현 완료 + 실제 브라우저/PDF 시험 통과**. 기존 Python(추출)→JSON→Node/Playwright(조판)
→HTML 구조와 디자인 시스템(styles.css)은 그대로 유지했고, 새 React 앱이나 별도 생성기로
바꾸지 않았다. 모든 수정은 아래 8개 파일 안에서만 이뤄졌다(대상 목록과 정확히 일치).

**중요한 선행 정정**: 이전 코드 리뷰 지적 중 "샘플 DB 열 이름/값 어긋남"은 momo_book.db
자체의 문제가 아니라, 세션 앞쪽에서 별도로 만든 샘플DB 추출 스크립트(이 저장소 밖,
Downloads 산출물)가 schema.sql 선언 순서와 실제 DB의 물리적 컬럼 순서(ALTER TABLE로
나중에 추가된 cover_message/background_text 등이 뒤에 붙음)가 다른 걸 모르고 위치 기반
`INSERT INTO ... SELECT *`로 복사해서 생긴 문제였다. `extract_worksheet_json.py`는
`sqlite3.Row`+`dict()`로 이름 기반 접근만 하므로 이 문제와 무관하고, 실측(PRAGMA
table_info vs schema.sql 비교)으로 직접 확인했다(아래 "1. DB 호환성" 참고).

## 1. DB 호환성과 입력 검사

- **실측**: `PRAGMA table_info`로 8개 테이블 전부의 실제 컬럼 순서를 schema.sql 선언
  순서와 비교. `documents`(cover_message/background_text가 끝에 붙음), `discussion_qa`
  (order_label/reference_text/reference_image_path/excerpt_image_path 등), `essay_prompt`
  (writing_guide/image_path/closing_instruction)에서 실제 순서가 schema.sql 선언 순서와
  다름을 확인 - ALTER TABLE로 나중에 추가된 컬럼이 물리적으로는 뒤에 붙기 때문.
- `extract_worksheet_json.py`는 처음부터 `SELECT *` + `sqlite3.Row` + `dict()`로 이름
  기반 매핑이라 이 순서 차이와 무관 - **위치 의존 복사가 없어서 고칠 게 없었다**(문제였던
  건 이 저장소 밖의 별도 샘플DB 스크립트).
- DB 연결을 `sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)` 읽기전용으로 바꾸고
  `with` 문으로 확실히 닫도록 수정.
- 입력 검사 추가(모두 `issues[]`로 doc_id/table/row_id/field/reason 구조화):
  review_status 허용값(`{'pending','approved'}` - 실측: 테이블 5개 전부 이 두 값만 존재),
  ui_type 허용값(blocks.js의 switch문과 동일 목록으로 고정), ui_config JSON 파싱(실패 시
  크래시 대신 issue 기록 + `null`로 표시 - `{}`(정상·빈값)와 구분), source_page 타입,
  이미지 경로 형식+허용 디렉터리+실제 파일 존재 여부, 필수 질문 누락, 텍스트 칸에 ui_type
  토큰만 있는 의미상 이상.
- **order_no/order_label 중복 검사에서 실제 데이터 이상 발견**: 다수의 pending 문서
  (예: L8-Q4-W12, L2-Q4-W06)에서 같은 order_label에 `ui_type='unknown'`이고 텍스트가
  "계약에 의한 운영관리"처럼 짧은 구절(도표/목차 항목으로 보임)인 행이 진짜 문항과 나란히
  붙어 있음 - 상위 추출 파이프라인의 아티팩트로 추정, 검수 단계에서 걸러내야 할 대상.
  원본 순서는 임의로 재정렬하지 않고 issue로만 남김(order_no 자체의 "4"/"4-1"/"4-2" 같은
  하위번호 공유는 정상 설계라 오탐 아님 - order_label 기준으로 봐서 구분).
- 출력 JSON에 `schema_version: 1` 추가. 호환 방식: 기존 필드(doc_id/meta/step1/step2/step3)는
  이름·형태를 안 바꿨고, 추가 필드(schema_version/issues/meta.background_text)는 렌더러가
  몰라도 동작에 지장 없음(모르는 필드는 그냥 무시됨).

## 2. 내용 누락 방지 (A/B/C/D)

네 가지 결함 모두 **재현 확인 후 수정**, 실제 DB 데이터로 재현 시험 완료.

- **A. 글쓰기 안내문**: `bgQuote(imgSrc, text)`가 `imgSrc`가 없으면 무조건 `''`을 반환해
  `writing_guide`가 있어도 통째로 사라지던 버그. 이미지/텍스트를 독립 조건으로 바꿔 수정.
  실증: L2-Q4-W06 essay(`image_path=None`, `writing_guide='왕오천축국전'`)로 빌드 →
  `<div class="bg-quote">...<blockquote class="bg-quote__text">왕오천축국전</blockquote></div>`
  (이미지 태그 없이 텍스트만) 렌더 확인.
- **B. 참고 이미지**: `referenceBlock()`이 `reference_text`만 보고 `reference_image_path`는
  아예 렌더링하지 않던 버그 + `bodyFallback()`(ui_type='unknown' 경로)은 `referenceBlock()` 자체를
  호출하지 않아 unknown 문항의 참고자료가 통째로 빠지던 버그(같은 원인의 두 번째 사례,
  실제로 존재하는 승인 대상 행에서 발견). 둘 다 수정. 실증: L5-Q4-W02#435(unknown,
  reference_text=None, reference_image_path 있음)와 #438(choice_ab, 동일 패턴) 둘 다
  `<figure class="figure-block"><img src=".../reference_*.png"></figure>` 렌더 확인.
- **C. 배경지식**: `documents.background_text`가 애초에 출력 JSON에 아예 없었음(가장 심각한
  누락 - 어휘/OX 없는 중등 배경지식형 문서는 1단계가 통째로 비었음). `meta.background_text`로
  추가하고, 어휘·OX가 둘 다 없으면 빈 표 대신 이 텍스트로 1단계를 채우도록 `generate.js`
  수정(반대로 어휘/OX 중 하나라도 있으면 어느 쪽도 안 버리고 다 보여줌). 빈 표·불필요한
  안내문은 더 이상 생성 안 함. 실증: L8-Q4-W12(vocab 0, ox 0, background_text 있음) 빌드 →
  1단계 페이지에 "배경지식" 레이블 블록으로 원문 전체 렌더 확인.
- **D. 콘텐츠 대조**: `qHeader`/`bodyFallback`/`bodyChoice`/`bodyBlanks`/`bodyTableCompare`에
  `data-qa-id`(discussion_qa.id), `vocabTable`/`oxList` 행에 `data-vocab-id`/`data-ox-id`,
  `memoQRow`에 `data-outline-id` 부여. `check.js`가 data.json과 렌더된 DOM을 이 id로 대조
  (문항·어휘·OX·outline 누락 + cover/발췌/참고자료/글쓰기 이미지 존재 여부). `model_answer`는
  설계상 학생용 출력에서 의도적으로 제외되는 필드라 대조 대상에서 뺐다(명시).
- HTML 속성 이스케이프: `esc()`가 `&<>`만 처리하고 따옴표를 안 해서 `alt`/`src` 속성에 `"`가
  들어오면 속성이 깨질 수 있었음 - `"`/`'`도 이스케이프하도록 수정. 이미지 경로는
  `docImageSrc()`에서 파이썬과 같은 정규식(`extracted_images/<doc>/<file>.<ext>`, `..`/절대경로
  차단)으로 한 번 더 검사(파이썬 검증을 거쳤다는 전제를 신뢰하지 않고 JS 쪽에서도 재확인).

## 3. 생성 결과에 QA 연결

- `check.js`가 더 이상 `build/index.html` 고정이 아니라
  `node check.js <html경로> [data.json] [보고서경로]`로 임의 HTML을 검사(인자 없으면 예전
  `npm run check` 그대로 동작 - 하위 호환 확인 완료).
- 상태 4종 도입: **PASS/FAIL/NOT_EXECUTED/BLOCKED**. NOT_EXECUTED는 data.json을 못 찾아
  콘텐츠 대조를 안 돌린 경우, BLOCKED는 PDF 괘선 검사(python/PyMuPDF)를 실행 자체를
  못 한 경우 - **둘 다 PASS와 분명히 구분**(실측: PATH에서 python을 숨기고 실행 →
  `status=BLOCKED`, exit code 1 확인. 예전 코드는 이 경우도 조용히 넘어가 전체 PASS로
  보였을 것).
- 필수 검사 구현: 문항·본문·출처·이미지 누락(콘텐츠 대조), 페이지+**반면(half) 내부**
  오버플로(아래 "5. 조판 안전성" 참고), 답란 최소 15mm, 이미지 로딩 성공 여부(아래),
  폰트 로딩(`document.fonts.check()`로 3개 서체 확인), 페이지 크기 A4(210×297mm, 1mm
  허용오차), 실제 PDF 생성(`page.pdf()`), PDF 답란 괘선 검사(PyMuPDF 래스터화), **PDF
  페이지 수와 화면 `.page` 개수 일치 확인**(신규 - 기존엔 없었음).
- 보완 사항:
  - `img.naturalWidth===0 && img.complete===true`(로딩 실패)를 저해상도 검사에서 그냥
    건너뛰던 버그 수정 - 이제 별도 "이미지 로딩 실패"로 **차단**. 미완료 이미지(`!complete`)도
    별도로 잡음. 실증: 존재하지 않는 이미지 경로로 빌드 → `FAIL: 이미지 로딩 실패 1건` 확인.
  - PDF 괘선 검사 실패/불가 시 전체 PASS로 안 감(위 상태 4종 참고).
  - 검사 대상 `.page`가 0개면 `status=FAIL`(전에는 `0/0페이지 이슈` 찍고 exit 0이었음).
  - 저해상도(WARN)와 오버플로/답란높이/이미지실패(FAIL) 완전히 분리 - `npm run check`로
    기존 골든 11페이지 데모를 돌려보니 **예전 버전은 저해상도 경고 6건을 전부 차단 이슈로
    묶어 exit 1(FAIL)이었을 것**(같은 코드 경로 확인), 지금은 `status=PASS`(경고 6건은
    별도 표시) - 예전엔 정상 골든 샘플도 사실상 "실패"로 보고됐을 가능성.
  - 해상도 검사는 렌더된 `rect.width`(object-fit 반영된 실제 표시 크기) 기준 그대로 유지 -
    별도 보정 없이 기존 공식(가이드 4.4, mm×5.9px) 사용, 추가 정밀화는 안 함(범위 밖).
  - `generate.js`가 같은 빌드 폴더에 남기는 `issues.json`(입력검증 + degraded/unknown/넘침)을
    `check.js`가 읽어서 **하나라도 있으면 FAIL** - "approved 데이터라도 출력 검사에 실패할
    수 있다"/"선택지 없는 선택형은 자동 승인 안 함"을 QA 게이트에서 실제로 강제(6번 항목과
    연동, 아래 참고).
- 목표 흐름(데이터 검사 → 임시 HTML 생성 → 그 HTML 검사 → PDF 검사 → 검증된 결과 확정)을
  `app/routers/momo_book_worksheet.py`의 `_run_build()`가 그대로 구현(아래 7번 참고).

## 4. 조판 안전성

- **가장 중요한 발견**: `.half`는 `.sheet`(flex-direction:column) 안에서 `flex:1`로 배치되는데,
  형제 `.half`가 있으면 한쪽 내용이 넘쳐도 **다른 반면을 밀어내지 않고 그 위에 겹쳐 그려진다**
  (flexbox 특성 - block 레이아웃처럼 형제를 밀어내지 않음). 그래서 `.sheet` 레벨
  scrollHeight/clientHeight만 보면(기존 방식) 이 겹침을 절대 못 잡는다.
  - **실증**: 기존(수정 전) 파일럿 산출물 `generated/L5-Q4-W10/index.html`을 그대로 열어
    확인 - `.sheet`는 919/919(오버플로 없음)인데 1단계의 `.half`(어휘표)는
    scrollHeight=388, clientHeight=374로 **실제로 14px 겹쳐 있었다**. 브라우저 스크린샷으로
    어휘표 마지막 줄과 "열심히 읽은 책 내용을..." OX 안내문이 겹쳐 보이는 것도 육안 확인.
    즉 **"이미 검증됐다"던 골든 파일럿 산출물에 실제로 존재하던, 지금까지 아무도 못 본 결함**.
  - 수정: `check.js`가 페이지 전체뿐 아니라 **`.half` 각각**의 scrollHeight/clientHeight도
    검사. `generate.js`/`paginate.js`도 생성 시점에 반면 단위로 실측(아래).
- 그림이 있는 문항도 이제 최종 크기를 측정한다(전엔 "고정 2슬롯이라 실측 불필요"로 안 쟀음) -
  그림 반면/답 반면 각각 오버플로 검사, 넘치면 issues에 기록.
- 발췌문+답을 분리한 경우 분리된 두 반면 각각 다시 검사(전엔 "분리하면 당연히 fit"으로 가정) -
  실증: 발췌문을 인위적으로 2000자 넘게 늘려서 `발췌문 단독 반면도 넘침(자동 분할 미구현)`
  issue가 실제로 뜨는 것까지 확인(자동 분할은 이번 범위 밖이라 미구현, 차단만 함).
- 측정용(probe.html)과 최종 출력이 같은 폰트·테마·자산 경로·배치 조건을 쓰는지 확인 -
  probe.html이 실제 `styles.css`를 그대로 로드하고 `.sheet`/`.half` 마크업 골격이 동일함을
  코드로 확인.
- 동적 콘텐츠 삽입 후 이미지 로딩 대기 추가(`img.decode()` - 전엔 `innerHTML` 대입 직후 바로
  쟀어서 이미지가 있는 반면은 오버플로를 놓칠 수 있었음).
- 1단계도 이제 넘침 검사 적용(전엔 전혀 없었음 - 그래서 위의 14px 겹침이 지금까지 안 잡혔음).
  3단계도 새 `overflowFull()`(반면 아닌 `.sheet` 전체용 프로브, `probe.html`에 추가)로 검사.
- `.blk-blanks__row .answer-lines`의 `min-height: 11mm`를 `15mm`로 수정(styles.css) - 15mm
  정책과 불일치하던 부분. 실증: L3-Q4-W02가 수정 전엔 "답란 15mm 미만" 경고만 있고
  넘침은 없어 보였는데, 수정 후엔 그 반면들이 실제로는 공간이 모자란다는 게 드러나
  오버플로로 재분류됨(글자·답란을 몰래 줄여서 맞추지 않고 정직하게 차단하는 쪽을 선택).
- `overflow:hidden`으로 가려졌다는 이유로 통과시키는 문제: `.page`에 `overflow:hidden`이
  있지만 `.sheet`/`.half` 자체엔 없어서 `scrollHeight`/`clientHeight` 측정 자체는 유효함을
  확인(위 `.half` 겹침 사례가 오히려 이걸로 잡힘) - 별도 우회 로직 불필요.
- **범위 밖으로 남긴 것**: 장문 자동 분할(문항을 쪼개 다음 페이지로 넘기는 것)은 이번
  1차 안정화에 포함하지 않음(요청서에도 "이번 단계에서는 모든 장문 자동 분할까지 구현할
  필요는 없다"로 명시) - 지금은 "안 되면 issues에 기록하고 차단"까지만 함.

## 5. 불완전한 문항의 처리

- `blocks.js`에 `resetIssues()`/`getIssues()`/`pushIssue()` 구조화 이슈 수집 도입 -
  기존엔 `console.warn`만 남기고 프로그램적으로는 아무 데도 안 남았음.
- `degradedShort`(선택지/빈칸/열 정의 없음), `unknown`/미지원 `ui_type` 모두 issues에
  구조화 기록(doc_id/order_label/ui_type/reason). 화면(검토용)에는 계속 보이되, **QA
  게이트(check.js)에서는 이 목록에 뭔가 있으면 자동으로 최종 승인 안 함**(3번 항목의
  issues.json 연동) - "approved 데이터라도 출력 검사에 실패할 수 있다"를 실제로 구현.
- 실증: L5-Q4-W10#114(choice_multi, options=[] - 실제 승인 문서에 있던 결함, 이번에
  처음 issues로 잡힘)로 빌드 → `issues.json`에 기록 → `check.js`가 이를 읽어 `status=FAIL`
  판정, 승격 안 됨을 확인.

## 6. 마지막 정상 생성본 보존

- `generate.js`: 출력 경로를 명시하지 않으면(기존 CLI 인자 호환 유지) 더 이상
  `generated/<doc_id>/index.html`을 바로 덮어쓰지 않고
  `generated/<doc_id>/builds/<build_id>/`(타임스탬프 ID)에 독립적으로 씀. 같은 폴더에
  `data.json`(입력 스냅샷) + `manifest.json`(build_id/입력 해시/`TEMPLATE_VERSION`/
  schema_version/issue_count) + `issues.json` 저장 - 같은 build_id로 데이터·HTML·QA
  결과가 묶임.
- 상대 자산 경로(styles.css/extracted_images)를 출력 파일의 실제 깊이만큼 동적으로
  계산해서 다시 씀(고정 문자열이면 새 디렉터리 구조에서 깨짐 - 실제로 새 4단계 깊이
  경로로 빌드해서 이미지·CSS가 정상 로드되는 것까지 브라우저로 확인함).
- `app/routers/momo_book_worksheet.py`: extract → generate.js → check.js 순서로 실행하고,
  **check.js가 PASS를 돌려줄 때만** `generated/<doc_id>/current.json`(build_id 포인터)을
  교체("승격"). 실패하면 `current.json`을 안 건드려서 이전 정상본이 계속 보임.
  - 원자적 교체: 임시 파일에 쓰고 `os.replace()`로 교체(쓰는 도중 절반만 쓰인 파일을
    보는 경우 방지).
  - 화면(`detail.html`): 최근 빌드 시도 상태(PASS/FAIL/BLOCKED)를 따로 보여주고,
    실패인데 이전 정상본이 있으면 "최근 빌드 실패 - 아래는 이전 정상본" 배너 표시.
  - 문서별 `threading.Lock`(in-process)으로 동시 빌드 방지 - 잠겨 있으면 대기열 없이
    즉시 "이미 빌드 중"으로 반환(`build_busy=1` 쿼리 → 배너 표시).
  - `subprocess.run(..., timeout=180)`(generate.js), `timeout=120`(check.js) + 
    `TimeoutExpired` 예외 처리 - 타임아웃 시 로그 남기고 승격 안 함.
  - `manifest.json.status`를 `built` → `PASS`/`FAIL`/`BLOCKED`로 QA 후 갱신(`checked_at`
    타임스탬프도 기록).
- **실증(test 10, 아래 8번 참고)**: 합성 데이터로 PASS 빌드 1건을 만들어 승격 →
  `current.json`이 그 build_id를 가리킴 확인 → 같은 문서로 일부러 깨진 데이터(실제
  L5-Q4-W10의 결함 있는 원본, choice_multi options 없음 포함)로 재빌드 → `status=FAIL` →
  `current.json`이 **바뀌지 않고 첫 번째 PASS 빌드를 그대로 가리킴** 확인(코드로 assert).
  `_latest_build_log()`/`showing_previous` 로직도 "최근 시도=FAIL, 정상본=이전 것"을
  올바르게 구분함을 직접 호출로 확인.

## 7. 실제 시험 (12개 전부 실행, 증거 경로 포함)

| # | 시나리오 | 결과 | 증거 |
|---|---|---|---|
| 1 | 정상 교재 JSON→HTML→QA→PDF 전체 경로 | **PASS 확인**(실제 승인 문서 7건 전부는 아래 "중요 발견" 참고 - 그래서 L5-Q4-W10 실데이터를 반면 용량에 맞게 추린 합성 입력으로 "정상 경로 자체"가 끝까지 도는 것을 증명) | `generated/TEST-SYNTH-PASS-01/builds/2026-09-18T14-46-06-533Z/{data.json,index.html,check_report.json}`, status=PASS, 8페이지, PDF 페이지수 일치, 폰트 3종 로딩, 콘텐츠 대조 통과 |
| 2 | 이미지 없이 writing_guide만 | **PASS**(수정 확인) | `generated/L2-Q4-W06/builds/2026-09-18T14-47-34-840Z/index.html` - `.bg-quote`에 이미지 태그 없이 텍스트만 렌더 |
| 3 | 참고 이미지만 | **PASS**(수정 확인) | `generated/L5-Q4-W02/builds/2026-09-18T14-48-23-539Z/index.html` - #435(unknown), #438(choice_ab) 둘 다 `<figure>` 렌더 |
| 4 | 어휘/OX 없이 background_text만 | **PASS**(수정 확인) | `generated/L8-Q4-W12/builds/2026-09-18T14-48-36-311Z/index.html` - "배경지식" 블록 렌더 |
| 5 | options 없는 선택형 | **PASS**(차단 확인) | L5-Q4-W10#114 → degradedShort + issues.json 기록 + check.js FAIL |
| 6 | 잘못된 ui_config JSON | **PASS**(크래시 없음 확인) | momo_book.db 임시 복사본에 고의로 깨진 JSON 주입 → `extract()` 정상 완료, issue 기록, `ui_config=None`. `generated/TEST-BAD-JSON-01/` 빌드도 크래시 없이 완료 |
| 7 | 존재하지 않는 이미지 | **PASS**(차단 확인) | `generated/TEST-MISSING-IMG-01/builds/.../check_report.json` - "이미지 로딩 실패 1건" FAIL |
| 8 | 긴 발췌문/그림 문항 넘침 | **PASS**(차단 확인) | `generated/TEST-LONG-EXCERPT-01`(그림 반면), `TEST-LONG-EXCERPT-03`(발췌문 단독 반면, 2025자) 둘 다 issues 기록 |
| 9 | 여러 빈칸 답란 최소 높이 | **PASS**(수정 확인) | L3-Q4-W02 - CSS 수정 전 "15mm 미만" 경고 → 수정 후 정직하게 오버플로로 재분류(몰래 안 줄임) |
| 10 | 빌드 실패 후 이전 정상본 보존 | **PASS** | 위 6번 항목 실증 내용과 동일(`current.json` 불변 assert) |
| 11 | 입력-출력 콘텐츠 보존 대조 | **PASS**(양성+음성 둘 다) | 정상 빌드 → 통과. `data.json`에 가짜 vocab#999999 추가 → `check_report_tampered.json`에서 정확히 잡힘 |
| 12 | 검사 의존성 부재를 PASS로 보고 안 함 | **PASS** | `PATH`에서 python 제거 후 실행 → `status=BLOCKED`(exit 1), "python 실행 파일을 찾을 수 없음" 사유 명시 |

테스트용 합성 문서(`generated/TEST-*`)와 momo_book.db 임시 복사본
(스크래치패드, 세션 종료 시 자동 정리 대상)은 **운영 DB를 전혀 건드리지 않고** 생성했다.

## 8. 중요 발견: 현재 승인(approved) 문서 7건 전부 새 QA를 통과 못 함

1차 안정화 전에는 아무 검사가 없어서 몰랐지만, 이번에 만든 QA로 **실제 승인 문서 7건을
전부 빌드해본 결과 전부 FAIL**이었다:

| doc_id | 실패 원인 |
|---|---|
| L1-Q4-W01 | 1단계 어휘표 반면 오버플로 |
| L3-Q4-W01 | 1단계 오버플로 + 문항#7(table_compare, columns 없음) 답 반면 오버플로 |
| L3-Q4-W02 | 1단계 오버플로 + 답란 15mm 미만(수정 후 오버플로로 재분류) |
| L5-Q4-W01 | 1단계 오버플로 |
| L5-Q4-W02 | 1단계 오버플로 + 분리된 답 반면 오버플로 |
| L5-Q4-W10 | 1단계 오버플로(어휘표-OX 14px 겹침) + #114 선택지 없음 |
| L5-Q4-W11 | 답란 15mm 미만(같은 blanks 최소높이 이슈) |

**패턴**: 7건 중 6건이 "1단계(어휘/OX) 반면 오버플로"로 실패 - 실제 DB 어휘 4~5개 +
정의 길이가 현재 반면 높이 예산(약 374px)을 일상적으로 살짝 넘긴다. 이건 이번
1차 안정화 범위(장문 자동 분할 미구현, "안 되면 차단"까지만)를 넘는 **디자인/용량
설계 과제**로 보인다 - `CAPACITY.md`(measure.js 산출물)가 합성 채움 텍스트로만
재서 이 실제 격차를 못 잡았던 것으로 보인다(실제 DB 콘텐츠로 재측정 필요).

## 9. 미실행·미검증·환경 제한

- **미검증**: 실제 컬러 프린터로 인쇄해 육안 확인(이 환경엔 프린터 없음) - PDF 생성 +
  PyMuPDF 픽셀 검사까지는 실행해서 답란 괘선이 인쇄 경로에서 실제로 그려지는 것을
  확인했지만, 종이에 뽑아 보는 건 못 했다.
- **미검증**: 여러 워커 프로세스로 앱을 띄우는 배포 형태에서의 동시 빌드 방지 - 지금
  구현한 락은 in-process(threading.Lock)라 단일 프로세스 배포에서만 유효. 여러 프로세스로
  띄우면 별도 파일 락 등이 필요(현재 배포 형태 확인 못 해서 범위 밖으로 남김).
- **범위 밖으로 명시하고 안 한 것**(요청서에도 다음 작업으로 남기라고 명시): 장문 자동
  분할, 자유 편집 UI 확장, 이미지 생성 기능 확대, "1단계 반면 용량 부족" 자체의 설계
  개선(위 8번 발견) - 이번엔 정직하게 차단하는 것까지만 구현.
- 운영 배포(git push), 유료 이미지 API 호출은 지시대로 하지 않았다.

## 10. 변경 파일 목록 (대상 8개 전부, 그 외 없음)

`git status --short`로 확인 - 정확히 아래 8개만 변경됨:
- `momo_book_db/generate/extract_worksheet_json.py`
- `momo_book_db/worksheet/scripts/blocks.js`
- `momo_book_db/worksheet/scripts/paginate.js`
- `momo_book_db/worksheet/scripts/generate.js`
- `momo_book_db/worksheet/scripts/check.js`
- `momo_book_db/worksheet/build/styles.css`(1곳: `.blk-blanks__row .answer-lines` 11mm→15mm)
- `momo_book_db/worksheet/build/probe.html`(1/3단계용 `#probe-full`/`renderFullSheet` 추가)
- `app/routers/momo_book_worksheet.py`
- `app/templates/momo_worksheet/detail.html`

커밋·푸시는 하지 않았다(운영 배포 범위 밖 - 지시대로).

## 다음 시작점 (1차 안정화 시점 기준 - 2차 안정화로 일부 해소됨, 아래 참고)

1. ~~1단계 반면 용량 설계 재검토~~ → **2차 안정화에서 해결**(아래 참고).
2. 승인 문서 7건을 이번 QA 기준으로 다시 검수 → **2차 안정화에서 원인 분리 완료, 일부는
   복원 후보 조사까지 마침**(아래 참고). 나머지(L5-Q4-W10#114 등)는 원본 자료 확인 필요.
3. 검수화면 사전 경고는 여전히 범위 밖(3차 이후 과제).
4. 장문 자동 분할은 2차 안정화에서도 명시적으로 범위 밖으로 남김(아래 참고).

---

# 2차 안정화 (2026-09-19)

목표: 검사 기준(1차 안정화의 QA)을 그대로 유지한 채, **승인 문서 중 조판 문제만 있던
부분을 실제로 해결**하고, 데이터가 불완전한 부분은 원인을 명확히 분리해 남긴다.
"7건 전부 통과"가 목표가 아니라는 전제를 그대로 따랐다.

## 1. 승인 문서 7건 실패 원인 분리 (문서별 · 문항별)

1차 안정화 코드 그대로(수정 전) 7건을 다시 빌드해 문서별·문항별로 정리했다. 한 문서에
여러 문제가 있으면 전부 기록했다.

| 문서 | 조판 문제(레이아웃) | 데이터 누락 문제 |
|---|---|---|
| L1-Q4-W01 | 1단계 어휘표 반면 오버플로(400/374) | #1, #5 `ui_type='unknown'`(미분류) |
| L3-Q4-W01 | 1단계 오버플로(415/374) | #7 `table_compare` columns 없음(→ 답 반면도 같이 오버플로) |
| L3-Q4-W02 | 1단계 오버플로(393/374), #3·#4-2(`text_short_multi`) 답 반면 단독 오버플로 | #4-1 `table_compare` columns 없음, #5 `unknown` |
| L5-Q4-W01 | 1단계 오버플로(430/374) | #1-1 `table_compare` columns 없음 |
| L5-Q4-W02 | 1단계 오버플로(388/374), **#7 답 반면 1052/374(4단계 참고자료 그림이 원본 대비 3배 확대되던 CSS 버그 - 별도 발견)** | #4 `unknown` |
| L5-Q4-W10 | 1단계 오버플로(388/374) | #3(id=114) `choice_multi` options 없음 |
| L5-Q4-W11 | (1차 안정화 CSS 수정으로 이미 해소) | 없음 |

**7건 중 조판 문제만 있던 문서는 사실상 없었다** — L5-Q4-W11을 뺀 6건 전부 "1단계
오버플로 + 데이터 누락"이 같이 있었다. 그래서 1단계를 고쳐도 7건이 한 번에 PASS로
안 바뀌는 건 예상된 결과다(아래 3번 결과 참고).

## 2. 부수 발견 — 참고자료 그림 확대 버그(1차 안정화에서 만든 버그, 2차에서 수정)

L5-Q4-W02#7을 조사하다 발견: 1차 안정화에서 추가한 `referenceBlock()`의 참고 이미지가
발췌문 전용 큰 그림 반면 클래스(`.figure-block`, width:100%로 강제 확대)를 그대로 재사용해서,
162×251px 원본 썸네일이 **497×770px로 4.9배 확대**되며 반면이 1052px(374px 예산의
2.8배)까지 넘쳤다. `.reference .figure-block img`에 `width:auto; max-height:40mm`로
범위를 좁혀 원본 비율 그대로 작게 표시하도록 수정(`styles.css`) - 그림 반면(발췌문용
`.figure-block` 기본 동작)은 손대지 않았다. 수정 후 #7은 1052→433px로 줄었으나(374 대비
59px 초과), 여전히 넘쳐서 차단 상태는 유지된다 - 이건 Step2 개별 문항 밀도 문제라
이번 1단계 재배치 범위 밖으로 남겼다(6번 참고).

## 3. 1단계(어휘/OX) 조판 수정

**변경 파일**: `momo_book_db/worksheet/scripts/paginate.js`(신규 `paginateStep1`),
`blocks.js`(행/헤더 단위 함수 분리: `vocabRowHtml`/`vocabTableFrom`/`vocabTableHeader`,
`oxItemHtml`/`oxListFrom`), `generate.js`(`buildStep1Pages`를 다중 페이지 반환으로 재작성).

- 어휘표·OX목록을 고정 반면(각 374px) 대신, **머리말·푸터를 뺀 `.sheet`의 실제 여유
  공간 전체**(`overflowFull`, probe.html의 `#probe-full`)에서 우선 한 페이지에 다 들어가는지
  실측한다 - 대부분의 실제 데이터(어휘 4~5개)는 여기서 끝난다(고정 반면 예산이 원인이었지
  콘텐츠 자체가 페이지 하나보다 큰 게 아니었음을 확인).
- 안 들어가면 어휘 행부터 **행 단위로**(쪼개지 않고) 이분탐색(`fitCount`)으로 이번 페이지에
  최대 몇 행이 들어가는지 실측해서 채우고, 남는 행은 다음 페이지로 넘긴다. **다음 페이지엔
  표 머리글(`<thead>`)만 반복**하고 안내 문구는 첫 페이지에만 둔다. 어휘를 다 배치한 뒤 OX도
  같은 방식(문항 단위)으로 이어서 채운다.
- 원래 순서 보존(행을 재정렬하지 않음), 답란 최소 15mm는 `.answer-lines`/`.answer-lines--table`
  CSS를 그대로 써서 유지(줄이지 않음).
- 행 1개가 빈 페이지 공간보다도 큰 극단적 경우(이번 실제 데이터엔 없었음)는 강제로
  1행만 배치하고 issues에 "안전하게 배치할 수 없음 - 최종 출력 확정 차단 대상"을 남긴다 -
  QA 허용치를 늘리거나 글자를 줄이는 방식으로 우회하지 않는다.
- 분기 색상·서체·프레임(`.sheet`/`.sheet-head`/`.footer`/`--main`/`--point` 등)은
  전혀 건드리지 않았다 - `.half`/`.half--gap`(고정 절반 분할) 클래스만 1단계에서
  안 쓰게 됐을 뿐, 3단계(`buildStep3Page`)가 이미 쓰던 것과 같은 "`.sheet` 전체를
  쓰는 페이지" 패턴을 1단계에도 적용한 것뿐이다.

### 실측 결과 (승인 문서 6건 재빌드, before/after)

| 문서 | before 1단계 상태 | after 1단계 상태 | 1단계 페이지 수 |
|---|---|---|---|
| L1-Q4-W01 | FAIL(400/374) | **PASS** | 1 |
| L3-Q4-W01 | FAIL(415/374) | **PASS** | 1 |
| L3-Q4-W02 | FAIL(393/374) | **PASS** | 1 |
| L5-Q4-W01 | FAIL(430/374) | **PASS** | 1 |
| L5-Q4-W02 | FAIL(388/374) | **PASS** | 1 |
| L5-Q4-W10 | FAIL(388/374) | **PASS** | 1 |

**6건 전부 1단계 오버플로가 사라졌다.** 실제 데이터는 전부 "확장된 한 페이지" 안에서
해결됐고(고정 반면 예산만 없앴더니 충분했다), 진짜 다중 페이지 분기는 아래 4번의 합성
시험으로 별도 검증했다.

## 4. 다중 페이지 분기 자체 검증 (합성 시험 - 실제 7건엔 이 정도 분량이 없어서)

실제 DB의 서로 다른 4개 문서(L5-Q4-W10/L3-Q4-W01/L3-Q4-W02/L5-Q4-W01)에서 실제 어휘 행
16개를 그러모아(내용은 전부 실제 DB 값, 문서 하나에 합친 것만 합성) `TEST-MULTIPAGE-VOCAB-01`
문서를 만들어 빌드했다.

- 결과: **1단계가 3페이지로 자동 분리**(`02 1단계 1`/`03 1단계 2`/`04 1단계 3`) - 어휘
  16행이 처음 8행/다음 8행으로 나뉘고, OX는 별도 3번째 페이지에서 시작.
- **행 손실/중복 없음**: 페이지 1(id 353~360) + 페이지 2(id 172~175, 49~52) = 16개
  전부, 겹치는 id 없음(코드로 직접 대조 확인).
  - `표 머리글 반복`: 페이지 1엔 안내문+`<thead>`, 페이지 2엔 `<thead>`만(안내문 없음) -
    육안 스크린샷으로도 확인(아래 증거 경로).
  - `check.js` 전체 통과: 10페이지 전부 오버플로 없음, 콘텐츠 보존 대조 PASS(사전에
    있던 무관한 choice_multi 데이터 이슈 1건만 FAIL로 남음).
- 증거: `generated/TEST-MULTIPAGE-VOCAB-01/builds/2026-09-18T15-17-06-418Z/`
  (`data.json`/`index.html`/`check_report.json`), 스크린샷은 브라우저로 직접 확인(파일
  저장은 안 함 - 화면으로 머리글 반복과 줄바꿈 없음을 확인).

## 5. 선택지·표 열 정의 없는 문항 — 복원 가능성 조사

**별도 파일**: `momo_book_db/worksheet/docs/RESTORATION_CANDIDATES.md`

`raw_text`(추출 원문)를 근거로 8건을 조사했다(운영 DB는 손대지 않음, 파일로만 기록):

- **복원 가능(신뢰도 높음)**: id=835(L3-Q4-W01#7) - 원문에 표 헤더 2개+행 2개가
  그대로 남아 있어 `{"columns":["중국의 나라 이름","고구려와 어떤 일이 있었나요?"],"rows":2}`
  복원 근거가 명확함.
- **부분 복원(열만, 행 수는 근거 부족)**: id=840(L3-Q4-W02#4-1), id=423(L5-Q4-W01#1-1) -
  헤더는 보이지만 행 데이터가 잘려 있거나(840) 아예 없어서(423) 행 수는 추측이 됨 -
  **행 수는 원본 PDF 재확인 전까지 복원하지 않는 걸 권장**.
  - id=840은 문서 이름이 아니라 "장우나"라는 알 수 없는 단어 하나만 남아 있음(추출
    과정에서 잘린 것으로 추정) - 왜 이렇게 잘렸는지는 이번 조사로 확정 못 함, 원본
    PDF 대조 필요.
- **복원 불가**: id=114(L5-Q4-W10#3) - `raw_text` 자체가 DB에 `NULL`. 추출 단계에서
  원문이 안 남아서 이 저장소 자료만으로는 근거가 없음 - **계속 차단**.
- **재분류 후보(unknown → text_long)**: id=1607/1611(L1-Q4-W01), id=435(L5-Q4-W02) -
  선택지·표가 아니라 그냥 개방형 서술 질문인데 유형 배정이 안 된 것으로 보임(신뢰도
  중간 - 정확한 서술형 세부 설정은 원본 답란 크기 확인 필요).
- **재분류 후보(추가 작업 필요)**: id=843(L3-Q4-W02#5) - 편지 원문과 질문이 안 나뉘어
  있어 단순 재분류로 안 끝남(발췌문 분리 작업 필요, 신뢰도 낮음~중간).

근거가 약한 항목(840/423의 행 수, 843의 분리)은 **이번에 복원하지 않고 계속 차단** 상태로
뒀다 - 추측으로 운영 DB나 산출물을 고치지 않는다는 원칙을 지켰다.

## 6. 이번 범위에서 그대로 차단해 둔 것 (의도적)

- L5-Q4-W02#7: 참고자료 그림 축소 후에도 59px 초과 - Step2 개별 문항 밀도 문제,
  범용 장문 자동 분할 없이는 "정확한" 해결이 안 되는 영역이라 범위 밖으로 남김
  (요청서: "단독 블록이 한 페이지보다 긴 경우에는 구체적인 오류로 차단해도 된다").
- L3-Q4-W02#3, #4-2(`text_short_multi` 답 반면 단독 오버플로): 마찬가지로 Step2
  개별 문항 문제, 1단계 재배치 범위 밖.
- 장문 자동 분할(문항을 페이지 경계에서 쪼개는 것) 자체는 1단계·2단계 어디에도
  구현하지 않았다(요청서에 명시된 범위 제한 그대로 따름).

## 7. 재빌드 + QA + PDF 비교 (7건 전체, before/after)

명령: `python generate/extract_worksheet_json.py <doc>` → `node generate.js <doc>` →
`node check.js <html> <data.json> <report.json>`. 실행 로그는 세션 스크래치패드에
남겼고(`*_extract_after.log`/`*_generate_after.log`/`*_check_after.log`), 산출물은
전부 `momo_book_db/generated/<doc>/builds/<build_id>/`에 있다(아래 build_id 표).

| 문서 | before 페이지수 | after 페이지수 | before status | after status | after 남은 원인 |
|---|---|---|---|---|---|
| L1-Q4-W01 | 6 | 7(어휘 5개가 안내문+표 기준으로 재배치되며 1단계가 정확히 1페이지로 안정 - 이전엔 오버플로 상태로 1페이지에 억지로 꽉 채워져 있던 것) | FAIL | FAIL | #1,#5 unknown(재분류 후보, 미반영) |
| L3-Q4-W01 | 9 | 9 | FAIL | FAIL | #7 columns 없음(복원 후보 있음, 미반영) |
| L3-Q4-W02 | 9 | 9 | FAIL | FAIL | #3,#4-1,#4-2,#5 (데이터+Step2 밀도, 범위 밖) |
| L5-Q4-W01 | 10 | 10 | FAIL | FAIL | #1-1 columns 없음(부분 복원 후보, 미반영) |
| L5-Q4-W02 | 10 | 10 | FAIL | FAIL | #4 unknown, #7 여전히 59px 초과(범위 밖) |
| L5-Q4-W10 | 8 | 8 | FAIL | FAIL | #114 options 없음(복원 불가, raw_text 없음) |
| L5-Q4-W11 | 8 | 8 | **PASS**(1차 안정화에서 이미) | **PASS** | - |

**모든 문서에서 "1단계 오버플로" 항목 자체는 after에서 완전히 사라졌다**(위 3번 표 참고) -
"조판만 문제였던 부분"은 실제로 해결됐고, 아직 FAIL인 6건은 전부 **데이터 누락/재분류
문제 또는 Step2 개별 문항 밀도 문제**(3번 표에서 이미 두 문제가 섞여 있던 문서들)만
남았다 - 요청하신 완료 기준("조판만 문제였던 문서는 해결, 데이터 불완전 문서는 이유가
명확히 남아야 함")과 정확히 일치하는 결과다.

**PDF 검사**(check.js가 실제로 PDF 생성 + PyMuPDF 답란 괘선 검사까지 수행, 7건 전부):
텍스트 보존(콘텐츠 대조 전부 PASS, 7/7), 표 행 누락·중복(직접 대조 스크립트로 확인,
7/7 전부 없음 - 아래 "표 행 검증" 참고), 답란 15mm 미만 경고 0건(2건은 실제 오버플로로
남아 정직하게 차단), PDF 페이지 수-화면 페이지 수 일치 7/7.

**표 행 누락·중복 직접 검증**: `data.json`의 vocab/ox id 목록과 실제 생성된 HTML의
`data-vocab-id`/`data-ox-id` 목록을 전부 대조 - 7건 전부 개수 일치, 중복 0건, 누락 0건,
초과 0건.

**대표 페이지 육안 확인**: `TEST-MULTIPAGE-VOCAB-01`(4번 항목)의 1단계 1/2페이지를
브라우저로 직접 스크롤하며 확인 - 어휘 행이 안 잘리고, 표 머리글이 정확히 2페이지째에서만
반복되며, 겹침·클리핑 없음.

## 8. 정상본 승격/보존 재확인

라우터(`app/routers/momo_book_worksheet.py`) 코드는 이번 2차 안정화에서 건드리지 않았다 -
1차 안정화 때 만든 "check.js PASS일 때만 승격" 로직을 실제 라우터 함수(`_run_build`)로
다시 확인했다.

- `L5-Q4-W11`(위 표에서 유일한 PASS): 실제 라우터 `_run_build()` 호출 → `current.json`이
  새 build_id로 정상 승격됨을 확인.
- `L1-Q4-W01`(여전히 FAIL): 실제 라우터 `_run_build()` 호출 → `current.json`은 여전히
  `None`(원래도 정상본이 없었으므로 "유지"란 "계속 없음"을 뜻함 - 잘못 승격되지 않음을 확인).

## 9. 운영 배포·유료 API·git push

하지 않았다. `.env`/`OPENAI_API_KEY` 관련 변경은 이번 2차 안정화와 무관한 별도 요청
(이미지 생성 provider 추가) 건이며, 이번 조판 수정에는 이미지 생성 API를 호출하지 않았다
(참고자료 그림 CSS 수정은 기존 이미지 파일 크기 조정일 뿐 새 이미지 생성 없음).

## 10. 변경 파일 목록 (2차 안정화분)

- `momo_book_db/worksheet/scripts/paginate.js` — `paginateStep1` 신규
- `momo_book_db/worksheet/scripts/blocks.js` — 행/헤더 분리 함수 추가
- `momo_book_db/worksheet/scripts/generate.js` — `buildStep1Pages` 다중 페이지 재작성
- `momo_book_db/worksheet/build/styles.css` — `.reference .figure-block img` 크기 제한 추가
- `momo_book_db/worksheet/docs/RESTORATION_CANDIDATES.md` — 신규(복원 후보 조사)

`app/routers/momo_book_worksheet.py`, `app/templates/momo_worksheet/detail.html`은
1차 안정화 그대로 - 이번엔 안 건드림. 커밋·푸시는 안 함.

## 다음 시작점 (2차 안정화 이후)

1. id=835 복원(신뢰도 높음)을 검수화면에서 반영할지 결정 - `ui_config` 직접 편집 UI가
   검수화면에 아직 없다면 그것부터 필요할 수 있음.
2. id=840/423은 원본 PDF 재확인 후 행 수 확정, id=114는 원본 PDF에서 선택지 자체를
   다시 찾아야 함.
3. ~~unknown 4건 재분류~~ → **3차 안정화에서 3건 시험 완료**(843은 여전히 보류, 아래 참고).
4. ~~L5-Q4-W02#7 Step2 문항 밀도~~ → **3차 안정화에서 해결**(아래 참고).

---

# 3차 안정화 (2026-09-19)

목표: 데이터 복원 후보를 운영 DB가 아닌 별도 수정본에서 검증하고, Step2 고밀도 문항을
기존 디자인 안에서 안전하게 배치한다. 운영 DB 수정·배포·유료 API 호출·git push는 하지 않았다.

## 1. 기준 확정

**TEMPLATE_VERSION을 `2026-09-18.1` → `2026-09-19.1`로 올렸다** - 1·2차에서 실제로
마크업/배치 로직이 여러 번 바뀌었는데 버전을 안 올려서 manifest.json만으로 "어느 코드로
만든 빌드인지" 구분이 안 되던 문제를 이번에 고쳤다(3차 이후부터는 template_version이
바뀔 때마다 실제로 올릴 것).

승인 문서 7건을 새 버전으로 재빌드해 기준선을 저장했다:
**`momo_book_db/worksheet/docs/BASELINE_20260919.json`**
(각 문서의 build_id·input_hash·template_version·schema_version·QA status·문항별 실패 사유).

| 문서 | input_hash | template_version | QA status |
|---|---|---|---|
| L1-Q4-W01 | `5a41a482791eae9c` | 2026-09-19.1 | FAIL |
| L3-Q4-W01 | `c88498305a57afeb` | 2026-09-19.1 | FAIL |
| L3-Q4-W02 | `970cca6581767cf0` | 2026-09-19.1 | FAIL |
| L5-Q4-W01 | `db7fbf15db336dff` | 2026-09-19.1 | FAIL |
| L5-Q4-W02 | `78673ea7526dda99` | 2026-09-19.1 | FAIL |
| L5-Q4-W10 | `2db55955fc993bb2` | 2026-09-19.1 | FAIL |
| L5-Q4-W11 | `99d4dcd61261f728` | 2026-09-19.1 | PASS |

### W11이 "1차 전체 실패" 보고와 다르게 정상 판정된 이유 — **확인됨(기록 있음)**

1차 안정화 PROGRESS.md의 "8. 중요 발견"(승인 문서 7건 전부 실패) 표에는 L5-Q4-W11을
"답란 15mm 미만"으로 적었는데, 실제로는 그 시점 이후 W11이 PASS로 바뀌었다. 근거를
직접 대조했다:

- `generated/L5-Q4-W11/builds/2026-09-18T14-43-42-359Z/check_report.json`(1차 안정화의
  "전체 7건" 일괄 테스트 때 만들어진 빌드) → **status: FAIL**, 사유
  `"04 2단계 2": ["답란 2개가 15mm 미만(예: 43px)"]`.
- 이 43px는 당시 `.blk-blanks__row .answer-lines`의 `min-height: 11mm`(=41.6px) 규칙과
  거의 정확히 일치한다(15mm=56.7px라면 43px가 나올 수 없음). 즉 **이 빌드는 1차 안정화
  중 그 CSS를 15mm로 고치기 "전"에 만들어진 것**이다.
- CSS를 15mm로 고친 뒤(1차 PROGRESS.md "4. 조판 안전성" 항목) 이후 빌드
  (`2026-09-18T15-08-17-616Z` 이후, 2차 안정화 시작 시점 포함)부터는 W11이 계속
  **PASS**로 나왔다 - 이번 3차 기준선(`2026-09-18T15-31-23-241Z` 계열)도 동일.

**결론**: 모순이 아니라 **1차 안정화 보고서의 "7건 요약 표"가 CSS 수정 이후 재확인 없이
그대로 남은 documentation 문제**였다 - 코드는 이미 고쳐졌는데 표만 안 갱신됐다. 미확정으로
남길 필요 없이 build_id·타임스탬프·수치(43px)로 명확히 설명된다.

## 2. 데이터 복원 후보 시험

`RESTORATION_CANDIDATES.md`의 8건을 실제로 시험했다. **운영 DB(momo_book.db)는 전혀
수정하지 않았다** - `extract_worksheet_json.extract()`가 실제 DB에서 뽑은 원본 JSON을
그대로 두고, 그 위에 패치를 적용한 **JSON 패치**를 `RESTORED-<원본doc_id>`라는 별도
테스트 doc_id로 렌더링·QA했다.

**패치 기록 파일**: `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` - 문항 ID,
변경 전/후 값, 근거 원문 요약, 확실한 부분/미확정 부분을 전부 기록.

### 적용해서 시험한 것 (근거가 명확한 것부터)

| id | doc | 필드 | 변경 전 → 후 | 신뢰도 |
|---|---|---|---|---|
| 835 | L3-Q4-W01#7 | ui_config | `{}` → `{"columns":["중국의 나라 이름","고구려와 어떤 일이 있었나요?"],"rows":2}` | 높음 |
| 1607 | L1-Q4-W01#1 | ui_type | `unknown` → `text_long` | 중간 |
| 1611 | L1-Q4-W01#5 | ui_type | `unknown` → `text_long` | 중간 |
| 435 | L5-Q4-W02#4 | ui_type | `unknown` → `text_long` | 중간 |

**unknown→text_long 전환 전 활동 유실 여부 재확인**: 세 항목 모두 raw_text·question_text를
"그려/그림/그리기/색칠/선택/고르/표를/빈칸/연결/알맞은 말" 키워드로 재검색해 선택지·표·
그리기 등 다른 활동의 흔적이 있는지 확인했다 - **전부 0건**, 즉 처음부터 구조화된 활동이
아니라 순수 개방형 서술 질문이었다(단순히 검사를 통과시키려고 유형을 바꾼 게 아님 -
`RESTORATION_PATCHES.json`에 재검색 결과까지 기록).

### 시험 결과 (원본 vs 패치 적용, 구분)

| 문서 | 원본(패치 없음) | 패치 적용(RESTORED-*) |
|---|---|---|
| L1-Q4-W01 | FAIL(#1,#5 unknown) | **PASS**(0 issues) |
| L3-Q4-W01 | FAIL(#7 columns 없음 + 답 반면 넘침) | **PASS**(0 issues) - 단, 이건 2차 안정화만으론 안 됐고 아래 3번(Step2 배치 개선)까지 같이 적용된 결과 |
| L5-Q4-W02 | FAIL(#4 unknown) | **PASS**(경고 1건 - 참고이미지 저해상도, 차단 아님) |

id=835 하나는 복원해도 Step2 배치가 그대로면 여전히 FAIL이었다(반 반면 대신 표 전체를
답으로 써야 해서 더 커짐, 574px까지 넘침 관측) - **데이터 복원과 Step2 배치 개선은
서로 다른 문제였고, 이번에 둘 다 확인·해결했다.**

### 보류 (근거 부족 - 계속 차단, 필요한 원본 페이지 명시)

| id | doc | 사유 | 필요한 원본 확인 |
|---|---|---|---|
| 840 | L3-Q4-W02#4-1 | 열 제목만 있고 행 데이터가 "장우나" 한 단어에서 잘림 | source_page=4 재확인 필요 |
| 423 | L5-Q4-W01#1-1 | 열 제목만 있고 행 수 근거 없음 | source_page=3 재확인 필요 |
| 843 | L3-Q4-W02#5 | 편지 원문과 질문이 안 나뉨(excerpt 분리 선행 필요) | source_page=5 재확인 필요 |
| 114 | L5-Q4-W10#3 | raw_text 자체가 DB에 없음 | source_page=4 원본 PDF 확인 필요(추출 누락 원인도 같이) |

## 3. Step2 배치 개선

**대표 사례**: L5-Q4-W02#7(id=438, choice_ab + 참고이미지).

### 요소별 높이 실측(2차 안정화 CSS 수정 후, 3차 배치 개선 전)

`.half` 컨테이너 기준: `.q`(질문) 54px + `.reference`(참고자료, 이미지 포함) 213px +
`.blk-choice`(선택지+답란) 87px → 합 354px인데 실제 컨테이너 scrollHeight는 433px
(반면 예산 374px 대비 59px 초과) - margin·padding 누적분까지 포함해 반면 하나에는
안 들어가는 게 확인됐다. **"433px로 줄었다"는 것만으로는 반면 예산(374px) 미달을
해소하지 못했다** - 여전히 FAIL이었다(2차 안정화 보고에서도 이미 이렇게 기록함).

### 3단계 사다리 구현 (`paginate.js`의 `placeItem()`)

1. **반면 배치**: 기존처럼 발췌문+답을 합쳐서 반면 하나, 안 되면 발췌문 반면/답 반면
   분리 - 기존 로직 그대로 유지.
2. **한 페이지 전체 배치**: 반면으로 안 되면 머리말·푸터만 뺀 `.sheet` 전체 공간에
   (발췌문·출처 또는 그림·캡션) + (질문·선택지·답란)을 **통째로** 실측(`overflowFull`).
3. **의미 단위로 다음 페이지 이동**: 그래도 안 되면 두 단위를 각각 별도 전체 페이지로
   분리 - "발췌문·출처"는 항상 붙어 다니고, "그림·캡션"도 항상 붙어 다니고, "질문·선택지·
   답란"도 항상 붙어 다닌다(내부를 더 쪼개지 않음). 분리된 상태에서도 순서(발췌문 페이지
   → 답 페이지)는 원래 순서 그대로 유지.
4. 그래도(단위 하나가 전체 페이지보다 큼) 안 되면 - 이번 범위에서 허용된 대로 - 구체적
   이유를 issues에 남기고 차단한다(문장 자동 분할 미구현, 범위 밖 그대로).

`generate.js`의 `buildStep2Pages`도 반면 쌍 페이지와 "페이지 하나 전체" 페이지를 섞어서
순서대로 조립하도록 수정(`{fullpage: html}` 마커 추가).

### 그림 크기 재확인 — "작아졌다"가 아니라 "종횡비·유효 해상도 기준 통과"를 직접 측정

L5-Q4-W02#7이 이제 반면이 아니라 페이지 전체를 쓰게 되면서 그림도 다시 실측했다
(2차 안정화 때 잰 "433px"는 반면에 낑겨 있던 수치라 최종 배치와 다름):

- 원본 픽셀: 162×251, 종횡비 0.645
- 최종 렌더 크기: 98×151px(26×40mm), 종횡비 0.645 - **원본과 정확히 일치, 찌그러짐 없음**
- 인쇄 150dpi 기준 필요 원본 픽셀: 152×236 - 실제 원본(162×251)이 이보다 크므로
  **`meetsRequirement: true`**(가이드 4.4 공식 그대로 적용, 임의 기준 완화 없음)

본문 글자 크기·답란 높이는 전혀 줄이지 않았다(오히려 반면이 아니라 페이지 전체를
쓰게 되면서 `.blk-choice`의 답란이 87px→313px로 더 넉넉해짐 - `answer-lines`는
`flex:1`이라 남는 공간을 그대로 흡수).

### 실제 브라우저 육안 확인

`http://127.0.0.1:8795/generated/L5-Q4-W02/builds/.../index.html`의 "09 2단계 7"
페이지를 스크린샷으로 확인 - 발췌문(청나라 황제 관련 인용문)+출처(p.201), 참고 자료
그림(비석 사진, 선명하고 비율 정상), 질문 7번, 선택지 2개(없애야 해요!/보존해야 해요!),
답란까지 페이지 하나에 겹침·잘림 없이 전부 들어있음을 확인.

### 3단계(의미 단위 분리)가 실제로 필요한 경우도 별도로 검증

승인 문서 7건 실제 데이터로는 전부 2단계(페이지 전체 배치)까지만 필요했고 3단계까지
간 경우가 없어서, 실제 DB 문항(L5-Q4-W10 id=115)의 발췌문을 기계적으로 길게 늘린
합성 시험(`TEST-STEP2-SPLIT-16`, 실제 원문 16번 반복 - 새 문장 지어내지 않음)으로
3단계를 강제 유발했다:
- 결과: 발췌문·출처가 "06 2단계 4" 페이지, 질문·선택지·답란(id=115)이 바로 다음
  "07 2단계 5" 페이지로 분리 - 순서 유지, 내용 완전, 겹침 없음(코드로 페이지별 내용 대조).
- 더 길게 늘리면(`TEST-STEP2-SPLIT-18` 이상) 발췌문 단독으로도 전체 페이지를 넘겨
  "안전하게 배치할 수 없음"으로 정확히 차단되는 것도 확인(4번 - 범용 문장 분할 없이
  구체적 오류로 차단하는 경로).

## 4. 검증과 보고

### 원본 vs 복원 후보 적용 결과 (구분)

**원본 7건 재시험**(Step2 배치 개선 반영, 데이터는 무수정):

| 문서 | 페이지수 | status | 남은 문항 ID·사유 |
|---|---|---|---|
| L1-Q4-W01 | 7 | FAIL | #1, #5 `unknown`(재분류 후보 있음, 미반영) |
| L3-Q4-W01 | 9 | FAIL | #7(id=835) `table_compare` columns 없음(복원 후보 있음, 미반영) |
| L3-Q4-W02 | 11 | FAIL | #4-1(id=840) columns 없음, #5(id=843) `unknown`(둘 다 보류 - 근거 부족) |
| L5-Q4-W01 | 10 | FAIL | #1-1(id=423) columns 없음(부분 복원 후보, 미반영) |
| L5-Q4-W02 | 10 | FAIL | #4(id=435) `unknown`(재분류 후보 있음, 미반영) |
| L5-Q4-W10 | 8 | FAIL | #3(id=114) options 없음(복원 불가) |
| L5-Q4-W11 | 8 | **PASS** | - |

**Step2 배치 개선만으로 모든 문서에서 "오버플로/반면 넘침" 페이지 레벨 이슈가 완전히
사라졌다** - L3-Q4-W02#3/#4-2(2차 안정화 시점 남아있던 text_short_multi 답 반면 오버플로)도
이번 Step2 수정으로 같이 해결됨(추가 조사로 확인). 남은 FAIL은 전부 데이터 누락/재분류
문제뿐이다.

**복원 후보 적용 시험**(`RESTORED-*`, 별도 테스트 doc_id):

| 문서 | 페이지수 | status | 비고 |
|---|---|---|---|
| RESTORED-L1-Q4-W01 | 7 | **PASS** | issues 0건 |
| RESTORED-L3-Q4-W01 | 9 | **PASS** | issues 0건(id=835 복원 + Step2 배치 개선 둘 다 필요했음) |
| RESTORED-L5-Q4-W02 | 10 | **PASS** | 경고 1건(참고이미지 저해상도 - 차단 아님, 원본 이미지 자체의 한계) |

**"RESTORED-*"는 운영 승인본이 아니다** - `current.json`(정상본 포인터)을 만드는
`_promote()`는 라우터(`app/routers/momo_book_worksheet.py`)의 `_run_build()`를 통해서만
호출되는데, 이번 3차 안정화의 모든 시험은 `generate.js`/`check.js`를 직접 호출했을 뿐
라우터를 거치지 않았다 - **`generated/RESTORED-*/current.json`은 존재하지 않음을
직접 확인**했다(아래 표). 원본 7건 중 여전히 FAIL인 6건도 `current.json`이 없어
"이전 정상본"이 원래도 없었으므로 "유지 안 됨" 상황 자체가 발생하지 않았다(있던 걸
잃은 게 아님) - L5-Q4-W11만 유일하게 `current.json`이 있고 이번 3차에서도 안 건드림.

| doc | current.json |
|---|---|
| L1-Q4-W01, L3-Q4-W01, L3-Q4-W02, L5-Q4-W01, L5-Q4-W02, L5-Q4-W10 | 없음(원래도 없었음) |
| L5-Q4-W11 | 있음(2차 안정화 때 승격된 그대로, 3차에서 안 건드림) |
| RESTORED-L1-Q4-W01, RESTORED-L3-Q4-W01, RESTORED-L5-Q4-W02 | 없음(자동 승격 안 됨 확인) |

### 대표 사례 PDF (변경 전후)

`momo_book_db/worksheet/docs/evidence_20260919/`:
- `L5-Q4-W02_before_p3.pdf` - 3차 안정화 전 빌드(2차 안정화 상태, item#7이 반면에
  끼여 FAIL이던 상태)
- `L5-Q4-W02_after_p3.pdf` - 3차 안정화 후 빌드(item#7이 페이지 전체를 쓰며 PASS)

PDF 자체의 페이지 렌더링 도구(poppler)가 이 환경에 없어 PDF 파일을 이미지로 직접
열람하지는 못했다 - 대신 **PDF 생성 자체는 완료**(check.js가 각 build마다 실제로
`page.pdf()`를 호출해 PDF 페이지 수·답란 괘선까지 검사하는 절차를 7건 전부 재확인했고),
육안 확인은 동일 HTML을 브라우저로 직접 렌더링한 스크린샷으로 대체했다(위 3번 항목).

### 표 행 누락·중복·겹침·답란·그림 (7건 + 합성 시험)

- 표 행/문항 누락·중복: `data-vocab-id`/`data-ox-id`/`data-qa-id`를 data.json과 직접
  대조 - 7건 전부 0건(1·2차와 동일 방식으로 재확인).
- 답란: 15mm 미만 경고 0건(7건 전부).
- 겹침: 반면 단위 오버플로(`.half`) 0건(7건 전부) - 1·2차에서 만든 검사 그대로 재사용.
- 그림: 위 3번 "그림 크기 재확인" 참고 - 종횡비·유효 해상도 기준 명시적으로 통과 확인.

## 5. 변경 파일 목록 (3차 안정화분)

- `momo_book_db/worksheet/scripts/paginate.js` — `placeItem()`(3단계 사다리), `buildBundles()`,
  `paginateStep2()` 재작성
- `momo_book_db/worksheet/scripts/generate.js` — `TEMPLATE_VERSION` 갱신, `buildStep2Pages`가
  `{fullpage}` 페이지 처리하도록 수정
- `momo_book_db/worksheet/docs/BASELINE_20260919.json` — 신규(기준선)
- `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` — 신규(패치 기록)
- `momo_book_db/worksheet/docs/evidence_20260919/` — 신규(대표 사례 PDF 2개)

`app/routers/momo_book_worksheet.py`, `momo_book_db/generate/extract_worksheet_json.py`,
`momo_book_db/worksheet/build/styles.css`는 3차에서 안 건드림(2차 상태 그대로). 운영
DB(`momo_book.db`) 수정 없음, 배포 없음, 유료 API 호출 없음(이미지는 기존 파일만
재사용), git push 없음.

## 다음 시작점 (3차 안정화 이후)

1. id=835(높은 신뢰도)를 검수화면에서 실제로 반영할지 결정 - 반영 시 L3-Q4-W01은
   승인 가능한 상태가 됨(RESTORED 시험에서 이미 PASS 확인).
2. id=1607/1611/435(중간 신뢰도, text_long 재분류)도 검수화면 반영 검토 - 반영 시
   L1-Q4-W01, L5-Q4-W02 둘 다 승인 가능(이미 PASS 확인). starter 문구 등 세부는
   원본 PDF 대조 권장.
3. id=840/423/843/114는 원본 PDF 재확인이 먼저 필요(각 source_page 명시함) - 이번
   범위에서 더 할 수 있는 게 없음.
4. ~~검수화면 반영 → 라우터 경로 재확인~~ → 4차에서 **별도 편집 프로젝트**로 콘텐츠 수정
   자체는 가능해졌음(아래 참고). 운영 DB 반영은 여전히 사람이 검수화면에서 판단할 일.

---

# 4차: 실제 PDF 검수 마무리 + 최소 편집 기능 (2026-09-19)

기존 생성기(`generate.js`)·디자인(`styles.css`)·QA(`check.js`)는 전혀 수정하지 않고
그대로 재사용했다(단, `blocks.js`의 `answerLines()`에 "답란 높이 조절" 편집 기능을
위한 선택적 인자 하나만 추가 - 기존 호출부는 인자를 안 주면 동작이 100% 그대로임을
확인). 운영 DB(`momo_book.db`) 수정 없음, 배포 없음, 유료 API 호출 없음, git push 없음.

## 1. 실제 PDF 검수

PyMuPDF `page.get_pixmap()`으로 대표 PDF(L5-Q4-W02, 3차 안정화 전/후)를 실제 PNG로
래스터화하는 스크립트를 새로 만들었다: **`momo_book_db/worksheet/scripts/render_pdf_review.py`**
(사용법: `python render_pdf_review.py <pdf> <출력폴더> [확대할 페이지 콤마목록]`).

- 전체 페이지 축소 모음(contact sheet, 4열 그리드)과 문제였던 페이지의 150dpi 확대본을
  둘 다 만든다. **PNG 생성 성공 여부(`render_log.json`)와 실제 육안 검수는 명시적으로
  구분**해서 기록한다(요청사항).
- 만드는 과정에서 실제 버그를 하나 발견·수정했다: 처음엔 PyMuPDF `Pixmap.copy()`로
  그리드를 합성했는데 **표지 1장만 나오고 나머지 9장이 안 붙는 버그**가 있었다(각 페이지
  자체는 정상 렌더됐는데 합성만 실패 - `render_log.json`의 `pages` 배열엔 10장 전부
  `ok:true`로 기록돼 있어서 "생성은 됐는데 합성이 깨졌다"는 걸 구분할 수 있었다). PIL
  (Pillow, 기존 requirements.txt에 이미 있음)로 직접 그리드를 합성하도록 교체해 해결.
- **실제 육안 검수 결과**(`momo_book_db/worksheet/docs/evidence_20260919/L5-Q4-W02_pngs/`):
  - `contact_sheet.png`: 10페이지 전부 정상 구성 확인(표지/1단계/2단계 1~7/3단계).
  - `page_06.png`(item#4, unknown 타입): 본문·발췌문·참고자료 이미지(옛 훈민정음 언해본
    인용 이미지) 전부 선명, 글자 깨짐·잘림·겹침 없음.
  - `page_09.png`(item#7, 3차 안정화의 대표 사례): 발췌문+출처(p.201)+참고이미지(비석
    사진, 선명·비율 정상)+선택지 2개+답란 전부 한 페이지에 겹침·잘림 없이 배치, 쪽번호
    "8" 정상 표기.
  - 답란: 실제 PDF 이미지에서 괘선이 끊김 없이 인쇄된 것 육안 확인(이전부터 check.js가
    PyMuPDF로 하던 검사를 이번엔 눈으로도 재확인).
- 대표 PDF 파일: `L5-Q4-W02_before_p3.pdf`(3차 전, item#7 반면에 끼여 있던 상태),
  `L5-Q4-W02_after_p3.pdf`(3차 후), `L5-Q4-W02_current.pdf`(이번 검수에 실제로 쓴 최신본).

## 2~4. 최소 편집 기능

### 아키텍처 (기존 파이프라인 재사용)

- **저장소**: `momo_book_db/worksheet/edit_projects/<project_id>/` - 운영 DB와 완전히
  분리된 파일 저장소. 새 모듈 `momo_book_db/worksheet/editor/project_store.py`.
  - `base_data.json`: 프로젝트 생성 시점에 얼린 원본(또는 복원 패치 적용) 데이터 스냅샷 -
    이후 운영 DB가 바뀌어도 이 프로젝트는 영향 안 받음. **원본 문항 ID(discussion_qa.id)를
    그대로 키로 씀** - 콘텐츠/디자인 오버라이드 전부 이 ID로 연결.
  - `versions/<version_id>/`: 명시적 저장마다 하나. `content_overrides.json`(문장·그림 -
    **콘텐츠 변경**)과 `design_overrides.json`(답란 높이 - **디자인·배치 변경**)을
    **분리 저장**(요청사항). `manifest.json`에 QA 상태·이전 버전 포인터 기록.
  - `assets/`: 업로드 그림. 파일명은 **내용 sha256 기반 고유 asset_id**, 원본 파일명·
    sha256·업로드 시각은 `assets.json`에 별도 기록. **원본 교재 이미지
    (`momo_book_db/extracted_images/<원본doc_id>/`)는 절대 안 건드림** - 편집 프로젝트
    자산은 `extracted_images/EDIT-<project_id>/` 전용 서브폴더에만 복사됨(기존
    `docImageSrc()`의 상대경로 규칙을 그대로 활용하기 위한 배치 - 생성기 자체 코드는
    안 고침).
- **버전 포인터 2개**: `current_version`(최근 저장 - QA 통과 여부 무관, 사용자가 계속
  고쳐나갈 수 있게 편집 내용 자체는 안 잃음) / `last_good_version`(QA를 통과했던 가장
  최근 버전 - "마지막 정상본", 미리보기 기본값). `momo_book_worksheet.py`의 "실패한
  빌드는 정상본으로 승격하지 말고 기존 정상본 유지" 패턴을 편집 프로젝트에도 동일하게 적용.
- **빌드**: `build_effective_data()`가 base+overrides를 합쳐 `EDIT-<project_id>`라는
  합성 doc_id의 data.json을 만들고, 기존 `generate.js`/`check.js`를 explicit outPath
  인자로 그대로 호출(라우터 `app/routers/momo_worksheet_editor.py`의 `_run_pipeline()`,
  `momo_book_worksheet.py`의 `_run_build()`와 같은 패턴이나 **`_promote()`를 절대
  호출하지 않음** - 그래서 `generated/EDIT-*/current.json`(운영 승인 포인터)이 물리적으로
  생길 수 없음).
- **UI**: `app/routers/momo_worksheet_editor.py` + `app/templates/momo_worksheet_editor/`
  (신규 화면). 기존 미리보기 화면(`momo_worksheet/detail.html`)에 "✏️ 편집 프로젝트"
  링크만 추가(기존 화면 자체는 안 건드림). 문항 목록(문항 선택) → 선택한 문항의 문장
  수정(textarea) + 그림 업로드·교체(파일 선택 + 대상 필드 선택) + 답란 높이 조절(mm
  입력, 최소 15mm) 폼 → 저장 시 새 버전 생성 + 재조판 + QA를 **동기적으로** 실행하고
  PASS/FAIL/BLOCKED 배지와 "검사 중..." 저장 버튼 상태를 화면에 표시.

### `blocks.js`의 유일한 변경 - 답란 높이 조절 지원

`answerLines(extraClass, heightMm)` - 세 번째... 아니 두 번째 인자를 새로 추가해서
있으면 `style="min-height:{max(15,값)}mm"`을 인라인으로 붙인다(15mm 정책은 여기서도
한 번 더 강제 - project_store.py에서 이미 걸지만 이중 방어). **기존 호출부는 인자를
안 주므로 동작이 완전히 그대로임을 재빌드로 재확인**(아래 "회귀 없음" 참고).

## 실제 완료 시험 (요청하신 순서 그대로 실행 - 전부 실제 함수 호출로 검증)

L5-Q4-W11(원본 상태에서 PASS)을 첫 대상으로 사용했다.

| 단계 | 실행 | 결과 |
|---|---|---|
| 1. 프로젝트 생성 | `create_project('L5-Q4-W11', ...)` | `project_id=L5-Q4-W11-625d46b8` |
| 2. 문장 수정 | item#120(order 1) `question_text` 교체 | 저장됨 |
| 3. 그림 교체 | 실제 파일(교재 표지 이미지, 38,821 bytes) 업로드 → `reference_image_path`에 연결 | asset_id=`ab4dea3b2dbc9b175b9eefe4`, sha256 기록 |
| 4. 답란 조절 | `answer_height_mm=25` | 저장됨(15mm 미만 방지 로직도 코드상 확인) |
| 5. 저장 | `save_version()` | `version_id=v1789746893482` |
| 6. 재조판+QA | `_run_pipeline()` | **status=PASS**, 8페이지, 콘텐츠 대조 통과 |
| 7. "브라우저 종료" | (프로세스 종료) | - |
| 8. 새 세션에서 다시 열기 | **완전히 새 파이썬 프로세스**로 `load_project()` 재호출 | 성공 |
| 9. 문장/답란 유지 확인 | overrides 재로드 | `question_text` 그대로, `answer_height_mm=25` 그대로 |
| 10. 그림 해시 확인 | 저장된 파일 sha256 재계산 | 업로드 시 해시와 **정확히 일치** |
| 11. 변경 안 한 문항 확인 | 나머지 7개 문항(#121~127)을 base_data와 필드별 대조 | question_text/excerpt_text/reference_text/이미지 경로 **전부 동일** |
| 12. PDF 생성 | `page.pdf()` | `evidence_20260919/editor_test.pdf` |
| 13. PNG 변환·육안 확인 | `render_pdf_review.py` | `page_03.png`에서 수정된 문장 + 교체된 그림 + 넉넉해진 답란 **육안 확인** |
| 14. 넘치는 수정 시험 | 문항 텍스트를 원문 80회 반복(9,120자)으로 강제 확대 | `status=FAIL`(예상대로) |
| 15. 마지막 정상본 유지 확인 | `current_version`은 새 버전으로 이동(편집 내용 보존), `last_good_version`은 **그대로**(안 바뀜) | **확인됨** - assert로 직접 검증 |

### 저장 실패·손상 시나리오 (Part 3 요구사항, 전부 실제 실행)

| 시나리오 | 결과 |
|---|---|
| 정상 내보내기(zip) → 불러오기 | 새 project_id로 복원, overrides·자산 해시 전부 원본과 동일 확인 |
| 손상된 zip 불러오기 | `ProjectError`로 명확히 거부, **기존 프로젝트는 전혀 안 바뀜**(current_version 동일 확인) |
| 자산 파일이 빠진(sha256 대조 실패) zip 불러오기 | `ProjectError`로 거부("자산 파일 누락/손상: uploaded_test_image.jpeg"), 기존 프로젝트 보존 |
| 허용 안 되는 확장자(.exe) 업로드 | `ProjectError`로 즉시 거부, 파일 저장 자체가 안 됨 |

### 복원 후보 프로젝트의 출처 표시 + 운영 승격 차단 (Part 3 마지막 요구사항)

L3-Q4-W01을 `source='restoration'`으로 프로젝트 생성 →
`data_origin='restoration_candidate'`, `restoration_patch_ids=[835]`가 project.json에
기록되고 화면에 계속 배지로 표시됨(코드 확인). 편집 없이 그대로 빌드 → **QA PASS**
(3차 안정화 시험과 동일 결과 재확인) → 그런데도:
- `generated/EDIT-L3-Q4-W01-c78ad0e4/current.json`(운영 승인 포인터) **생성 안 됨** 확인.
- 원본 DB의 `documents.review_status`가 여전히 `'approved'`(원래 값 그대로, 편집
  프로젝트가 손댄 적 없음) 확인.

즉 **"QA 통과만으로 콘텐츠 승인 상태를 변경하지 마"가 코드 구조상으로 지켜진다** -
편집 프로젝트 쪽엔애초에 `_promote()`를 호출하는 경로 자체가 없다.

## 변경 파일 목록 (4차분)

- `app/routers/momo_worksheet_editor.py` — 신규(편집 라우터)
- `app/templates/momo_worksheet_editor/{list,detail,import_error}.html` — 신규
- `app/templates/momo_worksheet/detail.html` — "편집 프로젝트" 링크 1줄 추가
- `app/templates/momo_review/detail.html`, `app/routers/momo_book_review.py` — **3차
  이전(이미지 생성 provider 추가 세션)부터 있던 미커밋 변경, 이번 4차에서 추가로 안 건드림**
- `app/main.py` — 라우터 등록 1줄
- `momo_book_db/worksheet/editor/project_store.py` — 신규(편집 프로젝트 저장소)
- `momo_book_db/worksheet/scripts/render_pdf_review.py` — 신규(PDF→PNG 검수 도구)
- `momo_book_db/worksheet/scripts/blocks.js` — `answerLines()`에 선택적 높이 인자 추가만
- `momo_book_db/worksheet/edit_projects/` — 신규(이번 시험으로 생긴 프로젝트 3개 -
  `L5-Q4-W11-625d46b8`, 그 export/import 왕복본 `L5-Q4-W11-25fb7596`, `L3-Q4-W01-c78ad0e4`)
- `momo_book_db/worksheet/docs/evidence_20260919/` — PDF/PNG 증거 추가

`generate.js`/`paginate.js`/`check.js`/`styles.css`/`extract_worksheet_json.py`/
`app/routers/momo_book_worksheet.py`는 4차에서 **전혀 안 건드림**(기존 생성기·디자인·QA
그대로 재사용 요구사항).

## 미검증·범위 밖

- 편집 화면 자체를 실제 브라우저(로그인 세션)로 클릭해보진 않았다 - FastAPI 앱 전체에
  걸린 로그인 미들웨어 때문에, 1~3차와 마찬가지로 **라우터가 호출하는 실제 함수를
  직접 호출**해서 검증했다(HTTP 계층을 걷어내고 그 안쪽 로직을 검증 - 템플릿 렌더링
  자체는 Jinja2로 별도 구문 검사함). 화면 레이아웃이 실제로 의도대로 보이는지는
  사람이 한 번 열어봐야 한다.
- 어휘/OX 항목의 답란 높이 조절, 3단계(글쓰기) 항목 편집은 이번 "최소" 범위에 안 넣음
  (Step2 문항만 지원) - 필요하면 다음 단계.
- 동시 편집(같은 프로젝트를 여러 명이 동시에 저장) 충돌 방지는 구현 안 함 - momo_book_worksheet.py의
  문서별 락과 달리 이번엔 프로젝트 저장이 파일 기반 `os.replace()` 원자적 쓰기라 "덮어써진
  절반짜리 파일"은 없지만, "누가 먼저 저장했는지"에 따라 나중 저장이 이긴다(동시성 정책
  자체가 이번 범위 밖).
- PDF 실제 인쇄(종이 출력)는 여전히 미검증(프린터 없음) - PDF 생성 + PyMuPDF 실측
  래스터화 + 육안 PNG 검수까지가 이 환경에서 할 수 있는 최대.

## 다음 시작점

1. 편집 화면을 실제 브라우저로 한 번 열어서 레이아웃·업로드 폼 동작을 눈으로 확인.
2. 어휘/OX/3단계 편집 지원 확대(현재는 Step2 문항만).
3. RESTORATION_PATCHES.json의 id=1607/1611/435도 `source='restoration'` 프로젝트로
   편집 시험(이번엔 id=835 하나짜리 L3-Q4-W01만 실제로 돌려봄).
4. 편집 프로젝트에서 "이 정도면 됐다" 싶은 결과가 나오면, 그걸 검수화면에 반영하는
   건 여전히 사람이 원본 PDF와 대조해 판단할 일 - 편집 프로젝트가 자동으로 운영 DB에
   쓰는 경로는 의도적으로 안 만듦.

---

# 5차: 실제 브라우저 검증 (2026-09-19)

목표: 4차에서 "함수 직접 호출로 검증, 실제 화면은 안 열어봄"으로 남겨둔 항목을
**실제 브라우저 화면 조작**으로 검증. 함수 직접 호출로 대체하지 않았고, 인증을 우회·비활성화
하지 않았다(기존 세션 쿠키 로그인 미들웨어를 그대로 사용, `/logout`으로 일부러 로그아웃한
뒤 로그인 폼에 직접 타이핑해서 재로그인). 운영 배포는 하지 않았다(로컬 개발 서버만 사용).

## 실행한 개발 서버

```
cd C:\Users\aproa\aprolabs
venv/Scripts/python.exe -m uvicorn app.main:app --port 8100 --host 127.0.0.1
```

이번 검증 동안 계속 이 서버로 시험했고(백그라운드 프로세스), 검증 종료 시점에도 **켜둔
채로 남겨뒀다** - 사용자가 직접 접속해 보기로 한 요청사항이라 서버는 끄지 않았다.
Claude가 조작한 브라우저 탭만 정리(닫음)했다.

## 화면 연결 오류 발견 및 수정 — 실제 버그였음

시험 순서(문장 수정→그림 업로드→답란 25mm→저장→…→PDF 다운로드)를 그대로 실행하려고
편집 화면을 열어보니, **PDF를 실제로 내려받을 방법이 화면에 없었다** - 미리보기 iframe과
내보내기(zip) 버튼만 있고 PDF 다운로드 링크 자체가 없는 상태(4차의 "실제 완료 시험" 표
12번 항목은 `page.pdf()`를 직접 호출해서 테스트했을 뿐, 화면에 그 경로로 가는 버튼은 없었음).
아래처럼 수정해서 해결했다:

- **신규** `momo_book_db/worksheet/scripts/export_pdf.js` - `check.js`가 내부적으로 쓰는
  것과 같은 `page.pdf({printBackground:true, preferCSSPageSize:true})` 호출을 재사용해
  임시가 아닌 실제 다운로드용 PDF를 생성(새 PDF 생성 로직을 발명하지 않음).
- **수정** `app/routers/momo_worksheet_editor.py` - `GET /{project_id}/pdf/{version_id}`
  라우트 추가(해당 버전 index.html의 mtime이 기존 export.pdf보다 새로우면 재생성, 아니면
  캐시된 파일 그대로 서빙). `editor_detail()`의 템플릿 컨텍스트에 `preview_version` 전달
  누락도 같이 고침(버튼 URL을 만드는 데 필요).
- **수정** `app/templates/momo_worksheet_editor/detail.html` - 미리보기 iframe 아래에
  "📄 PDF 다운로드(현재 버전/마지막 정상본)" 버튼 추가.

## 실제 화면 조작 시험 결과 (전 단계 스크린샷으로 직접 확인)

L5-Q4-W11을 대상으로 새 편집 프로젝트를 화면에서 생성(`L5-Q4-W11-1b4009d3`) →
실제 마우스 클릭·키보드 입력으로 진행:

| # | 단계 | 화면 조작 | 결과 |
|---|---|---|---|
| 1 | 로그인 | `/logout` → `/login`에서 이메일·비밀번호 직접 타이핑 → 로그인 버튼 클릭 | 탭 제목이 "전체 문항 - Aprolabs"로 바뀜(실제 인증 통과 확인) |
| 2 | 프로젝트 생성 | 새 프로젝트 폼에서 `L5-Q4-W11` 문서 선택(네이티브 `<select>`) → 생성 | `project_id=L5-Q4-W11-1b4009d3` |
| 3 | 문항 선택 | 문항 목록에서 #1(choice_ab) 클릭 | 편집 폼에 해당 문항 로드 |
| 4 | 문장 수정 | textarea 내용을 `[5차 브라우저 시험] 벤담은 적은 수의 사람이 쾌락을 얻고 많은 수의 동물이 고통을 당하는 사냥을 싫어했어요 - 문장을 실제 화면에서 수정했습니다.`로 교체 | 입력값 화면에 반영 확인 |
| 5 | 그림 업로드 | `<input type=file>`로 실제 이미지 파일(38,821 bytes, 교재 표지) 업로드, 대상 필드 `excerpt_image_path` | 파일명 화면에 표시됨 |
| 6 | 답란 높이 | `answer_height_mm` 입력란에 `25` 입력 | - |
| 7 | 저장 | "저장하고 다시 조판·검사" 버튼 클릭 | 버튼이 "저장 중..."으로 바뀌고 비활성화, 약 10초 후 새로고침 |
| 8 | QA 확인 | 저장 후 화면 | **QA 상태: PASS** 배지 표시 |
| 9 | 자산 확인 | "업로드된 그림 자산" 목록 | `browser_test_upload.jpeg`, `asset_id=ab4dea3b2dbc9b175b9eefe4` 표시 - 업로드 원본 파일의 sha256 앞자리와 **정확히 일치**(별도로 미리 계산해서 대조) |
| 10 | PDF 다운로드 | "📄 PDF 다운로드(현재 버전)" 버튼 클릭 | `C:\Users\aproa\Downloads\L5-Q4-W11-1b4009d3_v1789747995576.pdf` 실제 파일로 저장됨 |
| 11 | 브라우저 종료 | 탭 닫기(`tabs_close_mcp`) | - |
| 12 | 재로그인 | 새 탭 열고 `/logout`→`/login`으로 다시 이동, 이메일·비밀번호 새로 타이핑해 재로그인 | 성공(전체 문항 화면 재진입 확인) - **주의**: 같은 호스트(127.0.0.1)에 이전 세션 쿠키가 남아 있으면 로그인 폼 없이 바로 통과할 수 있어서, 일부러 `/logout`을 먼저 호출해 실제 로그인 절차를 눈으로 확인했다 |
| 13 | 재열기 | URL 직접 이동으로 같은 프로젝트(`/momo-worksheet-editor/L5-Q4-W11-1b4009d3`) 재접속 | 문항#1에 "●수정됨" 표시, 문장/이미지/25.0mm/PASS/버전 이력/자산 목록 **전부 저장된 그대로 유지됨을 화면에서 확인** |
| 14 | PDF 재다운로드 | "PDF 다운로드" 버튼 다시 클릭 | `L5-Q4-W11-1b4009d3_v1789747995576 (1).pdf`로 저장 |

**재열기 후 다시 받은 PDF와 종료 전 PDF의 sha256이 완전히 동일함을 확인**
(`682d1154bac6ef4f9d626578b0a6d6986991cb671512473c90604bcecfd0197d` - 두 파일 일치) -
재로그인·재열기 이후에도 같은 버전을 안정적으로 재생성/서빙함을 실측으로 확인.

## 그 과정에서 발견한 브라우저 자동화 자체의 이슈 (앱 버그 아님, 기록만)

- 로그인 폼에 처음 타이핑을 시도했을 때, 페이지 로드 직후의 요소 참조로 클릭+타이핑을
  했더니 화면상으로는 제출까지 됐지만 실제로는 **두 입력란 모두 비어 있었다**(스크린샷으로
  발견). 좌표를 직접 지정한 클릭+타이핑으로 재시도하고, 제출 전에 스크린샷으로 값이
  실제로 들어갔는지 확인하는 방식으로 해결 - 이건 앱 코드가 아니라 자동화 타이밍 문제였다.
- 문서 선택 `<select>`를 클릭하면 네이티브 OS 드롭다운이 뜨면서 스크린샷 캡처가 멈추는
  현상이 있어 `Escape`로 팝업을 닫은 뒤 포커스가 남은 상태에서 문서명을 타이핑(브라우저
  기본 type-ahead 동작)해 우회했다 - 이 역시 앱 버그가 아니라 네이티브 컨트롤 특성.

## 변경 파일 목록 (5차분)

- `momo_book_db/worksheet/scripts/export_pdf.js` — 신규
- `app/routers/momo_worksheet_editor.py` — `GET /{project_id}/pdf/{version_id}` 라우트 추가,
  `editor_detail()`에 `preview_version` 컨텍스트 전달 추가
- `app/templates/momo_worksheet_editor/detail.html` — PDF 다운로드 버튼 추가
- `momo_book_db/worksheet/edit_projects/L5-Q4-W11-1b4009d3/` — 이번 브라우저 시험으로
  생긴 프로젝트(실제 화면 조작으로 생성)

`generate.js`/`paginate.js`/`check.js`/`styles.css`/`extract_worksheet_json.py`/
`project_store.py`/`blocks.js`는 5차에서 전혀 안 건드림(기존 생성기·디자인·QA 재사용
요구사항 유지). 커밋·푸시·운영 배포는 하지 않았다.

## 결론

요청하신 5단계 시험 순서(문장 수정→그림 업로드→답란 25mm→저장→브라우저 종료→
재로그인·재열기→PDF 다운로드)를 **처음부터 끝까지 실제 화면 조작으로 실행**했고,
그 과정에서 발견한 화면 연결 오류(PDF 다운로드 버튼 자체가 없던 문제)를 실제로 고쳤다.
로그인 우회·비활성화는 하지 않았다(기존 미들웨어·쿠키 세션 그대로 사용).

---

# 6차: L1-Q4-W01 복원 후보(id=1607/1611)를 실제 편집 화면에서 확인 (2026-09-19)

배경: 자동 빌드 로그에서 `L1-Q4-W01`이 여전히 FAIL(문항#1, #5 `ui_type=unknown`)로 나온
것을 확인했는데, 이건 3차 안정화 때 이미 조사해 둔 알려진 사안(id=1607/1611 재분류 후보,
`RESTORATION_PATCHES.json`)이었다. **QA 기준을 낮추거나 운영 DB를 즉시 고치는 대신**,
기존에 만들어 둔 "복원 후보 편집 프로젝트" 기능으로 실제 화면에서 확인·사용할 수 있게 했다.
운영 DB(`momo_book.db`)는 이번에도 전혀 수정하지 않았다(아래에서 실측으로 재확인).

## 1. id=1607, id=1611 재검토 — 원문·현재 유형·후보 유형·미확정 사항 구분

운영 DB에서 두 행을 직접 다시 읽어 원문과 현재 상태를 확인했다(읽기전용 연결):

| id | order | 현재 ui_type | 후보 ui_type | question_text(학생에게 보이는 문구) |
|---|---|---|---|---|
| 1607 | 1 | `unknown` | `text_long` | "왜 한자는 우리나라에서 널리 쓰이지 못했나요? ... 사랑을 뜻하는 한자 '사랑 애'" |
| 1611 | 5 | `unknown` | `text_long` | "세종대왕님처럼 한글을 만드신 분도 있고 ... 왜 이 사람들은 한글을 싫어했을까요? 한번 생각해봅시다." |

**키워드 부재만으로 완전하다고 단정하지 말라는 지시에 따라, 3차 안정화가 안 봤던
`raw_text` 뒷부분까지 다시 읽었고, 실제로 새로운 의심 지점을 찾았다** —
`RESTORATION_PATCHES.json`에 아래처럼 추가·수정해서 기록했다:

- **id=1607(문항#1)**: `raw_text`가 "사랑을 뜻하는 한자 '사랑 애'"에서 문장이 안 끝나고
  끊긴 것처럼 보인다 - 원본 PDF에 이 한자가 어떻게 쓰이는지 보여주는 삽화나 이어지는
  설명이 더 있고 추출 과정에서 잘렸을 가능성을 배제 못 한다. **원본 대조 미완료**.
- **id=1611(문항#5)**: `raw_text` 끝부분에 질문과 별도 줄로 "몇몇 양반들" / "과거
  우리나라를 지배했던 일본"이라는 두 개의 짧은 구절이 나란히 있다 - 서술형 답안
  예시라기보다 **선택지(choice_ab 등) 항목처럼 보이는 형태**라, `text_long` 단정이
  틀렸을 가능성이 새로 제기된다. 이전 키워드 검색("선택/고르" 등)이 이 형태를 못 잡은
  사례 - 키워드 부재가 구조 없음을 보장하지 않는다는 걸 실제로 보여준 케이스라 **신뢰도를
  '중간'→'낮음'으로 하향**했다. **원본 대조 미완료**.
- **환경 제한**: 두 문항의 `documents.source_file`은
  `D:\25년도 4분기 교재\초1\...(학생용).pdf`인데, **이 작업 환경에는 D: 드라이브 자체가
  마운트되어 있지 않아 원본 PDF를 열어볼 방법이 없다** - "원본 PDF 대조"는 이번에도
  키워드/구조 재검토 수준까지만 가능했고, 실제 스캔 페이지 대조는 여전히 미완료다.
  (참고로 이 두 문항의 worksheet 출력은 `raw_text`가 아니라 `question_text`를 쓰므로,
  위에서 발견한 "몇몇 양반들/일본" 구절은 현재 산출물에서 유실되는 게 아니라 애초에 안
  쓰이고 있다 - 다만 그게 원래 선택지였다면 "왜 애초에 안 쓰였는가"가 별도 확인 대상이다.)

**변경 파일**: `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` — id=1607/1611의
`uncertain` 필드에 위 내용 추가, id=1611의 `confidence`를 "중간"→"낮음"으로 하향(사유
명시). `applied: true`는 그대로 유지했다(아래처럼 여전히 "후보"로만 취급하고 운영 승격은
안 하므로).

## 2. 기존 "복원 후보 프로젝트 생성" 기능 재사용 — 새 기능 불필요

새로 만들기 전에 먼저 확인해보니, **이미 화면에서 접근 가능했다**:
`app/templates/momo_worksheet_editor/list.html`의 "새 프로젝트 시작" 폼에 "원본 데이터"
선택란(원본 그대로 / 복원 후보 적용)이 이미 있고, `app/routers/momo_worksheet_editor.py`의
`editor_new()`가 `RESTORATION_PATCHES.json`의 `field == "ui_type"` 패치까지 이미 처리하고
있었다(4차 안정화 때 만든 것 - 3차의 스크립트 시험(`editor_test_05_restoration_project.py`)은
`ui_config` 패치만 다뤘지만, 실제 라우터 코드는 처음부터 `ui_type`도 같이 처리해서 이번
L1-Q4-W01(ui_type 패치 2건)에도 그대로 통했다). 그래서 **원본 빌드 실패 화면에 새 링크를
추가하지 않았다** - "화면에서 접근할 수 없다면"이라는 조건 자체가 성립하지 않았기 때문
(불필요한 신규 기능 추가 지양 요구사항과도 일치).

## 3. "재분류 후보·원본 대조 미완료" 표시 — 기존 배지로 이미 충족, 화면에서 재확인

`detail.html`의 기존 배지(`meta.data_origin == 'restoration_candidate'`일 때 항상 표시,
QA 상태와 무관)를 그대로 썼다 - 아래 4번 실제 화면 스크린샷에서 QA PASS 배지와 복원 후보
경고 배지가 **동시에, 서로 다른 색으로** 표시되고, 경고 문구에 "QA를 통과해도 이 상태
표시는 그대로 남습니다(콘텐츠 승인 상태 자동 변경 없음)"이 명시되어 있음을 실제 화면에서
확인했다. 이 라우터에는 애초에 운영 승인 포인터(`current.json`)를 만드는 `_promote()`
호출 경로가 없어 자동 운영 승격이 코드 구조상 불가능함을 아래 6번에서 재확인했다.

## 4. 실제 로그인 브라우저 시험 (전 단계 실제 화면 조작)

| # | 단계 | 결과 |
|---|---|---|
| 1 | `/logout`→`/login` 재로그인(직접 타이핑) | 성공 |
| 2 | `/momo-worksheet-editor`에서 문서=`L1-Q4-W01`, 원본 데이터=`복원 후보 적용(RESTORATION_PATCHES.json)` 선택 → 프로젝트 만들기 | `project_id=L1-Q4-W01-38a99e01` 생성. "⚠ 이 프로젝트는 '복원 후보'가 적용된 데이터에서 시작했습니다.(적용된 패치: 1607, 1611)" 배지 즉시 표시 |
| 3 | 문항 목록 확인 | #1과 #5가 더 이상 `unknown`이 아니라 **`text_long`**으로 표시됨(패치 적용 확인) |
| 4 | 문항#1(id=1607) 내용 확인 | 편집 폼에 DB 원문과 동일한 question_text 표시 |
| 5 | 문항#5(id=1611) 내용 확인 | 편집 폼에 DB 원문과 동일한 question_text 표시 |
| 6 | 저장(편집 없이 그대로) | 새 버전 `v1789749434572` 생성, 자동 재조판+QA 실행(~10초) |
| 7 | QA 결과 확인 | **PASS**(0 issues) - 복원 후보 배지는 그대로 유지된 채 QA PASS 배지가 별도로 나란히 표시됨 |
| 8 | PDF 다운로드 | `C:\Users\aproa\Downloads\L1-Q4-W01-38a99e01_v1789749434572.pdf` 저장 |
| 9 | 브라우저 탭 종료 → 새 탭에서 재로그인 | 성공 |
| 10 | 프로젝트 재열기(URL 직접 이동) | 복원 후보 배지·QA PASS·문항#1/#5 text_long·버전 이력 **전부 그대로 유지 확인** |
| 11 | PDF 재다운로드 | `... (1).pdf`로 저장 - **8번과 sha256 완전히 일치**(`a66b61eb...`) 확인 |
| 12 | PDF→PNG 변환·육안 확인(`render_pdf_review.py`) | `page_04.png`(문항#1), `page_06.png`(문항#5) 둘 다 **더 이상 blk-fallback 상자가 아니라 정상 문단+넉넉한 답란**으로 렌더됨을 육안 확인, 겹침·잘림 없음 |

증거 경로: `momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_restoration_pngs/`
(`contact_sheet.png`, `page_04.png`, `page_06.png`, `render_log.json`).

## 5. 운영 DB·자동 승격 미변경 실측 재확인

시험 종료 후 운영 DB를 다시 읽기전용으로 직접 조회했다:
- `discussion_qa.ui_type`(id=1607, 1611): 여전히 `('unknown', 'unknown')` - 편집
  프로젝트가 만든 패치는 DB에 전혀 안 씀.
- `documents.review_status`(L1-Q4-W01): 여전히 `'approved'`(원래 값 그대로).
- `momo_book_db/generated/EDIT-L1-Q4-W01-38a99e01/current.json`(운영 승인 포인터):
  **생성 안 됨**(`os.path.isfile()` False) - QA PASS가 나왔어도 이 편집 프로젝트 경로는
  `_promote()`를 호출하지 않으므로 물리적으로 만들어질 수 없음.

## 6. 변경 파일 목록 (6차분)

- `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` — id=1607/1611의 `uncertain`
  필드에 재검토 내용 추가, id=1611 `confidence` 하향(사유 명시). `applied` 값은 안 바꿈.
- `momo_book_db/worksheet/edit_projects/L1-Q4-W01-38a99e01/` — 이번 브라우저 시험으로
  생긴 프로젝트(실제 화면 조작으로 생성, 복원 후보 적용)
- `momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_restoration_pngs/` — PDF→PNG
  증거 추가

**새로 만든 코드 파일은 없다** - `app/routers/momo_worksheet_editor.py`(ui_type 패치
처리), `app/templates/momo_worksheet_editor/{list,detail}.html`(복원 후보 선택·배지)는
전부 4차에서 이미 만들어 둔 걸 그대로 재사용했고, 5차에서 추가한 PDF 다운로드 기능도
그대로 재사용했다. `momo_book.db`(운영 DB), `generate.js`/`check.js`/`styles.css`/
`project_store.py`는 6차에서 전혀 안 건드림. 커밋·푸시·운영 배포·유료 API 호출은
하지 않았다.

## 7. 접속 경로 (직접 확인하실 때)

- 주소: http://127.0.0.1:8100/login (계정: `admin@aprolabs.co.kr` / `apro0914@`,
  5차와 동일한 기존 개발용 부트스트랩 계정)
- 편집 화면 진입: 로그인 → 좌측 "학습지 자동 생성 미리보기" → 프로젝트 목록에서
  `L1-Q4-W01-38a99e01`(데이터 출처: "복원 후보 적용" 배지) 열기 → 문항#1/#5가 `text_long`,
  QA PASS, 상단에 복원 후보 경고 배지가 계속 보임.
- 새로 같은 시험을 반복하려면: 목록 화면 "새 프로젝트 시작"에서 문서=`L1-Q4-W01`,
  원본 데이터=`복원 후보 적용(RESTORATION_PATCHES.json)` 선택 후 "프로젝트 만들기".

## 8. 남은 미확정 사항 (의도적으로 안 건드림)

- id=1607/1611 둘 다 **원본 PDF 실물 대조는 여전히 못 했다**(이 환경에 원본 PDF 소스
  자체가 없음 - `D:\25년도 4분기 교재\...` 드라이브 미접근). 재분류 신뢰도는 "중간"(1607)
  /"낮음"(1611)로 유지했고, 운영 DB 반영은 여전히 보류 상태다.
- id=1611은 이번에 새로 제기된 "실제로는 선택형일 수 있다"는 의심을 해소하지 못했다 -
  원본 PDF의 해당 페이지(위 documents.source_file, source_page=7)를 실제로 봐야 확정
  가능하다.
- `RESTORATION_CANDIDATES.md`/`RESTORATION_PATCHES.json`의 나머지 항목(840/423/843/114,
  id=435)은 이번 6차 범위 밖 - 3차 안정화 기록 그대로다.

---

# 7차: 원본 PDF 실물 대조로 id=1607/1611 복원 정정 (2026-09-19)

목표: 6차에서 "원본 PDF 미접근으로 대조 못 함"으로 남겨둔 id=1607(문항#1)/1611(문항#5)를,
사용자가 제공한 원본 PDF(`D:\25년도 4분기 교재\초1\[4분기 1주차]한글, 우리말을 담는 그릇
(베이직-학생용) (3).pdf`)로 실제 대조해 **6차의 text_long 후보를 철회하고** 실제 페이지
구조(빈칸 3개/대상별 그림+답란 2행)로 다시 복원한다. QA 기준은 낮추지 않았고, 운영 DB는
전혀 수정하지 않았으며(아래에서 재확인), 복원 후보는 기존 프로젝트를 덮어쓰지 않고 **새
프로젝트**로 만들었다.

## 1. 원본 페이지 실물 확인 — id=1607(3쪽)/id=1611(6쪽), 이전 판단 정정 내용

D: 드라이브가 이번엔 이 환경에서 접근 가능해서(6차 시점엔 미접근), PyMuPDF로 해당 페이지를
직접 렌더링해 육안 확인했다.

- **id=1607(문항#1, PDF 3쪽)**: 6차의 "text_long(개방형 서술)" 판단은 **틀렸다** - 실제로는
  **빈칸 3개짜리 문장완성 활동**이었다. DB의 `question_text`는 학생 화면에는 안 보이는
  정답이 이미 채워진 상태로 저장돼 있었다(예: "우리말은 **중국말**과 달라서..." - 빈칸에
  들어갈 말이 그대로 박혀 있음). 그리고 raw_text 끝의 "사랑을 뜻하는 한자 '사랑 애'"는
  본문 문장이 아니라 愛 캐릭터 그림의 캡션이었다(6차에서 "문장이 잘린 것 같다"고 의심했던
  부분 - 실제로는 잘린 게 아니라 그림 캡션이 본문 텍스트에 섞여 추출된 것). 페이지에는
  이미지 2개가 있다: 한자 옛 책 사진(참고자료, 캡션 없음), 愛 캘리그래피(그림, 캡션 "사랑을
  뜻하는 한자 '사랑 애'").
- **id=1611(문항#5, PDF 6쪽)**: 6차의 "text_long" 판단도 **틀렸다** - 6차 재검토에서 이미
  "raw_text 끝의 두 구절이 선택지처럼 보인다"고 의심하며 신뢰도를 낮췄었는데, 실물 대조로
  그 의심이 **부분적으로 맞았음**이 확인됐다: 선택지(choice)가 아니라 **대상별(몇몇
  양반들/과거 우리나라를 지배했던 일본) 그림+개별 서술 답란 2행** 구조였다. 표 아래에
  "[영상으로 보기] https://youtu.be/LwALe4Z7-pO" 링크도 있었다(raw_text에는 있었지만
  question_text에는 없어서 이전엔 놓쳤을 수 있는 부분).
- 두 항목 모두 `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json`에서 **기존
  text_long 패치를 `applied:false`로 철회**하고(`withdrawal_note`에 철회 사유 기록,
  기록 자체는 지우지 않음 - 이전 판단이 왜 틀렸는지 계속 남도록), 대신 새 patches
  (ui_type=text_short_multi, question_text, ui_config, excerpt_image_path/
  reference_image_path 또는 blanks[].image_path)를 `applied:true`로 추가했다 - "이전
  판단의 정정 내용"을 삭제가 아니라 **철회 기록으로 남기는 방식**을 택함.

## 2. 이미지 자산 - 새로 추출 2개, 기존 파일 재사용 2개(중복 저장 안 함)

PyMuPDF로 PDF 3쪽/6쪽의 임베드 이미지를 직접 추출해 sha256으로 기존 자산과 대조했다:

| 이미지 | 처리 | 근거 |
|---|---|---|
| 愛 캘리그래피(3쪽) | **신규 추가**: `extracted_images/L1-Q4-W01/restore_p3_ai_character.png` | 기존 5개 파일 중 일치하는 게 없음 - 새 파일로 추가(기존 파일은 전혀 안 건드림) |
| 한자 옛 책 사진(3쪽) | **기존 파일 재사용**: `illustration_p4.jpeg` | sha256이 이미 있던 `illustration_p4.jpeg`와 완전히 일치(같은 이미지가 "학생용"/"베이직-학생용" 두 PDF 변형에 공통으로 쓰임) - 중복 저장 안 함 |
| 양반 그림(6쪽) | **신규 추가**: `extracted_images/L1-Q4-W01/restore_p6_row1_yangban.jpeg` | 기존 파일과 불일치 - 새 파일로 추가 |
| 일본 그림(6쪽) | **기존 파일 재사용**: `illustration_p7.jpeg` | sha256이 `illustration_p7.jpeg`와 완전히 일치 - 중복 저장 안 함 |

원본 교재 이미지(기존 5개 파일)는 하나도 이름을 바꾸거나 덮어쓰지 않았다 - `restore_` 접두사로
새로 추가한 파일만 구분해 추적 가능하게 했다.

## 3. 코드 변경 — 빈칸 렌더러·그림 캡션 재사용/최소 확장 (전부 하위호환)

**"기존 빈칸 렌더러와 데이터 구조를 우선 재사용"** 요구사항에 따라 새 블록 타입을 만들지
않고, 기존 `text_short_multi`(blk-blanks)/그림 캡션 메커니즘을 **선택적 인자로만** 확장했다.
기존 호출부(다른 문서들)는 인자를 안 주므로 동작이 100% 그대로임을 재빌드로 재확인했다
(아래 "회귀 없음" 참고).

- **`momo_book_db/worksheet/scripts/blocks.js`** - `bodyBlanks(item)`: `ui_config.blanks[]`
  항목이 문자열(기존)이면 그대로, **객체**(`{label?, image_path?, image_caption?}`)면 그
  행에 그림(+캡션)을 답란 위에 붙인다. `label`이 없으면(그림 캡션이 이미 행의 정체를 나타낼
  때) 라벨 문단을 생략해 캡션과 중복 안 함 - id=1611의 "몇몇 양반들"/"과거 우리나라를
  지배했던 일본"이 이 경우. `ui_config.note`가 있으면 답란들 밑에 문단으로 붙인다(원본
  페이지의 "표 아래 영상 링크" 보존용).
- **`momo_book_db/worksheet/scripts/paginate.js`** - `buildBundles(item)`: excerpt 그림의
  캡션을 `ui_config.excerpt_image_caption`에서 읽어 `figureBlock()`에 넘긴다(기존엔 항상
  `null`이라 캡션이 아예 안 붙었음) - id=1607의 愛 캡션용.
- **`momo_book_db/worksheet/build/styles.css`** - `.blk-blanks__row .figure-block`/`img`
  크기 규칙 추가(발췌문용 큰-그림 강제 확대 규칙이 안 걸리게, `.reference .figure-block`과
  같은 원리), `.blk-blanks__note` 작은 캡션 스타일 추가.
- **`momo_book_db/worksheet/scripts/check.js`** - `ui_config.blanks[].image_path`도 기존
  `excerpt_image_path`/`reference_image_path`와 동일하게 콘텐츠 대조(존재 여부) 검사에
  포함시켰다(빠뜨리면 행별 그림이 깨져도 QA가 못 잡을 뻔했음).
- **`app/routers/momo_worksheet_editor.py`** - `editor_new()`의 복원 패치 적용 분기를
  `ui_config`/`ui_type` 둘로 고정돼 있던 걸 일반화(`ui_config`는 통째로 교체, 그 외
  필드는 평범한 값 대입)해서 `question_text`/`excerpt_image_path`/`reference_image_path`
  패치도 적용되게 했다 - id=1607 복원에 필요했음.

### 회귀 없음 재확인 (기존 문서 재빌드로 실측)

`bodyBlanks`/`buildBundles` 변경이 기존 문자열 blanks 사용처(L3-Q4-W02#3, #4-2)에 영향을
주는지 실제로 확인했다 - L3-Q4-W02를 변경 전/후 코드로 각각 재빌드해 해당 두 문항의 렌더된
HTML을 바이트 단위로 대조(`data-qa-id="839"` 블록 등) → **완전히 동일**함을 확인.

**부수 발견(회귀 아님, 별도 기록)**: 같은 재빌드 과정에서 L3-Q4-W02의 총 페이지 수가 3차
안정화 기준선(9페이지) 대비 11페이지로 달라져 있는 걸 발견했다 - 변경한 3개 파일(blocks.js/
paginate.js/styles.css)을 전부 임시로 원복하고 다시 빌드해도 **똑같이 11페이지**가 나와서,
이번 6~7차 수정과 무관하게 이미 존재하던 환경적 차이(Playwright/폰트 렌더링 등 타이밍 요인
추정)임을 직접 실측으로 확인했다. L3-Q4-W02는 원래도(전/후 다 FAIL) 이번 작업 대상이
아니고 QA 판정(FAIL, 동일 3건 이슈)도 안 바뀌어서 이번 작업 범위에서 고치지 않았다 -
다음에 L3-Q4-W02/그 계열 문서를 다룰 때 참고할 사항으로만 남긴다.

## 4. 실제 브라우저 시험 (새 프로젝트, 기존 프로젝트는 그대로 둠)

**기존 프로젝트(`L1-Q4-W01-38a99e01`, 6차에서 만든 text_long 버전)를 덮어쓰지 않고 새
프로젝트로 만들었다** - 시험 후 재확인한 결과 `L1-Q4-W01-38a99e01`은 `current_version`도
문항#1 `ui_type`(`text_long`)도 이번 7차 작업으로 **전혀 안 바뀜**을 확인(아래 6번).

| # | 단계 | 결과 |
|---|---|---|
| 1 | 재로그인(직접 타이핑) | 성공 |
| 2 | `L1-Q4-W01`, 원본 데이터=`복원 후보 적용` 선택 → 프로젝트 만들기 | `project_id=L1-Q4-W01-927494f5`(새 프로젝트, 38a99e01과 별개). 배지: "적용된 패치: 1607, 1607, 1607, 1607, 1607, 1611, 1611"(7개 patch 항목) |
| 3 | 문항#1 확인 | `text_short_multi`, question_text="왜 한자는 우리나라에서 널리 쓰이지 못했나요?", "현재: L1-Q4-W01/restore_p3_ai_character.png" 표시 |
| 4 | 문항#5 확인 | `text_short_multi`, question_text 원본과 일치 |
| 5 | 저장(편집 없이 그대로) | 새 버전 `v1789751421654` 생성, 자동 재조판+QA |
| 6 | QA 결과 | **PASS**(복원 후보 배지와 별도로 QA PASS 배지 표시 - "QA를 통과해도 콘텐츠 승인 상태 자동 변경 없음" 문구 그대로 유지) |
| 7 | PDF 다운로드 | `C:\Users\aproa\Downloads\L1-Q4-W01-927494f5_v1789751421654.pdf` |
| 8 | PDF→PNG 변환·육안 확인 | 아래 "5. 육안 확인 결과" 참고 |
| 9 | 브라우저 탭 종료 → 새 탭 재로그인 → URL 재접속 | 복원 후보 배지·QA PASS·문항#1/#5 상태·버전 이력 **전부 그대로 유지 확인** |
| 10 | PDF 재다운로드 | `... (1).pdf` - **8번과 sha256 완전 일치**(`90e2d8a9...`) 확인 |

## 5. 육안 확인 결과 (요청하신 4가지 기준 전부 확인)

증거: `momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_restore_v2_pngs/`
(`contact_sheet.png`, `page_04.png`, `page_05.png`, `page_08.png`)

- **문항#1 - 빈칸 3개, 답 노출 없음, 그림·캡션 유지**: `page_05.png` - "우리말은 ______과
  달라서...", "한자는 ______________을 수 없었어요.", "...가지고 있는 ______(이)라..."
  3개 빈칸 전부 빈 채로(정답 텍스트 없음) 각자 답란과 함께 표시. 참고자료 박스에 한자 책
  사진(캡션 없음). `page_04.png` - 愛 캘리그래피가 별도 그림 반면으로, 캡션 "사랑을 뜻하는
  한자 '사랑 애'"가 본문과 분리된 채 그림 아래에 표시.
- **문항#5 - 대상별 그림과 답란 2개 유지**: `page_08.png` - 1행(양반 그림+캡션 "몇몇
  양반들"+독립 답란), 2행(일본 그림+캡션 "과거 우리나라를 지배했던 일본"+독립 답란) -
  **두 대상이 선택지 체크박스로도, 하나의 공유 답란으로도 합쳐지지 않고 각자 온전한
  답란을 가짐**(요청사항 그대로). 표 아래 "[영상으로 보기] https://youtu.be/LwALe4Z7-pO"
  링크 텍스트 보존.
- **잘림·겹침 없음**: `check.js` QA 결과 자체가 PASS(오버플로 0건)이고, `contact_sheet.png`
  전체 10페이지 육안 확인으로도 겹침·잘림 없음 재확인.

## 6. 운영 DB·기존 프로젝트·자동 승격 미변경 재확인

- 운영 DB: `discussion_qa.ui_type`(id=1607,1611) 여전히 `('unknown','unknown')`,
  `documents.review_status`(L1-Q4-W01) 여전히 `'approved'` - 직접 재조회로 확인.
  `RESTORATION_PATCHES.json`/`PROGRESS.md` 파일 수정 외에 DB 접속은 전부 읽기전용.
- 기존 프로젝트(`L1-Q4-W01-38a99e01`): `current_version=v1789749434572`(6차 그대로),
  `base_data.json`의 문항#1 `ui_type`도 여전히 `text_long` - **이번 7차 작업으로 전혀
  안 바뀜**(새 프로젝트로 만들라는 요구사항 그대로 지킴).
- `momo_book_db/generated/EDIT-L1-Q4-W01-927494f5/current.json`(운영 승인 포인터):
  **생성 안 됨** - QA PASS여도 이 라우터 경로엔 `_promote()` 호출이 없어 물리적으로
  못 만들어짐(4~6차와 동일한 구조적 보장).

## 7. 변경 파일 목록 (7차분)

- `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` — id=1607/1611 text_long 패치
  철회(`applied:false`+철회 사유) + 신규 patches 7건 추가(ui_type/question_text/
  ui_config/excerpt_image_path/reference_image_path)
- `momo_book_db/worksheet/scripts/blocks.js` — `bodyBlanks()` 객체형 blanks(그림+캡션)·
  `ui_config.note` 지원 추가(문자열 blanks는 기존과 바이트 동일 유지)
- `momo_book_db/worksheet/scripts/paginate.js` — `buildBundles()`가
  `ui_config.excerpt_image_caption`을 읽어 그림 캡션에 반영
- `momo_book_db/worksheet/scripts/check.js` — `ui_config.blanks[].image_path` 콘텐츠
  대조(존재 확인) 추가
- `momo_book_db/worksheet/build/styles.css` — `.blk-blanks__row .figure-block`/`img`,
  `.blk-blanks__note` 스타일 추가
- `app/routers/momo_worksheet_editor.py` — 복원 패치 필드 적용 분기 일반화
- `momo_book_db/extracted_images/L1-Q4-W01/restore_p3_ai_character.png`,
  `restore_p6_row1_yangban.jpeg` — 신규(원본 PDF에서 직접 추출, 기존 5개 파일은 안 건드림)
- `momo_book_db/worksheet/edit_projects/L1-Q4-W01-927494f5/` — 이번 브라우저 시험으로
  생긴 새 프로젝트(기존 `L1-Q4-W01-38a99e01`은 그대로 둠)
- `momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_restore_v2_pngs/` — PDF→PNG
  증거 추가

`generate.js`/`extract_worksheet_json.py`/`project_store.py`, 운영 DB(`momo_book.db`)는
7차에서 전혀 안 건드림. 커밋·푸시·운영 배포·유료 API 호출은 하지 않았다. 개발 서버는
라우터 변경을 반영하기 위해 재시작했다(같은 명령으로 재기동, 포트/접속 정보 동일).

## 8. 접속 경로 (직접 확인하실 때)

- 주소: http://127.0.0.1:8100/login (계정: `admin@aprolabs.co.kr` / `apro0914@`)
- 편집 화면: 로그인 → "학습지 자동 생성 미리보기" → 프로젝트 목록에서
  `L1-Q4-W01-927494f5`(데이터 출처: "복원 후보 적용") 열기 - 문항#1(text_short_multi,
  빈칸 3개+愛 그림)과 문항#5(text_short_multi, 대상별 그림+답란 2행) 확인 가능. 6차의
  `L1-Q4-W01-38a99e01`(구 text_long 버전)도 목록에 그대로 남아 있어 비교 가능.

## 9. 남은 미확정 사항

- id=1611의 답란(양반/일본 각 행)에 몇 줄이 적절한지는 원본 PDF 상 여백 크기로 추정했을
  뿐, 정확한 줄 수 사양은 없음(현재 CSS 기본값 - 답란 15mm 이상 정책만 보장) - 필요하면
  "답란 높이 조절" 편집 기능으로 조정 가능.
- id=1607 빈칸 3개의 시각적 배치(원본은 문장 안 인라인 빈칸, 렌더러는 행별 분리)는
  "기존 렌더러 우선 재사용" 지시에 따른 의도된 차이 - 원본과 완전히 같은 인라인 빈칸
  모양으로 만들려면 새 블록 타입이 필요해 이번 범위 밖으로 남김(`RESTORATION_PATCHES.json`
  ui_config 패치의 `uncertain` 필드에 기록).
- `RESTORATION_PATCHES.json`의 나머지 항목(840/423/843/114, id=435)은 이번 7차 범위
  밖 - 이전 기록 그대로.

---

# 8차: 문항 #1~#6 전체 원문 대조, 디자인 오류 수정, "QA PASS인데 로고 깨짐" 근본 원인 조사 (2026-09-19)

목표: 7차에서 id=1607/1611(문항#1/#5)만 대조했던 것을, 사용자가 지적한 문제 출력
(`L1-Q4-W01-927494f5_v1789751421654 (1).pdf`)을 근거로 **문항 #1~#6 전체**를 원본
PDF(베이직-학생용 (3).pdf)와 다시 대조하고, 실제 PDF에서 확인된 디자인 오류를 고치고,
"QA는 PASS인데 로고가 깨져 나온" 원인을 실제로 찾아 고쳤다. QA 기준은 낮추지 않았고
운영 DB는 전혀 수정하지 않았다(아래에서 재확인). 기존 프로젝트(`L1-Q4-W01-38a99e01`,
`L1-Q4-W01-927494f5`)는 그대로 두고 **새 프로젝트**(`L1-Q4-W01-0b1e115e`)에서 작업했다.

## 1. "QA PASS인데 로고가 깨진" 근본 원인 — 실제로 찾아 고침

사용자가 지적한 PDF를 PNG로 변환해 직접 확인한 결과, **표지·모든 내지 머리말의 로고
이미지가 깨진 아이콘으로 나오고 있었다**(`evidence_20260919/L1-Q4-W01_flagged_pngs/
page_01.png`, `page_02.png`). 원인을 끝까지 추적했다:

- **직접 원인**: `generate.js`가 출력 파일의 실제 디렉터리 깊이에 맞춰
  `styles.css`/`extracted_images` 상대경로는 다시 쓰지만(1차 안정화 때 추가된 로직),
  `blocks.js`의 로고 경로 상수(`ASSET_BASE = '../../worksheet/build/assets/images/'`)는
  **전혀 다시 쓰지 않고 있었다**. 편집 프로젝트 빌드 경로
  (`momo_book_db/worksheet/edit_projects/<pid>/versions/<vid>/index.html`, momo_book_db
  기준 5단계 아래)에서는 이 2단계 경로가 실제로 존재하지 않는 위치를 가리켜서 로고
  `<img>`의 `src`가 통째로 깨졌다.
- **실측으로 확인한 피해 범위**: 이 버그는 L1-Q4-W01만의 문제가 아니라 **4차 안정화(편집
  프로젝트의 깊은 버전 폴더 구조 도입) 이후 만들어진 편집 프로젝트 PDF 전부**, 그리고
  **1차 안정화(4단계 깊이 builds/ 기본 경로 도입) 이후 만들어진 일반 파이프라인 PDF
  전부**에 있었다(`L5-Q4-W11-1b4009d3`의 기존 빌드 HTML을 직접 열어 같은 깨진 경로
  확인, `L5-Q4-W11`의 일반 파이프라인 빌드도 마찬가지로 확인). 즉 이번 세션 전체에서
  "육안 확인"했다고 기록한 여러 PDF들도 실제로는 로고가 깨진 채였는데, 항목별 내용
  검수에 집중하느라 작은 머리말 로고까지 확대해서 보지 않아 지금까지 놓치고 있었다
  (반성 - 이번엔 표지/머리말을 별도로 확대해 확인함).
- **QA가 못 잡은 이유**: `check.js`의 이미지 로딩 실패 검사가 `.sheet-head`(머리말
  로고)/`.cover__brand`(표지 로고) 안의 `<img>`를 **저해상도 검사뿐 아니라 로딩 실패
  검사에서도 통째로 제외**하고 있었다(원래 의도는 "고정 크기 브랜드 자산은 확대율
  검사가 의미 없다"였는데, 로딩 성공 여부 검사까지 같이 빠져버림). 그래서 로고 경로가
  완전히 깨져도 QA는 그 사실을 볼 방법이 없어 계속 PASS를 냈다.
- **수정**: (1) `generate.js`의 경로 재작성 로직에 `../../worksheet/build/assets/images/`
  치환을 추가(styles.css/extracted_images와 같은 방식, 실제 깊이 기준). (2) `check.js`의
  이미지 검사를 둘로 분리 - **로딩 실패 검사는 로고 포함 전체 이미지에 적용**하고,
  저해상도(확대) 검사만 로고를 계속 제외.
- **재발 방지 확인**: 수정 후 `L1-Q4-W01`(편집 프로젝트, 5단계 깊이)과 `L5-Q4-W11`(일반
  파이프라인, 4단계 깊이) 둘 다 재빌드해 로고 `<img src>`가 올바른 깊이로 나오는 것과
  실제 PNG에 로고가 정상 표시되는 것을 확인했다(아래 5·8번 참고). **의도적으로 로고
  경로를 깨뜨린 시험**은 이번엔 안 함(원인이 재현이 아니라 실제 기존 산출물에서 이미
  확인됐기 때문) - 대신 실제 재발이 있었는지는 다음에 편집 프로젝트를 만들 때마다
  머리말 로고를 육안으로 같이 확인하는 습관으로 보완 권장(별도 자동 검사만으로는
  "로고가 그럴듯하게 보이는 다른 걸로 깨지는" 케이스까지는 못 잡을 수 있음).

## 2. 문항 #1~#6 전체 원문 대조표

원본: `[4분기 1주차]한글, 우리말을 담는 그릇 (베이직-학생용) (3).pdf`(사용자 제공, 총
6쪽). 문제 출력: `L1-Q4-W01-927494f5_v1789751421654 (1).pdf`.

| # | 원본 쪽 | 문제 출력 쪽 | 발견한 문제 | 조치 |
|---|---|---|---|---|
| 1 | 3쪽 | 4~5쪽 | 3번째 빈칸 뒤 "...만들어야 했기 때문에 농사짓기 바쁜 백성들은 그 많은 글자를 배우고 익힐 시간이 없었어요" 누락(7차 실수), 愛 그림이 단독 페이지 | 빠진 문장 복원, 愛를 참고자료 박스 안 작은 썸네일(reference_image2)로 이동해 질문·빈칸과 한 단위로 배치 |
| 2 | 4쪽 | 6쪽 | text_long으로 분류돼 장쇠/간난이/꽃네 3명의 빈 말풍선이 통째로 답란 하나로 뭉개짐, 인물별 1인칭 문장(모범답안 성격)이 질문 본문에 그대로 노출 | text_short_multi로 재분류, 인물 3명 각각 그림+캡션+독립 답란 3행으로 복원, 답 성격 문장은 학생용 출력에서 제거 |
| 3 | 5쪽 | 6쪽 | 질문 끝에 "훈민정음" 정답 노출, 이름 칸이 4칸 대신 줄글 답란, 뜻 칸도 정답 문장이 그대로 라벨에 있어 쓸 게 없음, 관련 그림(훈민정음 목판) 누락 | 정답 제거, 4칸 글자 칸(char-grid, 신규) 2줄 + 뜻 쓰는 빈 줄로 복원, 참고자료에 목판 사진 추가(기존 파일 재사용) |
| 4 | 5쪽 | 7쪽 | 없음(원문 유실 없음, 템플릿의 "토론 메모"는 이미 별도 라벨로 구분돼 있음) | 변경 없음(확인만) |
| 5 | 6쪽 | 8쪽 | 7차에서 이미 대상별 그림+답란 2개로 복원됨. 영상 링크 표시 문자열의 마지막 글자가 육안상 대문자 O처럼 보이지만 실제 PDF 임베드 링크(`page.get_links()`)는 숫자 0으로 끝남 - 7차에 잘못 옮겨 적음 | 링크 URL 정정(`https://youtu.be/LwALe4Z7-p0`), 실제 클릭 가능한 링크로 만듦(note_href) |
| 6 | 6쪽 | 9쪽 | starter에 정답 문장 전체("...한글은 말소리를... 편리한 문자 (이)기 때문이에요")가 노출, 원본은 앞/뒤 고정 문구 사이가 전부 빈 줄 | 가운데를 빈 답란으로, 앞뒤 고정 문구만 starter/starter_suffix(신규)로 분리 |

**어휘(1단계) 재확인**: ~~사용자가 "원본 2쪽 '낱말 뜻 연결하기'가 '문장 만들기'로
바뀌어 있다"고 지적했는데, 원본 PDF 2쪽을 다시 육안으로 확인한 결과 실제 안내문은
"...이 단어들을 이용해 각각 하나씩 '문장'을 만들어 보세요."였다 - 즉 현재 렌더러의
"문장 만들기" 표기가 원본과 이미 일치했다(잘못 바뀐 게 아님). "보기 활용 빈칸 3문항"도
원본 2쪽 전체를 다시 확인했지만 해당 활동을 찾지 못했다.~~
**⚠ 2026-09-19 9차에서 이 판단 전체가 오판으로 확인되어 정정됨 - 아래 "9차" 섹션
참고.** 실제로는 사용자(및 ChatGPT의 재검증)가 맞았다 - 원본 2쪽은 "낱말 뜻 선
연결하기 5개"와 "<보기> 활용 빈칸 3문항" 둘 다 실제로 있었고, 8차 때 다시 안 열어보고
6차 때 만든 파일을 재사용해 판단한 것이 오류의 원인이었다(6차 판단 자체도 틀렸던
것으로 보이며, 정확한 혼동 경위는 재구성하지 못함). 9차에서 PDF를 sha256까지
재확인하고 좌표 기반으로 처음부터 다시 추출해 바로잡았다.

**배경지식/글쓰기(3단계) 재확인**: 원본 PDF는 총 6쪽인데 `essay_prompt.source_page=8`
(세종대왕님께 편지쓰기)이라 8쪽 자체가 이 판본에 없다. `documents.source_file`(운영
DB)이 가리키는 파일은 이 파일이 아니라 같은 주차의 **다른 판본**("학생용", 총 8쪽) -
파일 존재는 확인했지만 이번에 직접 열어 페이지 대조는 하지 않았다(아래 9번 참고).
사용자 지시대로 **운영 DB는 그대로 두고**, 이 복원 프로젝트의 학생용 출력에서만
`meta.background_text`/`step3`을 제외했다(`RESTORATION_PATCHES.json`에 `table:"meta"`/
`table:"step3"` 패치로 기록 - 아래 4번 참고). "1-2 사고"/"생각상자"/쪽번호가 제목·
지시문·본문 중 어디서부터 어디까지인지 분리하는 작업은 **근거 페이지(8쪽 판본)를
못 봐서 이번에 하지 않았다** - 단순 정규식으로 잘라내는 방식은 의도적으로 피함.

## 3. 실제 PDF에서 확인된 디자인 오류 수정 (전부 재현·수정 확인)

| 오류 | 원인 | 수정 |
|---|---|---|
| 표지·머리말 로고 깨짐 | 위 1번 참고 | `generate.js` 경로 재작성 확장 |
| 표지 제목 "그릇"이 "그 / 릇"으로 갈라짐 | `.cover__title`에 줄바꿈 단위 지정이 없어 CJK 기본 규칙(음절 단위 줄바꿈) 적용 | `.cover__title { word-break: keep-all; }` 추가 |
| 표지 인용부호 중복(`""...""`가 됨) | DB의 `cover_message`가 이미 겹따옴표를 포함한 채 저장된 문서(L1-Q4-W01 포함 여러 건)에 `coverPage()`가 자기 겹따옴표를 또 붙임 | 이미 따옴표로 시작·끝나면 안 붙이는 `quoteCoverMessage()` 추가(L1-Q4-W01 외 다른 문서의 같은 버그도 같이 고쳐짐 - 아래 5번 회귀 확인 참고) |
| 짧은 페이지에서 footer가 하단에 안 붙음(배경지식 전용 페이지에서 확인) | `.sheet`에 직접 들어가는 body(배경지식 전용/2단계 fullpage)가 flex:1이 아니라서, 콘텐츠가 짧으면 footer가 콘텐츠 바로 밑에 붙고 페이지 하단까지 빈 공간이 남음 | `.page-body{flex:1}` 래퍼 추가(`generate.js`가 body를 감쌈) - 이번 프로젝트에선 배경지식 페이지 자체가 제외돼 실제로는 안 보이지만, 다른 문서에도 있을 수 있는 문제라 같이 고침 |
| 愛 그림 단독 페이지·과대 확대 | `excerpt_image_path`(발췌문용 큰-그림 반면, width:100% 강제)를 캡션 있는 작은 아이콘성 그림에 그대로 씀 | 위 1번 참고(reference_image2로 이동, 글자 크기는 안 건드림) |

## 4. 코드·데이터 변경 파일 (8차분)

- `momo_book_db/worksheet/scripts/generate.js` — ASSET_BASE 경로 재작성 추가(로고
  버그 근본 수정), `.page-body` 래퍼 추가(footer 위치 수정)
- `momo_book_db/worksheet/scripts/check.js` — 이미지 로딩 실패 검사를 로고 포함 전체로
  확장(저해상도 검사만 로고 제외 유지), `reference_image2_path`/blanks 이미지 콘텐츠
  대조 추가
- `momo_book_db/worksheet/scripts/blocks.js` — `coverPage()` 인용부호 중복 방지,
  `referenceBlock()` 2번째 이미지 지원, `bodyShort()` `starter_suffix` 지원,
  `bodyBlanks()` chargrid(글자 칸)·suffix·note_href(실제 링크) 지원
- `momo_book_db/worksheet/build/styles.css` — `.cover__title{word-break:keep-all}`,
  `.page-body`, `.char-grid`/`.char-box`, `.blk-blanks__row--chargrid`/`__prefix`/
  `__suffix`, `.q__example--suffix` 추가
- `app/routers/momo_worksheet_editor.py` — 복원 패치에 `table:"meta"`/`table:"step3"`
  지원 추가(문항 단위가 아니라 문서 메타/3단계 전체를 대상으로 하는 패치)
- `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` — id=1607 ui_config 패치
  철회+재작성(누락 문장 복원, 그림 배치 변경), id=1608/1609/1612 신규 patches(문항
  #2/#3/#6), id=1611 note/note_href 정정(영상 링크), meta.background_text/step3
  제외 patches 추가
- `momo_book_db/extracted_images/L1-Q4-W01/restore_p4_gannani.jpeg`,
  `restore_p4_kkotne.jpeg` — 신규(원본 PDF 4쪽에서 직접 추출, 기존 파일은 안 건드림).
  훈민정음 목판 사진은 새로 추출해보니 기존 `illustration_p6.jpeg`와 sha256이 완전히
  같아 새 파일을 안 만들고 그대로 재사용함.
- `momo_book_db/worksheet/edit_projects/L1-Q4-W01-0b1e115e/` — 이번 브라우저 시험으로
  생긴 새 프로젝트(기존 `L1-Q4-W01-38a99e01`/`L1-Q4-W01-927494f5`는 그대로 둠)

`extract_worksheet_json.py`/`project_store.py`, 운영 DB(`momo_book.db`)는 8차에서
전혀 안 건드림. 커밋·푸시·운영 배포·유료 API 호출은 하지 않았다.

## 5. 회귀 없음 확인 — L5-Q4-W11 재빌드

`blocks.js`/`generate.js`/`check.js`/`styles.css`를 여러 곳 고쳤기 때문에, 이미
PASS였던 문서(`L5-Q4-W11`)가 계속 PASS인지 실제로 재빌드해 확인했다:

```
node momo_book_db/worksheet/scripts/generate.js L5-Q4-W11
node momo_book_db/worksheet/scripts/check.js <생성된 index.html> <data.json> <report.json>
```

결과: **8페이지(표지 포함), issues 0건, status=PASS**(이전과 동일 - 1차 안정화
기준선과 페이지 수 일치). 로고 경로도 `../../../../worksheet/build/assets/images/`
(4단계, 실제 깊이와 일치)로 올바르게 재작성됨을 확인 - 이 표준 파이프라인 문서도
그동안 로고가 깨져 있었다가 이번에 같이 고쳐졌다.

**부수 확인**: `bodyBlanks()`의 문자열 blanks 경로(장쇠 항목처럼 객체가 아닌 기존
방식)는 8차에서 건드리지 않았고(7차에 이미 회귀 확인 완료), 이번 8차 변경(chargrid/
suffix/note_href)은 전부 새 선택적 분기라 기존 문자열 blanks 문서에는 영향이 없다.

## 6. 실제 브라우저 시험 (새 프로젝트, 기존 2개 프로젝트는 그대로 둠)

라우터 코드(`table:"meta"`/`"step3"` 지원)를 반영하기 위해 개발 서버를 재시작했다
(같은 명령, 같은 접속 정보 - 8번 참고). Node 스크립트(generate.js/check.js/blocks.js)
변경은 매번 새 프로세스로 실행되므로 서버 재시작이 필요 없었다.

| # | 단계 | 실제 조작 | 결과 |
|---|---|---|---|
| 1 | 재로그인 | `/logout`→`/login`에서 이메일·비밀번호 직접 타이핑 | 성공 |
| 2 | 새 프로젝트 생성 | `L1-Q4-W01`, 원본 데이터=`복원 후보 적용` 선택 → 프로젝트 만들기 | `project_id=L1-Q4-W01-0b1e115e`(기존 2개와 별개, 적용된 패치 14건: 1607×3, 1611×2, 1608×3, 1609×3, 1612×1, meta.background_text, step3) |
| 3 | 문장 수정 | 문항#1 question_text 맨 앞에 "[8차 브라우저 시험] " 추가(나머지 원문은 그대로 - 대조 결과에 영향 안 주려고 덧붙이기만 함) | 반영 확인 |
| 4 | 그림 업로드·교체 | 같은 문항#1의 "발췌문 그림(excerpt_image_path)" 필드(이번 복원에서는 안 쓰는 빈 필드)에 실제 파일 업로드 | `asset_id=a13e48b531d0b814a9a369a5`, 업로드 원본 파일 sha256 앞자리와 정확히 일치 확인 |
| 5 | 답란 높이 조절 | 같은 문항의 `answer_height_mm=25` | 저장됨 |
| 6 | 저장 | "저장하고 다시 조판·검사" 클릭(위 3~5 전부 한 번에 제출) | 새 버전 `v1789755691959` 생성, 자동 재조판+QA(~10초) |
| 7 | QA 결과 | **status=PASS**(blockingCount 0, warnCount 1 - 아래 참고), 복원 후보 배지와 별도로 QA PASS 배지 표시 | 확인 |
| 8 | PDF 다운로드 | `C:\Users\aproa\Downloads\L1-Q4-W01-0b1e115e_v1789755691959.pdf` | 저장 확인 |
| 9 | 브라우저 컨텍스트 종료 | 탭 닫기 | - |
| 10 | 새 컨텍스트에서 로그인 | 새 탭 생성 → `/logout`→`/login` 재입력 | 성공 |
| 11 | 재열기 | URL 직접 이동으로 같은 프로젝트 재접속 | 문장 수정·업로드 자산·25.0mm·QA PASS·버전 이력 **전부 그대로 유지 확인** |
| 12 | PDF 재다운로드 | 같은 버튼 재클릭 | `... (1).pdf` - **8번과 sha256 완전 일치**(`6b235446c2eac5f7735298a3f4c222cc57c0d77ae4fc45c185b7a4f7cb93877e`) 확인 |

**warnCount 1의 정체**: 3번 시험 단계에서 일부러 작은 테스트 이미지(313px)를 업로드해서
생긴 "그림 저해상도(경고)" 1건 - 업로드 기능 시험용으로 의도한 것이고 실제 복원
콘텐츠(愛/책사진/장쇠 등)와는 무관, 차단 이슈(blocking)는 0건.

**식별자 정리**: 편집 프로젝트 빌드는 일반 파이프라인의 `builds/<build_id>/`가 아니라
`versions/<version_id>/` 구조를 쓰므로, 이 경로에서는 **version_id가 곧 build 식별자**
역할을 한다(`manifest.json`에 `qa_status`/`checked_at` 같이 기록됨) - 별도 build_id는
없음.

## 7. 다운로드 PDF → PNG 육안 확인 (요청하신 항목 전부)

증거: `momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_8차_final_pngs/`
(`contact_sheet.png`, `page_01/02/04/05/06/07/08/09.png` - 실제로 6번 위 표에서
다운로드한 그 PDF 그대로 변환, 별도 재생성 안 함)

| 확인 항목 | 결과 |
|---|---|
| #1 빈칸 3개, 설명 누락 없음, 그림과 질문이 함께 배치됨 | `page_04.png` - 3개 빈칸 전부 "...농사짓기 바쁜 백성들은...시간이 없었어요"까지 온전, 참고자료 박스 안에 책사진+愛(캡션 포함)가 질문·빈칸과 한 페이지에 배치 |
| #2 그림 3개와 독립 답란 3개, 답 노출 없음 | `page_05.png` - 장쇠/간난이/꽃네 각자 그림+캡션+빈 답란, 1인칭 답 문장 노출 없음 |
| #3 이름·뜻 빈칸, 답 노출 없음 | `page_06.png` - "훈민정음" 노출 없음, 4칸×2줄 + 뜻 쓰는 빈 줄, 목판 사진 포함 |
| #5 대상별 그림·답란 2개 유지 | `page_08.png` - 양반/일본 각자 그림+캡션+독립 답란, 영상 링크가 실제 클릭 가능한 링크(밑줄)로 `...p0`(정정된 URL) 표시 |
| #6 빈 답란 유지, 완성 답 노출 없음 | `page_09.png` - "...이유는," 과 "(이)기 때문이에요." 사이가 완전히 빈 줄, 가운데 정답 문장 노출 없음 |
| 원본 어휘 활동 보존 | `page_02.png` - 어휘 5개+OX 5개, "문장 만들기" 안내문이 실제로 원본과 일치(위 2번 참고) |
| 깨진 이미지, 잘림, 겹침 없음 | QA report(`check_report.json`) blockingCount=0, 9페이지 전체 오버플로 없음, 로고 전부 정상 표시(1번 수정 반영) |
| 표지 줄바꿈·인용부호 정상 | `page_01.png` - "그릇"이 한 줄에 온전히, 겹따옴표 1쌍만 표시 |
| 저장 후 재열기에서 편집 내용과 자산 유지 | 6번 표 11번 단계 - 전부 유지 확인, PDF sha256도 재다운로드와 완전 일치 |

문항#4(`page_07.png`)도 확인 - 원문 유실 없음, "토론 메모"는 별도 라벨로 원본 활동과
구분됨.

## 8. 운영 DB·기존 프로젝트·자동 승격 미변경 재확인

- 운영 DB: `discussion_qa.ui_type`(id 1607~1612) 전부 원래 값 그대로
  (`unknown/text_long/text_short_multi/text_long/unknown/text_short`),
  `documents.review_status`(L1-Q4-W01) 여전히 `'approved'` - 직접 재조회 확인.
- 기존 프로젝트 2개(`L1-Q4-W01-38a99e01`: `current_version=v1789749434572`,
  `L1-Q4-W01-927494f5`: `current_version=v1789751421654`) - **이번 8차 작업으로
  전혀 안 바뀜**.
- `momo_book_db/generated/EDIT-L1-Q4-W01-0b1e115e/current.json`(운영 승인 포인터):
  **생성 안 됨** - QA PASS여도 이 라우터 경로엔 `_promote()` 호출이 없어 구조적으로
  못 만들어짐(이전 차수와 동일한 보장).

## 9. 미실행·미확정 항목 (정직하게 기록)

- ~~"보기 활용 빈칸 3문항"을 원본 PDF 2쪽 전체에서 찾지 못했다~~ **→ 9차에서 정정:
  실제로 있었다.** 8차 때 page_2.png를 다시 안 열어보고 6차의(잘못된) 파일을 재사용한
  게 원인 - 9차에서 좌표 기반으로 다시 추출해 "낱말 뜻 선 연결하기 5개 + <보기> 활용
  빈칸 3문항"으로 복원 완료(아래 "9차" 섹션 참고).
- **배경지식/글쓰기(3단계)가 실제로 8쪽짜리 "학생용" 판본에 있는지는 이번에도 직접
  열어 대조하지 못했다** - 파일 존재만 확인(`D:\25년도 4분기 교재\초1\[4분기 1주차]
  한글 우리말을 담는 그릇(학생용).pdf`, 8쪽). 이번 복원 프로젝트에서는 출처 불명으로
  판단해 학생용 출력에서 제외만 해뒀다(운영 DB `background_text`/`essay_prompt`
  필드 자체는 안 건드림 - 그 판본에서 온 정상 데이터일 수도 있음).
- **"1-2 사고"/"생각상자"/쪽번호 분리 작업은 하지 않았다** - 근거가 될 8쪽 판본의
  해당 페이지를 못 봐서, 추측으로 텍스트를 잘라내는 대신 필드 전체를 이번 출력에서만
  제외하는 더 보수적인 방법을 택했다.
- **id=840/423/843/114(L3-Q4-W02, L5-Q4-W01, L5-Q4-W10의 복원 후보)는 이번 8차
  범위 밖** - 3차 안정화 기록 그대로.
- 실제 컬러 프린터 인쇄 확인은 여전히 미실행(이 환경엔 프린터 없음).
- 의도적으로 "로고 경로를 다시 깨뜨려서 QA가 이제 진짜 잡는지" 재현 시험은 하지
  않았다 - 원인이 이미 실제 기존 산출물(다른 문서 포함)에서 확인됐기 때문에 별도
  재현이 불필요하다고 판단했다. 필요하시면 다음에 별도로 시험 가능.

## 10. 접속 경로 (직접 확인하실 때)

- 주소: http://127.0.0.1:8100/login (계정: `admin@aprolabs.co.kr` / `apro0914@`,
  기존 개발용 부트스트랩 계정)
- 편집 화면: 로그인 → "학습지 자동 생성 미리보기" → 프로젝트 목록에서
  **`L1-Q4-W01-0b1e115e`**(데이터 출처: "복원 후보 적용", 가장 최신) 열기 - 문항
  #1~#6 전부 복원된 상태, 최신 PDF는 같은 화면의 "PDF 다운로드" 버튼으로 받을 수
  있음(현재 `C:\Users\aproa\Downloads\L1-Q4-W01-0b1e115e_v1789755691959.pdf`,
  sha256 `6b235446c2eac5f7735298a3f4c222cc57c0d77ae4fc45c185b7a4f7cb93877e`).
  이전 두 프로젝트(`L1-Q4-W01-38a99e01`: 6차의 text_long 버전,
  `L1-Q4-W01-927494f5`: 7차의 문장 누락·愛 단독페이지 버전)도 목록에 그대로 남아
  있어 이번 8차 결과와 직접 비교 가능.

## 11. 다음 시작점

1. ~~사용자가 언급한 "보기 활용 빈칸 3문항"의 정확한 위치 재확인.~~ → **9차에서 해결**
   (아래 참고).
2. 8쪽짜리 "학생용" 판본을 직접 열어 배경지식/글쓰기 활동의 실제 내용을 대조하고,
   맞으면 이번 프로젝트에 다시 포함시킬지 결정.
3. id=1607의 빈칸 3개를 원본처럼 문장 안 인라인으로 배치하려면 새 블록 타입이
   더 필요함(현재는 행별 분리 - 의도된 절충, 7차에 기록).
4. `check.js`의 로고 로딩 검사가 이번에 새로 생겼으니, 다음에 아무 문서든 다시
   QA를 돌릴 때 "PASS인데 로고만 깨진" 사례가 더 없는지 한 번 훑어보는 것을 권장.

---

# 9차: 어휘(1단계) 대조 오류 정정 — 낱말 뜻 연결 5개 + 보기 활용 빈칸 3문항 복원 (2026-09-19)

배경: 8차 보고서의 "어휘 활동이 원본과 이미 일치한다"/"보기 활용 빈칸 3문항을 못
찾았다"는 기록에 대해, 사용자가 ChatGPT로 원본 2쪽을 다시 렌더링해 대조한 결과
**두 활동(낱말 뜻 선 연결하기 5개, `<보기>` 활용 빈칸 3문항) 모두 실제로 존재한다**는
지적을 받았다. 원본 PDF부터 다시 검증하고, 8차의 오류 원인을 확인한 뒤, 실제로
복원했다.

## 1. 대조에 사용한 원본 파일 확인 (요청하신 절대 경로·페이지 수·SHA256)

```
경로: D:\25년도 4분기 교재\초1\[4분기 1주차]한글, 우리말을 담는 그릇 (베이직-학생용) (3).pdf
페이지 수: 6
SHA256: 1bb272aa1db09813a47c6c2f2780c2804a45a5394e441690e16ac02abbf1de86
파일 크기: 675,295 bytes
```

**사용자가 이 대화에 첨부한 원본과 SHA256이 완전히 일치**(같은 파일, 판본 차이 아님).
즉 이번 대조 오류는 "다른 판본을 본 것"이 아니라 **제가 직접 읽은 내용을 잘못
기록한 것**이었다.

## 2. 8차 오류의 원인 — 실제로 확인된 것과 확인 못 한 것

- **확인된 것**: 8차 보고서 작성 시점에 `page_2.png`(스크래치패드 `L1_source_pages/`
  폴더)를 **다시 렌더링하지 않고, 6차 때 만든 파일을 그대로 재사용**해 판단했다(8차의
  코드 기록을 보면 8차 세션에서는 `[2,3,4]`(페이지 3~5)만 다시 렌더링했고 페이지 2는
  건드리지 않았다). 그 6차 파일 자체가 이미 잘못된 내용(실제 원본이 아니라 이
  프로젝트가 만든 "단어+문장 만들기" 표와 매우 비슷한 내용)을 담고 있었다.
- **확인 못 한 것(재구성 불가)**: 6차 시점에 정확히 어떤 경위로 잘못된 내용이
  `page_2.png`에 담기게 됐는지는 이번에 재구성하지 못했다 - 코드상으로는 `doc[1]`을
  올바르게 렌더링하는 것과 동일한 로직이었는데, 실제로 그렇게 저장됐는지 이번엔 원본
  파일이 사라져 대조할 수 없었다(스크래치패드 파일은 이후 여러 번 같은 이름으로
  덮어써짐). **재발 방지책**: 이번 9차부터는 원본 PDF를 다시 인용할 때마다 (1) sha256을
  먼저 재확인하고, (2) 해당 페이지를 매번 새로 렌더링하며, (3) 좌표 기반 텍스트
  추출(`get_text('dict')`)로 교차 확인하는 절차를 거쳤다(아래 3번).

## 3. 재검증 방법과 실제 원본 2쪽 내용

PyMuPDF `get_text('dict')`로 페이지 2의 모든 텍스트 조각을 y/x 좌표와 함께 추출하고,
같은 페이지를 300dpi로 새로 렌더링해 육안으로도 대조했다(둘 다 일치). 실제 원본 2쪽:

1. **"낱말의 뜻을 찾아 선으로 이어보세요."** - 왼쪽 열(천덕꾸러기/까막눈/한나절/
   따끔하게/까마득하다)과 오른쪽 열(뜻풀이 5개)을 선으로 잇는 문제. **인쇄된 줄
   순서가 정답 짝이 아님을 좌표로 확인**했다 - 예: "천덕꾸러기"(1행)의 실제 뜻은
   3행에 인쇄된 "남에게 미움을 받는 사람이나 물건."이고, "따끔하게"(4행)의 실제
   뜻은 1행에 인쇄된 "마음에 큰 자극을 받아 따갑다."다(단어 뜻 그대로 대조해 확인).
2. **"각 문장에 들어갈 알맞은 낱말을 `<보기>`에서 골라 쓰세요."** - `<보기>` 안타깝다/
   어리석다/자그마치, 번호 매긴 문장 3개. **PDF 추출 텍스트에는 빈칸에 정답이 이미
   채워져 있고("...읽는데 ( 자그마치 ) 일주일이...") 문장마다 "* 단어 : 뜻풀이" 줄도
   있었지만, 300dpi로 확대 렌더링해 육안으로 재확인한 결과 그 어느 것도 실제 페이지에
   보이지 않았다** - 빈 괄호만 인쇄돼 있었다(다른 문항들과 같은 "추출 텍스트에 숨은
   답" 패턴).

기존에 8차까지 렌더링했던 "단어+문장 만들기 표"/"O.X 퀴즈"는 실제로는 **3쪽**의 내용
(O.X 퀴즈)과 섞여 있었다 - 실제 3쪽은 O.X 퀴즈 5개 + 문항#1이 한 페이지에 같이 있고,
2쪽은 어휘 활동 전용이었다(문항#1 자체의 복원 내용·이미지는 이전 차수 그대로 맞았음 -
이번에 다시 대조해도 문제 없음, 아래 6번 참고).

증거: `momo_book_db/worksheet/docs/evidence_20260919/`의 `page_2_hires.png`(전체),
`blank_area_crop.png`(빈칸 확대), `page2_coords.json`(좌표 추출 원본 데이터).

## 4. 코드 변경 — 어휘 매칭/빈칸 채우기 전용 블록 신규 추가

기존 `vocabTable()`("단어+문장 만들기" 표)은 **다른 문서에서는 여전히 유효한 활동
유형**일 수 있어 손대지 않았고(그대로 재사용 가능하게 유지), 이 문서에만 필요한
새 활동 2종을 **선택적으로만 켜지는** 별도 경로로 추가했다(`data.step1.vocab_match`가
있을 때만 타는 분기 - 없는 기존 문서는 100% 기존 동작 그대로).

- `momo_book_db/worksheet/scripts/blocks.js` — `vocabMatchBlock(words, definitions)`
  (뜻 연결하기 - 두 열을 정답 표시 없이 원본 인쇄 순서 그대로 나열, `data-vocab-id`로
  콘텐츠 대조 유지), `vocabFillBlankBlock(bank, sentences)`(보기+빈칸 문장, 정답·
  뜻풀이 없이 빈 괄호만) 신규 추가.
- `momo_book_db/worksheet/scripts/generate.js` — `buildStep1Pages()`에
  `vocab_match` 분기 추가(기존 vocab/ox 처리 로직은 그대로 유지, else-if로 분리).
- `momo_book_db/worksheet/build/styles.css` — `.vocab-match`/`.vocab-fillblank`/
  `.inline-blank` 등 신규 스타일 추가(기존 `.vocab-table` 스타일은 안 건드림).
- `app/routers/momo_worksheet_editor.py` — 복원 패치에 `table:"step1"` 지원 추가
  (문항 단위가 아니라 `data["step1"]` 전체 필드를 대상으로 하는 패치 - meta/step3와
  같은 패턴).
- `momo_book_db/worksheet/docs/RESTORATION_PATCHES.json` — 8차의 "원본과 일치"
  판단을 철회 기록(`vocab_match_WITHDRAWN_NOTE`, applied:false로 정정 이력만 남김)
  + `table:"step1"` 신규 patches 2건(`vocab_match`, `vocab_fillblank`) 추가.
- `momo_book_db/worksheet/edit_projects/L1-Q4-W01-d1f71612/` — 이번 시험으로 생긴
  새 프로젝트(기존 3개 프로젝트 `L1-Q4-W01-38a99e01`/`L1-Q4-W01-927494f5`/
  `L1-Q4-W01-0b1e115e`는 전부 그대로 둠).

`extract_worksheet_json.py`/`project_store.py`, 운영 DB(`momo_book.db`)는 9차에서도
전혀 안 건드림 - `vocabulary.definition`은 여전히 NULL(재확인, 아래 6번). 커밋·푸시·
운영 배포·유료 API 호출은 하지 않았다.

### 회귀 없음 확인

`L5-Q4-W11`을 재빌드(`vocab_match` 필드가 없는 일반 문서) - **8페이지, issues 0건,
기존과 동일**. 새로 추가한 분기가 `data.step1.vocab_match` 존재 여부로만 갈라지므로
기존 문서에는 전혀 영향이 없음을 재확인했다.

## 5. 실제 다운로드 PDF 확인

새 프로젝트(`L1-Q4-W01-d1f71612`, "복원 후보 적용")를 실제 로그인 브라우저에서 생성 →
저장(자동 재조판+QA) → **QA PASS** → "PDF 다운로드" 버튼으로 실제 파일 다운로드 →
PNG로 변환해 확인:

- `page_02.png` - 낱말 뜻 연결하기 5개(정답 표시 없음) + `<보기>` 빈칸 3문항(빈 괄호,
  정답·뜻풀이 노출 없음) + O.X 퀴즈 5개, 한 페이지에 전부 정상 배치, 겹침·잘림 없음.
- 나머지 7개 페이지(표지, 문항#1~#6)는 8차 결과와 동일하게 유지됨(로고·제목 줄바꿈·
  인용부호·愛 그림 배치·문항별 복원 내용 전부 재확인, `contact_sheet.png` 참고).

이번엔 사용자가 "나머지 수정은 보존"이라고 명시해, 텍스트 편집·이미지 업로드·답란
조절 등 추가 편집 조작은 다시 하지 않고 **저장(자동 QA)→다운로드**만 실제 브라우저로
수행했다 - 편집 폼 자체가 정상 동작한다는 것은 8차에서 이미 실제 조작으로 확인했다.

## 6. 운영 DB·기존 프로젝트 미변경 재확인

- `vocabulary.definition`(id 493~497, 운영 DB): 여전히 전부 `NULL` - 이번 정의
  텍스트는 patch의 `after`에만 있고 DB에는 쓰지 않았다.
- 기존 프로젝트 3개(`L1-Q4-W01-38a99e01`/`L1-Q4-W01-927494f5`/`L1-Q4-W01-0b1e115e`)
  - `current_version` 전부 이번 작업 전과 동일, 변경 없음.
- `momo_book_db/generated/EDIT-L1-Q4-W01-d1f71612/current.json`(운영 승인 포인터):
  생성 안 됨(구조적으로 불가능, 기존과 동일한 보장).

## 7. 최종 검증 결과 (요청하신 형식)

| 항목 | 값 |
|---|---|
| project_id | `L1-Q4-W01-d1f71612` |
| version_id | `v1789757839434` |
| QA 상태 | PASS |
| 다운로드 PDF 경로 | `C:\Users\aproa\Downloads\L1-Q4-W01-d1f71612_v1789757839434.pdf` |
| 다운로드 PDF SHA256 | `0fda74a8ad7b56b4a63d7afe8d488e903f94a61e2a24e13f9e39635cf793f4c1` |
| 원본 대조 파일 SHA256 | `1bb272aa1db09813a47c6c2f2780c2804a45a5394e441690e16ac02abbf1de86`(사용자 제공값과 일치 확인) |

## 8. 접속 경로

- 주소: http://127.0.0.1:8100/login (계정: `admin@aprolabs.co.kr` / `apro0914@`)
- 편집 화면: 로그인 → "학습지 자동 생성 미리보기" → **`L1-Q4-W01-d1f71612`**(가장
  최신, 어휘 활동까지 전부 복원된 상태) 열기. 이전 3개 프로젝트도 목록에 그대로 남아
  있어 비교 가능.

## 9. 남은 미확정 항목 (변동 없음)

- 8쪽짜리 "학생용" 판본(`documents.source_file`이 가리키는 파일)을 직접 열어 배경
  지식/글쓰기 활동을 대조하는 작업은 이번에도 하지 않음 - 7~8차 기록 그대로.
- id=1607 빈칸 3개의 인라인 배치(원본처럼 문장 속 빈칸)는 여전히 범위 밖 - 7차 기록
  그대로.
- 이번 9차 오류(6차의 잘못된 렌더링 원인)의 정확한 경위는 재구성하지 못함 - 재발
  방지책(매번 sha256 재확인 + 재렌더링 + 좌표 추출 교차 확인)만 마련해 둠.

---

# 10차: L1-Q4-W01 학생용 답란·배치 개선 (2026-09-19)

배경: 6~9차에서 **내용**(빠진 문장, 잘못 식별한 어휘 활동, 유형 재분류)을 복원·정정한
뒤, 이번엔 **배치·답란 형태**를 개선해달라는 요청이었다. 어휘 선 연결(2쪽) 간격 확대,
문항#1(3쪽) 인라인 빈칸화, 문항#2(4쪽) 괘선 답란→말풍선, 문항#3(5쪽) 글자칸-문장 연결
4가지를 코드로 구현하고, 기존 프로젝트(38a99e01/927494f5/0b1e115e/d1f71612)는 전혀
건드리지 않은 채 새 복제 프로젝트에서 작업했다.

## 1. 기준 버전 (변경 전)

```
project_id: L1-Q4-W01-d1f71612
version_id: v1789757839434
PDF: L1-Q4-W01-d1f71612_v1789757839434.pdf
SHA256: 0fda74a8ad7b56b4a63d7afe8d488e903f94a61e2a24e13f9e39635cf793f4c1
```

## 2. 코드 변경 (전부 하위호환 - 기존 문서가 새 옵션을 쓰지 않으면 출력 방식 그대로)

- `blocks.js`
  - `bodyBlanks(item)`의 `blanks[]` 항목에 `{bubble:true, image_path?, image_caption?}`
    지원 추가 - 괘선 답란 대신 빈 말풍선(`.speech-row`/`.speech-bubble`) 렌더.
  - `{chargrid:N, prefix?, mid_suffix?, blank_width_mm?, suffix?}`에 `mid_suffix`/
    `blank_width_mm` 추가 - 두 줄(문장1+글자칸 / 글자칸+문장2)을 한 줄로 이어 붙임.
  - `referenceBlock(item)`이 `reference_image2_path`가 있을 때 두 이미지를
    `.reference__images` flex-row로 나란히 배치(기존엔 세로로 쌓임).
  - 신규 `inlineBlankSpan(widthMm)`/`bodyInlineBlanks(item)` - `item.ui_config.
    inline_blanks`(문단별 `{parts:[{text}|{blank,width_mm}]}` 배열)를 문장 속 인라인
    빈칸으로 렌더. `answerSlotHtml`의 `text_short_multi` 분기가 `ui_config.
    inline_blanks` 존재 여부로 기존 `bodyBlanks`/신규 `bodyInlineBlanks`를 선택 -
    **문서 ID 하드코딩 없이 활동별 설정으로 분기**(요청하신 구현 주의사항 충족).
- `generate.js` - OX 안내문 클래스를 `step-intro--tight`→`step-intro--section-gap`로
  변경(빈칸 활동과 OX 사이 구분 여백).
- `check.js` - 답란 최소 높이(15mm) 검사 대상을 `.answer-lines`뿐 아니라
  `.speech-bubble`까지 포함하도록 일반화(말풍선도 동일 기준 적용, 요청하신 "기존
  15mm QA 규칙과 충돌 시 유형별로 구분" 중 "말풍선도 최소 필기 공간은 보장" 쪽을
  택함). `.inline-blank`(짧은 채워넣기용)는 의도적으로 제외 - 긴 서술형 최소 기준과
  섞이지 않게 구분.
- `styles.css`
  - `.vocab-match__cols` 중앙 간격 6mm→25mm, `.vocab-match__item`에 연결점 표시용
    `::before`/`::after`(2.6mm 원, `--point` 색) 추가.
  - `.step-intro--section-gap`(신규), `.vocab-fillblank` 상단 여백 4mm→5mm.
  - `.reference__images`(신규, flex-row) + 내부 `.figure-block` 테두리/배경 제거.
  - **버그 수정**: `.blk-blanks__row--chargrid`가 `flex-direction`을 명시하지 않아
    부모 `.blk-blanks__row`의 `column`을 상속받아 "글자 이름은/글자칸/이에요."가
    세로로 쌓이던 문제 - `flex-direction: row` 명시로 수정.
  - `.speech-row`/`.speech-row__figure`/`.speech-bubble`(신규) - 그림+말풍선 좌우
    배치, 말풍선은 줄 없는 필기 공간(`min-height:15mm`, `border-radius:6mm 6mm 6mm
    1mm`).
  - `.inline-blanks`/`.inline-blanks__para`(신규) - 처음엔 `justify-content:center`로
    작성했다가 렌더 확인 중 참고 이미지와 빈칸 문단 사이 여백이 과도한 것을 발견해
    `flex-start`+`margin-top:2mm`로 수정(자체 발견·자체 수정, 아래 4번 스모크 시험
    단계에서 확인).
  - 작업 중 `.inline-blank` 규칙이 실수로 중복 작성된 것을 `grep`으로 확인 후 제거.

## 3. 원본 대조·내용 보존 - 6~9차 복원 내용은 전부 그대로 유지, 배치만 변경

이번 요청은 배치 개선이라 원본 재대조는 하지 않았고, 대신 6~9차에서 이미 확정한
내용(어휘 5+뜻풀이 5+보기빈칸 3+OX 5, 문항#1 빈칸 3개+전체 문장, 문항#2 인물별
그림+답란 3개, 문항#3 글자칸-질문, 문항#4~6)이 배치만 바뀐 채 그대로 남아있는지
아래 5번에서 페이지별로 확인했다.

## 4. 실제 시험 - 스모크 시험 → 복제 프로젝트 실제 브라우저 시험

1. **스모크 시험**: 임시 프로젝트(`L1-Q4-W01-888a0775`, 실 사용자 프로젝트 아님)에서
   직접 Python 호출로 빌드해 CSS/JS 반복 조정(위 3번 `.inline-blanks` 여백 수정이
   이 단계에서 나옴). 확인 후 프로젝트 폴더와 `generated/EDIT-L1-Q4-W01-888a0775`를
   삭제해 정리.
2. **실제 로그인 브라우저**: `/login` 2차 시도로 로그인(1차는 입력 필드가 비어
   있었음, 기존 패턴과 동일) → 편집기 목록에서 L1-Q4-W01 "복원 후보 적용" 선택
   (네이티브 select: 클릭→Down→Tab, ref 기반) → "프로젝트 만들기" 클릭 → 새 프로젝트
   **`L1-Q4-W01-baa15e9d`** 생성 확인(목록에 기존 4건 포함 5건 전부 표시, 기존
   프로젝트는 그대로 있음) → 문항#1 질문 앞에 "[10차 배치 개선 시험] " 표시만 추가
   (원문 전체 보존) → 저장 → QA PASS 배지 확인 → **`v1789759385316`** → PDF 다운로드
   → 탭 닫고 새 탭에서 재로그인(역시 2차 시도) → 프로젝트 URL 직접 접속 → 편집
   내용·QA PASS·버전 이력·자산 목록 전부 유지 확인 → PDF 재다운로드 → 최초 다운로드와
   바이트 단위 동일 해시 확인.

```
project_id : L1-Q4-W01-baa15e9d
version_id : v1789759385316   (이 편집기 파이프라인은 별도 build_id 개념이 없고
                                version_id가 곧 실질적 빌드 식별자 - 8차 기록과 동일)
check_report.json: status=PASS, blockingCount=0, warnCount=0
최종 PDF(서버 저장분 export.pdf, 재다운로드분과 동일):
  경로: momo_book_db/worksheet/edit_projects/L1-Q4-W01-baa15e9d/versions/
        v1789759385316/export.pdf
  SHA256: 373757c8e29bbcb247551701830993d435925da3b727ab20d7e9ea937ab84980
  크기  : 751,806 bytes
  (최초 다운로드분·재열기 후 재다운로드분 모두 동일 해시로 확인)
```

## 5. 다운로드 PDF → 새 PNG 폴더 육안 확인 (이전 PNG 재사용 안 함)

`momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_10차_final_pngs/`에 방금
다운로드한 실제 PDF를 새로 렌더링해 7페이지 전부 확인:

- **2쪽(어휘)**: 확인 못 함(계약서상 "2쪽" 슬롯이 실제로는 1단계 반면에 포함돼
  `page_02.png`가 표지+1단계인지 별도 확인이 빠짐 - 아래 9번 미확정 항목 참고).
  ~~아래에서 정정~~ → contact_sheet 및 개별 페이지 확인 결과 1단계(어휘+OX)는
  `page_02.png`에 포함되어 있었고, 연결점(●) 표시와 넓어진 중앙 간격, 뜻풀이 줄바꿈,
  항목 순서(재정렬 안 됨) 전부 확인함.
- **page_03.png (문항#1)**: 편집 표시 "[10차 배치 개선 시험]" 확인, 이미지 두 장이
  테두리 상자 없이 나란히 배치, 인라인 빈칸 3개(폭 가변, 아래 별도 괘선 없음),
  "...농사짓기 바쁜 백성들은...없었어요."까지 문장 전체 보존, 캡션-그림 연결 확인.
- **page_04.png (문항#2)**: 말풍선 3행(인물별 그림+이름 좌측, 줄 없는 필기 공간
  말풍선 우측), 답 노출 없음, 필기 공간 충분히 확보됨.
- **page_05.png (문항#3)**: "이 글자의 이름은 [4칸] 이에요."와 "[4칸] 은 [뜻 쓰는
  빈 공간] 라는 뜻이에요."가 각각 한 줄로 붙어서 읽힘(flex-direction 버그 수정 확인),
  "훈민정음" 답 노출 없음, 바로 아래 문항#4(괘선 서술형)는 영향 없이 그대로.
- **page_06.png (문항#5)**: 이번 4가지 개선 범위 밖(요청 항목에 문항#5는 포함 안 됨)
  - 8차 상태(양반/일본 그림 + 독립 괘선 답란 2개, 영상 링크 "...p0") 그대로 유지되고
    있음을 확인, 답 노출 없음.
- **page_07.png (문항#6)**: 마찬가지로 범위 밖, 일반 서술형 질문+괘선 답란, 답 노출
  없음, 8차 상태 그대로.
- 전 페이지: 깨진 이미지·잘림·겹침 없음.

## 6. 운영 DB·기존 프로젝트·자동 승격 미변경 재확인

```
discussion_qa.ui_type (id 1607/1608/1609/1611/1612):
  [(1607,'unknown'), (1608,'text_long'), (1609,'text_short_multi'),
   (1611,'unknown'), (1612,'text_short')]   -- 운영 DB 원본값, 이번에도 불변
documents.review_status (L1-Q4-W01): 'approved'  -- 불변
vocabulary.definition (L1-Q4-W01, 앞 5건): 전부 None  -- 불변(운영 DB엔 정의 없음,
  편집기 프로젝트 오버라이드로만 존재 - 6~9차와 동일)

기존 프로젝트 current_version/last_good_version (단일 버전 폴더만 존재, 추가 버전 없음):
  L1-Q4-W01-38a99e01 : v1789749434572
  L1-Q4-W01-927494f5 : v1789751421654
  L1-Q4-W01-0b1e115e : v1789755691959
  L1-Q4-W01-d1f71612 : v1789757839434
  → 전부 10차 이전 기록과 동일, 10차 작업으로 추가 버전 생성되지 않음(새 프로젝트
    baa15e9d에서만 작업).

momo_book_db/generated/EDIT-L1-Q4-W01-baa15e9d : 존재하지 않음(편집기 프로젝트는
  표준 파이프라인의 generated/ 트리와 분리되어 있어 자동 승격 포인터(current.json)
  자체가 해당 없음).
```

## 7. L5-Q4-W11 재빌드 회귀 확인 (10차 코드 변경 반영 후 새로 재실행)

10차에서 공용 파일(`blocks.js`/`generate.js`/`check.js`/`styles.css`)을 바꿨으므로,
클린 기준 문서 L5-Q4-W11을 **10차 코드 반영 후 새로 재빌드**해 회귀를 확인했다(기존
`generated/L5-Q4-W11/data.json` 재사용, 새 build 폴더에만 씀 - 기존 `current.json`
포인터는 건드리지 않음, 아래 확인).

```
$ node scripts/generate.js L5-Q4-W11 ../generated/L5-Q4-W11/data.json
작성 완료: .../builds/2026-09-18T19-29-57-877Z/index.html (7p 내지 + 표지 1p), issues: 0건

$ node scripts/check.js .../builds/2026-09-18T19-29-57-877Z/index.html
=== 검사 완료: 8페이지, status=PASS (차단 이슈 0건 / 경고 0건) ===
✅ 표지 / 1단계 / 2단계 1~5 / 3단계 / 콘텐츠 보존 대조 통과
```

기존 정상본 포인터(`generated/L5-Q4-W11/current.json`)는 이번 재빌드로 바뀌지 않음
(`build_id: 2026-09-18T15-19-47-464Z`, 이전 그대로) - 확인용 재빌드일 뿐 운영 노출본
교체 아님. L5-Q4-W11은 참고 이미지 블록을 쓰지 않는 문서라 `.reference__images` CSS
변경은 이 문서엔 영향 없음(no-op) - 페이지 수·이슈·경고 전부 기준과 동일해 **회귀
없음** 확인.

## 8. 실제 프린터 출력 — 미실행

이 환경엔 프린터가 없어 실제 종이 출력은 시도하지 않음(6~9차와 동일한 제약).

## 9. 미실행·미확정 항목

- 2쪽(어휘) PNG를 별도 파일명으로 분리 확인하지 않고 contact sheet + 개별 페이지
  조합으로 확인함 - 파일 자체는 `page_02.png`로 존재, 내용은 5번에서 서술한 대로
  확인 완료(수치상 문제 없음, 절차 기록만 남김).
- 7~9차부터 이어진 미확정 항목(8쪽 "학생용" 판본 직접 대조, id=1607 인라인 배치
  범위 등)은 이번 10차 배치 개선으로 인해 자연히 일부(id=1607 인라인 배치)는
  **이번에 구현 완료**됨 - 나머지(8쪽 판본 대조)는 여전히 범위 밖으로 남음.

## 10. 접속 경로 (직접 확인하실 때)

```
1. http://127.0.0.1:8100/login 에서 로그인(admin@aprolabs.co.kr / apro0914@)
2. "학습지 자동 생성 미리보기" 메뉴 진입
3. L1-Q4-W01-baa15e9d 프로젝트 열기 (목록에 기존 4개 복원 후보 프로젝트와 함께 표시)
4. 최신 PDF: C:\Users\aproa\Downloads\L1-Q4-W01-baa15e9d_v1789759385316.pdf
   (SHA256: 373757c8e29bbcb247551701830993d435925da3b727ab20d7e9ea937ab84980)
```

운영 DB 수정·운영 배포·유료 API 사용·git push는 이번에도 실행하지 않음.

---

# 11차: 최종 배포본 정리 — 시험 표시 제거 + 어휘 연결점 위치 수정 (2026-09-19)

배경: 10차 최종 PDF를 실제 검수한 결과 두 가지 수정 요청 - (1) 문항#1 제목의 "[10차
배치 개선 시험]" 표시를 최종 배포용 내용에서 제거(시험용 프로젝트·증거는 보존),
(2) 어휘 선 연결하기 연결점이 25mm 간격의 중앙 부근에 찍혀 선 그을 공간이 없던 문제
수정(왼쪽 점은 낱말 상자 오른쪽 가장자리 가까이, 오른쪽 점은 뜻풀이 상자 왼쪽
가장자리 가까이, 25mm 공간을 실제 선 긋기용으로 비움).

## 1. 시험용 프로젝트·증거 보존 확인

10차 시험 프로젝트 `L1-Q4-W01-baa15e9d`(및 6~9차의 `38a99e01`/`927494f5`/`0b1e115e`/
`d1f71612`)는 이번에도 **전혀 건드리지 않음** - "[10차 배치 개선 시험]" 표시는 이
프로젝트의 편집 내용(content_overrides)에만 있던 것이라, 새 프로젝트를 "복원 후보
적용"으로 다시 만들면(RESTORATION_PATCHES.json에는 애초에 이 표시가 없음) 자동으로
빠진다 - 별도로 지울 코드가 필요 없었다. 대신 **최종 배포용 새 프로젝트**를 만들어
거기서 확인했다(기존 관행대로 - 교정마다 새 프로젝트).

```
기존 5개 프로젝트 current_version 재확인(변동 없음):
  L1-Q4-W01-38a99e01 : v1789749434572
  L1-Q4-W01-927494f5 : v1789751421654
  L1-Q4-W01-0b1e115e : v1789755691959
  L1-Q4-W01-d1f71612 : v1789757839434
  L1-Q4-W01-baa15e9d : v1789759385316   -- 10차 시험 프로젝트, 증거 그대로 보존
```

## 2. 코드 변경 (공용 CSS 1건, 문서 ID 하드코딩 없음)

`momo_book_db/worksheet/build/styles.css` - `.vocab-match__item::after`/`::before`
(연결점) 위치만 수정. 기존엔 양쪽 다 `-12.6mm`로 25mm 간격의 중앙 부근(거의 같은
위치)에 찍혀 선을 그을 공간이 없었다 - `right:-2mm`(왼쪽 열, 낱말 상자 가장자리 바로
바깥)/`left:-2mm`(오른쪽 열, 뜻풀이 상자 가장자리 바로 바깥)로 수정해 사이 21mm를
비워둠. `top:50%`+`translateY(-50%)`(항목 자신의 높이 기준 세로 중앙 정렬)는 기존
그대로 - 이미 요청하신 "각 점은 해당 항목의 세로 중앙에 맞춘다"를 만족하고 있었음.
항목 순서(재정렬 금지)도 기존 그대로 - 이번에 건드린 파일은 이 CSS 규칙 하나뿐.

## 3. 실제 시험

1. **스모크 시험**: Python으로 직접 임시 프로젝트(`L1-Q4-W01-7d8b636d`, "복원 후보
   적용" 방식으로 생성 - 편집 화면을 거치지 않아 시험 표시 자체가 안 생김) 빌드 →
   QA PASS(0/0) → PDF 렌더 → 2·3쪽 육안 확인(연결점 위치·문항#1 제목 둘 다 정상)
   → 확인 후 프로젝트 폴더 삭제(운영 미승격 확인 - `generated/EDIT-*` 폴더 자체가
   생기지 않음).
2. **실제 로그인 브라우저**: 로그인(세션 쿠키 유지 상태로 자동 로그인됨) → 편집기
   목록에서 L1-Q4-W01 "복원 후보 적용" 선택(ref 기반 Down+Tab) → "프로젝트 만들기"
   → 새 프로젝트 **`L1-Q4-W01-068f39ae`** 생성(문항#1 제목이 처음부터 "왜 한자는
   우리나라에서 널리 쓰이지 못했나요?"로 깨끗함, 편집 없이) → "저장하고 다시
   조판검사" 클릭(편집 내용 변경 없이 저장만) → QA PASS 확인 → **`v1789784229220`**
   → PDF 다운로드 → 탭 닫고 새 탭에서 프로젝트 URL 재접속(재로그인 불필요, 세션
   유지) → 편집 내용·QA PASS·버전 이력 전부 유지 확인 → PDF 재다운로드 → 최초
   다운로드와 바이트 단위 동일 해시 확인.

```
project_id : L1-Q4-W01-068f39ae
version_id : v1789784229220   (이 편집기 파이프라인은 version_id가 곧 실질적 빌드
                                식별자 - 8·10차 기록과 동일, 별도 build_id 없음)
check_report.json: status=PASS, blockingCount=0, warnCount=0
최종 PDF(서버 저장분 export.pdf, 최초·재다운로드 전부 동일):
  경로: momo_book_db/worksheet/edit_projects/L1-Q4-W01-068f39ae/versions/
        v1789784229220/export.pdf
  SHA256: b97936852e4ba56494bf2dcaec09a191862008104981ad79e77f3409b1049c6f
  크기  : 751,349 bytes
```

## 4. 다운로드 PDF → 새 PNG 폴더 2·3쪽 렌더링 확인 (이전 PNG 재사용 안 함)

`momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_11차_final_pngs/`에 실제
다운로드한 PDF를 새로 렌더링:

- **page_02.png(어휘 선 연결하기)**: 왼쪽 열(천덕꾸러기 등) 점이 각 낱말 상자 오른쪽
  가장자리 바로 바깥에, 오른쪽 열(뜻풀이) 점이 각 상자 왼쪽 가장자리 바로 바깥에
  찍혀 있고, 그 사이 넓은 공간(25mm 중 약 21mm)이 실제로 비어 있어 선을 그을 수 있음
  확인. 항목 순서(정답끼리 재정렬 안 됨), 뜻풀이 줄바꿈, 빈칸 3문항, OX 5문항 전부
  10차와 동일하게 유지.
- **page_03.png(문항#1)**: 제목이 "왜 한자는 우리나라에서 널리 쓰이지 못했나요?"로
  "[10차 배치 개선 시험]" 표시 없이 깨끗함. 인라인 빈칸 3개, 전체 문장("...농사짓기
  바쁜 백성들은...없었어요."까지), 한자책 사진+愛 그림 나란히 배치는 10차와 동일하게
  유지, 정답 노출 없음.

## 5. 운영 DB·기존 프로젝트·자동 승격 미변경 재확인

```
discussion_qa.ui_type (id 1607/1611): [(1607,'unknown'), (1611,'unknown')]  -- 불변
documents.review_status (L1-Q4-W01): 'approved'  -- 불변
기존 5개 프로젝트(38a99e01/927494f5/0b1e115e/d1f71612/baa15e9d): 전부 단일 버전
  폴더만 존재, 10차 이전 기록과 동일 - 이번 작업으로 추가 버전 생성 안 됨.
generated/EDIT-L1-Q4-W01-068f39ae: 존재하지 않음(편집기 프로젝트는 표준 파이프라인의
  generated/ 트리와 분리 - 자동 승격 포인터 자체가 해당 없음).
```

## 6. 접속 경로 (직접 확인하실 때)

```
1. http://127.0.0.1:8100/login 에서 로그인
2. "학습지 자동 생성 미리보기" 메뉴 진입
3. L1-Q4-W01-068f39ae 프로젝트 열기 (최종 배포용, 시험 표시 없음)
   - L1-Q4-W01-baa15e9d(10차 시험용, 증거 보존)도 목록에 그대로 남아 있음
4. 최신 PDF: C:\Users\aproa\Downloads\L1-Q4-W01-068f39ae_v1789784229220.pdf
   (SHA256: b97936852e4ba56494bf2dcaec09a191862008104981ad79e77f3409b1049c6f)
```

운영 DB 수정·운영 배포·유료 API 사용·git push는 이번에도 실행하지 않음.

---

# 12차: 모모의 책장 교재 편집기 — 페이지별 편집 P1 (2026-09-19)

배경: `momo_page_editor_plan.md`(사용자 제공 기획서)를 구현 지침으로 삼아, "선택한 페이지만
고정 편집 + 후보 미리보기 + 적용" 기능을 기존 편집기 위에 실제로 연결했다. 기획서 §10의
P1(페이지 고정과 직접 편집) 범위만 이번에 완료하고, P2(공통 템플릿)·P3(AI 대화)·P4(이미지
생성)는 이번 범위에서 UI·기능을 확장하지 않았다(§9 API 제공자 연결 코드 유무만 확인).

## 0. 먼저 확인한 것

1. `AGENTS.md`/`00_START_HERE.md`/`WORK_START_PROMPT.txt`/`SOURCE_ORIGINALS` - 저장소 전체를
   찾아봤지만 **존재하지 않음**(`implementation/` 디렉터리도 없음) - 없는 파일을 새로 만들지
   않고 `PROGRESS.md`(11차까지)만 인계 문서로 확인하고 진행했다.
2. 기존 구조 조사: FastAPI 편집기(`app/routers/momo_worksheet_editor.py`) + Jinja
   (`app/templates/momo_worksheet_editor/`), Node 조판(`generate.js`/`blocks.js`/
   `paginate.js`/`check.js`/`export_pdf.js`), 저장소(`worksheet/editor/project_store.py`,
   `worksheet/edit_projects/<pid>/{base_data.json, versions/<vid>/{content_overrides.json,
   design_overrides.json, index.html, check_report.json}, assets/}`) 전부 실제 코드를 읽어
   확인. **핵심 발견**: 현재 저장 흐름(`editor_save` → `_run_pipeline` → `node generate.js`)은
   문항 하나만 고쳐도 **문서 전체를 처음부터 다시 배치**한다 - 이 상태로는 "선택 페이지 외
   불변"을 구조적으로 보장할 수 없어, 페이지 구성표를 얼리고 조각 하나만 다시 렌더링하는
   별도 엔진이 필요하다고 판단(아래 A절).
3. 기준 프로젝트 확인: `L1-Q4-W01-068f39ae`(11차 완료본) - `versions/v1789784229220`
   그대로 존재, 단일 버전, 이번 작업에서 전혀 건드리지 않음(아래 F절에서 재확인).
   `L5-Q4-W11`은 편집기 프로젝트 3건(`1b4009d3`/`25fb7596`/`625d46b8`)이 있어 회귀 확인에
   `L5-Q4-W11-1b4009d3`(v1789747995576)를 사용했다.
4. 제공자 연결 코드만 확인(실호출 없음): `app/services/tagger.py`/`classifier.py`에
   `anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))` 연결 코드 존재.
   `.env`에 `ANTHROPIC_API_KEY`만 설정되어 있고(존재 여부만 확인, 값 미출력)
   `OPENAI_API_KEY`/`GEMINI_API_KEY`/`GOOGLE_API_KEY`는 미설정. 즉 **현재 실제로 연결된
   제공자는 Claude(Anthropic) 하나뿐** - P3(대화 편집)를 나중에 붙일 때 "실제 연결된
   제공자 목록"에는 이것만 노출해야 하고, 이번 12차에서는 대화 기능 자체를 만들지 않았다.
5. `SOURCE_ORIGINALS`·운영 DB·승인 포인터·기존 사용자 편집본: 전부 손대지 않음(F절 실측).

## 1. 아키텍처 — "페이지 구성표 고정 + 조각 단위 재렌더링"

기존 `generate.js`의 `main()`은 `sections`(페이지별 HTML 문자열 배열)를 만든 뒤
`sections.join('\n')`으로 문서를 조립한다. 이 배열 자체가 이미 "페이지 단위"이므로, 이
배열을 **한 번 얼려서(page_manifest) 저장**하고, 편집 시에는 **딱 하나의 조각(item의
lead/answer/merged 부분)만 다시 만들어 그 조각의 원본 문자열을 치환**하는 방식을 택했다.
새 배치 알고리즘을 만들지 않고 기존 `paginateStep2`/`buildStep1Pages`/`buildStep2Pages`
등을 얼리는 시점에 그대로 재사용한다("기존 자동 조판을 그대로 재사용" 원칙).

- `paginate.js`: `half()`/`placeItem()`/`paginateStep2()`가 각 반면·전체페이지 조각에
  `{itemId, part}` 이름표(tag)를 추가로 들고 다니도록 확장(기존 `generate.js`는 `.html`/
  `.center`만 읽으므로 하위 호환 100%). `buildBundles`/`half`를 `module.exports`에 추가.
- `generate.js`: 자산 상대경로 치환 로직을 `fixAssetPaths(html, outPath)` 함수로 분리해
  export(표준 파이프라인과 페이지 편집기가 "하나의" 구현을 공유 - 따로 관리하다 한쪽만
  고쳐지는 위험 제거). `buildStep1Pages`/`buildStep2Pages`/`buildStep3Page`/`wrapHalves`도
  export. `require.main === module`로 감싸서 `require()`해도 `main()`이 즉시 실행되지
  않게 함(기존 `node generate.js ...` CLI 호출은 100% 그대로 동작 - 실제 재확인함, 아래 2절).
- `page_manifest.js`(신규): `freeze(data)` - `generate.js`/`paginate.js`의 기존 함수만
  그대로 불러써서 페이지 배열을 만들고, 각 페이지에 `page_id`(신규 발급, 영구 식별자),
  `role`/`role_index`, `layout_type`(halves/fullpage/opaque/cover), `slots`(조각별
  `{itemId, part, html}` - `html`은 감싸이기 전 원본 문자열이라 `html_raw`의 부분 문자열임이
  보장됨), `html_raw`, `content_hash`를 붙인다. `assemble(manifest, overrides, ...)` -
  다른 페이지는 `html_raw` 그대로, 수정된 페이지만 `overrides[page_id]`로 치환해 이어붙이고
  `fixAssetPaths()`로 최종 문서를 만든다(표준 파이프라인과 동일한 문서 골격).
- `page_candidate.js`(신규) - **검증 가능한 편집 명령 계약**(기획서 §6/§7, 후속 LLM 재사용
  대상): 요청 JSON `{operation, page:{layout_type,html_raw,slots}, target:{itemId,part},
  item_effective}` → `paginate.buildBundles(item)`로 해당 조각만 다시 렌더링 →
  `layout_type`이 halves면 `paginate.overflow()`(반면 예산), fullpage면 `overflowFull()`
  (전체 페이지 예산)로 **그 조각 하나만** 실측 오버플로 검사 → 넘치면 구체적 이유와 함께
  `{status:'blocked', reason, part, budget}` 반환(자동 축소·다음 페이지 이동 없음) → 안
  넘치면 원본 조각 문자열이 페이지 안에서 정확히 1회 나오는지 확인 후 그 부분만 치환한
  `html_raw`를 반환. `set_text`/`set_answer_area`/`replace_image` 세 연산을 지원(기획서
  예시 명령 중 이번 범위).
- `worksheet/editor/page_store.py`(신규, `project_store.py`는 전혀 수정 안 함):
  `freeze_manifest`(명시적 전환 - 그 프로젝트의 `/pages` 화면에 실제로 들어왔을 때만,
  이미 있으면 재사용), `create_proposal`(후보 생성 - base_revision 검증, Pillow로 실제
  이미지 디코딩·최소 해상도·용량 검증 후 `ps.add_asset` 재사용), `apply_proposal`(원자적
  적용 - proposal을 먼저 `applying`으로 표시해 중복 적용 방지 → 기존 `ps.save_version`으로
  content/design overrides에 이번 변경분만 병합해 새 편집 버전 생성 → `page_manifest.js
  assemble`로 문서 조립 → 기존 `check.js` 그대로 실행 → **QA PASS일 때만** 새 페이지
  리비전을 커밋, FAIL이면 페이지 구성표는 전혀 안 바뀌고 오류만 보고), `cancel_proposal`,
  `restore_revision`(이전 페이지 구성으로 복원 - 새 리비전으로 기록, 기존 리비전을
  지우거나 덮지 않음).
- `app/routers/momo_worksheet_page_editor.py`(신규 라우터, 기존 `momo_worksheet_editor.py`
  는 한 줄도 안 고침) + `app/templates/momo_worksheet_editor/pages.html`(왼쪽 썸네일/가운데
  적용본·후보 비교/오른쪽 직접 수정 패널) + `pages_history.html`/`pages_error.html`.
  기존 문항 편집 화면(`detail.html`)에 "🧷 페이지별 편집(신규)" 버튼을 추가해 실제 앱에서
  발견 가능하게 연결(정적 데모 아님, 같은 프로젝트·같은 버전 체인·같은 PDF 다운로드
  라우트를 공유).
- **부수 발견 및 수정**: 편집 프로젝트 미리보기(`/momo-worksheet-editor/{pid}/preview/
  {vid}/index.html`)가 실제 파일 깊이(5단계)에 맞춘 상대경로를 그대로 서빙하는데, 이
  라우트의 URL 경로는 4단계뿐이라 브라우저가 `../`를 루트에서 클램프해
  `/worksheet/build/styles.css` 같은, 지금까지 아무 마운트도 없던 경로를 요청 - **기존
  문항 편집 화면의 미리보기도 지금까지 CSS·이미지가 전부 404였다**(PDF 다운로드는 파일
  경로를 직접 여는 별도 코드라 안 걸림 - 실제 PDF는 항상 정상이었음). `app/main.py`에
  `/worksheet/build`, `/extracted_images`를 루트로 미러링하는 정적 마운트 2개를 추가해
  수정(페이지 편집기의 후보 미리보기도 같은 문제라 필수였음) - 실제 브라우저로 수정 전/후
  차이를 확인(아래 3절).

## 2. 핵심 불변 조건의 수학적 검증(구현 직후, 실제 자료로)

- `page_manifest.js freeze()`로 만든 페이지 배열을 그대로 이어붙여 조립한 문서와
  `generate.js`가 **같은 데이터로 직접 만든 문서**를 diff → **L5-Q4-W11, L1-Q4-W01-
  068f39ae(가장 복잡한 문서 - vocab_match/inline_blanks/bubble/chargrid 전부 포함) 둘 다
  byte 단위로 완전히 동일**함을 확인(단순 추정이 아니라 실측).
- `page_candidate.js`로 문항 1607의 문장만 바꾼 후보를 만들어 전체 문서에 대입 →
  변경 전/후 문서를 `<section class="page">` 단위로 쪼개 비교 → **정확히 그 문항이 속한
  페이지 1개만 다름, 나머지 6개 페이지는 문자열 완전 동일**을 확인.
- `check.js`를 그 대입된 문서에 그대로 돌려 PASS(콘텐츠 보존 대조 포함) 확인.

## 3. 실제 브라우저 시험 (복제 프로젝트, 정상 로그인)

기존 프로젝트(`38a99e01`/`927494f5`/`0b1e115e`/`d1f71612`/`baa15e9d`/`068f39ae`)는 전부
그대로 두고, "복원 후보 적용"으로 새 프로젝트 **`L1-Q4-W01-132be3d8`**을 만들어 시험했다
(첫 시도에서 데이터 출처 드롭다운을 "원본 그대로"로 잘못 둔 채 만든 프로젝트
`L1-Q4-W01-e2457d25`는 즉시 폐기·삭제 - 실제로 만든 진짜 프로젝트가 아니므로 기록만 남김).

1. 문항 편집 화면에서 최초 저장(QA PASS) → "🧷 페이지별 편집(신규)" 클릭 → 페이지 구성표
   최초 고정(freeze, 7페이지: 표지/1단계1/2단계1~5).
2. **문장 수정**(문항 1607, 3쪽): "[12차 페이지편집 시험]" 추가 → 후보 생성(정상,
   반면/전체페이지 예산 안에서 실측 확인) → 전/후 미리보기 비교 → 적용·저장 → QA PASS,
   새 페이지 리비전 커밋.
3. **그림 업로드·교체**(문항 1608, 4쪽 참고자료 그림 - 기존엔 비어 있던 필드): 실제
   PNG 파일 업로드(Pillow로 디코딩·해상도 검증 통과) → 후보 생성 → 적용 → 해당 페이지의
   "참고자료" 박스에만 반영, **같은 페이지의 다른 3개 인물 그림(장쇠/간난이/꽃네, 별도
   필드)은 전혀 영향 없음**(5절 PNG 확인).
4. **답란 높이 조절 - 넘침 차단**(문항 1612, 7쪽): 200mm 입력 → 실제 화면에
   "⛔ 수정한 내용이 반면(페이지 절반) 안에 들어가지 않습니다 (현재 답란 높이 200mm).
   답란 높이를 줄여 보세요. 다음 페이지로 넘기거나 글자를 자동으로 줄이지 않습니다"
   차단 메시지 표시, 적용 버튼 비활성(후보 자체가 'blocked' 상태라 적용 라우트 진입 시
   서버에서도 재차 거부) - **적용 전 정상본 불변** 확인.
5. **취소**: 같은 문항에 30mm(정상 범위)로 후보를 새로 만든 뒤 "후보 취소" 클릭 →
   `project.json.current_version`·`page_manifest.json.revision_id` 둘 다 클릭 전후 완전
   동일(파일로 직접 재확인) - "적용 전 취소 시 현재 버전·마지막 정상본 불변" 확인.
6. **오래된 후보(두 탭) 시험**: 탭A(문항 1612)·탭B(문항 1609)에서 같은 기준 리비전으로
   각각 후보 생성 → 탭A 먼저 적용(성공, 리비전 전진) → 탭B(새로고침 안 한 채)에서
   "적용" 클릭 → 실제로 **"다른 저장이 먼저 반영되어 이 후보의 기준이 오래되었습니다.
   후보를 다시 생성한 뒤 적용해주세요."** 오류가 화면에 표시되고 적용은 거부됨. 탭B의
   편집 내용(입력한 문장)은 그대로 남아 있어, 후보를 다시 생성해 적용 → 정상 반영됨
   (편집 유실 없음 확인).
7. 위 4건의 적용을 모두 마친 뒤 페이지 구성표 리비전 체인(`rev_8e5395116782` →
   `rev_e4ad244130e0` → `rev_2e6f8a0d933a` → `rev_90a5e7af3230` → `rev_c7b46514075c`)을
   따라 **최초 고정본 vs 최종본**을 페이지별로 비교: 표지·1단계 페이지, 그리고 한 번도
   건드리지 않은 문항 1611(6쪽) 페이지는 4번의 적용을 거치는 동안 **문자열 완전 동일**,
   수정한 4개 문항(1607/1608/1609/1612)이 속한 페이지만 변경됨(모든 `page_id`는 리비전이
   바뀌어도 동일 - 영구 식별자 유지 확인).
8. **새 브라우저 컨텍스트로 재열기**: 탭을 모두 닫고 새 탭에서 `/login`부터 재접속(30일
   로그인 유지 쿠키로 자동 로그인됨 - 기존 세션 정책 그대로) → 프로젝트 재열기 →
   `revision=rev_c7b46514075c`(최종 상태) 그대로, 문항 1607 텍스트·문항 1608 이미지 경로
   (`EDIT-L1-Q4-W01-132be3d8/60c58ea6a44d5952f783f322.png`) 유지 확인. 업로드 이미지
   파일의 sha256(`60c58ea6...61f53d`)이 업로드 시점과 재열기 후 완전히 동일함을 직접 대조.
9. PDF 다운로드 → 닫았다 다시 다운로드 → **두 파일 sha256 완전 동일**
   (`486195c5dd15f25122707d4c66d247fb6a16e43861bb6a46275d306e0ace2110`).

## 4. 페이지별 래스터 확인 (전체 해시가 아니라 페이지 단위, 요청하신 방식대로)

실제 다운로드한 PDF(위 8-9번, `L1-Q4-W01-132be3d8_v1789795354338.pdf`)를 새 PNG 폴더
(`momo_book_db/worksheet/docs/evidence_20260919/L1-Q4-W01_12차_page_editor_pngs/`)에
렌더링해 7페이지 전부 육안 확인:

- 표지/1단계(2쪽): 6~11차 내용 그대로.
- 3쪽(문항#1): "[12차 페이지편집 시험]" 반영, 인라인 빈칸·참고 이미지 2장·전체 문장 보존.
- 4쪽(문항#2): 업로드한 그림이 "참고자료" 박스에 반영, 인물별 그림+말풍선 3개는 무관하게
  그대로(장쇠/간난이/꽃네 원본 이미지·캡션 완전 동일).
- 5쪽(문항#3·#4): 1609 문장에 "[탭 B 최종]" 반영(제가 테스트 중 문장 끝이 아니라
  중간에 삽입해 문장이 다소 부자연스러워졌음 - 시험용 텍스트라 문제 아님), 글자칸 배치·
  1610(문항#4) 완전 무관.
- 6쪽(문항#5): **전혀 건드리지 않아 8~11차 상태와 완전히 동일**(양반/일본 그림, 영상
  링크 `...p0`).
- 7쪽(문항#6): "[탭 A 충돌시험]" 반영, 글자칸-설명 연결 유지.
- 전 페이지 정답 노출·깨진 이미지·잘림·겹침 없음.

## 5. 회귀 확인 - L5-Q4-W11 기존 경로

`L5-Q4-W11-1b4009d3`의 저장된 `effective_data.json`을 (새 `page_manifest.js`가 아니라)
**기존 `generate.js` CLI로 그대로** 재빌드 → `status=PASS, 차단 0건, 경고 1건`(기존에도
있던 그림 저해상도 경고 - 이번 세션 변경과 무관, 10차에 기록된 값과 동일), 8페이지,
콘텐츠 보존 대조 통과. `paginate.js`/`generate.js`에 가한 변경(조각 이름표 추가, 함수
export, `fixAssetPaths` 분리)이 **기존 표준 조판 결과에 아무 영향이 없음**을 재확인.

## 6. 운영 DB·기존 프로젝트·SOURCE_ORIGINALS 미변경 재확인

```
discussion_qa.ui_type/question_text (id 1607/1608/1609/1611/1612): 운영 DB 원본값 그대로
  (1607 unknown, 1608 text_long, 1609 text_short_multi, 1611 unknown, 1612 text_short)
L1-Q4-W01-068f39ae: current_version=v1789784229220 그대로, 단일 버전, page_manifest.json
  생성 안 됨(페이지 고정 편집은 프로젝트별로 명시적으로 그 화면에 들어가야만 시작됨 -
  일괄 자동 전환 없음을 실측 확인).
SOURCE_ORIGINALS: 저장소에 해당 이름의 파일/폴더 자체가 없음(0절 참고) - 보존 여부
  자체가 해당 없는 항목.
```

## 7. 미해결·다음 시작점

- **UI 상호작용 안정성**: 이번 회차의 실제 브라우저 시험 중, 버튼 클릭이 `ref` 기반으로는
  간헐적으로 등록되지 않고(같은 요소를 좌표 기반으로 다시 클릭하면 성공) - 서버 로그로
  교차 확인한 결과 **애플리케이션 자체의 버그가 아니라 자동화 도구의 클릭 등록 문제**였다
  (요청이 실제로 서버까지 간 경우는 항상 의도대로 처리됨). 다만 "적용 완료" 배지가
  아직 없는 후보(status=ok)가 그대로 남아 있다가, 사용자가 취소 없이 페이지를 벗어나면
  다음 방문 때 오래된 후보처럼 보일 수 있는 자잘한 UX 다듬기 여지는 있음(발견 즉시
  `list_active_proposal`이 "가장 최근 시도가 취소·적용·오류로 끝났으면 활성 후보 없음"을
  반환하도록 수정 완료 - 재현 확인함).
- **P1 범위 밖으로 의도적으로 비워둔 것**(화면에 "미연결"로 명시): 그림 폭/높이·자르기·
  캡션 조절, 좌우/상하 배치 조절, 1단계(어휘/OX)·3단계(글쓰기)·표지 페이지의 직접 편집
  (현재는 고정·조회만 가능 - "🔒" 표시), AI 대화 탭(회색, 클릭 불가).
- **P2(공통 템플릿)**: 착수 안 함 - 착수 시 새 템플릿 버전 저장 + 교재 복사본 미리보기
  구조가 필요하며, 이번 페이지 편집과는 별도 모드로 유지해야 함(기획서 §3 그대로).
- **P3(AI 대화)**: 착수 안 함. `page_candidate.js`의 입력 계약(operation/target/
  item_effective)이 이미 "검증된 편집 명령"의 형태이므로, 향후 LLM은 이 계약에 맞는
  JSON만 만들면 됨 - 실제 조판·검증은 항상 이 스크립트가 담당하도록 설계해 둠. 실제
  연결된 제공자는 Claude(Anthropic) 하나뿐임을 확인(0-4절) - 유료 호출 승인 없이는
  실연동 시험 자체가 BLOCKED.
- **실제 프린터 출력**: 미실행(환경 제약, 기존과 동일).

## 8. 변경 파일 목록 (12차분)

```
신규:
  momo_book_db/worksheet/scripts/page_manifest.js
  momo_book_db/worksheet/scripts/page_candidate.js
  momo_book_db/worksheet/editor/page_store.py
  app/routers/momo_worksheet_page_editor.py
  app/templates/momo_worksheet_editor/pages.html
  app/templates/momo_worksheet_editor/pages_history.html
  app/templates/momo_worksheet_editor/pages_error.html
수정:
  momo_book_db/worksheet/scripts/paginate.js   (조각 이름표 tag 추가, buildBundles/half export)
  momo_book_db/worksheet/scripts/generate.js   (fixAssetPaths 분리·export, require.main 가드)
  app/main.py                                   (새 라우터 등록, /worksheet/build·
                                                  /extracted_images 루트 마운트 추가)
  app/templates/momo_worksheet_editor/detail.html (페이지별 편집 신규 진입 버튼)
```

## 9. 접속 경로 (직접 확인하실 때)

```
1. http://127.0.0.1:8100/login 로그인
2. "학습지 자동 생성 미리보기" → L1-Q4-W01-132be3d8 열기 → "🧷 페이지별 편집(신규)" 클릭
   (또는 바로 http://127.0.0.1:8100/momo-worksheet-editor/L1-Q4-W01-132be3d8/pages)
3. 최신 PDF: C:\Users\aproa\Downloads\L1-Q4-W01-132be3d8_v1789795354338.pdf
   (SHA256: 486195c5dd15f25122707d4c66d247fb6a16e43861bb6a46275d306e0ace2110)
4. 최종 페이지 구성표 revision: rev_c7b46514075c / 적용된 편집 버전: v1789795354338
```

운영 DB 수정·운영 배포·유료 API 사용·git push는 이번에도 실행하지 않음.
