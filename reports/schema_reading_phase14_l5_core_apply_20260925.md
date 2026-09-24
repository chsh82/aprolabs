# 스키마리딩x어휘 — 14단계: L5 신규 핵심 48건(L5_CANDIDATE 6 + 신규 42) 학년 근거 재검증 + REUSE 판정 + 단일 트랜잭션 적재

- 작성일: 2026-09-25 (`date` 명령으로 시스템 현재 날짜 직접 확인)
- 범위: 12단계(`reports/schema_reading_phase12_l4_l5_spiral_review_20260924.md`)가 확정한
  `group=E_34_L5_spiral_check`의 `L5_CANDIDATE` 6건(대등·사후·본론·논술·부가·성향)을 이번
  세션이 "고1 학년 근거"를 다시 확인한 뒤 재채택했고, 나머지는 literacy.db의
  V(schemareading-tooldict)/S(schemareading-schema) level=5 원천에서 이번 세션이 새로
  선정한 42건(V 18 + S 24, 검토 후보 모수 64건 중)이다. `L5_SPIRAL_REVIEW` 28건(phase12가
  이미 나선형 반복으로 분류)은 지시대로 이번 배치에서 완전히 제외했다(읽기만 재확인,
  쓰기 없음). "부여"·"수집"은 이번에도 계속 보류(L5_SPIRAL_REVIEW 28건에 포함되어
  자동으로 제외됨 — 별도 조치 불필요).
- **13단계(L4) 신규 49건, 기존 L4 관련 S 13건, "가변성" 1건(HOLD)은 이번 세션이 전혀
  건드리지 않았다** — 읽지도 쓰지도 않았고, 적재 후 체크섬으로 불변을 직접 재확인했다(6절).
- literacy.db는 이번 세션 내내 `mode=ro`+`PRAGMA query_only=ON`으로만 열었다(SELECT만
  실행, INSERT/UPDATE/DELETE/DDL 없음). research DB 쓰기는 4번 작업의 단일 트랜잭션
  한 번만 수행했다. git commit/push 없음.

---

## 0. 핵심 요약

1. **L5_CANDIDATE 6건을 "고1 학년 근거"를 별도로 재검증한 뒤 전부 채택했다.** literacy.db의
   `grade_level` 컬럼은 V(tooldict)/S(schema) 두 소스 전체(3,382건)에서 예외 없이 NULL이라
   (1-2절), "레벨=5 숫자"만으로 고1을 자동 확정하지 않았다. 대신 `docs/literacy/07-학년경계정책-L5L6.md`
   (2026-09-24, 사용자가 명시적으로 확정한 정책: "L5=고1, L6=고2~3")를 직접 재확인해,
   이 프로젝트가 `terms.level=5`를 고1로 매핑하는 근거를 별도 문서로 이미 확정해 두었음을
   재확인했다. 이 정책은 momo-textbook 소스의 개별 학생-학년 태그(`grade_level`)가 아니라
   "학습 도구어 사전/스키마 어휘 목록" 자체의 6단계 패키지 레벨에 대한 것이므로, 개별
   학년 태그만큼 강한 증거는 아니다 — 이 차이를 리스크로 명시했다(7절 1번).
2. **신규 42건(V 18 + S 24)을 literacy.db V/S level=5 원천(V 108건 중 미검토 74건, S 507건
   중 미검토 507건)에서 선정했다.** 번호 붙은 동형이의 표기(V, 12건: 결정01·기술01·기술03·
   기원04·기원05·답03·대체02·대체03·등05·사상15·사적02·상기19), POS 누락 1건(바람직),
   인문 철학 소분류 28건 전체(형이상학·윤리학 등, POS 전부 NULL이며 다수가 절 형태의
   긴 표제어라 고1 일반 어휘로 보기 어려움), 그 외 과도하게 전문적인 명명 이론·인명 결합
   표제어(해양저확장설·맨틀 대류설·호상열도·모스굳기계·레오폴드의 대지윤리·아리스토텔레스의
   바람직한 정치 등)는 후보에서 제외했다. **목표 50건을 채우려고 근거 부족한 항목을 넣지
   않았다** — 실제로 개별 검토를 마친 44건(V 18+S 24, HOLD 2건 제외)만 최종 배치에 포함했다.
3. **동형이의어 검사에서 신규 HOLD 2건을 발견했다**: "검정"(색깔을 뜻하는 압도적으로
   우세한 일상 의미와 충돌 — literacy.db에는 "자격 검사" 뜻 1개만 있지만, 탈맥락 정의
   매칭 퀴즈에서 학생이 거의 확실히 "검은색"을 먼저 떠올릴 위험이 phase13의 "정상" 사례보다
   훨씬 크다고 판단), "내면"(literacy.db에 저장된 뜻이 표준국어대사전 1번 의미인 "물건의
   안쪽"뿐이고, 독서·문학 지문에서 압도적으로 자주 쓰이는 2번 의미 "사람의 정신이나 마음"이
   빠져 있음 — phase13의 "가변성" HOLD와 같은 유형의 "저장된 정의가 실제 쓰임과 다름"
   문제). **이 2건만 개별 HOLD 처리했고 작업 전체를 멈추지 않았다.**
4. **반의어/근접어 캐주션 쌍을 다수 발견해 결과표에 기록했다**: 가변/불변(반의어, 불변은
   배치 밖), 거시/미시(대비 짝, 미시는 배치 밖), 사후/향후(반의 시간 개념, 향후는 배치 밖),
   성향/지향(L4 기존 콘텐츠와 근접, 유추/유사급 낮은 위험), 개관/개괄(동의어에 가까운
   근접), 구축/구현(뜻이 비슷해 실제로 자주 혼용), 해령/해구(형태가 반대인 지형, 배치
   안에 둘 다 있음), 발산형 경계/수렴형 경계(짝 개념), 쪼개짐/깨짐(짝 개념),
   인간중심주의 자연관/생태중심주의 자연관(반의 짝, 표제어 끝부분 동일), 도시화/교외화(방향이
   반대인 짝). 189쌍 규모의 전수 스캔은 phase13과 동일 방법론(같은 길이·글자 1개 차이)으로
   수행했다(3절).
5. **REUSE 판정: 0건.** research 서버 `vocabulary_contents` 5,772건(L4 신규 49건 포함) 전체의
   lemma와 50건(HOLD 2건 포함 전체 검토 대상) lemma를 정확 일치 대조한 결과 겹치는 표제어가
   하나도 없었다.
6. **`level_status`/`generation_status`/`source_version`은 phase13과 같은 방어 패턴을
   재사용하되, `source_version`만 L4와 구분되는 새 고유 마커
   (`schema_reading_literacy_l5_manual_v1`)를 부여했다.** `vocab_level=5`,
   `target_grade_band='고등 1학년'`(위 1번 정책 근거), `level_status='REVIEW_BOUNDARY'`
   (새 열거값 발명 없음), `generation_status=NULL`(화이트리스트 회피 방어선 유지).
7. **적재 전 게이트 7개 전부 PASS, 단일 트랜잭션으로 48건(HOLD 2건 제외) 삽입, 멱등성
   재실행 확인(2회차 삽입 0건, 스킵 48건), 적재 후 검증 전부 PASS.** 기존 5,772행(L4
   49건 포함) 체크섬은 마이그레이션 전후 완전 동일, 신규 48행 전부 `student_exposure=0`/
   `public_ready=0`, `vocabulary_items`/`vocabulary_multiformat_items` 어느 쪽도 새
   콘텐츠를 참조하지 않아(각각 0건) 구조적으로 출제 불가능함을 확인했다.
8. **최종 분류: REUSE 0 / NEW_PRIVATE_LOADABLE 48(V 24 + S 24) / HOLD 2(V, 검정·내면).**

---

## 1. 작업 1 — L5_CANDIDATE 6건 재검토 ("고1" 학년 근거)

### 1-1. literacy.db 재조회 결과

이번 세션이 직접 `data/literacy.db`(mode=ro)를 재조회한 결과:

| 표제어 | term_id | literacy.db `level` | `grade_level` | `grade_source` | `source` | `review_status` |
|---|---:|---:|---|---|---|---|
| 대등 | 4868 | 5 | NULL | manual | schemareading-tooldict | 검수전 |
| 사후 | 4920 | 5 | NULL | manual | schemareading-tooldict | 검수전 |
| 본론 | 4904 | 5 | NULL | manual | schemareading-tooldict | 검수전 |
| 논술 | 4859 | 5 | NULL | manual | schemareading-tooldict | 검수전 |
| 부가 | 4906 | 5 | NULL | manual | schemareading-tooldict | 검수전 |
| 성향 | 4934 | 5 | NULL | manual | schemareading-tooldict | 검수전 |

phase12 시점 값(`literacy_stored_level=5`, `original_level=L5(package)`)과 완전히 일치했다
(term_id/표제어/레벨 불일치 0건).

### 1-2. "고1" 학년 근거 — 숫자만으로 확정하지 않은 근거

`terms.grade_level` 컬럼을 전체 소스별로 집계한 결과, **V(schemareading-tooldict)·
S(schemareading-schema)·krdict·sajaseongeo-pdf 네 소스 전부 `grade_level`이 예외 없이
NULL**이다(momo-textbook 소스 821건만 1~8 값을 가짐 — 이는 초·중등 학년 척도로 다른
정책, L5/L6과 무관). 즉 **이 6건에는 개별 문항 수준의 "학년 태그"가 literacy.db 안에
전혀 없다** — "레벨=5"라는 숫자 자체가 유일한 신호다.

그래서 이번 세션은 숫자를 그대로 받아들이는 대신, 이 프로젝트가 `terms.level=5`를
어떤 학년으로 "정책적으로" 매핑하기로 확정했는지를 문서에서 직접 확인했다:

- `docs/literacy/07-학년경계정책-L5L6.md`(2026-09-24 작성, phase11이 직접 갱신) —
  **"확정 기준: L5 = 고1, L6 = 고2~3"**이라고 명시하며, 이 기준이 코드 2곳
  (`scripts/literacy/auto_review_level.py`의 `LEVEL_TABLE`, `app/vocabulary_quiz/routers/multiformat.py`의
  `GRADE_LABELS`)에도 이미 반영돼 있음을 확인했다(문서 18~19행).
- `reports/schema_reading_phase1_baseline_20260923.md` 8절 — L5/L6 학년 대응이 애초에
  3-way 불일치(구 `terms.level` 문서, 구 `GRADE_LABELS` 코드, 사용자가 그 시점에 명시한
  현재 서비스 기준)였다가, phase11이 "L5=고1/L6=고2~3"으로 통일 확정한 경위를 확인했다.

**결론: 이 6건의 "고1" 근거는 개별 학년 태그(momo-textbook처럼)가 아니라, 이 프로젝트가
2026-09-24에 명시적으로 확정·문서화한 "학습 도구어 사전 6단계 패키지 레벨 ↔ 학년" 매핑
정책(L5=고1)이다.** 이는 "레벨=5 숫자니까 고1"이라는 순환 논리가 아니라, 사용자 승인을
거친 별도 정책 문서를 인용한 것이지만, momo-textbook처럼 학생 개인의 실제 학년 데이터로
검증된 것은 아니라는 한계가 있다 — 이 차이를 리스크로 남긴다(7절 1번).

### 1-3. 6건 개별 검토 결과

6건 전부 literacy.db 전체(전 source·전 level)에서 정확히 1개 행만 존재해(동음이의 분리
행 없음), 뜻이 갈라진 사례가 아니다. 정의 대조 결과:

| 표제어 | literacy.db 정의 | 학생용 정의 일치 | 품사 | 판정 |
|---|---|---|---|---|
| 대등 | 서로 견주어 높고 낮음이나 낫고 못함이 없이 비슷함. | 일치 | 명사 | PASS |
| 사후 | 일이 끝난 뒤. 또는 일을 끝낸 뒤. | 일치 | 명사 | PASS(주의: '향후'와 반의, 배치 밖) |
| 본론 | 말이나 글에서 주장이 있는 부분. | 일치 | 명사 | PASS |
| 논술 | 어떤 것에 관하여 의견을 논리적으로 서술함. | 일치 | 명사 | PASS |
| 부가 | 주된 것에 덧붙임. | 일치 | 명사 | PASS |
| 성향 | 성질에 따른 경향. | 일치 | 명사 | PASS(주의: 기존 L4 '지향'과 근접) |

**6건 전부 최종 배치에 포함했다(HOLD 없음).**

---

## 2. 작업 1 — 신규 42건 선정 (V/S level=5 원천)

### 2-1. 모집단과 제외 기준

- **V(schemareading-tooldict) level=5: 108건.** 그중 34건은 phase12의 `E_34_L5_spiral_check`가
  이미 검토했으므로(L5_CANDIDATE 6 + L5_SPIRAL_REVIEW 28, 위 1절/제외 대상), 나머지
  74건이 이번 세션의 신규 검토 대상이다. 74건 중:
  - 번호 붙은 동형이의 분리 표기 12건 제외(phase11/12와 동일 기준 재적용): 결정01,
    기술01, 기술03, 기원04, 기원05, 답03, 대체02, 대체03, 등05, 사상15, 사적02, 상기19.
  - POS 누락 1건 제외: 바람직(데이터 품질 문제, `pos=NULL` — V 소스에서는 이례적).
  - 남은 61건 중 이번 세션이 의미·동형이의 위험을 직접 검토해 **20건을 선정**했고,
    검토 과정에서 2건(검정, 내면)을 HOLD 판정했다(3절) → **최종 V 신규 18건**.
- **S(schemareading-schema) level=5: 507건**(과학 263 / 사회 216 / 인문 철학 28). 규모가
  매우 커서 이번 세션은 **전수 검토 대신 대표 클러스터를 직접 읽고 검토**했다
  (과학: 판구조론·지구 구성 물질 1~2주차, 사회: 인간과 사회·자연환경과 인간·생활공간과
  인간 1~3주차). 이는 507건 전체를 검토한 것이 아니라는 뜻이며, **507건 중 검토하지
  않은 나머지는 이번 세션의 판단 범위 밖으로 명시적으로 남겨 둔다**(7절 리스크 3번).
  - **인문 철학 28건은 통째로 제외했다.** 28건 전부 `pos=NULL`이고("형이상학", "윤리학"
    등), 다수가 "소피스트의 상대주의 윤리관", "플라톤의 이상주의 윤리",
    "레오폴드의 대지윤리"처럼 인명·절 형태가 결합된 긴 개념 이름이라 일반적인 "표제어"
    형태가 아니다. 내용도 "생활과 윤리"/"윤리와 사상" 선택과목 수준의 전문 용어
    (공리주의, 의무론, 덕윤리, 스콜라철학 윤리 등)로, 고1 공통 교육과정보다 상급·선택
    과정에 가깝다고 판단해 근거 부족으로 제외했다(단순히 "재미없어서"가 아니라 POS
    누락 + 표제어 형태 + 교육과정 수준 세 가지 구체적 근거).
  - 과학/사회에서도 named-theory·정책 슬로건형 표제어("해양저확장설", "맨틀 대류설",
    "호상열도", "모스굳기계", "착한 사마리아인 법", "환경 영향 평가 제도", "저탄소
    녹색성장", "환경 파시즘", "레오폴드의 대지윤리", "아리스토텔레스의 바람직한 정치",
    "동양/서양의 행복론·이상사회")은 제외하고, 일반적인 개념 명사 위주로 선정했다.
  - 최종 **과학 12건 + 사회 12건 = S 신규 24건.**

### 2-2. 최종 신규 42건 목록

**V 신규 18건**: 가변, 가치관, 간략, 감안, 개관, 개괄, 거시, 계승, 계통, 광범위, 구축,
구현, 국면, 급진, 난점, 내재, 논거, 논리적 (검정·내면은 HOLD, 3절)

**S 신규 24건(과학 12 + 사회 12)**:

| 교과 | 소분류/주차 | 표제어 |
|---|---|---|
| 과학(지구과학) | 판구조론 / 1주차 | 연약권, 맨틀, 발산형 경계, 해령, 열곡, 수렴형 경계, 해구, 열점 |
| 과학(지구과학) | 지구 구성 물질 / 2주차 | 광물, 결정형, 쪼개짐, 깨짐 |
| 사회 | 인간과 사회 / 1주차 | 사회구조, 사회제도, 정주환경 |
| 사회 | 자연환경과 인간 / 2주차 | 인간중심주의 자연관, 생태중심주의 자연관, 생태도시, 슬로시티, 녹색소비 |
| 사회 | 생활공간과 인간 / 3주차 | 도시화, 교외화, 도시문제, 인구공동화 |

행별 근거(원천 term_id, literacy.db 정의, 학생용 정의·예문, 캐주션)는
`data/import/schema_reading_phase14_l5_core_final_20260925.csv`/`.jsonl`에 전부 기록했다.

---

## 3. 작업 2 — 학생용 뜻풀이·예문 + 자동 검사

### 3-1. 뜻 일치 검사

50건(HOLD 2건 포함 전체 검토 대상) 전부에 대해 literacy.db `definition`과 학생용 정의를
나란히 놓고 핵심 의미소가 보존되는지 직접 대조했다(예: "도시화" DB="인구가 도시 지역으로
집중되는 과정으로, 도시적 생활 양식으로의 변화를 말한다." / 학생용="인구가 도시로 몰리면서
도시적인 생활 방식으로 바뀌어 가는 과정" — 완전 일치). **48건(HOLD 2건 제외) 전부 PASS.**

### 3-2. 예문의 목표어 실제 용법

각 예문에서 목표어(또는 활용형)가 정의된 뜻으로 자연스럽게 쓰였는지 직접 읽고 확인했다
(예: "구현" — "이 프로그램은 사용자의 아이디어를 실제 화면으로 구현해 준다" → "구체적인
사실로 나타나게 함"의 뜻으로 정확히 쓰임). `example_target_form` 컬럼에 예문 안 실제
표면형을 기록했다. **48건 전부 PASS.**

### 3-3. 동형이의어 검사 — 신규 HOLD 2건

- **"검정"(4841)**: literacy.db 저장 정의는 "일정한 규정에 따라 자격이나 조건을 검사하여
  결정함"(교과서 검정, 자격 검정) 1개 행뿐이다. 그러나 "검정"은 색깔을 뜻하는 기초
  어휘(검정=black)가 일상에서 압도적으로 더 흔하게 쓰인다. phase13의 "정상" 사례는
  "literacy.db에 저장된 뜻 1개만 있으면 배치 유지"라는 기준을 세웠지만, 이번에는
  경쟁하는 동형이의어가 완전히 다른 품사·개념 영역(색채어 vs 추상 행위 명사)이고 빈도
  격차가 훨씬 커서, 탈맥락 정의 매칭 퀴즈에서 학생이 "검정"을 보고 거의 확실히
  "검은색"을 먼저 떠올릴 위험이 있다고 판단해 **개별 HOLD**했다.
- **"내면"(4855)**: literacy.db 저장 정의는 "물건의 안쪽"(표준국어대사전 1번 의미)
  1개 행뿐이다. 그러나 독서·문학 지문에서는 "인물의 내면 묘사", "내면의 갈등"처럼
  "사람의 정신이나 마음의 작용"을 가리키는 2번 의미가 압도적으로 더 자주 쓰인다.
  literacy.db가 이 2번 의미를 담은 행을 별도로 갖고 있지 않아(전체 재쿼리 결과 "내면"은
  1행뿐), 저장된 정의를 그대로 쓰면 학생이 실제로 접할 뜻과 다른 정의를 배우게 된다 —
  phase13의 "가변성" HOLD(원문이 표제어를 직접 정의하지 않음)와 같은 계열의 "저장된
  정의가 실제 쓰임과 다름" 문제로 판단해 **개별 HOLD**했다.
- **처리**: 이 2건만 HOLD 처리했고, 작업 전체를 멈추지 않았다. 나머지 48건은 계속
  진행해 적재까지 완료했다.

### 3-4. 반의어/근접어 짝 스캔 (189쌍 규모, phase13 "지향/지양" 방법론 재사용)

50건(HOLD 2건 포함) lemma 쌍(같은 길이, 글자 1개 차이) 전수 대조 + 50건 대
`vocabulary_contents` 5,772건(L4 신규 49건 포함) lemma 전수 대조를 수행했다(원시 후보
50건 내부 9쌍 + 50건 대 기존 171쌍 = 180쌍, 이번 세션도 표제어만 겹치는 우연한 음절
일치가 대부분이었다). 수작업으로 재검토한 결과, 실제로 "형태가 비슷하고 뜻이 반대 또는
밀접하게 대비되는" 진짜 위험 쌍은 다음과 같다(전부 배치에 포함하되 캐주션을 남김):

| 짝 | 관계 | 처리 |
|---|---|---|
| 가변 / 불변 | 반의어(어근 '변' 공유) | 가변만 포함, 불변은 배치 밖 |
| 거시 / 미시 | 대비 짝(교육상 함께 배우는 개념) | 거시만 포함, 미시는 배치 밖 |
| 사후 / 향후 | 반의 시간 개념(어근 '후' 공유) | 사후만 포함, 향후는 기존 db에도 없음(배치 밖) |
| 성향 / 지향(기존 L4) | 근접 개념('경향/방향') | 둘 다 존재, 유추/유사급 낮은 위험으로 캐주션 |
| 개관 / 개괄 | 동의어에 가까운 근접(둘 다 배치 안) | 둘 다 포함, 캐주션 |
| 구축 / 구현 | 뜻이 비슷해 실제로 자주 혼용(둘 다 배치 안) | 둘 다 포함, 캐주션 |
| 해령 / 해구 | 형태가 반대인 지형(둘 다 배치 안) | 둘 다 포함, 캐주션 |
| 발산형 경계 / 수렴형 경계 | 짝 개념(둘 다 배치 안, 교과서에서 함께 배움) | 둘 다 포함, 캐주션 |
| 쪼개짐 / 깨짐 | 짝 개념(둘 다 배치 안, 교과서에서 함께 배움) | 둘 다 포함, 캐주션 |
| 인간중심주의 자연관 / 생태중심주의 자연관 | 반의 짝(표제어 끝부분 동일, 둘 다 배치 안) | 둘 다 포함, 캐주션 |
| 도시화 / 교외화 | 방향이 반대인 짝(둘 다 배치 안) | 둘 다 포함, 캐주션 |

나머지 원시 후보 대부분(가치관/가스관, 계승/스승, 광물/곡물, 부가/첨가 등)은 우연한
음절 일치일 뿐 뜻·품사가 무관했다(phase13과 동일 패턴).

---

## 4. 작업 3 — S(교과개념어) 24건 적합성 대조

`terms.subject_category`/`note`(소분류/주차)를 이번 세션이 직접 재조회해 아래 표와
행 단위로 일치함을 확인했다(전부 이번 세션 신규 작성분이므로 phase12 CSV와 대조할
과거 값은 없다 — literacy.db 원문과 직접 대조).

| 교과 | 소분류 | 주차 | 건수 | 정의 일치 | 판정 |
|---|---|---:|---:|---|---|
| 과학(지구과학) | 판구조론 | 1 | 8 | 일치 | PASS |
| 과학(지구과학) | 지구 구성 물질 | 2 | 4 | 일치 | PASS |
| 사회 | 인간과 사회 | 1 | 3 | 일치 | PASS |
| 사회 | 자연환경과 인간 | 2 | 5 | 일치 | PASS |
| 사회 | 생활공간과 인간 | 3 | 4 | 일치 | PASS |

과학 12 : 사회 12로 균형을 맞췄다(L4의 과학7:사회7 균형과 유사한 취지). **24건 전부
PASS, 교과 부적합 HOLD 없음.** 단, 교과 전문가(지구과학/사회) 검수는 아직 거치지
않았다(phase12/13이 이미 지적한 리스크의 연장 — 7절 2번).

---

## 5. 작업 3 — REUSE 판정 + dry-run 표

### 5-1. REUSE 판별

서버 `vocabulary_quiz_research.db`(mode=ro, `PRAGMA query_only=ON`)에서 `vocabulary_contents`
**5,772건**(L4 신규 49건 포함 현재 상태) 전체의 `lemma`를 `scripts/vocab/phase14_apply_l5_core.py`와
같은 세션에서 `phase13_reuse_check.py`를 경로만 바꿔 그대로 재사용해 대조했다. **정확
일치 0건.** 부분 문자열 포함 관계도 스캔했으나(개관↔개관식, 검정↔검정깨/검정콩) 전부
다른 단어의 부분 문자열일 뿐 같은 표제어가 아니었다.

**결론: 50건 전부 REUSE=0.**

### 5-2. 최종 분류 (dry-run 표)

| 분류 | 건수 | V | S |
|---|---:|---:|---:|
| REUSE | 0 | 0 | 0 |
| NEW_PRIVATE_LOADABLE | 48 | 24 | 24 |
| HOLD | 2 | 2 | 0 |
| **합계** | **50** | **26** | **24** |

행별 근거는 `data/import/schema_reading_phase14_l5_core_final_20260925.csv`의
`final_classification`/`caution`/`hold_reason` 컬럼 참고.

---

## 6. 작업 4 — 게이트 기반 단일 트랜잭션 적재

### 6-1. 스키마/정책 재확인 (라이브)

서버에서 이번 세션이 직접 재확인:

- `vocabulary_contents`/`vocabulary_content_levels` 마이그레이션 전 행수: 양쪽 다
  **5,772건**(phase13 적재 직후 상태 그대로, L4 49건 포함 — 그 사이 변경 없음).
- `level_status` 기존 사용 값(`PROVISIONAL_AUTO`/`REVIEW_BOUNDARY`) 중 **`REVIEW_BOUNDARY`**를
  재사용(phase13과 동일 근거: 자동 채점 미실시, 사람이 처음 작성·대조).
- `generation_method`=`MANUAL_STRUCTURED_AUTHORING`, `qa_method`=`DETERMINISTIC_PLUS_HEURISTIC`
  (기존 값 재사용), `generation_status`=NULL(화이트리스트 회피 방어선 유지).
- `source_version`을 L4(`schema_reading_literacy_l4_manual_v1`)와도 다른 새 고유 마커
  **`schema_reading_literacy_l5_manual_v1`**로 부여했다 — quiz.py/multiformat.py가
  요구하는 `source_version=2.1.29`와도, L4 배치와도 절대 겹치지 않게 하는 방어선.
- `vocab_level=5`, `target_grade_band='고등 1학년'`(1-2절 정책 근거), `level_source`는
  L4의 `MANUAL_LITERACY_L4_MATCH`와 구분되는 `MANUAL_LITERACY_L5_MATCH`.

### 6-2. 게이트별 pass/fail

| 게이트 | 내용 | 결과 |
|---|---|---|
| 1 | `APP_ENV=research` 재확인(`.env` + 실행 중 프로세스 `/proc/<PID>/environ`, PID 1190871) | **PASS** |
| 2 | SQLite Backup API 백업 생성(`vq_research_backup.py` 그대로 재사용, 라벨 `phase14-l5-core-pre-migration`) | **PASS** |
| 3 | 백업 무결성(`integrity_check`=ok, FK 위반 0)·행수 일치(원본=백업, 5,772=5,772) | **PASS** |
| 4 | 마이그레이션 전 체크섬 스냅샷 | **PASS** — `vocabulary_contents` 해시 `24f1a9be...fbd4b`, `vocabulary_content_levels` 해시 `3d7835bd...496f4a8`(5,772건) |
| 5 | `NEW_PRIVATE_LOADABLE` 48건만 단일 트랜잭션으로 `vocabulary_contents`+`vocabulary_content_levels` 동시 삽입 | **PASS** — 48건 모두 커밋, 예외 0건 |
| 6-a | 적재 후 기존 5,772행 체크섬 불변(신규 48건 제외하고 재계산) | **PASS** — `24f1a9be...fbd4b`/`3d7835bd...496f4a8` 그대로 (L4 49건 포함 전부 byte-for-byte 동일) |
| 6-b | 신규 48건 `student_exposure`/`public_ready` 전부 0 | **PASS**(48/48) |
| 6-c | `PRAGMA integrity_check` / `foreign_key_check` (적재 후) | **PASS**(ok / 위반 0) |
| 6-d | `vocabulary_items`/`vocabulary_multiformat_items` 신규 콘텐츠 참조 여부 | **PASS** — 둘 다 0건 참조(vocabulary_items 5,723건 불변, vocabulary_multiformat_items 1,289건 불변) |
| 7 | 멱등성 — 동일 스크립트 재실행 | **PASS** — 2회차 신규 삽입 0건, 스킵(이미 존재) 48건 |

**하나도 fail하지 않아 롤백을 실행하지 않았다.**

### 6-3. 적재 스크립트/증적

- `scripts/vocab/phase14_apply_l5_core.py`(신규, `phase13_apply_l4_core.py`를 L5용으로
  최소 수정: `BATCH_ID`/`MERGE_SOURCE`/`SOURCE_VERSION`/`TARGET_GRADE_BAND`/`vocab_level`/
  `content_id` 접두사(`SR_L5CORE_`)/`level_source`만 변경, 게이트 로직은 동일)
- `scripts/vocab/vq_research_backup.py`, `scripts/vocab/vq_checksum_core_tables.py`(phase4/13과
  동일 스크립트 재사용, 신규 파일 아님)
- 서버 산출물: `~/scratch/phase14_l5_core/`(lemmas.json, reuse_candidates.json,
  phase14_rows.json, checksum_before.json, checksum_after_full.json)

---

## 7. 리스크 (우선순위 순)

1. **"고1" 근거가 개별 학년 태그가 아니라 정책 문서(L5=고1) 기반이다** — literacy.db의
   `grade_level` 컬럼이 V/S 소스 전체에서 NULL이라(1-2절), 이번 48건의 "고1" 근거는
   momo-textbook처럼 학생 개인의 실제 학년 데이터로 검증된 것이 아니라, 프로젝트가
   2026-09-24에 정책적으로 확정한 "학습 도구어 사전/스키마 어휘 목록의 6단계 패키지
   레벨 ↔ 학년" 매핑(문서 근거)이다. 사용자 승인을 거친 공식 정책이지만, 개별 태그보다
   증거 강도가 약하다는 점을 다음 세션이 알아야 한다.
2. **S(교과개념어) 24건은 교과 전문가(지구과학/사회) 검수를 아직 거치지 않았다** —
   phase12/13이 이미 지적한 리스크가 이번에도 해소되지 않았다. `level_status=REVIEW_BOUNDARY`/
   `boundary_flag=1`로 구조적으로 표시해 뒀다.
3. **S(schema) level=5 507건 중 이번 세션이 실제로 검토한 것은 일부(과학 12+사회 12 선정,
   인문 철학 28건 전체 검토 후 제외)뿐이다** — 507건 전체를 행 단위로 훑지는 않았다.
   나머지(예: 과학 3주차 이후, 사회 4주차 이후)에서 추가로 채택 가능한 L5 후보가 더
   있을 수 있으나, 이번 세션은 "근거 확인된 만큼만"이라는 지시에 따라 검토한 범위
   안에서만 확정했다. 다음 배치에서 이어서 검토할 수 있다.
4. **"검정"·"내면" 2건은 HOLD 상태로 미적재** — 향후 재검토하려면 "검정"은 "교과서
   검정"류 맥락을 예문에 강하게 못박거나 다른 표제어(예: "검정색"과 명확히 분리)로
   재작성해야 하고, "내면"은 literacy.db에 없는 2번 의미(사람의 정신/마음)를 다른
   출처에서 보강해야 한다.
5. **캐주션 표시된 근접/대비 쌍(가변/불변, 거시/미시, 성향/지향, 구축/구현, 해령/해구,
   쪼개짐/깨짐, 발산형·수렴형 경계, 인간중심주의·생태중심주의 자연관, 도시화/교외화)은
   지향/지양급은 아니지만, 실제 문항을 만들 때는 두 개념을 함께 보여주거나 구분 문구를
   넣는 것을 권장한다** — 일부는 교과서에서 의도적으로 대비해서 가르치는 짝 개념이라
   완전한 배제 대상이 아니다.
6. **`source_version`/`generation_status`를 의도적으로 표준값과 다르게 설정**했다(6-1절
   방어선) — 향후 이 48건을 정식 파이프라인에 편입시키려면 `source_version`을 `2.1.29`로,
   `generation_status`를 `PRIVATE_SERVER_READY_CANDIDATE`로 **의도적으로 변경하는 별도
   마이그레이션이 필요하다**(자동으로 편입되지 않음, phase13과 동일 방어선).

---

## 8. 적재 후 최종 검증

| 항목 | 결과 |
|---|---|
| 기존 콘텐츠(5,772건, L4 49건 포함) 체크섬 불변 | **PASS** — 신규 `SR_L5CORE_%`를 제외한 5,772건만 재계산해도 마이그레이션 전과 완전 동일 |
| `vocabulary_content_levels` 기존 5,772건 체크섬 불변 | **PASS** |
| 신규 48건 `student_exposure`=0 전부 | **PASS**(48/48) |
| 신규 48건 `public_ready`=0 전부 | **PASS**(48/48) |
| `vocabulary_items`(구식 퀴즈 문항 테이블) 행수 | 5,723건, 불변 — 신규 content_id를 참조하는 행 0건 |
| `vocabulary_multiformat_items`(신식 퀴즈 문항 테이블) 행수 | 1,289건, 불변 — 신규 content_id를 참조하는 행 0건 |
| `vocabulary_content_literacy_links` 행수 | 142건, 불변(이번 세션은 이 테이블을 전혀 건드리지 않음) |
| `SR_L4CORE_%` 49건 존재 여부 | 49건 그대로 존재(불변) |
| `PRAGMA integrity_check` | ok |
| `PRAGMA foreign_key_check` | 위반 0건 |
| 코드 변경 | 없음(앱 코드 파일 수정 0건, 신규 스크립트 1개만 작성) |
| git 작업 | 없음(commit/push 없음) |
| literacy.db 변경 여부 | 없음(mode=ro 유지) |
| "가변성" 1건(L4 HOLD)·L4 S 13건 | 이번 세션에서 조회·수정 모두 하지 않음 |
| `L5_SPIRAL_REVIEW` 28건 | 이번 세션에서 조회만 하고 쓰기 없음, 배치에 포함하지 않음 |

**학생 공개·운영 배포를 하지 않았음을 구조적으로 확인**: 신규 48건을 참조하는
`vocabulary_items`/`vocabulary_multiformat_items` 행이 0건이라, 어느 라우터의 "출제
가능" 쿼리도 이 콘텐츠를 절대 반환할 수 없다.

---

## 9. 산출물 목록

### 로컬(`C:\Users\aproa\aprolabs`, 커밋하지 않음)

- 본 보고서: `reports/schema_reading_phase14_l5_core_apply_20260925.md`
- 결과표: `data/import/schema_reading_phase14_l5_core_final_20260925.csv`,
  `data/import/schema_reading_phase14_l5_core_final_20260925.jsonl`
- 스크립트: `scripts/vocab/phase14_apply_l5_core.py`(신규, `phase13_apply_l4_core.py` 최소
  수정본)
  (`scripts/vocab/vq_research_backup.py`, `scripts/vocab/vq_checksum_core_tables.py`,
  `scripts/vocab/phase13_reuse_check.py`는 기존 스크립트를 경로만 바꿔 그대로 재사용,
  신규 파일 아님)

### 서버(`aprolabs`, research)

- 백업: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase14-l5-core-pre-migration-20260924-233807`
- 백업 SHA-256: `ce1a1ada129a24d2fb01d7897836229440f153660428640d1e49ec02ad3c4b9b`
- 백업 크기: 12,247,040 bytes
- 백업 검증: 행수 일치(5,772=5,772), `integrity_check`=ok, `foreign_key_check` 위반 0
- 작업 스크립트/체크섬/중간 산출물: `~/scratch/phase14_l5_core/`
- 실제 변경: `vocabulary_quiz_research.db`의 `vocabulary_contents`+
  `vocabulary_content_levels`에 각각 48행 신규 삽입(content_id `SR_L5CORE_<literacy_term_id>`)
  — 그 외 기존 테이블/행 변경 없음(8절 체크섬으로 증명)

### 롤백 절차 (참고용, 실행 안 함 — 게이트 전부 PASS)

```bash
ssh aprolabs
cp /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.rollback-before-restore-$(date +%Y%m%d-%H%M%S)
cp /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase14-l5-core-pre-migration-20260924-233807 \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
sqlite3 /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db "PRAGMA integrity_check;"
```

기존 `scripts/vocab/rollback_literacy_link_apply.py`를 `--backup-path`만 이번 백업 경로로
바꿔 사용해도 복구 가능(새 롤백 스크립트를 만들지 않았다).
