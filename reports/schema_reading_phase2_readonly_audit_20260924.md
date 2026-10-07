# 스키마리딩·어휘 퀴즈 DB 통합 — 2단계 읽기 전용 조사 보고서

- 작성일: 2026-09-24 (지시문의 "오늘 날짜 2026-09-23"과 시스템 현재 날짜가 달라, 시스템 현재 날짜를 파일명·본문에 사용했다 — 내용에는 영향 없음)
- 범위: 1단계(`reports/schema_reading_phase1_baseline_20260923.md`)에 이어지는 읽기 전용 조사. **이번 세션에서 로컬·서버 어느 DB에도 INSERT/UPDATE/DELETE/ALTER/CREATE를 실행하지 않았다.** 서버는 전부 `sqlite3 -readonly` 또는 `PRAGMA query_only=ON`으로만 접근했다.
- 확인된 git HEAD: 로컬 `d3d5b77` / 서버 `9a42961` (로컬이 momo_b2b_tablet 관련 커밋 1개 앞섬 — 이번 조사 대상 파일과는 무관, 참고용으로만 기록)

---

## 0. 선행 확인 — "v3 문서" 소재 불명 (막힌 지점, 조사는 계속 진행)

지시문은 `schema_reading_integration_revised_after_server_audit_v3.md`(v3)가 `data/import/internal_vocab_upper_restore_v1.zip` 안에 있다고 전제했다. 실제로 확인한 결과:

- `internal_vocab_upper_restore_v1.zip`(38개 파일)에는 그 파일명이 **존재하지 않는다.** 저장소 전체(`find . -iname "*v3*"`, `*integration_revised*`, `*server_audit*`)에서도 찾지 못했다.
- 대신 이 zip에는 `SCHEMA_READING_REFERENCE_REVIEW.md`(스키마리딩 참고자료 검토 — DB 통합 설계 수정사항)와 `CLAUDE_CODE_HANDOFF.md`가 들어 있으며, 내용상 "v2 폐기 후 서버 감사 결과를 반영한 수정판"이라는 지시문의 설명과 가장 가깝다. 이번 조사는 이 두 문서와 1단계 보고서(`schema_reading_phase1_baseline_20260923.md` 10~12절, 실질적으로 v2를 대체하는 수정안)를 v3의 실질적 대체 기준으로 삼아 진행했다.
- **결론: v3라는 이름의 파일은 존재하지 않는다.** 이름 불일치가 단순 오타인지, 아직 전달되지 않은 별도 파일인지는 이번 조사로 판단할 수 없어 사용자 확인이 필요한 막힌 지점으로 남긴다. 나머지 조사(1~5번)는 지시대로 중단하지 않고 전부 수행했다.

## 0-1. 원천 파일 확인 (SHA-256 · 내부 XLSX)

두 파일 모두 존재한다.

| 파일 | SHA-256 |
|---|---|
| `data/import/internal_vocab_upper_restore_v1.zip` | `ed83a8f9cc8d577780f243c9e95b38a7b01b90da00b53f988d137dc54ccda389` |
| `data/import/스키마리딩 참고자료.zip` | `616abed02292fa244988d69c229e89ab652a559ecd0a7972d7518cc1e28dde26` |

`internal_vocab_upper_restore_v1.zip`(38개 파일) 핵심 구성: `README.md`, `SCHEMA_READING_REFERENCE_REVIEW.md`, `CLAUDE_CODE_HANDOFF.md`, `CONTINUOUS_WORK_REPORT.md`, `PILOT_50_REPORT.md`, 파이썬 스크립트 6개(`build_restore_audit.py` 등), `tests/`, `data/`(감사 JSON/JSONL 다수), 그리고 `source_xlsx/초등 어휘 db/`에 원본 XLSX 2개.

`스키마리딩 참고자료.zip`(11개 파일): `학습 도구어 사전 레벨1~6 (0925최종버전).xlsx`, `스키마 맵 작업용 스키마 정리(레벨 3,4).xlsx`, `스키마 카테고리 분류.xlsx`, `어휘퀴즈DB (1).xlsx`, `에이프로 교재 중 초등 어휘 모음_(최종).xlsx`, `스키마 어휘 목록(레벨2~5 완성).xlsx`, `스키마 리딩 주차별 주제 및 스키마 정리(1115 버전).xlsx`, PDF 2개, PPTX 2개.

**두 zip에 공통으로 들어 있는 핵심 XLSX 2개는 바이트 단위로 완전히 동일**하다 (SHA-256 대조):

| 파일 | SHA-256 | 비고 |
|---|---|---|
| 학습 도구어 사전 레벨1~6 (0925최종버전).xlsx | `17b40c6e0f3dd0e4f153e09c208ff29d17c34c00dfa4b56da6e0d4e19ac77a48` | ivur zip·참고자료 zip·현재 `raw/schema-reading/`(gitignored) 세 곳 모두 동일 |
| 스키마 어휘 목록(레벨2~5 완성).xlsx | `b071bb647bc396a5183c064fce1bc2004be191918a387e5484fc234b0f1ce85b` | 위와 동일하게 세 곳 모두 동일 |

`raw/schema-reading/`가 이 두 파일과 SHA-256까지 완전히 같다는 사실이 이번 조사에서 가장 중요한 전제다 — 즉 **literacy.db에 이미 적재된 데이터는 지금 두 zip에 들어 있는 바로 그 원본 XLSX에서 나온 것**이며, 별도의/유실된 원본이 아니다.

반례도 하나 발견했다: `어휘퀴즈DB` 파일은 이름이 비슷해도 **다른 파일**이다.
- `raw/schema-reading/어휘퀴즈DB.xlsx` SHA-256 `63ed4d0f30147a73a4a659fcaf9cebb54a1c3e0e136fc8769cbf109a49092581`
- `스키마리딩 참고자료.zip`의 `어휘퀴즈DB (1).xlsx` SHA-256 `78903d5b1e7b86fb28db5304137762619bbbab3b3b643a5bfa710f11bd57d988`

둘 다 시트 1개(`어휘퀴즈DB`), 339행(헤더+338행), 컬럼 `제목/단어/뜻(정답)/보기1~3/...`으로 구조는 동일해 보이지만 **파일 자체는 다르다** (버전 차이 가능성). 이 파일은 이번 단계에서 다루지 않는 문항 자료(`quiz_items` 대상, 미적재 상태 유지)라 상세 diff는 하지 않았지만, 다음 단계에서 이 파일을 다룰 때는 "raw/의 버전"과 "이번 zip의 버전" 중 무엇을 쓸지 먼저 확인해야 한다.

---

## 1. `app/literacy` 실제 저장 구조 (모델·마이그레이션·literacy.db 실제 스키마)

- 모델: `app/literacy/models.py`의 `Term`(표제어 통합 테이블, `terms`). 관련 필드: `category`, `headword`, `origin`, `definition`, `pos`, `sense_category`, `subject_category`, `grade_level`(1~12), `level`(0~6), `grade_source`(auto/manual), `source`, `license`, `external_id`, `review_status`, `note`. `UNIQUE(source, external_id)` 제약.
- DB 경로: `app/literacy/db.py` → `data/literacy.db` (로컬·서버 모두 존재, SHA-256 `369ea39427870d49959540739852d604ff51edd8bd910434698e73845cc41afb`, 최종 수정 9/2).
- 마이그레이션: `app/literacy/migrations/002_add_level.py`(레벨 0~6 컬럼 도입). 작업 지시서 `docs/literacy/04-스키마리딩어휘적재.md`, 결과 보고서 `docs/literacy/04-스키마리딩어휘.md`.
- 실제 적재 스크립트: `scripts/literacy/import_schemareading_vocab.py` + 파서 `scripts/literacy/schemareading_parser.py`. **`--dry-run`으로 재실행해 저장 없이 출력만 재현**했고(아래 2절), literacy.db 현재 상태와 정확히 일치함을 확인했다.

`source`별 실제 저장 형태(직접 SQL로 확인):

| source | 건수 | headword | definition NULL | pos NULL | sense_category NULL | subject_category NULL | grade_level | level | external_id 형태 |
|---|---:|---|---:|---:|---:|---:|---|---|---|
| `schemareading-tooldict`(학습도구어) | 1,321 | 원본 그대로(괄호 정리 등 일부 정제) | 0 | 20 | 1,321(전부 NULL) | 1,321(전부 NULL) | 전부 NULL | 0~6 정수 | `L{레벨}-{원본엑셀행번호}` (예: `L1-10`) |
| `schemareading-schema`(교과스키마) | 2,061 | 원본 그대로 | 0(전부 채움 — 662절 참고) | 2,061(전부 NULL, 이 자료엔 품사 개념 없음) | 0(퀴즈 오답 생성용, 항상 채움) | 0 | 전부 NULL | 0~6 정수 | `{sheet}-{원본행번호}` (`social-1`/`sci-1`/`phil-1`) |

- `week`(주차) 정보는 **정규화된 컬럼이 아니라 `note`(자유 텍스트, 예: `"소분류: ... / 주차: 1주차"`)에만 존재** — 1단계 보고서 6절과 동일하게 재확인.
- `schema_topics`/`schema_topic_terms` 같은 정규화 주제 테이블은 **존재하지 않는다.**
- **주의: `definition` NULL이 tooldict/schema 모두 "0건"으로 나오는 이유는 원본 수집 당시가 아니라, 2026-09-01 밤에 실행된 별도 AI 뜻풀이 보강 작업 때문이다 — 자세한 내용은 2절 참고. 이 사실은 `docs/literacy/04-스키마리딩어휘.md`(정의 채움률 66.2%/99.1%로 기술)에는 반영돼 있지 않다.**

---

## 2. 두 ZIP의 원본 XLSX ↔ literacy.db 행 단위 대조

### 2-1. 재현 방법 — 실제 적재 스크립트를 그대로 재실행

`scripts/literacy/import_schemareading_vocab.py --dry-run`을 실행해(쓰기 없음, 트랜잭션 아예 없음) literacy.db를 만든 바로 그 코드로 같은 원본 XLSX를 다시 파싱했다. 결과:

```
tooldict 총 대표 표제어: 1321건 (L1 366 / L2 597 / L3 80 / L4 85 / L5 108 / L6 85)
schema 총 대표 표제어: 2061건 (L1 261 / L2 235 / L3 227 / L4 223 / L5 507 / L6 608)
같은 레벨 내 동형이의어 병합: 16건 (문서와 정확히 일치)
나선형 반복(같은 source 내 여러 레벨 재등장): tooldict 613건 / schema 158건
```

레벨별 건수가 literacy.db의 실제 분포와 **정확히 일치**한다. 이어서 각 대표 행의 `external_id`로 DB를 조회해 (a)~(d)를 행 단위로 판정했다(스크립트 코드를 그대로 가져와 재사용 — 재구현하지 않음).

### 2-2. 집계 결과

| 구분 | tooldict (원천 2,029건 유효행 → 대표 1,321건) | schema (원천 2,252건 유효행 → 대표 2,061건) |
|---|---:|---:|
| (a) 정상 포함(대표 행 headword·definition·level 일치) | 1,309 | 1,364 |
| (d) 원본과 definition이 다름(전수 = 2026-09-01 AI 보강, 아래 2-3 참고) | 12 | 697 |
| (b) 중복 — 같은 레벨 내 병합(42건: 단순중복 25+동일 1+동형이의어 16) | 42행 → 대표 1,321건에 흡수 | 해당 없음(schema는 같은 레벨 내 중복 없음) |
| (b) 중복 — 나선형 반복(다른 레벨 재등장, note에 기록 후 대표만 유지) | 613개 표제어군, folded 664행 | 158개 표제어군, folded 178행 |
| (c) 의도적 제외 — 원본 데이터 오류로 폐기 | 3개 definition (「삶」L4 1건,「정하다」L1 2건, `_KNOWN_BAD_DEFINITIONS` 하드코딩) | 0건 |
| (c) 의도적 제외 — 별도 자료라 이번 단계에서 미적재 | `어휘퀴즈DB.xlsx`(338문항, `quiz_items` 대상) | 인문철학 617행(헤드워드 공백, 작업 미완 상태로 확인됨 — 28건만 유효) |

**단순 건수 차이로 "누락"이라 판정하지 않고, 매 건 external_id 기준으로 직접 대조**했다: `iter_tooldict_entries()`/`iter_schema_entries()`가 만든 대표 행 1,321+2,061=3,382건 전부가 literacy.db에 해당 `external_id`로 존재했다(`missing_in_db=0`, 양쪽 모두). 즉 대표 행 선정 로직이 만드는 집합과 DB가 100% 일치하며, 실제 "빠진" 행은 없다 — 원본 대비 줄어든 건수(2,029→1,321, 2,252→2,061)는 전부 위 표의 (b)/(c) 사유로 설명된다.

### 2-3. 중요 발견 — 697+12건 "definition 불일치"는 원본 오류가 아니라 사후 AI 보강

원본 XLSX를 그대로 재파싱하면 definition이 `None`(빈 칸)이어야 할 709건(tooldict 12 + schema 697)이 실제 DB에는 문장이 채워져 있었다. 직접 대조한 결과:

- 전부 `note`에 `"[AI 자동 생성 뜻풀이]"` 표시, `review_status='검수완료'`, `reviewed_at='2026-09-01 23:47:34'`(709건 전부 동일 시각 — 단일 배치 작업).
- 697건은 정확히 `2,061 - 1,364(원본 채움률 66.2%) = 697`과 일치 — schema 자료의 **비어 있던 definition 전량**이 이 배치에서 채워졌다.
- tooldict의 12건은 원본에서 `definition`이 빈 칸이었던 특수 케이스로, 실제로는 "계획을"(`계획을 세우다`), "고쳐"(`고치다`의 활용형), "소리내어" 등 **2어절 관용구/활용형이 별도 행으로 쪼개져 있어 원래 정의 칸이 비어 있던 표제어**들이다. 예: 엑셀 25행 `계획을`은 정의 칸이 원래 `None`이고, 26행 `세우다`에만 "계획, 방안 따위를 정하거나 짜다."가 있음 — 그런데 DB의 25행(`L1-25`)에는 "앞으로 할 일의 절차나 내용을 미리 정하는 대상."이라는 AI 생성 문장이 채워져 있다.

이 709건은 **원본 데이터 오류가 아니라, `docs/literacy/04-스키마리딩어휘.md` 작성 이후에 실행된 별도 AI 뜻풀이 보강 작업의 정상적인 결과**로 판단한다(같은 시각대에 krdict 25건·sajaseongeo-pdf 37건의 "AI 자동 레벨 부여"도 함께 발생 — 5절 참고, 같은 세션의 연속 작업으로 보임). **다만 이 보강 작업은 `docs/literacy/04-스키마리딩어휘.md`(정의 채움률을 66.2%/99.1%로 서술)에 전혀 반영돼 있지 않다** — 문서와 실제 DB 상태가 어긋난 상태이므로, 다음 단계 전에 문서 갱신이 필요하다. 이 709건을 수행한 스크립트/프롬프트는 이번 조사에서 찾지 못했다(`scripts/literacy/`에 관련 파일 없음) — 재현 불가능한 수작업/일회성 스크립트였을 가능성이 있다.

### 2-4. 참고자료 zip의 나머지 5개 XLSX — 역할만 확인(행 단위 대조는 이번 범위 밖)

`스키마리딩 참고자료.zip`의 나머지 파일은 `SCHEMA_READING_REFERENCE_REVIEW.md`가 이미 읽기 전용으로 검토했고, 이번 조사에서 직접 열어 보조로 재확인했다:

- `스키마 맵 작업용 스키마 정리(레벨 3,4).xlsx`, `스키마 카테고리 분류.xlsx` — 개념 분류 참고자료, 현행 키와 미연결. 미적재.
- `스키마 리딩 주차별 주제 및 스키마 정리(1115 버전).xlsx` — 주차·주제·개념어 관계. 미적재(§6에서 언급한 "정규화 topic 테이블 부재"의 원천 자료).
- `어휘퀴즈DB (1).xlsx` — 위 0-1절 참고. 미적재.
- **`에이프로 교재 중 초등 어휘 모음_(최종).xlsx` — 새로 발견한 사실**: 시트 `초1`~`초6`, 유효 행 합계 835건(초1 173/초2 172/초3 153/초4 143/초5 103/초6 91). 처음에는 `momo-textbook`(821건)의 원천으로 추정했으나, 직접 대조한 결과 **다른 자료였다**: 이 파일의 표제어 상당수("가로지르다", "가파른", "간과하다" 등)가 literacy.db 전체 어디에도 없었고, 일부 일치어("가엾다")도 정의 문장이 달랐다(`"마음이 아플 만큼 불쌍하다."` vs DB `"마음이 아플 정도로 불쌍하고 딱하다."`). `docs/literacy/03-교재어휘적재.md` 1절을 확인하니 `momo-textbook`은 **`momo_book_db/momo_book.db`(교재DB, 881개 중 정제 821건)에서 온 것**으로 명시돼 있어 이 xlsx와는 무관함을 확인했다. 즉 이 파일은 **완전히 별도의, 지금까지 한 번도 적재된 적 없는 세 번째 초등 어휘 원천**이다. 다음 단계 범위를 정할 때 참고할 것.

---

## 3. vocabulary_quiz ↔ literacy 어휘 대조 (V=vocabulary_quiz / S=literacy)

**사용 DB**: V는 **서버** `~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`(`vocabulary_contents` 5,723건, `content_id`/`lemma`/`pos`/`canonical_definition`을 `sqlite3 -readonly`로 조회해 로컬로 가져옴). S는 로컬 `data/literacy.db`(서버와 SHA-256까지 동일함을 1단계에서 이미 확인했으므로 로컬 사용).

### 3-1. 표제어 문자열 완전일치 기준 대조

| 비교 대상 | S 건수 | V 매칭 | 매칭률 |
|---|---:|---:|---:|
| S = schemareading-tooldict + schemareading-schema (3,382건) | 3,382 | **0건** | 0% |
| S = literacy 전체(7,312건, krdict/momo-textbook/sajaseongeo-pdf 포함) | 7,312 | **148건** | 2.0% |

세부: `momo-textbook` 143/821건 일치, `sajaseongeo-pdf` 7/225건 일치, `krdict`·`schemareading-*`는 **0건**. "가치", "고려", "공존", "사회", "경험" 같은 흔한 학습도구어/스키마어조차 V 어휘 목록(구경꾼, 불허하다, 멀찍이, 도토리묵 등 일상 초등 어휘 위주)에 없었다 — 우연한 샘플링 오류가 아니라 **두 어휘 풀의 성격 자체가 다름**을 보여준다. 이는 1단계 보고서가 이미 지적한 "internal_vocab_upper_restore_v1 패키지의 상위 2,320행 대비 V 5,723건과 표제어 완전일치 0건"과 정확히 같은 결론이며, 이번에는 **범위를 3,382건 전체로 확장해도 0건**임을 재확인했다.

### 3-2. 다의어(하나의 표제어가 여러 뜻/품사)

- V(`vocabulary_contents`): **0건** — 5,723개 `content_id`가 5,723개의 서로 다른 `lemma`를 가짐(동일 lemma가 2행 이상 나타나는 경우 없음). 즉 V는 현재 "표제어당 뜻 1개"만 모델링한다.
- S(literacy `terms`): source 내부에서는 헤드워드가 유일하도록 이미 병합됐지만(2절), **source 간에는 66건**이 의도적으로 별도 행 유지(예: "교류"가 tooldict/schema/momo-textbook 3곳에 각각 다른 뜻으로 존재) — `docs/literacy/04-스키마리딩어휘.md` "source 간 병합 금지 규칙" 절 참고.

### 3-3. 양쪽 DB에 모두 등장하는 단어 수

- V 고유 lemma 5,723개, S(tooldict+schema) 고유 headword 3,334개 — 교집합 **0개**.
- V 5,723개 vs S(literacy 전체) 고유 headword 교집합 **148개**.

### 3-4. literacy 콘텐츠 1개당 대응 vocabulary 콘텐츠 수 (1:1/1:N/N:1)

일치하는 148건 전부 **1:1**이었다(`literacy_term → V행` 분포 `{1: 150}`행 카운트 기준, N>1 사례 0건). 반대 방향(`V행 → literacy_term`)도 거의 전부 1:1이며 **N:1(여러 literacy term이 1개 V행에 매칭)은 2건뿐**이었다(literacy 전체 기준, `{1: 146, 2: 2}`).

### 3-5. 결론 — `literacy_term_id` 단일 컬럼으로 충분한가

**충분하다.** 근거:

1. 현재 tooldict+schema(3,382건, 이번 통합의 실질 대상)는 V와 **표제어 완전일치가 0건**이므로, 통합 1단계에서는 어차피 대부분의 `literacy_term_id`가 NULL로 남는다 — 기존 V 콘텐츠에 값을 채우는 문제가 아니라, **앞으로 literacy 표제어를 근거로 새 V 콘텐츠를 만들 때 그 출처를 추적하는 문제**다.
2. V 쪽에 다의어가 전혀 없어(표제어당 정확히 1행) "하나의 V 콘텐츠가 여러 literacy 의미에 대응"할 걱정이 구조적으로 없다.
3. 실제 겹치는 148건에서도 N:M 관계가 사실상 없었다(N:1이 148건 중 2건뿐, 1:N은 0건).
4. 두 DB가 물리적으로 분리된 SQLite 파일이라 SQL FK를 걸 수 없다(1단계 10절에서 이미 확인) — 이런 제약에서 N:M 링크 테이블을 새로 만드는 비용은 지금 확인된 실사용 패턴(1:1 또는 NULL)에 비해 과설계다.

**→ 1단계가 제안한 대로 `vocabulary_contents`에 nullable `literacy_term_id INTEGER` 컬럼 1개만 추가하는 안을 그대로 유지 권고.** (이번 세션에서 실제 컬럼/테이블은 만들지 않았다.)

---

## 4. `vocabulary_content_levels` 레벨 분포 — 직접 SQL vs `audit_research_db.py` 대조

- 저장소 `scripts/vocab/`에는 `audit_research_db.py`가 **없다**(`ls scripts/vocab/audit_research_db.py` → 파일 없음). 1단계가 사용한 패키지(`data/import/schema_reading_phase1_baseline_v1.zip`)에 들어 있던 사본을 다시 꺼내(`/tmp` 재추출, 수정 없음) 그대로 재사용했다.
- 서버(`~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`)에 대해 `python3 audit_research_db.py --database ... --output ...`을 읽기 전용(`mode=ro`, `PRAGMA query_only=ON`)으로 재실행 → `vocabulary_content_levels.rows=5723`, `integrity_check=['ok']`, `foreign_key_error_count=0`이지만 **`distributions`는 이번에도 빈 `{}`**였다. 1단계가 지적한 버그(스크립트의 `categories` 하드코딩 목록에 `vocab_level`/`level_status`가 없고 `vocabulary_level`만 있어 매칭 실패)가 그대로 재현됨을 확인했다 — 코드를 고치지 않고 스크립트 신뢰성 자체를 검증하는 것이 이번 항목의 목적이므로 수정하지 않았다.
- 이 스크립트 결과를 그대로 믿지 않고, **직접 작성한 SQL로 서버 DB를 읽기 전용 재집계**했다:

```sql
SELECT vocab_level, COUNT(*) FROM vocabulary_content_levels GROUP BY vocab_level;
-- 0=612, 1=1635, 2=1621, 3=1825, 4=30  (5·6은 결과에 행 자체가 없음 = 0건)

SELECT level_status, COUNT(*) FROM vocabulary_content_levels GROUP BY level_status;
-- PROVISIONAL_AUTO=3580, REVIEW_BOUNDARY=2143

SELECT level_version, COUNT(*) FROM vocabulary_content_levels GROUP BY level_version;
-- level_policy_v0.1 = 5723 (전량, 단일 버전)

SELECT is_active, COUNT(*) FROM vocabulary_content_levels GROUP BY is_active;
-- 1 = 5723 (전량 활성)

SELECT vocab_level, level_status, COUNT(*) FROM vocabulary_content_levels GROUP BY 1,2;
-- 0/PROVISIONAL_AUTO=586, 0/REVIEW_BOUNDARY=26
-- 1/PROVISIONAL_AUTO=1285, 1/REVIEW_BOUNDARY=350
-- 2/PROVISIONAL_AUTO=21,   2/REVIEW_BOUNDARY=1600
-- 3/PROVISIONAL_AUTO=1682, 3/REVIEW_BOUNDARY=143
-- 4/PROVISIONAL_AUTO=6,    4/REVIEW_BOUNDARY=24
```

**두 방법(스크립트 실행 결과 + 직접 SQL)을 대조한 결과**: 스크립트는 이 컬럼들에 대해 출력을 내놓지 못했으므로(빈 `{}`) 직접 SQL로만 값을 확보했다. 이 값은 1단계 보고서(`schema_reading_phase1_baseline_20260923.md` 3절) 및 이식 검증 보고서(`vocabulary_level_import_verification_20260923.md` 4·14절, 최종 판정 PASS)와 **정확히 일치**한다. **L5·L6은 0건 — 데이터 자체가 없다.**

---

## 5. L5·L6 "옛 표시 기준" ↔ "확정된 새 기준" 차이의 실제 영향

### 5-1. 두 기준

| | L5 | L6 |
|---|---|---|
| 기존 표시 기준(코드+문서 3곳이 서로 일치) | 고1~2 | 고3 |
| 사용자가 이번 지시문에서 확정한 새 기준 | **고1** | **고2~3** |

기존 "고1~2/고3" 기준이 일치하는 3곳: ① `app/vocabulary_quiz/routers/multiformat.py` L105-108 `GRADE_LABELS`, ② `docs/literacy/04-스키마리딩어휘.md`(L58-78 학년 대응표) 및 자매 지시서 `docs/literacy/04-스키마리딩어휘적재.md`, ③ `docs/vocabulary/LEVEL_POLICY_v0.1.md`(및 원본 `data/import/vocabulary_leveling_v1.zip`의 `LEVEL_POLICY.md`). 추가로 `scripts/literacy/auto_review_level.py` L48-56의 `LEVEL_TABLE`(Gemini 프롬프트용 하드코딩 표)과 `data/import/admin_level_quiz_v1.zip`(`PRODUCT_SPEC.md`/`CLAUDE_CODE_MIGRATION.md`, 이미 구현·배포된 관리자 레벨 퀴즈 기능의 설계 원본)도 동일한 옛 기준을 쓴다.

### 5-2. ① `vocabulary_content_levels`(관리자 퀴즈가 실제로 쓰는 테이블)에 미치는 영향

**영향 없음(현재는).** 4절에서 확인했듯 L5·L6 행이 **0건**이다. `LEVEL_POLICY.md`/`LEVEL_POLICY_v0.1.md`도 "현재 원천에는 5등급 이상 어휘가 없으므로 L5·L6을 억지로 채우지 않는다"고 명시한다(원천: 김광해 계열 국어교육용 어휘 등급 2~4등급만 보유). 즉 **재배정이 필요한 기존 행 자체가 없다** — 다만 향후 "고등 어휘 자료 확장" 시 이 정책 문서와 `assign_levels.py`(`data/import/vocabulary_leveling_v1.zip`)가 새 기준으로 갱신되지 않으면 처음부터 잘못된 기준으로 다시 쌓이게 된다.

### 5-3. ① literacy.db `terms.level`(관리자 퀴즈와 무관하지만 통합 대상)에는 실제 영향이 있다

`terms.level`은 L5·L6에 실제 데이터가 있다 — 직접 SQL 재집계:

| source | level=5 | level=6 | 근거 |
|---|---:|---:|---|
| `schemareading-schema` | 507 | 608 | 원본 XLSX "단계" 열(1~6단계) 앞자리를 그대로 `level`에 저장(§1). "옛 3~6단계"가 실제 몇 학년인지 원본에 명시적 근거 없음 — `SCHEMA_READING_REFERENCE_REVIEW.md`가 이미 "각 단계를 서비스 학년으로 자동 등치하지 않는다"고 경고 |
| `schemareading-tooldict` | 108 | 85 | 위와 동일(엑셀 시트명 `Level5`/`Level6`) |
| `krdict`(관용구·속담) | **25** | 0 | `scripts/literacy/auto_review_level.py`가 **옛 기준 표(L5=고1~2/L6=고3)를 Gemini 프롬프트에 넣어 2026-09-01 23:46:45에 자동 판정**, `review_status='검수완료'`로 이미 확정 저장. 예: `치(를) 떨다`(id 15) note="[AI 자동 레벨 부여: 5] ... 고등학교 수준", `풍운의 뜻`(id 128) note="... 고등 1~2학년 수준의 어휘력이 요구됨" |
| `sajaseongeo-pdf`(사자성어) | **37** | 0 | 위와 같은 배치(219/225건 AI 레벨 부여, 6건은 definition 없어 제외). 예: `가렴주구`(id 7088) note="... 고등학교 국어 및 한국사 교과서에서 ..." |
| **합계** | **677** | **693** | 1단계 보고서 8절의 `level 분포: ... 5=677, 6=693`과 정확히 일치 |

**핵심 리스크**: krdict·사자성어의 62건(25+37)은 "고등학교 수준"이라고만 판단했을 뿐 **고1과 고2~3을 구분한 근거가 원래 프롬프트에 없다**(`LEVEL_TABLE`이 "고1~2"를 하나로 묶어 제시했으므로). 새 기준(L5=고1 단독)으로 재정렬하려면 이 62건은 **AI 판정 근거 텍스트만으로는 고1/고2~3 중 어느 쪽인지 기계적으로 재분류할 수 없다** — 재프롬프트 또는 사람 재검수가 필요하다. schemareading 쪽 615건(507+108)도 마찬가지로 원본 "단계" 라벨과 실제 학년의 대응이 확정되지 않은 상태다(1단계 12절에 이미 "사용자 결정 필요"로 기록됨).

### 5-4. ② 관리자 레벨별 퀴즈 출제 로직(`admin_level_quiz_v1`)에 미치는 영향

`app/vocabulary_quiz/routers/multiformat.py`(현재 배포 HEAD 기준, 1단계 9절과 동일 위치 재확인):

- L98-108: `LEVEL_VERSION="level_policy_v0.1"`, `VALID_LEVELS=frozenset(range(7))`, `GRADE_LABELS={..., 5: "고등 1~2학년", 6: "고등 3학년"}` — **옛 기준 문자열이 그대로 하드코딩**돼 있다.
- L192 `_matching_level_content_ids()` / L203 `_select_level_candidates()`: 필터링은 오직 `vocabulary_content_levels.vocab_level`/`level_status`/`level_version`/`is_active`만 사용 — `GRADE_LABELS`는 **화면 표시 문자열일 뿐 후보 선정 로직에는 관여하지 않는다.**
- L230 `/api/vocabulary-quiz/availability`, L426 `POST /sessions`: L5·L6 요청 시 후보 수 0 → `409 INSUFFICIENT_LEVEL_CANDIDATES`로 이미 막혀 있다(`data/import/admin_level_quiz_v1.zip`의 `PRODUCT_SPEC.md`에도 "L5·L6는 구조를 보여주되 비활성화"라고 설계돼 있음. 실제 배포 코드도 이와 일치).

**결론: 기능적 영향(어떤 콘텐츠가 출제되는지)은 없다** — L5·L6 후보가 0건이므로 새 기준이든 옛 기준이든 결과가 같다(둘 다 항상 "후보 없음"). **영향은 표시 문자열(`GRADE_LABELS`)뿐**이며, 지금 이 문자열을 보는 사람(관리자)에게 틀린 학년 정보("고등 1~2학년"/"고등 3학년")를 보여주고 있는 상태다.

### 5-5. 수정이 필요한 대상 목록 (이번 단계에서는 수정하지 않음 — 목록화만)

**코드**
- `app/vocabulary_quiz/routers/multiformat.py` L105-108 `GRADE_LABELS` — 표시 문자열만 영향, 데이터 영향 없음(5-4절)
- `scripts/literacy/auto_review_level.py` L48-56 `LEVEL_TABLE` — **이미 62건에 영향을 준 프롬프트**. 향후 남은 대상(krdict level IS NULL 159건, sajaseongeo-pdf level IS NULL 6건)에 계속 쓰이면 새 오분류가 늘어남
- `app/literacy/models.py` L35 주석("5~6 (고1~2=5... 고3=6)") — 코드 동작에 영향 없는 주석이지만 다음 작업자 혼동 방지 차원에서 갱신 대상

**문서**
- `docs/literacy/04-스키마리딩어휘.md` L58-78 "레벨 0~6 체계와 학년 대응표" — schemareading 3,382건의 권위 문서
- `docs/literacy/04-스키마리딩어휘적재.md` L17-27(원 지시서, 이력 보존 목적상 낮은 우선순위)
- `docs/vocabulary/LEVEL_POLICY_v0.1.md` — vocabulary_content_levels 권위 문서(현재 L5·L6엔 데이터 없어 급하지 않음)
- `data/import/vocabulary_leveling_v1.zip`(`LEVEL_POLICY.md`)과 `data/import/admin_level_quiz_v1.zip`(`PRODUCT_SPEC.md`, `CLAUDE_CODE_MIGRATION.md`) — 배포 완료 기능의 설계 산출물(아카이브, 우선순위 낮음)

**데이터(재검수 필요, 이번 단계에서 손대지 않음)**
- literacy.db `terms.level=5`(677건) / `level=6`(693건), 특히 원본 판정 근거가 "고1~2"를 하나로 묶은 62건(krdict 25 + sajaseongeo-pdf 37)은 새 기준으로 고1/고2~3 재분류 근거가 없어 **재프롬프트 또는 수동 재검수 필요**
- schemareading-tooldict/schema의 L5·L6 615건은 원본 "단계" 라벨 자체의 실제 학년 대응이 아직 미확정(1단계부터 이어지는 막힌 지점)

---

## 요약

**1) `app/literacy` 기존 자산 재활용 범위**
`terms`/`examples`/`collection_runs` 테이블, `Term` 모델, `import_schemareading_vocab.py`+`schemareading_parser.py`(결정론적으로 재현 가능함을 이번에 직접 검증)는 **그대로 재사용**한다. 신규 원천 테이블은 만들지 않는다(1단계 결론 유지). 다만 (a) 2026-09-01 AI 보강 배치가 문서화되지 않은 점, (b) `schema_topics`류 정규화 테이블이 아직 없는 점(YAGNI로 다음 단계로 계속 유보 권고)은 재활용 전에 인지하고 있어야 한다.

**2) 두 DB 연결 방법과 근거**
`vocabulary_contents`에 nullable `literacy_term_id INTEGER` 컬럼 1개만 추가(1단계 제안 유지, 이번 세션에서 미적용). 근거: tooldict+schema 3,382건과 V 5,723건의 표제어 완전일치가 **0건**이고, literacy 전체로 넓혀도 148건뿐이며 그마저 거의 전부 1:1(V 쪽 다의어 0건, N:1은 148건 중 2건뿐)이라 N:M 링크 테이블을 정당화할 실사용 패턴이 없다.

**3) L5·L6 정책 수정 대상**
코드 1곳(`multiformat.py` GRADE_LABELS, 표시만 영향)·스크립트 1곳(`auto_review_level.py` LEVEL_TABLE, **이미 62건에 실질 영향**)·문서 4~5곳(§5-5 목록). `vocabulary_content_levels`는 L5·L6 데이터가 0건이라 관리자 퀴즈 기능 자체는 지금 당장 영향이 없지만, literacy.db `terms.level`은 5=677/6=693건이 이미 옛 기준으로 채워져 있고 그중 62건(krdict+사자성어)은 재분류 근거가 부족해 **다음 단계 전 사람 재검수가 필요**하다.

**4) 다음 dry-run 단계 입력값·자동 검증 조건**
- 입력: (i) `vocabulary_contents`(5,723건, 서버 기준) 스냅샷, (ii) `literacy.db terms`(schemareading-tooldict 1,321 + schemareading-schema 2,061, 필요시 momo-textbook/sajaseongeo-pdf의 148건 매칭 후보 포함), (iii) L5·L6 재검수가 필요한 62건 + 615건 목록(재분류 완료 전까지 별도 플래그로 보류)
- 자동 검증 조건(모두 통과해야 진행):
  - `literacy_term_id` 백필 대상 건수가 헤드워드 완전일치 148건(또는 향후 정규화 매칭 로직이 정한 건수)과 정확히 같아야 함, 새로 생성되는 V 행은 0이어야 함(이번 단계는 백필만, 신규 콘텐츠 생성 아님)
  - 재실행 시 `inserted=0/updated=0/unchanged=N`(멱등성)
  - `student_exposure`/`public_ready` 컬럼 값 변경 0건(assertion)
  - L5·L6 정책이 확정되기 전에는 `terms.level`을 `vocab_level`에 자동 매핑하는 코드 경로가 아예 실행되지 않아야 함(가드: 명시적 `--confirm-level-policy` 플래그 없이는 거부)
  - 62건(krdict+사자성어 AI 레벨 부여) 및 615건(schemareading L5/L6)은 재검수 완료 전까지 dry-run 대상에서 명시적으로 제외되고, 제외 건수가 로그에 정확히 찍혀야 함

---

## 주요 리스크·충돌 사항 (최상위 요약)

1. **v3 문서 소재 불명** — 지시된 파일명이 어디에도 없음. 사용자 확인 필요(0절).
2. **정책 충돌(재확인, 데이터 실영향 포함)** — 코드·문서 5곳이 "L5=고1~2/L6=고3"을 쓰는데 사용자가 확정한 새 기준은 "L5=고1/L6=고2~3". `vocabulary_content_levels`는 영향 없음(L5·L6 데이터 0건)이지만, literacy.db `terms.level`에는 이미 옛 기준으로 채워진 1,370건(677+693)이 있고 그중 62건은 AI가 "고1~2"를 구분 없이 판정해 새 기준으로 기계적 재분류가 불가능하다.
3. **어휘 풀 분리(확인됨, 우려 아님)** — vocabulary_quiz(V)와 literacy 스키마리딩 자료(S)는 표제어가 사실상 겹치지 않는다(0/3,382). `literacy_term_id` 단일 컬럼 설계는 이 사실 위에서 타당하다.
4. **문서-DB 드리프트** — `docs/literacy/04-스키마리딩어휘.md`가 서술하는 정의 채움률(66.2%/99.1%)이 2026-09-01 AI 보강 이후의 실제 DB 상태(사실상 100%)와 다르다. 이 보강을 수행한 스크립트를 찾지 못해 재현 불가 — 별도 확인 필요.
5. **참고자료 zip의 "에이프로 교재 중 초등 어휘 모음" xlsx**가 `momo-textbook`(821건, `momo_book_db` 출처)과는 무관한 제3의 미적재 초등 어휘 자료(835행)임을 새로 확인 — 다음 단계 범위 결정 시 참고.
6. **`어휘퀴즈DB.xlsx`의 두 버전이 SHA-256 기준으로 서로 다름**(raw/ 사본 vs 참고자료 zip 사본) — 이 파일을 다룰 다음 단계에서 어느 버전을 정본으로 쓸지 먼저 확인 필요.

보고서 파일: `reports/schema_reading_phase2_readonly_audit_20260924.md`
