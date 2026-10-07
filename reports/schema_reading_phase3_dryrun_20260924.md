# 스키마리딩x어휘 DB 통합 — 3단계 dry-run 준비 보고서 (읽기 전용)

- 작성일: 2026-09-24 (`date` 명령으로 시스템 현재 날짜 직접 확인)
- 범위: 2단계(`reports/schema_reading_phase2_readonly_audit_20260924.md`)에 이어지는
  읽기 전용 dry-run. **이번 세션에서 로컬·서버 어느 DB에도 INSERT/UPDATE/DELETE/ALTER/CREATE를
  실행하지 않았다.** literacy.db는 항상 `file:...?mode=ro`(URI 모드)로만 열었고, 서버는
  전부 `sqlite3 ... -readonly` 또는 `PRAGMA query_only=ON`으로만 접근했다.
- **2단계 보고서에 이미 확립된 사실(148건 완전일치 기준집합, 두 zip의 SHA-256, literacy.db
  스키마 구조, 62건/615건 근거, 어휘퀴즈DB.xlsx 버전 차이 등)은 재조사하지 않고 그대로
  인용·재사용했다.** 이번 보고서가 새로 수행한 것은 (a) v3/v4 문서 재확인, (b) 148건 기준집합을
  독립적으로 재현하고 행 단위 의미 대조, (c) 연결 기수성 반례 확인, (d) 62/615건 교집합 정의,
  (e) L5/L6=0건 재검증, (f) SHA-256 재계산 대조, (g) dry-run 판정표 생성 및 2회 실행 멱등성
  검증, (h) 백업/체크섬 절차 설계(가벼운 검증만 실제 수행)다.

---

## 0. v3·v4 문서 소재 확인

- **v3**(`schema_reading_integration_revised_after_server_audit_v3.md`): 지시받은 경로
  `data/import/schema_reading_integration_revised_after_server_audit_v3.md`에 **loose 파일로
  실제 존재**한다. 전문을 읽었다 — 2단계가 zip 안에서만 찾다가 놓친 것이 맞았다(2단계 0절의
  "v3 파일명이 어디에도 없다"는 결론은 zip 내부 검색만 했기 때문이며, 이번에 지정받은 경로로
  바로 열어 확인했다). v3의 핵심 확정 사항(L0~L6 학년 정책, `internal_vocab_upper_restore_v1.zip`의
  성격, 다음 실행 단계 6개)은 아래 각 절에서 반영했다.
- **v4**(`schema_reading_phase3_dryrun_handoff_v4.md` 또는 유사명): **찾지 못했다.**
  확인한 범위: `data/import/` 전체 목록, `reports/` 전체 목록, 두 zip
  (`data/import/schema_reading_phase1_baseline_v1.zip`, `data/import/internal_vocab_upper_restore_v1.zip`,
  `data/import/스키마리딩 참고자료.zip`) 내부 파일명 전체, 그리고 저장소 전체에서
  `find`(venv/node_modules 제외) + `grep -ril`로 "v4"/"phase3"/"dryrun"/"dry-run"/"dry_run"/"handoff"
  키워드 검색. "handoff"로는 `app/vocab/HANDOFF.md`, `docs/vocab/HANDOFF.md`,
  `internal_vocab_upper_restore_v1.zip` 안의 `CLAUDE_CODE_HANDOFF.md`가 걸렸지만 전부 이번
  스키마리딩x어휘 통합과 무관한(또는 이미 2단계가 확인한) 별개 문서였다. **v4 문서는 누락된
  것으로 보고 v3 + phase2 보고서만으로 이번 단계를 진행했다.**

---

## 1. 두 DB의 실제 스키마·경로 확인

### 1-1. literacy.db (S)

`app/literacy/db.py`는 환경변수 분기 없이 항상 `REPO_ROOT/data/literacy.db`를 고정 경로로 쓴다
(로컬 전용 파일, `APP_ENV`와 무관). 이번 세션에서 `mode=ro` URI로 직접 스키마를 재확인했다.

```sql
CREATE TABLE terms (
    id INTEGER NOT NULL,
    category TEXT NOT NULL, headword TEXT NOT NULL, origin TEXT,
    definition TEXT, pos TEXT, sense_category TEXT, subject_category TEXT,
    grade_level INTEGER, grade_source TEXT NOT NULL, source TEXT NOT NULL,
    license TEXT, external_id TEXT, collected_at DATETIME NOT NULL,
    updated_at DATETIME, review_status TEXT NOT NULL, note TEXT,
    level INTEGER, reviewed_at DATETIME,
    PRIMARY KEY (id),
    CONSTRAINT uq_terms_source_external_id UNIQUE (source, external_id)
);
```
PK=`id`(정수), 자연키=`(source, external_id)`. `examples.term_id`가 `terms.id`를 FK로 참조
(`ON DELETE CASCADE`), `collection_runs`는 독립 테이블(적재 이력 로그).
`PRAGMA integrity_check` → `ok`, `PRAGMA foreign_key_check` → 위반 0건(이번 세션 재확인).
`terms` 총 7,312건(재확인, 2단계와 일치).

### 1-2. vocabulary_quiz 연구용 DB (V, 서버)

`app/vocabulary_quiz/db.py`는 `VOCABULARY_QUIZ_DB_PATH` 환경변수로 경로를 결정한다(로컬 기본값은
`data/vocab/vocabulary_quiz_rnd.db`이며 **이번 조사 대상이 아니다** — 지시문대로 로컬 DB는 이름이
다르고 내용도 별개다). 서버에서 실제로 확인:

```
$ ssh aprolabs "grep -E '^APP_ENV|^VOCABULARY_QUIZ_DB_PATH' ~/aprolabs/.env"
APP_ENV=research
VOCABULARY_QUIZ_DB_PATH=/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db

$ ssh aprolabs "cat /proc/1089154/environ | tr '\0' '\n' | grep -E '^APP_ENV|^VOCABULARY_QUIZ_DB_PATH'"
APP_ENV=research
VOCABULARY_QUIZ_DB_PATH=/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
```
`.env` 값과 실행 중인 `app.main:app` uvicorn 프로세스(PID 1089154)의 `/proc/<PID>/environ`이
byte-for-byte 일치 — 1단계·2단계가 확인한 것과 동일한 결론을 이번에도 독립적으로 재확인했다.
이번 조사는 이 경로(`~/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`)를 **서버에서
`sqlite3 -readonly` SELECT로만** 조회했다.

```sql
CREATE TABLE vocabulary_contents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id TEXT NOT NULL UNIQUE,   -- 자연키('SC_V191_B001_001')
    sense_id TEXT, lexical_entry_id TEXT, batch_id TEXT,
    lemma TEXT NOT NULL, pos TEXT,
    canonical_definition TEXT, student_definition TEXT,
    example_sentence TEXT, example_target_form TEXT,
    generation_method TEXT, qa_method TEXT, generation_status TEXT,
    student_exposure INTEGER NOT NULL DEFAULT 0,
    public_ready INTEGER NOT NULL DEFAULT 0,
    quality_batch_id TEXT, hold_reason TEXT, merge_source TEXT,
    source_version TEXT NOT NULL, is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE vocabulary_content_levels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id TEXT NOT NULL REFERENCES vocabulary_contents(content_id),
    vocab_level INTEGER NOT NULL CHECK (vocab_level BETWEEN 0 AND 6),
    target_grade_band TEXT, level_score REAL, level_confidence REAL,
    level_status TEXT NOT NULL CHECK (level_status IN ('PROVISIONAL_AUTO','REVIEW_BOUNDARY')),
    boundary_flag INTEGER NOT NULL DEFAULT 0, level_source TEXT,
    level_version TEXT NOT NULL, level_reason_json TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE (content_id, level_version)
);
```
- PK=`id`(정수), 자연키=`content_id`(TEXT UNIQUE) — **`vocabulary_content_levels`의 FK는
  정수 `id`가 아니라 TEXT `content_id`를 참조**한다는 점이 이번에 새로 명시적으로 확인한
  세부사항이다(1·2단계는 "content_id 기준"이라고만 서술, 실제 `REFERENCES` 절 문법까지는
  인용하지 않았음). `literacy_term_id` nullable 컬럼을 추가한다면 `vocabulary_contents`(정수
  `id` PK, TEXT `content_id` 자연키) 쪽에 붙는 것이지 `vocabulary_content_levels`와는 무관하다.
- `PRAGMA integrity_check` → `ok`, `PRAGMA foreign_key_check` → 위반 0건(이번 세션 재확인).
- 스냅샷: `vocabulary_contents` 5,723건, `vocabulary_content_levels` 5,723건(재확인, 2단계와 일치).

---

## 2. 148건 기준집합 재현 — 분석기 스크립트

### 2-1. 방법

새 스크립트 `scripts/vocab/analyze_schema_reading_link_dryrun.py`를 작성했다(읽기 전용,
어떤 DB에도 쓰지 않음). 동작:

1. `data/literacy.db`를 `file:...?mode=ro`로 열어 `terms` 7,312건을 읽는다.
2. `vocabulary_contents`/`vocabulary_content_levels`는 서버에 직접 접속할 방법이 없으므로(SSH
   콘솔만 가능), 사전에 **서버에서 SELECT 전용으로 내보낸 CSV 스냅샷**을 입력으로 받는다.
   스냅샷 생성 명령(전부 `-readonly`, 서버에 어떤 변경도 없음):
   ```
   ssh aprolabs "sqlite3 'file:/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db?mode=ro' \
     -readonly -header -csv \"SELECT id, content_id, sense_id, lexical_entry_id, batch_id, lemma, pos, \
     canonical_definition, student_definition, generation_method, qa_method, generation_status, \
     student_exposure, public_ready, source_version, is_active FROM vocabulary_contents\""
   ssh aprolabs "sqlite3 'file:...?mode=ro' -readonly -header -csv \"SELECT content_id, vocab_level, \
     target_grade_band, level_status, level_version, is_active FROM vocabulary_content_levels\""
   ```
   두 CSV는 5,724행(헤더+5,723) 각각 — 문서화된 건수와 정확히 일치.
3. `unicodedata.normalize("NFC", s).strip()`으로만 정규화한 뒤 `V.lemma == S.headword` 완전일치를
   구한다(그 외 가공 없음 — 2단계와 동일한 "완전일치" 정의).
4. 일치하는 모든 (V content_id, S term_id) 쌍을 행 단위로 펼쳐 JSONL로 저장한다. 각 행에 표제어,
   V/S 품사·정의, V source_version/generation_method, S source, 양쪽 현재 레벨, 이 headword에 대한
   S 행 수/V 행 수, 기수성 분류(`1:1`/`1:N`/`N:1`/`N:M`), 그리고 **자동 승인 근거가 아닌 참고용**
   문자열 유사도 힌트(`definition_heuristic_note`)를 담는다.

### 2-2. 재현 결과

```
literacy terms: 7312건
vocabulary_contents 스냅샷: 5723건
vocabulary_content_levels 스냅샷: 5723건
V lemma 중복(같은 lemma가 2개 이상 content_id): 0건
표제어 완전일치(고유 headword 기준): 148건
headword x source 조합까지 펼친 총 행(비교쌍) 수: 150
```

2단계 보고서 3절의 숫자(고유 headword 148건, `{1: 150}`/`{1:146, 2:2}` 행 카운트)와 **정확히
일치**한다 — 다른 방법(별도로 새로 작성한 스크립트, 서버에서 새로 뽑은 CSV)으로 독립적으로
재현해도 같은 148/150이 나온다는 뜻이다.

### 2-3. 멱등성 검증(2회 실행)

같은 입력으로 스크립트를 2회 실행하고 출력 JSONL을 `diff`했다.

```
1회차 idempotency_hash=1cf851999b9e56444ee624b29ea224ffbe27d3631a6b289234bc80485daa7fb4
2회차 idempotency_hash=1cf851999b9e56444ee624b29ea224ffbe27d3631a6b289234bc80485daa7fb4
diff candidate_rows_run1.jsonl candidate_rows_run2.jsonl  →  차이 없음 (identical)
```
이어서 판정 스크립트(3절)까지 포함한 전체 파이프라인도 2회 실행해 `verdicts_run1.jsonl`과
`verdicts_run2.jsonl`을 `diff`했고 **완전히 동일**했다. 분석기는 결정론적이며 재실행해도 다른
결과를 내지 않는다.

---

## 3. 148건(150행) 의미 검증 — 사람이 직접 정의를 대조한 결과

`scripts/vocab/build_link_dryrun_verdicts.py`를 추가로 작성했다. 이 스크립트는 카디널리티가
1:1이 아니거나(→ `MULTIPLE_LINKS`), L5/L6이 걸려 있거나(→ `OLD_LEVEL_REVIEW`), 정의가 한쪽만
비어 있는(→ `UNVERIFIED_DEFINITION`) 행은 규칙으로 분류하되, **그 규칙에 걸리지 않는 애매한
행은 `_MANUAL_VERDICTS` 표에 에이전트가 직접 두 정의를 읽고 판단한 결과를 넣도록 강제**했다
(표에 없는 회색지대 행이 조용히 자동 승인되는 경로가 없도록 함).

전체 150행을 실제로 읽었다(요약, 전체는 `data/import/schema_reading_link_dryrun_verdicts_20260924.csv`
/ `.jsonl` 참고):

| status | 건수 | 판단 근거 요약 |
|---|---:|---|
| `APPROVABLE_CANDIDATE` | 142 | 65건은 정의 문자열 완전 동일, 나머지 77건은 문자열은 다르지만 직접 대조한 결과 같은 뜻의 패러프레이즈(예: 겨우내 "한겨울 동안 계속해서" ≈ "겨울 동안 내내", 부축하다 "겨드랑이를 붙잡아 걷는 것을 돕다" ≈ "다른 사람이 몸을 움직이는 것을 곁에서 도와주다") |
| `MULTIPLE_LINKS` | 4 (headword 2개: 속수무책, 혼비백산) | 아래 4절 참고 — 같은 V content가 momo-textbook과 sajaseongeo-pdf 두 S term에 동시에 매칭 |
| `AMBIGUOUS_SENSE` | 2 | **시샘**: V정의가 "'시새움'의 준말"이라는 상호참조 형식이라 그 자체로 완결된 뜻풀이가 아니어서 S정의(질투/시기의 전형적 정의)와의 등가성을 문자열만으로 직접 검증 불가. **평론**: V정의는 대상 제한 없는 일반적 "평가하여 논함"인데 S정의는 "작품이나 특정 대상에 대한 분석"으로 범위가 좁아 하위 의미일 가능성 — 사람 재확인 필요 |
| `UNVERIFIED_DEFINITION` | 2 | **능가하다**: S정의에 "한쪽으로 치우친 성질능력이나"라는 구두점 누락으로 보이는 원문 손상(쉼표 탈락 추정: "성질, 능력이나"). **설상가상**: S정의 앞에 "1994학년도 1차 수능"이라는 출제 연도 메타데이터가 뜻풀이 본문에 섞여 들어감(사자성어 PDF 추출 아티팩트) — 본문 자체는 V와 동일하나 필드 정제 전에는 definition 컬럼을 그대로 신뢰할 수 없음 |
| `OLD_LEVEL_REVIEW` | 0 | 148건 매칭 집합 안에는 literacy_level·vocab_level 어느 쪽도 5·6이 없음(최댓값 S=2, V=3) — 62건/615건과 **교집합 0**(6절 참고) |
| `NO_MATCH` | 0 (정의상) | 이 표는 "완전일치 148건" 기준집합만 다루므로 정의상 NO_MATCH가 없다. V 5,723건 중 5,575건, S 7,312건 중 7,162건은 애초에 매칭 후보가 아니므로 이 표에 없다(별도 행으로 나열하지 않음 — 5,000행 이상을 전부 NO_MATCH로 채우는 것은 실익이 없다고 판단) |

**콘텐츠별 "실제 승인 건수"를 최종 결론으로 주장하지 않는다** — 위 표는 상태별 집계이며,
`AMBIGUOUS_SENSE`/`UNVERIFIED_DEFINITION` 8행은 다음 단계 전에 사람 재검수가 필요하다.

---

## 4. 연결 기수성 — 행 단위 반례 확인

148건 기준집합에서 실제로 관찰된 기수성(V→S 방향 기준, "V 1건이 S 몇 건에 대응하는가"):

| 기수성 | 건수(row) | 건수(headword) | 사례 |
|---|---:|---:|---|
| `1:1` | 146 | 146 | 예: 가파르다(V:SC_V192_B002_005 ↔ S:3198) |
| `1:N` | 4 | 2 | **속수무책**(V:SC_V1976_B076_042 ↔ S:3185 momo-textbook, S:7201 sajaseongeo-pdf), **혼비백산**(V:SC_V19134_B134_003 ↔ S:3015 momo-textbook, S:7306 sajaseongeo-pdf) |
| `N:1` | 0 | 0 (V lemma 중복 0건이므로 구조적으로 발생 불가) | — |
| `N:M` | 0 | 0 (N:1 성립 불가이므로 N:M도 불가) | — |
| `0:1`/`1:0`("매칭 없음") | — | V 5,575건 / S 7,162건 | 대부분의 행 — nullable 컬럼이 NULL로 남는 정상 케이스 |

### 4-1. 단일 nullable 컬럼을 무너뜨리는 실제 반례

**속수무책**과 **혼비백산** 2건은 1단계·2단계가 제안한 `vocabulary_contents.literacy_term_id
INTEGER`(nullable, 단일 값) 컬럼 설계를 **문자 그대로는 충족시키지 못하는 실제 반례**다:

- V content `SC_V1976_B076_042`(lemma=속수무책)는 literacy.db에 `term_id=3185`(momo-textbook,
  level=1, def="손을 묶은 것처럼 어찌할 도리가 없어 꼼짝 못 함.")와 `term_id=7201`(sajaseongeo-pdf,
  level=2, def="손을 묶은 것처럼 어찌할 도리가 없어 꼼짝 못함.") **양쪽 모두**와 대응한다.
  두 정의는 사실상 동일한 의미(공백 하나 차이)지만, **레벨은 1 대 2로 서로 다르다** — 즉 이 둘은
  "완전히 같은 행의 중복"이 아니라 "같은 뜻, 다른 출처, 다른 레벨"을 가진 별개 행이다.
- 단일 컬럼에는 둘 중 하나의 `literacy_term_id`만 넣을 수 있으므로, 다른 한쪽 출처(momo-textbook
  또는 sajaseongeo-pdf)로의 연결 정보와 그 출처가 가진 레벨 값은 **컬럼 설계상 영구히 유실**된다.
  이는 2단계가 이미 짚은 "source 간 병합 금지 규칙"(2단계 3-2절, source 간 66건 별도 유지)이
  V-S 연결에도 그대로 적용되는 사례다.
- **결론(반례 있음, 그러나 영향은 작음)**: 148건 중 146건(98.6%)은 `1:1`이라 단일 컬럼으로
  충분하지만, **2건(1.4%)은 구조적으로 단일 컬럼이 정보를 잃는다.** 1·2단계의 "단일 컬럼으로
  충분하다"는 결론을 완전히 뒤집을 정도는 아니지만("과반수 1:1"이라는 결론 자체는 유지됨),
  "예외 없이 충분하다"는 주장은 성립하지 않는다. 권고: 단일 nullable 컬럼을 기본으로 채택하되,
  이런 `1:N` 사례가 나오면 **더 신뢰도 높은/서비스에 가까운 출처 하나만 선택하고 나머지는
  버린다는 명시적 우선순위 규칙**(예: momo-textbook > sajaseongeo-pdf > schemareading-* 같은
  source 우선순위표)을 다음 단계에서 정의해야 한다. 이번 세션은 이 규칙을 확정하지 않았다
  (규칙 확정은 DB 쓰기로 이어지는 결정이라 범위 밖).

### 4-2. 교차 DB ID 참조 무결성 검증 설계 (제안만, 미실행)

두 SQLite가 별도 파일이라 SQL FK를 걸 수 없다. 제안하는 애플리케이션 레벨 체크:

1. `scripts/vocab/verify_literacy_link_integrity.py`(신규, 다음 단계에서 작성) — 항상 읽기 전용:
   - `vocabulary_contents.literacy_term_id`가 NOT NULL인 모든 행에 대해 literacy.db에 해당
     `id`의 `terms` 행이 실제로 존재하는지 확인(dangling reference 검출).
   - 존재하면 `(headword, pos)` 스냅샷을 같이 저장해 두고, 다음 실행 때 literacy.db 쪽 값이
     바뀌었는지(headword/definition drift) 비교해 경고.
   - 위반 발견 시 종료 코드 비0 + 위반 목록 출력 — CI/배포 전 게이트로 사용.
2. literacy.db `terms`에 대한 삭제는 **하드 삭제 금지, review_status 등으로 소프트 삭제** 정책을
   권고(현재 모델에 소프트 삭제 컬럼이 없으므로 이것도 다음 단계 설계 항목).
3. 배포 파이프라인에 이 스크립트를 "두 DB 동시 배포 후 자동 실행"으로 넣어, literacy.db와
   vocabulary_quiz DB가 서로 다른 시점에 롤백되는 경우를 감지한다.

---

## 5. 62건·615건 — 정의·교집합·합집합 (재검증)

### 5-1. 정의 (2단계 5-3절 인용 + 독립 재확인)

이번 세션에서 아래 SQL을 literacy.db에 `mode=ro`로 직접 실행해 2단계 결과를 재확인했다(재조사
아님, 숫자만 재계산):

```sql
SELECT source, level, COUNT(*) FROM terms WHERE level IN (5,6) GROUP BY source, level;
-- krdict              | 5 | 25
-- sajaseongeo-pdf      | 5 | 37
-- schemareading-schema | 5 | 507
-- schemareading-schema | 6 | 608
-- schemareading-tooldict | 5 | 108
-- schemareading-tooldict | 6 | 85
-- (총 1,370건 — 2단계 5-3절의 677+693과 정확히 일치)

SELECT source, level, COUNT(*) FROM terms
WHERE level IN (5,6) AND note LIKE '%AI 자동 레벨 부여%' GROUP BY source, level;
-- krdict          | 5 | 25   ← 62건의 절반
-- sajaseongeo-pdf | 5 | 37   ← 62건의 나머지 절반
-- (schemareading-schema/tooldict의 L5·L6 615건에는 이 note가 없음 — AI 자동 판정이 아니라
--  원본 엑셀 "단계" 값을 그대로 저장한 것)
```

- **62건** = `krdict`(25건, level=5) + `sajaseongeo-pdf`(37건, level=5). 전부
  `scripts/literacy/auto_review_level.py`의 옛 `LEVEL_TABLE`(L5="고1~2" 통합)을 Gemini 프롬프트에
  넣어 2026-09-01 23:46:45에 자동 판정, `note`에 `[AI 자동 레벨 부여: 5]` 표시, `review_status='검수완료'`.
  **62건 전부 level=5이며, level=6인 건은 0건이다**(krdict·사자성어 원천에는 애초에 L6 후보가 없음).
- **615건** = `schemareading-schema`(507건, level=5) + `schemareading-tooldict`(108건, level=5) =
  507+108=615. 이 615건은 AI 자동 판정이 아니라 **원본 XLSX의 "단계"(1~6단계) 열 값을 그대로
  `level`에 저장**한 것 — 서비스 학년으로의 대응이 애초에 이루어진 적이 없다(2단계 인용, `note`에
  AI 관련 표시 없음을 이번에 재확인).
- 2단계 원문 "schemareading 쪽 615건(507+108)"은 **level=5만** 합산한 숫자다(level=6인
  schemareading-schema 608건·schemareading-tooldict 85건은 615에 포함되지 않음). 이 615는 L6을
  포함하지 않는다는 점을 이번 세션에서 명확히 했다(2단계 원문은 이 구분을 명시하지 않아 혼동 소지가
  있었음).

### 5-2. 교집합·합집합

- **교집합(62건 ∩ 615건) = 0건** — source가 서로 배타적이다(`krdict`/`sajaseongeo-pdf` vs
  `schemareading-schema`/`schemareading-tooldict`).
- **합집합(62건 ∪ 615건) = 677건**, 그리고 이 677은 **level=5 전체 건수(25+37+507+108=677)와
  정확히 일치**한다 — 즉 62건과 615건을 합치면 `terms.level=5`인 행 전부를 남김없이 덮는다.
- **양쪽 다 빠진 부분(=level=6 전체) = 693건**(`schemareading-schema` 608 + `schemareading-tooldict`
  85, krdict/사자성어에는 L6 데이터가 없음). 이 693건은 **62건에도 615건에도 속하지 않는
  별도의 미해결 집합**이다 — schemareading L5(615건)와 원인은 같다(원본 "단계" 값을 그대로 저장,
  서비스 학년 대응 미확정)이지만 2단계 원문이 "615건"이라는 숫자로 명시적으로 언급한 것은 L5뿐이므로,
  이번 보고서에서 L6 693건을 별도로 표기해 다음 단계가 두 그룹(L5 677건 + L6 693건 = 1,370건 전체)을
  모두 다뤄야 함을 명확히 한다.

### 5-3. 148건 기준집합과의 교집합

148건(150행) 매칭 집합 안에서 literacy_level 분포는 `{0: 62, 1: 51, 2: 37}`(2절에서 이미 집계) —
**level 5·6이 전혀 없다.** 따라서 **148건 기준집합 ∩ (62건 ∪ 615건 ∪ 693건) = 0건**이다. 이번
dry-run이 다루는 "연결 후보"와 L5/L6 재검수 대상은 현재로서는 서로 완전히 분리되어 있다 — 다음
단계에서 148건을 백필하더라도 L5/L6 정책 미확정 문제와는 부딪히지 않는다(단, 향후 매칭 로직이
schemareading L5/L6 3,382건까지 확장되면 즉시 부딪힌다).

### 5-4. `vocabulary_content_levels` L5/L6 재확인

```sql
SELECT vocab_level, COUNT(*) FROM vocabulary_content_levels GROUP BY vocab_level;
-- 0=612, 1=1635, 2=1621, 3=1825, 4=30  (5, 6은 결과 행 자체가 없음)
```
이번 세션에서 독립적으로 재실행한 결과 **L5·L6 = 0건**을 재확인했다(2단계 결과를 그대로 믿지
않고 이번에도 직접 SQL 실행). L5/L6 학년 라벨 자동 치환이나 기존 62건의 자동 재분류는 **수행하지
않았다.**

---

## 6. 두 원본 ZIP·raw XLSX 해시 재계산 (재조사 아님, 대조만)

이번 세션에서 SHA-256을 다시 계산해 2단계 값과 대조했다(파일이 그 사이 바뀌지 않았는지 확인 목적).

| 파일 | 이번 세션 계산값 | 2단계 보고서 값 | 일치 |
|---|---|---|---|
| `data/import/internal_vocab_upper_restore_v1.zip` | `ed83a8f9...4ccda389` | 동일 | ✅ |
| `data/import/스키마리딩 참고자료.zip` | `616abed0...e28dde26` | 동일 | ✅ |
| 학습 도구어 사전(0925최종).xlsx (raw / ivur zip / 참고자료 zip 3곳) | `17b40c6e...ac77a48` (3곳 동일) | 동일 | ✅ |
| 스키마 어휘 목록(레벨2~5).xlsx (raw / ivur zip / 참고자료 zip 3곳) | `b071bb64...f234b0f1ce85b` (3곳 동일) | 동일 | ✅ |
| `raw/schema-reading/어휘퀴즈DB.xlsx` | `63ed4d0f...49092581` | 동일 | ✅ |
| `스키마리딩 참고자료.zip`의 `어휘퀴즈DB (1).xlsx` | `78903d5b...11bd57d988` | 동일 | ✅ |

**raw/의 `어휘퀴즈DB.xlsx`와 참고자료 zip의 `어휘퀴즈DB (1).xlsx`는 이번에도 SHA-256이 서로
다르다** — 2단계 발견(버전 차이)을 그대로 재확인, 재조사하지 않았다.

### 6-1. "338개 옛 문항" 정의 — 이번 세션에 근거와 함께 확정

v3 문서 원문: "`스키마리딩 참고자료.zip`은 ... 학습 흐름, 전체 XLSX, **옛 퀴즈 338행** 등을
포함한다"(v3 확정 입력 절), "옛 퀴즈 338행은 별도 선택지 검증 후 활용한다"(v3 다음 실행 단계 6번).
2단계 0-1절이 이미 확인한 `어휘퀴즈DB.xlsx`(339행=헤더 1+데이터 338, 컬럼 `단어/뜻(정답)/보기1~4/
정답번호/난이도/출처(도서)/태그/대분류/중분류/소분류`)와 이번에 직접 연 파일의 실제 행 수(339,
`openpyxl`로 재확인)가 정확히 일치한다. **따라서 "338개 옛 문항" = `어휘퀴즈DB.xlsx`(raw/ 버전과
참고자료 zip 버전 두 종류 존재, SHA-256 다름)의 데이터 행 338건으로 정의를 확정한다.**

**이번 dry-run에서는 이 338건의 적재·평가를 범위에서 제외했다** — 위 148/150건 판정표에도, 6절
논의에도 338건은 포함되지 않는다. `raw/`와 참고자료 zip 중 어느 버전을 정본으로 쓸지는 다음
단계 전에 결정이 필요하다(2단계가 이미 남긴 막힌 지점, 이번에도 해소하지 않음).

---

## 7. 백업·체크섬·게이트 절차 설계 (제안만 — 무거운 백업/실제 적용은 하지 않음)

### 7-1. 이번 세션에 실제로 수행한 가벼운 읽기 전용 검증(제안이 아니라 실행 결과)

```
서버 vocabulary_quiz_research.db: PRAGMA integrity_check → ok
서버 vocabulary_quiz_research.db: PRAGMA foreign_key_check → 위반 0건
로컬 literacy.db: PRAGMA integrity_check → ok
로컬 literacy.db: PRAGMA foreign_key_check → 위반 0건

서버 테이블 행수 스냅샷(다음 단계 "무변경 확인"의 사전 기준값):
  vocabulary_contents=5723, vocabulary_content_levels=5723, vocabulary_items=5723,
  vocabulary_multiformat_items=1289
  SUM(student_exposure)=0, SUM(public_ready)=0  (전량 미노출/비공개 상태)
```

### 7-2. 다음 마이그레이션 단계 전 게이트 체크리스트 (제안, 이번 세션에서 미실행)

1. **백업**: `vocabulary_contents`/`vocabulary_content_levels` 두 테이블만 `sqlite3 .dump`로
   논리 백업(파일 전체 복사는 이번 단계 범위 밖 — 다음 실제 마이그레이션 직전에 수행). literacy.db도
   동일하게 `terms` 테이블 논리 백업.
2. **체크섬**: 마이그레이션 전후로 `SELECT content_id, lemma, pos, canonical_definition,
   student_definition FROM vocabulary_contents ORDER BY content_id`를 정렬 후 SHA-256 해시. 새
   컬럼(`literacy_term_id`) 추가·백필 후에도 **이 해시가 동일해야 함**(기존 컬럼 값 불변 증명).
3. **student_exposure/public_ready 무변경 확인**: 마이그레이션 전후 `SUM(student_exposure)`,
   `SUM(public_ready)`, 그리고 두 컬럼의 `(content_id, value)` 전체 목록 해시가 동일해야 함(이번
   세션 기준값: 둘 다 0 — 값이 0에서 다른 값으로 바뀌면 즉시 실패 처리).
4. **`integrity_check`/`foreign_key_check`**: 마이그레이션 직후 두 DB 모두 재실행, `ok`/위반 0건
   아니면 롤백.
5. **멱등성**: 백필 스크립트를 동일 입력으로 2회 실행 → `inserted=0/updated=0/unchanged=N`(2회차).
6. **교차 DB 참조 무결성**: 4-2절에서 설계한 `verify_literacy_link_integrity.py`를 마이그레이션
   직후 실행, dangling reference 0건 확인.
7. **62건/615건/693건(L5·L6) 제외 확인**: 백필 대상 목록에 literacy_level 또는 vocab_level이
   5·6인 행이 하나도 없어야 함(로그에 제외 건수 명시) — `--confirm-level-policy` 같은 명시적
   플래그 없이는 L5/L6 관련 코드 경로 자체가 실행되지 않도록 가드(2단계 6절 제안 유지).
8. **MULTIPLE_LINKS 처리 규칙 확정 여부**: 4-1절의 속수무책/혼비백산류(1:N) 처리 우선순위 규칙이
   문서화되지 않은 상태로는 백필을 진행하지 않는다(둘 중 하나를 임의로 골라 넣지 않음).
9. **338건(어휘퀴즈DB.xlsx) 범위 제외 확인**: 마이그레이션 스크립트가 이 파일을 참조하지 않는지
   코드 리뷰로 확인(6-1절 정의 기준).

---

## 8. 요약

**1) 148건의 의미 검증 상태별 건수** (전체 150행, 2절·3절)

| status | 건수 |
|---|---:|
| `APPROVABLE_CANDIDATE` | 142 |
| `MULTIPLE_LINKS` | 4 (headword 2개) |
| `AMBIGUOUS_SENSE` | 2 |
| `UNVERIFIED_DEFINITION` | 2 |
| `OLD_LEVEL_REVIEW` | 0 |
| `NO_MATCH` | 0(정의상 해당 없음) |

**2) 두 DB의 연결 기수성 증거** (4절): 관찰된 것은 `1:1`(146행) / `1:N`(4행, headword 2개 —
속수무책·혼비백산, 같은 headword가 momo-textbook·sajaseongeo-pdf 두 출처에 서로 다른 레벨로
존재) 뿐. `N:1`/`N:M`은 V lemma 중복이 0건이라 구조적으로 발생하지 않는다(단, 스키마에 lemma
UNIQUE 제약이 없어 향후 V에 중복 lemma가 생기면 발생 가능 — 방어 코드 필요).

**3) 62건·615건의 정의와 교집합/합집합** (5절): 62건=krdict(25)+사자성어(37), 전부 level=5,
AI 자동 레벨 부여. 615건=schemareading-schema(507)+schemareading-tooldict(108), 전부 level=5,
원본 "단계" 값 그대로 저장(AI 아님). 교집합 0, 합집합 677(=level=5 전체와 정확히 일치). 별도로
level=6 693건(전부 schemareading, 62/615 어느 쪽에도 포함 안 됨)이 남아 있다.

**4) L5/L6 정책이 실제 데이터·코드에 미치는 영향**: `vocabulary_content_levels`는 L5·L6=0건이라
관리자 퀴즈 기능에는 영향 없음(재확인 완료). `terms.level`에는 이미 1,370건(677+693)이 옛 기준으로
채워져 있고, 그중 62건은 "고1~2"를 구분 없이 판정해 새 기준(L5=고1 단독)으로 기계적 재분류 불가.
148건 기준집합과는 교집합 0이라 **이번 dry-run 자체는 이 문제와 충돌하지 않는다.**

**5) 단일 컬럼 vs 관계 테이블 선택 근거 및 반례** (4-1절): 146/148(98.6%)이 1:1이라 단일 nullable
`literacy_term_id` 컬럼이 기본안으로 타당하지만, **속수무책·혼비백산 2건은 실제 반례**다 — 같은
V content가 서로 다른 레벨을 가진 2개의 S term과 동시에 대응해, 단일 컬럼으로는 한쪽 정보가
유실된다. 다음 단계 전에 source 우선순위 규칙(어느 쪽을 남길지)을 확정해야 한다.

**6) 다음 마이그레이션 단계 게이트** (7-2절 9개 항목): 백업, 체크섬, exposure/public_ready
무변경, integrity/FK 재검사, 멱등성, 교차 DB 참조 무결성, 62/615/693건 제외 확인,
MULTIPLE_LINKS 처리 규칙 확정, 338건 범위 제외 확인.

---

## 산출물

- 분석기 스크립트: `scripts/vocab/analyze_schema_reading_link_dryrun.py` (읽기 전용, 148/150건
  기준집합 재현 + 힌트 생성, 2회 실행 결과 동일 해시로 멱등성 확인됨)
- 판정 스크립트: `scripts/vocab/build_link_dryrun_verdicts.py` (규칙 분류 + 수동 판단 오버레이,
  회색지대 자동 승인 방지 구조)
- 판정표: `data/import/schema_reading_link_dryrun_verdicts_20260924.csv`,
  `data/import/schema_reading_link_dryrun_verdicts_20260924.jsonl` (150행, 상태·근거 포함)
- 본 보고서: `reports/schema_reading_phase3_dryrun_20260924.md`

## 주요 리스크·막힌 지점 (최상위 요약)

1. **v4 문서 누락** — 지시받은 이름/키워드로 저장소 전체·두 zip 내부를 찾았으나 없음. v3 +
   phase2만으로 진행했다(0절).
2. **1:N 반례 확정** — 속수무책·혼비백산 2건은 단일 컬럼 설계의 예외이며, 처리 우선순위 규칙이
   아직 없다(4-1절). 규칙 없이 백필하면 안 됨.
3. **62건/615건/693건 재검수 부담** — 특히 693건(level=6)은 2단계 원문의 "615건"이라는 표현에
   포함되지 않아 다음 단계 계획에서 누락되기 쉽다(5-1·5-2절).
4. **어휘퀴즈DB.xlsx 버전 미확정** — raw/ 버전과 참고자료 zip 버전(둘 다 338행이지만 SHA-256
   다름) 중 정본을 아직 정하지 않았다(6절). 338건은 이번 dry-run 범위에서 제외했다.
5. **AMBIGUOUS_SENSE/UNVERIFIED_DEFINITION 8건** — 시샘·평론(의미 범위 재확인 필요), 능가하다·
   설상가상(원문 텍스트 손상 의심) — 사람 재검수 전까지 백필 대상에서 제외 권고.
