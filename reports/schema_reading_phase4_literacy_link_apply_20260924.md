# 스키마리딩x어휘 DB 통합 — 4단계 연구용 비공개 링크 적용 보고서

- 작성일: 2026-09-24 (`date` 명령으로 시스템 현재 날짜 직접 확인, 서버 UTC 기준으로는 2026-09-23 심야)
- 범위: 3단계(`reports/schema_reading_phase3_dryrun_20260924.md`, 읽기 전용 dry-run)에
  이어, **research 환경에서만** 실제 쓰기(신규 관계 테이블 생성 + 142건 링크 행 삽입)를
  수행했다. 3단계가 확립한 사실(SHA-256, 스키마 구조, 148/150건 판정 근거 등)은
  재조사하지 않고 그대로 인용·재사용했다.
- **운영/학생 공개 관련 변경 없음**: `student_exposure`/`public_ready` 어느 쪽도 손대지
  않았고, 마이그레이션 전후 양쪽 합계 모두 0으로 동일하다(6절). git commit/push 없음.

---

## 0. 인프라 게이트 1 — APP_ENV=research 재확인

이전 세션 결과를 신뢰하지 않고 이번 세션에서 직접 재확인했다.

- **vocabulary_quiz(V) 측**: `app/vocabulary_quiz/db.py`는 `VOCABULARY_QUIZ_DB_PATH`
  환경변수로 경로를 결정한다. 서버에서 `.env`와 **실제 실행 중인 프로세스**(PID
  1100600, `uvicorn app.main:app --port 8000`)의 `/proc/<PID>/environ`을 각각
  확인했고 byte-for-byte 일치했다:
  ```
  APP_ENV=research
  VOCABULARY_QUIZ_DB_PATH=/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
  ```
  → **research 환경의 실제 대상 DB는 서버의 `vocabulary_quiz_research.db`다.**
- **literacy(S) 측**: `app/literacy/db.py`를 직접 읽어 확인 — 환경변수 분기가
  전혀 없고 **항상 로컬 `REPO_ROOT/data/literacy.db`를 고정 경로로 쓴다**
  (`APP_ENV`와 무관). 로컬 `data/vocab/vocabulary_quiz_rnd.db`는 이번 조사와
  무관한, 이름이 다른 별개 DB임을 재확인했다(이번 세션에서 열지 않았다).

---

## 1. 142건 재현 (2단계 요구사항 1)

3단계 판정표(`data/import/schema_reading_link_dryrun_verdicts_20260924.csv`, 150행)에서
`status=APPROVABLE_CANDIDATE`인 행만 추출했다.

```
총 150행: APPROVABLE_CANDIDATE 142 / MULTIPLE_LINKS 4 / AMBIGUOUS_SENSE 2 / UNVERIFIED_DEFINITION 2
142건: vocab_content_id 중복 0건, literacy_term_id 중복 0건 (구조적 1:1 재확인)
literacy.db에서 142개 literacy_term_id 전부 재조회 성공 (mode=ro)
```

재현 스크립트: `scripts/vocab/build_literacy_link_targets.py` (읽기 전용).
출력: `data/import/schema_reading_link_142_targets_20260924.json`
(headword, vocab_content_id, literacy_term_id, literacy_source, literacy_headword,
evidence 등 포함).

---

## 2. 재검사 — "APPROVABLE_CANDIDATE" 라벨을 믿지 않고 직접 재대조 (요구사항 2)

### 2-1. AI 자동 보강 정의 겹침 재검증

2단계 보고서 2-3절이 밝힌 "2026-09-01 23:47:34 AI 자동 생성 뜻풀이" 배치(709건 —
schemareading-schema 697건 + schemareading-tooldict 12건, `note` LIKE
`%AI 자동 생성 뜻풀이%`)와 142건의 literacy_term_id를 literacy.db에 직접 재조회해
대조했다.

```
142건의 literacy_source 분포(DB 직접 재조회): {'momo-textbook': 138, 'sajaseongeo-pdf': 4}
AI 자동 생성 뜻풀이(2026-09-01 23:47:34) 배치와 겹치는 건수: 0건
```

**142건 전부가 momo-textbook/sajaseongeo-pdf 출처이고, AI 자동 보강 배치(schemareading
전용)와 아예 겹치지 않는다** — 이 배치가 우려하는 리스크(AI가 생성한 정의를 사람이
승인한 canonical 정의처럼 취급하는 것)는 이번 142건에는 해당하지 않는다.

### 2-2. 새로 발견한 리스크 — 문서화되지 않은 두 번째 AI 레벨 배치

위 재검증 도중, 142건 중 4건(개과천선/고진감래/일석이조/일편단심, 전부
sajaseongeo-pdf)의 `note`에 `[AI 자동 레벨 부여: N]`(N=1 또는 2)이 있고
`reviewed_at='2026-09-02 01:52:50'`임을 발견했다. 이는 2·3단계 보고서가 문서화한
"62건 AI 자동 레벨 부여" 배치(`reviewed_at='2026-09-01 23:46:45'`, 전부 level=5)와
**시각도 다르고 레벨 값도 다른 별개의 배치**다 — 2·3단계의 `WHERE level IN (5,6)`
쿼리로는 발견되지 않았던 것(이 4건은 level 1·2라서 걸리지 않음).

- **이 배치는 definition이 아니라 level(과 그 근거 설명)만 건드렸다** — 4건의
  literacy.db definition은 전부 V definition과 **문자열 완전 동일**이었다(3단계
  판정표에도 이미 "완전히 동일"로 기록됨). V definition은 literacy.db와 독립적인
  vocabulary_quiz 파이프라인 산출물이므로, 이 우연한 완전 일치는 이 4건의
  definition이 AI가 지어낸 것이 아니라 원본 그대로임을 뒷받침하는 강한 방증이다.
- 따라서 이 4건은 **정의 신뢰성 문제가 아니므로 링크 대상에서 제외하지 않았다**
  (링크 테이블은 level을 다루지 않는다).
- 다만 이 두 번째 배치의 존재 자체는 **62건/615건/693건 집계에 아직 반영되지 않은
  미문서화 리스크**다 — level 정책(L5/L6) 재검수 범위를 다음 단계에서 넓혀야 할
  수 있다. 이번 세션은 이 발견을 기록만 하고 62/615/693/338건 집합 정의는 전혀
  바꾸지 않았다(사용자가 이미 확정한 숫자를 재논의하지 않는다는 지시를 지켰다).

### 2-3. 그 밖의 구조적 재검사

| 항목 | 결과 |
|---|---:|
| vocab_content_id 중복 | 0건 |
| literacy_term_id 중복 | 0건 |
| vocab_pos ≠ literacy_pos (양쪽 다 비어있지 않은데 다름) | 0건 |
| literacy_level ∈ {5,6} | 0건 (분포: {0:61, 1:48, 2:33}) |
| vocab_current_level ∈ {5,6} | 0건 |
| CSV의 literacy_source/literacy_definition과 literacy.db 직접 재조회 값 불일치 | 0건 |

### 2-4. 의미 대조 재확인 (표본 재독)

142건 전체를 다시 훑어 V/S 정의 쌍을 직접 대조했다(3단계가 이미 판단한 결과에 대한
독립적 재확인). 특히 문자열 유사도가 낮게 나온 건들(예: 부축하다 0.25, 빼곡하다
0.32, 샘솟다 0.34, 연거푸 0.31)도 다시 읽었고, 전부 사전적 패러프레이즈 범위 안의
동의로 판단했다(예: 부축하다 — V "겨드랑이를 붙잡아 걷는 것을 돕다" vs S "다른
사람이 몸을 움직이는 것을 곁에서 도와주다": 후자가 더 일반화된 표현이지만 핵심
행위(신체 이동을 곁에서 도움)가 일치). 이번 재대조로 **기각한 건은 없다.**

### 2-5. 결론

**142건 전부가 재검사를 통과했다. 재검사로 걸러낸 건은 0건이다.** 최종 연구용
비공개 링크 대상 수 = **142건**.

---

## 3. 이번 apply에서 명시적으로 제외한 것 (요구사항 3, 적용 후 재검증 포함)

| 제외 대상 | 건수 | 이번 apply 후 재검증 결과 |
|---|---:|---|
| MULTIPLE_LINKS(속수무책·혼비백산, 1:N) | 4행(headword 2개) | `vocabulary_content_literacy_links`에서 두 content_id(`SC_V1976_B076_042`, `SC_V19134_B134_003`)로 조회 → **0행** (링크되지 않음 확인) |
| 의미 보류 4건(시샘·평론·능가하다·설상가상) | 4행 | 4개 content_id로 조회 → **0행** (링크되지 않음 확인) |
| 옛 레벨 정책 62건(krdict 25+sajaseongeo-pdf 37, 전부 level=5, 2026-09-01 23:46:45 AI 레벨 부여) | 62건 | 애초에 148/150건 기준집합과 교집합 0(3단계 5-3절 재확인 사실 재사용) — 이번 142건의 literacy_term_id 중 이 62건에 속하는 것 0건(직접 재조회로 재확인) |
| 원본 L5 615건(schemareading-schema 507+tooldict 108) | 615건 | 위와 동일 근거로 교집합 0 — 142건의 literacy_source에 schemareading-schema/tooldict가 아예 없음(직접 재조회로 재확인: momo-textbook 138 + sajaseongeo-pdf 4) |
| L6 693건(schemareading-schema 608+tooldict 85) | 693건 | 위와 동일 근거로 교집합 0 |
| 옛 문항 338건(`어휘퀴즈DB.xlsx`) | 338건 | 마이그레이션 스크립트 5개(`build_literacy_link_targets.py`/`vq_research_backup.py`/`migrate_vocabulary_quiz_add_literacy_links.py`/`import_literacy_links.py`/`verify_literacy_link_integrity.py`) 전부 코드 리뷰 — `어휘퀴즈DB`, `quiz_items`, `vocabulary_items` 등 옛 문항 관련 테이블/파일을 참조하는 코드가 전혀 없음을 확인 |

**모든 제외 목록에 대해 연결·레벨 어느 쪽도 변경하지 않았다.**

---

## 4. 1:N 반례(속수무책·혼비백산) — 신규 관계 테이블에서의 표현

### 4-1. 기존 스키마 재확인(신규 테이블 설계 전 필수 확인)

`vocabulary_quiz_research.db`의 전체 테이블 목록과 스키마(`vq_schema.sql`, 이번
세션에 `.schema`로 재확인)를 검토한 결과, **`vocabulary_contents`를 literacy.db
term에 연결하는 기존 다대다 관계/참조 테이블은 없다**(`vocabulary_multiformat_items`의
`source_content_ids_json`은 콘텐츠↔퀴즈문항 관계이지 literacy 연결이 아님). 별도로
`app/vocab/`(idiom DB, `scripts/vocab/seed_from_literacy.py`)도 확인했으나 이는
완전히 다른 모듈(사자성어 idiom 테이블)이라 이번 케이스와 무관하다. → **동등 기능이
없으므로 신규 테이블 설계로 진행.**

### 4-2. 신규 테이블 스키마

```sql
CREATE TABLE IF NOT EXISTS vocabulary_content_literacy_links (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id         TEXT NOT NULL REFERENCES vocabulary_contents(content_id),
    literacy_term_id   INTEGER NOT NULL,   -- literacy.db(별도 파일) terms.id, 교차 DB 참조
    literacy_source    TEXT NOT NULL,      -- 연결 시점 스냅샷(drift 감지용)
    literacy_headword  TEXT NOT NULL,      -- 연결 시점 스냅샷(drift 감지용)
    link_status        TEXT NOT NULL DEFAULT 'CANDIDATE'
                        CHECK (link_status IN ('CANDIDATE','APPROVED','REJECTED')),
    link_method        TEXT NOT NULL,
    evidence           TEXT,
    created_at         TEXT DEFAULT (datetime('now')),
    updated_at         TEXT DEFAULT (datetime('now')),
    UNIQUE (content_id, literacy_term_id)
);
CREATE INDEX IF NOT EXISTS idx_vcll_content_id ON vocabulary_content_literacy_links(content_id);
CREATE INDEX IF NOT EXISTS idx_vcll_literacy_term_id ON vocabulary_content_literacy_links(literacy_term_id);
```

`student_exposure`/`public_ready` 계열 컬럼이 전혀 없다 — 이 테이블은 어떤
학생 노출 경로에도 연결돼 있지 않다(앱 코드가 아직 참조하지 않으며, 이번
세션에서 참조 코드를 추가하지도 않았다).

`UNIQUE(content_id, literacy_term_id)`는 **복합키**다(`content_id` 단독 UNIQUE가
아님) — 즉 같은 `content_id`가 서로 다른 `literacy_term_id` 여러 개와 동시에
연결될 수 있어 1:N을 구조적으로 지원한다.

### 4-3. "두 출처 다 보존" 증명 — 단, 실제 삽입 없이

사용자 지시상 MULTIPLE_LINKS 4행은 **이번 apply에서 연결하지 않는다**(3절). 따라서
"실제 행으로 증명"은 **연구 DB에 실제로 커밋하지 않는 읽기 전용 방식**으로
수행했다(`scripts/vocab/prove_multiple_links_schema_capability.py`, INSERT 문
자체가 코드에 없고 `PRAGMA query_only=ON`으로 쓰기 원천 차단):

```
인덱스 목록에서 sqlite_autoindex_vocabulary_content_literacy_links_1:
  unique=1 columns=['content_id', 'literacy_term_id']
→ UNIQUE 제약이 (content_id, literacy_term_id) 복합키임 확인: True

속수무책 SC_V1976_B076_042 -> 3185(momo-textbook): 현재 존재=0
속수무책 SC_V1976_B076_042 -> 7201(sajaseongeo-pdf): 현재 존재=0
혼비백산 SC_V19134_B134_003 -> 3015(momo-textbook): 현재 존재=0
혼비백산 SC_V19134_B134_003 -> 7306(sajaseongeo-pdf): 현재 존재=0

content_id=SC_V1976_B076_042: literacy_term_id 후보 [3185, 7201] (서로 다른 2개)
  → UNIQUE(content_id, literacy_term_id)는 content_id가 같아도 literacy_term_id가
    다르면 별도 행으로 허용 → 이 스키마는 1:N을 구조적으로 지원(실제 삽입 안 함)
content_id=SC_V19134_B134_003: literacy_term_id 후보 [3015, 7306] (서로 다른 2개)
  → 동일 결론
```

**결론**: 스키마는 momo-textbook·sajaseongeo-pdf 두 출처를 별도 행으로 동시에
보존할 수 있음을 제약 정의(PRAGMA index_list)로 증명했다. 다만 사용자 지시에
따라 **이번 세션에서는 이 4행을 실제로 삽입하지 않았다** — 두 정의(공백 하나
차이) 중 어느 쪽을 우선할지, 또는 둘 다 남길지에 대한 source 우선순위 규칙이
아직 확정되지 않았기 때문이다(3단계 4-1절이 이미 지적한 미해결 지점, 이번
세션도 확정하지 않음).

---

## 5. 백업 · 무결성 · 복원 확인 (인프라 게이트 2·3)

### 5-1. 백업

- 스크립트: `scripts/vocab/vq_research_backup.py` (SQLite Backup API,
  `sqlite3.Connection.backup()` 사용)
- 백업 파일: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase4-pre-migration-20260923-214414`
  (서버, 12,124,160 bytes)
- 백업 파일 SHA-256: `3effa4cd76f45376fd8801f58e3e7f44595826aa1b94a8c1035cff3e4f917cff`

### 5-2. 백업 무결성·복원 가능성 검증 (실제 수행 결과)

```
원본 행수: {vocabulary_contents: 5723, vocabulary_content_levels: 5723}
백업 행수: {vocabulary_contents: 5723, vocabulary_content_levels: 5723}   → 일치
백업 PRAGMA integrity_check → ok
백업 PRAGMA foreign_key_check → 위반 0건
```

백업 파일을 원본과 별도로 열어(`mode=ro`) 직접 확인했다 — "복사됐다고 가정"하지
않고 실제로 다시 읽어 검증했다.

### 5-3. 롤백(복구) 절차

이번 세션은 **모든 게이트를 통과**했으므로 롤백을 실행하지 않았다. 만약 이후
문제가 발견되면:

```bash
ssh aprolabs
cp /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.rollback-before-restore-$(date +%Y%m%d-%H%M%S)
cp /home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase4-pre-migration-20260923-214414 \
   /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db
sqlite3 /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db "PRAGMA integrity_check;"
```
(복구 전 현재 상태도 한 번 더 백업해 두는 절차를 포함시켰다.) 이 백업 파일 하나로
`vocabulary_content_literacy_links` 테이블 생성과 142건 삽입 이전 상태로 완전히
되돌릴 수 있다(이 신규 테이블 외에는 아무것도 바꾸지 않았으므로).

---

## 6. 체크섬 · 기존 데이터 불변 확인 (인프라 게이트 4·7)

방법: 3단계 보고서 7-2절이 제안한 방식(정렬된 PK 기준 SHA-256) 그대로 재사용
(`scripts/vocab/vq_checksum_core_tables.py`).

| 항목 | 마이그레이션 전 | 마이그레이션 후 | 일치 |
|---|---|---|---|
| `vocabulary_contents` 행수 | 5723 | 5723 | ✅ |
| `vocabulary_contents` 체크섬(content_id/lemma/pos/canonical_definition/student_definition) | `117ad373...4cc926b` | `117ad373...4cc926b` | ✅ **동일** |
| `vocabulary_content_levels` 행수 | 5723 | 5723 | ✅ |
| `vocabulary_content_levels` 체크섬 | `6a0c97cc...b0fc6f30` | `6a0c97cc...b0fc6f30` | ✅ **동일** |
| `(content_id, student_exposure, public_ready)` 체크섬 | `6d24c34e...b408e20b` | `6d24c34e...b408e20b` | ✅ **동일** |
| `SUM(student_exposure)` | 0 | 0 | ✅ |
| `SUM(public_ready)` | 0 | 0 | ✅ |
| `PRAGMA integrity_check` | ok | ok | ✅ |
| `PRAGMA foreign_key_check` 위반 | 0 | 0 | ✅ |
| `vocabulary_content_literacy_links` 존재 | 없음 | 있음, 142행 | (신규 테이블 — 기존 데이터 아님) |

**기존 행은 단 하나도 바뀌지 않았다**(체크섬 완전 동일) — 신규 테이블에 142행이
추가된 것 외에는 DB에 아무 변화가 없다. 스냅샷 원본:
`data/import/schema_reading_link_apply_checksum_before_20260924.json`,
`data/import/schema_reading_link_apply_checksum_after_20260924.json`.

---

## 7. 교차 DB 참조 무결성 (인프라 게이트 7-1)

서버에서 `vocabulary_content_literacy_links` 142행을 SELECT 전용으로 내보내
(`data/import/schema_reading_link_142_applied_snapshot_20260924.json`) 로컬로
가져온 뒤, `scripts/vocab/verify_literacy_link_integrity.py`로 로컬
`data/literacy.db`(mode=ro)와 대조했다.

```
검증 대상 링크 행수: 142
OK(스냅샷과 현재 literacy.db 일치): 142
DANGLING(literacy.db에 존재하지 않음): 0
DRIFTED(headword/source가 스냅샷과 다름): 0
최종: PASS
```

142건 전부 literacy.db에 실제로 존재하고, 연결 시점에 스냅샷한
headword/source와 현재 값이 완전히 같다(세션 도중 literacy.db가 바뀌지
않았다는 방증이기도 하다).

---

## 8. 멱등성 (인프라 게이트 7-5)

`import_literacy_links.py --apply`를 동일 입력으로 2회 실행했다.

```
1회차: 신규 삽입 142건, 스킵(이미 존재) 0건
2회차: 신규 삽입 0건, 스킵(이미 존재) 142건
```

재실행해도 추가 변경이 없다 — 멱등.

---

## 9. 게이트별 pass/fail 요약

| 게이트 | 결과 |
|---|---|
| 1. APP_ENV=research 재확인(서버 `.env` + 실행 중 프로세스 environ 일치) | **PASS** |
| 2. SQLite Backup API 백업 생성 | **PASS** |
| 3. 백업 무결성(integrity_check ok, FK 위반 0)·행수 일치(원본=백업) | **PASS** |
| 4. 마이그레이션 전 체크섬 스냅샷 저장 | **PASS**(수행 완료, 6절) |
| 5. dry-run 결과가 최종 링크 대상 집합(142건)과 정확히 일치 | **PASS**(스킵 0, 누락 0) |
| 6. apply는 research 환경에서만(하드 가드로 db-path basename·APP_ENV 이중 확인) | **PASS** |
| 7-a. 교차 DB 참조 유효성(dangling 0, drift 0) | **PASS** |
| 7-b. `PRAGMA integrity_check` (apply 후) | **PASS**(ok) |
| 7-c. `PRAGMA foreign_key_check` (apply 후) | **PASS**(위반 0) |
| 7-d. 기존 데이터 불변(체크섬 전후 동일) | **PASS** |
| 7-e. `student_exposure`/`public_ready` 합계 여전히 0 | **PASS** |
| 7-f. 재실행 멱등성(2회차 삽입 0) | **PASS** |
| 제외 목록(62/615/693/338/MULTIPLE_LINKS 4/의미보류 4) 미변경 재검증 | **PASS**(3절) |

**하나도 fail하지 않았다 — 롤백을 실행하지 않았다.** (5-3절에 절차만 대기용으로
기록해 둠.)

---

## 10. 최종 요약

- **실제 링크 건수: 142건** (신규 테이블 `vocabulary_content_literacy_links`에
  `link_status='CANDIDATE'`로 삽입, research 서버 DB에만 적용)
- **보류 건수: 8건**
  - MULTIPLE_LINKS 4행(속수무책·혼비백산, 두 출처 동시 대응) — source 우선순위
    규칙 미확정으로 보류. 스키마는 1:N을 지원함을 증명했으나 실제 삽입은 다음
    단계 결정 후로 미룸.
  - 의미 보류 4건(시샘·평론·능가하다·설상가상) — 재검토 카드 참고
    (`reports/schema_reading_review_cards/`).
- **제외(연결·레벨 미변경, 적용 후 재검증 통과)**: 옛 레벨 정책 62건, 원본 L5
  615건, L6 693건, 옛 문항 338건.
- **재검사 결과**: 142건 전부 AI 자동 보강 정의(2026-09-01 배치)와 무관함을
  literacy.db 직접 재조회로 확인, 142건 전부 재검사 통과(기각 0건). 재검사
  과정에서 미문서화 리스크(2026-09-02 두 번째 AI 레벨 배치) 1건을 새로 발견해
  기록했다(2-2절) — 이번 apply의 정확성에는 영향 없음.
- **백업**: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase4-pre-migration-20260923-214414`
  (SHA-256 `3effa4cd76f45376fd8801f58e3e7f44595826aa1b94a8c1035cff3e4f917cff`), 무결성/복원 검증 PASS.
- **게이트**: 전부 PASS, 중단/롤백 없음.
- **가장 중요한 리스크**: (1) MULTIPLE_LINKS 4행의 source 우선순위 규칙이 아직
  없어 속수무책·혼비백산은 여전히 링크 불가 상태다. (2) 2026-09-02 두 번째 AI
  레벨 배치(미문서화)가 62/615/693건 집계 밖에 존재할 수 있어, 다음 L5/L6 정책
  작업 전 전수 스캔이 필요하다. (3) 시샘/평론/능가하다/설상가상 4건은 원본
  데이터 정제(구두점 복원, 메타데이터 제거) 또는 국어 상식 확인이 선행돼야
  링크 가능하다.

---

## 산출물

### 로컬 (`C:\Users\aproa\aprolabs`, 커밋하지 않음)

- 스크립트:
  - `scripts/vocab/build_literacy_link_targets.py` (142건 재현 + AI 보강 재검증, 읽기 전용)
  - `scripts/vocab/vq_research_backup.py` (SQLite Backup API 백업 + 검증, 서버 실행용)
  - `scripts/vocab/vq_checksum_core_tables.py` (체크섬 스냅샷, 서버 실행용)
  - `scripts/vocab/migrate_vocabulary_quiz_add_literacy_links.py` (신규 테이블 DDL, 서버 실행용)
  - `scripts/vocab/import_literacy_links.py` (142건 임포터, dry-run/apply, 서버 실행용)
  - `scripts/vocab/verify_literacy_link_integrity.py` (교차 DB 참조 검증, 로컬 실행용)
  - `scripts/vocab/prove_multiple_links_schema_capability.py` (1:N 스키마 증명, 실삽입 없음, 서버 실행용)
- 데이터:
  - `data/import/schema_reading_link_142_targets_20260924.json`
  - `data/import/schema_reading_link_142_reverify_20260924.jsonl`
  - `data/import/schema_reading_link_142_applied_snapshot_20260924.json`
  - `data/import/schema_reading_link_MULTIPLE_LINKS_schema_proof_20260924.json`
  - `data/import/schema_reading_link_apply_checksum_before_20260924.json`
  - `data/import/schema_reading_link_apply_checksum_after_20260924.json`
- 재검토 카드: `reports/schema_reading_review_cards/{시샘,평론,능가하다,설상가상}_20260924.md`
- 본 보고서: `reports/schema_reading_phase4_literacy_link_apply_20260924.md`

### 서버(`aprolabs`, research)

- 백업: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-phase4-pre-migration-20260923-214414`
- 작업 스크립트/체크섬 사본: `~/scratch/phase4_literacy_link/` (스크립트 6개 + JSON 산출물)
- 실제 변경: `vocabulary_quiz_research.db`에 `vocabulary_content_literacy_links`
  테이블 신규 생성(142행) — 그 외 기존 테이블/행 변경 없음(6절 체크섬으로 증명)
