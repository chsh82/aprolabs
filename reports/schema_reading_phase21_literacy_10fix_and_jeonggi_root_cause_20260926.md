# 스키마리딩x어휘 DB 통합 — 21단계: 서버 literacy.db 10건 사자성어 날짜오염 수정 + "정기" 문항-콘텐츠 불일치 원인 조사

- 작성일: 2026-09-26 (`date` 명령으로 시스템 현재 날짜 확인)
- 범위: (1) 로컬-서버 `literacy.db` 스키마·10건 행별 비교 + 전체 불일치 범위 확인,
  (2) 서버 `literacy.db` 백업 + dry-run, (3) 서버에 10건만 단일 트랜잭션 적용 +
  검증, (5) "정기"(SR_L4CORE_4786) 문항-콘텐츠 불일치 원인 조사 + 별도 dry-run
  (DB 쓰기 없음). 작업 4는 이번 지시에 포함되지 않았다(번호 체계는 상위 지시문의
  것을 그대로 따름).
- 재조사하지 않고 그대로 인용한 사실: `reports/schema_reading_phase7_literacy_repr_error_remediation_20260924.md`
  (로컬 10건 적용 절차·게이트·백업 SHA-256), `scripts/literacy/sajaseongeo_parser.py`
  (수정된 `_DATE_TOKEN`/`_DATE_LEAD_RE`), `scripts/literacy/apply_sajaseongeo_datefix_10.py`
  (로컬 적용 스크립트), `reports/schema_reading_phase16_quiz_pilot_dryrun_20260925.md`/
  `phase17_quiz_pilot_semantic_review_20260926.md`/`phase20_pilot_audit_and_scope_fix_20260926.md`
  (파일럿 40문항, "정기" HOLD 2건 판정과 그 근거).
- **절대 제약 준수**: `vocabulary_quiz_research.db`(`vocabulary_contents` 등)에는
  이번 세션 내내 어떤 쿼리도 실행하지 않았다(5절은 phase16/17/20이 이미 기록해
  둔 값을 인용만 함). `literacy.db` 파일 전체를 복사해 로컬↔서버를 동기화하지
  않았다(10건 정밀 UPDATE만). git commit/push는 하지 않았다.

---

## 0. 요약 (호출한 에이전트용 — 6절에도 동일 내용을 더 자세히 반복)

1. **로컬-서버 literacy.db 스키마는 완전히 동일**(21개 `sqlite_master` 항목,
   byte-identical). `terms` 테이블 컬럼 구조도 19개 컬럼 완전 일치.
2. **10건 외 추가 불일치는 없음** — `terms` 테이블 전체 7,312행 × 19개 컬럼을
   전수 비교한 결과, 로컬-서버 간 차이가 나는 행은 정확히 그 10개 id
   (7148,7188,7191,7192,7197,7223,7236,7243,7266,7288)뿐이었고, 그 10건도
   오직 `definition` 컬럼만 달랐다(다른 18개 컬럼은 10건 포함 전체 7,312행이
   전부 동일).
3. **서버 백업 완료**: `/home/chsh82/aprolabs/data/backups/literacy.db.bak-sajaseongeo-datefix-server-pre-20260926-093229`
   (SHA-256 `8c7d29b7c2648193df0abdb337a7e4fc8e465ddd22df3f443f37372eeed74de2`,
   4,251,648 bytes, `PRAGMA integrity_check`=ok, `foreign_key_check` 위반 0).
4. **서버 10건 적용 완료, 게이트 전부 PASS** — 하드가드(정확히 10건 id 일치)/
   교차검증(서버 원문에서 날짜 제거 시 로컬 계산값과 정확히 일치)/백업 무결성/
   UPDATE 후 10건 값 일치 10/10/체크섬 불변/나머지 215건 explicit 비교 동일/
   `integrity_check` ok/`foreign_key_check` 위반 0/멱등성(재실행 0건). 적용 후
   로컬-서버 `terms` 테이블을 다시 전수 비교한 결과 **7,312행 전부 byte-identical**
   (10건 포함, 완전히 수렴).
5. **"정기"(SR_L4CORE_4786) 불일치 원인**: phase16 문항 생성 스크립트가
   `vocabulary_contents`를 매번 다시 읽지 않고 스크립트 내부에 하드코딩된 정의
   문자열을 쓴 것이 근본 원인. phase17이 이 하드코딩 값을 원천(literacy.db)에
   맞게 한 차례 수정했으나(스크립트/문항 텍스트 쪽만), `vocabulary_contents.student_definition`
   자체를 갱신하는 코드 경로는 애초에 존재하지 않아 구버전 문구가 그대로
   남았다. **"기한이나 기간이 일정하게 정해져 있는 것"(문항 쪽, 원천에 더 가까움)이
   맞고, "일정한 기간마다 되풀이하도록 정한 것"(콘텐츠 쪽, 구버전)이 틀렸다.**
   수정안을 별도 dry-run 파일로 제출(DB 쓰기 없음, `vocabulary_contents` 미접근).

---

## 1. 로컬-서버 literacy.db 스키마·10건 행별 비교 + 전체 불일치 범위 확인

### 1-1. 스키마 비교

두 DB의 `sqlite_master`(type, name, sql) 전체를 `mode=ro`로 재조회해 비교했다
(테이블 5개: `terms`, `examples`, `hanja`, `term_hanja`, `quiz_items`,
`collection_runs` + 관련 인덱스/자동인덱스, 총 21개 항목):

```
로컬 sqlite_master 항목 수: 21
서버 sqlite_master 항목 수: 21
두 리스트를 파이썬 리스트 비교(==) 결과: 완전 동일(True)
```

**결론: 스키마는 테이블·인덱스·제약조건(FK, UNIQUE 등)까지 완전히 동일하다.**
`terms` 테이블은 양쪽 다 19개 컬럼(`id, category, headword, origin, definition,
pos, sense_category, subject_category, grade_level, grade_source, source,
license, external_id, collected_at, updated_at, review_status, note, level,
reviewed_at`)으로 일치했다.

### 1-2. 대상 10건 행별 재대조

두 DB에서 10개 id를 재조회해 `headword`/`definition`을 대조했다. **서버는
전부 phase7의 "이전(오염)" 값과 정확히 일치**(예: id=7148 `동병상련` 서버
`definition` 선두에 `1994학년도 1차 수능 `가 그대로 남아 있음), **로컬은
phase7이 이미 적용한 "이후(정상)" 값과 일치**(선두 날짜 접두어 없음) — 사전에
알려진 "서버는 10건 전부 오염 상태"가 재확인됐다.

### 1-3. 전체 불일치 범위 확인 (핵심 — 10건 외 추가 오염 여부)

로컬·서버 `terms` 테이블 전체(각 7,312행 × 19개 컬럼)를 id 기준으로 순회하며
모든 컬럼 값을 전수 비교했다(`data/import/schema_reading_phase21_literacy10_terms_full_diff_pre_20260926.json`에
비교 원본 저장):

```
로컬 행수 7,312 / 서버 행수 7,312 (id 집합 완전 일치, 양쪽에만 있는 id 0건)
값이 하나라도 다른 행: 정확히 10건
그 10건의 id 집합: {7148,7188,7191,7192,7197,7223,7236,7243,7266,7288} — 지시받은 목록과 정확히 일치
그 10건에서 달라진 컬럼: 오직 'definition' 하나뿐(나머지 18개 컬럼은 이 10건도 동일)
10건을 제외한 나머지 7,302건: 19개 컬럼 전부 byte-identical
```

**결론: 10건 외 추가 불일치는 없다.** phase7 적용(2026-09-24) 이후 로컬에서만
일어난 다른 작업(예: phase13~20의 momo_book.db/vocabulary_quiz_research.db
관련 작업들)이 `literacy.db`의 `terms` 테이블 자체에는 전혀 흔적을 남기지
않았음을 이번 전수 비교로 확인했다 — 우려했던 "로컬에서만 조용히 더 벌어진
drift"는 존재하지 않는다.

---

## 2. 서버 literacy.db 백업 + dry-run

### 2-1. 재사용 방식과 불가피한 차이점

`apply_sajaseongeo_datefix_10.py`(로컬 phase7 스크립트)를 서버 대상으로
그대로 재사용하려 했으나, 서버에는 `fitz`(PyMuPDF)와 `raw/sajaseongeo/` 원본
PDF가 **둘 다 없음**을 실측 확인했다(`python3 -c "import fitz"` → `ModuleNotFoundError`,
`ls raw/sajaseongeo/` → `No such file or directory`). 따라서 `merge_sources()`를
서버에서 직접 실행할 수 없어, 다음과 같이 최소한만 다르게 만든
`scripts/literacy/apply_sajaseongeo_datefix_10_server.py`를 신규 작성했다:

- 새 definition 10건 값은 **로컬에서 이미 `merge_sources()`로 재계산**해
  `scripts/literacy/sajaseongeo_datefix_10_new_defs.json`에 담아 서버로 전달.
  로컬 재계산 결과 10건 전부 로컬 DB의 현재(이미 수정된) 값과 정확히 일치함을
  먼저 확인했다(`current_local_def == computed_new_def`, 10/10 True).
- 다만 이 값을 서버 스크립트가 **맹목적으로 신뢰하지 않도록**, GATE 0b를
  추가했다: 서버에 실제 저장된 오염 definition에서 날짜 리드(`_DATE_LEAD_RE`,
  `sajaseongeo_parser.py`와 동일한 정규식을 fitz 의존성 없이 이 스크립트에
  복제)를 제거한 결과가 JSON의 계산값과 **정확히 일치**하는지 10건 전부
  대조했다 — 즉 "로컬 계산값을 신뢰"가 아니라 "서버 원문 자체에서 재현되는지"를
  검증했다.
- 그 외 절차(하드가드 → 백업 → 체크섬 스냅샷 → 단일 트랜잭션 UPDATE → 검증 →
  멱등성)는 로컬 스크립트와 완전히 동일한 구조를 그대로 재사용했다.

**부수 발견 1(GATE 0b 최초 실행 시 재현)**: id=7236(`유유상종類類`)에서 headword
비교가 한 번 실패했다 — phase7이 이미 문서화한 것과 동일한 원인(CJK 통합
한자 U+985E vs CJK 호환용 한자 U+F9D0 코드포인트 불일치)이 이번에도 그대로
재현됐다. 원인: `sajaseongeo_datefix_10_new_defs.json`을 처음 만들 때 phase7
보고서 본문에서 이 글자를 그대로 복사해 왔는데, 보고서 본문이 U+985E로
렌더링돼 있어 실제 DB 저장값(U+F9D0)과 달랐다. **서버에서 실제 다운로드한
`server_terms.json` 덤프의 headword 문자열을 파이썬으로 직접 복사해 JSON을
패치**해 해결했다(직접 타이핑하지 않음 — phase7이 남긴 교훈을 그대로 적용).
이 문제는 headword 교차검증(부가 안전장치)에서만 발생했고, UPDATE 자체는
`id` 기준이라 이 문제와 무관하게 정확했을 것이지만, 안전장치가 실제로
작동해 문제를 잡아냈다는 점에서 GATE 0b의 가치를 보여준다.

### 2-2. 백업 (SQLite Backup API, 서버에서 실행)

```
백업 경로: /home/chsh82/aprolabs/data/backups/literacy.db.bak-sajaseongeo-datefix-server-pre-20260926-093229
백업 SHA-256: 8c7d29b7c2648193df0abdb337a7e4fc8e465ddd22df3f443f37372eeed74de2
크기: 4,251,648 bytes
원본 terms 행수: 7312 / 백업 terms 행수: 7312 -> 일치
백업 PRAGMA integrity_check -> ok
백업 PRAGMA foreign_key_check -> 위반 0건
GATE PASS
```

### 2-3. 복구(롤백) 명령 — 문서화만, 미실행(게이트 실패 없었음)

```bash
# 서버(aprolabs SSH)에서 실행
# (필요 시) 복구 전 현재 상태도 먼저 백업
cp ~/aprolabs/data/literacy.db ~/aprolabs/data/backups/literacy.db.rollback-before-restore-$(date +%Y%m%d-%H%M%S)
cp ~/aprolabs/data/backups/literacy.db.bak-sajaseongeo-datefix-server-pre-20260926-093229 ~/aprolabs/data/literacy.db
python3 -c "import sqlite3; c=sqlite3.connect('file:/home/chsh82/aprolabs/data/literacy.db?mode=ro', uri=True); print(c.execute('PRAGMA integrity_check').fetchone())"
```
(서버에는 `sqlite3` CLI도 설치돼 있어(`/usr/bin/sqlite3`) `sqlite3 ~/aprolabs/data/literacy.db "PRAGMA integrity_check;"`로 대체 가능)

### 2-4. dry-run 결과 (`--apply` 없이 실행)

```
GATE 0 (하드 가드): 실제 대상 id = [7148,7188,7191,7192,7197,7223,7236,7243,7266,7288]
  기대 목록과 정확히 일치 -> PASS
GATE 0b (서버 원문 날짜제거 결과 vs 로컬 계산값): 10/10 일치 -> PASS
GATE 1 (새 definition에 '학년도' 잔존 없음): PASS
UPDATE 전 sajaseongeo-pdf 전체 체크섬: 행수=225, sha256=ddddd914c655db536d5ce5069d040e8305685703ffaeb772bf4b4616dc8213a7
DRY-RUN 종료 (DB 변경 없음)
```

10건 변경 전/후(서버 원문 → 계산된 새 값) 전체 텍스트:

| id | headword | 서버 원문(이전, 날짜 접두 포함) | 적용 예정 값(이후) |
|---:|---|---|---|
| 7148 | 동병상련 | `1994학년도 1차 수능 같 은 병을 앓는 사람끼리...` | `같 은 병을 앓는 사람끼리...이르는 말.` |
| 7188 | 사필귀정 | `1994학년도 2차 수능 모든 일은...` | `모든 일은 반드시 바른길로 돌아 감.` |
| 7191 | 상전벽해 | `1994학년도 2차 수능 뽕나무밭이...` | `뽕나무밭이 변하여...비유적으로 이르는 말` |
| 7192 | 새옹지마 | `1994학년도 2차 수능 인생의...` | `인생의 길흉화복은...어렵다는 말.` |
| 7197 | 설상가상 | `1994학년도 1차 수능 눈 위에...` | `눈 위에 서리가...이르는 말.` |
| 7223 | 연목구어 | `1994학년도 2차 수능 나무에...` | `나무에 올라가서...이르는 말.` |
| 7236 | 유유상종類類(U+F9D0) | `1994학년도 1차 수능 같은 무리끼리...` | `같은 무리끼리 서로 사귐` |
| 7243 | 이열치열 | `1994학년도 1차 수능 열은...` | `열은 열로써 다스림...쓰는 말이다.` |
| 7266 | 전화위복 | `1994학년도 2차 수능 재앙과...` | `재앙과 화난이 바뀌어...복이 됨.` |
| 7288 | 초록동색 | `1994학년도 1차 수능 같은 처지(處地)...` | `같은 처지(處地)의 사람과...기우는 것.` |

(전체 정의 문장은 `data/import/schema_reading_phase21_literacy10_terms_full_diff_pre_20260926.json`에
있음 — 날짜 접두어 제거 외 본문 변경이 없음은 로컬 phase7의 34개 단위테스트가
이미 증명한 로직을 그대로 재사용한 것이라 재검증됐다.)

**예상 외 행은 없었다** — 하드가드가 기대 10건과 정확히 일치했으므로 중단 없이
다음 단계로 진행했다.

---

## 3. 서버에 10건만 단일 트랜잭션 적용 + 검증

### 3-1. 경로 재확인

`app/literacy/db.py`의 `DB_PATH = REPO_ROOT / "data" / "literacy.db"`가
`APP_ENV`와 무관하게 하드코딩돼 있음을 서버에서 직접
`grep -n 'DB_PATH' ~/aprolabs/app/literacy/db.py`로 재확인했다(phase4의 결론
재확인). 서버의 `.env`는 `APP_ENV=research`였으나 이 값이 `literacy.db` 경로
분기에 전혀 영향을 주지 않음을 코드로 재확인했다. 실제 경로는
`/home/chsh82/aprolabs/data/literacy.db` 하나뿐.

### 3-2. 적용 (`--apply`, 서버에서 실행)

```
=== 4. UPDATE 적용 (정확히 10건, definition 컬럼만, 단일 트랜잭션) ===
UPDATE 완료: 10건
```

### 3-3. 적용 후 검증

| 게이트 | 결과 |
|---|---|
| 10건 definition이 계산된 새 값과 정확히 일치 | **PASS** (10/10) |
| 10건 level 값(3,2,3,2,2,4,2,1,2,3) — phase7 로컬 적용 결과와 동일 | **PASS** |
| sajaseongeo-pdf 전체 225건 체크섬(id\|headword\|level, UPDATE 전후) | **PASS** (완전 동일: `ddddd914c655...`) |
| 10건 제외 나머지 215건 definition explicit 비교(적용 전후) | **PASS** — 완전 동일 |
| `PRAGMA integrity_check` (적용 후) | **PASS** (ok) |
| `PRAGMA foreign_key_check` (적용 후) | **PASS** (위반 0) |
| 재실행 멱등성(동일 하드가드 쿼리 재실행) | **PASS** (대상 0건) |
| **적용 후 로컬-서버 `terms` 테이블 전수 재비교(7,312행×19컬럼)** | **PASS** — 차이 0건, 완전히 수렴(byte-identical) |

**하나도 fail하지 않았다 — 롤백을 실행할 필요가 없었다.**

적용 후 서버 `literacy.db`:
- 파일 크기: 4,251,648 bytes (변경 전과 동일 — SQLite 페이지 크기 내 텍스트
  치환이라 페이지 수 불변)
- SHA-256: `8dd6622c8fc4214370cee130859abb3c5ff2c9994993aa95599862ba59b58cdb`
  (백업 SHA-256 `8c7d29b7...`과 다름 — definition 10건이 실제로 바뀌었다는
  파일 수준 증거)

---

## 4. 산출물

### 신규/수정 코드 (로컬, git 미커밋)

- `scripts/literacy/apply_sajaseongeo_datefix_10_server.py` — 신규. 서버
  literacy.db 대상 10건 게이트 기반 적용 스크립트(`_DATE_LEAD_RE` 복제 +
  GATE 0b 서버 원문 교차검증 추가, 그 외 로컬 스크립트와 동일 구조).
  `--apply` 없이 실행하면 dry-run. 서버에도 `/tmp/literacy_fix_run/`에
  실행용 사본을 남겨 뒀다(scp로 전달, 저장소 코드와 동일).
- `scripts/literacy/sajaseongeo_datefix_10_new_defs.json` — 신규. 로컬에서
  `merge_sources()`로 재계산한 10건의 `headword`/`new_definition`(headword는
  DB 저장 바이트를 그대로 복사, 직접 타이핑 아님).

### 신규 데이터/보고서 (로컬, git 미커밋)

- `data/import/schema_reading_phase21_literacy10_terms_full_diff_pre_20260926.json`
  — 1-3절 전체 불일치 범위 확인 원본(10건 diff 상세, 적용 전 스냅샷).
- `data/import/schema_reading_phase21_literacy10_server_apply_result_20260926.json`
  — 서버 적용 스크립트가 출력한 결과 스냅샷(백업 경로/SHA-256/체크섬/게이트
  결과 전부 포함, 서버에서 scp로 회수).
- `data/import/schema_reading_phase21_jeonggi_student_definition_fix_dryrun_20260926.json`
  — 5절 "정기" 수정 제안 dry-run 파일(별도, DB 미변경).
- 본 보고서: `reports/schema_reading_phase21_literacy_10fix_and_jeonggi_root_cause_20260926.md`

### 실제 변경된 유일한 DB

- 서버 `/home/chsh82/aprolabs/data/literacy.db` — **오직 10개 id
  (7148,7188,7191,7192,7197,7223,7236,7243,7266,7288)의 `definition` 컬럼만**
  변경. 로컬 `data/literacy.db`는 이번 세션에서 읽기만 했고 전혀 변경하지
  않았다(phase7에서 2026-09-24에 이미 적용 완료된 상태 그대로).

---

## 5. "정기"(SR_L4CORE_4786) 문항-콘텐츠 불일치 원인 조사 + 수정안 dry-run (DB 쓰기 없음)

**절대 제약 준수**: 이 절을 작성하기 위해 `vocabulary_quiz_research.db`에
어떤 쿼리도 실행하지 않았다. 아래 "현재 콘텐츠 값"은 이번 세션이 직접
조회한 것이 아니라 `reports/schema_reading_phase20_pilot_audit_and_scope_fix_20260926.md`
(1-4절, "호출 세션이 서버 직접 조회로 재확인 완료")가 이미 기록해 둔 값을
그대로 인용한 것이다. 이번 세션이 직접 재조회한 것은 오직 `literacy.db`
id=4786(원천 정의)뿐이다.

### 5-1. 원인 추적

`scripts/vocab/phase16_build_quiz_pilot_dryrun.py`를 직접 열어 확인한 결과,
L4 표제어 목록은 `(lemma, content_id, term_id, pos, definition,
example_sentence, example_target_form, vocab_level)` 튜플이 **스크립트 소스
코드에 하드코딩**돼 있다(L16-37). "정기" 행(L31-32)은 현재:

```python
("정기", "SR_L4CORE_4786", "4786", "명사", "기한이나 기간이 일정하게 정해져 있는 것",
 "우리 반은 매달 첫째 주에 정기 모임을 엽니다.", "정기", "4"),
```

이 스크립트는 문항을 만들 때 `vocabulary_contents.student_definition`을
매번 DB에서 다시 읽어오지 않는다 — 이 하드코딩 문자열을 정답 텍스트/
`explanation` 생성에 직접 사용한다. `reports/schema_reading_phase17_quiz_pilot_semantic_review_20260926.md`
말미의 "4. 추가 — '정기' 해설 수정(2026-09-26, 18단계 1번 작업)" 절이
증언하듯, phase17이 발견한 문제(하드코딩 값이 literacy.db 원문의 "되풀이"
뉘앙스를 임의로 추가했다는 것)를 고치면서 **이 스크립트 파일의 하드코딩
문자열 자체를 `"일정한 기간마다 되풀이하도록 정한 것"` → `"기한이나 기간이
일정하게 정해져 있는 것"`으로 수정**했고(이번 세션이 실제 파일에서 현재값이
이미 후자임을 확인), 그 수정된 스크립트로 문항을 재생성해
`vocabulary_multiformat_items.explanation`/`options_json`에 반영했다.

그런데 이 수정은 **스크립트 소스 코드와 그것이 만들어 내는 문항 텍스트에만**
적용됐다. `vocabulary_contents.student_definition`은 이 문항 생성 흐름의
입력값이 아니라(생성 스크립트가 이 필드를 조회하는 코드 자체가 없음),
phase18 적재 과정의 다른 지점에서 독립적으로 관리되는 값이었기 때문에,
phase17/18의 어느 단계에서도 이 필드를 갱신하는 코드 경로가 없었다.
phase20이 실제로 서버를 재조회해 발견한 것처럼, 그 결과 `vocabulary_contents.student_definition`은
phase16의 2026-09-25 구버전 문구(`"일정한 기간마다 되풀이하도록 정한 것"`)에
그대로 머물러 있고, 문항 텍스트만 2026-09-26에 한 번 더 갱신된 값
(`"기한이나 기간이 일정하게 정해져 있는 것"`)을 갖게 됐다 — **"갱신을
깜빡한 것"이 아니라 "이 필드를 건드리는 스크립트 자체가 애초에 없었던
것"**이 근본 원인이다.

### 5-2. 어느 쪽이 맞는가 — 원천 대조

이번 세션이 로컬·서버 양쪽 `literacy.db`에서 id=4786을 직접 재조회했다
(양쪽 byte-identical, 이번 10건 UPDATE와 무관한 행이라 phase7 이후 계속
불변):

```
id=4786, headword=정기, source=schemareading-tooldict, external_id=L4-129, level=4
definition = "기한이나 기간이 일정하게 정하여져 있는 것. 또는 그 기한이나 기간."
```

- **문항 쪽(2026-09-26 재생성)**: `"기한이나 기간이 일정하게 정해져 있는 것"`
  — 원천의 "정하여져"를 구어체 "정해져"로 축약하고 "또는 그 기한이나 기간"
  (동일 개념의 명사형 재진술)을 생략했을 뿐, **핵심 의미(고정된 기한/기간)를
  그대로 보존**한다.
- **콘텐츠 쪽(`vocabulary_contents.student_definition`, phase20 인용)**:
  `"일정한 기간마다 되풀이하도록 정한 것"` — 원천에 없는 "되풀이(반복)"
  개념을 추가한다. phase17 3-3절이 이미 지적한 바로 그 문제가 콘텐츠 쪽에는
  아직 남아 있다.

**판정: 문항 쪽(원천에 더 가까움)이 맞고, `vocabulary_contents.student_definition`
(구버전, 원천에 없는 "되풀이" 뉘앙스를 포함)이 틀렸다/낡았다.** 이미
phase17이 "어느 문구가 원천에 더 가까운가"를 판단해 제안 1을 냈고, 그
판단이 스크립트/문항 쪽에는 반영됐으므로, 이번에는 그 동일한 판단 방향으로
콘텐츠 쪽을 맞추는 것이 논리적으로 일관된다(새로운 판단을 내리는 것이
아니라, 이미 내려진 판단을 아직 반영되지 않은 곳에 적용하는 것).

### 5-3. 수정안 (별도 dry-run 파일, 이번 세션에서 미적용)

`data/import/schema_reading_phase21_jeonggi_student_definition_fix_dryrun_20260926.json`에
전체를 담았다. 요지:

```
대상 DB   : vocabulary_quiz_research.db (서버, research) — 이번 세션 미접근
대상 테이블: vocabulary_contents
대상 행   : content_id = 'SR_L4CORE_4786'
대상 컬럼 : student_definition
현재값(phase20 인용) : "일정한 기간마다 되풀이하도록 정한 것"
제안값               : "기한이나 기간이 일정하게 정해져 있는 것"
건드리지 않는 컬럼   : student_exposure, public_ready, level_status, is_active, id
제안 SQL(미실행)     : UPDATE vocabulary_contents SET student_definition = '기한이나 기간이
                       일정하게 정해져 있는 것' WHERE content_id = 'SR_L4CORE_4786';
```

실제 적용을 원할 경우, 이번 세션이 서버 `literacy.db`에 쓴 것과 동일한
패턴(백업 → 하드가드[정확히 1건, content_id 일치] → 단일 트랜잭션 UPDATE →
`integrity_check`/`foreign_key_check` → 재실행 멱등성)을
`vocabulary_quiz_research.db`에 대해 재사용할 것을 제안한다(이번 세션은
실행하지 않았다).

### 5-4. 40건 판정표에 미치는 영향 (언급만, 실제 전환 안 함)

이 수정안이 실제로 적용되면, phase20이 HOLD로 남긴 사유("문항-콘텐츠 텍스트
불일치")가 근거를 잃으므로 **재판정 시 PASS로 바뀔 여지가 있다**. 다만:

- 이번 세션은 `vocabulary_contents`를 전혀 건드리지 않았으므로 이 전환은
  **일어나지 않았다**.
- 설령 값이 갱신되더라도, HOLD → PASS 전환은 이번 세션이 직접 하지 않는다
  (지시사항). 실제 전환은 (a) 콘텐츠 담당자/사용자가 위 수정안을 승인하고
  (b) `vocabulary_contents.student_definition`이 실제로 갱신된 뒤, (c) 별도
  세션이 phase20과 동일한 방식(정답 유일성/오답 배제력/원문 대조)으로
  두 문항을 다시 사람 수준으로 재판정해야 한다.

---

## 6. 최종 요약 (호출한 에이전트용)

1. **10건 외 추가 불일치**: 없음. 로컬-서버 `terms` 테이블 7,312행×19컬럼
   전수 비교 결과 차이는 정확히 그 10개 id, `definition` 컬럼 하나뿐이었다
   (1-3절). 스키마도 21개 `sqlite_master` 항목 완전 일치.
2. **백업**: `/home/chsh82/aprolabs/data/backups/literacy.db.bak-sajaseongeo-datefix-server-pre-20260926-093229`,
   SHA-256 `8c7d29b7c2648193df0abdb337a7e4fc8e465ddd22df3f443f37372eeed74de2`
   (4,251,648 bytes, integrity ok, fk 위반 0). 복구 명령은 2-3절.
3. **적용 결과**: **전 게이트 PASS**. 하드가드/GATE 0b 교차검증/백업 무결성/
   10건 값 일치(10/10)/체크섬 불변/나머지 215건 explicit 동일/integrity_check
   ok/fk 위반 0/멱등성(0건)/**적용 후 로컬-서버 완전 수렴(7,312행 byte-identical)**.
   롤백 0회.
4. **"정기" 불일치 원인**: `phase16_build_quiz_pilot_dryrun.py`가 정의 문구를
   `vocabulary_contents`에서 매번 읽지 않고 스크립트에 하드코딩한 값을 쓰기
   때문 — phase17이 이 하드코딩 값(과 그로 만들어진 문항 텍스트)만 원천에
   맞게 고쳤고, `vocabulary_contents.student_definition`을 갱신하는 코드
   경로가 애초에 없어 구버전 문구가 남았다. **문항 쪽(원천에 가까움)이
   맞고 콘텐츠 쪽(구버전, "되풀이" 뉘앙스 추가)이 틀렸다.** 수정안은
   `data/import/schema_reading_phase21_jeonggi_student_definition_fix_dryrun_20260926.json`에
   제출(DB 쓰기 없음). 적용되면 HOLD 2건이 PASS로 바뀔 여지가 있으나 이번
   세션은 전환하지 않았다.
5. **만든 파일**: 4절 참고(스크립트 2개, 데이터/제안서 3개, 본 보고서 1개).
6. **가장 중요한 리스크**:
   - (a) 서버 `literacy.db`는 이번 적용 전까지 phase7(2026-09-24) 로컬 수정이
     전혀 반영되지 않은 채(마지막 서버 mtime 2026-09-01) 8일 넘게 오염된
     상태로 남아 있었다 — 로컬과 서버 사이에 동기화 절차가 없다는 구조적
     리스크가 이번에 실제로 드러난 사례다. 앞으로 로컬에서 literacy.db를
     수정할 때마다 서버 반영 여부를 별도로 확인·적용해야 한다(자동 동기화
     메커니즘 없음).
   - (b) "정기" 콘텐츠 필드 갱신은 아직 미적용 상태로 남아 있다 — 사용자
     승인 없이는 다음 세션도 이 필드를 건드리면 안 된다.
   - (c) 이번 서버 적용 스크립트(`apply_sajaseongeo_datefix_10_server.py`)는
     `merge_sources()`를 서버에서 실행할 수 없어 로컬 계산값을 전달받는
     방식으로 우회했다 — 향후 사자성어 정의를 또 수정해야 할 일이 생기면
     이 우회 방식(GATE 0b 교차검증 포함)을 재사용하거나, 서버에 `fitz`+원본
     PDF를 갖추는 방안을 검토할 필요가 있다.
