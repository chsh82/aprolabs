# 공식 5등급 기반 중등 어휘 목록 구축 — 1차 정리 + 검수 준비 (읽기 전용)

- 일자: 2026-10-01
- 범위: 국립국어원 공식 5등급(중1~3, 경계 L3~L4 신호) 17,000행을 정리하고
  연구 DB 5,950건과 대조해 신규 어휘 확장 후보를 선별한다. **DB 쓰기
  전혀 없음** — `vocab_level`·문항·매니페스트·공개 플래그 변경 없음,
  뜻풀이·예문 작성 없음, 문항 생성 없음, momolib 무관.
- 산출 스크립트: `scripts/vocab/nikl_grade5_candidate_list.py`

## 0. 선행 조사 인용(재검증 안 함)

`reports/nikl_l3_backfill_investigation_20260930.md` +
`data/import/nikl_l3_backfill_census_20260930.csv`(49건)가 이미 227건
수동 배치 중 공식5등급 매칭 49건을 전수조사했다(사람 검수 상태·연결
문항·뜻풀이 유무까지 전부 확인 완료). **이번 작업은 그 49건을
재조사하지 않는다 — 여전히 "검수 대기 후보"로 그대로 둔다.** 이번
스크립트가 아래 3절에서 그 49건을 독자적으로 재현해 교차검증만
했을 뿐, 재분류하거나 판정을 바꾸지 않았다.

매칭 방법론은 `scripts/vocab/nikl_official_match.py`(표제어+품사
조인, 동형이의 신중 처리)와 그 검증된 산출물
`data/import/nikl_official_match_full_20260929.csv`를 그대로
재사용했다(방향만 반대 — 공식목록 → DB).

## 1. 공식 5등급 17,000행 정리

| 항목 | 값 |
|---|---:|
| 원천 행 수 | **17,000**(확인) |
| 고유 어휘 수(어휘만) | **16,551** |
| 고유 어휘 수(어휘+동형번호 단위) | **17,000** |

어휘+동형번호 단위가 원천 행수와 정확히 같다는 것은, 이 시트 안에서
(어휘, 동형번호) 조합이 이미 전부 유일하다는 뜻이다 — 동형이의 후보를
합치지 않고 그대로 뒀다(지시대로).

**품사 분포**(한 행이 "명사/부사"처럼 여러 품사를 가질 수 있어 토큰
합계가 17,000을 넘는다):

| 품사 | 건수 |
|---|---:|
| 명사 | 13,078 |
| 동사 | 2,246 |
| 형용사 | 852 |
| 부사 | 789 |
| 관형사 | 237 |
| 의존명사 | 54 |
| 감탄사 | 51 |
| 대명사 | 12 |
| 수사 | 11 |
| 품사없음 | 5 |
| 보조동사 | 1 |
| 보조형용사 | 1 |

**분야**: 행의 분야 필드(의미 「n」별로 "/"로 구분) 안에 "전문어"가
한 번이라도 들어간 행이 **4,706건**(27.7%) — 나머지는 전부 일반어.
**공식 자료 자체의 품사 체계에 "고유명사" 태그가 없다**(전수 확인,
0건) — 4절의 고유명사 위험 플래그가 전부 False인 이유다.

## 2. DB 5,950건과 대조 — 3분류

**분류 기준(표제어 일치만으로 확정하지 않음)**: 표제어+품사가 일치하는
DB content가 있고, 그 content가 `nikl_official_match_full_20260929.csv`
(이미 검증된 산출물)에서 "공식 5등급·단일일치"로 **확정**된 경우만
"기존 콘텐츠와 연결 후보"로 둔다. 품사가 다르거나, DB 쪽이 다중후보/
표제어만일치 상태이거나, 공식목록 전체(전 등급 포함)에서 같은
표제어+품사가 서로 다른 동형번호로 2개 이상 존재하면("동형이의 위험")
자동 확정하지 않고 "확인 필요"로 내린다.

| 분류 | 건수 |
|---|---:|
| 기존 콘텐츠와 연결 후보 | **48** |
| 동형이의·품사 확인 필요 | **14** |
| 신규 어휘 후보 | **16,938** |
| **합계** | **17,000** |

**49건 census와의 교차검증(투명성 기록)**: 처음에는 자체 뜻풀이
bigram-유사도 재매칭을 시도했으나, 진단 결과 49건 census 중 19건이
"후보가 사실 하나뿐인데도 유사도 점수가 낮다"는 이유만으로 잘못
"확인 필요"로 떨어지는 회귀가 발견됐다(짧은 공식 의미문 vs DB
뜻풀이의 bigram 중첩이 우연히 낮게 나온 경우가 많았음). 이 회귀를
없애기 위해 **자체 재매칭을 버리고, 이미 검증된
`nikl_official_match_full_20260929.csv`를 단일 진실 공급원으로
재사용**하도록 수정했다. 그 결과 49건 중 **48건이 그대로 "연결
후보"로 재현**됐고, 나머지 1건("개관")은 공식목록 전체(전 등급)
기준으로 봤을 때 같은 표제어+품사가 서로 다른 동형번호로 2개 존재하는
진짜 동형이의 구조가 있어 — **등급5 단일 범위 안에서만 보면 안
보이던 위험을 전체 목록 기준 검사로 새로 발견**해 "확인 필요"로
보수적으로 내렸다(결함이 아니라 더 엄격한 추가 발견). 49건 전부
(48+1) 계정이 맞는다 — census 쪽에 없던 새로운 content_id가 끼어든
경우는 0건(content_id 기준 직접 대조로 확인).

## 3. 1차 검토 배치 100건 — 재현 가능한 층화표본

**방법**: "신규 어휘 후보" 16,938건을 (대표 품사, 전문어 여부) 조합
18개 층으로 나누고, 각 층 크기에 비례 배분(최소 1건 보장) 후 고정
시드(`random.Random(20261001)`)로 `random.sample()`해 100건을 뽑았다
— 동일 스크립트를 다시 돌리면 같은 100건이 재현된다. 비례배분
반올림으로 100건에 못 미치면 미선정 잔여 후보에서 같은 시드로
보충했다.

| 위험 플래그 | 건수(100건 중) | 판정 근거 |
|---|---:|---|
| 전문용어_위험 | **31** | 공식목록 분야 필드에 "전문어" 포함 |
| 다의어_위험 | **9** | 공식목록 전체(전 등급) 기준 같은 표제어+품사가 동형번호 2개 이상 |
| 고유명사_위험 | **0** | 공식 자료 자체에 고유명사 품사 태그가 없어 이 신호로는 탐지 불가(1절 참고) — "없음"이 아니라 "이 자료로는 판정 불가"임을 명시 |

품사 분포(100건): 명사 68·동사 13·형용사 6·부사 4·관형사 2·감탄사 2·
의존명사 2·수사 1·품사없음 1·대명사 1·보조동사 1(토큰 합계 기준).
중복 표제어 없음(100건 전부 서로 다른 표제어).

**이번 100건은 표제어·품사·공식등급·전문용어/고유명사/다의어 위험
플래그·선정사유뿐**이다 — 뜻풀이·예문·문항은 전혀 생성하지 않았다.

## 4. 레벨 근거 기록 원칙

공식 5등급은 **"경계(L3~L4)"**로만 기록한다(100건 전부, 그리고 17,000행
전체에 적용할 원칙으로서). 추가 근거 없이 L3나 L4 하나로 확정하지
않는다. "기존 콘텐츠와 연결 후보"(48건)에 대해서도 **기존 DB
`vocab_level`을 분류 근거로 재사용하지 않았다** — 그 content가 현재
어떤 레벨이든, 추천은 순수하게 공식 등급(5→경계)에서만 나온다(기존
정책과 동일, `reports/nikl_base_level_policy_20260929.md` 참고).

## 5. 관리자 검수 화면 재사용 설계안(설계만 — 이번엔 구현/배포 안 함)

**범위 재확인**: 사용자가 "이번 범위는 목록 정리와 검수 준비까지다"라고
명시했으므로, 아래는 **설계 문서일 뿐이며 실제 테이블·라우트·템플릿
파일을 만들지 않았다.** 스키마 변경·마이그레이션 실행·git push·서버
배포 전혀 없음.

### 왜 tier1 화면을 그대로 못 쓰는가

기존 `/vocab-official-grade-review/`(tier1, 31건)는 구조적으로
**이미 `vocabulary_contents`에 존재하는 content_id**를 전제한다 —
`routers/official_grade_review.py`의 `index()`/`detail()`/
`save_judgment()`가 전부 `VocabularyOfficialGradeReference.content_id`를
조회 키로 쓰고, `ref.priority_tier != 1`이면 404를 던진다. 이번
100건은 **content_id 자체가 아직 없는 신규 어휘**라 같은 테이블/같은
키로는 들어갈 수 없다 — 구조가 다른 검수 대상이다.

### 제안 1: 별도 스테이징 테이블(기존 패턴 그대로 복제)

```sql
CREATE TABLE vocabulary_grade5_candidate_batch (
    candidate_id          TEXT PRIMARY KEY,  -- 예: 'G5B1-0001'(배치번호-순번), content_id 아님
    batch_no              INTEGER NOT NULL,  -- 1(이번 100건), 2(다음 100건)...
    lemma                 TEXT NOT NULL,
    pos                   TEXT NOT NULL,
    homonym_number        INTEGER,
    official_grade        TEXT NOT NULL,     -- '5' 고정(이번 배치)
    proposed_level_note   TEXT NOT NULL,     -- '경계(L3~L4)' 고정
    specialized_risk       INTEGER NOT NULL, -- 전문용어_위험
    proper_noun_risk       INTEGER NOT NULL, -- 고유명사_위험(이번 배치는 전부 0)
    polysemy_risk          INTEGER NOT NULL, -- 다의어_위험
    selection_reason       TEXT,             -- 선정_사유
    source_file_sha256     TEXT NOT NULL,    -- 공식 xlsx 해시(기존과 동일 상수)
    computed_at             TEXT NOT NULL,
    computed_by_script      TEXT NOT NULL
);
```
`vocabulary_official_grade_reference`와 같은 "순수 참조, 서빙 코드가
안 건드림" 원칙을 그대로 따르되, **PK를 `content_id`가 아니라
`candidate_id`로 분리**해 "아직 콘텐츠가 아닌 것"과 "이미 콘텐츠인
것"을 테이블 수준에서 구조적으로 섞이지 않게 한다(기존
`vocabulary_official_grade_reference`를 nullable-content_id로 확장하는
대안도 검토했으나, 그러면 PK가 애매해지고 기존 5,950건 참조 데이터와
신규 후보가 한 테이블에 섞여 조회 로직이 복잡해진다 — 분리를 권고).

### 제안 2: 새 판정 테이블(append-only, 사용자 지정 선택지)

```sql
CREATE TABLE vocabulary_grade5_candidate_judgments (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id             TEXT NOT NULL,
    judgment                 TEXT NOT NULL CHECK (judgment IN ('L3','L4','경계 유지','제외')),
    rationale                TEXT,
    reviewer_user_id          TEXT NOT NULL,
    reviewer_email            TEXT,
    reviewed_at               TEXT NOT NULL DEFAULT (datetime('now')),
    source_data_version_at_review TEXT NOT NULL,  -- tier1과 동일한 staleness 해시 패턴
    created_at                TEXT DEFAULT (datetime('now'))
);
```
tier1의 `기본 레벨 조정/현재 유지/뜻 확인/보류`와는 **다른 질문**(신규
후보를 L3로 볼지/L4로 볼지/경계로 둘지/아예 이번 확장에서 뺄지)이라
사용자가 지정한 대로 `L3 / L4 / 경계 유지 / 제외` 4지선다로 분리한다.

### 제안 3: 라우트/템플릿 — 기존 명명 규칙 그대로 복제

- `app/vocabulary_quiz/models_grade5_candidate_review.py` —
  `VocabularyGrade5CandidateBatch` + `VocabularyGrade5CandidateJudgment`
  두 모델(tier1의 `models_official_grade_review.py`와 같은 구조).
- `app/vocabulary_quiz/grade5_candidate_review.py` — 순수 로직
  (`ordered_batch_candidate_ids`, `latest_judgment`, `judgment_is_stale`,
  `save_judgment`, `next_candidate_id` — tier1의
  `official_grade_review.py` 함수명 패턴 그대로).
- `app/vocabulary_quiz/routers/grade5_candidate_review.py` — 새 경로
  `/vocab-grade5-candidate-review`(tier1의 `/vocab-official-grade-review`
  와 완전히 별도, 그 라우터는 한 글자도 안 건드림), `require_admin`
  그대로 재사용. `GET /?batch=1`(배치별 목록), `GET /{candidate_id}`,
  `POST /{candidate_id}/judgment`.
- 템플릿 `official_grade_review_index.html`/`_detail.html`과 같은
  Tailwind 레이아웃을 그대로 복제한 `grade5_candidate_review_index.html`/
  `_detail.html` — 단, 상세 화면에는 **뜻풀이·예문 입력란을 아예
  만들지 않는다**(이번 단계 범위 밖임을 화면에서도 강제).

### "배치 등록" = 재실행 가능한 적재 스크립트

`scripts/vocab/migrate_add_grade5_candidate_batch.py`(테이블 생성,
`migrate_add_official_grade_reference.py`와 동일한 `db_path_guard`
안전장치 재사용) + `scripts/vocab/load_grade5_candidate_batch.py
--batch 1 --csv data/import/nikl_grade5_first_batch_100_20261001.csv`
(dry-run 기본, `--apply` 필요, 같은 compare-and-swap 충돌 가드) —
101~200번째 배치는 같은 스크립트를 `--batch 2 --csv <다음 100건>`으로
재실행하면 된다. **이번 작업에서는 이 스크립트들을 작성도 실행도
하지 않았다** — 설계만 남긴다.

## 6. 집계 요약(사용자 지정 구분 그대로)

| 구분 | 건수 | 비고 |
|---|---:|---|
| 원천 행 수 | 17,000 | 공식 5등급 시트 |
| 고유 어휘 수(어휘 단위) | 16,551 | |
| 고유 어휘 수(어휘+동형번호 단위) | 17,000 | 동형이의 분리 유지 |
| 기존 연결 후보 | 48 | 49건 census와 교차검증 완료(1건은 추가 동형이의 발견으로 확인필요 이동) |
| 의미 확인 필요 | 14 | 품사 불일치/다중후보/동형이의 위험 |
| **신규 후보** | **16,938** | **문항 수 아님 — 콘텐츠 자체가 아직 없음** |
| 첫 검토 배치 | 100 | 층화표본(품사×전문어 여부), seed=20261001 재현 가능 |

**신규 후보 16,938건은 출제 가능한 문항 수가 아니다** — 뜻풀이·예문도
없고 문항도 전혀 없는, 순수 "이 단어를 향후 검토할지 말지"를 묻는
어휘 목록이다.

## 7. 산출 파일

| 파일 | 행수 | 내용 |
|---|---:|---|
| `data/import/nikl_grade5_full_20261001.csv` | 17,000 | 공식5등급 원본 정리(어휘/동형번호/품사/어종/원어/의미/분야) |
| `data/import/nikl_grade5_crossref_20261001.csv` | 17,000 | 3분류 태그 + 매칭 content_id + 동형이의 위험 |
| `data/import/nikl_grade5_first_batch_100_20261001.csv` | 100 | 1차 검토 배치(표제어/품사/공식등급/제안레벨/위험 3종/선정사유) |
| `data/import/nikl_grade5_stats_20261001.json` | - | 재실행 검증용 집계 원본 |
| `scripts/vocab/nikl_grade5_candidate_list.py` | - | 재실행 스크립트(DB 내보내기 쿼리는 파일 상단 주석에 포함) |

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
