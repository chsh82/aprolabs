# 공식 5등급 1차 배치 100건 — 관리자 검수 기능 구현·배포

- 일자: 2026-10-01
- 범위: `reports/nikl_grade5_candidate_list_and_review_design_20261001.md`의
  설계를 실제 구현해 연구 서버에 배포한다. **완료 범위는 "사용자가
  100건을 검수할 수 있는 상태"까지** — 뜻풀이·예문·문항 신규 생성,
  momolib 작업, 기존 vocab_level·문항·매니페스트·공개 플래그 변경
  전혀 없음.

## 1. 입력 고정 확인(재분석 없음)

| 파일 | SHA-256 |
|---|---|
| `data/import/nikl_grade5_first_batch_100_20261001.csv`(100행, 확정본) | `2d32eabf023a8a59eae4767f8d9973216c93414b3ca15ae049efb6a109af10bb` |
| 공식 xlsx(report_seq=1160) | `6eec715bca39d1006702da020f5c61a7a1f3db81fdb6101619f68d0b0ff70b53`(이전과 동일 — 자료 버전 변경 없음 확인) |

**동형번호·공식 의미 필드 복원**: 확정본 CSV에는 `동형번호`/`의미` 컬럼이
없었다(표제어·품사·위험플래그·선정사유만 보존됨 — 생성 스크립트가 최종
출력 시 해당 필드를 드롭했었음). 검수 화면에 "표제어·품사·동형번호"와
"공식 자료의 기존 짧은 뜻풀이"를 표시해야 하므로, **같은 스크립트를
seed=20261001 그대로 재실행**(재선정이 아니라 동일 로직의 순수 재현)해
중간 단계의 (표제어,품사,동형번호,의미) 값을 복원했다 — 표제어·품사·
선정사유 100건이 순서까지 원본과 100% 일치함을 확인한 뒤에만 사용.
결과물: `data/import/nikl_grade5_first_batch_100_ENRICHED_20261001.csv`
(SHA-256 `994c6397d134932bf53a16d9495ae7130bcb875da2c5cf64ec95264c0d40c907`).

## 2. 스키마 구현

`scripts/vocab/migrate_add_grade5_candidate_batch.py`로 2개 테이블 추가:

- `vocabulary_grade5_candidate_batch` — PK는 `candidate_id`(content_id
  아님, 아직 vocabulary_contents에 없는 신규 어휘라서). `vocabulary_
  contents`/`vocabulary_content_levels`에 FK 없음 - 완전히 독립.
- `vocabulary_grade5_candidate_judgments` — append-only, 선택지
  `L3/L4/경계 유지/제외`. `(candidate_id, submission_token)` 부분
  유니크 인덱스(토큰 NULL이 아닐 때만)로 이중 제출을 DB 레벨에서
  차단한다(4절 참고).

**candidate_id 파생**: `"G5-" + sha256(f"{lemma}|{pos}|{homonym_number}|
{source_file_sha256}")[:16]` — 순수 (표제어,품사,동형번호,자료버전)의
함수라 행 순서에 의존하지 않고, 같은 CSV를 재적재하면 항상 같은 ID가
나온다. 공식 자료 버전이 바뀌면 같은 단어라도 다른 ID가 생겨 버전
드리프트가 "조용한 충돌" 대신 "새 ID 출현"으로 드러난다.

**재적재 멱등성**: `scripts/vocab/apply_grade5_candidate_batch.py`가
`apply_official_grade_reference.py`와 동일한 compare-and-swap을 쓴다 -
같은 candidate_id가 이미 있고 값이 완전히 같으면 SKIP, 하나라도 다르면
전체 트랜잭션 중단(덮어쓰지 않음). 격리 환경에서 2회 연속 실행해 실측
확인(4절).

## 3. 관리자 메뉴·화면

- 사이드바 "📘 문해력 · 어휘" 섹션에 **"📗 중등 어휘 레벨 검수"** 링크
  추가(`app/templates/base.html`, 기존 tier1 링크 바로 아래, 동일한
  active-state 패턴).
- 목록 `GET /vocab-grade5-candidate-review/` — 진행률(N/100), 판정별
  건수(L3/L4/경계 유지/제외/미검수), `?filter=unjudged` 미검수 필터.
- 상세 `GET /vocab-grade5-candidate-review/{candidate_id}` — 표제어·
  품사·동형번호, 공식 자료 원문 의미(읽기 전용), 전문용어·고유명사·
  다의어 위험 표시, "공식 5등급 → 경계(L3~L4)" 근거 문구만 표시 —
  뜻풀이·예문 입력란 자체가 화면에 없다(범위 제한을 UI로 강제).
- 판정 폼: `L3(중1~2)/L4(중3)/경계 유지/제외` 4지선다, **기본 선택
  없음**(렌더링된 HTML에 `checked` 속성 없음 확인), 근거 입력란 선택.
- 저장 후 **다음 미검수 항목**으로 자동 이동(이미 검수된 항목은
  건너뜀 - tier1의 단순 "다음 순번"과 다름, `next_unjudged_candidate_
  id()`가 전담). 상세 화면 하단에 이전/다음 네비게이션을 둬 **이미
  검수한 항목도 자유롭게 재방문 가능**.
- '제외' 판정도 다른 판정과 동일하게 판정 테이블에 행 하나 추가될
  뿐 - `vocabulary_grade5_candidate_batch` 행을 삭제·수정하는 코드는
  라우터/서비스 계층 어디에도 없다(4절에서 실측 확인).

## 4. 이중 제출 방지 + 재판정 이력 + 최신 집계(격리 환경, 실제 HTTP로 검증)

**메커니즘**: 상세 GET마다 `secrets.token_urlsafe(16)`로 새 토큰을 발급해
숨은 필드로 내려보낸다. 같은 토큰이 두 번 POST되면(더블클릭 등)
`(candidate_id, submission_token)` 유니크 인덱스가 두 번째 INSERT를
`IntegrityError`로 막고, 서비스 계층이 그 예외를 잡아 새 행을 만들지
않은 채 **이미 저장된 그 행을 그대로 반환**한다. 상세 페이지를 다시
열면 새 토큰이 나오므로, 의도적 재판정은 같은 judgment 값이라도 정상
새 이력이 된다.

연구 DB 사본 + 격리 scratch 앱 인스턴스(별도 git worktree, 스크래치
포트)에서 **실제 HTTP 요청 경로로** 검증(생성 로직 재호출이 아님):

| 검증 항목 | 방법 | 결과 |
|---|---|---|
| 적재 멱등성 | `apply_grade5_candidate_batch.py --apply`를 스크래치 DB에 2회 연속 실행 | 1회차: 신규 100건 삽입. 2회차: "기존 100건, 신규 0, 동일값 스킵 100, 충돌 0" - **PASS** |
| 접근제어(비로그인) | 쿠키 없이 목록/상세 GET, 판정 POST | 전부 **302**(로그인 리디렉션) |
| 접근제어(비관리자) | 스크래치 전용 테스트 계정(별도 격리 사본의 `aprolabs.db`에만 존재, 운영 계정 아님)의 `is_admin`을 0으로 전환 후 목록 GET | **403** |
| 판정 저장 + 다음 미검수 이동 | 실제 로그인 → 상세 GET(토큰 획득) → POST 저장 | **303**, 서로 다른 미검수 후보로 순차 이동 확인 |
| 중복 제출(더블클릭 시뮬레이션) | 같은 candidate_id·같은 토큰으로 POST 2회 연속 | 둘 다 303 응답했지만 **DB에는 행 1개만** 생성 확인(직접 재조회) |
| 재판정·최신 집계 | 같은 candidate를 다시 GET(새 토큰) → 다른 판정값(L4)으로 재제출 | 판정 테이블에 **2개 행 모두 보존**(L3→L4), 목록 화면은 **L4만** 표시(latest-only 집계 확인) |
| '제외' 시맨틱 | 다른 후보를 '제외'로 판정 | `vocabulary_grade5_candidate_batch` 행 그대로 존재, 배치 총수 100 불변 |

테스트 계정·스크래치 DB·git worktree·프로세스 전부 테스트 종료 후 삭제
(연구 DB·운영 사용자 DB는 전혀 건드리지 않음 - 격리 사본에서만 작업).

## 5. 배포

- 진단: 로컬 `main`이 origin/main 대비 3개 커밋 앞서 있었고(이전 턴들의
  읽기전용 조사 산출물, 전부 vocab 관련), 서버 HEAD는 origin/main과
  정확히 일치(`6b79458`) — 분리 가능 확인.
- 이번 기능 커밋에는 정확히 아래 파일만 포함(무관한 기존 미커밋
  변경 - momo_worksheet_page_editor.py, momo_book_db/* 등 - 전혀
  포함 안 함): 신규 모델/서비스/라우터/템플릿 5개, `app/main.py`·
  `base.html` diff, 신규 스크립트 2개, `ENRICHED` CSV, 이 보고서.
- `git push origin main` → 연구 서버 `git pull` → 실제 연구 DB에
  `migrate_add_grade5_candidate_batch.py` 실행(APP_ENV=research 확인,
  SQLite Backup API 백업+복원성 검증 통과) → `apply_grade5_candidate_
  batch.py --apply` 실행(GATE 1~11 전부 PASS, 100건 신규 삽입, 콘텐츠
  5,950·문항 1,553·공식등급참조 5,950 불변) → `aprolabs.service` 재시작.

## 6. 라이브 검증

읽기 전용으로 확인 가능한 범위: 라이브 DB에 `vocabulary_grade5_
candidate_batch` 100건·`vocabulary_grade5_candidate_judgments` 0건
적재 확인, 서비스 재시작 후 정상 기동 확인. **실제 관리자 로그인을
통한 화면 클릭 검증은 이 턴(fork 실행 단위)에서 브라우저 세션에
접근할 수 없어 수행하지 못했다** — tier1 배포 턴과 동일하게, 이
부분은 오케스트레이터/사용자가 다음 턴에서 실제 로그인 브라우저로
이어서 확인해야 한다.

## 7. 결론 — 실제 검수 URL·메뉴 위치

- **라이브 검수 URL**: `https://aprolabs.co.kr/vocab-grade5-candidate-review/`
- **관리자 메뉴 위치**: 좌측 사이드바 "📘 문해력 · 어휘" 섹션 →
  "📗 중등 어휘 레벨 검수"(기존 "🏛️ 국립국어원 등급 검수(tier1)"
  바로 아래)
- vocab_level·문항·매니페스트·공개 플래그 변경 없음, momolib 무관,
  뜻풀이·예문·문항 신규 생성 없음 - 전부 확인됨.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
