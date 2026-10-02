# L3 중등 보강 1차 30어휘·60문항 - 관리자 검수용 적재 + 미리보기 배치 구현

- 일자: 2026-10-07
- 범위: (1) 중복 보류 13건 용어 정정 + 실제 중복 검사, (2) 최종 콘텐츠·문항
  파일 고정(ID·버전·해시), (3) 비공개 적재(GATE 1~11), (4) 레벨 판정과
  검수 상태 분리, (5) 관리자 미리보기에 4번째 모드로 추가(조회·응시·채점·
  검수), (6) 검증 후 커밋·배포·라이브 확인.
- 보류 51건·잔여 40건은 전혀 건드리지 않았다. RULE_A/B 1,408건·기존
  레벨·학생 공개(`public_ready`/`student_exposure` 전부 0)·momolib 변경
  없음. 완료된 분류기 실험·전체 조사는 반복하지 않았다.

## 1. "중복 보류 13건" — 용어 정정 + 실제 중복 검사 결과

**정정**: 지난 보고서의 "중복 13건"은 실제로는 **콘텐츠 중복이 아니라
보류 사유(다의어 + 품사불명확) 두 개가 같은 단어에 동시에 적용된
경우**를 뜻했다 - 용어를 잘못 썼다. 이번에 실제 "중복"(비교 대상: 후보
내부/기존 DB·문항/다른 배치)을 아래처럼 직접 검사했다.

**비교 대상과 기준(3가지, 명확히 구분)**:

| 구분 | 비교 대상 | 방법 | 결과 |
|---|---|---|---|
| ① 후보 내부 중복 | L3 121건 서로 간 | lemma, (lemma,pos,동형번호), official_meaning_short 텍스트 완전일치 | **0건** |
| ② 기존 DB·문항과의 중복 | `vocabulary_contents`(5,950건 전체)·`vocabulary_items`·`vocabulary_multiformat_items`(모든 source_version) | 선정 30건 표제어를 라이브로 직접 대조 | **0건** |
| ③ 다른 배치와의 중복 | 기존 파일럿 3종(L4/L5 40건, L6 40건, L0~L3 미리보기 184건) 매니페스트 | 선정 30건 표제어를 세 매니페스트와 직접 대조 | **0건** |

**121 = 51 + 30 + 40 집합 확인**: 51(보류) + 30(1차 선정) + 40(잔여) = 121,
라이브 재조회로도 동일(사람 L3 판정 121건 불변). **선정 30건에 미해결
중복은 없었다** - 그래서 제외 처리된 항목도 없다(30/30 전부 적재).

## 2. 최종 콘텐츠·문항 파일 고정 (ID·버전·해시)

**content_id = candidate_id 그대로 재사용**(새 ID 체계 없음 - 이미
(lemma,pos,동형번호,자료버전)의 결정적 함수라 안정적). 적재 직전
30건 전부를 **다시 조회**해 여전히 L3인지 재확인(변경 0건 - "원천 정보가
변경된 판정은 적용하지 않는다"를 실제로 지켰다는 증거).

| 필드 | 값 |
|---|---|
| source_version | `nikl_grade5_l3_batch1_v1` |
| generator_version | `nikl_grade5_l3_batch1_dryrun_v1` |
| 콘텐츠 CSV SHA-256 | `310847c5531a7c453e229f4496484d1ffc21d5705d7bcd9f34a6e6c5497cecbb` |
| 매니페스트 JSON SHA-256 | `a09fb6e455d86271f4cec0c581db5fea02c3513c1e4f1eb7c47e3fc7b3caeb8a` |

원천 식별자(candidate_id)·공식 뜻풀이(canonical_definition)·학생용
뜻풀이(student_definition)·예문·최신 사람 L3 판정을 전부 별도 컬럼으로
연결했다(`data/import/nikl_grade5_l3_batch1_final_content_20261007.csv`).

## 3. 비공개 적재 (GATE 1~11, 연구 DB 실제 적용)

연구 환경(`APP_ENV=research`)·실제 DB 경로
(`/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`)를
확인한 뒤 SQLite Backup API로 백업(`...db.bak_grade5_l3_batch1_20261002-121334`,
integrity_check=ok로 복원 가능성 검증)하고 별도 source_version으로 적재했다.

- GATE 6: 콘텐츠 30건·문항 60건 조립, 전부 `student_exposure=0`/
  `public_ready=0`로 **하드코딩**(입력값 무시).
- GATE 7(동일 ID·동일 내용 SKIP / 동일 ID·다른 내용 중단): 신규 삽입
  30/60건, 충돌 0건.
- GATE 8~9: 단일 트랜잭션 커밋, `integrity_check=ok`, `foreign_key_check`
  위반 0건.
- GATE 10(무관 데이터 불변): RULE_A/B 1,408→1,408, `vocabulary_content_
  levels` 총수 5,950→5,950(**이 30건에는 레벨 행을 아예 만들지 않음** -
  3절 참고), 다른 source_version 콘텐츠/문항 수 불변.
- GATE 11: 적재 30건 전부 `student_exposure=0`/`public_ready=0`.
- **멱등성**: 같은 명령을 다시 실행한 결과 "신규 0, 동일값 스킵 30/60"으로
  **실제 UPDATE/INSERT 0건** - 로컬 사본·연구 DB 둘 다에서 재현 확인.

기존 검증된 임포트 구조(apply_official_grade_reference.py의 GATE 패턴)를
그대로 재사용했고, 매니페스트 구조도 기존 세 파일럿과 동일한 형식
(`data/vocab/*.json`, item_id 화이트리스트)으로 맞췄다.

## 4. 레벨 판정과 검수 상태 분리

- **사람의 L3 판정**: `vocabulary_grade5_candidate_judgments`(기존 테이블,
  이번에 전혀 쓰지 않음 - 읽기만 함).
- **콘텐츠·문항 검수 판정**: `vocabulary_publish_reviews`(momolib 1순위
  공개검토와 같은 테이블·같은 verdict 체계를 재사용하되, 완전히 별도
  content_id 집합·별도 라우트로 분리 - momolib 대상은 건드리지 않음).
- 검수 화면 상세 페이지에는 "사람의 레벨 판정(참고용, 이 화면에서
  바꾸지 않음)"이라는 문구와 근거를 항상 먼저 보여주고, 그 아래 별도
  섹션에서 콘텐츠·문항 검수 판정을 받는다 - **레벨 판정을 문항 승인으로
  확대 해석하지 않는다**.
- 검수 전에는 어떤 판정도 미리 채우지 않는다(폼 제출이 있어야만 행
  생성). 로컬 테스트로 판정 1건 저장 후 직접 재확인한 결과: `vocabulary_
  publish_reviews`에 1행 생성, `vocabulary_grade5_candidate_judgments`의
  해당 레벨 판정은 **그대로 L3 불변**, `student_exposure`/`public_ready`도
  **그대로 0/0 불변**.

## 5. 관리자 미리보기에 4번째 모드로 추가

기존 L4/L5 파일럿·L6 파일럿·L0~L3 확장 미리보기와 완전히 같은 격리·
매니페스트 화이트리스트 설계를 그대로 따라 `app/vocabulary_quiz/routers/
multiformat.py`에 **"L3 중등 보강 1차"** 모드를 추가했다(기존 세 모드·
일반 출제 경로는 한 글자도 건드리지 않음).

- **격리**: `source_version=nikl_grade5_l3_batch1_v1` 전용, `item_types`에
  CROSSWORD 포함 시 422, 다른 세 모드와 동시 선택 시 422.
- **다른 점(명시)**: 이 30건은 레벨 정책 파이프라인을 거친 적이 없어
  `vocabulary_content_levels` 행이 없다 - 그래서 선택 함수가 L4/L5/L6
  파일럿처럼 `level_status`/`vocab_level`을 확인하지 않고, 대신
  `content.is_active=1`·`student_exposure=0`·`public_ready=0`만
  확인한다(가짜 레벨 행을 만들어 "레벨 미확정"을 감추지 않음).
- **조회·응시·채점**: `/api/vocabulary-quiz/grade5-l3-batch1-availability`
  (신규) + 기존 `/sessions`·`/sessions/{id}/next`·`/sessions/{id}/answer`·
  `/sessions/{id}/result`(코드 수정 없이 그대로 재사용 - mode-agnostic).
- **검수**: `/vocab-grade5-l3-batch1-review/`(신규, 4절 참고).

## 6. 검증 · 커밋 · 배포 · 라이브 확인

**로컬 DB 사본으로 실측 검증**(실제 서명 쿠키로 로그인한 상태의 정상
API 경로 - `require_admin`을 가짜로 바이패스하지 않음):

| 항목 | 결과 |
|---|---|
| 접근제어(비로그인) | availability/검수화면 모두 **302** |
| 조회(availability) | 60문항/30고유어휘/유형별 30+30 정확히 일치 |
| 세션 생성 | 정상 200, `pilot_info`/`l6_pilot_info`/`existing_l0l3_preview_info`는 전부 `None`(교차오염 없음) |
| CROSSWORD 거부 | **422** |
| 모드 동시 선택 거부 | **422** |
| 제출 전 정답 비노출 | `/next` 응답 전체에 `correct_option`/`answer_payload` 문자열 **0건** |
| 응시·채점 | 10문항 전부 정상 채점 |
| 결과 조회 | `grade5_l3_batch1_info`에만 값, 다른 세 필드는 `None` |
| 검수 목록/상세 | 정상 렌더링, 모델 예측 관련 문자열 0건 |
| 검수 판정 저장 | 303, `vocabulary_publish_reviews` 1행 생성, 레벨 판정·공개 플래그 불변 |
| 적재 멱등성 | 로컬·연구 DB 둘 다 재실행 시 0건 변경 |
| 비대상 데이터 불변 | GATE 10(5,950건 레벨 테이블·1,408건 RULE_A/B 등) |
| 매니페스트 정합성 | 적재 전/후 매니페스트 60건 화이트리스트와 DB 조회 결과 완전 일치(`PilotBatchIntegrityError` 미발생) |

**커밋·배포**: 코드(13개 파일)를 커밋해 `git push`로 연구 서버에 배포한
뒤, 서버가 새 커밋(86ef070)을 pull했음을 확인하고 **그 서버에서 직접**
`apply_grade5_l3_batch1.py --apply`를 실행해 실제 연구 DB에 적재했다
(로컬에서 만든 파일을 그대로 올려치지 않음 - 배포 후 서버 쪽 경로에서
재실행). 적용 후 같은 명령을 다시 실행해 멱등성(0건 변경)을 연구 DB
에서도 재확인했다.

**라이브 확인**(정상 API 경로, 비로그인 상태로): 홈페이지·신규 검수
화면·신규 availability API 모두 동일하게 **302**(로그인 리디렉션) -
500/404 없이 정상 배포됨을 확인. **실제 관리자 로그인을 통한 화면
클릭 검증은 이 턴에서 브라우저 세션에 접근할 수 없어 수행하지
못했다**(이전 턴들과 동일한 제약) - 아래 URL로 로그인 후 직접 확인
부탁드린다.

**테스트 기록 정리**: 로컬 테스트는 전부 격리된 DB 사본
(`%TEMP%\dbtest4\...`, 테스트 종료 후 삭제)에서만 수행했다 - 연구 DB에는
테스트용 세션(`vocabulary_multiformat_sessions`)이나 테스트 검수 판정
(`vocabulary_publish_reviews`, content_id LIKE 'G5-%')이 **0건**임을 라이브
재조회로 확인했다(정리할 테스트 기록 자체가 없음).

### 실제 검수 URL·메뉴 위치

- **문항 조회·응시(관리자 전용 다유형 퀴즈 화면)**: `https://aprolabs.co.kr/vocabulary-quiz/multiformat/play`
  → "L3 중등 보강 1차 문항만 출제" 체크박스 선택
- **콘텐츠·문항 검수 화면**: `https://aprolabs.co.kr/vocab-grade5-l3-batch1-review/`
- **메뉴 위치**: 좌측 사이드바 "📘 문해력 · 어휘" → "🌱 L3 중등 보강
  1차 검수"(검수 화면), 다유형 퀴즈 화면은 기존 메뉴 경로 그대로(체크박스만
  추가됨).

## 7. 산출 파일

| 파일 | 내용 |
|---|---|
| `app/vocabulary_quiz/routers/multiformat.py` | 4번째 미리보기 모드 추가(기존 세 모드·일반 경로 무변경) |
| `app/templates/vocabulary_quiz/multiformat_play.html` | 체크박스·JS 모드 분기 추가 |
| `app/vocabulary_quiz/grade5_l3_batch1_review.py` | 검수 서비스(vocabulary_publish_reviews 재사용) |
| `app/vocabulary_quiz/routers/grade5_l3_batch1_review.py` | 검수 라우터(신규 경로) |
| `app/templates/vocabulary_quiz/grade5_l3_batch1_review_index.html`/`_detail.html` | 검수 화면 템플릿 |
| `data/vocab/nikl_grade5_l3_batch1_manifest_v1.json` | 60문항 고정 매니페스트 |
| `data/import/nikl_grade5_l3_batch1_final_content_20261007.csv` | 30콘텐츠 최종본(ID·버전 고정) |
| `scripts/vocab/apply_grade5_l3_batch1.py` | 적재 스크립트(GATE 1~11) |

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
