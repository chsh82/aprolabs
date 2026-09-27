# momolib 관리자용 어휘 퀴즈 배포 리허설

- 일자: 2026-09-28
- main 병합·push·운영 DB 변경·실제 배포: **없음**(이번 단계 전부 준비/리허설)

## 1. 커밋·export 패키지 고정

| 저장소 | 커밋 |
|---|---|
| momolib | `77dd11e` (+ 리허설 중 발견한 테스트 버그 수정 `199f1e7`, 브랜치 `feat/vocab-quiz-admin-migration`) |
| aprolabs | `323f2ae` |

### export 패키지 파일·해시(고정)
| 파일 | sha256 | 건수 |
|---|---|---:|
| `contents.json` | `b4e6c511...b32315` | 227 |
| `content_levels.json` | `9316957a...02a3d` | 227 |
| `export_manifest.json` | `a4e2d4d9...2467e` | - |
| `data/vocab/pilot_l4l5_manifest_v1.json` (= momolib `data/vocab_quiz/pilot_l4l5_manifest_v1.json`) | `96b7da7e...82e45` | 40 |
| `data/vocab/pilot_l6_manifest_v1.json` (= momolib 쪽 동일 경로) | `f9aaae5a...80d` | 40 |

aprolabs export 디렉터리의 `pilot_l4l5_items.json`/`pilot_l6_items.json`은
위 두 매니페스트 파일과 **내용이 정확히 동일**(다른 파일명의 중복 기록,
`json.load` 비교로 확인) - momolib이 실제로 읽는 운영 파일은
`data/vocab_quiz/pilot_*.json` 쪽이다.

### 실제 이식 건수(다시 한번 명시)
- **콘텐츠 227건 / 문항(파일럿) 80건(l4l5 40 + l6 40) = 총 307행 규모**
- **연구 DB 전체 5,950건 중 227건만 이식했다 - "5,950건을 이식했다"는
  표현은 어디에도 쓰지 않는다.** (5,950/1,369는 export 스크립트가 사전에
  확인만 하는 전체 DB 기준선이며, 실제 이식 대상이 아니다.)

### 227↔80 연결 확인(리허설 DB에 실제 적재해 SQL로 직접 확인, 파일 대조가 아님)
```sql
SELECT COUNT(*) FROM vocab_quiz_contents;                         -- 227
SELECT COUNT(*) FROM vocab_quiz_content_levels;                    -- 227
SELECT pilot_key, COUNT(*) FROM vocab_quiz_pilot_items GROUP BY 1;  -- l4l5=40, l6=40
SELECT COUNT(*) FROM vocab_quiz_pilot_items p
  WHERE p.source_content_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM vocab_quiz_contents c WHERE c.content_id=p.source_content_id);
                                                                     -- 0 (고아 없음)
SELECT COUNT(DISTINCT source_content_id) FROM vocab_quiz_pilot_items; -- 40
```
**결론**: 파일럿 80문항 전부 227건 안의 콘텐츠를 정확히 참조한다(고아 0건).
다만 227건 중 실제로 파일럿이 걸려 있는 것은 40건(l4l5 20 + l6 20)뿐이고,
나머지 187건은 미리보기 전용(파일럿 없음) - "80문항이 227건 전체에
연결"되는 게 아니라 "80문항이 227건 중 40건에, 고아 없이 연결"된다.

## 2. momolib 실제 설정 재확인 (push 없이 확인 가능한 것만)

| 항목 | 확인 결과 | 확인 방법 |
|---|---|---|
| 자동배포 트리거 | `.github/workflows/deploy.yml` - `main` 브랜치 **push 시에만** 발동 | 로컬 파일 직접 읽음 |
| 배포 절차 | `git pull origin main` → venv 활성화 → `pip install -r requirements.txt` → `chmod` 2회 → `sudo systemctl restart momolib` | 위와 동일 |
| **마이그레이션 실행 방식 - 중요** | **자동배포 스크립트 어디에도 `flask db upgrade`가 없다.** 이 저장소에는 `deploy.sh`도 없다 - 마이그레이션은 **완전히 수동**(서버에 SSH로 들어가 직접 실행)이라는 뜻이다. | `deploy.yml` 전문 확인 + `grep -r "db upgrade\|flask db"` 결과 0건 |
| 관리자 역할명 | `super_admin`(role_level 1), `hq_manager`(2), `hq_essay_manager`(3) 외 `branch_owner`(4)/`branch_manager`(5)/`teacher`(6)/`parent`(7)/`student`(8) - `app/models/user.py:21-28,46-52`. 이번 기능은 `requires_role('super_admin', 'hq_manager')` 사용(`app/vocab_quiz/routes.py`) | 코드 직접 확인 |
| 운영 환경변수 실제 값 | **확인 불가** - 아래 3번 참고 |

### 운영 서버 접속 시도 결과(실패, 투명하게 기록)
momolib 배포 서버(`34.158.201.115`, user `chsh82`, `.github/workflows/deploy.yml` 기준)에
로컬에 있는 키 2개(`~/.ssh/momolib_gcp`, `~/.ssh/momoai_gcp`)로 접속을
시도했으나 **둘 다 `Permission denied (publickey)`로 거부됐다.** 실제 배포
키는 GitHub Actions Secret(`SSH_PRIVATE_KEY`)에만 있고 로컬에는 없는 것으로
보인다. 더 많은 키를 무작위로 시도하지 않고 여기서 멈췄다 - **운영
서버의 실제 `FLASK_ENV`/`DATABASE_URL` 값은 이번 세션에서 직접 확인하지
못했다.** (참고: momolib은 aprolabs처럼 `APP_ENV`라는 변수명을 쓰지
않는다 - `config.py`가 읽는 변수는 `FLASK_ENV`이며, `run.py:4`가
`create_app(os.environ.get('FLASK_ENV', 'development'))`로 전달한다.)

## 3. 운영 DB 백업 복원 리허설 — **격리 사본 불가, 로컬 재현으로 대체(한계 명시)**

2번의 SSH 접속 실패로 운영 서버의 `pg_dump` 백업 자체를 가져올 방법이
없어, **운영 PostgreSQL 백업을 읽기 전용으로 복원한 리허설은 이번
세션에서 수행하지 못했다.** 대신 지시사항의 대체 조건("격리 사본을 만들
수 없다면 기존 로컬 PostgreSQL에서 동일 절차를 재실행하고 한계를
명시")에 따라, 이전 단계에서 이미 구축한 **로컬 독립 PostgreSQL**
(portable 16.4, 관리자 권한 불필요, 운영과 무관)에 **새 리허설 전용
DB**(`momolib_vocab_rehearsal`)를 만들어 처음부터 재현했다:

1. **베이스라인 재현**: 이번 이식 코드를 적용하기 *전* 커밋(`e04834a`)으로
   임시 체크아웃해 `db.create_all()` + `flask db stamp head`로
   "마이그레이션 적용 전 운영과 동일한 스키마" 상태를 만듦(47개 테이블).
2. `feat/vocab-quiz-admin-migration`으로 복귀 → `flask db upgrade` →
   52개 테이블(+5, 정확히 신규 테이블만).
3. `import_vocab_quiz_export.py` dry-run → `--apply`(227/227/40/40 삽입).
4. 관리자 응시·채점 통합 테스트(22건) 실행 - 전부 통과.
5. **재실행 멱등성**: `--apply` 재실행 시 신규 0건(전부 스킵) - *단, 통합
   테스트 4번 실행 직후 1회는 거짓 CONFLICT가 발생했다*: 통합 테스트의
   "매니페스트 불일치 복원" 단계가 삭제한 문항을 복원할 때
   `source_content_id`를 빠뜨려 원본과 미세하게 다른 행을 남겼기 때문 -
   **리허설 중 실제로 발견한 버그**이며 즉시 수정했다(momolib
   `199f1e7`). 수정 후 재실행하니 정상적으로 신규 0건(완전한 멱등성).
6. **롤백**: `flask db downgrade` → 신규 테이블 5개 완전 제거(47개로
   복귀) 확인. 기존 `BankQuestion`/`QuizQuestion`/`Curriculum` 행수는
   시작부터 끝까지 0건 그대로(변경 없음).
7. 리허설 DB 삭제, 로컬 PostgreSQL 프로세스 정상 종료.

### 한계(정직하게 명시)
- **실제 운영 데이터로 리허설한 것이 아니다** - 로컬 리허설 DB는 momolib의
  현재 코드가 만드는 스키마와 동일하지만, 운영 서버에 쌓인 실제 행 수·
  데이터 특이사항(예: 대용량 테이블의 인덱스 성능, 실제 FK 데이터 분포)은
  반영돼 있지 않다.
- 운영 `FLASK_ENV`/`DATABASE_URL`이 로컬 재현과 동일한지 확인하지 못했다.
- 운영 PostgreSQL 버전이 로컬(16.4)과 다를 가능성이 있다(확인 못함).
- **권장**: 실제 배포 전에 사용자가 직접 운영 서버에서 `pg_dump`를 받아
  격리 인스턴스에 복원해 이 리허설을 다시 한 번 실행하거나, 최소한 운영
  PostgreSQL 버전과 `FLASK_ENV` 값을 확인해 알려주면 이 문서를 갱신할 수
  있다.

## 4. 확인 결과 요약

| 확인 항목 | 결과 |
|---|---|
| 기존 BankQuestion/QuizQuestion/LMS 스키마·행 수 | **불변**(리허설 전/후 0건 그대로, 마이그레이션 diff도 신규 테이블 5개뿐) |
| 227개 콘텐츠·파일럿 80문항 적재 | **정확**(227/227/40/40, SQL로 직접 확인) |
| 80문항의 227건 참조 정합성 | **고아 0건**, 실제 파일럿 연결은 40건(20+20), 나머지 187건은 미리보기 전용 |
| 비관리자(teacher) 차단 | 403 확인 |
| 비로그인 차단 | 302(로그인 리다이렉트) 확인 |
| 정답 사전 비노출 | 응시 화면 응답 본문에 `answer_payload_json`/`correct_option` 미포함 확인 |
| 학생용 라우트 미연결 | `app/vocab_quiz/`는 CMS(`templates/cms/vocab`)·LMS·기존 학생 포털 라우트를 어디서도 import/참조하지 않음(코드 검색 재확인) |

## 5. 배포 체크리스트 (실행 가능, 순서·중단조건·복구 포함)

**전제**: 아래는 사용자가 실제로 실행할 명령이다. 이번 세션은 이 중 어느
것도 대신 실행하지 않았다(main 병합·push·운영 DB 변경·배포 없음).

### 5-1. 병합 전
```bash
# 1) PR 리뷰 - momolib feat/vocab-quiz-admin-migration -> main
#    diff에 아바타 커밋(91de794 등)이 섞이지 않았는지 재확인
git log --oneline origin/main..feat/vocab-quiz-admin-migration

# 2) 로컬에서 마지막으로 한 번 더 dry-run (독립 로컬 PG 권장)
python import_vocab_quiz_export.py            # dry-run
python tests/test_vocab_quiz_import.py         # 16건
python tests/test_vocab_quiz_integration.py    # 22건
```
**중단 조건**: 위 세 명령 중 하나라도 실패하면 병합하지 않는다.

### 5-2. 병합 + 배포(자동)
```bash
git checkout main && git merge feat/vocab-quiz-admin-migration
git push origin main   # 이 순간 GitHub Actions가 자동으로 git pull+재시작을 실행함
```
**중단 조건**: push 전에 `git log origin/main..main`으로 의도한 커밋만
있는지 마지막으로 확인. 의도치 않은 다른 변경이 섞여 있으면 push하지
않는다.

### 5-3. 마이그레이션(수동, 자동배포에 없음 - 반드시 이 단계를 잊지 말 것)
```bash
ssh chsh82@<momolib-server>
cd ~/momolib && source venv/bin/activate
flask db upgrade   # 신규 테이블 5개만 생성됨(다른 테이블 영향 없음)
flask db current   # head가 37b334ba3fc8(또는 그 이후)인지 확인
```
**중단 조건**: `flask db upgrade` 실패 시 즉시 `flask db downgrade`로
되돌리고 배포를 중단, 원인 파악 전까지 아래 5-4를 진행하지 않는다.

### 5-4. 데이터 import(수동, 서버에서 실행)
```bash
# export 패키지가 서버의 ~/momolib/data/vocab_quiz_import/,
# data/vocab_quiz/에 이미 git으로 배포돼 있어야 함(git pull로 자동 포함됨)
python import_vocab_quiz_export.py             # dry-run 먼저
python import_vocab_quiz_export.py --apply     # 227/227/40/40 삽입 기대
```
**중단 조건**: dry-run 출력의 "신규" 건수가 227/227/40/40이 아니면(즉
서버의 export 패키지가 로컬과 다르면) `--apply`를 실행하지 않고 원인
파악. `--apply` 실행 중 `FAIL(CONFLICT)`가 뜨면 그 즉시 아무것도 커밋되지
않았으므로 안전 중단 - 충돌 항목만 확인하고 재시도.

### 5-5. 배포 후 확인
```bash
# 관리자 계정으로 브라우저에서
GET https://<momolib-domain>/vocab-quiz/            # 200, 총 227건 표시
GET https://<momolib-domain>/vocab-quiz/pilot/l4l5  # "일치, 응시 가능"
GET https://<momolib-domain>/vocab-quiz/pilot/l6    # "일치, 응시 가능"
# 비관리자 계정(또는 시크릿창 비로그인)으로
GET https://<momolib-domain>/vocab-quiz/            # 403 또는 로그인 리다이렉트여야 함
```

### 5-6. 복구(문제 발생 시)
```bash
# 데이터만 되돌리기(스키마는 유지) - 신규 5개 테이블을 비움
psql $DATABASE_URL -c "
  DELETE FROM vocab_quiz_pilot_attempts;
  DELETE FROM vocab_quiz_pilot_sessions;
  DELETE FROM vocab_quiz_pilot_items;
  DELETE FROM vocab_quiz_content_levels;
  DELETE FROM vocab_quiz_contents;
"
# 스키마까지 완전히 되돌리기
cd ~/momolib && source venv/bin/activate
flask db downgrade   # 신규 테이블 5개 제거, 기존 테이블 무관(리허설로 확인함)

# 코드 자체를 되돌려야 하면(라우트/블루프린트 제거)
git revert <병합 커밋 SHA>
git push origin main   # 자동배포로 원상복구
```

## 6. 이번 단계에서 하지 않은 것
main 병합, push, 운영 PostgreSQL 접속·변경, 실제 배포. 운영 서버 SSH
접속은 시도했으나 권한 거부로 실패했고, 더 이상 시도하지 않았다.
