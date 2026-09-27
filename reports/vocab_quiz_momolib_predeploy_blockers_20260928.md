# momolib 어휘 퀴즈 배포 전 차단 조건 조사 — 운영 배포는 **BLOCKED**

- 일자: 2026-09-28
- momolib 커밋: `77dd11e` → `199f1e7` → `ee158fd`(이번 단계, 브랜치
  `feat/vocab-quiz-admin-migration`), aprolabs: 이 보고서
- **운영 판정: BLOCKED**(4절) - main 병합·push·운영 DB 쓰기·실제 배포는
  이번에도 하지 않았다.

## 1. 마이그레이션 미적용 상태에서 신규 코드 기동 영향 (로컬 재현, 9/9 확인)

로컬 독립 PostgreSQL에 **구 코드(커밋 `e04834a`)로 만든 베이스라인**
(vocab_quiz_* 테이블 없음, 47개 테이블)을 만들고, 그 위에서 **신규
코드**(`create_app()`)를 그대로 기동해 확인했다
(`tests/test_vocab_quiz_premigration_boot.py`):

| 상황 | 결과 |
|---|---|
| `create_app()` 자체(앱 부팅) | **영향 없음** - 신규 모델 import는 DB 스키마를 요구하지 않아 정상 성공 |
| 기존 라우트(`/`, `/auth/login`) | **영향 없음** - 신규 라우트를 한 번도 안 건드리면 평소와 동일 |
| 비로그인 상태로 `/vocab-quiz/` 접근 | 인증 게이트(`@login_required`)가 DB 쿼리보다 먼저 걸려 **302**(DB 접근 자체가 발생하지 않음) |
| 관리자로 로그인해 `/vocab-quiz/` 접근(테이블 없음) | **이 요청만 실패**. development(DEBUG=True) 설정은 미처리 예외가 그대로 전파(테스트 클라이언트 기준 - 실 서버라면 브라우저에 Werkzeug 디버거가 뜰 수 있는 상황), **production(DEBUG=False, 실제 배포 설정) 은 깨끗한 500 응답**으로 처리됨(Flask 표준 예외 처리) |
| 신규 라우트 실패 *직후* 기존 라우트 재요청 | **정상**(같은 세션에서도 다른 요청은 전혀 영향받지 않음) |

**결론**: 마이그레이션이 늦게 적용돼도 "새 라우트를 열 때만" 실패하고,
**앱 전체 기동이나 기존 기능에는 어떤 영향도 없다** - 다만 실제 서버가
`DEBUG=True`로 잘못 설정돼 있으면 안 된다(momolib `config.py`의
`ProductionConfig`는 `Config.DEBUG=False`를 상속하므로 정상 설정상
문제없음 - 실제 운영 환경변수가 이 클래스를 그대로 쓰는지는 2절/4절
참고).

## 2. "마이그레이션 파일 먼저 전달 → 백업·upgrade → 코드 배포" 절차 설계 및 로컬 검증

### 현재 자동배포(`.github/workflows/deploy.yml`, 변경 없음, 그대로 인용)
```yaml
on: push: branches: [main]
script: |
  cd ~/momolib
  git pull origin main        # <- 코드와 마이그레이션 파일이 동시에 들어옴
  source venv/bin/activate
  pip install -r requirements.txt -q
  chmod -R 755 ~/momolib/app/static
  chmod o+x /home/chsh82
  sudo systemctl restart momolib   # <- 재시작 시점부터 신규 코드가 곧바로 살아남
```
**문제**: `git pull`이 코드와 마이그레이션 파일을 **동시에** 가져오고,
그 직후 `restart`가 즉시 일어난다. `flask db upgrade`가 이 흐름 어디에도
없다(0건 확인, 재확인). 즉 지금 이대로 배포하면:
1. 재시작 순간부터 신규 라우트 코드가 살아있음
2. 그런데 마이그레이션은 **누군가 나중에 수동으로** 실행해야 함
3. 그 사이(수 초~수 시간)는 "코드가 스키마보다 먼저 배포된 구간"이다

1절 결과로 이 구간이 **안전하다는 것**(기존 기능 무영향, 신규 라우트만
500)은 확인했지만, 사용자가 명시적으로 "그 구간 자체가 생기지 않게"
요구했으므로 아래처럼 순서를 바꾼다.

### 설계한 절차(코드가 스키마보다 먼저 배포되는 구간을 원천 차단)
```bash
# --- 0. (사전) 로컬에서 이 순서를 검증(2-1절에서 실제로 재현) ---

# --- 1. 마이그레이션 파일만 서버로 전달 (git pull 아님, scp만) ---
scp -i <서버 접속 키> \
    migrations/versions/37b334ba3fc8_add_vocab_quiz_tables_isolated_admin_.py \
    chsh82@<momolib-server>:~/momolib/migrations/versions/

# --- 2. 서버에서 백업 ---
ssh chsh82@<momolib-server>
pg_dump "$DATABASE_URL" -Fc -f ~/momolib_backup_pre_vocabquiz_$(date +%Y%m%d-%H%M%S).dump
pg_restore --list ~/momolib_backup_pre_vocabquiz_*.dump | head   # 백업 파일 자체 유효성 확인

# --- 3. 구 코드로 마이그레이션만 적용 (git pull 전, 아직 신규 코드 없음) ---
cd ~/momolib && source venv/bin/activate
flask db upgrade         # <- 이 시점 앱 코드는 여전히 구버전, 신규 모델 파일 없어도 성공함
flask db current         # head가 37b334ba3fc8인지 확인

# --- 4. 이제서야 코드 배포(기존 자동배포와 동일) ---
#    (로컬에서) git push origin main
#    -> GitHub Actions: git pull + pip install + restart
#    이 시점에는 이미 테이블이 존재하므로 restart 즉시 /vocab-quiz/도 정상
```

### 이 절차를 로컬에서 실제로 재현해 검증함
1. 구 코드(`e04834a`)로 베이스라인(47개 테이블) 생성.
2. **구 코드 체크아웃 상태에서, 신규 마이그레이션 파일 하나만
   `migrations/versions/`에 수동 복사**(실제 scp를 흉내).
3. `flask db upgrade` 실행 → **성공**(에러 없음). `app/models/vocab_quiz.py`
   가 아직 없어도(구 코드 상태) 문제없이 5개 테이블이 만들어짐 - alembic
   `upgrade()`는 리비전 파일에 이미 적힌 raw SQL(`op.create_table`)만
   실행하고, ORM 모델 클래스 존재 여부와 무관하기 때문.
4. 같은 구 코드로 `bank_questions` 등 기존 테이블 조회 - 정상.
5. 신규 테이블 5개가 실제로 생성됐음을 확인.

**결론**: 마이그레이션 파일만 먼저 전달해 구 코드 상태에서 백업·upgrade를
끝내고, 그 다음에야 `git push`로 코드를 배포하면 "코드가 스키마보다
먼저 배포되는 구간"이 **완전히 사라진다.** 이 설계는 로컬에서 실제로
재현해 성공을 확인했지만, **운영 워크플로 파일(`deploy.yml`)은 이번에도
전혀 수정하지 않았다** - 위 절차는 사용자가 수동으로 실행하는 절차이며,
자동배포 자체를 바꾸자는 것이 아니다.

## 3. SSH 접속 실패 원인 조사 (비밀키 내용 미출력, 새 키 생성 안 함)

### 확인한 사실
- 서버: `34.158.201.115`, 사용자 `chsh82`(`.github/workflows/deploy.yml`
  기준) - 네트워크·포트는 정상(핸드셰이크 성공).
- `ssh -v` 결과: `Authentications that can continue: publickey`(비밀번호
  등 다른 인증 방식 없음), 클라이언트가 키를 정상적으로 제시했으나
  **서버가 그 공개키를 모른다(`Permission denied (publickey)`)** -
  네트워크·경로·형식 문제가 아니라 **서버 authorized_keys 등록 문제**로
  확정.
- 로컬 후보 키 3개의 공개키 지문(비밀키 내용 아님, 안전하게 공개 가능):

| 키 파일 | 지문(SHA256) | .pub 코멘트 | 시도 결과 |
|---|---|---|---|
| `momolib_gcp` | `ZMjEX1ekujXGgD2UCgN+Ibg8iVlORXRRf6GzqySm7+Q` | `momolib` | `chsh82`로 거부, `momolib` 사용자로도 거부 |
| `momolib_local` | `NrgHhRZaTHfZzucSqro/0db5Rkt6GfxyojUWTVVzjm0` | `chsh82` | 시도 안 함(이름상 로컬용으로 판단, 서버 키 아닐 가능성) |
| `momoai_gcp` | `7ENn1AkXf+xI/2OFTdN+sb4rHio26Xl1BbxE7pbyKXQ` | `claude-code@momoai` | `chsh82`로 거부 |
| (참고) `aprolabs_deploy` | `TAJZABoPGR3tBEun8CgiefQ3wtx4PECuHCvkX9pBwzc` | `github-actions` | **aprolabs 서버에서는 성공** - 비교 기준 |

`~/.ssh/config`에는 `aprolabs` 호스트 별칭만 있고 momolib 서버용 별칭은
없다. 실제 배포에 쓰이는 키는 GitHub Actions Secret
(`secrets.SSH_PRIVATE_KEY`)에만 있고 **로컬 파일로는 존재하지 않는
것으로 보인다** - `momolib_gcp`는 이름과 코멘트로 볼 때 이 목적으로
만들어졌으나, 서버의 `authorized_keys`에 등록되는 단계가 완료되지 않은
것으로 추정된다(등록 여부를 서버에 접속해서 확인할 방법이 없어 추정에
그침).

### 필요한 것 (정확히 이것만 있으면 재개 가능)
**둘 중 하나**:
1. 사용자가 서버(`chsh82@34.158.201.115`)의 `~/.ssh/authorized_keys`에
   `momolib_gcp.pub`(지문 `ZMjEX1e...`)를 직접 추가해 주거나,
2. 이미 그 서버에 등록된 다른 개인 키/자격 증명을 알려주면(이 세션이
   접근 가능한 경로로) 그것으로 재시도.

이 세션은 더 이상 키를 추측·생성하거나 다른 사용자명으로 반복 시도하지
않는다.

## 4. 운영 배포 판정: **BLOCKED**

다음 3가지를 이번에도 확인하지 못했다(3절의 접속 실패가 직접 원인):
- 운영 `FLASK_ENV`/`DATABASE_URL`의 **실제 값**(로컬 파일에는 값이 없고,
  서버의 실제 환경변수를 읽을 방법이 없음)
- 운영 PostgreSQL **백업 가능 여부**(서버에서 `pg_dump`를 실행해 볼
  방법이 없음)
- **실제 마이그레이션 대상 DB**가 로컬 리허설과 같은 스키마 상태인지
  (운영 DB의 현재 alembic head가 `c3d4e5f6a7b8`인지 직접 확인 못함)

**따라서 운영 배포 판정은 BLOCKED이다.** 1절·2절의 로컬 재현이 전부
통과했다는 사실은 "로컬 절차가 안전하게 설계됐다"는 것만 증명하며,
**"운영 배포 준비 완료"로 바꾸지 않는다.** 이 판정은 3절의 접속 문제가
해결되고 위 3가지가 직접 확인될 때까지 유효하다.

## 5. 이번 단계에서 완성한 것 (로컬 브랜치만, push 없음)

| 파일 | 내용 |
|---|---|
| `tests/test_vocab_quiz_premigration_boot.py`(momolib, 커밋 `ee158fd`) | 1절 검증(9/9 통과) - development/production 설정 차이까지 구분 |
| 이 보고서(aprolabs) | 2절 설계+로컬 검증, 3절 SSH 진단, 4절 BLOCKED 판정 |

기존 `tests/test_vocab_quiz_import.py`/`test_vocab_quiz_integration.py`
(`77dd11e`/`199f1e7`)는 이번에 다시 실행해 여전히 전부 통과함을
재확인했다(수정 없음).

## 6. 하지 않은 것
운영 PostgreSQL 접속·쓰기, `main` 병합, push, 실제 배포,
`.github/workflows/deploy.yml` 수정, SSH 키 신규 생성, 비밀키 내용 출력.
