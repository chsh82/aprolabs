# aprolabs 작업 완료 성과 검토 보고서 — 2026-10-08

작성 시각: 2026-10-08 01:16:21 KST
작성 주체: Davinci profile
대상 저장소: `C:/Users/aproa/aprolabs`
서버 저장소: `/home/chsh82/aprolabs`

## 1. 결론

이번 작업 묶음은 완료 상태다.

완료 기준:

- 로컬 Git 작업트리 clean.
- `origin/main` 최신 커밋까지 push 완료.
- 서버 repo가 `origin/main`과 동기화됨.
- `aprolabs.service` active.
- 어휘 DB 전환 핵심 범위 완료.
- schema/literacy 잔여 변경과 historical artifacts 정리 완료.
- momo worksheet page editor 잔여 변경 정리 완료.
- Git에 넣지 않을 local-only 산출물은 별도 artifact 폴더로 이동하고 SHA-256 manifest 작성 완료.

최종 HEAD:

- local HEAD: `b49b0ca`
- origin/main: `b49b0ca`
- server HEAD: `b49b0ca`
- server origin/main: `b49b0ca`

서비스:

- `aprolabs.service`: `active`

## 2. 대표 성과 요약

### 2.1 어휘 DB 전환 프로젝트 완료

주요 완료 사항:

- L3 옵션1 whitelist 기반 연결 완료.
- RULE_A 1,368건 live DB 적용 완료.
- RULE_B 40건은 정책상 보류로 정리.
- 관리자 API smoke와 앱 선택 함수 smoke 완료.
- 벨기에 residue 2개 item 원인 확인 및 비활성화 dry-run 완료.
- 감사 추적 보고서와 DB 적용 보고서를 GitHub/server에 동기화.

최종 live availability:

| level/mode | words | items |
|---|---:|---:|
| L2 all_candidates | 166 | 625 |
| L2 auto_only | 78 | 289 |
| L3 all_candidates | 75 | 170 |
| L3 auto_only | 8 | 31 |

안전 경계:

- L3 option1은 broad `source_version` 확장이 아니라 manifest 기반 `item_id` whitelist만 허용.
- `level=None` 일반/혼합 모드는 기존 `SOURCE_VERSION="2.1.29"` 동작 유지.
- `confidence_mode=auto_only`는 option1 `REVIEW_BOUNDARY` 보강분을 포함하지 않음.
- 학생 공개 플래그, allowlist, feature flag 변경 없음.

### 2.2 schema/literacy 안전 변경 정리

커밋: `76e4769` — `fix: hold ambiguous krdict textbook fallbacks`

주요 완료 사항:

- momo-textbook krdict fallback에서 `definitions[0]`를 무조건 채택하던 위험 방지.
- 원본 정의가 없고 krdict 동음이의/다의어가 있으면 자동 채택하지 않고 보류.
- 회귀 테스트 추가: `tests/test_krdict_fallback_hold_fix.py`.
- phase15/16 caution 필드 보정 및 `집단` 문구 조사 오류 수정.

검증:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile scripts/literacy/import_textbook_vocab.py tests/test_krdict_fallback_hold_fix.py
C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_krdict_fallback_hold_fix.py
```

결과:

- py_compile: pass
- 회귀 테스트: `29/29 passed`

### 2.3 momo worksheet page editor 정리

커밋: `8046e25` — `feat: refine momo worksheet page editor controls`

주요 완료 사항:

- `set_image_layout` proposal operation 추가.
- 참고자료/발췌문 그림 캡션·높이 조절 지원.
- 표지/1단계/3단계 페이지 직접 편집 불가 사유를 UI에 표시.
- `restore_revision()`이 최신 overrides와 과거 HTML을 섞어 QA가 거짓 FAIL하던 문제 수정.
- PDF pixel diff 도구 추가: `momo_book_db/worksheet/scripts/pdf_pixel_diff.py`.

검증:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile app/routers/momo_worksheet_page_editor.py momo_book_db/worksheet/editor/page_store.py momo_book_db/worksheet/scripts/pdf_pixel_diff.py
node --check momo_book_db/worksheet/scripts/blocks.js
node --check momo_book_db/worksheet/scripts/paginate.js
```

결과:

- Python syntax: pass
- JavaScript syntax: pass

제한:

- 실제 Playwright generate/check smoke는 로컬 browser executable 누락/권한 문제로 차단됨.
- 이 제한은 기존 Hermes/browser tool 권한 blocker와 같은 환경 계열 문제로 분류.

### 2.4 historical audit artifacts 보존

커밋: `b49b0ca` — `docs: archive schema reading audit artifacts`

주요 완료 사항:

- schema-reading phase1~13 보고서 보존.
- schema/literacy dry-run, apply, snapshot, crosscheck JSON/CSV/JSONL 보존.
- vocabulary quiz pilot 관련 2026-09-28~29 보고서 보존.
- 총 `55 files changed, 26273 insertions(+)`.

비밀값 점검:

- `api_key`, `secret`, `token`, `password`, `authorization`, `bearer`, `sk-` 패턴을 스캔.
- `_DATE_TOKEN` false-positive 외 credential 패턴은 발견하지 못함.
- `.env*` 또는 secret 원문은 읽거나 출력하지 않음.

### 2.5 local-only artifact 보존 정리

Git에 넣지 않을 파일 2개를 repo 밖 보존 폴더로 이동했다.

보존 위치:

```text
C:\Users\aproa\Documents\Agent-Work\artifacts\aprolabs_closeout_20261008\
```

보존 manifest:

```text
C:\Users\aproa\Documents\Agent-Work\artifacts\aprolabs_closeout_20261008\manifest.json
```

보존 항목:

- `l2_check.json`
  - size: `14,860`
  - sha256: `62c1682565a7796ae733410a5d23c1f8a558569af1c0420e254d508b2667ed7b`
- `vocab_transition_option1_general_l3_deploy_preservation_pack_20261007.zip`
  - size: `34,287`
  - sha256: `1836e50a10adc2abf911f4f59195b5e442c510dc5f0890753c4f49d20b43f0e7`

## 3. 주요 커밋 목록

최신순:

- `b49b0ca` — docs: archive schema reading audit artifacts
- `8046e25` — feat: refine momo worksheet page editor controls
- `76e4769` — fix: hold ambiguous krdict textbook fallbacks
- `a1799ff` — docs: close out vocabulary transition project
- `f65aa94` — docs: add post RULE_A vocabulary follow-up
- `d1c5fde` — docs: record aprolabs worktree cleanup status
- `647f49c` — docs: record RULE_B post RULE_A review
- `e627860` — docs: record RULE_A live DB apply
- `b3d8763` — docs: add RULE_A dry-run approval artifacts
- `6b33147` — docs: add L3 option1 transition audit trail
- `c0b215d` — L3 보강 옵션1 일반 L3 후보 연결 및 연구사이트 배포 기록

## 4. 산출물 위치

핵심 보고서:

- `reports/vocab_transition_project_closeout_20261007.md`
- `reports/schema_reading_cleanup_commit_plan_20261008.md`
- `reports/momo_worksheet_page_editor_cleanup_20261008.md`
- `reports/schema_reading_historical_artifacts_cleanup_20261008.md`
- `reports/aprolabs_completion_review_report_20261008.md`

핵심 코드/데이터:

- `app/vocabulary_quiz/routers/multiformat.py`
- `data/vocab/l3_general_inclusion_option1_manifest_v1.json`
- `tests/test_l3_general_inclusion_option1.py`
- `scripts/literacy/import_textbook_vocab.py`
- `tests/test_krdict_fallback_hold_fix.py`
- `app/routers/momo_worksheet_page_editor.py`
- `app/templates/momo_worksheet_editor/pages.html`
- `momo_book_db/worksheet/editor/page_store.py`
- `momo_book_db/worksheet/scripts/blocks.js`
- `momo_book_db/worksheet/scripts/paginate.js`
- `momo_book_db/worksheet/scripts/pdf_pixel_diff.py`

Git 밖 보존 artifact:

- `C:\Users\aproa\Documents\Agent-Work\artifacts\aprolabs_closeout_20261008\manifest.json`
- `C:\Users\aproa\Documents\Agent-Work\artifacts\aprolabs_closeout_20261008\l2_check.json`
- `C:\Users\aproa\Documents\Agent-Work\artifacts\aprolabs_closeout_20261008\vocab_transition_option1_general_l3_deploy_preservation_pack_20261007.zip`

## 5. 실행·검증 명령 요약

최근 최종 검증:

```bash
git -C C:/Users/aproa/aprolabs status --short
git -C C:/Users/aproa/aprolabs rev-parse --short HEAD
git -C C:/Users/aproa/aprolabs rev-parse --short origin/main
ssh -o BatchMode=yes -o ConnectTimeout=8 aprolabs 'cd /home/chsh82/aprolabs && git rev-parse --short HEAD && git rev-parse --short origin/main && systemctl is-active aprolabs.service'
```

결과:

- local status: clean
- local HEAD: `b49b0ca`
- origin/main: `b49b0ca`
- server HEAD: `b49b0ca`
- server origin/main: `b49b0ca`
- service: `active`

코드/테스트 검증:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile scripts/literacy/import_textbook_vocab.py tests/test_krdict_fallback_hold_fix.py
C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_krdict_fallback_hold_fix.py
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile app/routers/momo_worksheet_page_editor.py momo_book_db/worksheet/editor/page_store.py momo_book_db/worksheet/scripts/pdf_pixel_diff.py
node --check momo_book_db/worksheet/scripts/blocks.js
node --check momo_book_db/worksheet/scripts/paginate.js
```

결과:

- krdict fallback 회귀 테스트: `29/29 passed`
- Python syntax checks: pass
- JavaScript syntax checks: pass

어휘 DB 측 smoke/검증은 각 전용 보고서에 기록되어 있다.

## 6. 미해결 또는 보류 항목

### 6.1 Hermes/browser/Playwright tooling blocker

상태: 미해결, 프로젝트 핵심 완료에는 영향 없음.

증상:

- `agent-browser CLI not found`
- `[WinError 5] 액세스가 거부되었습니다`
- Playwright smoke 시 `chromium_headless_shell-1234` executable 없음

확인된/관련 경로:

- `C:\Users\aproa\AppData\Local\hermes\tools\agent-browser-0.26.0-win32-x64`
- `C:\Users\aproa\AppData\Local\hermes\tools\chromium-1208`
- `C:\Users\aproa\AppData\Local\hermes\tools\chromium_headless_shell-1234\...`

판정:

- 현재 비승격 세션에서는 권한 복구 불가.
- 관리자 권한 Windows 세션에서 stale tool directory 권한 복구 또는 삭제 후 재설치 필요.

### 6.2 RULE_B 40건

상태: 보류.

이유:

- 방향이 `L2 -> L1`이라 L2 공급량 확대 목표와 반대.
- 별도 정책 승인과 dry-run이 필요.

### 6.3 Belgium residue 2 active items

상태: 보류, 안전.

이유:

- active item 2개는 남아 있지만 option1 whitelist 밖이며 runtime guard로 출제 후보에서 제외됨.
- 비활성화 dry-run만 완료했고 live DB는 변경하지 않음.
- 실제 비활성화는 optional hygiene 작업으로 별도 승인 필요.

## 7. 안전성 검토

지켜진 경계:

- `.env*`, API key, token, password, credential 원문을 읽거나 출력하지 않음.
- 운영 untracked `.env*`, backup, log, tmp 폴더 일괄 삭제 없음.
- `git clean -fd` 사용 없음.
- DB write는 사용자 승인된 RULE_A와 L3 option1 범위에서만 수행.
- 학생 공개/allowlist/feature flag 변경 없음.
- 보고서/산출물은 커밋 전 `git diff --cached --check`와 필요한 syntax/test 검증을 거침.

## 8. 기본 프로젝트 검토 요청용 체크리스트

기본 프로젝트/검토 담당 agent에게 아래 항목을 확인하도록 요청하면 된다.

검토 대상:

- repo: `C:/Users/aproa/aprolabs`
- branch: `main`
- HEAD: `b49b0ca`
- 핵심 보고서: `reports/aprolabs_completion_review_report_20261008.md`

검토 요청:

1. 최종 HEAD `b49b0ca` 기준으로 local/origin/server 동기화가 일관적인지 확인.
2. 어휘 DB 전환 결과가 보고서와 일치하는지 확인.
3. RULE_A 적용, RULE_B 보류, Belgium residue 보류 판단이 정책적으로 타당한지 확인.
4. schema/literacy 변경 `76e4769`의 `definitions[0]` fallback 방지 로직과 테스트가 충분한지 확인.
5. momo worksheet 변경 `8046e25`가 P1 범위에 맞고, Playwright blocker로 인해 남은 검증 제한이 명확히 기록됐는지 확인.
6. historical artifacts commit `b49b0ca`가 보존 가치가 있고 secret/credential 위험이 없는지 확인.
7. Git 밖 artifact manifest가 충분한 보존 정보를 담고 있는지 확인.
8. 추가로 진행해야 할 운영 작업이 있는지, 있다면 DB/배포/서비스 재시작 필요 여부를 별도 승인 대상으로 분리.

권장 판정 기준:

- 필수 완료 여부: pass/fail
- 운영 위험: low/medium/high
- 추가 승인 필요 항목: 별도 목록화
- blocker: Hermes/browser tooling처럼 프로젝트 성과와 분리 가능한지 명시

## 9. 최종 판정

aprolabs 작업 묶음은 성과 검토 요청 가능한 완료 상태다.

다음 실제 작업은 선택 사항이다.

- Hermes/browser tooling 복구
- RULE_B 별도 정책 검토
- Belgium residue live deactivation 여부 결정
- momo-marketing 또는 Hermes 멀티 에이전트 프로젝트 착수
