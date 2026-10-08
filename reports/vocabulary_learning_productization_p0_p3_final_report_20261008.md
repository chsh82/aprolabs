# Vocabulary Learning Productization — P0~P3 Execution Report

작성 기준: 2026-10-08, Davinci profile, `C:/Users/aproa/aprolabs`

## Executive conclusion

현재 프로젝트는 `P0 Runtime 복구/연결`은 기존 `2.1.29` 로컬 runtime 기준으로 복구·검증 완료했다.
다만 `P1 v1.7.5 staging package 실제 적재`는 실제 package 파일 5개가 로컬에서 발견되지 않아 `BLOCKED_INPUT`이다.
따라서 `P2/P3`는 현재 DB에 이미 존재하는 L1~L3 runtime flow에 대해서만 TestClient E2E를 검증했고, v1.7.5 신규 staging content 기준 browser/E2E 검증은 완료라고 말할 수 없다.

## 기준점 확인

- Local repo: `C:/Users/aproa/aprolabs`
- Branch: `main`
- Local HEAD: `0ec7a28`
- Origin HEAD: `0ec7a28`
- Server HEAD: `0ec7a28`
- `aprolabs.service`: `active`
- Local working tree: code changes exist only for runtime compatibility patch plus new local reports/import tooling; no git commit/push performed.

## WHAT WAS ACTUALLY DONE

### 1. Runtime map / asset inventory

Confirmed runtime assets:

- FastAPI entrypoint: `app/main.py`
- Vocabulary runtime module: `app/vocabulary_quiz/`
- Main multiformat runtime/API: `app/vocabulary_quiz/routers/multiformat.py`
- Legacy quiz runtime: `app/vocabulary_quiz/routers/quiz.py`
- Templates: `app/templates/vocabulary_quiz/`
- Schema: `data/vocab/vocabulary_quiz_schema.sql`
- Runtime DB connection: `app/vocabulary_quiz/db.py`
- Local runtime DB: `data/vocab/vocabulary_quiz_rnd.db`
- Isolated v1.7.5 staging DB copy: `data/vocab/staging/vocabulary_quiz_v175_staging.db`

Created/updated local report artifacts:

- `reports/runtime_asset_inventory.md`
- `reports/runtime_availability.csv`
- `reports/runtime_integration_final_report.md`
- `reports/runtime_schema_mapping.csv`
- `reports/staging_backup_report.md`
- `reports/import_dry_run.csv`
- `reports/staging_import_result.csv`
- `reports/staging_import_errors.csv`
- `reports/browser_validation.md`
- `reports/e2e_results.csv`
- `reports/response_logging_validation.md`
- `reports/regression_report.md`
- `reports/v175_runtime_integration_probe/`
- `reports/vocabulary_learning_productization_p0_p3_final_report_20261008.md`

Created local staging import tooling:

- `scripts/vocab/import_v175_staging.py`

### 2. P0 runtime failure reproduced and fixed

Reproduced failing runtime path:

- Command: `C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_vocabulary_quiz_play.py`
- Original failure: `TypeError: cannot use 'tuple' as a dict key (unhashable type: 'dict')`
- Root cause: current Starlette `Jinja2Templates.TemplateResponse` signature requires `TemplateResponse(request, name, context, ...)`, but vocabulary runtime routers used the older `TemplateResponse(name, context, ...)` style.

Patched vocabulary routers only:

- `app/vocabulary_quiz/routers/quiz.py`
- `app/vocabulary_quiz/routers/multiformat.py`
- `app/vocabulary_quiz/routers/review.py`
- `app/vocabulary_quiz/routers/publish_review.py`
- `app/vocabulary_quiz/routers/official_grade_review.py`
- `app/vocabulary_quiz/routers/level_overview.py`
- `app/vocabulary_quiz/routers/grade5_l3_batch1_review.py`
- `app/vocabulary_quiz/routers/grade5_candidate_review.py`

Patch scope: changed only `TemplateResponse(...)` call shape for current Starlette compatibility. No content/DB/canonical changes.

### 3. P1 staging import preparation

Prepared but did not import:

- Created isolated staging DB copy: `data/vocab/staging/vocabulary_quiz_v175_staging.db`
- Created guarded importer: `scripts/vocab/import_v175_staging.py`
- Ran missing-package dry run probe:
  - Command: `C:/Users/aproa/aprolabs/venv/Scripts/python.exe scripts/vocab/import_v175_staging.py --package-dir C:/Users/aproa/aprolabs/data/import/v175_missing_package --database C:/Users/aproa/aprolabs/data/vocab/staging/vocabulary_quiz_v175_staging.db --output-dir C:/Users/aproa/aprolabs/reports/v175_runtime_integration_probe`
  - Result: `PACKAGE CHECK FAILED: 5 missing file(s)`

Missing package files:

- `staging_import_v1.7.5.csv`
- `staging_import_manifest_v1.7.5.json`
- `service_ready_recalculated_v1.7.5.csv`
- `type_a_patch_v1.7.5.csv`
- `type_b_patch_v1.7.5.csv`

### 4. P2/P3 local runtime validation against existing 2.1.29 runtime

Validated existing L1~L3 multiformat runtime via FastAPI TestClient:

- `/vocabulary-quiz/multiformat/play` renders: PASS
- L1 availability API returns candidates: PASS
- L1 session create / next / answer / result: PASS
- L2 availability API returns candidates: PASS
- L2 session create / next / answer / result: PASS
- L3 availability API returns candidates: PASS
- L3 session create / next / answer / result: PASS
- Total smoke checks: `16/16 PASS`

Observed current local multiformat runtime availability:

- L1 all_candidates: 316 items / 87 words
- L1 auto_only: 244 items / 67 words
- L2 all_candidates: 332 items / 88 words
- L2 auto_only: 4 items / 1 word
- L3 all_candidates: 338 items / 91 words
- L3 auto_only: 316 items / 85 words

## WHAT WAS NOT DONE

- No production DB import.
- No actual v1.7.5 staging import because package files are absent.
- No browser-authenticated manual/E2E session with real browser.
- No student cohort pilot started.
- No behavioral evidence generated.
- No PUBLIC_READY promotion.
- No canonical overwrite.
- No large-scale P4 content expansion.
- No git commit/push/deploy.

## WHAT WAS VERIFIED

### Code/syntax

Command:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile app/vocabulary_quiz/routers/quiz.py app/vocabulary_quiz/routers/multiformat.py app/vocabulary_quiz/routers/review.py app/vocabulary_quiz/routers/publish_review.py app/vocabulary_quiz/routers/official_grade_review.py app/vocabulary_quiz/routers/level_overview.py app/vocabulary_quiz/routers/grade5_l3_batch1_review.py app/vocabulary_quiz/routers/grade5_candidate_review.py
```

Result: PASS, no output/errors.

### Legacy quiz runtime regression

Command:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_vocabulary_quiz_play.py
```

Result:

- `21/21 PASS`
- Includes login guard, admin access, session creation, 20 questions, scoring, result, exposure flag non-change, QA hub rendering, `idiom.db` checksum unchanged.

### Import pipeline regression

Command:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_vocabulary_quiz_import.py
```

Result:

- `24/24 PASS`
- Includes package validation, hard validation guards, dry-run rollback, apply on TESTONLY DB, idempotency, exposure flag preservation, `idiom.db` unchanged.

### Level import pipeline regression

Command:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_vocabulary_level_import.py
```

Result:

- `26/26 PASS`
- Includes level validation guards, dry-run rollback, apply on TESTONLY DB, idempotency, existing table invariants, `idiom.db` unchanged.

### Multiformat L1~L3 E2E smoke

Command: one-off FastAPI TestClient script.

Result:

- `16/16 PASS`
- Sessions/responses created during smoke were cleaned up.
- Post-check confirmed:
  - `vocabulary_quiz_sessions`: 0
  - `vocabulary_quiz_attempts`: 0
  - `vocabulary_multiformat_sessions`: 0
  - `vocabulary_multiformat_responses`: 0

### DB integrity

Local runtime DB:

- `PRAGMA integrity_check`: `ok`
- `PRAGMA foreign_key_check`: `[]`
- orphan item view: 0
- content-without-item view: 0

Staging DB copy:

- Path: `data/vocab/staging/vocabulary_quiz_v175_staging.db`
- Size: `12099584`
- `PRAGMA integrity_check`: `ok`
- `PRAGMA foreign_key_check`: `[]`
- Counts:
  - `vocabulary_contents`: 5723
  - `vocabulary_items`: 5723
  - `vocabulary_multiformat_items`: 1289
  - `vocabulary_content_levels`: 5723
- Content source versions in staging DB copy:
  - `2.1.29`: 5723
  - `v1.7.5`: 0

## WHAT IS STILL ASSUMED

- The real v1.7.5 staging package may exist outside the searched local paths or under a different filename.
- The generated importer maps the expected package field names; this is prepared tooling, not proven against the real package.
- Current L1~L3 E2E PASS refers to existing `2.1.29` runtime data, not v1.7.5 imported content.
- Actual server `.env` DB path was not printed/read because `.env` may contain secrets.

## CURRENT BLOCKER

### Primary blocker: BLOCKED_INPUT — v1.7.5 package files not found

Actual staging import cannot proceed because the verified package files are unavailable:

- `staging_import_v1.7.5.csv`
- `staging_import_manifest_v1.7.5.json`
- `service_ready_recalculated_v1.7.5.csv`
- `type_a_patch_v1.7.5.csv`
- `type_b_patch_v1.7.5.csv`

This blocks:

- P1 actual v1.7.5 staging import
- P2 browser/E2E validation specifically against v1.7.5 content
- P3 targeted patch on v1.7.5 content
- P4 content expansion
- P5 closed pilot

### Secondary blocker: response logging schema gap for closed pilot

Current schema is enough for basic admin/internal answer logging, but does not yet satisfy the required pilot evidence contract exactly.

Missing or not snapshotted directly on response rows:

- `response_time_ms`
- `student_cohort_id`
- `sense_id`
- `service_level`
- `attempt_no` as an exposure attempt number distinct from cloze `attempt_count`
- `presented_at`
- `item_status_at_exposure`
- `content_release_version`
- `is_internal_tester`
- `is_verified_student`

## NEXT SINGLE PRIORITY

`Project 01A: recover or regenerate the verified v1.7.5 staging package files`.

Once package files are available:

1. Run dry-run against `data/vocab/staging/vocabulary_quiz_v175_staging.db`.
2. If dry-run PASS, run actual staging import with `--apply` on the isolated staging DB only.
3. Re-run DB integrity and L1/L2/L3 E2E against imported v1.7.5 rows.
4. Only after P1/P2 pass, patch response logging schema for closed pilot evidence fields.

## Continued execution after BLOCKED_INPUT

대표 지시에 따라 package 복구를 재시도했고, 추가로 Closed Pilot logging blocker를 해소했다.

### v1.7.5 package recovery retry

- Search root: `C:\Users\aproa`
- Indexed files: 28,219
- Archives scanned: 66
- Archive member hits: 0
- Result: required five package files still `NOT_FOUND`
- Report files:
  - `reports/v175_package_recovery_report.md`
  - `reports/v175_recovery_manifest.json`
  - `reports/v175_reconstruction_feasibility.md`

Decision preserved: package originals or deterministic/human-reviewed upstream proof 없이 재구성 import는 하지 않는다.

### Closed Pilot behavioral evidence logging patch

Implemented after P1 remained `BLOCKED_INPUT`, because this was the next runtime-side blocker for eventual closed pilot.

Changed files:

- `app/vocabulary_quiz/models.py`
- `app/vocabulary_quiz/routers/multiformat.py`
- `data/vocab/vocabulary_quiz_schema.sql`
- `scripts/vocab/migrate_vocabulary_pilot_evidence_logging.py`
- `tests/test_vocabulary_pilot_evidence_logging.py`

DB migrations applied locally:

- `data/vocab/vocabulary_quiz_rnd.db`
  - backup: `data/vocab/vocabulary_quiz_rnd.db.bak-pilot-evidence-20261008-121216`
- `data/vocab/staging/vocabulary_quiz_v175_staging.db`
  - backup: `data/vocab/staging/vocabulary_quiz_v175_staging.db.bak-pilot-evidence-20261008-121234`

Added session-level fields:

- `student_cohort_id`
- `is_internal_tester`
- `is_verified_student`

Added response/exposure-level fields:

- `sense_id`
- `service_level`
- `response_time_ms`
- `attempt_no`
- `presented_at`
- `item_status_at_exposure`
- `content_release_version`

Behavior:

- Default session traffic remains `is_internal_tester=1`, `is_verified_student=0` so internal tests do not become student evidence.
- `presented_at` is written when `/sessions/{session_id}/next` serves the item.
- `response_time_ms` and `attempt_no` are written on `/answer`.
- `sense_id`, `service_level`, `item_status_at_exposure`, and `content_release_version` are snapshotted on response row creation.

Additional verification after this patch:

- `tests/test_vocabulary_pilot_evidence_logging.py`: `16/16 PASS`
- L1/L2/L3 evidence E2E smoke: `15/15 PASS`
- `tests/test_multiformat_quiz_play.py`: `135/135 PASS`
- Existing regressions after patch:
  - `tests/test_vocabulary_quiz_play.py`: `21/21 PASS`
  - `tests/test_vocabulary_quiz_import.py`: `24/24 PASS`
  - `tests/test_vocabulary_level_import.py`: `26/26 PASS`
- DB cleanup/integrity after tests:
  - local runtime DB `PRAGMA integrity_check`: `ok`
  - local runtime DB `PRAGMA foreign_key_check`: `[]`
  - local runtime DB sessions/responses after cleanup: `0 / 0`
  - staging DB `PRAGMA integrity_check`: `ok`
  - staging DB `PRAGMA foreign_key_check`: `[]`

### Live-server validation and auth TemplateResponse patch

After the pilot logging patch, a local ASGI server was started for live HTTP validation:

```bash
VOCABULARY_QUIZ_DB_PATH='C:/Users/aproa/aprolabs/data/vocab/vocabulary_quiz_rnd.db' C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8017
```

A new runtime blocker was found on the unauthenticated redirect path:

- `/vocabulary-quiz/multiformat/play` redirected to `/login`
- `/login` failed with `TypeError: cannot use 'tuple' as a dict key (unhashable type: 'dict')`
- Root cause: `app/routers/auth.py` still used old Starlette `TemplateResponse("login.html", context)` style.

Patch applied:

- `app/routers/auth.py`
  - login GET and login error response now use `TemplateResponse(request, "login.html", context, ...)`.

Live-server verification after patch:

- `/login`: `200`
- unauthenticated `/vocabulary-quiz/multiformat/play`: redirect to login, then `200`
- authenticated live HTTP/API L1/L2/L3 smoke: `12/12 PASS`
- cleanup/integrity after live smoke:
  - `PRAGMA integrity_check`: `ok`
  - `PRAGMA foreign_key_check`: `[]`
  - sessions/responses: `0 / 0`

Browser automation attempt:

- Attempted via Hermes browser automation tool.
- Result: `BLOCKED_RUNTIME`
- Exact blocker: `agent-browser CLI not found`; reinstall failed with Windows `[WinError 5] 액세스가 거부되었습니다` while renaming Hermes tool directory.
- Updated report: `reports/browser_validation.md`

### Hermes browser automation repair diagnostics

Continued on the `agent-browser` blocker without deleting anything.

Diagnostics:

- `hermes pm doctor` fails with `PermissionError: [WinError 5] 액세스가 거부되었습니다` on `C:\Users\aproa\AppData\Local\hermes\tools\agent-browser-0.26.0-win32-x64`.
- Parent tools directory ACL grants `aproa` full control, but the child `agent-browser-0.26.0-win32-x64` directory itself cannot be listed or inspected.
- `icacls` and `ls` on that child directory both return access denied.

Classification: `BLOCKED_RUNTIME` caused by stale/corrupt/inaccessible Hermes tool package directory.

Repair note written:

- `reports/hermes_agent_browser_repair_20261008.md`

No deletion/takeown/reinstall was executed because fixing that directory requires elevated Windows permissions and is outside safe non-elevated runtime validation.

### Claude Code managed audit and F1 TemplateResponse cleanup

Representative delegated Claude Code interactive/PTA session:

- Started Claude Code in `C:/Users/aproa/aprolabs`.
- `tmux` was unavailable on this Windows Git Bash environment (`tmux: command not found`), so Claude Code was controlled through a background PTY session and then a bounded `claude -p` attempt.
- Claude independently audited current changes and reported:
  - F1: old `templates.TemplateResponse("...", context)` calls remain outside vocabulary routes and will fail under current Starlette.
  - F2: migration-before-code ordering is required for pilot evidence columns.
  - F3: evidence fields exist, but verified-student/internal flags are not yet server-side cohort enforced.
  - F5: v1.7.5 package recovery conclusion remains blocked; package files still not found.
  - F6: requirements vs installed FastAPI/Starlette version drift should be locked later.

Action taken after Claude audit:

- Applied the mechanical F1 TemplateResponse cleanup across remaining app routes and `zoom_reports/review_app.py`.
- Confirmed no remaining `templates.TemplateResponse("` pattern in repo.

Additional files modified by this cleanup:

- `app/isbn.py`
- `app/routers/answer_keys.py`
- `app/routers/crawl.py`
- `app/routers/dashboard.py`
- `app/routers/journal.py`
- `app/routers/literacy_admin.py`
- `app/routers/momo_bookshelf.py`
- `app/routers/momo_book_review.py`
- `app/routers/momo_book_worksheet.py`
- `app/routers/momo_worksheet_editor.py`
- `app/routers/momo_worksheet_page_editor.py`
- `app/routers/questions.py`
- `app/routers/reading_essay.py`
- `app/routers/suneung.py`
- `app/routers/upload.py`
- `app/routers/zoom_summaries.py`
- `zoom_reports/review_app.py`

Verification after F1 cleanup:

- Search for old pattern `templates.TemplateResponse("`: `0` matches.
- `py_compile` over all newly changed F1 files plus login/vocabulary route files: PASS.
- `tests/test_vocabulary_pilot_evidence_logging.py`: `16/16 PASS`
- `tests/test_vocabulary_quiz_play.py`: `21/21 PASS`
- TestClient smoke:
  - `GET /login`: PASS (`200`)
  - unauthenticated `GET /vocabulary-quiz/multiformat/play`: PASS (`302` to login)
- DB post-check:
  - `PRAGMA integrity_check`: `ok`
  - `PRAGMA foreign_key_check`: `[]`
  - multiformat sessions/responses: `0 / 0`

## Final status

- P0 Runtime recovery/connection: PASS for existing runtime after TemplateResponse patch.
- Pilot evidence logging contract: PASS locally and on isolated staging DB schema.
- P1 v1.7.5 staging import: BLOCKED_INPUT.
- P2 L1/L2/L3 E2E: PASS for existing runtime data; BLOCKED for v1.7.5 imported content.
- P3 targeted patch: PARTIALLY advanced for runtime evidence logging; content-specific targeted patch remains blocked by v1.7.5 package absence.
- P4+ content/pilot work: NOT STARTED per stop rule.
