# Runtime Integration Final Report — Vocabulary Master v1.7.5

## 1. 실제 Runtime 발견 경로

- Runtime repo: `C:/Users/aproa/aprolabs`
- FastAPI entrypoint: `app/main.py`
- Vocabulary runtime module: `app/vocabulary_quiz/`
- Main multiformat runtime/API: `app/vocabulary_quiz/routers/multiformat.py`
- Legacy quiz runtime: `app/vocabulary_quiz/routers/quiz.py`
- Templates: `app/templates/vocabulary_quiz/`
- Schema: `data/vocab/vocabulary_quiz_schema.sql`
- Runtime DB connection: `app/vocabulary_quiz/db.py`

## 2. Staging DB 상태

`STAGING_DB_AVAILABLE`: PASS for isolated local staging DB.

- Staging DB created/copied from local runtime DB:
  - `data/vocab/staging/vocabulary_quiz_v175_staging.db`
- Production DB was not used.
- Integrity: `ok`
- FK violations: `0`

## 3. Migration 상태

`MIGRATION_ACCESS`: PARTIAL.

- Existing targeted migration scripts found under `scripts/vocab/`.
- No formal Alembic migration stack found.
- No v1.7.5-specific migration was run because the package files are absent and current schema can hold the mapped staging fields.

## 4. Import Script 상태

`IMPORT_SCRIPT_READY`: PARTIAL/PREPARED.

Created:

- `scripts/vocab/import_v175_staging.py`

Capabilities implemented:

- dry-run default with rollback
- `--apply` explicit commit mode
- package file existence checks
- canonical `atomic_sense_id` and `item_id` preservation
- duplicate item detection
- FK validation
- level/status/option/answer/version validation
- version-aware conflict detection
- staging exposure forced off (`student_exposure=0`, `public_ready=0`)
- pre-commit backup when `--apply` is used
- CSV outputs:
  - `import_dry_run.csv`
  - `staging_import_result.csv`
  - `staging_import_errors.csv`

## 5. Dry Run 결과

`DRY_RUN_PASS`: FAIL/BLOCKED.

Executed command shape:

```bash
python scripts/vocab/import_v175_staging.py --package-dir C:/Users/aproa/aprolabs/data/import/v175_missing_package --database C:/Users/aproa/aprolabs/data/vocab/staging/vocabulary_quiz_v175_staging.db --output-dir C:/Users/aproa/aprolabs/reports/v175_runtime_integration_probe
```

Result:

- Package check failed.
- Missing required files: 5.
- No DB rows imported.
- Report: `reports/import_dry_run.csv`
- Errors: `reports/staging_import_errors.csv`

## 6. 실제 Import 결과

`ACTUAL_IMPORT_PASS`: NOT RUN.

Reason: dry-run cannot pass without the real v1.7.5 package files.

Required files not found after broad local search including zip contents:

- `staging_import_v1.7.5.csv`
- `staging_import_manifest_v1.7.5.json`
- `service_ready_recalculated_v1.7.5.csv`
- `type_a_patch_v1.7.5.csv`
- `type_b_patch_v1.7.5.csv`

## 7. L1 Browser 결과

`L1_BROWSER_PASS`: BLOCKED.

Reason: no v1.7.5 content imported into staging, so L1 browser content cannot be truthfully validated.

## 8. L2 Browser 결과

`L2_BROWSER_PASS`: BLOCKED.

Same blocker.

## 9. L3 Browser 결과

`L3_BROWSER_PASS`: BLOCKED.

Same blocker.

## 10. E2E 결과

`L1_E2E_PASS`: BLOCKED
`L2_E2E_PASS`: BLOCKED
`L3_E2E_PASS`: BLOCKED

Report: `reports/e2e_results.csv`

## 11. Response Logging 결과

`RESPONSE_LOG_PASS`: FAIL/PARTIAL.

Observed schema supports:

- user/session: partial (`user_id`, `session_id`)
- item_id: yes
- correct: yes (`is_correct`)
- selected answer: yes via `submitted_payload_json` or legacy `selected_option`
- attempt: partial (`attempt_count` in multiformat)
- content release: partial (`source_version` on session)

Schema gaps for exact Project 01 requirements:

- response time field not found
- response row does not snapshot `sense_id`
- response row does not snapshot `level`
- response row does not snapshot content release directly

Report: `reports/response_logging_validation.md`

## 12. Regression 결과

`BASELINE_REGRESSION_PASS`: BLOCKED_BY_IMPORT_NOT_RUN / pre-import baseline readable.

- Staging DB integrity ok.
- Existing local baseline copy remained readable.
- No public/student exposure flags changed by this run.
- Full regression after v1.7.5 import cannot be asserted because import did not run.

Report: `reports/regression_report.md`

## 13. 실제 남은 blocker

Primary blocker:

- `BLOCKED_PACKAGE_FILES_NOT_FOUND`

Secondary blockers after package is supplied:

- response logging schema gap for exact fields, especially response time
- browser/E2E must be rerun after actual staging import
- Hermes browser automation may still be blocked by previously documented Windows tool permission issue, unless browser validation is performed by another route

## 14. 최종 판정

`BLOCKED_RUNTIME`

Runtime itself exists and staging DB/import tooling has been recovered locally, but the actual verified v1.7.5 package is unavailable. Therefore the requested end-to-end runtime integration cannot reach GO.

## 15. 다음 단 하나의 프로젝트

`Project 01A: Locate or regenerate the verified v1.7.5 staging package files`

Scope:

- recover/provide the five required v1.7.5 files
- rerun `import_v175_staging.py` dry-run against `data/vocab/staging/vocabulary_quiz_v175_staging.db`
- proceed to actual staging import only after dry-run PASS

Project 02 must not start yet.
