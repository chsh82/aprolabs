# Regression Report — baseline v2.1 preservation

- checked_at: 2026-10-08T10:43:46.351487
- database: `C:\Users\aproa\aprolabs\data\vocab\staging\vocabulary_quiz_v175_staging.db`
- import state: no v1.7.5 rows imported because package files are unavailable

## Baseline counters after staging setup

```json
{
  "contents_2_1_29": 5723,
  "items_2_1_29": 5723,
  "public_ready_contents": 0,
  "public_ready_items": 0,
  "student_exposure_contents": 0,
  "student_exposure_items": 0
}
```

## Assessment

- Existing baseline DB copy remains readable and passes integrity checks.
- No PUBLIC/PILOT/student exposure flags were changed by this run.
- Full baseline v2.1 299-item regression cannot be asserted from v1.7.5 import impact because actual import did not run.

Result: `BASELINE_REGRESSION_BLOCKED_BY_IMPORT_NOT_RUN`.
