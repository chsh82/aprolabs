# RULE_A live DB apply — 2026-10-07

## Summary

- Applied RULE_A only: 1,368 rows `L3 -> L2` in live research DB.
- RULE_B remained pending: 40 rows.
- Student/public exposure flags unchanged: target nonzero count 0.
- No deployment or service restart was performed.

## Paths

- DB: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- Backup: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-rule-a-pre-20261007-231842`
- Manifest: `/home/chsh82/aprolabs/reports/vocab_transition_rule_a_dryrun_manifest_20261007_20261007-224108.csv`
- Status JSON: `/home/chsh82/aprolabs/reports/vocab_transition_rule_a_apply_status_20261007_20261007-231842.json`

## Verification

- before integrity: `ok`
- after integrity: `ok`
- foreign_key_check violations: `0`
- update rows: `1368`
- RULE_A remaining: `0`
- RULE_B remaining: `40`
- target rows now L2: `1368`

## Availability before -> after

- L2 all_candidates: `92/346` -> `166/625`
- L2 auto_only: `6/23` -> `78/289`
- L3 all_candidates: `149/444` -> `75/170`
- L3 auto_only: `80/297` -> `8/31`

## API smoke after apply

Authenticated TestClient smoke against the live research DB returned HTTP 200 for all checked availability endpoints.

- L2 all_candidates: `166/625`
- L2 auto_only: `78/289`
- L3 all_candidates: `75/170`
- L3 auto_only: `8/31`

## Notes

After applying RULE_A, `review_status='RULE_PROPOSED_PENDING_APPROVAL'` rows for the moved RULE_A set still exist in the official-grade reference table, but they no longer match the active RULE_A condition because their live `vocab_level` is now L2. RULE_B remains `40`.

## External effects

- Live DB write: yes, RULE_A 1,368 row update.
- Deploy: none.
- Service restart: none.
- Student/public flags: unchanged.
- RULE_B: unchanged.
