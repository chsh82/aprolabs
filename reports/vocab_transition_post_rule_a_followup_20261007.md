# post-RULE_A follow-up smoke and whitelist investigation — 2026-10-07

## Scope

Read-only follow-up after RULE_A live DB apply.

Performed:

- Investigated the 2 whitelist-excluded batch1 item ids seen in admin UI smoke logs.
- Rechecked L2/L3 candidate selection counts with the app selection function.
- Rechecked L2/L3 availability API through authenticated TestClient.

Not performed:

- DB write.
- Deployment.
- Service restart.
- Student/public flag change.

## DB integrity

- `PRAGMA integrity_check`: `ok`

## batch1 whitelist-excluded item investigation

Suspect item ids:

- `MF_G5L3B1_C_G5-0fa0e0a975e55d66`
- `MF_G5L3B1_M_G5-0fa0e0a975e55d66`

Findings:

| item_id | item_type | content_id | lemma | source_version | item active | content active | level rows | whitelist member |
|---|---|---|---|---|---:|---:|---:|---:|
| `MF_G5L3B1_C_G5-0fa0e0a975e55d66` | CONTEXT_MEANING | `G5-0fa0e0a975e55d66` | 벨기에 | `nikl_grade5_l3_batch1_v1` | 1 | 1 | 0 | false |
| `MF_G5L3B1_M_G5-0fa0e0a975e55d66` | MEANING_CHOICE | `G5-0fa0e0a975e55d66` | 벨기에 | `nikl_grade5_l3_batch1_v1` | 1 | 1 | 0 | false |

Interpretation:

- The 2 items are the known excluded `벨기에` content.
- They remain active in `vocabulary_multiformat_items` and `vocabulary_contents`.
- They have no `vocabulary_content_levels` row.
- They are not in the current option1 whitelist manifest.
- Existing whitelist guard correctly filters them out.

Recommendation:

- No urgent write needed because runtime filtering is already working.
- If DB hygiene is desired later, prepare a separate dry-run to either deactivate the 2 item rows or mark/hold the content. Do not delete rows directly without a rollback manifest.

## batch1 source_version item distribution

`source_version='nikl_grade5_l3_batch1_v1'` currently has:

| item_type | is_active | count |
|---|---:|---:|
| CONTEXT_MEANING | 0 | 29 |
| CONTEXT_MEANING | 1 | 30 |
| MEANING_CHOICE | 0 | 29 |
| MEANING_CHOICE | 1 | 30 |

This matches the historical pattern: 30 active contents in DB source_version, but current revised/approved manifest excludes 1 content (`벨기에`), leaving 29 approved batch1 contents for option1.

## app selection-function smoke

Direct `_select_level_candidates()` counts:

| level | mode | candidate item count | sample |
|---:|---|---:|---|
| 2 | all_candidates | 625 | `MF_A_SC_V19100_B100_005`, `MF_B_SC_V19100_B100_005`, `MF_C_SC_V19100_B100_005`, `MF_A_SC_V19100_B100_029`, `MF_B_SC_V19100_B100_029` |
| 2 | auto_only | 289 | `MF_A_SC_V19100_B100_029`, `MF_B_SC_V19100_B100_029`, `MF_C_SC_V19100_B100_029`, `MF_D_SC_V19100_B100_029`, `MF_A_SC_V19100_B100_039` |
| 3 | all_candidates | 170 | `MF_D_SC_V19101_B101_001`, `MF_D_SC_V19111_B111_027`, `MF_D_SC_V19135_B135_028`, `MF_D_SC_V1922_B022_013`, `MF_D_SC_V1924_B024_023` |
| 3 | auto_only | 31 | `MF_A_SC_V19111_B111_027`, `MF_B_SC_V19111_B111_027`, `MF_C_SC_V19111_B111_027`, `MF_D_SC_V19111_B111_027`, `MF_A_SC_V19135_B135_028` |

## authenticated API smoke

Authenticated TestClient returned HTTP `200 application/json` for all checked endpoints.

| endpoint | result |
|---|---:|
| `/api/vocabulary-quiz/availability?level=2&confidence_mode=all_candidates` | 166 words / 625 items |
| `/api/vocabulary-quiz/availability?level=2&confidence_mode=auto_only` | 78 words / 289 items |
| `/api/vocabulary-quiz/availability?level=3&confidence_mode=all_candidates` | 75 words / 170 items |
| `/api/vocabulary-quiz/availability?level=3&confidence_mode=auto_only` | 8 words / 31 items |

## L3 auto_only decision

Current L3 auto_only is small: `8 words / 31 items`.

Reason:

- RULE_A moved most original L3 `PROVISIONAL_AUTO` rows to L2.
- Option1 L3 supplement is `REVIEW_BOUNDARY`, so it supports all_candidates but not auto_only.

Recommendation:

- Keep current state for now.
- Do not mass-promote option1 to `PROVISIONAL_AUTO` merely to increase auto_only supply.
- If L3 auto_only is operationally required, run a separate sample-based review to decide which option1 rows are safe to promote.

## Conclusion

The post-RULE_A state is technically consistent:

- L2 supply improved.
- L3 all_candidates remains usable due to option1 supplement.
- L3 auto_only is intentionally small.
- The 2 batch1 excluded `벨기에` items are known DB residue and are safely blocked by whitelist filtering.
