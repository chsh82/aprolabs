# aprolabs worktree cleanup final status — 2026-10-07

## Scope

Final cleanup pass after L3 option1, RULE_A dry-run, RULE_A live apply, and RULE_B post-review.

## Completed cleanup during this run

Removed root temporary files after inspection:

- `lemma_list.txt`
- `script_b64.txt`
- `reports/vocab_transition_option1_general_l3_commit_message_20261007.txt`

Preserved inspected evidence:

- `reports/aprolabs_root_temp_files_review_20261007.md`
- `reports/script_b64_decoded_preview_20261007.py`

Committed/pushed vocabulary transition audit trail through:

- `647f49c` — `docs: record RULE_B post RULE_A review`

## Current git synchronization

Local `main` and `origin/main` are synchronized at `647f49c` after the RULE_B post-review commit.

Server `/home/chsh82/aprolabs` was also fast-forwarded to `647f49c`.

## Remaining local worktree categories

These are intentionally not bulk-deleted.

### 1. Current task local backup artifact

- `reports/vocab_transition_option1_general_l3_deploy_preservation_pack_20261007.zip`

Recommendation: keep local-only. Do not commit binary zip because constituent text artifacts are already tracked.

### 2. Current cleanup audit artifacts

- `reports/aprolabs_worktree_cleanup_proposal_20261007.md`
- `reports/aprolabs_root_temp_files_review_20261007.md`
- `reports/script_b64_decoded_preview_20261007.py`
- `reports/aprolabs_worktree_cleanup_final_status_20261007.md`

Recommendation: commit the markdown cleanup records. Keep or delete the decoded preview later depending on whether the nonsul extraction test needs preservation.

### 3. Existing vocab quiz pilot reports

- `reports/vocab_quiz_*.md`

Recommendation: separate review. These are older pilot readiness/QA artifacts and should not be deleted as cleanup noise.

### 4. schema/literacy data and reports

- `data/import/schema_reading_*`
- `reports/schema_reading_*`
- `scripts/literacy/*`
- `tests/test_krdict_fallback_hold_fix.py`

Recommendation: separate schema/literacy cleanup track. Do not mix with vocabulary transition commits.

### 5. momo worksheet/page editor changes

- `app/routers/momo_worksheet_page_editor.py`
- `app/templates/momo_worksheet_editor/pages.html`
- `momo_book_db/worksheet/*`

Recommendation: separate momo worksheet track. Do not revert or commit under the vocabulary DB task.

### 6. Server operational untracked files

Server still has operational/untracked items such as `.env*`, `deploy_backups/`, logs, generated assets, and `tmp/`.

Recommendation: do not bulk delete. `.env*` files were not read or printed. `deploy_backups/` and DB backups must remain for rollback/audit.

## Safety conclusion

No broad `git clean -fd` was run. No server operational cleanup was performed. Remaining changes are now separated into safe tracks rather than mixed with the completed vocabulary DB transition work.
