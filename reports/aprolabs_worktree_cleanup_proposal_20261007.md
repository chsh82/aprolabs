# aprolabs worktree cleanup proposal — 2026-10-07

목적: 대표님 승인 없이 삭제/정리하지 않고, 현재 로컬·서버 working tree 잔여물을 분류해 다음 액션 후보만 제안한다.

## 현재 상태 요약

로컬 `C:/Users/aproa/aprolabs`:

- modified: 12개
- untracked: 77개
- 방금 완료한 L3 옵션1 일반 L3 연결 변경은 `c0b215d`로 커밋·푸시 완료되어 더 이상 unstaged 대상이 아니다.
- 남은 잔여물은 대부분 이전 어휘/문해력/워크시트 작업 산출물 또는 임시 파일이다.

서버 `/home/chsh82/aprolabs`:

- tracked modified: 0개
- `HEAD == origin/main == c0b215d`
- 남은 것은 운영/백업/로그 성격의 untracked 파일/폴더다.

## A. 이번 L3 옵션1 작업 관련 — 로컬 보존물

현재 로컬에만 남은 파일:

- `reports/vocab_transition_option1_general_l3_commit_message_20261007.txt`
- `reports/vocab_transition_option1_general_l3_deploy_preservation_pack_20261007.zip`

판단:

- 커밋 메시지 txt는 이미 커밋 완료 후 역할이 끝났다.
- zip은 scp 직접 배포분 보존용이다. 이미 동일 내용이 GitHub `c0b215d`와 보고서에 남았으므로 필수는 아니다.

제안:

1. 보수안: 둘 다 그대로 둔다.
2. 정리안: txt는 삭제, zip은 `reports/`가 아니라 로컬 archive 위치로 이동.
3. 강정리안: 둘 다 삭제. 단, zip 삭제 전 SHA256은 보고서에 이미 기록되어 있다.

권장: `txt 삭제 + zip은 당분간 유지`.

## B. 어휘 DB 작업 관련 — 이전 단계 보고/산출물

예시:

- `reports/vocab_transition_reconcile_001_report_20261007.md`
- `reports/vocab_transition_execution_readiness_20261007.md`
- `reports/vocab_transition_approval_options_20261007.md`
- `reports/vocab_transition_option1_l3_backfill_apply_20261007.md`
- `reports/vocab_transition_post_option1_followup_20261007.md`
- `reports/vocab_transition_selection_code_impact_20261007.md`
- `reports/vocab_quiz_*`
- `reports/literacy_krdict_*`
- `scripts/literacy/scan_momo_textbook_krdict_fallback_821_full.py`
- `tests/test_krdict_fallback_hold_fix.py`

판단:

- 이번 L3 옵션1 작업의 의사결정 근거가 된 보고서들이 포함되어 있다.
- 일부는 GitHub에 커밋할 가치가 있을 수 있지만, 산출물이 많아 한꺼번에 커밋하면 기록이 지저분해질 수 있다.

제안:

1. `vocab_transition_*_20261007` 계열만 별도 문서 커밋 후보로 검토.
2. `vocab_quiz_*`, `krdict_*`는 별도 트랙으로 분리해 나중에 검토.
3. 지금 삭제 금지.

권장: *삭제하지 말고*, 다음 단계에서 `vocab_transition_*` 보고서만 먼저 커밋 후보/보관 후보로 나눈다.

## C. 문해력/schema_reading 이전 산출물

예시:

- `data/import/schema_reading_*`
- `reports/schema_reading_*`
- `data/import/literacy_*`
- `reports/literacy_repr_*`
- `scripts/literacy/import_textbook_vocab.py` modified

판단:

- 현재 L3 옵션1 배포와 직접 관련 없음.
- 다만 과거 문해력/교재어휘 작업 증빙일 가능성이 높아 삭제 위험이 있다.

제안:

- 이번 턴에서는 건드리지 않는다.
- 별도 “schema_reading/literacy 산출물 정리” 단계에서만 다룬다.

권장: 보류.

## D. momo_book_db / worksheet / page editor 작업

modified:

- `app/routers/momo_worksheet_page_editor.py`
- `app/templates/momo_worksheet_editor/pages.html`
- `momo_book_db/PROGRESS.md`
- `momo_book_db/worksheet/editor/page_store.py`
- `momo_book_db/worksheet/scripts/blocks.js`
- `momo_book_db/worksheet/scripts/paginate.js`

untracked:

- `momo_book_db/worksheet/scripts/pdf_pixel_diff.py`

판단:

- 별도 워크시트/페이지 에디터 작업 트랙으로 보인다.
- 현재 어휘 DB 작업과 섞으면 안 된다.

제안:

- 이번 턴에서는 건드리지 않는다.
- 다음 큰 단계가 워크시트라면 이 묶음을 별도 테스트/커밋 대상으로 전환.

권장: 보류.

## E. 루트 임시/진단 파일

- `l2_check.json`
- `lemma_list.txt`
- `script_b64.txt`

판단:

- 이름상 임시 진단 파일 가능성이 높다.
- 하지만 내용 확인 없이 삭제하지 않는다.

제안:

- 다음 단계에서 내용/크기/생성 의도 확인 후 삭제 후보로 올린다.

권장: 확인 후 삭제 후보.

## F. 서버 untracked 운영 파일

서버 `/home/chsh82/aprolabs` untracked:

- `.env.bak_20260822_024712`
- `.env]`
- `deploy_backups/`
- `tmp/`
- `momo_b2b_tablet/assets/generated/`
- `momo_b2b_tablet/backup.log`
- `momo_b2b_tablet/server.log`
- `momo_b2b_tablet_data/`
- `momo_book_db/extracted_images.bak_*`
- `nonsul_kb/extract_prompt.txt`
- `nonsul_kb/run_extract_test.py`

판단:

- 서버 운영/백업/로그/데이터 성격이 강하다.
- `.env*`는 민감 가능성이 있어 읽거나 출력하지 않는다.
- `deploy_backups/`에는 방금 배포 백업이 포함되어 있어 당장 삭제하면 안 된다.

제안:

- 서버 untracked는 당장 정리하지 않는다.
- `.env]` 같은 이상 파일명도 내용 확인 없이 건드리지 않는다.
- 필요 시 별도 승인으로 `du -sh` 수준의 크기 점검만 먼저 한다.

권장: 보류.

## 권장 다음 단계

대표님 확인을 받아 아래 순서로 진행 권장:

1. 로컬 루트 임시 파일 3개(`l2_check.json`, `lemma_list.txt`, `script_b64.txt`) 내용/크기만 확인하고 삭제 후보 여부 보고.
2. 이번 L3 작업 보존물 중 `commit_message txt` 삭제 여부 확인.
3. `vocab_transition_*` 이전 보고서를 “커밋할 것 / 로컬 보관만 할 것”으로 추가 분류.
4. 나머지 schema_reading/worksheet/server 운영 파일은 지금은 건드리지 않음.

## 절대 금지 제안

- `git clean -fd` 금지.
- 서버 untracked 일괄 삭제 금지.
- `.env*` 내용 읽기/출력 금지.
- momo_book_db/schema_reading 산출물 무검토 삭제 금지.
