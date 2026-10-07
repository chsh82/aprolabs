# vocab_transition_option1_l3_backfill_apply_20261007

- 작성 시각(KST): 2026-10-07 15:23:25 +0900
- task_id: vocab-transition-option1-l3-backfill-apply-20261007
- 승인 범위: 옵션 1 — 보수적 L3 보강 64 연결 우선
- 실제 DB: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- 적용 방식: 단일 트랜잭션으로 `vocabulary_content_levels`에 대상 64콘텐츠 L3 레벨행 INSERT
- level_status: `REVIEW_BOUNDARY`
- level_version: `level_policy_v0.1`
- RULE_A/B, 학생 공개, allowlist/feature flag, momolib, 배포: 미수행

## 1. 기준 문서·상태 재확인

검토 기준 문서:

1. `reports/vocab_transition_reconcile_001_report_20261007.md`
2. `reports/vocab_transition_reconcile_001_status.yaml`
3. `reports/vocab_transition_execution_readiness_20261007.md`
4. `reports/vocab_transition_execution_readiness_status.yaml`
5. `reports/vocab_transition_approval_options_20261007.md`
6. `C:/Users/aproa/AppData/Local/hermes/cache/documents/doc_370afed4b625_vocab_project_hermes_handoff_20261007.md`

현재 상태 확인:

- 로컬 repo HEAD: `6ac220e`
- 연구 서버 repo HEAD: `b8fbe7c`
- 연구 DB integrity_check: 적용 전/후 `ok`
- 적용 전 대상 64콘텐츠 레벨행: 0
- 적용 전 RULE_A/RULE_B: 1368 / 40
- 적용 전 전체 `vocabulary_content_levels`: 5950

주의: 연구 서버 git HEAD는 기준 문서의 `6ac220e`가 아니라 `b8fbe7c`였다. DB 스키마·대상 매니페스트·publish review 기준이 옵션 1 조건과 일치하여 DB 레벨행 적용은 진행했다. 배포나 코드 변경은 하지 않았다.

## 2. 백업

- 백업 경로: `/home/chsh82/aprolabs_data/vocabulary_quiz/backups/vocabulary_quiz_research.db.bak-option1-l3-backfill-pre-20261007-20261007-062230`
- 백업 크기: 17223680 bytes
- 백업 SHA-256: `e953ac5faa0458a9ba15d3db702fa1475aa6da560b22aedcefcf40f271db10e0`
- 백업 검증: row counts match, integrity_check ok, foreign_key_check 0건

## 3. 대상 manifest / dry-run 고정

- 로컬 manifest: `C:/Users/aproa/aprolabs/reports/vocab_transition_option1_l3_backfill_manifest_20261007.csv`
- manifest 행 수: 128 item rows
- manifest 고유 content_id: 64
- manifest SHA-256: `95ff2da82db1da7f278abc49fb58322b0f61ed067942d0caf499972da9021d77`
- 원격 dry-run manifest SHA-256: `95ff2da82db1da7f278abc49fb58322b0f61ed067942d0caf499972da9021d77`

대상 구성:

| 구분 | 콘텐츠 | 문항 | 기준 |
|---|---:|---:|---|
| batch1 | 29 | 58 | 개정 manifest 29콘텐츠·58문항 |
| batch2 승인분 | 35 | 70 | 최신 publish review `APPROVED_CANDIDATE` |
| 합계 | 64 | 128 | 옵션 1 대상 |

제외 유지:

- batch2 미개별승인 3콘텐츠: G5-0c775f5c27362d4a(None), G5-2fac71523c3c7e31(None), G5-3aedc662710832d4(None)
- 벨기에: manifest 대상에 없음, 포함하지 않음
- 비활성 v1 문항: 포함하지 않음

## 4. 실제 적용 결과

- INSERT rows: 64
- 적용 전 `vocabulary_content_levels`: 5950
- 적용 후 `vocabulary_content_levels`: 6014
- 적용 후 보강 source 레벨행: 64
- 적용 후 target 레벨행: 64
- DB 파일 SHA-256(after): `967154ed125ac12a94675541c97a8bcd9fa2c60454a6ada2f0ac2f9776b59d1a`
- DB 파일 크기(after): 17281024 bytes
- integrity_check(after): `ok`
- foreign_key_check 위반(after): 0건

트랜잭션·멱등성:

- 적용 전 대상 64콘텐츠에 동일 `level_version` 레벨행이 있으면 중단하도록 사전 검사했다.
- INSERT는 `NOT EXISTS(content_id, level_version)` 조건과 콘텐츠 활성/비공개 조건을 함께 사용했다.
- 첫 적용 시 64행이 아니면 rollback하도록 실행했다.

## 5. 검증 쿼리 결과

| 검증 항목 | 적용 전 | 적용 후 | 판정 |
|---|---:|---:|---|
| 대상 레벨행 | 0 | 64 | PASS |
| 전체 content_levels | 5950 | 6014 | PASS (+64) |
| 활성 대상 문항 | 128 | 128 | PASS |
| 대상 public_ready 비0 | 0 | 0 | PASS |
| 대상 student_exposure 비0 | 0 | 0 | PASS |
| RULE_A 후보 | 1368 | 1368 | PASS (미적용) |
| RULE_B 후보 | 40 | 40 | PASS (미적용) |
| contents/items/publish/judgments row count | 불변 | 불변 | PASS |

독립 재검증 출력:

```json
{
  "integrity": "ok",
  "fk_violations": 0,
  "total_levels": 6014,
  "option1_source_rows": 64,
  "option1_distinct_contents": 64,
  "rule_a": 1368,
  "rule_b": 40,
  "public_nonzero": 0
}
```

## 6. L3 공급량 재계산

현재 배포 코드의 일반 레벨모드는 아직 `source_version='2.1.29'`만 조회한다. 따라서 이번 DB 레벨행 INSERT만으로 현재 코드 경로의 `l3_supply_current_*` 값은 그대로다. 다만 옵션 1 대상 whitelist를 레벨 선택 풀에 포함하는 기준으로 재계산하면 아래와 같다.

| 조건 | all_candidates | auto_only | 비고 |
|---|---:|---:|---|
| 현재 코드 기준 L3 | 85 어휘 / 316 문항 | 80 어휘 / 297 문항 | source_version 2.1.29 only |
| 옵션1 whitelist 포함 L3 | 149 어휘 / 444 문항 | 80 어휘 / 297 문항 | REVIEW_BOUNDARY라 auto_only 증가는 없음 |

옵션1 whitelist 포함 유형별(all_candidates): `{'CONTEXT_CLOZE': 61, 'CONTEXT_MEANING': 149, 'MATCH_WORD_MEANING': 1, 'MEANING_CHOICE': 149, 'WORD_FROM_DEFINITION': 84}`

## 7. 변경 파일과 git 상태

생성/갱신한 로컬 산출물:

- `reports/vocab_transition_option1_l3_backfill_manifest_20261007.csv`
- `reports/vocab_transition_option1_l3_backfill_apply_20261007.md`
- `reports/vocab_transition_option1_l3_backfill_apply_status.yaml`

로컬 git status 요약:

```text
M app/routers/momo_worksheet_page_editor.py
 M app/templates/momo_worksheet_editor/pages.html
 M data/import/schema_reading_phase15_l4l5_audit_20260925.csv
 M data/import/schema_reading_phase15_l4l5_audit_20260925.jsonl
 M data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.csv
 M data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.jsonl
 M momo_book_db/PROGRESS.md
 M momo_book_db/worksheet/editor/page_store.py
 M momo_book_db/worksheet/scripts/blocks.js
 M momo_book_db/worksheet/scripts/paginate.js
 M reports/schema_reading_phase15_s_cards_20260925.md
 M scripts/literacy/import_textbook_vocab.py
?? data/import/krdict_fallback_821_full_audit_20260924.json
?? data/import/literacy_l4_batch1_50_dryrun_20260924.csv
?? data/import/literacy_l4_batch1_50_dryrun_20260924.jsonl
?? data/import/literacy_repr_errors_26_scan_20260924.json
?? data/import/sajaseongeo_datefix_10_apply_result_20260924.json
?? data/import/schema_reading_integration_revised_after_server_audit_v3.md
?? data/import/schema_reading_link_142_applied_snapshot_20260924.json
?? data/import/schema_reading_link_142_reverify_20260924.jsonl
?? data/import/schema_reading_link_142_targets_20260924.json
?? data/import/schema_reading_link_MULTIPLE_LINKS_schema_proof_20260924.json
?? data/import/schema_reading_link_apply_checksum_after_20260924.json
?? data/import/schema_reading_link_apply_checksum_before_20260924.json
?? data/import/schema_reading_link_dryrun_verdicts_20260924.csv
?? data/import/schema_reading_link_dryrun_verdicts_20260924.jsonl
?? data/import/schema_reading_phase10_73drafts_crosscheck_20260924.csv
?? data/import/schema_reading_phase10_73drafts_crosscheck_20260924.jsonl
?? data/import/schema_reading_phase12_l4_l5_spiral_review_20260924.csv
?? data/import/schema_reading_phase12_l4_l5_spiral_review_20260924.jsonl
?? data/import/schema_reading_phase21_jeonggi_student_definition_fix_dryrun_20260926.json
?? data/import/schema_reading_phase21_literacy10_server_apply_result_20260926.json
?? data/import/schema_reading_phase21_literacy10_terms_full_diff_pre_20260926.json
?? data/import/schema_reading_phase23_vq_contents_snapshot_20260926.json
?? data/import/schema_reading_phase24_homonym_scan_20260927.json
?? data/import/schema_reading_phase24_l6_core_inserted_20260927.json
?? data/import/schema_reading_phase24_l6_core_rows_20260927.json
?? data/import/schema_reading_phase24_reuse_scan_20260927.json
?? data/import/schema_reading_phase24_vq_contents_snapshot_20260927.json
?? data/import/schema_reading_phase3_dryrun_handoff_v4.md
?? data/import/schema_reading_phase4_research_link_decisions_v5.md
?? data/import/schema_reading_phase9_142links_l4l6_crosscheck_20260924.json
?? data/import/schema_reading_phase9_vq_level_availability_20260924.json
?? data/import/schema_reading_phase9_vs_summary_20260924.json
?? data/import/schema_reading_phase9_vs_terms_detail_20260924.csv
?? l2_check.json
?? lemma_list.txt
?? momo_book_db/worksheet/scripts/pdf_pixel_diff.py
?? reports/literacy_krdict_fallback_dryrun_6_20260924.csv
?? reports/literacy_krdict_fallback_dryrun_6_20260924.md
?? reports/literacy_repr_errors_26_verdict_20260924.csv
?? reports/literacy_repr_errors_26_verdict_20260924.jsonl
?? reports/schema_reading_multiple_links_and_holds_dryrun_20260924.md
?? reports/schema_reading_phase10_73drafts_crosscheck_20260924.md
?? reports/schema_reading_phase11_l5l6_boundary_and_l4_batch1_20260924.md
?? reports/schema_reading_phase12_l4_l5_spiral_review_20260924.md
?? reports/schema_reading_phase13_l4_core50_apply_20260925.md
?? reports/schema_reading_phase1_baseline_20260923.md
?? reports/schema_reading_phase2_readonly_audit_20260924.md
?? reports/schema_reading_phase3_dryrun_20260924.md
?? reports/schema_reading_phase4_literacy_link_apply_20260924.md
?? reports/schema_reading_phase5_ai_level_audit_and_holds_20260924.md
?? reports/schema_reading_phase6_ai_level_full_audit_20260924.md
?? reports/schema_reading_phase7_literacy_repr_error_remediation_20260924.md
?? reports/schema_reading_phase8_krdict_fallback_full_audit_20260924.md
?? reports/schema_reading_phase9_l4_l6_baseline_20260924.md
?? reports/schema_reading_review_cards/
?? reports/vocab_quiz_4812_particle_fix_20260929.md
?? reports/vocab_quiz_live_qa_e2e_20260929.md
?? reports/vocab_quiz_pilot_allowlist_20260929.md
?? reports/vocab_quiz_pilot_readiness_and_db_baseline_20260929.md
?? reports/vocab_quiz_student_pilot_blocker_resolution_20260929.md
?? reports/vocab_quiz_student_pilot_deploy_rehearsal_20260928.md
?? reports/vocab_transition_approval_options_20261007.md
?? reports/vocab_transition_execution_readiness_20261007.md
?? reports/vocab_transition_execution_readiness_status.yaml
?? reports/vocab_transition_option1_l3_backfill_apply_20261007.md
?? reports/vocab_transition_option1_l3_backfill_apply_status.yaml
?? reports/vocab_transition_option1_l3_backfill_manifest_20261007.csv
?? reports/vocab_transition_reconcile_001_report_20261007.md
?? reports/vocab_transition_reconcile_001_status.yaml
?? script_b64.txt
?? scripts/literacy/scan_momo_textbook_krdict_fallback_821_full.py
?? tests/test_krdict_fallback_hold_fix.py
HEAD=6ac220e
```

연구 서버 git status 요약:

```text
HEAD=b8fbe7c
?? .env.bak_20260822_024712
?? .env]
?? momo_b2b_tablet/assets/generated/
?? momo_b2b_tablet/backup.log
?? momo_b2b_tablet/server.log
?? momo_b2b_tablet_data/
?? momo_book_db/extracted_images.bak_20260828005728/
?? momo_book_db/extracted_images.bak_20260928063721/
?? nonsul_kb/extract_prompt.txt
?? nonsul_kb/run_extract_test.py
```

## 8. 범위 준수와 남은 사항

수행하지 않은 것:

- RULE_A 1,368건 UPDATE 없음
- RULE_B 40건 UPDATE 없음
- 학생 공개/public_ready/student_exposure 전환 없음
- allowlist/feature flag 변경 없음
- momolib 작업 없음
- 배포 없음
- 기존 문항 row/source_version 수정 없음

남은 사항:

- 실제 관리자 일반 레벨 출제 UI에 옵션1 whitelist를 포함하려면 코드 변경·배포 승인이 별도로 필요하다. 이번 작업은 DB 레벨행과 대상 manifest/검증까지 완료했다.
