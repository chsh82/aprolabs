# 벨기에 residue item 비활성화 dry-run — 2026-10-07

## 범위

Known whitelist-excluded batch1 residue item 2건에 대해 DB hygiene 목적의 비활성화 dry-run만 수행했다.

실제 live DB 변경은 하지 않았다.

## 대상

- `MF_G5L3B1_C_G5-0fa0e0a975e55d66`
- `MF_G5L3B1_M_G5-0fa0e0a975e55d66`

두 item 모두 `벨기에` / `G5-0fa0e0a975e55d66` / `nikl_grade5_l3_batch1_v1`이며 현재 option1 whitelist에는 없다.

## dry-run 결과

- live DB integrity before: `ok`
- dry-run update rows: `2`
- dry-run integrity after: `ok`
- dry-run foreign key violations: `0`
- live DB readback integrity after dry-run: `ok`
- live DB mutated: `False`

## 판단

현재 runtime whitelist guard가 이미 두 item을 출제 후보에서 제외하므로 긴급 적용은 필요 없다.

다만 DB hygiene를 원하면 별도 승인 후 아래처럼 적용할 수 있다.

- `vocabulary_multiformat_items`의 위 2개 item만 `is_active=0` 처리
- `vocabulary_contents`의 content row는 삭제하지 않음
- public/student flag 변경 없음
- 전체 DB 백업 및 rollback manifest 필요

## 산출물

- status JSON: `/home/chsh82/aprolabs/reports/vocab_transition_belgium_residue_dryrun_status_20261007_20261008-000515.json`
- dry-run DB: `/home/chsh82/aprolabs/tmp/vocabulary_quiz_belgium_deactivate_dryrun_20261008-000515.db`
