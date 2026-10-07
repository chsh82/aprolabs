# schema/literacy historical artifacts cleanup plan — 2026-10-08

## 목적

어휘 DB 전환과 momo worksheet 정리 이후에도 남아 있던 과거 schema-reading / vocabulary-quiz 산출물을 분류했다. 이 파일들은 대부분 2026-09-23~2026-09-29의 읽기 전용 조사, dry-run, apply 결과, QA/파일럿 리허설 기록이다.

## 커밋 후보

### schema-reading / literacy DB integration artifacts

- `reports/schema_reading_phase1_baseline_20260923.md`
- `reports/schema_reading_phase2_readonly_audit_20260924.md`
- `reports/schema_reading_phase3_dryrun_20260924.md`
- `reports/schema_reading_phase4_literacy_link_apply_20260924.md`
- `reports/schema_reading_phase5_ai_level_audit_and_holds_20260924.md`
- `reports/schema_reading_phase6_ai_level_full_audit_20260924.md`
- `reports/schema_reading_phase7_literacy_repr_error_remediation_20260924.md`
- `reports/schema_reading_phase9_l4_l6_baseline_20260924.md`
- `reports/schema_reading_phase10_73drafts_crosscheck_20260924.md`
- `reports/schema_reading_phase11_l5l6_boundary_and_l4_batch1_20260924.md`
- `reports/schema_reading_phase12_l4_l5_spiral_review_20260924.md`
- `reports/schema_reading_phase13_l4_core50_apply_20260925.md`
- `reports/schema_reading_multiple_links_and_holds_dryrun_20260924.md`
- `reports/schema_reading_review_cards/*.md`

### data/import evidence

- `data/import/schema_reading_*20260924*.json/csv/jsonl/md`
- `data/import/schema_reading_phase21_*20260926*.json`
- `data/import/schema_reading_phase23_*20260926*.json`
- `data/import/schema_reading_phase24_*20260927*.json`
- `data/import/literacy_l4_batch1_50_dryrun_20260924.*`
- `data/import/sajaseongeo_datefix_10_apply_result_20260924.json`

### vocabulary quiz pilot reports

- `reports/vocab_quiz_*_20260928.md`
- `reports/vocab_quiz_*_20260929.md`

## 제외/보류

- `l2_check.json`: momo worksheet sample/debug JSON으로 별도 보존.
- `reports/vocab_transition_option1_general_l3_deploy_preservation_pack_20261007.zip`: 로컬 보존팩, Git 제외.
- `.env*`, 운영 백업, 로그/임시 서버 디렉터리: 접근/정리 대상 아님.

## 비밀값 점검

헤더/초기 구간 기준으로 `api_key`, `secret`, `token`, `password`, `authorization`, `bearer`, `sk-` 패턴을 스캔했다. `schema_reading_phase6_ai_level_full_audit_20260924.md`의 `_DATE_TOKEN`이 false-positive로 잡혔고, 실제 credential 패턴은 발견하지 못했다.

## 안전 경계

- DB write 없음.
- 서버 배포 없음.
- 서비스 재시작 없음.
- 학생 공개/allowlist/feature flag 변경 없음.
- `.env` 또는 credential 값 출력 없음.

## 판정

이 산출물들은 현재 repo의 historical audit trail로 보존 가치가 있다. 코드 변경과 분리해 별도 커밋으로 기록한다.
