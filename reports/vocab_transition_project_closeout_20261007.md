# 어휘 DB 전환 프로젝트 최종 종료 보고서 — 2026-10-07/08

## 결론

어휘 DB 전환 프로젝트의 핵심 범위는 완료됐다.

완료된 핵심 성과:

1. L3 옵션1 64콘텐츠·128문항을 일반 관리자 L3 `all_candidates` 후보에 whitelist 기반으로 연결했다.
2. RULE_A 1,368건을 연구 DB에서 `L3 -> L2`로 적용했다.
3. RULE_B 40건은 정책상 보류로 정리했다.
4. 관리자 API smoke 및 앱 선택 함수 smoke를 완료했다.
5. 벨기에 residue 2개 item의 원인과 안전 상태를 확인했고, 별도 비활성화 dry-run까지 완료했다.
6. 보고서와 감사 추적 산출물을 GitHub와 서버에 동기화했다.

## 현재 live 상태

- 서버 repo: `/home/chsh82/aprolabs`
- live DB: `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`
- service: `aprolabs.service active`
- RULE_A live remaining: `0`
- RULE_A moved pending/reference rows: `1,368`
- RULE_B remaining: `40`
- DB integrity: `ok`

현재 availability:

| level/mode | words | items |
|---|---:|---:|
| L2 all_candidates | 166 | 625 |
| L2 auto_only | 78 | 289 |
| L3 all_candidates | 75 | 170 |
| L3 auto_only | 8 | 31 |

## 주요 커밋

- `c0b215d` — L3 보강 옵션1 일반 L3 후보 연결 및 연구사이트 배포 기록
- `6b33147` — L3 option1 transition audit trail
- `b3d8763` — RULE_A dry-run approval artifacts
- `e627860` — RULE_A live DB apply 기록
- `647f49c` — RULE_B post RULE_A review
- `d1c5fde` — worktree cleanup status
- `f65aa94` — post RULE_A vocabulary follow-up

## 벨기에 residue 처리 상태

대상:

- `MF_G5L3B1_C_G5-0fa0e0a975e55d66`
- `MF_G5L3B1_M_G5-0fa0e0a975e55d66`

확인:

- 표제어: 벨기에
- source_version: `nikl_grade5_l3_batch1_v1`
- item/content active: 1
- level rows: 0
- option1 whitelist member: false
- runtime guard: whitelist 밖이라 출제 후보에서 제외됨

추가 dry-run:

- dry-run update rows: 2
- live DB mutated: false
- live item rows remained active after dry-run: true

결론:

- 긴급 위험 없음.
- 실제 비활성화는 별도 명시 승인 후 진행 가능.

## 로컬-only 잔여 파일

안전상 삭제하지 않고 남긴 파일:

- `l2_check.json`
  - momo worksheet/샘플 가능성이 있어 이번 어휘 DB 종료 작업에서 삭제하지 않음.
- `reports/vocab_transition_option1_general_l3_deploy_preservation_pack_20261007.zip`
  - L3 옵션1 배포 보존팩. Git에는 넣지 않는 것이 맞지만 로컬 백업으로 보존.

정리 완료:

- `lemma_list.txt` 삭제
- `script_b64.txt` 삭제
- `reports/script_b64_decoded_preview_20261007.py` 삭제
- 임시 commit message 파일 삭제

## 남은 blocker

### Hermes browser automation

상태: 미해결, 프로젝트 핵심 완료에는 영향 없음.

원인:

- Windows tool directory permission denied.
- blocked paths:
  - `C:\Users\aproa\AppData\Local\hermes\tools\agent-browser-0.26.0-win32-x64`
  - `C:\Users\aproa\AppData\Local\hermes\tools\chromium-1208`

시도:

- `hermes pm doctor`
- `hermes pm install agent-browser`
- `icacls`
- `takeown`
- browser health check

결론:

- 현재 비승격 세션에서는 복구 불가.
- 관리자 권한 Windows 세션에서 권한 복구 또는 stale tool directory 삭제 후 재설치 필요.

## 최종 판단

어휘 DB 전환 프로젝트는 운영상 완료 상태다.

완료 기준:

- DB 전환 완료.
- 서비스 active.
- API smoke pass.
- GitHub/server 동기화 완료.
- 감사 보고서 생성 완료.
- 위험 잔여 항목은 runtime guard 또는 보류 정책으로 안전하게 관리됨.

추가 선택 작업:

1. 벨기에 2개 item 실제 비활성화 적용 — optional, 별도 승인 필요.
2. L3 auto_only 보강 — optional, 별도 교육정책/검수 필요.
3. Hermes browser automation 권한 복구 — optional tooling fix.
4. schema/literacy 및 momo worksheet 잔여 변경은 별도 프로젝트 트랙.
