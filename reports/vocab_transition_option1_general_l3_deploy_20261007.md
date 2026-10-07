# vocab_transition_option1_general_l3_deploy_20261007

- 배포 시각(KST): 2026-10-07 18:22~18:23
- 대상: aprolabs 연구 사이트 `/home/chsh82/aprolabs`, `aprolabs.service`
- 범위: L3 보강 옵션1 64콘텐츠·128문항을 일반 관리자 L3 `all_candidates` 후보에 포함
- 승인: 대표님이 Slack에서 `배포해`, `db write 도 하고`, `aprolabs 는 연구용 사이트라 어차피 고객들이 못봐요`라고 명시 승인

## 1. 배포 파일

서버에 직접 반영한 파일:

- `/home/chsh82/aprolabs/app/vocabulary_quiz/routers/multiformat.py`
- `/home/chsh82/aprolabs/data/vocab/l3_general_inclusion_option1_manifest_v1.json`
- `/home/chsh82/aprolabs/tests/test_l3_general_inclusion_option1.py`
- `/home/chsh82/aprolabs/reports/vocab_transition_option1_general_l3_code_rehearsal_20261007.md`
- `/home/chsh82/aprolabs/reports/vocab_transition_option1_general_l3_code_rehearsal_status.yaml`
- `/home/chsh82/aprolabs/tmp/apply_l3_option1_db.py`

서버 기존 파일 백업:

- `/home/chsh82/aprolabs/deploy_backups/l3_option1_20261007-182209/`

## 2. DB write / backup

대상 DB:

- `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db`

DB 백업:

- `/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak_l3_option1_20261007-092225`

DB 적용 스크립트 결과:

```json
{
  "backup_path": "/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db.bak_l3_option1_20261007-092225",
  "manifest_rows": 128,
  "manifest_contents": 64,
  "inserted_level_rows": 0,
  "exact_option1_level_rows": 64,
  "integrity_after": "ok",
  "foreign_key_violations": 0,
  "public_flags_nonzero_all_contents": 0
}
```

해석:

- DB write 스크립트는 `mode=rw`로 실행했고, 쓰기 전 백업을 만들었다.
- 옵션1 L3 level row 64건은 이미 적용되어 있어 추가 INSERT는 `0`건이었다.
- 적용 후 정확히 64 content가 `level_policy_v0.1 / L3 / REVIEW_BOUNDARY / active` 상태로 확인됐다.
- 공개 플래그는 여전히 전체 0건이다.

## 3. 서버 검증

### 코드/테스트

서버에서 실행:

```bash
/home/chsh82/aprolabs/venv/bin/python -m py_compile \
  app/vocabulary_quiz/routers/multiformat.py \
  tests/test_l3_general_inclusion_option1.py \
  tmp/apply_l3_option1_db.py
```

결과: 통과.

서버에서 테스트 함수 직접 실행 결과:

```text
PASS test_l3_all_candidates_includes_only_option1_whitelist_items
PASS test_l3_auto_only_does_not_include_review_boundary_option1_items
PASS test_non_l3_level_does_not_include_option1_whitelist_even_if_level_row_matches
PASS test_level_none_availability_remains_source_version_only
```

### Live DB + deployed code 직접 계산

서버에서 live DB 경로를 명시하고 `_level_availability()` 직접 호출:

```text
all_candidates 444 149 {'MEANING_CHOICE': 149, 'WORD_FROM_DEFINITION': 84, 'CONTEXT_MEANING': 149, 'CONTEXT_CLOZE': 61, 'MATCH_WORD_MEANING': 1}
auto_only 297 80 {'MEANING_CHOICE': 80, 'WORD_FROM_DEFINITION': 79, 'CONTEXT_MEANING': 80, 'CONTEXT_CLOZE': 58, 'MATCH_WORD_MEANING': 0}
```

의미:

- 일반 관리자 L3 `all_candidates`: 옵션1 포함 후 149어휘·444문항으로 증가
- `auto_only`: REVIEW_BOUNDARY 옵션1을 포함하지 않아 기존 80어휘·297문항 유지

### DB read-back

```text
integrity ok
option1_level_rows 64
public_flags_nonzero 0
```

### 서비스 재시작

```text
active
MainPID=2191821
ActiveEnterTimestamp=Wed 2026-10-07 09:22:25 UTC
```

Journal:

```text
Application startup complete.
Uvicorn running on http://127.0.0.1:8000
```

### HTTP smoke

로그인 필요 경로라 302가 정상 응답이다.

```text
http://127.0.0.1:8000/literacy/health 302
http://127.0.0.1:8000/api/vocabulary-quiz/availability?level=3 302
https://aprolabs.co.kr/literacy/health 302
https://aprolabs.co.kr/api/vocabulary-quiz/availability?level=3 302
```

## 4. 외부 영향

- 연구 DB write 스크립트 실행: 예, 백업 생성 후 실행
- 실제 INSERT: 0건, 이미 64건 적용되어 있었음
- DB 무결성: ok
- FK 위반: 0
- 학생 공개/allowlist/feature flag 변경: 없음
- 서비스 재시작: 완료
- git push/commit: 없음

## 5. 남은 확인

로그인 세션으로 `/vocabulary-quiz/multiformat/play`에서 L3 `all_candidates` 가용량 UI가 149어휘·444문항으로 보이는지 클릭스루 확인하면 된다. API/라우트는 인증 전 302로 정상 보호되고, 내부 함수 기준 결과는 확인됐다.
