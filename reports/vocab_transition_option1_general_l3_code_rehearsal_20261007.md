# vocab_transition_option1_general_l3_code_rehearsal_20261007

- 작성 시각(KST): 2026-10-07 17:57:08 +0900
- 범위: L3 보강 옵션1 64콘텐츠·128문항을 일반 관리자 L3 `all_candidates` 후보에 포함하는 로컬 코드 리허설
- 원칙: 실제 DB write 없음, 배포 없음, 학생 공개/allowlist/feature flag 변경 없음

## 결론

로컬 코드 리허설을 완료했다. 일반 관리자 레벨 모드에서 `selected_vocab_level=3` 및 `confidence_mode=all_candidates`일 때만, 옵션1 manifest에 고정된 item_id 128건을 기존 `SOURCE_VERSION="2.1.29"` 후보에 추가하도록 구현했다.

안전 경계는 다음과 같이 유지했다.

- `source_version IN (...)`처럼 배치 전체를 넓히지 않음
- `data/vocab/l3_general_inclusion_option1_manifest_v1.json`의 item_id whitelist만 허용
- `level=None` 전체/혼합 일반 모드는 기존 `SOURCE_VERSION="2.1.29"`만 사용
- `confidence_mode=auto_only`는 옵션1 `REVIEW_BOUNDARY` 보강분을 포함하지 않음
- 비 whitelist batch item은 제외
- 학생 공개 플래그, allowlist, feature flag, 배포는 변경하지 않음

## 변경 파일

- `app/vocabulary_quiz/routers/multiformat.py`
  - 옵션1 일반 L3 포함 manifest 경로/검증 로더 추가
  - `_select_level_candidates()`에서 L3 `all_candidates`일 때만 whitelist item_id를 추가 조회
  - source_version 전체 확장이 아니라 item_id whitelist 기반으로 제한
- `data/vocab/l3_general_inclusion_option1_manifest_v1.json`
  - `reports/vocab_transition_option1_l3_backfill_manifest_20261007.csv` 기준 128 item / 64 content를 data/vocab에 고정
- `tests/test_l3_general_inclusion_option1.py`
  - in-memory SQLite 기반 회귀 테스트 4건 추가

## manifest 검증

명령:

```bash
venv/Scripts/python.exe - <<'PY'
import json
from pathlib import Path
rows=json.loads(Path('data/vocab/l3_general_inclusion_option1_manifest_v1.json').read_text(encoding='utf-8'))
print('rows', len(rows))
print('distinct_items', len({r['item_id'] for r in rows}))
print('distinct_contents', len({r['content_id'] for r in rows}))
print('source_versions', sorted({r['source_version'] for r in rows}))
print('item_types', sorted({r['item_type'] for r in rows}))
PY
```

결과:

```text
rows 128
distinct_items 128
distinct_contents 64
source_versions ['nikl_grade5_l3_batch1_v1', 'nikl_grade5_l3_batch2_v1']
item_types ['CONTEXT_MEANING', 'MEANING_CHOICE']
```

## 테스트/검증

### py_compile

명령:

```bash
venv/Scripts/python.exe -m py_compile app/vocabulary_quiz/routers/multiformat.py tests/test_l3_general_inclusion_option1.py
```

결과: exit code 0, 출력 없음.

### pytest 직접 실행

명령:

```bash
venv/Scripts/python.exe -m pytest tests/test_l3_general_inclusion_option1.py -q
```

결과:

```text
C:\Users\aproa\aprolabs\venv\Scripts\python.exe: No module named pytest
```

해석: 프로젝트 venv에 pytest가 설치되어 있지 않아 pytest runner는 실행 불가.

### 동일 테스트 함수 직접 실행

명령:

```bash
venv/Scripts/python.exe - <<'PY'
import tests.test_l3_general_inclusion_option1 as t
for name in [
    'test_l3_all_candidates_includes_only_option1_whitelist_items',
    'test_l3_auto_only_does_not_include_review_boundary_option1_items',
    'test_non_l3_level_does_not_include_option1_whitelist_even_if_level_row_matches',
    'test_level_none_availability_remains_source_version_only',
]:
    getattr(t, name)()
    print('PASS', name)
PY
```

결과:

```text
PASS test_l3_all_candidates_includes_only_option1_whitelist_items
PASS test_l3_auto_only_does_not_include_review_boundary_option1_items
PASS test_non_l3_level_does_not_include_option1_whitelist_even_if_level_row_matches
PASS test_level_none_availability_remains_source_version_only
```

## 확인된 동작

1. L3 `all_candidates`는 기존 `SOURCE_VERSION="2.1.29"` 문항과 option1 whitelist batch item을 함께 포함한다.
2. L3 `auto_only`는 `REVIEW_BOUNDARY` option1 item을 포함하지 않는다.
3. L2 등 비-L3 level은 whitelist item이 level row를 가져도 일반 후보에 포함하지 않는다.
4. `level=None` availability는 batch item을 세지 않고 기존 source version만 센다.
5. unwhitelisted batch source item은 L3 `all_candidates`에서도 제외된다.

## 실제 외부 영향

- DB write: 0
- 배포: 없음
- 연구 서버 변경: 없음
- 학생 공개/allowlist/feature flag 변경: 없음
- git add/commit/push: 없음

## 다음 승인 필요

실제 연구 DB/서비스 반영은 아직 하지 않았다. 다음 단계는 별도 승인 후에만 가능하다.

- 로컬 변경을 서버 코드에 배포할지 여부
- 서버의 DB 상태가 이미 옵션1 L3 level row 64건을 포함하는지 재확인
- 배포 후 관리자 L3 availability/API smoke test 실행
- 학생 공개/allowlist/feature flag는 계속 별도 승인 필요
