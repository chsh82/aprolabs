# schema/literacy 잔여 변경 정리 계획 — 2026-10-08

## 범위

어휘 DB 전환 종료 후 남은 schema/literacy 관련 변경을 분리해 정리한다.

이번 단계의 목표는 기존 phase8/15/16 산출물과 재발 방지 테스트를 안전하게 커밋해, 어휘 DB 작업과 섞이지 않게 만드는 것이다.

## 커밋 후보

### krdict fallback definitions[0] 재발 방지 묶음

- `scripts/literacy/import_textbook_vocab.py`
- `tests/test_krdict_fallback_hold_fix.py`
- `scripts/literacy/scan_momo_textbook_krdict_fallback_821_full.py`
- `reports/schema_reading_phase8_krdict_fallback_full_audit_20260924.md`
- `reports/literacy_repr_errors_26_verdict_20260924.csv`
- `reports/literacy_repr_errors_26_verdict_20260924.jsonl`
- `reports/literacy_krdict_fallback_dryrun_6_20260924.csv`
- `reports/literacy_krdict_fallback_dryrun_6_20260924.md`
- `data/import/krdict_fallback_821_full_audit_20260924.json`
- `data/import/literacy_repr_errors_26_scan_20260924.json`

의미:

- momo-textbook krdict fallback에서 `definitions[0]`를 무조건 채택하던 위험을 막는다.
- 원본 정의가 없고 krdict 동음이의/다의어가 있으면 자동 채택하지 않고 보류한다.
- 실제 DB 재적재는 하지 않는다.

### phase15/16 caution 및 문구 정정 묶음

- `data/import/schema_reading_phase15_l4l5_audit_20260925.csv`
- `data/import/schema_reading_phase15_l4l5_audit_20260925.jsonl`
- `data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.csv`
- `data/import/schema_reading_phase16_quiz_pilot_dryrun_20260925.jsonl`
- `reports/schema_reading_phase15_s_cards_20260925.md`

의미:

- 유사/유추/사법권 등 phase15 감사에서 본문에만 있던 caution을 결과표 caution 필드에 반영한다.
- 집단 설명 문구의 조사 오류 `무리'이라는`을 `무리'라는`으로 고친다.
- 파일 산출물 정정이며 DB write는 없다.

## 검증

실행 완료:

```bash
C:/Users/aproa/aprolabs/venv/Scripts/python.exe -m py_compile scripts/literacy/import_textbook_vocab.py tests/test_krdict_fallback_hold_fix.py
C:/Users/aproa/aprolabs/venv/Scripts/python.exe tests/test_krdict_fallback_hold_fix.py
```

결과:

- py_compile: pass
- 회귀 테스트: `29/29 passed`

## 제외/보류

이번 커밋에 넣지 않을 항목:

- schema/literacy의 다른 phase9~35 대량 산출물 중 현재 변경/테스트와 직접 관련 없는 파일
- momo worksheet/page editor 변경
- `l2_check.json`
- L3 option1 보존 zip

## 안전 경계

- DB write 없음.
- 배포 없음.
- 학생 공개/allowlist/feature flag 변경 없음.
- 서버 서비스 재시작 없음.
- `.env` 또는 secret 값 읽기/출력 없음.
