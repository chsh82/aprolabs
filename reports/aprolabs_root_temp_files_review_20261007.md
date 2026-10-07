# 루트 임시/보존 파일 확인 결과 — 2026-10-07

대상:

- `l2_check.json`
- `lemma_list.txt`
- `script_b64.txt`
- `reports/vocab_transition_option1_general_l3_commit_message_20261007.txt`

## 확인 결과

### `l2_check.json`

- 크기: 14,860 bytes
- 라인 수: 439
- SHA256: `62c1682565a7796ae733410a5d23c1f8a558569af1c0420e254d508b2667ed7b`
- 내용 요약: `momo-edition/1` 스키마의 `L2-Q2-W08` 워크시트/교재 JSON으로 보임.
- 판단: 현재 어휘 DB 작업과 무관. 다만 momo_book_db/worksheet 작업의 샘플/디버그 입력일 가능성이 있어 즉시 삭제 비권장.
- 추천: 워크시트 작업 정리 단계까지 보류하거나, 필요 없다고 확인되면 삭제.

### `lemma_list.txt`

- 크기: 520 bytes
- 라인 수: 2
- SHA256: `5307d1cb78d6fa1b39cec385ef005aee406f6aa22361bcd54a43496e1d1ce3d6`
- 내용 요약: 첫 줄 `73`, 둘째 줄은 깨진 한글처럼 보이는 lemma 목록.
- 판단: 인코딩이 깨진 임시 추출물 또는 잘못 생성된 진단 파일 가능성이 높음.
- 추천: 삭제 후보. 단, 삭제 전 필요하면 `cp`로 scratch/archive에 보관 가능.

### `script_b64.txt`

- 크기: 8,576 bytes
- 라인 수: 1
- SHA256: `3e116a6b950a34aa131ccd252772156237c69607fc0c0d2f874ed58d53f5c174`
- 내용 요약: base64 인코딩된 Python 스크립트로 보임. 미리보기상 `extract_teacher.py`, `anthropic`, `pdftext` 등의 문자열이 포함된 추출/파싱 스크립트 가능성.
- 판단: 루트에 둘 파일은 아니지만, 과거 PDF/교사용 추출 작업 복구용 조각일 가능성이 있어 즉시 삭제는 약간 위험.
- 추천: 삭제 전 디코딩해서 파일 목적을 확인하거나 archive로 이동.

### `reports/vocab_transition_option1_general_l3_commit_message_20261007.txt`

- 크기: 368 bytes
- 라인 수: 6
- SHA256: `f6d48a6e8c87dacfa23fca7ca91030ae4f16a1038b9503bd55fac97395e8a03b`
- 내용 요약: 이미 완료된 `c0b215d` 커밋 메시지 초안.
- 판단: 역할 완료.
- 추천: 삭제 가능.

## 권장 액션

가장 안전한 다음 액션:

1. `reports/vocab_transition_option1_general_l3_commit_message_20261007.txt` 삭제.
2. `lemma_list.txt`는 삭제 후보지만, 대표님 승인 후 삭제.
3. `script_b64.txt`는 먼저 디코딩 검사 후 보관/삭제 판단.
4. `l2_check.json`은 momo worksheet 트랙과 관련 가능성이 있어 보류.

## 승인 요청안

- A안: 커밋 메시지 txt만 삭제하고 나머지는 보류.
- B안: 커밋 메시지 txt + lemma_list.txt 삭제, script_b64는 디코딩 검사, l2_check는 보류.
- C안: 삭제 없이 전부 보류하고 다음 트랙으로 이동.

Davinci 권장: B안.
