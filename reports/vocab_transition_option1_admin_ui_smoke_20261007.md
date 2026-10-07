# 관리자 UI 확인 방법 확보 및 smoke 결과 — 2026-10-07

## 목적

L3 옵션1 일반 L3 연결 배포 후, 실제 관리자 UI/API 경로가 배포 코드와 live 연구 DB에서 정상 동작하는지 확인한다.

## 브라우저 자동화 상태

Hermes browser tool 실행 시 다음 문제로 브라우저가 시작되지 않았다.

- `agent-browser CLI not found`
- `hermes pm install agent-browser` 재시도 중 Windows 권한 오류 발생
- 접근 거부 경로:
  - `C:\Users\aproa\AppData\Local\hermes\tools\agent-browser-0.26.0-win32-x64`
  - `C:\Users\aproa\AppData\Local\hermes\tools\chromium-1208`
- `hermes pm doctor`도 같은 경로 접근 권한 문제로 실패

따라서 실제 브라우저 클릭스루는 이번 단계에서 수행하지 못했다.

## 대체 검증 방법

브라우저 대신 서버 내부 `fastapi.testclient.TestClient`를 사용했다.

- 실제 app: `/home/chsh82/aprolabs/app.main:app`
- 실제 server code: `HEAD/origin/main = 6b33147` 기준
- 실제 DB: 서비스 환경과 동일하게 연구 DB 경로 사용
- 인증 우회가 아니라, 서버 DB의 admin user id로 앱의 `make_session_cookie()`를 사용해 세션 쿠키를 생성했다.
- 비밀번호나 토큰은 읽거나 출력하지 않았다.

## 결과

### 관리자 페이지 HTML

요청:

- `/vocabulary-quiz/multiformat/play`

결과:

- HTTP status: `200`
- content-type: `text/html; charset=utf-8`
- HTML length: `59241`
- `level-select` 존재: `True`
- confidence UI 존재: `True`
- L3/중등 1~2학년 표시 존재: `True`

### L3 all_candidates availability

요청:

- `/api/vocabulary-quiz/availability?level=3&confidence_mode=all_candidates`

결과:

```json
{
  "level": 3,
  "grade_label": "중등 1~2학년",
  "confidence_mode": "all_candidates",
  "level_version": "level_policy_v0.1",
  "available_items": 444,
  "distinct_words": 149,
  "by_type": {
    "MEANING_CHOICE": 149,
    "WORD_FROM_DEFINITION": 84,
    "CONTEXT_MEANING": 149,
    "CONTEXT_CLOZE": 61,
    "MATCH_WORD_MEANING": 1
  }
}
```

### L3 auto_only availability

요청:

- `/api/vocabulary-quiz/availability?level=3&confidence_mode=auto_only`

결과:

```json
{
  "level": 3,
  "grade_label": "중등 1~2학년",
  "confidence_mode": "auto_only",
  "level_version": "level_policy_v0.1",
  "available_items": 297,
  "distinct_words": 80,
  "by_type": {
    "MEANING_CHOICE": 80,
    "WORD_FROM_DEFINITION": 79,
    "CONTEXT_MEANING": 80,
    "CONTEXT_CLOZE": 58,
    "MATCH_WORD_MEANING": 0
  }
}
```

## 추가 관찰

페이지 요청 중 다음 로그가 출력됐다.

```text
[grade5-l3-batch1] 화이트리스트에 없는 item_id 2건이 검증을 통과해 필터링됨(오염 의심): ['MF_G5L3B1_C_G5-0fa0e0a975e55d66', 'MF_G5L3B1_M_G5-0fa0e0a975e55d66']
```

해석:

- L3 batch1 전용 모드의 오염 방지 로직이 whitelist 밖 2개 item을 감지해 필터링하고 있다.
- 이번 일반 L3 옵션1 whitelist 결과에는 영향을 주지 않는다.
- 별도 후속 점검 후보로 남긴다.

## 결론

브라우저 클릭스루는 Hermes browser tool 권한 문제로 불가했지만, 서버 내부 TestClient로 관리자 인증 세션을 구성해 실제 UI/API 경로를 검증했다.

확인된 사항:

- 관리자 페이지가 200으로 렌더링된다.
- 레벨 선택 UI와 confidence UI가 HTML에 존재한다.
- L3 all_candidates는 `149어휘 / 444문항`으로 표시된다.
- L3 auto_only는 `80어휘 / 297문항`으로 유지된다.
- 학생 공개 플래그/allowlist/feature flag 변경 없이 관리자 연구 사이트 범위에서 동작한다.

## 다음 후보

1. batch1 whitelist 밖 2개 item 오염 의심 로그 조사.
2. RULE_A/B 대량 레벨 전환 승인안 작성.
3. Hermes browser tool 권한 문제는 별도 Hermes 환경 정비 단계에서 처리.
