# 손글씨 인식 제공자 - 약관 조사 및 결정 기록

2026-09-27. 손글씨 인식(`/api/runtime/recognize`) 기본 제공자를 정하면서
조사한 내용과 최종 결정을 기록한다. 파트너사와 계약할 때 그대로 참고
자료로 쓸 수 있게 원문을 인용하고 출처를 남긴다.

## 결정

**기본 제공자: Anthropic(Claude).** Gemini는 코드에는 남아 있지만 기본값이
아니고, `GEMINI_API_KEY`가 없으면 호출 자체가 막힌다(`RECOGNITION_PROVIDER=
gemini`를 명시하거나 요청마다 `provider=gemini`를 넘겨야만 시도되고, 키가
없으면 `RecognitionError`로 바로 실패한다 - `edition/recognize.py`).

**판단 근거(사용자 결정)**:
- Gemini 쪽 조항이 명확하고 교육용 예외가 없음 - 우리 교재가 정확히 해당.
- 비용 차이가 월 몇 달러 수준이라 위험을 감수할 이유가 없음.
- B2B 계약에서 파트너가 개인정보 처리 현황을 물었을 때 약관 위반 상태로는
  답할 수 없음.

**전환 방법**: 나중에 계약 경로(예: Vertex AI 등 기업 계약)가 정리되면
`RECOGNITION_PROVIDER=gemini` 환경변수만 바꾸면 된다(코드 수정 불필요).

## 1. Gemini Developer API - 연령 조항 (사용 불가 판단의 핵심 근거)

출처: [Gemini API Additional Terms of Service](https://ai.google.dev/gemini-api/terms) - "Age Requirements" 절.

> "You must be 18 years of age or older to use the APIs. You also will not
> use the Services as part of a website, application, or other service
> (collectively, "API Clients") that is directed towards or is likely to
> be accessed by individuals under the age of 18."

**해석**: API를 직접 쓰는 사람뿐 아니라, 그 API를 이용해 만든 서비스
("API Client")가 "18세 미만이 이용할 가능성이 있는" 것이기만 해도 금지
대상이다. 유료/무료 등급 구분이 없고, 교육기관·학부모 동의 등 어떤
예외 조항도 없다(2026-09-27 확인). 모모의 책장은 초·중등 학생이 태블릿에
직접 손글씨를 쓰는 서비스라 정확히 이 조항에 해당한다.

**참고(비슷한 사례)**: 다른 국내 교육 프로젝트(BOOKIT, 초·중등 대상)도
같은 조항을 발견해 벤더를 재검토한 기록이 있다
([GitHub 이슈](https://github.com/AI-X-16-1/BOOKIT/issues/54)).

**데이터 보관 정책(참고용 - 위 연령 조항이 더 근본적 문제라 여기서는
부차적)**: 유료 API는 기본적으로 프롬프트/응답을 모델 학습에 쓰지 않지만
([데이터 로깅 정책](https://ai.google.dev/gemini-api/docs/logs-policy)),
남용 감지 목적으로 최대 55일 보관되고, 완전 무보관(zero data retention)은
표준 API가 아니라 Vertex AI 기업 계약에서만 제공된다
([ZDR 문서](https://ai.google.dev/gemini-api/docs/zdr)).

## 2. Anthropic - 미성년자 대상 서비스는 허용, 단 안전조치 조건부

출처: [Responsible Use of Anthropic's Models: Guidelines for Organizations
Serving Minors](https://support.claude.com/en/articles/9307344) ·
[Child safety guidance for developers](https://support.claude.com/en/articles/15591275)

Anthropic 소비자 제품(claude.ai)은 18세 미만 직접 가입을 막지만, **API로
만든 제품이 미성년자를 대상으로 하는 것 자체는 금지하지 않는다.** 대신
아래 안전조치를 요구한다:

- **연령 확인(age verification)**: "의도한 사용자만 접근하도록 하는 연령
  확인 체계"
- **콘텐츠 모니터링·필터링**: "부적절하거나 유해한 콘텐츠를 차단하는
  콘텐츠 조정·필터링"
- **모니터링·신고 체계**: "잠재적 문제를 식별·대응하는 모니터링·신고
  메커니즘"
- **법규 준수(필수)**: "COPPA 등 적용 가능한 아동 안전·개인정보 보호
  규정을 준수해야 함"
- **고지(필수)**: "사람이 아니라 AI 시스템과 상호작용하고 있음을
  사용자에게 고지해야 함"
- Anthropic은 위반 여부를 주기적으로 감사(audit)하며, 미준수 시 계정
  정지·해지 가능.

Anthropic은 특정 벤더·프로그램 사용을 강제하지 않으며, 참고 자료로
Thorn·Tech Coalition·Internet Watch Foundation을 언급한다. "학원이 계정을
관리하는 구조가 이 요건을 자동으로 만족하는지"는 가이드에 명시돼 있지
않다 - 조직이 "실제 최종 사용자가 서비스와 상호작용하는 방식"에 맞춰
직접 판단해 조치를 구성해야 한다.

## 3. 우리 구조에서 어떻게 충족할지 (검토)

**구조 확인**: 현재 momo_b2b_tablet은 학생이 Anthropic/Google 계정을
직접 만들거나 그 서비스에 로그인하는 구조가 **아니다.** `ANTHROPIC_API_KEY`는
서버(`edition/api.py`)에서만 쓰이고, 학생은 학원이 발급한 edition URL로
태블릿에서 우리 화면만 본다(`renderer/adapters/api.js` - 세션 교환도
아직 launch 토큰 기반 파트너 인증이 붙기 전 단계). 즉 "학생이 Claude에
직접 가입"하는 경로 자체가 없고, 접근은 학원(파트너사)이 관리한다 -
사용자가 물어본 "학원이 계정을 만드는 구조"가 맞다.

| Anthropic 요구사항 | 현재 상태 | 비고 |
|---|---|---|
| 연령 확인 | **학원이 접근을 관리**(학생이 직접 가입 안 함) - 개별 사용자 연령 확인 API는 없음 | 파트너 계약으로 "학원이 등록한 학생만 접근"을 명문화하면 이 요건의 실질적 대체가 됨. 파트너 세션 인증(launch 토큰)이 아직 미구현이라 **지금은 URL만 알면 누구나 접근 가능** - 이 부분은 실제 안전조치라 하기 어려움(아래 "남은 일" 참고) |
| 콘텐츠 모니터링·필터링 | **구조적으로 위험이 낮음** - 인식 프롬프트가 "이미지 속 글자를 그대로 옮겨 적어라"는 단일 목적 OCR이지 자유 대화가 아님(SPEC §8 고정 프롬프트, 제공자 무관 동일) | 열린 채팅 인터페이스가 없어 유해 콘텐츠 생성 경로 자체가 좁음 |
| 모니터링·신고 | `recognition_log`에 모든 인식 호출(제공자·모델·텍스트·unclear 수)이 이미 남음(2026-09-27 provider 컬럼 추가) | 이상 응답을 주기적으로 점검하는 절차는 아직 없음(운영 절차로 남겨 둘 것) |
| 법규 준수(COPPA 등) | **국내는 개인정보보호법 제22조의2**(만 14세 미만 아동 개인정보 처리 시 법정대리인 동의 필요)가 대응 조항 - 별도 검토 필요 | 학원이 학생 등록 시 보호자 동의를 이미 받는 구조인지 파트너사에 확인 필요 |
| AI 시스템임을 사용자에게 고지 | 학생 화면의 "글자로 확인하기" 시트에 안내 문구 추가함(2026-09-27, `renderer.js`) - "인공지능(AI)이 손글씨를 읽어서 보여줘요" | 완료 |

**남은 일(우선순위 낮음, 별도 과제로 분리)**:
- 파트너 세션 인증(launch 토큰 교환)이 아직 구현 전이라, "학원이 등록한
  학생만 접근"이 기술적으로 강제되지 않는다 - 지금은 edition URL을 아는
  누구나 접근 가능(§6/§7 이후 단계 몫으로 이미 문서화돼 있던 항목).
  실제 서비스 전에는 이게 사실상의 "연령 확인" 대체 수단이 되므로
  우선순위를 올려야 한다.
- 국내 개인정보보호법(만 14세 미만 법정대리인 동의) 요건은 momo_b2b_tablet
  코드가 아니라 파트너(학원) 쪽 학생 등록 절차에 달려 있다 - 계약서에
  "파트너가 보호자 동의를 확보한 학생만 등록한다"는 조항이 필요.

## 4. 파트너 계약(B2B)에 반영할 조항 제안

1. 파트너(학원)는 서비스에 등록하는 학생에 대해 법정대리인(보호자) 동의를
   확보한 상태여야 한다(개인정보보호법 제22조의2 대응).
2. 손글씨 이미지는 [현재 제공자]의 API로 전송되어 텍스트 인식에만
   사용되며, 그 제공자의 데이터 사용 정책(학습 미사용 여부, 보관 기간)을
   명시한다 - 제공자를 바꾸면 이 조항도 같이 갱신해야 함.
3. 학생 화면에 "AI가 손글씨를 인식한다"는 사실이 고지된다는 점을
   명시(이미 구현됨).
4. 서비스는 파트너가 등록·관리하는 학생만 접근하며, 개별 학생이 직접
   가입하지 않는다는 점(연령 확인 요건의 실질적 근거).

## 5. 별도 과제(낮은 우선순위)

Vertex AI 등 Google의 기업 계약 경로에 위 "Age Requirements"와 다른
조항이 있는지는 아직 확인하지 않았다. 지금은 비용이 문제되는 규모가
아니므로(Claude 기준 월 몇 달러 수준) 우선순위를 낮춰 뒀다 - 필요해지면
이 섹션에 이어서 조사한다.
