"""⑦단계: 같은 필기 이미지를 여러 모델(제공자 포함)로 인식해 나란히 비교한다
(사용자 지시 2026-09-23 "그때 비교할 수 있게, 같은 필기에 대해 모델을 바꿔
호출할 수 있는 옵션을 남겨 주세요").

실제 태블릿에서 쓴 필기 PNG를 준비한 뒤 이 스크립트로 여러 모델 결과를 비교하면 됨.
--models는 "제공자:모델"을 콤마로 구분한다(제공자 생략 시 anthropic으로 간주 -
기본 인식 제공자와 맞춤, edition/recognize.py 참고).

기본 비교 대상은 Claude 3종(Haiku 4.5/Sonnet 5/Opus 5.5)이다 - 2026-09-27
사용자 지시로 기본 인식 제공자를 Gemini에서 Anthropic으로 되돌리면서(약관
조사 결과, SPEC_손글씨_인식_제공자_정책.md 참고) 등급 비교 대상도 Claude로
바꿨다. Gemini는 --models로 여전히 비교 대상에 넣을 수 있다(GEMINI_API_KEY
필요, 계약 경로 정리 전까지는 참고용).
ANTHROPIC_API_KEY(anthropic)/GEMINI_API_KEY(gemini) 환경변수가 있어야 한다.

실행:
    python tests/compare_recognition_models.py 필기.png
    python tests/compare_recognition_models.py 필기.png "학생이 답한 질문: ..."
    python tests/compare_recognition_models.py 필기.png "..." \
        --models anthropic:claude-haiku-4-5-20251001,anthropic:claude-sonnet-5,gemini:gemini-3.1-flash-lite
"""
from __future__ import annotations

import argparse
import asyncio
import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from edition.recognize import RecognitionError, recognize_handwriting  # noqa: E402

# 사용자 지시(2026-09-27, 2차): 기본 인식 제공자를 Anthropic으로 되돌리면서
# 등급 비교 기본 대상도 Claude 3종으로. "Opus 5.5"의 정확한 API 모델 ID는
# 이 저장소의 다른 곳(시스템 프롬프트 등)에서 확인된 적이 없어 기존 명명
# 규칙(claude-haiku-4-5-20251001의 "4-5"가 "4.5")을 그대로 적용해 추정했다 -
# 실제 실행 전에 정확한 ID인지 확인 필요.
DEFAULT_TARGETS = [
    "anthropic:claude-haiku-4-5-20251001",
    "anthropic:claude-sonnet-5",
    "anthropic:claude-opus-5-5",
]


def _parse_target(spec: str) -> tuple[str, str]:
    if ":" in spec:
        provider, model = spec.split(":", 1)
        return provider.strip(), model.strip()
    return "anthropic", spec.strip()


DEFAULT_PROMPT = "\n".join([
    "이미지는 초등학교 5학년 학생이 태블릿에 펜으로 쓴 한국어 손글씨 답안입니다.",
    "학생이 답한 질문: (비교 테스트용 - 실제 질문 문맥 없음)",
    "할 일: 이미지에 적힌 글자를 보이는 그대로 옮겨 적으세요.",
    "- 맞춤법과 띄어쓰기를 고치지 마세요. 내용을 보태거나 질문에 맞게 바꾸지 마세요.",
    "- 줄바꿈은 쓴 그대로 유지하세요.",
    "- 도저히 알아볼 수 없는 글자는 [?] 로 적으세요. 지운 흔적과 낙서는 무시하세요.",
    'JSON 하나로만 답하세요. 예: {"text": "옮겨 적은 내용", "unclear": 0}',
])


async def run(image_path: Path, prompt: str, targets: list[tuple[str, str]]) -> None:
    image_bytes = image_path.read_bytes()
    print(f"이미지: {image_path} ({len(image_bytes)} bytes)\n")
    for provider, model in targets:
        label = f"{provider}:{model}"
        try:
            result = await recognize_handwriting(image_bytes, prompt, model=model, provider=provider)
            print(f"[{label}] {result['latency_ms']}ms unclear={result['unclear']}")
            print(f"  text: {result['text']!r}")
        except RecognitionError as e:
            print(f"[{label}] 실패({e.code}): {e}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(description="필기 이미지를 여러 모델(제공자 포함)로 인식 비교")
    parser.add_argument("image", type=Path, help="필기 PNG 파일 경로")
    parser.add_argument("prompt", nargs="?", default=DEFAULT_PROMPT, help="인식 프롬프트(생략 시 기본 프롬프트)")
    parser.add_argument("--models", default=",".join(DEFAULT_TARGETS),
                         help="쉼표로 구분한 '제공자:모델' 목록(제공자 생략 시 gemini)")
    args = parser.parse_args()

    if not args.image.exists():
        print(f"파일을 찾을 수 없음: {args.image}")
        return 1

    targets = [_parse_target(t) for t in args.models.split(",") if t.strip()]
    asyncio.run(run(args.image, args.prompt, targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())
