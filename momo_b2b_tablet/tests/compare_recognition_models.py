"""⑦단계: 같은 필기 이미지를 여러 모델로 인식해 나란히 비교한다(사용자 지시
2026-09-23 "그때 비교할 수 있게, 같은 필기에 대해 모델을 바꿔 호출할 수 있는
옵션을 남겨 주세요").

실제 태블릿에서 쓴 필기 PNG를 준비한 뒤 이 스크립트로 여러 모델 결과를 비교하면 됨.
ANTHROPIC_API_KEY 환경변수가 있어야 한다.

실행:
    python tests/compare_recognition_models.py 필기.png "학생이 답한 질문: ..."
    python tests/compare_recognition_models.py 필기.png "..." --models claude-haiku-4-5-20251001,claude-sonnet-5,claude-opus-5
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

DEFAULT_MODELS = ["claude-haiku-4-5-20251001", "claude-sonnet-5", "claude-opus-5"]
DEFAULT_PROMPT = "\n".join([
    "이미지는 초등학교 5학년 학생이 태블릿에 펜으로 쓴 한국어 손글씨 답안입니다.",
    "학생이 답한 질문: (비교 테스트용 - 실제 질문 문맥 없음)",
    "할 일: 이미지에 적힌 글자를 보이는 그대로 옮겨 적으세요.",
    "- 맞춤법과 띄어쓰기를 고치지 마세요. 내용을 보태거나 질문에 맞게 바꾸지 마세요.",
    "- 줄바꿈은 쓴 그대로 유지하세요.",
    "- 도저히 알아볼 수 없는 글자는 [?] 로 적으세요. 지운 흔적과 낙서는 무시하세요.",
    'JSON 하나로만 답하세요. 예: {"text": "옮겨 적은 내용", "unclear": 0}',
])


async def run(image_path: Path, prompt: str, models: list[str]) -> None:
    image_bytes = image_path.read_bytes()
    print(f"이미지: {image_path} ({len(image_bytes)} bytes)\n")
    for model in models:
        try:
            result = await recognize_handwriting(image_bytes, prompt, model=model)
            print(f"[{model}] {result['latency_ms']}ms unclear={result['unclear']}")
            print(f"  text: {result['text']!r}")
        except RecognitionError as e:
            print(f"[{model}] 실패({e.code}): {e}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(description="필기 이미지를 여러 모델로 인식 비교")
    parser.add_argument("image", type=Path, help="필기 PNG 파일 경로")
    parser.add_argument("prompt", nargs="?", default=DEFAULT_PROMPT, help="인식 프롬프트(생략 시 기본 프롬프트)")
    parser.add_argument("--models", default=",".join(DEFAULT_MODELS), help="쉼표로 구분한 모델 목록")
    args = parser.parse_args()

    if not args.image.exists():
        print(f"파일을 찾을 수 없음: {args.image}")
        return 1

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    asyncio.run(run(args.image, args.prompt, models))
    return 0


if __name__ == "__main__":
    sys.exit(main())
