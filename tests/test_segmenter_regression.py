"""segmenter 회귀 테스트.

사용법:
    python3 tests/test_segmenter_regression.py --baseline tests/segmenter_baseline.json
"""
import sys
import os
import json
import argparse
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import api_usage  # noqa
from app.models.passage import PipelineJob, ExamPaper  # noqa
from app.models.answer_key import AnswerKey, AnswerKeyItem  # noqa
from app.models.question import Question  # noqa
from app.models.user import User  # noqa
from app.services.layout_analyzer import extract_pdf_text
from app.services.segmenter import segment_text

_PASS = "\033[92m[PASS]\033[0m"
_WARN = "\033[93m[WARN]\033[0m"
_FAIL = "\033[91m[FAIL]\033[0m"
_SKIP = "\033[90m[SKIP]\033[0m"


def run(baseline_path: str) -> bool:
    with open(baseline_path, encoding="utf-8") as f:
        baseline = json.load(f)

    print(f"baseline: {baseline['commit']} ({baseline['created_at'][:10]}), {baseline['total_files']}파일\n")

    total_files = 0
    total_questions = 0
    warnings = []
    failures = []

    for filename, bdata in baseline["files"].items():
        file_path = bdata.get("file_path")
        if not file_path or not os.path.exists(file_path):
            print(f"  {_SKIP} {filename} (PDF 없음: {file_path})")
            continue

        total_files += 1
        print(f"  실행 중: {filename}", end="", flush=True)

        try:
            text, _, _ = extract_pdf_text(file_path)
            new_segs = segment_text(text, question_hints=[])
        except Exception as e:
            failures.append(f"{filename}: 실행 오류 — {e}")
            print(f"  → {_FAIL} (실행 오류)")
            continue

        new_qs = {q["number"]: q for q in new_segs.get("questions", [])}
        old_qs = {q["number"]: q for q in bdata["questions"]}
        file_ok = True

        # 문항 수 감소
        if len(new_qs) < bdata["question_count"]:
            lost = sorted(set(old_qs.keys()) - set(new_qs.keys()))
            failures.append(
                f"{filename}: 문항 수 {bdata['question_count']}→{len(new_qs)} (누락: {lost})"
            )
            file_ok = False

        for num, old_q in old_qs.items():
            new_q = new_qs.get(num)
            if new_q is None:
                continue  # 이미 위에서 처리

            new_stem = (new_q.get("stem") or "").strip()
            new_choices = len(new_q.get("choices") or {})

            # stem 비어짐
            if old_q["stem_length"] > 5 and len(new_stem) < 5:
                failures.append(
                    f"{filename} Q{num}: stem 비어짐 (기존 {old_q['stem_length']}자)"
                )
                file_ok = False

            # choices 감소
            if new_choices < old_q["choices_count"]:
                failures.append(
                    f"{filename} Q{num}: choices {old_q['choices_count']}→{new_choices}"
                )
                file_ok = False

            # stem 길이 50%+ 변화 경고
            old_len = old_q["stem_length"]
            new_len = len(new_stem)
            if old_len > 10 and new_len > 0:
                ratio = abs(new_len - old_len) / old_len
                if ratio >= 0.5:
                    warnings.append(
                        f"{filename} Q{num}: stem {old_len}→{new_len}자 ({ratio*100:.0f}% 변화)"
                    )

        total_questions += len(new_qs)
        print(f"  → {_PASS if file_ok else _FAIL} ({len(new_qs)}문항)")

    print()
    print("=" * 60)

    if warnings:
        for w in warnings:
            print(f"  {_WARN} {w}")
        print()

    if failures:
        for fl in failures:
            print(f"  {_FAIL} {fl}")
        print()

    if failures:
        print(f"결과: {_FAIL} — {total_files}파일 {total_questions}문항 | 회귀 {len(failures)}건 경고 {len(warnings)}건")
        return False
    else:
        print(f"결과: {_PASS} — {total_files}파일 {total_questions}문항 | 회귀 0건 경고 {len(warnings)}건")
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, help="baseline JSON 경로")
    args = parser.parse_args()
    ok = run(args.baseline)
    sys.exit(0 if ok else 1)
