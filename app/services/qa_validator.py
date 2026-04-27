"""
G-1: QA 검증 — segment 결과물 품질 검사
"""
import re
import json

_SCORE_RE = re.compile(r'\[[23]점\]')
_KO_QEND_RE = re.compile(
    r'(?:것은|것인가|엇인가|무엇인가|있는가|없는가|옳은가|맞는가|알맞은가|고르면)[?？]'
)
_QMARK_RE = re.compile(r'[?？]')
_STMT_RE = re.compile(r'(?:이다|였다)[.。](?=[\s　]+\S)')


def _find_stem_end(text: str) -> int:
    """stem 종결 인덱스. 분리 불가 시 -1."""
    m = _SCORE_RE.search(text)
    if m and text[m.end():].strip():
        return m.end()

    ko_matches = list(_KO_QEND_RE.finditer(text))
    if ko_matches:
        last_m = ko_matches[-1]
        if text[last_m.end():].strip():
            return last_m.end()

    positions = [m.start() for m in _QMARK_RE.finditer(text)]
    if positions:
        last_q = positions[-1]
        if text[last_q + 1:].strip():
            return last_q + 1

    m = _STMT_RE.search(text)
    if m and text[m.end():].strip():
        return m.end()

    return -1


def _detect_stem_bogi_merge(stem: str) -> bool:
    """stem 안에 bogi가 혼입됐는지 판단."""
    if not stem or len(stem) < 50:
        return False
    idx = _find_stem_end(stem)
    if idx < 0:
        return False
    remaining = stem[idx:].strip()
    return len(remaining) >= 50


def _infer_expected_questions(questions: list) -> set:
    """발견된 번호에서 연속 범위 갭으로 누락 번호 추론."""
    nums = sorted(
        q.get("number") for q in questions
        if isinstance(q.get("number"), int)
    )
    if not nums:
        return set()
    missing = set()
    for i in range(len(nums) - 1):
        gap = nums[i + 1] - nums[i]
        if 1 < gap <= 5:
            for m in range(nums[i] + 1, nums[i + 1]):
                missing.add(m)
    return missing


def validate_segments(segments: dict, pdf_path: str = None) -> dict:
    """
    segment 결과물 품질 검사.
    반환: {passed, issues, stats}
    """
    questions_raw = segments.get("questions", [])
    if isinstance(questions_raw, str):
        questions_raw = json.loads(questions_raw)
    questions = list(questions_raw)

    issues = []

    for q in questions:
        num = q.get("number")
        stem = (q.get("stem") or "").strip()
        choices = q.get("choices") or {}
        choices_count = len(choices)
        existing_bogi = (q.get("bogi") or "").strip()

        if len(stem) < 5:
            issues.append({
                "type": "empty_stem",
                "severity": "high",
                "question_number": num,
                "detail": f"stem {len(stem)}자",
            })

        if choices_count == 0:
            issues.append({
                "type": "choices_incomplete",
                "severity": "medium",
                "question_number": num,
                "detail": "choices 없음",
            })
        elif choices_count < 5:
            issues.append({
                "type": "choices_incomplete",
                "severity": "medium",
                "question_number": num,
                "detail": f"choices {choices_count}/5",
            })

        if not existing_bogi and _detect_stem_bogi_merge(stem):
            issues.append({
                "type": "stem_bogi_merged",
                "severity": "medium",
                "question_number": num,
                "detail": f"stem {len(stem)}자에 bogi 혼입 의심",
            })

    for m_num in sorted(_infer_expected_questions(questions)):
        issues.append({
            "type": "missing_question",
            "severity": "high",
            "question_number": m_num,
            "detail": f"Q{m_num} 누락 (범위 내 불연속)",
        })

    passed = not any(i["severity"] == "high" for i in issues)
    stats = {
        "question_count": len(questions),
        "issue_count": len(issues),
        "high_count": sum(1 for i in issues if i["severity"] == "high"),
        "medium_count": sum(1 for i in issues if i["severity"] == "medium"),
    }

    return {"passed": passed, "issues": issues, "stats": stats}
