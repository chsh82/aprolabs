"""samples/*.layout.json(사람이 검수·확정한 골든 시안) 대비 정규화 결과 비교
- 사용자 지시 3번. golden에는 검수 단계에서 고친 오타가 들어있어(정규화는
이걸 고치면 안 됨) 완전 일치 대신 golden_compare.similar()의 퍼지 유사도로
비교한다(임계값 미달만 실패로 본다 - 사소한 오타/공백 차이는 "예상된 차이").

실행:
    python tests/test_golden_comparison.py
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

PKG_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = PKG_ROOT.parent
TESTS_DIR = Path(__file__).resolve().parent
for p in (PKG_ROOT, REPO_ROOT, TESTS_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from golden_compare import (  # noqa: E402
    SIMILARITY_THRESHOLD,
    extract_golden_excerpts,
    extract_golden_hanja_rows,
    extract_golden_ox,
    extract_golden_questions,
    extract_golden_vocab,
    load_golden,
    similar,
)
from normalize.run import normalize_document  # noqa: E402

_PASS, _FAIL = "[PASS]", "[FAIL]"
_results: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _results.append((ok, label))
    print(f"{_PASS if ok else _FAIL} {label}")


def run() -> bool:
    for doc_id in ("L2-Q2-W08", "L5-Q3-W10", "L9-Q3-W07"):
        print(f"\n--- {doc_id} ---")
        doc = normalize_document(doc_id)
        golden = load_golden(doc_id)

        qa_by_label = {q.order_label: q for q in doc.qa}

        # ---------- 질문 텍스트 ----------
        golden_questions = extract_golden_questions(golden)
        for label, golden_text in golden_questions.items():
            mine = qa_by_label.get(label)
            if mine is None:
                check(False, f"{doc_id} q[{label}]: 정규화 결과에 이 order_label이 없음")
                continue
            score = similar(golden_text, mine.question_text)
            check(score >= SIMILARITY_THRESHOLD,
                  f"{doc_id} q[{label}]: 질문 텍스트 유사도 {score:.2f} (임계값 {SIMILARITY_THRESHOLD})")

        # ---------- 제시문(excerpt) ----------
        # 중학생(L9) 일부 문항은 DB의 excerpt_text 컬럼 자체가 비어있고 제시문이
        # question_text 안에 인용문 형태로 섞여 있다(SPEC §4 "중등 제시문이
        # 질문 안에 섞임") - Stage 1은 독해유형 라벨만 기계적으로 떼어내고
        # 인용문·질문 분리는 LLM 몫으로 남겨둔다(설계상 의도된 경계). 그래서
        # excerpt_text가 비어있어도 golden 제시문이 question_text 안에 들어있는지
        # 확인하고, 있으면 "아직 안 갈라짐(예상됨)"으로 통과시킨다.
        golden_excerpts = extract_golden_excerpts(golden)
        for label, golden_text in golden_excerpts.items():
            if not golden_text.strip():
                continue
            mine = qa_by_label.get(label)
            if mine is None:
                check(False, f"{doc_id} excerpt[{label}]: 정규화 결과에 해당 order_label이 없음")
                continue
            if mine.excerpt_text:
                score = similar(golden_text, mine.excerpt_text)
                check(score >= SIMILARITY_THRESHOLD,
                      f"{doc_id} excerpt[{label}]: 제시문 유사도 {score:.2f} (임계값 {SIMILARITY_THRESHOLD})")
                continue
            fallback_score = similar(golden_text, mine.question_text)
            if fallback_score >= SIMILARITY_THRESHOLD:
                check(True, f"{doc_id} excerpt[{label}]: excerpt_text는 비어있지만 question_text 안에 "
                            f"그대로 있음(아직 안 갈라짐 - LLM 단계 몫, 예상된 상태) 유사도 {fallback_score:.2f}")
            else:
                check(False, f"{doc_id} excerpt[{label}]: 제시문이 question_text 안에도 없음 "
                              f"(유사도 {fallback_score:.2f})")

        # ---------- 어휘(vocab) - 표제어만(뜻풀이는 정규화 단계에서 아직 None이라 비교 제외) ----------
        golden_vocab = extract_golden_vocab(golden)
        my_words = [v.word for v in doc.vocab]
        for gv in golden_vocab:
            check(gv["w"] in my_words, f"{doc_id} vocab: 표제어 {gv['w']!r}가 정규화 결과에 있음")

        # ---------- OX 문장 ----------
        golden_ox = extract_golden_ox(golden)
        my_ox_texts = [o.question for o in doc.ox]
        for i, go in enumerate(golden_ox):
            best = max((similar(go["s"], mt) for mt in my_ox_texts), default=0.0)
            check(best >= SIMILARITY_THRESHOLD,
                  f"{doc_id} ox[{i}]: 최고 유사도 {best:.2f} (임계값 {SIMILARITY_THRESHOLD})")

        # ---------- 한자 뜻풀이(hanja glossary) ----------
        golden_hanja = [row["v"] for row in extract_golden_hanja_rows(golden) if "v" in row]
        my_hanja_terms = [g.term for g in doc.hanja_glossary]
        for term in golden_hanja:
            check(term in my_hanja_terms, f"{doc_id} hanja: 용어 {term!r}가 정규화 결과에 있음")
        golden_gap_count = sum(1 for row in extract_golden_hanja_rows(golden) if row.get("gap"))
        my_gap_count = sum(1 for g in doc.hanja_glossary if g.gap_after)
        if golden_gap_count or my_gap_count:
            check(golden_gap_count == my_gap_count,
                  f"{doc_id} hanja: gap 표시 개수 일치(golden {golden_gap_count} / mine {my_gap_count})")

    n_fail = sum(1 for ok, _ in _results if not ok)
    print(f"\n총 {len(_results)}건 중 실패 {n_fail}건")
    return n_fail == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
