"""방식 A(정규화 결과) vs 방식 B(비전 파싱) 자동 비교 - 표본 문서들에 대해 실행.

사용자 지시(2026-09-24) 4번: "제시문·질문 분리, 어순, 표·목록 구조, layout_hint
분포"를 비교해 보고서를 만든다.

사용법:
    python -m vision_parse.compare --doc-ids L5-Q4-W02,L9-Q3-W07 --out report.md
    python -m vision_parse.compare --all-in-db --out report.md   # vision_extract.db에 있는 문서 전부
"""
from __future__ import annotations

import argparse
import io
import re
import sqlite3
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PKG_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, PKG_ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from normalize.db import get_connection as get_momo_connection  # noqa: E402
from normalize.run import normalize_document  # noqa: E402
from vision_parse import db as vdb  # noqa: E402

# normalize/text_repair.py의 것과 동일한 신호 - 한글 사이 공백 3개 이상 연속되면
# 어순/단어가 흐트러진 것으로 의심(자세한 근거는 text_repair.py 주석 참고).
_SUSPICIOUS_RESIDUE = re.compile(r"[가-힣][,.]?\s{3,}[가-힣]")

# "제시문·질문 미분리" 신호 - 인용부호로 시작하는데 excerpt_text가 비어 있고
# question_text 혼자 300자를 넘으면 아직 안 갈라진 것으로 본다(느슨한 휴리스틱 -
# 정밀 판정은 아님, 상대 비교용).
_QUOTE_START_RE = re.compile(r'^[\["“‘]')


def _unsplit_score(excerpt: str | None, question: str | None) -> bool:
    if excerpt:
        return False
    if not question:
        return False
    return len(question) > 300 or bool(_QUOTE_START_RE.match(question.strip()))


def _scramble_count(*texts: str | None) -> int:
    return sum(1 for t in texts if t and _SUSPICIOUS_RESIDUE.search(t))


_PAGE_NUM_RE = re.compile(r"\d+")


def _known_page_range(doc_id: str) -> tuple[int, int] | None:
    """momo_book.db에 실제 기록된 쪽수(excerpt_page/evidence_page)의 최소~최대
    범위 - 원본 DB 원문은 노출하지 않고 정수 범위만 읽는다(읽기 전용)."""
    conn = get_momo_connection()
    try:
        pages = [r[0] for r in conn.execute(
            "SELECT excerpt_page FROM discussion_qa WHERE doc_id=? AND excerpt_page IS NOT NULL "
            "UNION SELECT evidence_page FROM ox_quiz WHERE doc_id=? AND evidence_page IS NOT NULL",
            (doc_id, doc_id),
        )]
    finally:
        conn.close()
    if not pages:
        return None
    return min(pages), max(pages)


def _page_number_plausibility(doc_id: str, b_items) -> dict:
    """B가 뽑은 page_number 중 몇 건이 원본 DB에 기록된 실제 쪽수 범위 밖인지
    - 항목 단위 1:1 대조가 아니라 "이 문서에서 나올 법한 쪽수 범위" 안에 있는지
    보는 느슨한 개연성 체크다(정밀 검증 아님 - 상대 비교/이상치 탐지용)."""
    page_range = _known_page_range(doc_id)
    cited = implausible = unparseable = 0
    for it in b_items:
        pn = it["page_number"]
        if not pn:
            continue
        cited += 1
        nums = [int(x) for x in _PAGE_NUM_RE.findall(str(pn))]
        if not nums:
            unparseable += 1
            continue
        if page_range is not None:
            lo, hi = page_range
            if any(n < max(lo - 5, 1) or n > hi + 5 for n in nums):
                implausible += 1
    return {"cited": cited, "implausible": implausible, "unparseable": unparseable,
            "known_range": page_range}


def compare_doc(doc_id: str) -> dict:
    doc = normalize_document(doc_id)
    a_qa = doc.qa

    conn = vdb.get_connection()
    try:
        b_items = conn.execute(
            "SELECT * FROM vision_item WHERE doc_id = ? ORDER BY page_no, item_index", (doc_id,)
        ).fetchall()
        b_pages = conn.execute(
            "SELECT page_no, elapsed_sec, input_tokens, output_tokens, parse_error "
            "FROM vision_page WHERE doc_id = ? ORDER BY page_no", (doc_id,)
        ).fetchall()
    finally:
        conn.close()

    a_unsplit = sum(1 for q in a_qa if _unsplit_score(q.excerpt_text, q.question_text))
    b_unsplit = sum(1 for it in b_items if _unsplit_score(it["excerpt_text"], it["question_text"]))

    a_scramble = sum(_scramble_count(q.excerpt_text, q.question_text) for q in a_qa)
    b_scramble = sum(_scramble_count(it["excerpt_text"], it["question_text"]) for it in b_items)

    a_vocab_struct = sum(1 for v in doc.vocab if v.definition)  # A가 잡는 구조는 정의뿐
    b_table_or_blanks = sum(1 for it in b_items if it["table_json"] not in (None, "null") or
                             (it["blanks_json"] and it["blanks_json"] != "[]"))

    layout_dist: dict[str, int] = {}
    for it in b_items:
        shape = it["layout_shape"] or "(null)"
        layout_dist[shape] = layout_dist.get(shape, 0) + 1

    parse_errors = sum(1 for p in b_pages if p["parse_error"])
    page_plaus = _page_number_plausibility(doc_id, b_items)

    return {
        "doc_id": doc_id,
        "a_qa_count": len(a_qa),
        "a_unsplit": a_unsplit,
        "a_scramble_hits": a_scramble,
        "a_open_flags": len(doc.all_flags()),
        "b_item_count": len(b_items),
        "b_unsplit": b_unsplit,
        "b_scramble_hits": b_scramble,
        "b_table_or_blanks": b_table_or_blanks,
        "b_pages": len(b_pages),
        "b_parse_errors": parse_errors,
        "b_elapsed_sec": sum(p["elapsed_sec"] for p in b_pages),
        "b_input_tokens": sum(p["input_tokens"] for p in b_pages),
        "b_output_tokens": sum(p["output_tokens"] for p in b_pages),
        "layout_dist": layout_dist,
        "b_page_cited": page_plaus["cited"],
        "b_page_implausible": page_plaus["implausible"],
        "b_page_unparseable": page_plaus["unparseable"],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-ids", help="쉼표로 구분한 doc_id 목록")
    ap.add_argument("--all-in-db", action="store_true", help="vision_extract.db에 있는 문서 전부")
    ap.add_argument("--out", default=None, help="마크다운 리포트 저장 경로(생략하면 표준출력)")
    args = ap.parse_args()

    if args.doc_ids:
        doc_ids = [d.strip() for d in args.doc_ids.split(",") if d.strip()]
    elif args.all_in_db:
        conn = vdb.get_connection()
        doc_ids = [r[0] for r in conn.execute(
            "SELECT DISTINCT doc_id FROM vision_page WHERE parse_error IS NULL ORDER BY doc_id"
        )]
        conn.close()
    else:
        ap.error("--doc-ids 또는 --all-in-db 필요")
        return 2

    results = [compare_doc(d) for d in doc_ids]

    lines = []
    lines.append(f"# 방식 A vs 방식 B 자동 비교 ({len(results)}건 표본)\n")
    lines.append("| doc_id | A 항목 | A 미분리(추정) | A 어순뒤섞임 | A 열린플래그 | "
                  "B 항목 | B 미분리(추정) | B 어순뒤섞임 | B 표/빈칸포착 | B 페이지 | B파싱오류 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    tot = {k: 0 for k in ("a_qa_count", "a_unsplit", "a_scramble_hits", "a_open_flags",
                           "b_item_count", "b_unsplit", "b_scramble_hits", "b_table_or_blanks",
                           "b_pages", "b_parse_errors", "b_page_cited", "b_page_implausible",
                           "b_page_unparseable")}
    layout_total: dict[str, int] = {}
    total_elapsed = total_in = total_out = 0.0
    for r in results:
        lines.append(f"| {r['doc_id']} | {r['a_qa_count']} | {r['a_unsplit']} | {r['a_scramble_hits']} | "
                      f"{r['a_open_flags']} | {r['b_item_count']} | {r['b_unsplit']} | "
                      f"{r['b_scramble_hits']} | {r['b_table_or_blanks']} | {r['b_pages']} | "
                      f"{r['b_parse_errors']} |")
        for k in tot:
            tot[k] += r[k]
        for shape, cnt in r["layout_dist"].items():
            layout_total[shape] = layout_total.get(shape, 0) + cnt
        total_elapsed += r["b_elapsed_sec"]
        total_in += r["b_input_tokens"]
        total_out += r["b_output_tokens"]

    lines.append(f"| **합계** | {tot['a_qa_count']} | {tot['a_unsplit']} | {tot['a_scramble_hits']} | "
                 f"{tot['a_open_flags']} | {tot['b_item_count']} | {tot['b_unsplit']} | "
                 f"{tot['b_scramble_hits']} | {tot['b_table_or_blanks']} | {tot['b_pages']} | "
                 f"{tot['b_parse_errors']} |")

    lines.append("\n## layout_hint.shape 분포 (표본 전체)\n")
    for shape, cnt in sorted(layout_total.items(), key=lambda x: -x[1]):
        lines.append(f"- {shape}: {cnt}건")

    lines.append("\n## 쪽수 오추출 개연성 체크 (느슨한 휴리스틱 - 항목 단위 1:1 대조 아님)\n")
    lines.append(f"- page_number이 채워진 항목: {tot['b_page_cited']}건")
    lines.append(f"- 원본 DB에 기록된 쪽수 범위를 벗어난 것으로 의심: {tot['b_page_implausible']}건")
    lines.append(f"- 숫자를 못 뽑은(형식 이상) 것: {tot['b_page_unparseable']}건")

    cost = (total_in * 3 + total_out * 15) / 1_000_000
    lines.append(f"\n## 비용/시간 실측 ({tot['b_pages']}쪽)\n")
    lines.append(f"- 총 처리 시간: {total_elapsed:.0f}초 ({total_elapsed/60:.1f}분)")
    lines.append(f"- 페이지당 평균: {total_elapsed/max(tot['b_pages'],1):.1f}초, "
                 f"in={total_in/max(tot['b_pages'],1):.0f} out={total_out/max(tot['b_pages'],1):.0f} 토큰")
    lines.append(f"- 추정 비용($3/M in, $15/M out 가정): ${cost:.2f}")
    total_pages_full = 2712
    lines.append(f"\n305건 전체({total_pages_full}쪽) 추정: "
                 f"${cost/tot['b_pages']*total_pages_full:.0f}, "
                 f"{total_elapsed/tot['b_pages']*total_pages_full/3600:.1f}시간(순차)")

    report = "\n".join(lines)
    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"저장됨: {args.out}")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
