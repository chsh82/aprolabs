"""방식 B(비전 파싱) 진행 상황 조회 - 읽기 전용, 실행 중인 run.py를 건드리지 않는다.

사용법:
    python -m vision_parse.status
"""
from __future__ import annotations

import io
import sys
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PKG_ROOT = Path(__file__).resolve().parent.parent
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

from vision_parse import db as vdb  # noqa: E402
from vision_parse.run import _all_doc_ids  # noqa: E402


def main() -> int:
    total_docs = len(_all_doc_ids())
    conn = vdb.get_connection()
    try:
        done_docs = conn.execute("SELECT COUNT(DISTINCT doc_id) FROM vision_page").fetchone()[0]
        total_pages = conn.execute("SELECT COUNT(*) FROM vision_page").fetchone()[0]
        parse_errors = conn.execute("SELECT COUNT(*) FROM vision_page WHERE parse_error IS NOT NULL").fetchone()[0]
        sums = conn.execute(
            "SELECT COALESCE(SUM(elapsed_sec),0), COALESCE(SUM(input_tokens),0), "
            "COALESCE(SUM(output_tokens),0) FROM vision_page"
        ).fetchone()
        total_elapsed, total_in, total_out = sums
        # "현재도 실패 상태인" 문서만 센다 - 같은 doc_id를 나중에 재실행해 성공했으면
        # 예전 실패 기록은 이력일 뿐 현재 상태가 아니다(doc_id별 가장 최근 run_log만 봄).
        still_failing = conn.execute(
            "SELECT doc_id FROM run_log rl WHERE status='failed' AND run_at = "
            "(SELECT MAX(run_at) FROM run_log WHERE doc_id = rl.doc_id)"
        ).fetchall()
        last = conn.execute(
            "SELECT doc_id, status, run_at FROM run_log ORDER BY id DESC LIMIT 5"
        ).fetchall()
    finally:
        conn.close()

    print(f"문서: {done_docs}/{total_docs} 완료 ({done_docs/total_docs*100:.1f}%)")
    print(f"페이지: {total_pages}쪽 처리됨, JSON 파싱 오류 {parse_errors}건")
    print(f"현재 실패 상태인 문서: {len(still_failing)}건" +
          (f" ({', '.join(r['doc_id'] for r in still_failing)})" if still_failing else ""))
    print(f"누적 토큰: in={total_in:,} out={total_out:,}")
    cost = (total_in * 3 + total_out * 15) / 1_000_000
    print(f"누적 추정 비용($3/M in, $15/M out 가정): ${cost:.2f}")

    # 실제 API 처리 시간(elapsed_sec 합)만 기준으로 속도를 잰다 - 세션 중간에
    # 쉬었다 다시 돈 시간(대화 텀 등)까지 들어가는 벽시계 기준은 왜곡된다.
    if total_pages > 0:
        avg_pages_per_doc = total_pages / done_docs
        sec_per_page = total_elapsed / total_pages
        remaining_docs = total_docs - done_docs
        remaining_pages_est = remaining_docs * avg_pages_per_doc
        remaining_sec = remaining_pages_est * sec_per_page
        print(f"쪽당 평균 처리 시간(API 호출 기준): {sec_per_page:.1f}초")
        print(f"남은 {remaining_docs}건(추정 {remaining_pages_est:.0f}쪽) 예상 소요: "
              f"약 {remaining_sec/3600:.1f}시간(순차, 실제 API 처리 시간 기준 - "
              f"이 세션이 계속 실행 중이어야 함)")

    print("\n최근 5건:")
    for r in last:
        print(f"  {r['run_at']}  {r['doc_id']}: {r['status']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
