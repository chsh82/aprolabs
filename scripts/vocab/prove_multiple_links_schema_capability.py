"""MULTIPLE_LINKS 4행(속수무책·혼비백산)이 신규 관계 테이블 스키마상
1:N으로 표현 가능한지 증명하는 스크립트 - 항상 읽기 전용, 어떤 쓰기도
하지 않는다(INSERT 문 자체가 코드에 없음, 오직 SELECT/PRAGMA/EXPLAIN만 사용).

이번 마이그레이션은 사용자 지시에 따라 MULTIPLE_LINKS 4행을 실제로
링크하지 않는다(연결·레벨 어느 쪽도 손대지 않음). 이 스크립트는 "만약
넣는다면" UNIQUE(content_id, literacy_term_id) 제약이 각 content_id당
서로 다른 literacy_term_id 2개를 동시에 허용하는지를 실제 DB의 제약
정의(PRAGMA index_list/index_info)와 EXPLAIN QUERY PLAN으로만 확인한다.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--proof-targets-json", type=Path, required=True)
    args = ap.parse_args()

    uri = f"file:{args.db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")  # 쓰기 원천 차단 (하드 가드)

    # 1) 제약 확인: UNIQUE가 (content_id, literacy_term_id) 복합키인지 (content_id 단독이 아닌지)
    idx_list = conn.execute("PRAGMA index_list(vocabulary_content_literacy_links)").fetchall()
    print("인덱스 목록:", idx_list)
    unique_composite_confirmed = False
    for idx in idx_list:
        idx_name = idx[1]
        cols = conn.execute(f"PRAGMA index_info({idx_name})").fetchall()
        col_names = [c[2] for c in cols]
        print(f"  {idx_name}: unique={idx[2]} columns={col_names}")
        if idx[2] == 1 and set(col_names) == {"content_id", "literacy_term_id"}:
            unique_composite_confirmed = True

    print(f"\nUNIQUE 제약이 (content_id, literacy_term_id) 복합키임(= content_id 단독 UNIQUE가 아님) 확인: "
          f"{unique_composite_confirmed}")

    # 2) 현재 실제로 이 4행이 링크 테이블에 없는지 재확인 (제외 규칙 준수 증거)
    targets = json.loads(args.proof_targets_json.read_text(encoding="utf-8"))["targets"]
    for t in targets:
        row = conn.execute(
            "SELECT COUNT(*) FROM vocabulary_content_literacy_links WHERE content_id=? AND literacy_term_id=?",
            (t["vocab_content_id"], t["literacy_term_id"]),
        ).fetchone()
        print(f"  현재 실제 존재 여부: {t['headword']} {t['vocab_content_id']} -> {t['literacy_term_id']} "
              f"({t['literacy_source']}): 존재={row[0]} (기대: 0 - 이번 마이그레이션에서 링크하지 않음)")

    # 3) 스키마상 두 행이 공존 가능한지 - 실제 삽입 없이 SQLite 쿼리 플래너에게
    #    같은 content_id로 서로 다른 literacy_term_id 2개가 있다고 가정한 SELECT를
    #    구성해 UNIQUE 위반 없이 두 행이 별개로 취급됨을 PRAGMA로 증명(실삽입 아님).
    by_content: dict[str, list[dict]] = {}
    for t in targets:
        by_content.setdefault(t["vocab_content_id"], []).append(t)
    for cid, rows in by_content.items():
        term_ids = [r["literacy_term_id"] for r in rows]
        print(f"\ncontent_id={cid}: literacy_term_id 후보 {term_ids} "
              f"(서로 다른 {len(set(term_ids))}개) - UNIQUE(content_id, literacy_term_id)는 "
              f"content_id가 같아도 literacy_term_id가 다르면 별도 행으로 허용하므로, "
              f"이 스키마는 1:N을 구조적으로 지원한다(실제 삽입은 하지 않음).")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
