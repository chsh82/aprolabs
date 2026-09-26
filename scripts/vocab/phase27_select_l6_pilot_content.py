#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase27 항목1: L6 비공개 V 콘텐츠 69건(phase24 배치1 V37 + phase26 배치2 V32)
중 최대 20건을 선정한다. 읽기 전용(SELECT만) - DB에 아무것도 쓰지 않는다.

제외 대상: 매커니즘/이성/신장 HOLD 3건(애초에 DB에 적재된 적이 없어 이 쿼리
결과에 나타나지 않음), S(교과개념어) 13건(level_reason_json.is_S_subject_concept
로 필터링).

선정 방식: literacy_term_id 오름차순으로 처음 20건을 결정론적으로 선택한다
(임의 판단이 재현되지 않는 것을 방지). 각 후보의 caution을 함께 저장해 다음
단계(문항 생성)가 근접 유의어 정보를 참조할 수 있게 한다.

사용:
    python3 scripts/vocab/phase27_select_l6_pilot_content.py \\
        --db-path <research db 사본 또는 원본, 읽기 전용으로만 연다> \\
        --out data/import/schema_reading_phase27_l6_pilot_selected_20260927.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

L6_SOURCE_VERSIONS = ("schema_reading_literacy_l6_manual_v1", "schema_reading_literacy_l6_manual_v2")
MAX_SELECT = 20


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    rows = conn.execute(
        """
        SELECT c.content_id, c.lemma, c.pos, c.source_version, l.level_reason_json
        FROM vocabulary_contents c
        JOIN vocabulary_content_levels l ON c.content_id = l.content_id
        WHERE c.source_version IN (?, ?)
        ORDER BY c.content_id
        """,
        L6_SOURCE_VERSIONS,
    ).fetchall()
    conn.close()

    v_rows = []
    s_count = 0
    for content_id, lemma, pos, source_version, level_reason_raw in rows:
        lrj = json.loads(level_reason_raw)
        if lrj.get("is_S_subject_concept"):
            s_count += 1
            continue
        v_rows.append({
            "content_id": content_id,
            "lemma": lemma,
            "pos": pos,
            "source_version": source_version,
            "literacy_term_id": lrj.get("literacy_term_id"),
            "caution": lrj.get("caution", ""),
        })

    print(f"L6 전체: {len(rows)}건 (V={len(v_rows)}, S={s_count})")
    assert len(v_rows) == 69, f"기대와 다른 V 행 수: {len(v_rows)} (기대값 69)"
    assert s_count == 13, f"기대와 다른 S 행 수: {s_count} (기대값 13)"

    v_rows_sorted = sorted(v_rows, key=lambda r: int(r["literacy_term_id"]))
    selected = v_rows_sorted[:MAX_SELECT]
    print(f"선정: {len(selected)}건 (literacy_term_id 오름차순 상위 {MAX_SELECT}건)")
    for r in selected:
        print(f"  {r['literacy_term_id']} {r['lemma']} ({r['content_id']})" +
              (f" - caution 있음" if r["caution"] else ""))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(selected, f, ensure_ascii=False, indent=1)
    print(f"\n저장: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
