"""vocabulary_contents / vocabulary_content_levels 체크섬 스냅샷 (읽기 전용).

3단계 보고서 7-2절이 제안한 방식(정렬된 PK 기준 SHA-256)을 그대로 재사용한다:
`SELECT content_id, lemma, pos, canonical_definition, student_definition
FROM vocabulary_contents ORDER BY content_id`를 정규화해 해시한다.
vocabulary_content_levels도 동일한 방식(content_id, level_version 정렬)으로
추가 해시한다. student_exposure/public_ready 합계·전체 (content_id, value)
목록 해시도 함께 남긴다(마이그레이션 전후 비교용).

사용:
    python3 vq_checksum_core_tables.py --db-path <research db> \\
        --out <json 경로> --label before|after
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path


def sha256_of_rows(rows: list[tuple]) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join("" if v is None else str(v) for v in row).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--label", required=True)
    args = ap.parse_args()

    uri = f"file:{args.db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")

    vc_rows = conn.execute(
        "SELECT content_id, lemma, pos, canonical_definition, student_definition "
        "FROM vocabulary_contents ORDER BY content_id"
    ).fetchall()
    vc_hash = sha256_of_rows(vc_rows)
    vc_count = len(vc_rows)

    vcl_rows = conn.execute(
        "SELECT content_id, vocab_level, target_grade_band, level_status, level_version, boundary_flag, is_active "
        "FROM vocabulary_content_levels ORDER BY content_id, level_version"
    ).fetchall()
    vcl_hash = sha256_of_rows(vcl_rows)
    vcl_count = len(vcl_rows)

    exposure_rows = conn.execute(
        "SELECT content_id, student_exposure, public_ready FROM vocabulary_contents ORDER BY content_id"
    ).fetchall()
    exposure_hash = sha256_of_rows(exposure_rows)
    sum_exposure = conn.execute("SELECT COALESCE(SUM(student_exposure),0) FROM vocabulary_contents").fetchone()[0]
    sum_public_ready = conn.execute("SELECT COALESCE(SUM(public_ready),0) FROM vocabulary_contents").fetchone()[0]

    integrity = conn.execute("PRAGMA integrity_check").fetchall()
    fk_check = conn.execute("PRAGMA foreign_key_check").fetchall()

    tables_present = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    literacy_link_table_exists = "vocabulary_content_literacy_links" in tables_present
    link_count = None
    if literacy_link_table_exists:
        link_count = conn.execute("SELECT COUNT(*) FROM vocabulary_content_literacy_links").fetchone()[0]

    conn.close()

    snapshot = {
        "label": args.label,
        "db_path": str(args.db_path),
        "vocabulary_contents_row_count": vc_count,
        "vocabulary_contents_checksum_sha256": vc_hash,
        "vocabulary_content_levels_row_count": vcl_count,
        "vocabulary_content_levels_checksum_sha256": vcl_hash,
        "exposure_public_ready_checksum_sha256": exposure_hash,
        "sum_student_exposure": sum_exposure,
        "sum_public_ready": sum_public_ready,
        "integrity_check": integrity[0][0] if integrity else None,
        "foreign_key_violations": len(fk_check),
        "vocabulary_content_literacy_links_table_exists": literacy_link_table_exists,
        "vocabulary_content_literacy_links_row_count": link_count,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    print(f"\n저장: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
