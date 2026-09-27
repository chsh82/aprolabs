#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""momolib 1차 이식용 export 패키지 생성 - 읽기 전용(SELECT만), 연구 DB에
어떤 것도 쓰지 않는다.

이 스크립트는 실행 시점의 연구 DB 전체 기준선(vocabulary_contents 5,950건,
vocabulary_multiformat_items 1,369건, 그중 파일럿 40+40건)이 정확히 그
수치와 일치하는지부터 검증한다 - 하나라도 다르면 즉시 실패한다(드리프트를
조용히 넘기지 않음, "건수·해시를 고정"하라는 지시를 스크립트 단으로 강제).

이식 실제 대상(1차 범위=관리자 전용 미리보기+파일럿)은 전체 5,950건이
아니라 L4~L6 신규 배치 227건(스키마리딩 관련 5개 source_version)과 두
파일럿(각 40건, data/vocab/pilot_*.json에 이미 전체 payload가 있어 그대로
재사용)이다 - 나머지 2.1.29 일반 풀 5,723건은 schema_reading 프로젝트와
무관해 이식 대상에서 제외한다. 이 스코프 판단은 export_manifest.json의
"baseline_full_db" 절에 전체 5,950/1,369 수치를 그대로 기록해 감사
가능하게 하고, "exported_payload" 절에는 실제로 내보낸 227+40+40건만
분리해서 남긴다.

사용:
    python3 scripts/vocab/export_l6_content_for_momolib.py \\
        --ssh-host aprolabs \\
        --remote-db /home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db \\
        --out-dir data/export/vocab_quiz_momolib_export_v1
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

EXPECTED_TOTAL_CONTENTS = 5950
EXPECTED_TOTAL_ITEMS = 1369
EXPECTED_PILOT_L4L5 = 40
EXPECTED_PILOT_L6 = 40

L4_L6_SOURCE_VERSIONS = [
    "schema_reading_literacy_l4_manual_v1",
    "schema_reading_literacy_l5_manual_v1",
    "schema_reading_literacy_l6_manual_v1",
    "schema_reading_literacy_l6_manual_v2",
    "schema_reading_l6_evidence_grounded_v1",
]
PILOT_L4L5_SOURCE_VERSION = "schema_reading_l4l5_pilot_dryrun_v1"
PILOT_L6_SOURCE_VERSION = "schema_reading_l6_pilot_dryrun_v1"


def run_sqlite(ssh_host: str, remote_db: str, sql: str) -> str:
    cmd = ["ssh", ssh_host, f"sqlite3 -json {remote_db} \"{sql}\""]
    out = subprocess.run(cmd, capture_output=True, timeout=30, check=True)
    return out.stdout.decode("utf-8")


def canonical_hash(rows: list[dict]) -> str:
    """행 순서에 의존하지 않는 안정적 해시 - content_id/item_id로 정렬 후
    JSON 직렬화해서 sha256. 재실행해도 항상 같은 해시가 나와야 "재현 가능"함을
    보장한다."""
    key = "content_id" if rows and "content_id" in rows[0] else "item_id"
    ordered = sorted(rows, key=lambda r: r[key])
    blob = json.dumps(ordered, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ssh-host", default="aprolabs")
    ap.add_argument("--remote-db", default="/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--pilot-l4l5-manifest", type=Path, default=Path("data/vocab/pilot_l4l5_manifest_v1.json"))
    ap.add_argument("--pilot-l6-manifest", type=Path, default=Path("data/vocab/pilot_l6_manifest_v1.json"))
    args = ap.parse_args()

    total_contents = json.loads(run_sqlite(args.ssh_host, args.remote_db, "SELECT COUNT(*) AS n FROM vocabulary_contents"))[0]["n"]
    total_items = json.loads(run_sqlite(args.ssh_host, args.remote_db, "SELECT COUNT(*) AS n FROM vocabulary_multiformat_items"))[0]["n"]
    print(f"기준선 확인: vocabulary_contents={total_contents}, vocabulary_multiformat_items={total_items}")
    if total_contents != EXPECTED_TOTAL_CONTENTS:
        raise SystemExit(f"FAIL: vocabulary_contents={total_contents} != 고정 기준선 {EXPECTED_TOTAL_CONTENTS} - 연구 DB가 이 스크립트 작성 시점과 달라졌습니다. 임의로 진행하지 않고 중단합니다.")
    if total_items != EXPECTED_TOTAL_ITEMS:
        raise SystemExit(f"FAIL: vocabulary_multiformat_items={total_items} != 고정 기준선 {EXPECTED_TOTAL_ITEMS} - 중단.")

    pilot_l4l5_n = json.loads(run_sqlite(
        args.ssh_host, args.remote_db,
        f"SELECT COUNT(*) AS n FROM vocabulary_multiformat_items WHERE source_version='{PILOT_L4L5_SOURCE_VERSION}'",
    ))[0]["n"]
    pilot_l6_n = json.loads(run_sqlite(
        args.ssh_host, args.remote_db,
        f"SELECT COUNT(*) AS n FROM vocabulary_multiformat_items WHERE source_version='{PILOT_L6_SOURCE_VERSION}'",
    ))[0]["n"]
    print(f"파일럿 확인: L4L5={pilot_l4l5_n}, L6={pilot_l6_n}")
    if pilot_l4l5_n != EXPECTED_PILOT_L4L5 or pilot_l6_n != EXPECTED_PILOT_L6:
        raise SystemExit(f"FAIL: 파일럿 건수가 고정 기준선(40+40)과 다릅니다(실제 {pilot_l4l5_n}+{pilot_l6_n}) - 중단.")

    versions_sql = "','".join(L4_L6_SOURCE_VERSIONS)
    contents = json.loads(run_sqlite(
        args.ssh_host, args.remote_db,
        f"SELECT content_id, sense_id, lexical_entry_id, batch_id, lemma, pos, canonical_definition, "
        f"student_definition, example_sentence, example_target_form, generation_method, qa_method, "
        f"generation_status, student_exposure, public_ready, hold_reason, merge_source, source_version, "
        f"is_active FROM vocabulary_contents WHERE source_version IN ('{versions_sql}') ORDER BY content_id",
    ))
    levels = json.loads(run_sqlite(
        args.ssh_host, args.remote_db,
        f"SELECT vcl.content_id, vcl.vocab_level, vcl.target_grade_band, vcl.level_score, vcl.level_confidence, "
        f"vcl.level_status, vcl.boundary_flag, vcl.level_source, vcl.level_version, vcl.level_reason_json, "
        f"vcl.is_active FROM vocabulary_content_levels vcl "
        f"JOIN vocabulary_contents vc ON vc.content_id = vcl.content_id "
        f"WHERE vc.source_version IN ('{versions_sql}') ORDER BY vcl.content_id",
    ))
    print(f"L4~L6 신규 콘텐츠 227건 대상: contents={len(contents)}, levels={len(levels)}")
    if len(contents) != 227:
        raise SystemExit(f"FAIL: L4~L6 신규 콘텐츠가 227건이 아님(실제 {len(contents)}) - 중단.")

    if not args.pilot_l4l5_manifest.exists() or not args.pilot_l6_manifest.exists():
        raise SystemExit("FAIL: 파일럿 매니페스트 파일(data/vocab/pilot_*.json)이 없습니다 - 중단.")
    pilot_l4l5_rows = json.loads(args.pilot_l4l5_manifest.read_text(encoding="utf-8"))
    pilot_l6_rows = json.loads(args.pilot_l6_manifest.read_text(encoding="utf-8"))
    if len(pilot_l4l5_rows) != 40 or len(pilot_l6_rows) != 40:
        raise SystemExit("FAIL: 파일럿 매니페스트 파일 건수가 40건이 아님 - 중단.")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "contents.json").write_text(json.dumps(contents, ensure_ascii=False, indent=1), encoding="utf-8")
    (args.out_dir / "content_levels.json").write_text(json.dumps(levels, ensure_ascii=False, indent=1), encoding="utf-8")
    (args.out_dir / "pilot_l4l5_items.json").write_text(json.dumps(pilot_l4l5_rows, ensure_ascii=False, indent=1), encoding="utf-8")
    (args.out_dir / "pilot_l6_items.json").write_text(json.dumps(pilot_l6_rows, ensure_ascii=False, indent=1), encoding="utf-8")

    manifest = {
        "export_tool": "scripts/vocab/export_l6_content_for_momolib.py",
        "baseline_full_db": {
            "vocabulary_contents": total_contents,
            "vocabulary_multiformat_items": total_items,
            "pilot_l4l5_items_in_db": pilot_l4l5_n,
            "pilot_l6_items_in_db": pilot_l6_n,
            "note": "연구 DB 전체 기준선(고정 검증값) - 이식 대상 전체가 아님, 아래 exported_payload만 실제 이식 대상",
        },
        "exported_payload": {
            "contents_count": len(contents),
            "contents_sha256": canonical_hash(contents),
            "content_levels_count": len(levels),
            "content_levels_sha256": canonical_hash(levels),
            "pilot_l4l5_items_count": len(pilot_l4l5_rows),
            "pilot_l4l5_items_sha256": canonical_hash(pilot_l4l5_rows),
            "pilot_l6_items_count": len(pilot_l6_rows),
            "pilot_l6_items_sha256": canonical_hash(pilot_l6_rows),
            "source_versions_included": L4_L6_SOURCE_VERSIONS,
        },
    }
    (args.out_dir / "export_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"\n저장: {args.out_dir}/ (contents.json, content_levels.json, pilot_l4l5_items.json, pilot_l6_items.json, export_manifest.json)")
    print(json.dumps(manifest, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
