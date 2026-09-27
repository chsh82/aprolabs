#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase28 항목1: phase27 최종 산출물(문항 JSON·결과표·생성/검증/병합 스크립트)의
체크섬과 item_id 40개를 고정하고, 서버의 실제 L6 콘텐츠와 다시 대조한다.
읽기 전용(SELECT만) - DB에 아무것도 쓰지 않는다.

확인 항목:
  1. phase27 산출물 파일들의 SHA-256이 이 스크립트에 박아 둔 고정값과 정확히
     일치하는지(누가 파일을 바꿨다면 여기서 걸림)
  2. 문항 JSON의 item_id 40개가 이 스크립트에 박아 둔 고정 목록과 정확히 같은지
  3. 선정된 20개 content_id가 서버 DB에 전부 존재·is_active=1이고
     level_status='REVIEW_BOUNDARY', student_exposure=0, public_ready=0인지
  4. 각 문항의 explanation/보기에 들어있는 정의 문자열이 "지금" DB의
     student_definition과 정확히 같은지 - 문항 생성 시점(phase27) 이후 콘텐츠가
     바뀌었으면 여기서 걸려서 중단해야 한다

하나라도 실패하면 이후 적재 단계(phase28_apply_l6_pilot_quiz_items.py)를
진행하지 않는다.

사용:
    python3 scripts/vocab/phase28_freeze_check.py --db-path <research db, 읽기 전용>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

FROZEN_CHECKSUMS = {
    "data/import/schema_reading_phase27_l6_pilot_items_20260927.json":
        "24d1d01a7484fef6d1735cd64b7797cf2d71aaeea52adddc8806d1ae0f767328",
    "data/import/schema_reading_phase27_l6_pilot_result_20260927.csv":
        "a9896df12ef3d4f7a0e659558c49483f2aa528d8bc3a802b75bd7792b95310f1",
    "data/import/schema_reading_phase27_l6_pilot_selected_20260927.json":
        "8139bda2d83d37e0a008c28fac3bd1d5f51ff4791cdd0c84766316f23333734e",
    "data/import/schema_reading_phase27_l6_pilot_auto_verify_20260927.json":
        "784420de0cf02ca64da63d79d02bcdcd646ccbeb1c6c013c45d1dbd819a8deae",
    "scripts/vocab/phase27_select_l6_pilot_content.py":
        "8093d1293aa0d0a649b495e22ed43953ac014470d0098013af4a8612e950033a",
    "scripts/vocab/phase27_generate_l6_pilot_items.py":
        "c0ba2aa7783d7996e33cdda497c1ac2a3e9391343c2bfa6075f82ae2cb3ff163",
    "scripts/vocab/phase27_verify_l6_pilot_items.py":
        "1cc708fe50fccfd5b225ac599a1c51ae5a565c148e49dfafee43d4b6f6d29d37",
    "scripts/vocab/phase27_finalize_l6_pilot_results.py":
        "e6457028f5e513ce9cb07020e976e6d7e429e86eee08c7fe2fb2f265f9daa722",
}

FROZEN_ITEM_IDS = sorted(
    f"MF_{prefix}_SC_SRL6PILOT_20260927_L6_{seq:03d}"
    for seq in range(1, 21)
    for prefix in ("A", "C")
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-path", type=Path, required=True)
    args = ap.parse_args()

    ok = True

    print("=== 1. phase27 산출물 체크섬 고정 확인 ===")
    for rel, expected in FROZEN_CHECKSUMS.items():
        path = REPO_ROOT / rel
        if not path.exists():
            print(f"[FAIL] {rel}: 파일 없음")
            ok = False
            continue
        actual = sha256_file(path)
        match = actual == expected
        print(f"{'[PASS]' if match else '[FAIL]'} {rel}: {actual}" + ("" if match else f" (기대값 {expected})"))
        ok = ok and match

    items_path = REPO_ROOT / "data/import/schema_reading_phase27_l6_pilot_items_20260927.json"
    items = json.load(open(items_path, encoding="utf-8"))

    print("\n=== 2. item_id 40개 고정 목록 일치 확인 ===")
    actual_ids = sorted(it["item_id"] for it in items)
    ids_match = actual_ids == FROZEN_ITEM_IDS
    print(f"{'[PASS]' if ids_match else '[FAIL]'} item_id 40개 == 고정 목록")
    if not ids_match:
        print("  차이:", set(actual_ids) ^ set(FROZEN_ITEM_IDS))
    ok = ok and ids_match

    conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")

    content_ids = sorted({it["source_content_id"] for it in items})
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""
        SELECT c.content_id, c.is_active, c.student_exposure, c.public_ready, c.student_definition,
               l.level_status
        FROM vocabulary_contents c
        JOIN vocabulary_content_levels l ON c.content_id = l.content_id
        WHERE c.content_id IN ({placeholders})
        """,
        content_ids,
    ).fetchall()
    live = {r[0]: {"is_active": r[1], "student_exposure": r[2], "public_ready": r[3],
                   "student_definition": r[4], "level_status": r[5]} for r in rows}

    print(f"\n=== 3. 선정 20개 content_id 서버 재대조 ({len(content_ids)}개) ===")
    missing = [c for c in content_ids if c not in live]
    print(f"{'[PASS]' if not missing else '[FAIL]'} 전부 존재: 누락 {len(missing)}건 {missing}")
    ok = ok and not missing

    ineligible = [
        c for c, v in live.items()
        if v["is_active"] != 1 or v["student_exposure"] != 0 or v["public_ready"] != 0
        or v["level_status"] != "REVIEW_BOUNDARY"
    ]
    print(f"{'[PASS]' if not ineligible else '[FAIL]'} is_active=1/exposure=0/public=0/REVIEW_BOUNDARY 전부 충족: "
          f"위반 {len(ineligible)}건 {ineligible}")
    ok = ok and not ineligible

    print("\n=== 4. 문항 생성 당시 정의와 지금 DB 정의 일치(드리프트 검사) ===")
    drift = []
    for it in items:
        cur = live.get(it["source_content_id"])
        if cur is None:
            drift.append((it["item_id"], "content_id 없음"))
            continue
        if cur["student_definition"] not in it["explanation"]:
            drift.append((it["item_id"], f"explanation 불일치: DB={cur['student_definition']!r}"))
        if cur["student_definition"] not in it["options_json"]:
            drift.append((it["item_id"], f"options_json에 현재 정의 없음"))
    print(f"{'[PASS]' if not drift else '[FAIL]'} 드리프트 0건 (실제 {len(drift)}건)")
    for d in drift:
        print("  ", d)
    ok = ok and not drift

    conn.close()

    print(f"\n{'[PASS]' if ok else '[FAIL]'} 항목1 전체 종합 - "
          f"{'적재 단계로 진행 가능' if ok else '중단, 적재 단계를 실행하지 말 것'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
