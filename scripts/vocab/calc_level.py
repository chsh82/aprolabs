"""idiom.hanja_score/abstraction_score/frequency_score/level_min을 계산해 채운다.

계산식은 전부 app/vocab/services/leveling.py에 있다 - 이 스크립트는
DB에서 재료(획수, inclusion_evidence, literacy 레벨)를 모아 그 함수들을
호출하고 결과를 저장하기만 한다. 나중에 한자 급수 데이터가 들어와도 이
스크립트는 그대로 두고 leveling.py만 바꾸면 된다.

우선순위(사용자 결정 2026-09-03):
    1순위 - level_note에 남아 있는 literacy DB 원래 레벨(0~6)을
            level_min_from_literacy_level()로 1~12학년으로 환산해서 쓴다.
    2순위 - literacy 레벨이 없는 항목만 hanja_score/frequency_score로
            추정한다(estimate_level_min). hanja_score는 idiom_hanja가
            4자 다 있어야 계산되므로, literacy 레벨도 없고 idiom_hanja도
            없는 항목은 level_min이 계속 NULL로 남는다.

hanja_score/frequency_score는 level_min 산정 방식과 무관하게, 계산
가능하면 항상 채운다(참고용 - 나중에 재계산의 재료가 된다).
abstraction_score는 산정 방법이 없어 항상 NULL이다.

2순위 경로로 level_min이 정해진 항목만 level_note에 "[획수 기준 임시]"를
덧붙인다(사용자 결정 - 어디까지나 임시값이라는 표시).

실행:
    python scripts/vocab/calc_level.py --dry-run   # 저장 없이 결과만 출력
    python scripts/vocab/calc_level.py              # 실제 저장
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

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402
from app.vocab.services import leveling  # noqa: E402

_LITERACY_LEVEL_RE = re.compile(r"literacy DB 기준 \(level=(\d+)\)")


def fetch_idioms(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT idiom_id, headword, level_note FROM idiom").fetchall()
    idioms = []
    for idiom_id, headword, level_note in rows:
        chars = conn.execute(
            "SELECT position, char FROM idiom_hanja WHERE idiom_id = ? ORDER BY position", (idiom_id,)
        ).fetchall()
        strokes = []
        if len(chars) == 4:
            for _pos, char in chars:
                stroke = conn.execute("SELECT stroke_count FROM hanja WHERE char = ?", (char,)).fetchone()
                if stroke and stroke[0] is not None:
                    strokes.append(stroke[0])
        evidence_types = [
            r[0] for r in conn.execute(
                "SELECT source_type FROM inclusion_evidence WHERE idiom_id = ?", (idiom_id,)
            ).fetchall()
        ]
        idioms.append({
            "idiom_id": idiom_id, "headword": headword, "level_note": level_note,
            "stroke_counts": strokes if len(strokes) == 4 else [],
            "evidence_types": evidence_types,
        })
    return idioms


def compute(idiom: dict) -> dict:
    hanja_score = leveling.compute_hanja_score(idiom["stroke_counts"])
    frequency_score = leveling.compute_frequency_score(idiom["evidence_types"])

    m = _LITERACY_LEVEL_RE.search(idiom["level_note"] or "")
    if m:
        level_min = leveling.level_min_from_literacy_level(int(m.group(1)))
        source = "literacy"
    else:
        level_min = leveling.estimate_level_min(hanja_score, None, frequency_score)
        source = "estimated" if level_min is not None else "none"

    return {
        **idiom,
        "hanja_score": hanja_score,
        "frequency_score": frequency_score,
        "level_min": level_min,
        "source": source,
    }


def print_report(results: list[dict]) -> None:
    by_source = {"literacy": [], "estimated": [], "none": []}
    for r in results:
        by_source[r["source"]].append(r)

    print(f"=== 대상 {len(results)}건 ===")
    print(f"literacy 레벨 우선 적용: {len(by_source['literacy'])}건")
    print(f"획수/빈도 기준 추정: {len(by_source['estimated'])}건")
    print(f"둘 다 불가(level_min NULL 유지): {len(by_source['none'])}건")

    print(f"\n--- 추정 경로 샘플 5건 ---")
    for r in by_source["estimated"][:5]:
        print(f"  {r['headword']}: hanja_score={r['hanja_score']} frequency_score={r['frequency_score']} "
              f"-> level_min={r['level_min']}")

    if by_source["none"]:
        print(f"\n--- level_min NULL로 남는 항목({len(by_source['none'])}건) ---")
        print(f"  {[r['headword'] for r in by_source['none']]}")

    print(f"\n--- literacy 경로 샘플 5건 ---")
    for r in by_source["literacy"][:5]:
        print(f"  {r['headword']}: level_min={r['level_min']} (hanja_score={r['hanja_score']}, "
              f"frequency_score={r['frequency_score']}도 참고용으로 같이 저장)")


def run(dry_run: bool) -> list[dict]:
    conn = sqlite3.connect(get_vocab_db_path())
    try:
        idioms = fetch_idioms(conn)
        results = [compute(i) for i in idioms]

        if dry_run:
            return results

        for r in results:
            level_note = r["level_note"] or ""
            if r["source"] == "estimated":
                level_note = level_note + " / [획수 기준 임시] level_min 추정"
            conn.execute(
                """
                UPDATE idiom
                SET hanja_score = ?, abstraction_score = NULL, frequency_score = ?,
                    level_min = ?, level_note = ?
                WHERE idiom_id = ?
                """,
                (r["hanja_score"], r["frequency_score"], r["level_min"], level_note, r["idiom_id"]),
            )
        conn.commit()
        return results
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="idiom 3축 점수 + level_min 계산")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 결과만 출력")
    args = parser.parse_args()

    results = run(dry_run=args.dry_run)
    print_report(results)

    if args.dry_run:
        print("\n--dry-run - DB에 저장하지 않음.")
    else:
        print(f"\n완료: {len(results)}건 갱신")
    return 0


if __name__ == "__main__":
    sys.exit(main())
