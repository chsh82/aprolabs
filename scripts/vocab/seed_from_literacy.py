"""literacy DB(`data/literacy.db`)의 사자성어 225건을 vocab DB(`data/vocab/idiom.db`)로
1회성 이관.

app/vocab/ 원칙(다른 모듈 import 금지)은 app/vocab/ 안쪽 코드에만 적용된다.
이 스크립트는 scripts/ 아래에 있는 1회성 배치이므로 literacy.db와 idiom.db를
둘 다 열어 다리 역할을 한다 - scripts/literacy/import_sajaseongeo.py 등
기존 배치 스크립트와 같은 위치의 코드다.

사용자 결정(2026-09-02):
- inclusion_evidence를 함께 채운다. source_type='momo'(idiom 스키마의
  CHECK 제약이 허용하는 값), detail에는 literacy terms.license(출처 PDF
  자료명)를 적는다.
- literacy의 level(0~6)은 idiom.level_min에 넣지 않는다 - 3축 점수
  기반 재산정 체계와 척도가 다르므로 섞으면 나중에 재계산이 불가능해진다.
  대신 level_note에 "literacy DB 기준 (level=N)"으로 참고 기록만 남긴다.
  3축 점수(hanja_score/abstraction_score/frequency_score)는 비워 둔다.
- origin_source/origin_story는 검수 전이라 NULL로 둔다(사자성어 유래는
  literacy DB에 아예 없는 정보이기도 하다).
- status는 전부 'draft'로 넣는다(literacy의 review_status와 무관).

literacy 225건 중 6건(정의 없음 - 금란지교/금상첨화/섬섬옥수/양자택일/
위풍당당/유유상종)은 definition도 level도 NULL이다. idiom.hanja는
NOT NULL이지만 이 6건도 origin(한자)은 채워져 있어 문제없다 - meaning만
NULL로 들어간다.

한자 낱글자 분해(hanja/idiom_hanja)와 용례(example)는 이번 이관 대상이
아니다(사용자가 지정한 4개 조건에 없음) - literacy의 term_hanja(156건에만
있음)를 옮기려면 훈음("물 수")을 훈/음으로 쪼개는 별도 작업이 필요해서
범위를 넘어간다. 필요해지면 별도로 요청할 것.

실행:
    python scripts/vocab/seed_from_literacy.py --dry-run   # 저장 없이 결과만 출력
    python scripts/vocab/seed_from_literacy.py              # 실제 적재
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.literacy.db import get_db_path as get_literacy_db_path  # noqa: E402
from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402

SOURCE_TYPE = "momo"

# literacy level(0~6) -> grade_band 참고용 매핑 (idiom.level_min과는 무관 - 척도가 다름)
_LEVEL_TO_GRADE_BAND = {0: "elem", 1: "elem", 2: "elem", 3: "mid", 4: "mid", 5: "high", 6: "high"}


def fetch_literacy_sajaseongeo() -> list[dict]:
    conn = sqlite3.connect(get_literacy_db_path())
    rows = conn.execute(
        """
        SELECT headword, origin, definition, level, license
        FROM terms
        WHERE category = '사자성어'
        ORDER BY id
        """
    ).fetchall()
    conn.close()
    return [
        {"headword": r[0], "hanja": r[1], "meaning": r[2], "level": r[3], "license": r[4]}
        for r in rows
    ]


def build_level_note(level: int | None) -> str:
    if level is None:
        return "literacy DB 기준 (level 없음 - 정의 미확보)"
    return f"literacy DB 기준 (level={level})"


def run(dry_run: bool) -> dict:
    entries = fetch_literacy_sajaseongeo()

    if dry_run:
        return {"entries": entries}

    conn = sqlite3.connect(get_vocab_db_path())
    conn.execute("PRAGMA foreign_keys=ON")

    inserted = 0
    skipped_existing = 0
    evidence_added = 0
    try:
        for e in entries:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO idiom
                    (headword, hanja, meaning, origin_source, origin_story,
                     hanja_score, abstraction_score, frequency_score,
                     level_min, level_note, status)
                VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, 'draft')
                """,
                (e["headword"], e["hanja"], e["meaning"], build_level_note(e["level"])),
            )
            if cur.rowcount == 0:
                skipped_existing += 1
                idiom_id = conn.execute(
                    "SELECT idiom_id FROM idiom WHERE headword = ?", (e["headword"],)
                ).fetchone()[0]
            else:
                inserted += 1
                idiom_id = cur.lastrowid

            already_has_evidence = conn.execute(
                "SELECT 1 FROM inclusion_evidence WHERE idiom_id = ? AND source_type = ?",
                (idiom_id, SOURCE_TYPE),
            ).fetchone()
            if not already_has_evidence:
                conn.execute(
                    """
                    INSERT INTO inclusion_evidence (idiom_id, source_type, detail, grade_band)
                    VALUES (?, ?, ?, ?)
                    """,
                    (idiom_id, SOURCE_TYPE, e["license"], _LEVEL_TO_GRADE_BAND.get(e["level"])),
                )
                evidence_added += 1

        conn.commit()
    finally:
        conn.close()

    return {
        "entries": entries,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "evidence_added": evidence_added,
    }


def print_dry_run_report(result: dict) -> None:
    entries = result["entries"]
    no_meaning = [e for e in entries if not e["meaning"]]
    print(f"=== literacy 사자성어 총 {len(entries)}건 ===")
    print(f"정의 없음(meaning NULL로 들어갈 것) {len(no_meaning)}건: "
          f"{[e['headword'] for e in no_meaning]}")

    print("\n=== 샘플 5건 (실제 이관 시 idiom 테이블에 들어갈 값) ===")
    for e in entries[:5]:
        print(f"- headword={e['headword']} hanja={e['hanja']}")
        print(f"  meaning={e['meaning']}")
        print(f"  level_note={build_level_note(e['level'])}")
        print(f"  inclusion_evidence: source_type={SOURCE_TYPE}, detail={e['license']}, "
              f"grade_band={_LEVEL_TO_GRADE_BAND.get(e['level'])}")

    print("\n--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")


def main() -> int:
    parser = argparse.ArgumentParser(description="literacy DB 사자성어 -> vocab DB 1회성 이관")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 결과만 출력")
    args = parser.parse_args()

    result = run(dry_run=args.dry_run)

    if args.dry_run:
        print_dry_run_report(result)
        return 0

    print(f"idiom 신규 삽입: {result['inserted']}건 (기존 존재로 건너뜀 {result['skipped_existing']}건)")
    print(f"inclusion_evidence 추가: {result['evidence_added']}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())
