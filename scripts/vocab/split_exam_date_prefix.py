"""idiom.meaning 앞에 그대로 남아 있는 출제 정보("1994학년도 1차 수능 ...")를
떼어내 inclusion_evidence(source_type='exam')로 옮긴다.

fix_duplicates.py로 유유상종을 정리하다가 발견한 것과 같은 패턴이다 -
scripts/literacy/sajaseongeo_parser.py의 날짜 토큰 제거 정규식은
"YYYY학년도 예비 시행"과 "YYYY.수능/YYYY.N" 두 형식만 다루는데, 1994학년도
수능은 그해에만 두 번(1차/2차) 시행돼서 "YYYY학년도 N차 수능"이라는
세 번째 형식이 됐다 - 이 형식은 정규식이 못 잡아서 definition 앞에
그대로 남았다. 전수 조사 결과 전부 1994학년도 사례였다(9건, 유유상종
포함 10건 중 유유상종은 fix_duplicates.py에서 이미 처리함).

실행:
    python scripts/vocab/split_exam_date_prefix.py --dry-run   # 저장 없이 대상만 출력
    python scripts/vocab/split_exam_date_prefix.py              # 실제 분리
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

_EXAM_DATE_PREFIX_RE = re.compile(r"^(\d{4}학년도\s+\d차\s+수능)\s+(?=\S)")


def strip_exam_date_prefix(meaning: str | None) -> tuple[str | None, str | None]:
    if not meaning:
        return meaning, None
    m = _EXAM_DATE_PREFIX_RE.match(meaning)
    if not m:
        return meaning, None
    return meaning[m.end():], m.group(1)


def find_targets(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT idiom_id, headword, meaning FROM idiom WHERE meaning IS NOT NULL").fetchall()
    targets = []
    for idiom_id, headword, meaning in rows:
        cleaned, detail = strip_exam_date_prefix(meaning)
        if detail:
            targets.append({
                "idiom_id": idiom_id, "headword": headword,
                "meaning": meaning, "cleaned_meaning": cleaned, "detail": detail,
            })
    return targets


def print_report(targets: list[dict]) -> None:
    print(f"=== 출제정보 접두어가 붙은 meaning: {len(targets)}건 ===\n")
    for t in targets:
        print(f"- idiom_id={t['idiom_id']} {t['headword']}")
        print(f"    before: {t['meaning']!r}")
        print(f"    after : {t['cleaned_meaning']!r}")
        print(f"    inclusion_evidence 추가: source_type='exam', detail={t['detail']!r}")
        print()


def run(dry_run: bool) -> list[dict]:
    conn = sqlite3.connect(get_vocab_db_path())
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        targets = find_targets(conn)
        if dry_run:
            return targets

        for t in targets:
            conn.execute(
                "UPDATE idiom SET meaning = ? WHERE idiom_id = ?",
                (t["cleaned_meaning"], t["idiom_id"]),
            )
            conn.execute(
                "INSERT INTO inclusion_evidence (idiom_id, source_type, detail) VALUES (?, 'exam', ?)",
                (t["idiom_id"], t["detail"]),
            )
        conn.commit()
        return targets
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="idiom.meaning의 출제정보 접두어 분리")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 대상만 출력")
    args = parser.parse_args()

    targets = run(dry_run=args.dry_run)
    print_report(targets)

    if args.dry_run:
        print("--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
    else:
        print(f"완료: {len(targets)}건 분리")
    return 0


if __name__ == "__main__":
    sys.exit(main())
