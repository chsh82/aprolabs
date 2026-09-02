"""idiom.meaning 앞에 그대로 남아 있는 출제 정보("1994학년도 1차 수능 ...")를
떼어내 inclusion_evidence(source_type='exam')로 옮긴다.

fix_duplicates.py로 유유상종을 정리하다가 발견한 것과 같은 패턴이다 -
scripts/literacy/sajaseongeo_parser.py의 날짜 토큰 제거 정규식은
"YYYY학년도 예비 시행"과 "YYYY.수능/YYYY.N" 두 형식만 다루는데, 1994학년도
수능은 그해에만 두 번(1차/2차) 시행돼서 이 정규식이 못 잡는 형식들이
definition 앞에 그대로 남았다.

지금까지 확인된 형식 2가지(docs/vocab/MISSING.md에도 기록):
    A. "YYYY학년도 N차 수능 <정의>" - 10건(유유상종 포함, fix_duplicates.py에서
       처리한 1건 제외 9건은 이 스크립트가 처리)
    B. "YYYY N차 수능 <정의>" (학년도 표기 없음) - 1건(적반하장). 처음엔
       놓쳤다가 published_150.csv 검토 중 발견 - "학년도"가 선택 그룹이라
       A/B 둘 다 이 정규식 하나로 잡는다.

idiom 221건 전체를 연도 숫자 시작/괄호 시작/시험명 포함 등 넓은 기준으로
재검사했고(2026-09-03), 이 둘 외의 형식은 없음을 확인했다. 새 표제어를
literacy DB에서 추가로 이관할 때는 그 넓은 기준으로 다시 스캔해서 세
번째 형식이 없는지 확인할 것.

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

_EXAM_DATE_PREFIX_RE = re.compile(r"^(\d{4}(?:학년도)?\s+\d차\s+수능)\s+(?=\S)")


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
