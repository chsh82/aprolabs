"""idiom.hanja를 전부 NFKC 정규화해서 저장한다.

fix_duplicates.py 작업 중 발견한 것 - PDF 추출 과정에서 일부 한자가
CJK Compatibility Ideographs(U+F900대)로 들어왔다. 육안으로는 표준
한자와 똑같이 보이지만 코드포인트가 달라서 문자열 비교・글자별 분해가
전부 어긋난다. `unicodedata.normalize('NFKC', ...)`는 이런 호환용
코드포인트를 표준 코드포인트로 되돌려준다(같은 글자라는 걸 확인하는
것이지 다른 글자로 갈아치우는 게 아니다).

사용자 결정(2026-09-02): idiom.hanja를 기준으로 삼는다 - 앞으로
idiom_hanja.char/hanja.char도 전부 이 정규화된 idiom.hanja에서
가져온다(split_hanja.py). literacy 쪽 글자는 참고만 한다.

실행:
    python scripts/vocab/normalize_hanja_nfkc.py --dry-run   # 저장 없이 바뀔 행만 출력
    python scripts/vocab/normalize_hanja_nfkc.py              # 실제 갱신
"""
from __future__ import annotations

import argparse
import io
import sqlite3
import sys
import unicodedata
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402


def find_targets(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT idiom_id, headword, hanja FROM idiom").fetchall()
    targets = []
    for idiom_id, headword, hanja in rows:
        normalized = unicodedata.normalize("NFKC", hanja)
        if normalized != hanja:
            targets.append({"idiom_id": idiom_id, "headword": headword, "before": hanja, "after": normalized})
    return targets


def print_report(targets: list[dict]) -> None:
    print(f"=== NFKC 정규화로 바뀔 idiom.hanja: {len(targets)}건 ===\n")
    for t in targets:
        before_cp = [hex(ord(c)) for c in t["before"]]
        after_cp = [hex(ord(c)) for c in t["after"]]
        print(f"- {t['headword']}: {t['before']!r} {before_cp} -> {t['after']!r} {after_cp}")


def run(dry_run: bool) -> list[dict]:
    conn = sqlite3.connect(get_vocab_db_path())
    try:
        targets = find_targets(conn)
        if dry_run:
            return targets
        for t in targets:
            conn.execute("UPDATE idiom SET hanja = ? WHERE idiom_id = ?", (t["after"], t["idiom_id"]))
        conn.commit()
        return targets
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="idiom.hanja NFKC 정규화")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 대상만 출력")
    args = parser.parse_args()

    targets = run(dry_run=args.dry_run)
    print_report(targets)

    if args.dry_run:
        print("\n--dry-run - DB에 저장하지 않음.")
    else:
        print(f"\n완료: {len(targets)}건 정규화")
    return 0


if __name__ == "__main__":
    sys.exit(main())
