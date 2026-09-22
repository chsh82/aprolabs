"""vocabulary_items.stem 중 표제어에 작은따옴표가 빠진 문구를 보정한다.

배경: TYPE_A_MEANING_CHOICE 5,723건 전수 조사 결과, 표제어를 감싸는 방식이
두 가지 섞여 있었다 - '가루약'의 뜻으로 가장 알맞은 것은?(2,479건, 따옴표 있음)
vs 다음 문장에서 하물며의 뜻으로 알맞은 것은 무엇인가요?(3,244건, 따옴표 없음).
따옴표 없는 3,244건은 전수 확인 결과 "다음 문장에서 {lemma}의 뜻으로 알맞은
것은 무엇인가요?" 템플릿 단 하나로 100% 일치한다(lemma만 다름, 다른 변형 0건) -
문구 자체를 바꾸지 않고 lemma만 작은따옴표(’ ‘)로 감싸는 최소 수정만 한다.

정확히 이 템플릿과 일치하는 stem만 대상으로 한다(부분 포함이 아니라 완전
일치) - 다른 문구/이미 따옴표가 있는 행이 실수로 섞이지 않도록.

기본은 dry-run(변경될 행을 전부 나열만 하고 DB에 손대지 않음). --apply를
줘야 실제 UPDATE하며, 적용 전 sqlite3.Connection.backup() API로 스냅샷을 뜬다.
운영 서버에도 같은 안전장치(APP_ENV 가드, --database 경로 대조)를 적용한다.

실행 예:
    APP_ENV=local_rnd VOCABULARY_QUIZ_DB_PATH=data/vocab/vocabulary_quiz_rnd.db \\
    python scripts/vocab/fix_meaning_choice_missing_quotes.py --database data/vocab/vocabulary_quiz_rnd.db
    (기본 dry-run, --apply 붙이면 실제 적용)
"""
from __future__ import annotations

import argparse
import io
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
if sys.platform == "win32" and (sys.stderr.encoding or "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocabulary_quiz.db import get_db_path  # noqa: E402
from app.vocab.db import get_db_path as get_idiom_db_path  # noqa: E402

TEMPLATE = "다음 문장에서 {lemma}의 뜻으로 알맞은 것은 무엇인가요?"
LEFT_QUOTE, RIGHT_QUOTE = "‘", "’"  # ' '
_ALLOWED_APPLY_ENVS = ("local_rnd", "research")


def assert_apply_allowed() -> None:
    app_env = os.environ.get("APP_ENV", "")
    if app_env not in _ALLOWED_APPLY_ENVS:
        print(f"거부: APP_ENV={app_env!r} 에서는 --apply를 허용하지 않습니다 "
              f"({', '.join(_ALLOWED_APPLY_ENVS)}만 허용).", file=sys.stderr)
        sys.exit(2)
    print(f"환경 확인: APP_ENV={app_env!r} (실제 적용 허용)")


def assert_local_database_path(db_path: Path) -> None:
    resolved = db_path.resolve()
    idiom_path = get_idiom_db_path().resolve()
    if resolved == idiom_path or resolved.name == "idiom.db":
        print(f"거부: --database가 기존 사자성어 DB(idiom.db)를 가리킵니다: {resolved}", file=sys.stderr)
        sys.exit(2)
    expected = get_db_path().resolve()
    if resolved != expected:
        print(f"거부: --database({resolved})가 VOCABULARY_QUIZ_DB_PATH로 지정된 공식 경로"
              f"({expected})와 다릅니다.", file=sys.stderr)
        sys.exit(2)


def find_targets(conn: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
    """반환: [(item_id, lemma, old_stem, new_stem), ...] - TEMPLATE과 완전히 일치하는 행만."""
    rows = conn.execute("SELECT item_id, lemma, stem FROM vocabulary_items").fetchall()
    targets = []
    for item_id, lemma, stem in rows:
        if not lemma or not stem:
            continue
        if stem != TEMPLATE.format(lemma=lemma):
            continue
        new_stem = TEMPLATE.format(lemma=f"{LEFT_QUOTE}{lemma}{RIGHT_QUOTE}")
        targets.append((item_id, lemma, stem, new_stem))
    return targets


def run(database: Path, apply: bool) -> int:
    print(f"=== stem 따옴표 보정 ({'실제 적용' if apply else 'DRY-RUN'}) ===")
    assert_local_database_path(database)
    if apply:
        assert_apply_allowed()

    conn = sqlite3.connect(database)
    total_before = conn.execute("SELECT COUNT(*) FROM vocabulary_items").fetchone()[0]

    targets = find_targets(conn)
    print(f"대상: {len(targets)}건 (템플릿과 완전 일치하는 행만)")
    for item_id, lemma, old_stem, new_stem in targets[:5]:
        print(f"  {item_id} [{lemma}]")
        print(f"    전: {old_stem}")
        print(f"    후: {new_stem}")
    if len(targets) > 5:
        print(f"  ... 외 {len(targets) - 5}건")

    if not targets:
        print("변경할 행이 없습니다.")
        conn.close()
        return 0

    if not apply:
        print("\nDRY-RUN이므로 DB는 변경되지 않았습니다. 적용하려면 --apply를 붙여 재실행하세요.")
        conn.close()
        return 0

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = database.with_name(f"{database.name}.bak-{timestamp}")
    src = sqlite3.connect(database)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    print(f"\n백업 완료(sqlite3.Connection.backup API): {backup_path}")

    for item_id, lemma, old_stem, new_stem in targets:
        conn.execute(
            "UPDATE vocabulary_items SET stem = ?, updated_at = datetime('now') "
            "WHERE item_id = ? AND stem = ?",
            (new_stem, item_id, old_stem),
        )
    conn.commit()

    total_after = conn.execute("SELECT COUNT(*) FROM vocabulary_items").fetchone()[0]
    remaining = find_targets(conn)
    print(f"\n적용 완료: {len(targets)}건 수정")
    print(f"행 수 불변 확인: 전 {total_before} -> 후 {total_after}")
    print(f"남은 미보정 행(재검사): {len(remaining)}건 (0이어야 함)")
    conn.close()
    return 0 if total_before == total_after and not remaining else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="vocabulary_items.stem 따옴표 누락 보정 (기본 dry-run)")
    parser.add_argument("--database", type=Path, default=get_db_path())
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    return run(args.database, args.apply)


if __name__ == "__main__":
    sys.exit(main())
