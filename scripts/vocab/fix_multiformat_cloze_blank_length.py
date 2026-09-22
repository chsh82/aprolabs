"""vocabulary_multiformat_items(CONTEXT_CLOZE)의 빈칸 밑줄 길이를 정답 글자수에 맞춘다.

배경: 원본 파일럿 생성기(ZIP 안 generate_pilot.py 179행)가 빈칸을 실제 정답
길이와 무관하게 고정 4칸("＿＿＿＿")으로 만들었다. input_hint 필드는 이미
"N글자"로 정확한 길이를 알려주는데, 화면에 보이는 밑줄은 늘 4칸이라 예를
들어 정답이 3글자(작업복)인데 밑줄은 4칸으로 보여 혼란을 준다는 사용자
보고로 확인함.

215건 전수 조사 결과 prompt마다 "＿＿＿＿"가 정확히 1회씩만 등장하고
answer_text가 전부 비어있지 않음을 확인했다(scripts/vocab/... 개발 중 검증) -
그 1회 등장하는 자리를 정답 글자수만큼의 밑줄로 교체하는 최소 수정만 한다.
문구의 나머지 부분(뜻 도움말, 예문)은 건드리지 않는다.

기본은 dry-run. --apply를 줘야 실제 UPDATE하며, 적용 전 sqlite3.Connection.
backup() API로 스냅샷을 뜬다. 정답 길이와 이미 일치하는 행(우연히 4글자인
경우 등)은 자동으로 건너뛰어 멱등하다.

실행 예:
    APP_ENV=local_rnd VOCABULARY_QUIZ_DB_PATH=data/vocab/vocabulary_quiz_rnd.db \\
    python scripts/vocab/fix_multiformat_cloze_blank_length.py --database data/vocab/vocabulary_quiz_rnd.db
    (기본 dry-run, --apply 붙이면 실제 적용)
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
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

FIXED_BLANK = "＿＿＿＿"  # ＿＿＿＿ (원본 파일럿 생성기가 항상 쓰는 고정 4칸)
BLANK_CHAR = "＿"  # ＿
_BLANK_RUN_RE = re.compile(BLANK_CHAR + "+")
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


def find_targets(conn: sqlite3.Connection) -> tuple[list[tuple[str, str, str]], list[str]]:
    """반환: ([(item_id, old_prompt, new_prompt), ...], [이상 건 item_id 목록]).

    밑줄 런(연속된 ＿)을 정규식으로 찾아 그 실제 길이를 정답 글자수와 비교한다
    - 고정 4칸 문자열과의 단순 substring 매칭이 아니다. 그렇게 하면 정답이
    5글자 이상이라 이미 5개 이상으로 올바르게 고쳐진 밑줄도 그 안에 4글자
    부분열이 우연히 포함되어 있어 "아직 안 고쳐짐"으로 오탐하고, 다시 잘라
    덧붙이는 식으로 매 실행마다 밑줄이 계속 늘어나는 멱등성 버그가 있었다
    (실제로 재현: 정답 길이 5~6인 10건에서 발생) - 런의 실제 길이를 기준으로
    판단해야 이미 올바른 길이인 행을 다시 건드리지 않는다."""
    rows = conn.execute(
        "SELECT item_id, prompt, answer_payload_json FROM vocabulary_multiformat_items "
        "WHERE item_type = 'CONTEXT_CLOZE'"
    ).fetchall()
    targets = []
    anomalies = []
    for item_id, prompt, payload_json in rows:
        payload = json.loads(payload_json)
        answer = payload.get("answer_text") or ""
        matches = list(_BLANK_RUN_RE.finditer(prompt))
        if not answer or len(matches) != 1:
            anomalies.append(item_id)
            continue
        run = matches[0]
        if len(run.group()) == len(answer):
            continue  # 이미 정답 길이와 일치 - 손대지 않음(멱등)
        new_prompt = prompt[:run.start()] + BLANK_CHAR * len(answer) + prompt[run.end():]
        targets.append((item_id, prompt, new_prompt))
    return targets, anomalies


def run(database: Path, apply: bool) -> int:
    print(f"=== CONTEXT_CLOZE 빈칸 길이 보정 ({'실제 적용' if apply else 'DRY-RUN'}) ===")
    assert_local_database_path(database)
    if apply:
        assert_apply_allowed()

    conn = sqlite3.connect(database)
    total_before = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE item_type='CONTEXT_CLOZE'"
    ).fetchone()[0]

    targets, anomalies = find_targets(conn)
    print(f"대상: {len(targets)}건 (정답 길이와 밑줄 길이가 다른 행)")
    if anomalies:
        print(f"이상 건(빈칸 0/2개 이상 또는 정답 없음, 건드리지 않음): {len(anomalies)}건 - {anomalies[:5]}")
    for item_id, old_prompt, new_prompt in targets[:5]:
        print(f"  {item_id}")
        print(f"    전: {old_prompt!r}")
        print(f"    후: {new_prompt!r}")
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

    for item_id, old_prompt, new_prompt in targets:
        conn.execute(
            "UPDATE vocabulary_multiformat_items SET prompt = ?, updated_at = datetime('now') "
            "WHERE item_id = ? AND prompt = ?",
            (new_prompt, item_id, old_prompt),
        )
    conn.commit()

    total_after = conn.execute(
        "SELECT COUNT(*) FROM vocabulary_multiformat_items WHERE item_type='CONTEXT_CLOZE'"
    ).fetchone()[0]
    remaining, _ = find_targets(conn)
    print(f"\n적용 완료: {len(targets)}건 수정")
    print(f"행 수 불변 확인: 전 {total_before} -> 후 {total_after}")
    print(f"남은 미보정 행(재검사): {len(remaining)}건 (0이어야 함)")
    conn.close()
    return 0 if total_before == total_after and not remaining else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="다유형 어휘 퀴즈 CONTEXT_CLOZE 빈칸 길이 보정 (기본 dry-run)")
    parser.add_argument("--database", type=Path, default=get_db_path())
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    return run(args.database, args.apply)


if __name__ == "__main__":
    sys.exit(main())
