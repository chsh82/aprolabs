"""situation4용 상황문 CSV를 로드하기 전에 검사한다.

quiz-api.md의 핵심 제약: "situation은 성어를 직접 언급하지 않는 서술문이어야
한다. 성어가 문장에 들어가면 문제가 성립하지 않는다." 사람이 쓰다 보면
무심코 자기 성어나 다른 성어의 헤드워드를 문장에 넣기 쉬워서, 로드 전에
221개 전체 headword를 기준으로 부분 문자열 포함 여부를 기계적으로
검사한다(자기 것이든 남의 것이든 - 다른 성어 이름이 들어가면 그 자체로
힌트가 되거나 다른 문항과 혼선을 줄 수 있어 둘 다 막는다).

CSV 컬럼은 load_examples.py와 같다: idiom_id, sentence, context_type,
grade_band, source. context_type='situation'이 아닌 행은 이 검사 대상이
아니다(situation4 전용 규칙이므로).

실행:
    python scripts/vocab/lint_situations.py <csv경로>
종료 코드 0 = 위반 없음, 1 = 위반 있음(로드하지 말 것).
"""
from __future__ import annotations

import argparse
import csv
import io
import sqlite3
import sys
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path  # noqa: E402


def lint(csv_path: Path) -> list[dict]:
    conn = sqlite3.connect(get_db_path())
    id_to_headword = dict(conn.execute("SELECT idiom_id, headword FROM idiom").fetchall())
    all_headwords = list(id_to_headword.values())
    conn.close()

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    violations = []
    for row in rows:
        if row.get("context_type") != "situation":
            continue
        idiom_id = int(row["idiom_id"])
        sentence = row["sentence"]
        own_headword = id_to_headword.get(idiom_id)
        for hw in all_headwords:
            if hw in sentence:
                violations.append({
                    "idiom_id": idiom_id,
                    "own_headword": own_headword,
                    "found_headword": hw,
                    "is_own": hw == own_headword,
                    "sentence": sentence,
                })
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description="situation4 상황문 CSV 검수 - 성어 이름 포함 여부")
    parser.add_argument("csv_path", type=Path)
    args = parser.parse_args()

    violations = lint(args.csv_path)
    if not violations:
        print("위반 없음 - 로드해도 안전합니다.")
        return 0

    print(f"위반 {len(violations)}건 발견 - 로드하지 마세요:")
    for v in violations:
        which = "본인 성어" if v["is_own"] else "다른 성어"
        print(f"  idiom_id={v['idiom_id']} ({v['own_headword']}) -> 문장에 '{v['found_headword']}'({which}) 포함")
        print(f"    {v['sentence']}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
