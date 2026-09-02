"""literacy DB(`data/literacy.db`)의 term_hanja(사자성어 낱글자 훈음 분해)를
vocab DB(`data/vocab/idiom.db`)의 `hanja`/`idiom_hanja`로 옮긴다.

scripts/literacy/import_sajaseongeo.py의 노트대로, literacy의 사자성어
225건 중 156건에만 term_hanja(글자별 훈음 분해)가 있다(나머지 69건은
PDF2에만 있어서 낱글자 분해가 없음). 그 156건도 원본 PDF 파싱 과정의
결함이 그대로 남아 있어(글자 수가 4가 아니거나, 훈음 텍스트에 공백이
없거나, 콤마 누락으로 다음 글자 데이터가 섞이는 등) 전부 그대로 옮길 수
없다 - 그래서 이 스크립트는 4단계 게이트를 통과한 것만 idiom_hanja를
만들고, 나머지는 사유별로 목록만 보고한다(사용자 결정 - "억지로 파싱하지
마").

게이트(전부 통과해야 idiom_hanja 생성, 하나라도 실패하면 해당 성어는
건너뛰고 사유 목록에 기록):
    A. idiom.hanja 길이가 4가 아니다
       (fix_duplicates.py로 4건 전부 정리됨 - 이제 걸리는 게 있으면
       새로 생긴 데이터 문제라는 뜻이라 그대로 남겨 둔다)
    B. literacy에 이 headword의 term_hanja가 아예 없다 (69건 - PDF2 전용 항목)
    C. literacy term_hanja 행 수가 4가 아니다 (백척간두 1자, 이열치열 3자 -
       PDF1의 콤마 누락 버그, scripts/literacy/sajaseongeo_parser.py 참고)

과거에는 D 게이트로 "literacy 글자 조합이 idiom.hanja와 다르면 건너뛴다"를
뒀는데, 10건을 대조해 보니 6건은 코드포인트만 다른 같은 글자(NFKC
정규화로 확인, normalize_hanja_nfkc.py로 idiom.hanja 자체도 이미 정규화해
둠), 3건(독야청청/청천벽력/청출어람)은 literacy 쪽 글자가 한글 "푸"로
잘못 들어간 실제 오류, 1건(망양지탄)은 이체자 차이였다. 사용자 결정
(2026-09-02) - **idiom.hanja를 기준으로 삼는다.** literacy의 개별 글자는
훈음(hun/eum) 텍스트를 가져오는 용도로만 쓰고, 글자 자체(char)는 항상
idiom.hanja[position]에서 가져온다. 그래서 D 게이트는 없앴다 - literacy
글자가 다르더라도 건너뛰지 않고, 대신 참고용으로 "literacy_char_diff"
목록에 기록한다(무시하고 진행하되 뭐가 달랐는지는 남겨 둔다).

B/C를 통과한 성어는 idiom_hanja(position, char)를 만든다. 훈음("물 수")
분리는 별도 실패 사유가 있다:
    E. 훈음 텍스트에 한자가 섞여 있다(콤마 누락으로 다음 글자 데이터가
       붙은 것 - 예: "다스릴 치 熱더울 열")
    F. 훈음 텍스트에 전각공백(　)만 있고 일반 공백이 없다 - 예: "흙　토"
공백이 전혀 없는 경우(예: "일백백", "입구")는 사용자 결정(2026-09-02)에
따라 마지막 한 글자를 eum, 나머지를 hun으로 나눈다(음은 항상 한 글자) -
더 이상 실패로 치지 않는다. E/F에 걸린 글자만 hanja 테이블에 char만
넣고 hun/eum은 NULL로 둔다(그래도 idiom_hanja 연결 자체는 만든다 -
글자 정체성과 훈음 텍스트 품질은 별개 문제이므로).

같은 한자가 여러 성어에 나오면 hanja는 처음 본 값으로만 채운다
(INSERT OR IGNORE, char가 PK) - 사용자 결정.

실행:
    python scripts/vocab/split_hanja.py --dry-run   # 저장 없이 사유별 집계만 출력
    python scripts/vocab/split_hanja.py              # 실제 적재
"""
from __future__ import annotations

import argparse
import io
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.literacy.db import get_db_path as get_literacy_db_path  # noqa: E402
from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402

_CJK_RE = re.compile(r"[一-鿿]")


def fetch_literacy_term_hanja() -> dict[str, list[tuple[int, str, str]]]:
    """headword -> [(position, char, meaning_reading), ...] (position 순 정렬)."""
    conn = sqlite3.connect(get_literacy_db_path())
    rows = conn.execute(
        """
        SELECT t.headword, th.position, h.character, h.meaning_reading
        FROM terms t
        JOIN term_hanja th ON th.term_id = t.id
        JOIN hanja h ON h.id = th.hanja_id
        WHERE t.category = '사자성어'
        ORDER BY t.headword, th.position
        """
    ).fetchall()
    conn.close()
    by_head: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    for headword, position, character, meaning_reading in rows:
        by_head[headword].append((position, character, meaning_reading))
    return by_head


def fetch_vocab_idioms() -> list[tuple[int, str, str]]:
    conn = sqlite3.connect(get_vocab_db_path())
    rows = conn.execute("SELECT idiom_id, headword, hanja FROM idiom ORDER BY idiom_id").fetchall()
    conn.close()
    return rows


def try_split_hun_eum(meaning_reading: str | None) -> tuple[str, str] | None:
    """"물 수" -> ("물", "수"). 형식이 안 맞으면 None (억지로 나누지 않는다).

    사용자 결정(2026-09-02): 공백이 전혀 없는 경우("일백백", "입구")는
    마지막 한 글자를 eum, 나머지를 hun으로 나눈다(음은 항상 한 글자).
    한자가 섞였거나(콤마 누락 오염) 전각공백(\\u3000)이 있는 건 형식이
    다른 것으로 보고 그대로 NULL로 둔다 - 억지로 나누지 않는다.
    """
    if not meaning_reading:
        return None
    if _CJK_RE.search(meaning_reading):
        return None  # 한자 혼입(사유 F) - 콤마 누락으로 다음 글자 데이터가 붙은 경우
    if "　" in meaning_reading:
        return None  # 전각공백 - 일반 공백과 다른 형식이니 그대로 NULL

    if " " in meaning_reading:
        hun, eum = meaning_reading.rsplit(" ", 1)
        hun, eum = hun.strip(), eum.strip()
        if not hun or not eum:
            return None
        return hun, eum

    # 공백 없음(사유 E) - 마지막 한 글자를 eum, 나머지를 hun으로 나눈다
    stripped = meaning_reading.strip()
    if len(stripped) < 2:
        return None
    hun, eum = stripped[:-1], stripped[-1]
    if not hun or not eum:
        return None
    return hun, eum


def classify(idioms: list[tuple[int, str, str]], by_head: dict) -> dict:
    reason_a = []  # idiom.hanja 길이 != 4
    reason_b = []  # literacy에 term_hanja 없음
    reason_c = []  # term_hanja 행 수 != 4
    literacy_char_diff = []  # 참고용 - literacy 글자가 idiom.hanja와 다르지만 진행함
    candidates = []  # (idiom_id, headword, hanja, [(position 1~4, literacy_char, meaning_reading), ...])

    for idiom_id, headword, hanja in idioms:
        normalized_hanja = unicodedata.normalize("NFKC", hanja or "")
        if len(normalized_hanja) != 4:
            reason_a.append((headword, hanja))
            continue
        chars = by_head.get(headword)
        if not chars:
            reason_b.append(headword)
            continue
        if len(chars) != 4:
            reason_c.append((headword, len(chars), chars))
            continue
        chars_sorted = sorted(chars, key=lambda x: x[0])
        for i, (position, lit_char, meaning_reading) in enumerate(chars_sorted):
            if unicodedata.normalize("NFKC", lit_char) != normalized_hanja[i]:
                literacy_char_diff.append((headword, i + 1, normalized_hanja[i], lit_char))
        candidates.append(
            (idiom_id, headword, normalized_hanja,
             [(i + 1, c[2]) for i, c in enumerate(chars_sorted)])
        )

    format_fail = []  # (headword, position, char, meaning_reading)
    hanja_plan: dict[str, tuple[str | None, str | None]] = {}
    hanja_conflicts = []  # (char, first_reading, later_reading)
    idiom_hanja_plan = []  # (idiom_id, position, char)

    for idiom_id, headword, hanja, positions in candidates:
        for position, meaning_reading in positions:
            char = hanja[position - 1]  # idiom.hanja가 기준 - literacy 글자는 훈음 텍스트만 쓴다
            split = try_split_hun_eum(meaning_reading)
            if split is None:
                format_fail.append((headword, position, char, meaning_reading))
                hun, eum = None, None
            else:
                hun, eum = split

            if char in hanja_plan:
                existing_hun, existing_eum = hanja_plan[char]
                if (existing_hun, existing_eum) != (hun, eum) and (existing_hun or existing_eum):
                    hanja_conflicts.append((char, (existing_hun, existing_eum), (hun, eum)))
            else:
                hanja_plan[char] = (hun, eum)

            idiom_hanja_plan.append((idiom_id, position, char))

    return {
        "reason_a": reason_a,
        "reason_b": reason_b,
        "reason_c": reason_c,
        "literacy_char_diff": literacy_char_diff,
        "candidates": candidates,
        "format_fail": format_fail,
        "hanja_plan": hanja_plan,
        "hanja_conflicts": hanja_conflicts,
        "idiom_hanja_plan": idiom_hanja_plan,
    }


def print_report(idioms: list, result: dict) -> None:
    total = len(idioms)
    n_a, n_b, n_c = len(result["reason_a"]), len(result["reason_b"]), len(result["reason_c"])
    n_ok = len(result["candidates"])
    print(f"=== 대상 idiom 총 {total}건 ===")
    print(f"성공(idiom_hanja 생성 대상): {n_ok}건")
    print(f"건너뜀: {n_a + n_b + n_c}건")
    print(f"  A. idiom.hanja 길이가 4자가 아님: {n_a}건")
    for headword, hanja in result["reason_a"]:
        print(f"     {headword}: hanja={hanja!r}")
    print(f"  B. literacy에 훈음 분해(term_hanja) 없음: {n_b}건")
    print(f"     {result['reason_b']}")
    print(f"  C. literacy term_hanja 글자 수가 4가 아님: {n_c}건")
    for headword, n, chars in result["reason_c"]:
        print(f"     {headword}: {n}자 - {chars}")

    if result["literacy_char_diff"]:
        print(f"\n=== 참고: literacy 글자가 idiom.hanja와 달랐지만 idiom.hanja를 사용함: "
              f"{len(result['literacy_char_diff'])}건 ===")
        for headword, position, used_char, literacy_char in result["literacy_char_diff"]:
            print(f"     {headword}[{position}] idiom.hanja={used_char!r} vs literacy={literacy_char!r} "
                  f"(idiom.hanja 사용)")

    print(f"\n=== 성공 {n_ok}건 중 훈음(hun/eum) 형식 실패 글자 ===")
    print(f"  E/F. 한자 혼입 또는 전각공백으로 hun/eum을 NULL로 둘 글자: {len(result['format_fail'])}건")
    for headword, position, char, meaning_reading in result["format_fail"]:
        print(f"     {headword}[{position}] {char}: meaning_reading={meaning_reading!r}")

    if result["hanja_conflicts"]:
        print(f"\n=== 같은 한자, 다른 훈음 충돌(먼저 본 값 유지 예정): {len(result['hanja_conflicts'])}건 ===")
        for char, first, later in result["hanja_conflicts"]:
            print(f"  {char}: 먼저 본 값={first} vs 나중 값={later}")

    n_hanja = len(result["hanja_plan"])
    n_link = len(result["idiom_hanja_plan"])
    print(f"\n=== 적재 예정 ===")
    print(f"hanja 신규 행(고유 글자): {n_hanja}건")
    print(f"idiom_hanja 연결 행: {n_link}건 (= 성공 {n_ok}건 x 4글자)")


def run(dry_run: bool) -> dict:
    by_head = fetch_literacy_term_hanja()
    idioms = fetch_vocab_idioms()
    result = classify(idioms, by_head)

    if dry_run:
        return {"idioms": idioms, "result": result}

    conn = sqlite3.connect(get_vocab_db_path())
    conn.execute("PRAGMA foreign_keys=ON")
    hanja_inserted = 0
    link_inserted = 0
    try:
        for char, (hun, eum) in result["hanja_plan"].items():
            cur = conn.execute(
                "INSERT OR IGNORE INTO hanja (char, hun, eum, grade, is_basic900) VALUES (?, ?, ?, NULL, 0)",
                (char, hun, eum),
            )
            hanja_inserted += cur.rowcount

        for idiom_id, position, char in result["idiom_hanja_plan"]:
            cur = conn.execute(
                "INSERT OR IGNORE INTO idiom_hanja (idiom_id, position, char) VALUES (?, ?, ?)",
                (idiom_id, position, char),
            )
            link_inserted += cur.rowcount

        conn.commit()
    finally:
        conn.close()

    return {"idioms": idioms, "result": result, "hanja_inserted": hanja_inserted, "link_inserted": link_inserted}


def main() -> int:
    parser = argparse.ArgumentParser(description="literacy term_hanja -> vocab hanja/idiom_hanja 이관")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 사유별 집계만 출력")
    args = parser.parse_args()

    output = run(dry_run=args.dry_run)
    print_report(output["idioms"], output["result"])

    if args.dry_run:
        print("\n--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
    else:
        print(f"\nhanja 신규 삽입: {output['hanja_inserted']}건")
        print(f"idiom_hanja 신규 삽입: {output['link_inserted']}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())
