"""vocab DB(`data/vocab/idiom.db`)의 헤드워드 파싱 오염 4건 정리.

split_hanja.py --dry-run에서 "idiom.hanja 길이가 4자가 아님"으로 걸린
4건(양자택일兩/유방백세流/유유상종類類/임기응변臨)을 조사한 결과, 전부
같은 패턴의 오염이다 - PDF2 파싱 시 헤드워드 뒤에 한자 1~2글자가
잘못 붙어서(예: "양자택일" + "兩" -> "양자택일兩") 헤드워드는 5~6자가
되고 hanja 필드는 앞글자가 빠진 3~2자로 줄었다. 유출된 글자를 hanja
앞에 되돌리면 원래 4자 한자와 정확히 맞아떨어진다:

    corrupted.headword[:4]  == 정상 헤드워드
    corrupted.headword[4:]  == 유출된 한자 (1~2자)
    유출된 한자 + corrupted.hanja == 정상 4자 한자 (NFKC 정규화 후 비교)

유출된 글자는 육안으로는 정상 한자와 똑같이 보이지만 실제로는 다른
코드포인트다(CJK Compatibility Ideographs, U+F900대 - PDF2 파싱 스크립트가
헤드워드/한자 분리 시 훑던 '\\u4e00-\\u9fff' 범위 밖이라 한자로 인식하지
못해 헤드워드 쪽에 남은 것으로 추정된다). 그래서 파이썬 문자열을 그냥
비교하면 다른 문자로 보이고, `unicodedata.normalize('NFKC', ...)`를
거쳐야 정상 한자와 일치가 확인된다 - 강제로 갈아치우는 게 아니라 원래
같은 글자라는 걸 정규화로 확인하는 것.

이 스크립트는 이걸로 두 경우를 구분한다.

1) 정상 헤드워드가 이미 idiom 테이블에 따로 존재 (진짜 중복 - 3건:
   양자택일/유방백세/유유상종). 오염된 행을 삭제한다. 다만 오염된 행에만
   좋은 정의/레벨이 붙어 있고 정상 행은 definition이 비어 있는 경우가
   있다(양자택일·유유상종 - literacy 병합 시 헤드워드 텍스트가 달라 같은
   성어로 묶이지 못하고 별도 항목이 됐던 것) - 이 경우 삭제 전에 정상
   행의 meaning/level_note를 오염된 행 것으로 채워서 데이터를 살린다.
   유방백세는 두 행의 내용이 이미 같아서 채울 필요가 없다.

   살려오는 정의 앞에 "1994학년도 1차 수능" 같은 출제 정보가 붙어 있는
   경우(유유상종) - 이건 sajaseongeo_parser.py의 날짜 토큰 제거 정규식이
   "학년도 예비 시행"과 "YYYY.수능" 두 형식만 다루고 1994학년도 특유의
   "N차 수능"(그해만 두 번 시행돼서 생긴 표기) 형식을 놓친 것이다.
   사용자 결정(2026-09-02) - 이 접두어는 떼어 내고 meaning에는 순수
   정의만 남기고, 뗀 부분은 inclusion_evidence(source_type='exam')로
   따로 기록한다.

2) 정상 헤드워드가 아예 없음 (1건: 임기응변). 오염된 행을 삭제하고,
   복구용 정보(정상 헤드워드/한자/정의/레벨)를 docs/vocab/MISSING.md에
   적어 나중에 수작업으로 다시 넣을 수 있게 한다.

inclusion_evidence는 idiom_id ON DELETE CASCADE라 idiom 행을 지우면
자동으로 같이 지워진다(PRAGMA foreign_keys=ON) - 사용자가 명시적으로
같이 지우라고 했으므로 삭제 후 남은 행이 없는지 별도로 확인한다.

실행:
    python scripts/vocab/fix_duplicates.py --dry-run   # 저장 없이 계획만 출력
    python scripts/vocab/fix_duplicates.py              # 실제 삭제/갱신 + MISSING.md 기록
"""
from __future__ import annotations

import argparse
import io
import re
import sqlite3
import sys
import unicodedata
from datetime import date
from pathlib import Path

if sys.platform == "win32" and (sys.stdout.encoding or "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.vocab.db import get_db_path as get_vocab_db_path  # noqa: E402

MISSING_MD_PATH = REPO_ROOT / "docs" / "vocab" / "MISSING.md"

# "1994학년도 1차 수능 <정의>" 처럼 출제 정보가 정의 앞에 그대로 남은 경우 분리한다.
_EXAM_DATE_PREFIX_RE = re.compile(r"^(\d{4}학년도\s+\d차\s+수능)\s+(?=\S)")


def strip_exam_date_prefix(meaning: str | None) -> tuple[str | None, str | None]:
    """"1994학년도 1차 수능 같은 무리끼리 서로 사귐" -> ("같은 무리끼리 서로 사귐", "1994학년도 1차 수능")."""
    if not meaning:
        return meaning, None
    m = _EXAM_DATE_PREFIX_RE.match(meaning)
    if not m:
        return meaning, None
    return meaning[m.end():], m.group(1)


def fetch_corrupted(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT idiom_id, headword, hanja, meaning, level_note FROM idiom WHERE length(hanja) != 4"
    ).fetchall()
    out = []
    for idiom_id, headword, hanja, meaning, level_note in rows:
        clean_headword = headword[:4]
        leaked = headword[4:]
        reconstructed_hanja = leaked + hanja
        out.append({
            "idiom_id": idiom_id,
            "headword": headword,
            "hanja": hanja,
            "meaning": meaning,
            "level_note": level_note,
            "clean_headword": clean_headword,
            "leaked": leaked,
            "reconstructed_hanja": reconstructed_hanja,
        })
    return out


def plan(conn: sqlite3.Connection, corrupted: list[dict]) -> list[dict]:
    plans = []
    for c in corrupted:
        if len(c["clean_headword"]) != 4 or len(c["reconstructed_hanja"]) != 4:
            plans.append({**c, "action": "manual_review", "reason": "헤드워드 앞 4자/재구성 한자가 4자 규칙에 안 맞음"})
            continue

        sibling = conn.execute(
            "SELECT idiom_id, headword, hanja, meaning, level_note FROM idiom WHERE headword = ?",
            (c["clean_headword"],),
        ).fetchone()

        if sibling is None:
            plans.append({**c, "action": "delete_orphan"})
            continue

        sib_id, sib_headword, sib_hanja, sib_meaning, sib_level_note = sibling
        if unicodedata.normalize("NFKC", sib_hanja) != unicodedata.normalize("NFKC", c["reconstructed_hanja"]):
            plans.append({
                **c, "action": "manual_review",
                "reason": f"재구성 한자({c['reconstructed_hanja']})가 정상 행의 hanja({sib_hanja})와 "
                          f"NFKC 정규화 후에도 다름",
            })
            continue

        needs_salvage = not sib_meaning and c["meaning"]
        salvage_meaning, exam_detail = strip_exam_date_prefix(c["meaning"]) if needs_salvage else (None, None)
        plans.append({
            **c,
            "action": "delete_duplicate",
            "sibling_idiom_id": sib_id,
            "sibling_meaning": sib_meaning,
            "sibling_level_note": sib_level_note,
            "needs_salvage": needs_salvage,
            "salvage_meaning": salvage_meaning,
            "exam_evidence_detail": exam_detail,
        })
    return plans


def print_report(plans: list[dict]) -> None:
    print(f"=== 오염 행 {len(plans)}건 처리 계획 ===\n")
    for p in plans:
        print(f"- idiom_id={p['idiom_id']} headword={p['headword']!r} hanja={p['hanja']!r}")
        print(f"    복원: 정상헤드워드={p['clean_headword']!r} 유출글자={p['leaked']!r} "
              f"재구성한자={p['reconstructed_hanja']!r}")
        if p["action"] == "manual_review":
            print(f"    -> 자동 처리 보류(수동 확인 필요): {p['reason']}")
        elif p["action"] == "delete_orphan":
            print(f"    -> 정상 헤드워드 행 없음(고아). 이 행을 삭제하고 아래 내용을 "
                  f"docs/vocab/MISSING.md에 기록:")
            print(f"       headword={p['clean_headword']} hanja={p['reconstructed_hanja']} "
                  f"meaning={p['meaning']!r} level_note={p['level_note']!r}")
        elif p["action"] == "delete_duplicate":
            print(f"    -> 정상 행(idiom_id={p['sibling_idiom_id']}) 존재. 오염 행 삭제.")
            if p["needs_salvage"]:
                print(f"       정상 행의 meaning이 비어 있음 - 삭제 전에 오염 행의 값으로 채움:")
                if p["exam_evidence_detail"]:
                    print(f"         meaning: NULL -> {p['salvage_meaning']!r} "
                          f"(원문에서 출제정보 {p['exam_evidence_detail']!r} 분리)")
                    print(f"         inclusion_evidence 추가: source_type='exam', "
                          f"detail={p['exam_evidence_detail']!r}")
                else:
                    print(f"         meaning: NULL -> {p['salvage_meaning']!r}")
                print(f"         level_note: {p['sibling_level_note']!r} -> "
                      f"{p['sibling_level_note']!r} + 복구 안내")
            else:
                print(f"       정상 행에 이미 meaning이 있어 추가 갱신 없음.")
        print()


def build_missing_entry(p: dict) -> str:
    today = date.today().isoformat()
    return (
        f"## {p['clean_headword']} ({p['reconstructed_hanja']})\n\n"
        f"- 발견: {today}, fix_duplicates.py 실행 중 - 원래 헤드워드가 "
        f"`{p['headword']}`(파싱 오염, idiom_id={p['idiom_id']})로만 존재했고 정상 헤드워드 행이 없었다.\n"
        f"- 삭제된 오염 행의 내용(참고용, idiom 테이블에는 더 이상 없음):\n"
        f"  - meaning: {p['meaning']}\n"
        f"  - level_note: {p['level_note']}\n"
        f"- 할 일: `{p['clean_headword']}` headword, `{p['reconstructed_hanja']}` hanja로 idiom 행을 "
        f"수작업으로 다시 만들 것. meaning/level은 위 참고값을 검수해서 채운다.\n"
    )


def run(dry_run: bool) -> list[dict]:
    conn = sqlite3.connect(get_vocab_db_path())
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        corrupted = fetch_corrupted(conn)
        plans = plan(conn, corrupted)

        if dry_run:
            return plans

        missing_entries = []
        for p in plans:
            if p["action"] == "manual_review":
                continue

            if p["action"] == "delete_duplicate" and p["needs_salvage"]:
                recovery_note = (p["sibling_level_note"] or "") + " / 오염된 중복 항목(헤드워드 파싱 버그)에서 정의 복구"
                conn.execute(
                    "UPDATE idiom SET meaning = ?, level_note = ? WHERE idiom_id = ?",
                    (p["salvage_meaning"], recovery_note, p["sibling_idiom_id"]),
                )
                if p["exam_evidence_detail"]:
                    conn.execute(
                        "INSERT INTO inclusion_evidence (idiom_id, source_type, detail) VALUES (?, 'exam', ?)",
                        (p["sibling_idiom_id"], p["exam_evidence_detail"]),
                    )

            if p["action"] == "delete_orphan":
                missing_entries.append(build_missing_entry(p))

            conn.execute("DELETE FROM inclusion_evidence WHERE idiom_id = ?", (p["idiom_id"],))
            conn.execute("DELETE FROM idiom WHERE idiom_id = ?", (p["idiom_id"],))

        conn.commit()

        if missing_entries:
            MISSING_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
            existing = MISSING_MD_PATH.read_text(encoding="utf-8") if MISSING_MD_PATH.exists() else (
                "# 수작업으로 다시 넣어야 할 표제어\n\n"
                "fix_duplicates.py 등 정리 스크립트가 삭제하면서 발견한, 데이터가 없어져 "
                "수작업 복구가 필요한 항목을 여기에 기록한다.\n"
            )
            with open(MISSING_MD_PATH, "w", encoding="utf-8") as f:
                f.write(existing.rstrip() + "\n\n" + "\n".join(missing_entries))

        return plans
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="vocab idiom.db 헤드워드 파싱 오염 4건 정리")
    parser.add_argument("--dry-run", action="store_true", help="DB에 저장하지 않고 계획만 출력")
    args = parser.parse_args()

    plans = run(dry_run=args.dry_run)
    print_report(plans)

    if args.dry_run:
        print("--dry-run - DB에 저장하지 않음. 승인 후 --dry-run 없이 재실행하세요.")
    else:
        n_deleted = sum(1 for p in plans if p["action"] in ("delete_duplicate", "delete_orphan"))
        n_manual = sum(1 for p in plans if p["action"] == "manual_review")
        print(f"삭제 완료: {n_deleted}건, 수동 확인 보류: {n_manual}건")
        if any(p["action"] == "delete_orphan" for p in plans):
            print(f"docs/vocab/MISSING.md 갱신 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
