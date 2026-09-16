"""폰트 커버리지 검증 (문제 1)

momo_book.db 전체 텍스트(질문/발췌문/어휘 정의/배경 설명)를 스캔해서, 실제
학습지 렌더링에 쓰이는 두 본문 폰트(Noto Serif KR, Noto Sans KR — Gowun
Batang은 표지 전용이라 DB 텍스트에는 쓰이지 않음)의 유니코드 커버리지 밖에
있는 글자가 있는지 확인한다. 0건이 아니면 글자와 origin doc_id를 출력한다.

실행: python scripts/check-font-coverage.py  (build/assets/fonts/fonts.css를 읽음)
"""
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT.parent / "momo_book.db"
FONTS_CSS = ROOT / "build" / "assets" / "fonts" / "fonts.css"
OUT_JSON = ROOT / "docs" / "_font_coverage_raw.json"

# DB 텍스트 -> 실제로 렌더링에 쓰이는 본문 폰트 두 종류 (표지 전용 Gowun Batang 제외)
BODY_FAMILIES = {"Noto Serif KR", "Noto Sans KR"}

COLUMNS = [
    ("discussion_qa", "doc_id", "question_text"),
    ("discussion_qa", "doc_id", "excerpt_text"),
    ("vocabulary", "doc_id", "definition"),
    ("documents", "doc_id", "background_text"),
]


def parse_fonts_css(path):
    """family -> [(start, end), ...] 코드포인트 구간 리스트."""
    text = path.read_text(encoding="utf-8")
    blocks = re.findall(r"@font-face\s*\{([^}]*)\}", text, re.S)
    coverage = {}
    for block in blocks:
        fam_m = re.search(r"font-family:\s*'([^']+)'", block)
        range_m = re.search(r"unicode-range:\s*([^;]+);", block)
        if not fam_m or not range_m:
            continue
        family = fam_m.group(1)
        ranges = coverage.setdefault(family, [])
        for token in range_m.group(1).split(","):
            token = token.strip()
            m = re.match(r"U\+([0-9A-Fa-f]+)-([0-9A-Fa-f]+)$", token)
            if m:
                ranges.append((int(m.group(1), 16), int(m.group(2), 16)))
                continue
            m = re.match(r"U\+([0-9A-Fa-f]+)$", token)
            if m:
                cp = int(m.group(1), 16)
                ranges.append((cp, cp))
    return coverage


def make_checker(coverage, families):
    merged = []
    for fam in families:
        merged.extend(coverage.get(fam, []))
    merged.sort()

    def covered(cp):
        for start, end in merged:
            if start <= cp <= end:
                return True
            if start > cp:
                break
        return False

    return covered


def scan_db():
    """char -> set(doc_id) (문자당 최대 5개 doc_id만 보관)."""
    con = sqlite3.connect(str(DB_PATH))
    cur = con.cursor()
    char_docs = {}
    total_chars_seen = 0
    for table, doc_col, text_col in COLUMNS:
        cur.execute(f"SELECT {doc_col}, {text_col} FROM {table} WHERE {text_col} IS NOT NULL")
        for doc_id, text in cur.fetchall():
            for ch in text:
                total_chars_seen += 1
                docs = char_docs.setdefault(ch, set())
                if len(docs) < 5:
                    docs.add(doc_id)
    con.close()
    return char_docs, total_chars_seen


def main():
    coverage = parse_fonts_css(FONTS_CSS)
    covered = make_checker(coverage, BODY_FAMILIES)

    char_docs, total_chars_seen = scan_db()
    uncovered = {ch: sorted(docs) for ch, docs in char_docs.items() if not covered(ord(ch)) and not ch.isspace()}

    print(f"DB 전수 스캔: 글자 {total_chars_seen}개(공백 포함) 중 고유 문자 {len(char_docs)}종")
    print(f"검사 대상 폰트: {', '.join(sorted(BODY_FAMILIES))} (build/assets/fonts/fonts.css 기준)")
    print(f"미커버 문자: {len(uncovered)}종")
    for ch, docs in sorted(uncovered.items(), key=lambda kv: -len(char_docs[kv[0]])):
        cp = f"U+{ord(ch):04X}"
        print(f"  '{ch}' ({cp}) - doc_id: {', '.join(docs)}" + (" ..." if len(docs) == 5 else ""))

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(
            {
                "total_chars_seen": total_chars_seen,
                "unique_chars": len(char_docs),
                "uncovered_count": len(uncovered),
                "uncovered": {ch: docs for ch, docs in uncovered.items()},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if uncovered:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
