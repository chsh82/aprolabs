"""phase6이 찾은 momo_book.db 대표선정 오류 26건을 재현하고, literacy.db 쪽
실제 id(terms.id)까지 확정한다. 읽기 전용(momo_book.db mode=ro, literacy.db
raw sqlite3 SELECT만).
"""
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "literacy"))
from normalize import clean_headword, is_suspected_swap  # noqa: E402

TEXTBOOK_DB = REPO_ROOT / "momo_book_db" / "momo_book.db"
LITERACY_DB = REPO_ROOT / "data" / "literacy.db"

LEVEL_TO_GRADE = {f"L{i}": i for i in range(1, 10)}


def get_textbook_rows():
    conn = sqlite3.connect(f"file:{TEXTBOOK_DB.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT v.id, v.word, v.definition, v.example_sentence, d.level
        FROM vocabulary v JOIN documents d ON v.doc_id = d.doc_id
        """
    ).fetchall()
    conn.close()
    return rows


class Candidate:
    def __init__(self, row):
        self.vocab_id = row["id"]
        self.definition = row["definition"]
        self.level = row["level"]
        self.grade_level = LEVEL_TO_GRADE.get(row["level"])
        self.clean = clean_headword(row["word"])
        self.suspected = is_suspected_swap(self.clean.cleaned)


def main():
    rows = get_textbook_rows()
    cands = [Candidate(r) for r in rows]
    clean_ok = [c for c in cands if not c.suspected]
    print(f"전체 {len(cands)}행, 컬럼의심 제외 후 {len(clean_ok)}행")

    groups = defaultdict(list)
    for c in clean_ok:
        groups[c.clean.cleaned].append(c)

    dup_groups = {h: g for h, g in groups.items() if len(g) >= 2}
    print(f"정제 후 표제어 2회 이상 등장: {len(dup_groups)}건")

    multi_level_groups = {}
    for h, g in dup_groups.items():
        levels = {c.level for c in g}
        if len(levels) >= 2:
            multi_level_groups[h] = g
    print(f"서로 다른 레벨에 걸쳐 등장: {len(multi_level_groups)}건")

    results = []
    for h, g in multi_level_groups.items():
        ranked = sorted(g, key=lambda c: (c.grade_level, c.vocab_id))
        rep = ranked[0]
        others = ranked[1:]
        rep_def = rep.definition or ""
        max_other_len = max((len(o.definition or "") for o in others), default=0)

        flag = None
        if not rep_def:
            flag = "NULL_DEF"
        elif max_other_len > 0 and len(rep_def) < max_other_len * 0.5:
            flag = "SHORT_DEF"
        elif rep_def.count(".") >= 1:
            # 구두점/어순 손상 패턴: 마침표 뒤 아주 짧은 잔여 텍스트가 남는 경우
            # (2-2절의 "혼비백산/독불장군" 패턴 재현 - 정성적 신호라 사람이 최종 확인)
            last_seg = rep_def.split(".")[-1].strip()
            if 0 < len(last_seg) <= 6 and len(rep_def) > 10:
                flag = "PUNCT_DAMAGE"

        results.append({
            "headword": h,
            "rep_vocab_id": rep.vocab_id,
            "rep_level": rep.level,
            "rep_def": rep_def,
            "other_candidates": [(o.vocab_id, o.level, o.definition) for o in others],
            "flag": flag,
        })

    flagged = [r for r in results if r["flag"]]
    print(f"손상 신호로 걸린 건수: {len(flagged)}")
    for r in flagged:
        print(f"  [{r['flag']}] {r['headword']} rep_vocab_id={r['rep_vocab_id']} rep_level={r['rep_level']} rep_def={r['rep_def']!r}")
        for oc in r["other_candidates"]:
            print(f"      other: vocab_id={oc[0]} level={oc[1]} def={oc[2]!r}")

    # literacy.db 쪽 매핑
    print()
    print("=== literacy.db 교차 조회 ===")
    lconn = sqlite3.connect(f"file:{LITERACY_DB.as_posix()}?mode=ro", uri=True)
    lconn.row_factory = sqlite3.Row
    out_rows = []
    for r in flagged:
        vid = str(r["rep_vocab_id"])
        trow = lconn.execute(
            "SELECT id, headword, definition, source, external_id, review_status, level, category, note "
            "FROM terms WHERE source='momo-textbook' AND external_id=?",
            (vid,),
        ).fetchone()
        out_rows.append((r, trow))
        if trow:
            print(f"{r['headword']} -> literacy.db id={trow['id']} review_status={trow['review_status']} def={trow['definition']!r}")
        else:
            print(f"{r['headword']} -> literacy.db NOT FOUND (external_id={vid})")

    print()
    print(f"플래그 건수 합계: {len(flagged)} (NULL_DEF={sum(1 for r in flagged if r['flag']=='NULL_DEF')}, "
          f"SHORT_DEF={sum(1 for r in flagged if r['flag']=='SHORT_DEF')}, "
          f"PUNCT_DAMAGE={sum(1 for r in flagged if r['flag']=='PUNCT_DAMAGE')})")

    # 결과를 파일로 저장(다음 단계에서 재사용)
    import json
    out = []
    for r, trow in out_rows:
        out.append({
            "headword": r["headword"],
            "flag": r["flag"],
            "rep_vocab_id": r["rep_vocab_id"],
            "rep_level": r["rep_level"],
            "rep_def": r["rep_def"],
            "other_candidates": r["other_candidates"],
            "literacy_id": trow["id"] if trow else None,
            "literacy_review_status": trow["review_status"] if trow else None,
            "literacy_definition": trow["definition"] if trow else None,
            "literacy_note": trow["note"] if trow else None,
            "literacy_level": trow["level"] if trow else None,
            "literacy_category": trow["category"] if trow else None,
        })
    outpath = REPO_ROOT / "data" / "import" / "literacy_repr_errors_26_scan_20260924.json"
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"저장: {outpath}")


if __name__ == "__main__":
    main()
