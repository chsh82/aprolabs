#!/usr/bin/env python3
"""Phase13: read-only REUSE candidate lookup against vocabulary_contents (research DB).
Read-only (mode=ro). No writes."""
import json
import sqlite3
import sys

DB = "/home/chsh82/aprolabs_data/vocabulary_quiz/vocabulary_quiz_research.db"
LEMMAS_FILE = "/home/chsh82/scratch/phase13_l4_core/lemmas.json"
OUT_FILE = "/home/chsh82/scratch/phase13_l4_core/reuse_candidates.json"


def main():
    lemmas = json.load(open(LEMMAS_FILE, encoding="utf-8"))
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.execute("PRAGMA query_only=ON")
    cur = con.cursor()

    # exact lemma match candidates
    placeholders = ",".join("?" for _ in lemmas)
    cur.execute(
        f"""SELECT vc.content_id, vc.lemma, vc.pos, vc.canonical_definition,
                   vc.student_definition, vc.example_sentence, vc.generation_method,
                   vc.qa_method, vc.generation_status, vc.student_exposure, vc.public_ready,
                   vc.is_active, vcl.vocab_level, vcl.level_status
            FROM vocabulary_contents vc
            LEFT JOIN vocabulary_content_levels vcl ON vcl.content_id = vc.content_id AND vcl.is_active=1
            WHERE vc.lemma IN ({placeholders})""",
        lemmas,
    )
    cols = [d[0] for d in cur.description]
    exact = [dict(zip(cols, row)) for row in cur.fetchall()]

    # full lemma list for homonym/near-form scan (fetch all distinct lemma+pos, no defs to keep light)
    cur.execute("SELECT DISTINCT lemma, pos FROM vocabulary_contents")
    all_lemma_pos = cur.fetchall()

    result = {
        "exact_lemma_matches": exact,
        "all_lemma_pos_count": len(all_lemma_pos),
        "all_lemma_pos": [{"lemma": l, "pos": p} for l, p in all_lemma_pos],
    }
    json.dump(result, open(OUT_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"exact_lemma_matches={len(exact)} all_lemma_pos={len(all_lemma_pos)}")
    print(f"written to {OUT_FILE}")


if __name__ == "__main__":
    main()
